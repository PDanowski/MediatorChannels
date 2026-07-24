using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Extensions.DependencyInjection;

namespace MediatorChannels;

public interface IChannelMediatorBuilder
{
    IChannelMediatorBuilder RegisterRequestHandler<TRequest, TResponse, THandler>()
        where TRequest : IRequest<TResponse> where THandler : class, IRequestHandler<TRequest, TResponse>;
    IChannelMediatorBuilder RegisterRequestHandler<TRequest, THandler>()
        where TRequest : IRequest where THandler : class, IRequestHandler<TRequest>;
    IChannelMediatorBuilder RegisterSyncRequestHandler<TRequest, TResponse, THandler>()
        where TRequest : IRequest<TResponse> where THandler : class, ISyncRequestHandler<TRequest, TResponse>;
    IChannelMediatorBuilder RegisterSyncRequestHandler<TRequest, THandler>()
        where TRequest : IRequest where THandler : class, ISyncRequestHandler<TRequest>;
    IChannelMediatorBuilder RegisterNotificationHandler<TNotification, THandler>()
        where TNotification : INotification where THandler : class, INotificationHandler<TNotification>;
    IChannelMediatorBuilder RegisterSyncNotificationHandler<TNotification, THandler>()
        where TNotification : INotification where THandler : class, ISyncNotificationHandler<TNotification>;
    IChannelMediatorBuilder Scan(params Assembly[] assemblies);
}

public static class ChannelMediatorServiceCollectionExtensions
{
    public static IServiceCollection AddChannelMediator(this IServiceCollection services, Action<IChannelMediatorBuilder>? configure = null)
    {
        if (services == null) throw new ArgumentNullException(nameof(services));
        var builder = new ChannelMediatorBuilder(services);
        configure?.Invoke(builder);
        services.AddSingleton(builder.Registry);
        services.AddSingleton<IChannelMediator, ChannelMediator>();
        return services;
    }
}

public static class ChannelMediatorBootstrapper
{
    public static ServiceProvider Create(Action<IChannelMediatorBuilder>? configure = null)
    {
        var services = new ServiceCollection();
        services.AddChannelMediator(configure);
        return services.BuildServiceProvider();
    }
}

internal sealed class ChannelMediatorBuilder : IChannelMediatorBuilder
{
    private readonly IServiceCollection _services;
    internal HandlerRegistry Registry { get; } = new HandlerRegistry();
    public ChannelMediatorBuilder(IServiceCollection services) => _services = services;

    public IChannelMediatorBuilder RegisterRequestHandler<TRequest, TResponse, THandler>() where TRequest : IRequest<TResponse> where THandler : class, IRequestHandler<TRequest, TResponse>
        => AddRequest(typeof(TRequest), typeof(THandler), typeof(IRequestHandler<TRequest, TResponse>), false);
    public IChannelMediatorBuilder RegisterRequestHandler<TRequest, THandler>() where TRequest : IRequest where THandler : class, IRequestHandler<TRequest>
        => AddRequest(typeof(TRequest), typeof(THandler), typeof(IRequestHandler<TRequest>), false);
    public IChannelMediatorBuilder RegisterSyncRequestHandler<TRequest, TResponse, THandler>() where TRequest : IRequest<TResponse> where THandler : class, ISyncRequestHandler<TRequest, TResponse>
        => AddRequest(typeof(TRequest), typeof(THandler), typeof(ISyncRequestHandler<TRequest, TResponse>), true);
    public IChannelMediatorBuilder RegisterSyncRequestHandler<TRequest, THandler>() where TRequest : IRequest where THandler : class, ISyncRequestHandler<TRequest>
        => AddRequest(typeof(TRequest), typeof(THandler), typeof(ISyncRequestHandler<TRequest>), true);
    public IChannelMediatorBuilder RegisterNotificationHandler<TNotification, THandler>() where TNotification : INotification where THandler : class, INotificationHandler<TNotification>
        => AddNotification(typeof(TNotification), typeof(THandler), typeof(INotificationHandler<TNotification>), false);
    public IChannelMediatorBuilder RegisterSyncNotificationHandler<TNotification, THandler>() where TNotification : INotification where THandler : class, ISyncNotificationHandler<TNotification>
        => AddNotification(typeof(TNotification), typeof(THandler), typeof(ISyncNotificationHandler<TNotification>), true);

    public IChannelMediatorBuilder Scan(params Assembly[] assemblies)
    {
        if (assemblies == null) throw new ArgumentNullException(nameof(assemblies));
        foreach (var type in assemblies.Where(a => a != null).SelectMany(a => a.DefinedTypes).Where(t => t.IsClass && !t.IsAbstract))
        foreach (var contract in type.ImplementedInterfaces.Where(i => i.IsGenericType))
        {
            var definition = contract.GetGenericTypeDefinition();
            var argument = contract.GenericTypeArguments[0];
            if (definition == typeof(IRequestHandler<,>)) AddRequest(argument, type.AsType(), contract, false);
            else if (definition == typeof(IRequestHandler<>)) AddRequest(argument, type.AsType(), contract, false);
            else if (definition == typeof(ISyncRequestHandler<,>)) AddRequest(argument, type.AsType(), contract, true);
            else if (definition == typeof(ISyncRequestHandler<>)) AddRequest(argument, type.AsType(), contract, true);
            else if (definition == typeof(INotificationHandler<>)) AddNotification(argument, type.AsType(), contract, false);
            else if (definition == typeof(ISyncNotificationHandler<>)) AddNotification(argument, type.AsType(), contract, true);
        }
        return this;
    }

    private IChannelMediatorBuilder AddRequest(Type requestType, Type handlerType, Type contractType, bool sync)
    {
        _services.AddTransient(handlerType);
        Registry.AddRequest(requestType, new HandlerDescriptor(handlerType, contractType, sync, false));
        return this;
    }
    private IChannelMediatorBuilder AddNotification(Type notificationType, Type handlerType, Type contractType, bool sync)
    {
        _services.AddTransient(handlerType);
        Registry.AddNotification(notificationType, new HandlerDescriptor(handlerType, contractType, sync, true));
        return this;
    }
}

internal sealed class HandlerDescriptor
{
    public HandlerDescriptor(Type handlerType, Type contractType, bool sync, bool notification)
    {
        HandlerType = handlerType; Sync = sync; Notification = notification;
        if (!contractType.IsInterface || !contractType.IsAssignableFrom(handlerType)) throw new InvalidOperationException($"'{handlerType}' does not implement '{contractType}'.");
        Method = contractType.GetMethod(sync ? "Handle" : "HandleAsync") ?? throw new InvalidOperationException($"'{contractType}' has no matching handler method.");
        IsVoidRequest = !notification && contractType.GetGenericArguments().Length == 1;
    }
    public Type HandlerType { get; }
    public bool Sync { get; }
    public bool Notification { get; }
    public bool IsVoidRequest { get; }
    public MethodInfo Method { get; }
}
internal sealed class HandlerRegistry
{
    private readonly Dictionary<Type, HandlerDescriptor> _requests = new Dictionary<Type, HandlerDescriptor>();
    private readonly Dictionary<Type, List<HandlerDescriptor>> _notifications = new Dictionary<Type, List<HandlerDescriptor>>();
    public void AddRequest(Type type, HandlerDescriptor handler) { if (_requests.ContainsKey(type)) throw new InvalidOperationException($"Only one request handler may be registered for '{type}'."); _requests.Add(type, handler); }
    public void AddNotification(Type type, HandlerDescriptor handler) { if (!_notifications.TryGetValue(type, out var handlers)) _notifications[type] = handlers = new List<HandlerDescriptor>(); handlers.Add(handler); }
    public HandlerDescriptor GetRequest(Type type) => _requests.TryGetValue(type, out var handler) ? handler : throw new InvalidOperationException($"No request handler is registered for '{type}'.");
    public IReadOnlyList<HandlerDescriptor> GetNotifications(Type type) => _notifications.TryGetValue(type, out var handlers) ? handlers : Array.Empty<HandlerDescriptor>();
}
