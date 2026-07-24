using System.Threading;
using System.Threading.Tasks;

namespace MediatorChannels;

public interface IRequest<out TResponse> { }
public interface IRequest : IRequest<Unit> { }
public interface INotification { }

public interface IRequestHandler<in TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    Task<TResponse> HandleAsync(TRequest request, CancellationToken cancellationToken = default);
}

/// <summary>Handles a command that has no response body.</summary>
public interface IRequestHandler<in TRequest> where TRequest : IRequest
{
    Task HandleAsync(TRequest request, CancellationToken cancellationToken = default);
}

public interface ISyncRequestHandler<in TRequest, TResponse> where TRequest : IRequest<TResponse>
{
    TResponse Handle(TRequest request, CancellationToken cancellationToken = default);
}

/// <summary>Synchronously handles a command that has no response body.</summary>
public interface ISyncRequestHandler<in TRequest> where TRequest : IRequest
{
    void Handle(TRequest request, CancellationToken cancellationToken = default);
}
public interface INotificationHandler<in TNotification> where TNotification : INotification
{
    Task HandleAsync(TNotification notification, CancellationToken cancellationToken = default);
}

public interface ISyncNotificationHandler<in TNotification> where TNotification : INotification
{
    void Handle(TNotification notification, CancellationToken cancellationToken = default);
}

public readonly struct Unit
{
    public static readonly Unit Value = new Unit();
}

public interface IChannelMediator
{
    Task<TResponse> SendAsync<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default);
    TResponse Send<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default);
    Task SendAsync(IRequest request, CancellationToken cancellationToken = default);
    void Send(IRequest request, CancellationToken cancellationToken = default);

    /// <summary>Queues a notification and returns once it has been accepted. Subscribers run in the background.</summary>
    Task PublishAsync<TNotification>(TNotification notification, CancellationToken cancellationToken = default) where TNotification : INotification;
    void Publish<TNotification>(TNotification notification) where TNotification : INotification;
    /// <summary>Publishes to every subscriber and completes after all subscribers have completed.</summary>
    Task PublishAndWaitAsync<TNotification>(TNotification notification, CancellationToken cancellationToken = default) where TNotification : INotification;
}
