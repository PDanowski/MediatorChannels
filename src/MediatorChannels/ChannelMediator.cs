using System;
using System.Reflection;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;

namespace MediatorChannels;

/// <summary>Thread-safe in-process mediator implemented with <see cref="Channel{T}"/>.</summary>
public sealed class ChannelMediator : IChannelMediator, IDisposable
{
    private readonly HandlerRegistry _registry;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly Channel<RequestMessage> _requests = Channel.CreateUnbounded<RequestMessage>(new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
    private readonly Channel<NotificationMessage> _notifications = Channel.CreateUnbounded<NotificationMessage>(new UnboundedChannelOptions { SingleReader = true, AllowSynchronousContinuations = false });
    private readonly Task _requestWorker;
    private readonly Task _notificationWorker;
    private int _disposed;

    public ChannelMediator(IServiceProvider services)
    {
        _registry = services.GetRequiredService<HandlerRegistry>();
        _scopeFactory = services.GetRequiredService<IServiceScopeFactory>();
        _requestWorker = Task.Run(ProcessRequestsAsync);
        _notificationWorker = Task.Run(ProcessNotificationsAsync);
    }

    public async Task<TResponse> SendAsync<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default)
    {
        if (request == null) throw new ArgumentNullException(nameof(request));
        ThrowIfDisposed();
        var completion = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _requests.Writer.WriteAsync(new RequestMessage(request, completion, cancellationToken), cancellationToken).ConfigureAwait(false);
        var result = await WaitWithCancellation(completion.Task, cancellationToken).ConfigureAwait(false);
        return (TResponse)result;
    }

    public TResponse Send<TResponse>(IRequest<TResponse> request, CancellationToken cancellationToken = default)
        => SendAsync(request, cancellationToken).GetAwaiter().GetResult();

    public Task SendAsync(IRequest request, CancellationToken cancellationToken = default)
        => SendAsync<Unit>(request, cancellationToken);
    public void Send(IRequest request, CancellationToken cancellationToken = default)
        => SendAsync(request, cancellationToken).GetAwaiter().GetResult();

    public async Task PublishAsync<TNotification>(TNotification notification, CancellationToken cancellationToken = default) where TNotification : INotification
    {
        if (notification == null) throw new ArgumentNullException(nameof(notification));
        ThrowIfDisposed();
        await _notifications.Writer.WriteAsync(new NotificationMessage(notification, null, cancellationToken), cancellationToken).ConfigureAwait(false);
    }

    public void Publish<TNotification>(TNotification notification) where TNotification : INotification
        => PublishAsync(notification).GetAwaiter().GetResult();

    public async Task PublishAndWaitAsync<TNotification>(TNotification notification, CancellationToken cancellationToken = default) where TNotification : INotification
    {
        if (notification == null) throw new ArgumentNullException(nameof(notification));
        ThrowIfDisposed();
        var completion = new TaskCompletionSource<object>(TaskCreationOptions.RunContinuationsAsynchronously);
        await _notifications.Writer.WriteAsync(new NotificationMessage(notification, completion, cancellationToken), cancellationToken).ConfigureAwait(false);
        await completion.Task.ConfigureAwait(false);
    }

    private async Task ProcessRequestsAsync()
    {
        try
        {
            while (await _requests.Reader.WaitToReadAsync().ConfigureAwait(false))
            while (_requests.Reader.TryRead(out var message))
            {
                try
                {
                    message.CancellationToken.ThrowIfCancellationRequested();
                    var descriptor = _registry.GetRequest(message.Payload.GetType());
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var result = await InvokeRequest(scope.ServiceProvider.GetRequiredService(descriptor.HandlerType), descriptor, message.Payload, message.CancellationToken).ConfigureAwait(false);
                        message.Completion.TrySetResult(result);
                    }
                }
                catch (Exception ex) { message.Completion.TrySetException(ex); }
            }
        }
        finally { }
    }

    private async Task ProcessNotificationsAsync()
    {
        try
        {
            while (await _notifications.Reader.WaitToReadAsync().ConfigureAwait(false))
            while (_notifications.Reader.TryRead(out var message))
            {
                try
                {
                    message.CancellationToken.ThrowIfCancellationRequested();
                    using (var scope = _scopeFactory.CreateScope())
                    {
                        var handlers = _registry.GetNotifications(message.Payload.GetType());
                        var tasks = new Task[handlers.Count];
                        for (var i = 0; i < handlers.Count; i++) tasks[i] = InvokeNotification(scope.ServiceProvider.GetRequiredService(handlers[i].HandlerType), handlers[i], message.Payload, message.CancellationToken);
                        await Task.WhenAll(tasks).ConfigureAwait(false);
                    }
                    message.Completion?.TrySetResult(Unit.Value);
                }
                catch (Exception ex)
                {
                    if (message.Completion != null) message.Completion.TrySetException(ex);
                    // Fire-and-forget publishing intentionally does not surface subscriber exceptions.
                }
            }
        }
        finally { }
    }

    private static async Task<object> InvokeRequest(object handler, HandlerDescriptor descriptor, object request, CancellationToken token)
    {
        var value = descriptor.Method.Invoke(handler, new[] { request, (object)token });
        if (descriptor.Sync) return descriptor.IsVoidRequest ? Unit.Value : value!;
        var task = (Task)value!;
        await task.ConfigureAwait(false);
        if (descriptor.IsVoidRequest) return Unit.Value;
        return task.GetType().GetProperty("Result")!.GetValue(task)!;
    }

    private static async Task InvokeNotification(object handler, HandlerDescriptor descriptor, object notification, CancellationToken token)
    {
        var value = descriptor.Method.Invoke(handler, new[] { notification, (object)token });
        if (!descriptor.Sync) await ((Task)value!).ConfigureAwait(false);
    }

    private static async Task<T> WaitWithCancellation<T>(Task<T> task, CancellationToken cancellationToken)
    {
        if (!cancellationToken.CanBeCanceled) return await task.ConfigureAwait(false);
        var cancellation = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using (cancellationToken.Register(() => cancellation.TrySetResult(true)))
        {
            if (await Task.WhenAny(task, cancellation.Task).ConfigureAwait(false) != task)
                cancellationToken.ThrowIfCancellationRequested();
        }
        return await task.ConfigureAwait(false);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        _requests.Writer.TryComplete();
        _notifications.Writer.TryComplete();
    }
    private void ThrowIfDisposed() { if (Volatile.Read(ref _disposed) != 0) throw new ObjectDisposedException(nameof(ChannelMediator)); }
    private sealed class RequestMessage { public RequestMessage(object payload, TaskCompletionSource<object> completion, CancellationToken cancellationToken) { Payload = payload; Completion = completion; CancellationToken = cancellationToken; } public object Payload { get; } public TaskCompletionSource<object> Completion { get; } public CancellationToken CancellationToken { get; } }
    private sealed class NotificationMessage { public NotificationMessage(object payload, TaskCompletionSource<object>? completion, CancellationToken cancellationToken) { Payload = payload; Completion = completion; CancellationToken = cancellationToken; } public object Payload { get; } public TaskCompletionSource<object>? Completion { get; } public CancellationToken CancellationToken { get; } }
}
