# MediatorChannels

`MediatorChannels` is a small, thread-safe in-process alternative to MediatR. It uses `System.Threading.Channels`, supports .NET 5 and newer (the library targets `netstandard2.0`), and has no MediatR dependency.

## Capabilities

- Request queue: one registered handler consumes each request and returns a result.
- Notification topic: every registered handler receives each notification.
- Async and synchronous handlers and calling APIs.
- Explicit, type-safe registration or assembly scanning.
- DI and standalone bootstrap methods.

## Install and bootstrap

Add the package reference, then register handlers with the Microsoft DI container:

```csharp
services.AddChannelMediator(mediator => mediator
    .RegisterRequestHandler<GetName, string, GetNameHandler>()
    .RegisterNotificationHandler<UserCreated, AuditHandler>()
    .Scan(typeof(Program).Assembly));
```

For a small application without an existing DI container:

```csharp
using var provider = ChannelMediatorBootstrapper.Create(mediator =>
    mediator.Scan(typeof(Program).Assembly));
var bus = provider.GetRequiredService<IChannelMediator>();
```

## Define and use messages

```csharp
public sealed class GetName : IRequest<string> { public int Id { get; init; } }
public sealed class GetNameHandler : IRequestHandler<GetName, string>
{
    public Task<string> HandleAsync(GetName request, CancellationToken ct) => Task.FromResult("Ada");
}

public sealed class UserCreated : INotification { public int Id { get; init; } }
public sealed class AuditHandler : INotificationHandler<UserCreated>
{
    public Task HandleAsync(UserCreated notification, CancellationToken ct) => Task.CompletedTask;
}

public sealed class DeleteUser : IRequest { public int Id { get; init; } }
public sealed class DeleteUserHandler : IRequestHandler<DeleteUser>
{
    public Task HandleAsync(DeleteUser request, CancellationToken ct) => Task.CompletedTask;
}

var name = await bus.SendAsync(new GetName { Id = 1 }); // awaits the one queue consumer
await bus.SendAsync(new DeleteUser { Id = 1 });         // command: no response body
await bus.PublishAsync(new UserCreated { Id = 1 });     // queues work; subscribers run without waiting
await bus.PublishAndWaitAsync(new UserCreated { Id = 1 }); // waits for every topic subscriber
```

Use `ISyncRequestHandler<TRequest, TResponse>` and `ISyncNotificationHandler<TNotification>` when a handler is synchronous. `Send` and `Publish` are synchronous counterparts of the async APIs.
