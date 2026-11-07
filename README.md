# FireMQ .NET client

The official .NET client for [FireMQ](https://github.com/FireMQ/firemq), a RabbitMQ-compatible message broker. It speaks the native [FireMQ protocol](https://github.com/FireMQ/firemq/blob/main/spec/fire-protocol.md) over `fire://` (TCP) and `fires://` (TLS).

- Async-first API: every call returns a `Task`, subscriptions are `IAsyncEnumerable<Message>`.
- Safe defaults: queues and exchanges are durable, messages are persistent, and `PublishAsync` completes when the broker confirms the message.
- Targets .NET 8 and .NET Standard 2.1. One dependency: [MessagePack](https://www.nuget.org/packages/MessagePack).

## Install

```bash
dotnet add package FireMQ.Client
```

## Quickstart

Start a broker with `firemq --dev`, then:

```csharp
using FireMQ;

await using var client = await FireMQClient.ConnectAsync("fire://guest:guest@localhost:5673");
await client.DeclareQueueAsync("jobs");

// Completes when the broker has taken responsibility for the message.
await client.PublishAsync("", "jobs", "hello");

await using var jobs = await client.SubscribeAsync("jobs");
await foreach (var message in jobs)
{
    Console.WriteLine(message.GetString());
    message.Ack();
}
```

## Connecting

```csharp
var client = await FireMQClient.ConnectAsync(
    "fire://app:secret@mq.example.com:5673/production",
    new ConnectOptions { ConnectionName = "billing-worker", Heartbeat = TimeSpan.FromSeconds(15) });
```

The URL is `fire://user:password@host:port/vhost`. Use `fires://` for TLS. Everything after the scheme is optional:

| Part | Default |
|---|---|
| user and password | `guest` and `guest` |
| host | `localhost` |
| port | 5673 for `fire://`, 5674 for `fires://` |
| vhost | `/`. Percent-encode a vhost that contains `/`, for example `fire://host/prod%2Feu`. |

`ConnectOptions`:

| Option | Default | Meaning |
|---|---|---|
| `Heartbeat` | 30 s | Ping interval. The broker may lower it. `TimeSpan.Zero` turns heartbeats off. |
| `ConnectionName` | none | Shown in the management UI. |
| `ConnectTimeout` | 10 s | Limit for the TCP connection, the TLS handshake and the broker's welcome. |
| `TlsCaPem` | none | Extra trusted CA certificates, as PEM text or a path to a PEM file. |
| `TlsServerName` | URL host | Name to check the server certificate against. |
| `TlsInsecure` | `false` | Skip certificate verification. Only for local development. |

One `FireMQClient` is one connection. It is thread-safe; share it across the application. `client.Server` holds what the broker reported in its welcome: name, version, negotiated heartbeat and maximum frame size. `DisposeAsync` or `CloseAsync` closes the connection.

The client does not reconnect on its own. When the connection drops, pending calls fail with `ConnectionClosedException` and `client.Completion` completes. Create a new client to reconnect.

## Queues, exchanges and bindings

```csharp
// Durable by default. An empty name asks the broker to generate one.
QueueInfo info = await client.DeclareQueueAsync("orders", new QueueOptions
{
    Arguments = { ["x-message-ttl"] = 60_000, ["x-dead-letter-exchange"] = "dlx" },
});
Console.WriteLine($"{info.Name}: {info.Messages} messages, {info.Consumers} consumers");

// Not durable, exclusive to this connection, deleted when it closes.
var replies = await client.DeclareQueueAsync("", QueueOptions.Temporary);

await client.DeclareExchangeAsync("events", ExchangeType.Topic);
await client.BindQueueAsync("orders", "events", "order.*");
await client.BindExchangeAsync("audit", "events", "#"); // exchange to exchange
await client.UnbindQueueAsync("orders", "events", "order.*");

ulong purged = await client.PurgeQueueAsync("orders");
ulong deleted = await client.DeleteQueueAsync("orders", ifUnused: false, ifEmpty: false);
await client.DeleteExchangeAsync("events");
```

`QueueOptions` has `Durable` (default `true`), `Exclusive`, `AutoDelete`, `Passive` and `Arguments`. `ExchangeOptions` has `Durable` (default `true`), `AutoDelete`, `Internal`, `Passive` and `Arguments`. `ExchangeType` holds the four type names: `Direct`, `Fanout`, `Topic` and `Headers`.

## Publishing

```csharp
await client.PublishAsync("events", "order.created", body, new PublishOptions
{
    Mandatory = true,
    Properties = new MessageProperties
    {
        ContentType = "application/json",
        CorrelationId = requestId,
        ReplyTo = replies.Name,
        Expiration = TimeSpan.FromMinutes(5),
    },
    Headers = { ["tenant"] = "acme", ["attempt"] = 1 },
});

await client.PublishJsonAsync("", "orders", new Order(42, "book")); // sets content type application/json
```

`body` is a `ReadOnlyMemory<byte>` (a `byte[]` works) or a `string`, sent as UTF-8. Use exchange `""` to send straight to the queue named by the routing key.

`PublishAsync` completes when the broker confirms the message: it is on disk when the message is persistent and the queue durable, and in memory otherwise. Publishes from many tasks can run at once; the client matches the broker's cumulative confirms to each call.

| `PublishOptions` | Default | Meaning |
|---|---|---|
| `Persistent` | `true` | Write the message to disk. |
| `Mandatory` | `false` | Fail with `UnroutableException` when no queue receives the message. |
| `Confirm` | `true` | Wait for the broker's confirm. With `false`, the call completes once the message is queued for sending, and returns and rejections go unreported. |
| `Properties` | empty | `ContentType`, `ContentEncoding`, `CorrelationId`, `ReplyTo`, `Expiration`, `MessageId`, `Timestamp`, `Type`, `UserId`, `AppId`, `Priority`. |
| `Headers` | empty | Application headers. |

Header values can be `null`, `bool`, any integer type, `float`, `double`, `decimal` (sent as a double), `string`, `byte[]`, `DateTime` or `DateTimeOffset` (sent as Unix seconds), `Guid` (sent as a string), lists and dictionaries of these, and `JsonElement`.

A message body must fit in one frame. `client.Server.MaxFrame` is the limit, 16 MiB by default; a larger publish throws `ArgumentException` before anything is sent.

When the broker raises a memory or disk alarm, it asks clients to pause. `PublishAsync` then waits until the broker resumes.

## Consuming

```csharp
await using var subscription = await client.SubscribeAsync("orders", new SubscribeOptions { Prefetch = 50 });
await foreach (var message in subscription.WithCancellation(stoppingToken))
{
    try
    {
        var order = message.Deserialize<Order>();
        await HandleAsync(order);
        message.Ack();
    }
    catch (Exception)
    {
        message.Nack(requeue: false); // dead-letters when the queue has a dead-letter exchange
    }
}
```

| `SubscribeOptions` | Default | Meaning |
|---|---|---|
| `Prefetch` | 100 | Unacknowledged messages the broker may send ahead. |
| `NoAck` | `false` | Messages count as acknowledged once sent. Faster, but a crash loses them. |
| `Exclusive` | `false` | Fail with 405 if the queue has other consumers, and block new ones. |
| `Tag` | broker-chosen | Consumer tag shown in the management UI. |

Flow control uses credit. The client grants `Prefetch` at subscribe time and grants more in batches of half that window as you acknowledge or reject messages (with `NoAck`, as you read them). A message you have not settled holds its slot, so a slow consumer never gets flooded.

`subscription.NextAsync(token)` reads one message, for loops that need a timeout. It returns `null` once the subscription has ended.

The enumeration ends when you call `subscription.CancelAsync()`, when you close the client, or when the broker cancels the subscription, for example because the queue was deleted (`subscription.CancelCode` and `CancelReason` say why). It throws `ConnectionClosedException` when the connection drops. Messages already received when you cancel can still be read and acknowledged; unacknowledged messages go back to the queue when the connection closes.

To take a single message without subscribing:

```csharp
Message? message = await client.GetAsync("orders"); // null when the queue is empty
message?.Ack();
```

### Messages

| Member | Meaning |
|---|---|
| `Body` | The body as `ReadOnlyMemory<byte>`. |
| `GetString()` | The body decoded as UTF-8. |
| `Deserialize<T>()` | The body decoded as JSON with System.Text.Json. |
| `Exchange`, `RoutingKey` | Where the message was published. |
| `Properties` | `MessageProperties`; unset properties are `null`. |
| `Headers` | Integers arrive as `long`, floats as `double`, lists as `object?[]`, maps as `Dictionary<string, object?>`. |
| `Redelivered` | The message was delivered before and not acknowledged. |
| `DeliveryTag` | The broker's delivery tag. |
| `Ack()`, `Nack(requeue)` | Settle the message. Calling either again, or on a no-ack message, does nothing. |

## Errors

Every exception the client raises derives from `FireMQException`.

| Exception | When |
|---|---|
| `ServerException` | The broker refused a request. `Code` is the AMQP reply code: 403 access refused (also bad credentials at connect), 404 not found, 405 resource locked, 406 precondition failed, 530 not allowed. The connection stays usable. |
| `UnroutableException` | A mandatory publish reached no queue. `Exchange` and `RoutingKey` say which; `Code` is 312. |
| `NackedException` | The broker rejected a publish, for example because a queue with `x-overflow: reject-publish` is full. |
| `ConnectionClosedException` | The connection is closed: by you, by the broker (`Code` 320 when it shuts down), or by a network failure or missed heartbeats. |
| `ProtocolException` | The broker sent something the client cannot decode. |
| `FireMQException` | Connecting failed: the host is unreachable, the TLS handshake failed, or the connection timed out. |

```csharp
try
{
    await client.PublishAsync("", "orders", body, new PublishOptions { Mandatory = true });
}
catch (UnroutableException e)
{
    logger.LogWarning("no queue for {Key}", e.RoutingKey);
}
catch (ServerException e) when (e.Code == 404)
{
    // the exchange does not exist
}
```

When a publish fails with `ConnectionClosedException`, the broker may or may not have received the message. Publish it again on a new connection if duplicates are acceptable.

## TLS

Connect with `fires://`. The client checks the broker's certificate against the system trust store and the URL's host name.

```csharp
// A certificate from a public CA.
await FireMQClient.ConnectAsync("fires://app:secret@mq.example.com");

// A private or self-signed CA.
await FireMQClient.ConnectAsync("fires://app:secret@mq.internal:5674",
    new ConnectOptions { TlsCaPem = "/etc/firemq/ca.pem" });

// Local development only: accept any certificate.
await FireMQClient.ConnectAsync("fires://localhost:5674", new ConnectOptions { TlsInsecure = true });
```

The broker serves TLS when started with `--tls-cert` and `--tls-key`.

## Examples

[`examples/Examples`](examples/Examples) has the six RabbitMQ tutorials: `hello_world`, `work_queues`, `publish_subscribe`, `routing`, `topics` and `rpc`. Each runs its producer and consumers in one process. Queue and exchange names end in `.dotnet`.

```bash
FIREMQ_URL=fire://guest:guest@localhost:5673 dotnet run --project examples/Examples -- hello_world
dotnet run --project examples/Examples -- all
```

## Development

```bash
dotnet build
dotnet test                                   # unit tests; integration tests are skipped

firemq --ephemeral &                          # a broker for integration tests
FIREMQ_URL=fire://localhost:5673 dotnet test  # unit and integration tests
```

TLS tests also need `FIREMQ_TLS_URL` (for example `fires://localhost:5674`) and `FIREMQ_TLS_CA` (the broker certificate's PEM file).

Without a local .NET SDK, run the same commands in the official image:

```bash
docker run --rm -v "$PWD":/w -w /w mcr.microsoft.com/dotnet/sdk:8.0 dotnet test
```

The unit tests decode every frame in the protocol's [test vectors](tests/FireMQ.Client.Tests/TestVectors/frames.json) and drive the client against a scripted broker.

### Conformance

[`conformance/Driver`](conformance/Driver) is the driver for the FireMQ SDK conformance suite. Build it, then point `firemq-conformance` from the FireMQ repository at it:

```bash
dotnet build -c Release
firemq-conformance run --driver "dotnet conformance/Driver/bin/Release/net8.0/firemq-dotnet-conformance-driver.dll"
```

## License

Apache-2.0. See [LICENSE](LICENSE).
