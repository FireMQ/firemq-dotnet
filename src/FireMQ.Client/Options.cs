namespace FireMQ;

/// <summary>How to connect. The URL passed to <see cref="FireMQClient.ConnectAsync(string, ConnectOptions?, CancellationToken)"/> holds the address, credentials and vhost.</summary>
public sealed class ConnectOptions
{
    /// <summary>Heartbeat interval. The broker may lower it. <see cref="TimeSpan.Zero"/> disables heartbeats. Default 30 seconds.</summary>
    public TimeSpan Heartbeat { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Name shown for this connection in the management UI.</summary>
    public string? ConnectionName { get; set; }

    /// <summary>How long to wait for the TCP connection, the TLS handshake and the broker's welcome. Default 10 seconds.</summary>
    public TimeSpan ConnectTimeout { get; set; } = TimeSpan.FromSeconds(10);

    /// <summary>
    /// Extra trusted CA certificates for <c>fires://</c>, in PEM format, for example a
    /// self-signed certificate. The system trust store still applies.
    /// </summary>
    public string? TlsCaPem { get; set; }

    /// <summary>Server name to verify the certificate against. Defaults to the URL's host.</summary>
    public string? TlsServerName { get; set; }

    /// <summary>Skip certificate verification. Only for local development.</summary>
    public bool TlsInsecure { get; set; }
}

/// <summary>Options for <see cref="FireMQClient.DeclareQueueAsync"/>. Queues are durable by default.</summary>
public sealed class QueueOptions
{
    /// <summary>The queue survives a broker restart. Default true.</summary>
    public bool Durable { get; set; } = true;

    /// <summary>Only this connection may use the queue, and it is deleted when the connection closes.</summary>
    public bool Exclusive { get; set; }

    /// <summary>The queue is deleted when its last subscription is removed.</summary>
    public bool AutoDelete { get; set; }

    /// <summary>Only check that the queue exists; fail with 404 when it does not.</summary>
    public bool Passive { get; set; }

    /// <summary>Queue arguments with RabbitMQ's names, for example <c>x-message-ttl</c> or <c>x-dead-letter-exchange</c>.</summary>
    public IDictionary<string, object?> Arguments { get; set; } = new Dictionary<string, object?>();

    /// <summary>A queue that lives only as long as this connection: not durable, exclusive and auto-deleted.</summary>
    public static QueueOptions Temporary => new() { Durable = false, Exclusive = true, AutoDelete = true };

    /// <summary>Sets a queue argument and returns these options.</summary>
    public QueueOptions WithArgument(string key, object? value)
    {
        Arguments[key] = value;
        return this;
    }
}

/// <summary>Options for <see cref="FireMQClient.DeclareExchangeAsync"/>. Exchanges are durable by default.</summary>
public sealed class ExchangeOptions
{
    /// <summary>The exchange survives a broker restart. Default true.</summary>
    public bool Durable { get; set; } = true;

    /// <summary>The exchange is deleted when its last binding is removed.</summary>
    public bool AutoDelete { get; set; }

    /// <summary>The exchange cannot be published to directly, only through exchange bindings.</summary>
    public bool Internal { get; set; }

    /// <summary>Only check that the exchange exists; fail with 404 when it does not.</summary>
    public bool Passive { get; set; }

    /// <summary>Exchange arguments, for example <c>alternate-exchange</c>.</summary>
    public IDictionary<string, object?> Arguments { get; set; } = new Dictionary<string, object?>();

    /// <summary>Sets an exchange argument and returns these options.</summary>
    public ExchangeOptions WithArgument(string key, object? value)
    {
        Arguments[key] = value;
        return this;
    }
}

/// <summary>Options for <see cref="FireMQClient.PublishAsync(string, string, ReadOnlyMemory{byte}, PublishOptions?, CancellationToken)"/>. Messages are persistent and confirmed by default.</summary>
public sealed class PublishOptions
{
    /// <summary>Write the message to disk. It survives a restart when the queue is durable. Default true.</summary>
    public bool Persistent { get; set; } = true;

    /// <summary>Fail with <see cref="UnroutableException"/> when no queue receives the message.</summary>
    public bool Mandatory { get; set; }

    /// <summary>
    /// Wait for the broker's confirm. Default true. When false, the publish completes once
    /// the message is queued for sending, and returns and rejections are not reported.
    /// </summary>
    public bool Confirm { get; set; } = true;

    /// <summary>Message properties such as content type and correlation id.</summary>
    public MessageProperties Properties { get; set; } = new();

    /// <summary>Application headers. Values may be null, booleans, numbers, strings, byte arrays, lists or dictionaries.</summary>
    public IDictionary<string, object?> Headers { get; set; } = new Dictionary<string, object?>();
}

/// <summary>Options for <see cref="FireMQClient.SubscribeAsync"/>.</summary>
public sealed class SubscribeOptions
{
    /// <summary>
    /// How many unacknowledged messages the broker may send at once. Default 100. Values
    /// below 1 count as 1.
    /// </summary>
    public int Prefetch { get; set; } = 100;

    /// <summary>Treat messages as acknowledged once sent. Faster, but messages are lost if the consumer crashes.</summary>
    public bool NoAck { get; set; }

    /// <summary>Fail with 405 if the queue has other consumers, and block new ones.</summary>
    public bool Exclusive { get; set; }

    /// <summary>Consumer tag shown in the management UI. The broker picks one when empty.</summary>
    public string? Tag { get; set; }

    /// <summary>Consumer arguments.</summary>
    public IDictionary<string, object?> Arguments { get; set; } = new Dictionary<string, object?>();
}

/// <summary>What <see cref="FireMQClient.DeclareQueueAsync"/> reports.</summary>
public sealed class QueueInfo
{
    internal QueueInfo(string name, ulong messages, ulong consumers)
    {
        Name = name;
        Messages = messages;
        Consumers = consumers;
    }

    /// <summary>The queue's name. Useful when the broker generated it.</summary>
    public string Name { get; }

    /// <summary>Messages ready in the queue.</summary>
    public ulong Messages { get; }

    /// <summary>Active subscriptions on the queue.</summary>
    public ulong Consumers { get; }

    /// <inheritdoc />
    public override string ToString() => $"{Name} ({Messages} messages, {Consumers} consumers)";
}

/// <summary>Exchange types.</summary>
public static class ExchangeType
{
    /// <summary>Routes to queues whose binding key equals the routing key.</summary>
    public const string Direct = "direct";

    /// <summary>Routes to every bound queue.</summary>
    public const string Fanout = "fanout";

    /// <summary>Routes by pattern: <c>*</c> matches one word, <c>#</c> matches zero or more.</summary>
    public const string Topic = "topic";

    /// <summary>Routes by message headers, matched with the <c>x-match</c> binding argument.</summary>
    public const string Headers = "headers";
}

/// <summary>What the broker reported in its welcome.</summary>
public sealed class ServerInfo
{
    internal ServerInfo(string name, string version, string connectionId, TimeSpan heartbeat, uint maxFrame, IReadOnlyList<string> capabilities)
    {
        Name = name;
        Version = version;
        ConnectionId = connectionId;
        Heartbeat = heartbeat;
        MaxFrame = maxFrame;
        Capabilities = capabilities;
    }

    /// <summary>Server name, normally <c>firemq</c>.</summary>
    public string Name { get; }

    /// <summary>Server version.</summary>
    public string Version { get; }

    /// <summary>The connection's name in the management API.</summary>
    public string ConnectionId { get; }

    /// <summary>The negotiated heartbeat interval; zero when heartbeats are off.</summary>
    public TimeSpan Heartbeat { get; }

    /// <summary>Largest frame the broker accepts, in bytes. A message body must fit in one frame.</summary>
    public uint MaxFrame { get; }

    /// <summary>Capabilities the broker supports, for example <c>confirms</c> and <c>dead-letter</c>.</summary>
    public IReadOnlyList<string> Capabilities { get; }
}
