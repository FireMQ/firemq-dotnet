using System.Text;
using System.Text.Json;
using FireMQ.Protocol;

namespace FireMQ;

/// <summary>A message received from a subscription or from <see cref="FireMQClient.GetAsync"/>.</summary>
public sealed class Message
{
    private readonly FireMQClient? _client;
    private readonly Subscription? _subscription;
    private int _settled;

    internal Message(
        FireMQClient? client,
        Subscription? subscription,
        ulong deliveryTag,
        bool redelivered,
        string exchange,
        string routingKey,
        MessageProperties? properties,
        IDictionary<string, object?>? headers,
        ReadOnlyMemory<byte> body)
    {
        _client = client;
        _subscription = subscription;
        DeliveryTag = deliveryTag;
        Redelivered = redelivered;
        Exchange = exchange;
        RoutingKey = routingKey;
        Properties = properties ?? new MessageProperties();
        Headers = headers ?? new Dictionary<string, object?>();
        Body = body;
    }

    /// <summary>The message body, as published.</summary>
    public ReadOnlyMemory<byte> Body { get; }

    /// <summary>The exchange the message was published to. Empty for the default exchange.</summary>
    public string Exchange { get; }

    /// <summary>The routing key the message was published with.</summary>
    public string RoutingKey { get; }

    /// <summary>Message properties.</summary>
    public MessageProperties Properties { get; }

    /// <summary>
    /// Application headers. Integers arrive as <see cref="long"/>, floats as
    /// <see cref="double"/>, lists as <c>object?[]</c> and maps as
    /// <c>Dictionary&lt;string, object?&gt;</c>.
    /// </summary>
    public IDictionary<string, object?> Headers { get; }

    /// <summary>True when the message was delivered before and not acknowledged.</summary>
    public bool Redelivered { get; }

    /// <summary>The delivery tag, unique and increasing on the connection.</summary>
    public ulong DeliveryTag { get; }

    /// <summary>True when the message needs no acknowledgement, or has been acknowledged or rejected.</summary>
    public bool IsSettled => _client is null || Volatile.Read(ref _settled) != 0;

    /// <summary>The body decoded as UTF-8 text.</summary>
    public string GetString() => Encoding.UTF8.GetString(Body.Span);

    /// <summary>Decodes a JSON body.</summary>
    public T? Deserialize<T>(JsonSerializerOptions? options = null) => JsonSerializer.Deserialize<T>(Body.Span, options);

    /// <summary>
    /// Acknowledges the message. Does nothing for messages received with no-ack, or when the
    /// message is already settled.
    /// </summary>
    /// <exception cref="ConnectionClosedException">The connection is closed; the broker requeues the message.</exception>
    public void Ack() => Settle(new AckFrame { Tag = DeliveryTag });

    /// <summary>
    /// Rejects the message. With <paramref name="requeue"/> it is delivered again; otherwise
    /// it is dead-lettered when the queue has a dead-letter exchange, and dropped if not.
    /// Does nothing for messages received with no-ack, or when the message is already settled.
    /// </summary>
    /// <exception cref="ConnectionClosedException">The connection is closed; the broker requeues the message.</exception>
    public void Nack(bool requeue) => Settle(new NackFrame { Tag = DeliveryTag, Requeue = requeue });

    private void Settle(Frame frame)
    {
        if (_client is null || Interlocked.Exchange(ref _settled, 1) != 0)
        {
            return;
        }
        _client.Send(frame);
        _subscription?.Release(1);
    }

    /// <inheritdoc />
    public override string ToString() =>
        $"Message(tag {DeliveryTag}, exchange '{Exchange}', key '{RoutingKey}', {Body.Length} bytes{(Redelivered ? ", redelivered" : "")})";
}
