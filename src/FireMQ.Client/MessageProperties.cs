using FireMQ.Protocol;
using MessagePack;

namespace FireMQ;

/// <summary>
/// Message properties, mirroring AMQP's basic properties. Every property is optional;
/// <c>null</c> means unset.
/// </summary>
public sealed class MessageProperties
{
    /// <summary>MIME type of the body, for example <c>application/json</c>.</summary>
    public string? ContentType { get; set; }

    /// <summary>Encoding of the body, for example <c>gzip</c>.</summary>
    public string? ContentEncoding { get; set; }

    /// <summary>Ties a reply to its request.</summary>
    public string? CorrelationId { get; set; }

    /// <summary>Queue the receiver should reply to.</summary>
    public string? ReplyTo { get; set; }

    /// <summary>Per-message time to live, sent in whole milliseconds.</summary>
    public TimeSpan? Expiration { get; set; }

    /// <summary>Application message id.</summary>
    public string? MessageId { get; set; }

    /// <summary>Creation time, sent in whole seconds since the Unix epoch.</summary>
    public DateTimeOffset? Timestamp { get; set; }

    /// <summary>Application message type. Sent as <c>type</c>.</summary>
    public string? Type { get; set; }

    /// <summary>When set, the broker checks that it equals the connection's user.</summary>
    public string? UserId { get; set; }

    /// <summary>Application id.</summary>
    public string? AppId { get; set; }

    /// <summary>Priority, 0 to 255. Used by queues declared with <c>x-max-priority</c>.</summary>
    public byte? Priority { get; set; }

    /// <summary>True when no property is set.</summary>
    public bool IsEmpty => Count == 0;

    private int Count =>
        (ContentType is null ? 0 : 1) + (ContentEncoding is null ? 0 : 1) + (CorrelationId is null ? 0 : 1)
        + (ReplyTo is null ? 0 : 1) + (Expiration is null ? 0 : 1) + (MessageId is null ? 0 : 1)
        + (Timestamp is null ? 0 : 1) + (Type is null ? 0 : 1) + (UserId is null ? 0 : 1)
        + (AppId is null ? 0 : 1) + (Priority is null ? 0 : 1);

    internal void Write(ref MessagePackWriter w)
    {
        w.WriteMapHeader(Count);
        WriteStr(ref w, "content_type", ContentType);
        WriteStr(ref w, "content_encoding", ContentEncoding);
        WriteStr(ref w, "correlation_id", CorrelationId);
        WriteStr(ref w, "reply_to", ReplyTo);
        if (Expiration is { } exp)
        {
            if (exp < TimeSpan.Zero)
            {
                throw new ArgumentException("Expiration must not be negative");
            }
            w.Write("expiration");
            w.Write((ulong)(exp.Ticks / TimeSpan.TicksPerMillisecond));
        }
        WriteStr(ref w, "message_id", MessageId);
        if (Timestamp is { } ts)
        {
            long secs = ts.ToUnixTimeSeconds();
            if (secs < 0)
            {
                throw new ArgumentException("Timestamp must not be before the Unix epoch");
            }
            w.Write("timestamp");
            w.Write((ulong)secs);
        }
        WriteStr(ref w, "type", Type);
        WriteStr(ref w, "user_id", UserId);
        WriteStr(ref w, "app_id", AppId);
        if (Priority is { } p)
        {
            w.Write("priority");
            w.Write(p);
        }
    }

    private static void WriteStr(ref MessagePackWriter w, string key, string? value)
    {
        if (value is not null)
        {
            w.Write(key);
            w.Write(value);
        }
    }

    internal static MessageProperties Read(Dictionary<string, object?> h)
    {
        var p = new MessageProperties
        {
            ContentType = h.OptStr("content_type"),
            ContentEncoding = h.OptStr("content_encoding"),
            CorrelationId = h.OptStr("correlation_id"),
            ReplyTo = h.OptStr("reply_to"),
            MessageId = h.OptStr("message_id"),
            Type = h.OptStr("type"),
            UserId = h.OptStr("user_id"),
            AppId = h.OptStr("app_id"),
        };
        if (h.OptU64("expiration") is { } exp)
        {
            p.Expiration = TimeSpan.FromTicks((long)Math.Min(exp, (ulong)(TimeSpan.MaxValue.Ticks / TimeSpan.TicksPerMillisecond)) * TimeSpan.TicksPerMillisecond);
        }
        if (h.OptU64("timestamp") is { } ts && ts <= 253_402_300_799)
        {
            p.Timestamp = DateTimeOffset.FromUnixTimeSeconds((long)ts);
        }
        if (h.OptU64("priority") is { } pr)
        {
            p.Priority = (byte)Math.Min(pr, byte.MaxValue);
        }
        return p;
    }

    /// <summary>Returns a shallow copy.</summary>
    public MessageProperties Clone() => (MessageProperties)MemberwiseClone();
}
