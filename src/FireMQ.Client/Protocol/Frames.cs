using System.Buffers.Binary;
using MessagePack;

namespace FireMQ.Protocol;

/// <summary>A frame split into op, encoded header and payload, before header decoding.</summary>
internal readonly struct RawFrame
{
    public RawFrame(byte op, ReadOnlyMemory<byte> header, ReadOnlyMemory<byte> payload)
    {
        Op = op;
        Header = header;
        Payload = payload;
    }

    public byte Op { get; }
    public ReadOnlyMemory<byte> Header { get; }
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>
    /// Parses a frame without its length prefix: <c>op | hlen | header | payload</c>. This is
    /// also the WebSocket form of a frame.
    /// </summary>
    public static RawFrame Parse(ReadOnlyMemory<byte> frame)
    {
        if (frame.Length < 5)
        {
            throw new ProtocolException("malformed frame: shorter than 5 bytes");
        }
        var span = frame.Span;
        byte op = span[0];
        uint hlen = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(1, 4));
        if (hlen > (uint)(frame.Length - 5))
        {
            throw new ProtocolException("malformed frame: header length exceeds frame");
        }
        return new RawFrame(op, frame.Slice(5, (int)hlen), frame.Slice(5 + (int)hlen));
    }

    /// <summary>
    /// Splits one length-prefixed frame off the front of <paramref name="buffer"/>. Returns
    /// false until a whole frame is available.
    /// </summary>
    public static bool TryRead(ReadOnlyMemory<byte> buffer, uint maxFrame, out RawFrame frame, out int consumed)
    {
        frame = default;
        consumed = 0;
        if (buffer.Length < 4)
        {
            return false;
        }
        uint len = BinaryPrimitives.ReadUInt32BigEndian(buffer.Span);
        CheckLength(len, maxFrame);
        if ((ulong)buffer.Length < 4UL + len)
        {
            return false;
        }
        frame = Parse(buffer.Slice(4, (int)len));
        consumed = 4 + (int)len;
        return true;
    }

    /// <summary>Validates the value of a length prefix.</summary>
    public static void CheckLength(uint len, uint maxFrame)
    {
        if (len > maxFrame)
        {
            throw new ProtocolException($"frame of {len} bytes exceeds the maximum of {maxFrame} bytes");
        }
        if (len < 5)
        {
            throw new ProtocolException("malformed frame: shorter than 5 bytes");
        }
    }
}

/// <summary>A typed protocol frame.</summary>
internal abstract class Frame
{
    public abstract byte Op { get; }

    public string OpName => Protocol.Op.Name(Op);

    /// <summary>The raw payload, for frames that carry a message body.</summary>
    public virtual ReadOnlyMemory<byte> Payload => ReadOnlyMemory<byte>.Empty;

    /// <summary>Writes the MessagePack header map. Frames without fields write nothing.</summary>
    internal abstract void WriteHeader(ByteBuffer buffer);

    /// <summary>Encodes the frame with its length prefix, as sent on TCP and TLS.</summary>
    public byte[] Encode()
    {
        var header = new ByteBuffer(128);
        WriteHeader(header);
        var payload = Payload.Span;
        long total = 9L + header.Length + payload.Length;
        if (total > int.MaxValue)
        {
            throw new ArgumentException("frame too large");
        }
        var frame = new byte[total];
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(0, 4), (uint)(total - 4));
        frame[4] = Op;
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(5, 4), (uint)header.Length);
        header.WrittenSpan.CopyTo(frame.AsSpan(9));
        payload.CopyTo(frame.AsSpan(9 + header.Length));
        return frame;
    }

    /// <summary>
    /// Decodes a raw frame. Returns null for an op this version does not know. Throws
    /// <see cref="ProtocolException"/> when the header is not a MessagePack map.
    /// </summary>
    public static Frame? Decode(RawFrame raw)
    {
        Dictionary<string, object?> h;
        try
        {
            h = MsgPack.ReadHeader(raw.Header);
        }
        catch (Exception e) when (e is MessagePackSerializationException or EndOfStreamException or InvalidOperationException or OverflowException)
        {
            throw new ProtocolException($"invalid header for {Protocol.Op.Name(raw.Op)}: {e.Message}", e);
        }
        var p = raw.Payload;
        return raw.Op switch
        {
            Protocol.Op.Hello => HelloFrame.Read(h),
            Protocol.Op.Welcome => WelcomeFrame.Read(h),
            Protocol.Op.Ping => PingFrame.Instance,
            Protocol.Op.Pong => PongFrame.Instance,
            Protocol.Op.Close => CloseFrame.Read(h),
            Protocol.Op.Ok => OkFrame.Read(h),
            Protocol.Op.Error => ErrorFrame.Read(h),
            Protocol.Op.Flow => FlowFrame.Read(h),
            Protocol.Op.DeclareExchange => DeclareExchangeFrame.Read(h),
            Protocol.Op.DeleteExchange => DeleteExchangeFrame.Read(h),
            Protocol.Op.DeclareQueue => DeclareQueueFrame.Read(h),
            Protocol.Op.DeleteQueue => DeleteQueueFrame.Read(h),
            Protocol.Op.PurgeQueue => PurgeQueueFrame.Read(h),
            Protocol.Op.Bind => BindFrame.Read(h, unbind: false),
            Protocol.Op.Unbind => BindFrame.Read(h, unbind: true),
            Protocol.Op.Publish => PublishFrame.Read(h, p),
            Protocol.Op.Confirm => ConfirmFrame.Read(h),
            Protocol.Op.Return => ReturnFrame.Read(h, p),
            Protocol.Op.Subscribe => SubscribeFrame.Read(h),
            Protocol.Op.Unsubscribe => UnsubscribeFrame.Read(h),
            Protocol.Op.Credit => CreditFrame.Read(h),
            Protocol.Op.Deliver => DeliverFrame.Read(h, p),
            Protocol.Op.Cancelled => CancelledFrame.Read(h),
            Protocol.Op.Ack => AckFrame.Read(h),
            Protocol.Op.Nack => NackFrame.Read(h),
            Protocol.Op.Get => GetFrame.Read(h),
            Protocol.Op.GetOk => GetOkFrame.Read(h, p),
            _ => null,
        };
    }

    protected static Dictionary<string, object?> ArgsOf(Dictionary<string, object?> h) => h.Map("args");
}

internal sealed class AuthInfo
{
    public string Mech { get; set; } = "plain";
    public string User { get; set; } = "";
    public string Pass { get; set; } = "";
}

internal sealed class HelloFrame : Frame
{
    public uint Ver { get; set; }
    public string Vhost { get; set; } = "";
    public AuthInfo Auth { get; set; } = new();
    public uint Heartbeat { get; set; }
    public Dictionary<string, object?> Client { get; set; } = new();
    public List<string> Caps { get; set; } = new();

    public override byte Op => Protocol.Op.Hello;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("ver", Ver, always: true);
        m.Str("vhost", Vhost);
        m.Key("auth");
        int n = 1 + (Auth.User.Length > 0 ? 1 : 0) + (Auth.Pass.Length > 0 ? 1 : 0);
        m.W.WriteMapHeader(n);
        m.W.Write("mech");
        m.W.Write(Auth.Mech);
        if (Auth.User.Length > 0)
        {
            m.W.Write("user");
            m.W.Write(Auth.User);
        }
        if (Auth.Pass.Length > 0)
        {
            m.W.Write("pass");
            m.W.Write(Auth.Pass);
        }
        m.U64("heartbeat", Heartbeat, always: true);
        m.Map("client", Client);
        m.StrList("caps", Caps);
        m.End();
    }

    public static HelloFrame Read(Dictionary<string, object?> h)
    {
        var auth = h.Map("auth");
        return new HelloFrame
        {
            Ver = h.U32("ver"),
            Vhost = h.Str("vhost"),
            Auth = new AuthInfo { Mech = auth.Str("mech"), User = auth.Str("user"), Pass = auth.Str("pass") },
            Heartbeat = h.U32("heartbeat"),
            Client = h.Map("client"),
            Caps = h.StrList("caps"),
        };
    }
}

internal sealed class WelcomeFrame : Frame
{
    public uint Ver { get; set; }
    public string ServerName { get; set; } = "";
    public string ServerVersion { get; set; } = "";
    public string ConnId { get; set; } = "";
    public uint Heartbeat { get; set; }
    public uint MaxFrame { get; set; }
    public List<string> Caps { get; set; } = new();

    public override byte Op => Protocol.Op.Welcome;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("ver", Ver, always: true);
        m.Key("server");
        m.W.WriteMapHeader(2);
        m.W.Write("name");
        m.W.Write(ServerName);
        m.W.Write("version");
        m.W.Write(ServerVersion);
        m.Str("conn_id", ConnId, always: true);
        m.U64("heartbeat", Heartbeat, always: true);
        m.U64("max_frame", MaxFrame, always: true);
        m.StrList("caps", Caps);
        m.End();
    }

    public static WelcomeFrame Read(Dictionary<string, object?> h)
    {
        var server = h.Map("server");
        return new WelcomeFrame
        {
            Ver = h.U32("ver"),
            ServerName = server.Str("name"),
            ServerVersion = server.Str("version"),
            ConnId = h.Str("conn_id"),
            Heartbeat = h.U32("heartbeat"),
            MaxFrame = h.U32("max_frame"),
            Caps = h.StrList("caps"),
        };
    }
}

internal sealed class PingFrame : Frame
{
    public static readonly PingFrame Instance = new();
    public override byte Op => Protocol.Op.Ping;
    internal override void WriteHeader(ByteBuffer buffer) { }
}

internal sealed class PongFrame : Frame
{
    public static readonly PongFrame Instance = new();
    public override byte Op => Protocol.Op.Pong;
    internal override void WriteHeader(ByteBuffer buffer) { }
}

internal sealed class CloseFrame : Frame
{
    public ushort Code { get; set; }
    public string Msg { get; set; } = "";

    public override byte Op => Protocol.Op.Close;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("code", Code, always: true);
        m.Str("msg", Msg);
        m.End();
    }

    public static CloseFrame Read(Dictionary<string, object?> h) =>
        new() { Code = (ushort)Math.Min(h.U64("code"), ushort.MaxValue), Msg = h.Str("msg") };
}

internal sealed class OkFrame : Frame
{
    public uint Rid { get; set; }
    public string? QueueName { get; set; }
    public ulong? Messages { get; set; }
    public ulong? Consumers { get; set; }
    public bool Empty { get; set; }

    public override byte Op => Protocol.Op.Ok;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        if (QueueName is not null)
        {
            m.Str("name", QueueName, always: true);
        }
        m.OptU64("messages", Messages);
        m.OptU64("consumers", Consumers);
        m.Bool("empty", Empty);
        m.End();
    }

    public static OkFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        QueueName = h.OptStr("name"),
        Messages = h.OptU64("messages"),
        Consumers = h.OptU64("consumers"),
        Empty = h.Bool("empty"),
    };
}

internal sealed class ErrorFrame : Frame
{
    /// <summary>The failed request, or 0 for errors not tied to a request.</summary>
    public uint Rid { get; set; }
    public ushort Code { get; set; }
    public string Msg { get; set; } = "";
    public bool Fatal { get; set; }

    public override byte Op => Protocol.Op.Error;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid);
        m.U64("code", Code, always: true);
        m.Str("msg", Msg, always: true);
        m.Bool("fatal", Fatal);
        m.End();
    }

    public static ErrorFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        Code = (ushort)Math.Min(h.U64("code"), ushort.MaxValue),
        Msg = h.Str("msg"),
        Fatal = h.Bool("fatal"),
    };
}

internal sealed class FlowFrame : Frame
{
    public bool Pause { get; set; }

    public override byte Op => Protocol.Op.Flow;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.Bool("pause", Pause, always: true);
        m.End();
    }

    public static FlowFrame Read(Dictionary<string, object?> h) => new() { Pause = h.Bool("pause") };
}

internal sealed class DeclareExchangeFrame : Frame
{
    public uint Rid { get; set; }
    public string Name { get; set; } = "";
    public string Type { get; set; } = "";
    public bool Durable { get; set; }
    public bool AutoDelete { get; set; }
    public bool Internal { get; set; }
    public bool Passive { get; set; }
    public IDictionary<string, object?>? Args { get; set; }

    public override byte Op => Protocol.Op.DeclareExchange;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("name", Name, always: true);
        m.Str("type", Type, always: true);
        m.Bool("durable", Durable);
        m.Bool("auto_delete", AutoDelete);
        m.Bool("internal", Internal);
        m.Bool("passive", Passive);
        m.Map("args", Args);
        m.End();
    }

    public static DeclareExchangeFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        Name = h.Str("name"),
        Type = h.Str("type"),
        Durable = h.Bool("durable"),
        AutoDelete = h.Bool("auto_delete"),
        Internal = h.Bool("internal"),
        Passive = h.Bool("passive"),
        Args = ArgsOf(h),
    };
}

internal sealed class DeleteExchangeFrame : Frame
{
    public uint Rid { get; set; }
    public string Name { get; set; } = "";
    public bool IfUnused { get; set; }

    public override byte Op => Protocol.Op.DeleteExchange;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("name", Name, always: true);
        m.Bool("if_unused", IfUnused);
        m.End();
    }

    public static DeleteExchangeFrame Read(Dictionary<string, object?> h) =>
        new() { Rid = h.U32("rid"), Name = h.Str("name"), IfUnused = h.Bool("if_unused") };
}

internal sealed class DeclareQueueFrame : Frame
{
    public uint Rid { get; set; }
    public string Name { get; set; } = "";
    public bool Durable { get; set; }
    public bool Exclusive { get; set; }
    public bool AutoDelete { get; set; }
    public bool Passive { get; set; }
    public IDictionary<string, object?>? Args { get; set; }

    public override byte Op => Protocol.Op.DeclareQueue;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("name", Name);
        m.Bool("durable", Durable);
        m.Bool("exclusive", Exclusive);
        m.Bool("auto_delete", AutoDelete);
        m.Bool("passive", Passive);
        m.Map("args", Args);
        m.End();
    }

    public static DeclareQueueFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        Name = h.Str("name"),
        Durable = h.Bool("durable"),
        Exclusive = h.Bool("exclusive"),
        AutoDelete = h.Bool("auto_delete"),
        Passive = h.Bool("passive"),
        Args = ArgsOf(h),
    };
}

internal sealed class DeleteQueueFrame : Frame
{
    public uint Rid { get; set; }
    public string Name { get; set; } = "";
    public bool IfUnused { get; set; }
    public bool IfEmpty { get; set; }

    public override byte Op => Protocol.Op.DeleteQueue;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("name", Name, always: true);
        m.Bool("if_unused", IfUnused);
        m.Bool("if_empty", IfEmpty);
        m.End();
    }

    public static DeleteQueueFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        Name = h.Str("name"),
        IfUnused = h.Bool("if_unused"),
        IfEmpty = h.Bool("if_empty"),
    };
}

internal sealed class PurgeQueueFrame : Frame
{
    public uint Rid { get; set; }
    public string Name { get; set; } = "";

    public override byte Op => Protocol.Op.PurgeQueue;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("name", Name, always: true);
        m.End();
    }

    public static PurgeQueueFrame Read(Dictionary<string, object?> h) => new() { Rid = h.U32("rid"), Name = h.Str("name") };
}

/// <summary>Body of <c>bind</c> and <c>unbind</c>.</summary>
internal sealed class BindFrame : Frame
{
    public bool Unbind { get; set; }
    public uint Rid { get; set; }
    public string Exchange { get; set; } = "";
    public string Queue { get; set; } = "";
    public string ToExchange { get; set; } = "";
    public string Key { get; set; } = "";
    public IDictionary<string, object?>? Args { get; set; }

    public override byte Op => Unbind ? Protocol.Op.Unbind : Protocol.Op.Bind;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("exchange", Exchange, always: true);
        m.Str("queue", Queue);
        m.Str("to_exchange", ToExchange);
        m.Str("key", Key);
        m.Map("args", Args);
        m.End();
    }

    public static BindFrame Read(Dictionary<string, object?> h, bool unbind) => new()
    {
        Unbind = unbind,
        Rid = h.U32("rid"),
        Exchange = h.Str("exchange"),
        Queue = h.Str("queue"),
        ToExchange = h.Str("to_exchange"),
        Key = h.Str("key"),
        Args = ArgsOf(h),
    };
}

internal sealed class PublishFrame : Frame
{
    public ulong Seq { get; set; }
    public string Exchange { get; set; } = "";
    public string Key { get; set; } = "";
    public bool Mandatory { get; set; }
    public bool Persistent { get; set; }
    public MessageProperties? Props { get; set; }
    public IDictionary<string, object?>? Headers { get; set; }
    public ReadOnlyMemory<byte> Body { get; set; }

    public override byte Op => Protocol.Op.Publish;
    public override ReadOnlyMemory<byte> Payload => Body;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("seq", Seq);
        m.Str("exchange", Exchange);
        m.Str("key", Key);
        m.Bool("mandatory", Mandatory);
        m.Bool("persistent", Persistent);
        if (Props is { IsEmpty: false })
        {
            m.Key("props");
            Props.Write(ref m.W);
        }
        m.Map("headers", Headers);
        m.End();
    }

    public static PublishFrame Read(Dictionary<string, object?> h, ReadOnlyMemory<byte> body) => new()
    {
        Seq = h.U64("seq"),
        Exchange = h.Str("exchange"),
        Key = h.Str("key"),
        Mandatory = h.Bool("mandatory"),
        Persistent = h.Bool("persistent"),
        Props = MessageProperties.Read(h.Map("props")),
        Headers = h.Map("headers"),
        Body = body,
    };
}

/// <summary>Publisher confirm: <c>upto</c> for positive confirms, <c>seq</c> with <c>nack</c> for rejections.</summary>
internal sealed class ConfirmFrame : Frame
{
    public ulong Upto { get; set; }
    public ulong Seq { get; set; }
    public bool Nack { get; set; }
    public ushort Code { get; set; }
    public string Msg { get; set; } = "";

    public override byte Op => Protocol.Op.Confirm;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("upto", Upto);
        m.U64("seq", Seq);
        m.Bool("nack", Nack);
        m.U64("code", Code);
        m.Str("msg", Msg);
        m.End();
    }

    public static ConfirmFrame Read(Dictionary<string, object?> h) => new()
    {
        Upto = h.U64("upto"),
        Seq = h.U64("seq"),
        Nack = h.Bool("nack"),
        Code = (ushort)Math.Min(h.U64("code"), ushort.MaxValue),
        Msg = h.Str("msg"),
    };
}

internal sealed class ReturnFrame : Frame
{
    public ulong Seq { get; set; }
    public ushort Code { get; set; }
    public string Msg { get; set; } = "";
    public string Exchange { get; set; } = "";
    public string Key { get; set; } = "";
    public MessageProperties? Props { get; set; }
    public IDictionary<string, object?>? Headers { get; set; }
    public ReadOnlyMemory<byte> Body { get; set; }

    public override byte Op => Protocol.Op.Return;
    public override ReadOnlyMemory<byte> Payload => Body;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("seq", Seq, always: true);
        m.U64("code", Code, always: true);
        m.Str("msg", Msg, always: true);
        m.Str("exchange", Exchange);
        m.Str("key", Key);
        if (Props is { IsEmpty: false })
        {
            m.Key("props");
            Props.Write(ref m.W);
        }
        m.Map("headers", Headers);
        m.End();
    }

    public static ReturnFrame Read(Dictionary<string, object?> h, ReadOnlyMemory<byte> body) => new()
    {
        Seq = h.U64("seq"),
        Code = (ushort)Math.Min(h.U64("code"), ushort.MaxValue),
        Msg = h.Str("msg"),
        Exchange = h.Str("exchange"),
        Key = h.Str("key"),
        Props = MessageProperties.Read(h.Map("props")),
        Headers = h.Map("headers"),
        Body = body,
    };
}

internal sealed class SubscribeFrame : Frame
{
    public uint Rid { get; set; }
    public uint Sub { get; set; }
    public string Queue { get; set; } = "";
    public uint Credit { get; set; }
    public bool NoAck { get; set; }
    public bool Exclusive { get; set; }
    public string Tag { get; set; } = "";
    public IDictionary<string, object?>? Args { get; set; }

    public override byte Op => Protocol.Op.Subscribe;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.U64("sub", Sub, always: true);
        m.Str("queue", Queue, always: true);
        m.U64("credit", Credit, always: true);
        m.Bool("no_ack", NoAck);
        m.Bool("exclusive", Exclusive);
        m.Str("tag", Tag);
        m.Map("args", Args);
        m.End();
    }

    public static SubscribeFrame Read(Dictionary<string, object?> h) => new()
    {
        Rid = h.U32("rid"),
        Sub = h.U32("sub"),
        Queue = h.Str("queue"),
        Credit = h.U32("credit"),
        NoAck = h.Bool("no_ack"),
        Exclusive = h.Bool("exclusive"),
        Tag = h.Str("tag"),
        Args = ArgsOf(h),
    };
}

internal sealed class UnsubscribeFrame : Frame
{
    public uint Rid { get; set; }
    public uint Sub { get; set; }

    public override byte Op => Protocol.Op.Unsubscribe;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.U64("sub", Sub, always: true);
        m.End();
    }

    public static UnsubscribeFrame Read(Dictionary<string, object?> h) => new() { Rid = h.U32("rid"), Sub = h.U32("sub") };
}

internal sealed class CreditFrame : Frame
{
    public uint Sub { get; set; }
    public uint Credit { get; set; }

    public override byte Op => Protocol.Op.Credit;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("sub", Sub, always: true);
        m.U64("credit", Credit, always: true);
        m.End();
    }

    public static CreditFrame Read(Dictionary<string, object?> h) => new() { Sub = h.U32("sub"), Credit = h.U32("credit") };
}

internal sealed class DeliverFrame : Frame
{
    public uint Sub { get; set; }
    public ulong Tag { get; set; }
    public bool Redelivered { get; set; }
    public string Exchange { get; set; } = "";
    public string Key { get; set; } = "";
    public MessageProperties? Props { get; set; }
    public IDictionary<string, object?>? Headers { get; set; }
    public ReadOnlyMemory<byte> Body { get; set; }

    public override byte Op => Protocol.Op.Deliver;
    public override ReadOnlyMemory<byte> Payload => Body;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("sub", Sub, always: true);
        m.U64("tag", Tag, always: true);
        m.Bool("redelivered", Redelivered);
        m.Str("exchange", Exchange);
        m.Str("key", Key);
        if (Props is { IsEmpty: false })
        {
            m.Key("props");
            Props.Write(ref m.W);
        }
        m.Map("headers", Headers);
        m.End();
    }

    public static DeliverFrame Read(Dictionary<string, object?> h, ReadOnlyMemory<byte> body) => new()
    {
        Sub = h.U32("sub"),
        Tag = h.U64("tag"),
        Redelivered = h.Bool("redelivered"),
        Exchange = h.Str("exchange"),
        Key = h.Str("key"),
        Props = MessageProperties.Read(h.Map("props")),
        Headers = h.Map("headers"),
        Body = body,
    };
}

internal sealed class CancelledFrame : Frame
{
    public uint Sub { get; set; }
    public ushort Code { get; set; }
    public string Msg { get; set; } = "";

    public override byte Op => Protocol.Op.Cancelled;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("sub", Sub, always: true);
        m.U64("code", Code, always: true);
        m.Str("msg", Msg);
        m.End();
    }

    public static CancelledFrame Read(Dictionary<string, object?> h) => new()
    {
        Sub = h.U32("sub"),
        Code = (ushort)Math.Min(h.U64("code"), ushort.MaxValue),
        Msg = h.Str("msg"),
    };
}

internal sealed class AckFrame : Frame
{
    public ulong Tag { get; set; }
    public bool Multiple { get; set; }

    public override byte Op => Protocol.Op.Ack;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("tag", Tag, always: true);
        m.Bool("multiple", Multiple);
        m.End();
    }

    public static AckFrame Read(Dictionary<string, object?> h) => new() { Tag = h.U64("tag"), Multiple = h.Bool("multiple") };
}

internal sealed class NackFrame : Frame
{
    public ulong Tag { get; set; }
    public bool Multiple { get; set; }
    public bool Requeue { get; set; }

    public override byte Op => Protocol.Op.Nack;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("tag", Tag, always: true);
        m.Bool("multiple", Multiple);
        m.Bool("requeue", Requeue);
        m.End();
    }

    public static NackFrame Read(Dictionary<string, object?> h) =>
        new() { Tag = h.U64("tag"), Multiple = h.Bool("multiple"), Requeue = h.Bool("requeue") };
}

internal sealed class GetFrame : Frame
{
    public uint Rid { get; set; }
    public string Queue { get; set; } = "";
    public bool NoAck { get; set; }

    public override byte Op => Protocol.Op.Get;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.Str("queue", Queue, always: true);
        m.Bool("no_ack", NoAck);
        m.End();
    }

    public static GetFrame Read(Dictionary<string, object?> h) =>
        new() { Rid = h.U32("rid"), Queue = h.Str("queue"), NoAck = h.Bool("no_ack") };
}

internal sealed class GetOkFrame : Frame
{
    public uint Rid { get; set; }
    public ulong Tag { get; set; }
    public bool Redelivered { get; set; }
    public string Exchange { get; set; } = "";
    public string Key { get; set; } = "";
    public ulong Remaining { get; set; }
    public MessageProperties? Props { get; set; }
    public IDictionary<string, object?>? Headers { get; set; }
    public ReadOnlyMemory<byte> Body { get; set; }

    public override byte Op => Protocol.Op.GetOk;
    public override ReadOnlyMemory<byte> Payload => Body;

    internal override void WriteHeader(ByteBuffer buffer)
    {
        var m = MapWriter.Begin(buffer);
        m.U64("rid", Rid, always: true);
        m.U64("tag", Tag, always: true);
        m.Bool("redelivered", Redelivered);
        m.Str("exchange", Exchange);
        m.Str("key", Key);
        m.U64("remaining", Remaining, always: true);
        if (Props is { IsEmpty: false })
        {
            m.Key("props");
            Props.Write(ref m.W);
        }
        m.Map("headers", Headers);
        m.End();
    }

    public static GetOkFrame Read(Dictionary<string, object?> h, ReadOnlyMemory<byte> body) => new()
    {
        Rid = h.U32("rid"),
        Tag = h.U64("tag"),
        Redelivered = h.Bool("redelivered"),
        Exchange = h.Str("exchange"),
        Key = h.Str("key"),
        Remaining = h.U64("remaining"),
        Props = MessageProperties.Read(h.Map("props")),
        Headers = h.Map("headers"),
        Body = body,
    };
}
