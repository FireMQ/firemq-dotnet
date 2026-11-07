namespace FireMQ.Protocol;

/// <summary>Op codes of the FireMQ protocol, version 1.</summary>
internal static class Op
{
    public const byte Hello = 0x01;
    public const byte Welcome = 0x02;
    public const byte Ping = 0x03;
    public const byte Pong = 0x04;
    public const byte Close = 0x05;
    public const byte Ok = 0x06;
    public const byte Error = 0x07;
    public const byte Flow = 0x08;
    public const byte DeclareExchange = 0x10;
    public const byte DeleteExchange = 0x11;
    public const byte DeclareQueue = 0x12;
    public const byte DeleteQueue = 0x13;
    public const byte PurgeQueue = 0x14;
    public const byte Bind = 0x15;
    public const byte Unbind = 0x16;
    public const byte Publish = 0x20;
    public const byte Confirm = 0x21;
    public const byte Return = 0x22;
    public const byte Subscribe = 0x30;
    public const byte Unsubscribe = 0x31;
    public const byte Credit = 0x32;
    public const byte Deliver = 0x33;
    public const byte Cancelled = 0x34;
    public const byte Ack = 0x35;
    public const byte Nack = 0x36;
    public const byte Get = 0x37;
    public const byte GetOk = 0x38;

    /// <summary>The op's name as written in the spec, for example <c>declare_queue</c>.</summary>
    public static string Name(byte op) => op switch
    {
        Hello => "hello",
        Welcome => "welcome",
        Ping => "ping",
        Pong => "pong",
        Close => "close",
        Ok => "ok",
        Error => "error",
        Flow => "flow",
        DeclareExchange => "declare_exchange",
        DeleteExchange => "delete_exchange",
        DeclareQueue => "declare_queue",
        DeleteQueue => "delete_queue",
        PurgeQueue => "purge_queue",
        Bind => "bind",
        Unbind => "unbind",
        Publish => "publish",
        Confirm => "confirm",
        Return => "return",
        Subscribe => "subscribe",
        Unsubscribe => "unsubscribe",
        Credit => "credit",
        Deliver => "deliver",
        Cancelled => "cancelled",
        Ack => "ack",
        Nack => "nack",
        Get => "get",
        GetOk => "get_ok",
        _ => "unknown",
    };
}

/// <summary>Protocol constants.</summary>
internal static class Proto
{
    /// <summary>Bytes a TCP client sends before its first frame.</summary>
    public static ReadOnlySpan<byte> Magic => "FMQ1"u8;

    public const uint Version = 1;

    /// <summary>Largest frame accepted before <c>welcome</c>.</summary>
    public const uint MaxHelloFrame = 65_536;

    /// <summary>Default largest frame after <c>welcome</c>.</summary>
    public const uint DefaultMaxFrame = 16 * 1024 * 1024;

    public const int DefaultPort = 5673;
    public const int DefaultTlsPort = 5674;
}
