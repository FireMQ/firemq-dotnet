namespace FireMQ;

/// <summary>Base class for every error the client raises.</summary>
public class FireMQException : Exception
{
    /// <summary>Creates an exception with a message.</summary>
    public FireMQException(string message)
        : base(message)
    {
    }

    /// <summary>Creates an exception with a message and the error that caused it.</summary>
    public FireMQException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }
}

/// <summary>
/// The broker refused a request. <see cref="Code"/> is an AMQP reply code such as 404
/// (not found), 403 (access refused) or 406 (precondition failed).
/// </summary>
public class ServerException : FireMQException
{
    /// <summary>Creates an exception for a broker error.</summary>
    public ServerException(int code, string message)
        : base($"{message} (code {code})")
    {
        Code = code;
        ServerMessage = message;
    }

    /// <summary>The AMQP reply code.</summary>
    public int Code { get; }

    /// <summary>The reason as the broker sent it.</summary>
    public string ServerMessage { get; }
}

/// <summary>
/// A mandatory publish reached no queue. The broker returned the message with code 312
/// (no route).
/// </summary>
public class UnroutableException : FireMQException
{
    /// <summary>Creates an exception for a returned message.</summary>
    public UnroutableException(string exchange, string routingKey, int code = 312, string? serverMessage = null)
        : base($"message returned: no queue is bound to exchange '{exchange}' with key '{routingKey}'")
    {
        Exchange = exchange;
        RoutingKey = routingKey;
        Code = code;
        ServerMessage = serverMessage ?? "NO_ROUTE";
    }

    /// <summary>The exchange the message was published to.</summary>
    public string Exchange { get; }

    /// <summary>The routing key the message was published with.</summary>
    public string RoutingKey { get; }

    /// <summary>The AMQP reply code, 312 (no route).</summary>
    public int Code { get; }

    /// <summary>The reason as the broker sent it.</summary>
    public string ServerMessage { get; }
}

/// <summary>
/// The broker rejected a publish, for example because a queue declared with
/// <c>x-overflow: reject-publish</c> is full.
/// </summary>
public class NackedException : FireMQException
{
    /// <summary>Creates an exception for a rejected publish.</summary>
    public NackedException(int code, string message)
        : base($"publish rejected: {message} (code {code})")
    {
        Code = code;
        ServerMessage = message;
    }

    /// <summary>The AMQP reply code.</summary>
    public int Code { get; }

    /// <summary>The reason as the broker sent it.</summary>
    public string ServerMessage { get; }
}

/// <summary>
/// The connection is closed: the application closed it, the broker closed it, or the
/// network failed. Create a new client to reconnect.
/// </summary>
public class ConnectionClosedException : FireMQException
{
    /// <summary>Creates an exception for a closed connection.</summary>
    public ConnectionClosedException(string message, int? code = null, Exception? innerException = null)
        : base(message, innerException)
    {
        Code = code;
    }

    /// <summary>The close code the broker sent, when it sent one. 320 means the broker is shutting down.</summary>
    public int? Code { get; }
}

/// <summary>The broker sent something this client cannot decode.</summary>
public class ProtocolException : FireMQException
{
    /// <summary>Creates an exception for a protocol violation.</summary>
    public ProtocolException(string message, Exception? innerException = null)
        : base(message, innerException)
    {
    }
}
