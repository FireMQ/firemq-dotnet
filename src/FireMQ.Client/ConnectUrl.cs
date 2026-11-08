using System.Globalization;
using FireMQ.Protocol;

namespace FireMQ;

/// <summary>How a client reaches the broker.</summary>
internal enum Transport
{
    Tcp,
    Tls,
    Ws,
    Wss,
}

/// <summary>
/// A parsed <c>fire://</c>, <c>fires://</c>, <c>ws://</c> or <c>wss://</c> connection URL,
/// with the spec's defaults filled in: user and password <c>guest</c>, vhost <c>/</c>, and
/// ports 5673 (<c>fire</c>), 5674 (<c>fires</c>), 15672 (<c>ws</c>) and 15671 (<c>wss</c>).
/// </summary>
internal sealed class ConnectUrl
{
    public Transport Transport { get; private set; }
    public string Host { get; private set; } = "localhost";
    public int Port { get; private set; }
    public string Vhost { get; private set; } = "/";
    public string User { get; private set; } = "guest";
    public string Password { get; private set; } = "guest";

    /// <summary>WebSocket endpoint path, <c>/fire</c> by default. Empty for fire and fires.</summary>
    public string Path { get; private set; } = "";

    public static ConnectUrl Parse(string url)
    {
        if (string.IsNullOrWhiteSpace(url))
        {
            throw new ArgumentException("connection URL is empty", nameof(url));
        }
        if (!Uri.TryCreate(url.Trim(), UriKind.Absolute, out var uri))
        {
            throw new ArgumentException($"invalid connection URL '{Redact(url)}'", nameof(url));
        }
        var result = new ConnectUrl();
        int defaultPort;
        switch (uri.Scheme.ToLowerInvariant())
        {
            case "fire":
                result.Transport = Transport.Tcp;
                defaultPort = Proto.DefaultPort;
                break;
            case "fires":
                result.Transport = Transport.Tls;
                defaultPort = Proto.DefaultTlsPort;
                break;
            case "ws":
                result.Transport = Transport.Ws;
                defaultPort = 15672;
                break;
            case "wss":
                result.Transport = Transport.Wss;
                defaultPort = 15671;
                break;
            default:
                throw new ArgumentException($"unsupported scheme '{uri.Scheme}'; use fire, fires, ws or wss", nameof(url));
        }

        string host = uri.Host;
        if (host.StartsWith("[", StringComparison.Ordinal) && host.EndsWith("]", StringComparison.Ordinal))
        {
            host = host.Substring(1, host.Length - 2);
        }
        result.Host = host.Length == 0 ? "localhost" : host;
        result.Port = uri.IsDefaultPort || uri.Port <= 0 ? defaultPort : uri.Port;

        string userInfo = uri.UserInfo;
        if (userInfo.Length > 0)
        {
            int colon = userInfo.IndexOf(':');
            string user = colon < 0 ? userInfo : userInfo.Substring(0, colon);
            if (user.Length > 0)
            {
                result.User = Uri.UnescapeDataString(user);
            }
            if (colon >= 0)
            {
                result.Password = Uri.UnescapeDataString(userInfo.Substring(colon + 1));
            }
        }

        string? queryVhost = QueryParam(uri.Query, "vhost");
        string path = uri.AbsolutePath;
        if (result.Transport is Transport.Tcp or Transport.Tls)
        {
            string raw = path.TrimStart('/');
            result.Vhost = raw.Length == 0 ? queryVhost ?? "/" : Uri.UnescapeDataString(raw);
        }
        else
        {
            result.Path = path is "" or "/" ? "/fire" : path;
            result.Vhost = queryVhost ?? "/";
        }
        return result;
    }

    /// <summary><c>host:port</c>, with IPv6 hosts bracketed.</summary>
    public string Authority => Host.Contains(':')
        ? $"[{Host}]:{Port.ToString(CultureInfo.InvariantCulture)}"
        : $"{Host}:{Port.ToString(CultureInfo.InvariantCulture)}";

    public override string ToString()
    {
        string scheme = Transport switch
        {
            Transport.Tcp => "fire",
            Transport.Tls => "fires",
            Transport.Ws => "ws",
            _ => "wss",
        };
        return $"{scheme}://{User}@{Authority}/{Uri.EscapeDataString(Vhost)}";
    }

    private static string? QueryParam(string query, string name)
    {
        if (string.IsNullOrEmpty(query))
        {
            return null;
        }
        foreach (string pair in query.TrimStart('?').Split('&'))
        {
            int eq = pair.IndexOf('=');
            string key = eq < 0 ? pair : pair.Substring(0, eq);
            if (Uri.UnescapeDataString(key.Replace('+', ' ')) == name)
            {
                return eq < 0 ? "" : Uri.UnescapeDataString(pair.Substring(eq + 1).Replace('+', ' '));
            }
        }
        return null;
    }

    private static string Redact(string url)
    {
        int at = url.IndexOf('@');
        int scheme = url.IndexOf("://", StringComparison.Ordinal);
        if (at < 0 || scheme < 0 || at < scheme)
        {
            return url;
        }
        return url.Substring(0, scheme + 3) + "***" + url.Substring(at);
    }
}
