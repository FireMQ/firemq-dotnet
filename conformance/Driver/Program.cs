// Conformance driver for the FireMQ .NET SDK.
//
// Reads one JSON command per line on stdin and writes exactly one JSON reply per line on
// stdout, as described in spec/conformance.md of the FireMQ repository. Logs go to stderr.
//
//   firemq-conformance run --driver "dotnet conformance/Driver/bin/Release/net8.0/firemq-dotnet-conformance-driver.dll"

using System.Text;
using System.Text.Json;
using FireMQ;

var driver = new Driver(Console.OpenStandardOutput());
using var stdin = new StreamReader(Console.OpenStandardInput(), new UTF8Encoding(false));
while (await stdin.ReadLineAsync() is { } line)
{
    if (line.Trim().Length == 0)
    {
        continue;
    }
    await driver.HandleAsync(line);
}
await driver.ShutdownAsync();

internal sealed class Driver
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly Stream _stdout;
    private readonly Dictionary<string, Subscription> _subscriptions = new();
    private readonly Dictionary<string, Message> _unsettled = new();
    private FireMQClient? _client;
    private int _nextSub;

    public Driver(Stream stdout) => _stdout = stdout;

    public async Task HandleAsync(string line)
    {
        JsonElement id = default;
        var output = new MemoryStream();
        using (var w = new Utf8JsonWriter(output))
        {
            try
            {
                using var doc = JsonDocument.Parse(line);
                var cmd = doc.RootElement;
                id = cmd.GetProperty("id").Clone();
                string op = cmd.GetProperty("op").GetString() ?? "";
                var result = await RunAsync(op, cmd);
                w.WriteStartObject();
                w.WritePropertyName("id");
                id.WriteTo(w);
                w.WriteBoolean("ok", true);
                w.WritePropertyName("result");
                WriteValue(w, result);
                w.WriteEndObject();
            }
            catch (Exception e)
            {
                Console.Error.WriteLine($"driver: {e.GetType().Name}: {e.Message}");
                w.Reset(output);
                output.SetLength(0);
                w.WriteStartObject();
                w.WritePropertyName("id");
                if (id.ValueKind == JsonValueKind.Undefined)
                {
                    w.WriteNullValue();
                }
                else
                {
                    id.WriteTo(w);
                }
                w.WriteBoolean("ok", false);
                w.WriteStartObject("error");
                var (kind, code) = Classify(e);
                w.WriteString("kind", kind);
                if (code is { } c)
                {
                    w.WriteNumber("code", c);
                }
                w.WriteString("message", e.Message);
                w.WriteEndObject();
                w.WriteEndObject();
            }
        }
        output.WriteByte((byte)'\n');
        output.Position = 0;
        await output.CopyToAsync(_stdout);
        await _stdout.FlushAsync();
    }

    public async Task ShutdownAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
        }
    }

    private static (string Kind, int? Code) Classify(Exception e) => e switch
    {
        ServerException s => ("server", s.Code),
        UnroutableException u => ("unroutable", u.Code),
        NackedException n => ("nacked", n.Code),
        ConnectionClosedException c => ("closed", c.Code),
        NotSupportedException or NotImplementedException => ("unsupported", null),
        _ => ("other", null),
    };

    private async Task<object?> RunAsync(string op, JsonElement cmd)
    {
        switch (op)
        {
            case "hello":
                return new Dictionary<string, object?>
                {
                    ["sdk"] = "firemq-dotnet",
                    ["version"] = FireMQClient.Version,
                    ["capabilities"] = Array.Empty<object?>(),
                };

            case "connect":
                await DropClientAsync();
                _client = await FireMQClient.ConnectAsync(Str(cmd, "url"));
                return Empty();

            case "close":
                await DropClientAsync();
                return Empty();

            case "declare_queue":
            {
                var info = await Client.DeclareQueueAsync(Str(cmd, "name"), new QueueOptions
                {
                    Durable = Bool(cmd, "durable"),
                    Exclusive = Bool(cmd, "exclusive"),
                    AutoDelete = Bool(cmd, "auto_delete"),
                    Arguments = Map(cmd, "args"),
                });
                return new Dictionary<string, object?>
                {
                    ["name"] = info.Name,
                    ["messages"] = info.Messages,
                    ["consumers"] = info.Consumers,
                };
            }

            case "delete_queue":
                return new Dictionary<string, object?> { ["messages"] = await Client.DeleteQueueAsync(Str(cmd, "name")) };

            case "purge_queue":
                return new Dictionary<string, object?> { ["messages"] = await Client.PurgeQueueAsync(Str(cmd, "name")) };

            case "declare_exchange":
                await Client.DeclareExchangeAsync(Str(cmd, "name"), Str(cmd, "type"), new ExchangeOptions { Durable = Bool(cmd, "durable") });
                return Empty();

            case "delete_exchange":
                await Client.DeleteExchangeAsync(Str(cmd, "name"));
                return Empty();

            case "bind_queue":
                await Client.BindQueueAsync(Str(cmd, "queue"), Str(cmd, "exchange"), Str(cmd, "key"), Map(cmd, "args"));
                return Empty();

            case "unbind_queue":
                await Client.UnbindQueueAsync(Str(cmd, "queue"), Str(cmd, "exchange"), Str(cmd, "key"));
                return Empty();

            case "bind_exchange":
                await Client.BindExchangeAsync(Str(cmd, "destination"), Str(cmd, "source"), Str(cmd, "key"));
                return Empty();

            case "publish":
            {
                byte[] body = cmd.TryGetProperty("body_base64", out var b64) && b64.ValueKind == JsonValueKind.String
                    ? Convert.FromBase64String(b64.GetString()!)
                    : Encoding.UTF8.GetBytes(Str(cmd, "body"));
                await Client.PublishAsync(Str(cmd, "exchange"), Str(cmd, "key"), body, new PublishOptions
                {
                    Persistent = Bool(cmd, "persistent"),
                    Mandatory = Bool(cmd, "mandatory"),
                    Properties = Properties(cmd),
                    Headers = Map(cmd, "headers"),
                });
                return Empty();
            }

            case "subscribe":
            {
                int prefetch = cmd.TryGetProperty("prefetch", out var p) && p.TryGetInt32(out int n) ? n : 100;
                var subscription = await Client.SubscribeAsync(Str(cmd, "queue"), new SubscribeOptions
                {
                    Prefetch = prefetch,
                    NoAck = Bool(cmd, "no_ack"),
                });
                string id = $"sub-{++_nextSub}";
                _subscriptions[id] = subscription;
                return new Dictionary<string, object?> { ["sub"] = id };
            }

            case "next":
            {
                string id = Str(cmd, "sub");
                if (!_subscriptions.TryGetValue(id, out var subscription))
                {
                    throw new InvalidOperationException($"unknown subscription '{id}'");
                }
                int timeoutMs = cmd.TryGetProperty("timeout_ms", out var t) && t.TryGetInt32(out int ms) ? ms : 3000;
                using var timeout = new CancellationTokenSource(timeoutMs);
                Message? message;
                try
                {
                    message = await subscription.NextAsync(timeout.Token);
                }
                catch (OperationCanceledException) when (timeout.IsCancellationRequested)
                {
                    message = null;
                }
                return new Dictionary<string, object?> { ["message"] = Track(message) };
            }

            case "ack":
                Unsettled(cmd).Ack();
                return Empty();

            case "nack":
                Unsettled(cmd).Nack(Bool(cmd, "requeue"));
                return Empty();

            case "unsubscribe":
            {
                string id = Str(cmd, "sub");
                if (_subscriptions.TryGetValue(id, out var subscription))
                {
                    _subscriptions.Remove(id);
                    await subscription.CancelAsync();
                }
                return Empty();
            }

            case "get":
            {
                var message = await Client.GetAsync(Str(cmd, "queue"), Bool(cmd, "no_ack"));
                return new Dictionary<string, object?> { ["message"] = Track(message) };
            }

            default:
                throw new NotSupportedException($"unsupported op '{op}'");
        }
    }

    private FireMQClient Client => _client ?? throw new ConnectionClosedException("not connected");

    private async Task DropClientAsync()
    {
        _subscriptions.Clear();
        _unsettled.Clear();
        if (_client is { } client)
        {
            _client = null;
            await client.CloseAsync();
        }
    }

    private Message Unsettled(JsonElement cmd)
    {
        string tag = Str(cmd, "tag");
        if (!_unsettled.TryGetValue(tag, out var message))
        {
            throw new InvalidOperationException($"unknown delivery tag '{tag}'");
        }
        _unsettled.Remove(tag);
        return message;
    }

    private Dictionary<string, object?>? Track(Message? message)
    {
        if (message is null)
        {
            return null;
        }
        string tag = message.DeliveryTag.ToString();
        if (!message.IsSettled)
        {
            _unsettled[tag] = message;
        }
        string? text;
        try
        {
            text = StrictUtf8.GetString(message.Body.Span);
        }
        catch (DecoderFallbackException)
        {
            text = null;
        }
        return new Dictionary<string, object?>
        {
            ["body"] = text,
            ["body_base64"] = Convert.ToBase64String(message.Body.Span),
            ["exchange"] = message.Exchange,
            ["routing_key"] = message.RoutingKey,
            ["redelivered"] = message.Redelivered,
            ["tag"] = tag,
            ["properties"] = PropertiesToJson(message.Properties),
            ["headers"] = message.Headers,
        };
    }

    private static Dictionary<string, object?> PropertiesToJson(MessageProperties p)
    {
        var d = new Dictionary<string, object?>();
        if (p.ContentType is not null) d["content_type"] = p.ContentType;
        if (p.ContentEncoding is not null) d["content_encoding"] = p.ContentEncoding;
        if (p.CorrelationId is not null) d["correlation_id"] = p.CorrelationId;
        if (p.ReplyTo is not null) d["reply_to"] = p.ReplyTo;
        if (p.Expiration is { } exp) d["expiration"] = (ulong)(exp.Ticks / TimeSpan.TicksPerMillisecond);
        if (p.MessageId is not null) d["message_id"] = p.MessageId;
        if (p.Timestamp is { } ts) d["timestamp"] = ts.ToUnixTimeSeconds();
        if (p.Type is not null) d["type"] = p.Type;
        if (p.UserId is not null) d["user_id"] = p.UserId;
        if (p.AppId is not null) d["app_id"] = p.AppId;
        if (p.Priority is { } pr) d["priority"] = (int)pr;
        return d;
    }

    private static MessageProperties Properties(JsonElement cmd)
    {
        var p = new MessageProperties();
        if (!cmd.TryGetProperty("properties", out var props) || props.ValueKind != JsonValueKind.Object)
        {
            return p;
        }
        foreach (var prop in props.EnumerateObject())
        {
            var v = prop.Value;
            if (v.ValueKind == JsonValueKind.Null)
            {
                continue;
            }
            switch (prop.Name)
            {
                case "content_type": p.ContentType = v.GetString(); break;
                case "content_encoding": p.ContentEncoding = v.GetString(); break;
                case "correlation_id": p.CorrelationId = v.GetString(); break;
                case "reply_to": p.ReplyTo = v.GetString(); break;
                case "expiration": p.Expiration = TimeSpan.FromTicks(checked((long)v.GetUInt64() * TimeSpan.TicksPerMillisecond)); break;
                case "message_id": p.MessageId = v.GetString(); break;
                case "timestamp": p.Timestamp = DateTimeOffset.FromUnixTimeSeconds(v.GetInt64()); break;
                case "type": p.Type = v.GetString(); break;
                case "user_id": p.UserId = v.GetString(); break;
                case "app_id": p.AppId = v.GetString(); break;
                case "priority": p.Priority = v.GetByte(); break;
            }
        }
        return p;
    }

    private static Dictionary<string, object?> Empty() => new();

    private static string Str(JsonElement cmd, string name) =>
        cmd.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString()! : "";

    private static bool Bool(JsonElement cmd, string name) =>
        cmd.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.True;

    private static IDictionary<string, object?> Map(JsonElement cmd, string name)
    {
        var map = new Dictionary<string, object?>();
        if (cmd.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.Object)
        {
            foreach (var p in v.EnumerateObject())
            {
                map[p.Name] = FromJson(p.Value);
            }
        }
        return map;
    }

    /// <summary>JSON to plain .NET values. Integers stay integers.</summary>
    private static object? FromJson(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.String => e.GetString(),
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.Number => e.TryGetInt64(out long l) ? (object)l : e.TryGetUInt64(out ulong u) ? (object)u : (object)e.GetDouble(),
        JsonValueKind.Array => e.EnumerateArray().Select(FromJson).ToList(),
        JsonValueKind.Object => e.EnumerateObject().ToDictionary(p => p.Name, p => FromJson(p.Value)),
        _ => null,
    };

    /// <summary>Plain .NET values, as the SDK decodes them, to JSON. Integers stay integers.</summary>
    private static void WriteValue(Utf8JsonWriter w, object? value)
    {
        switch (value)
        {
            case null: w.WriteNullValue(); break;
            case bool b: w.WriteBooleanValue(b); break;
            case string s: w.WriteStringValue(s); break;
            case long l: w.WriteNumberValue(l); break;
            case int i: w.WriteNumberValue(i); break;
            case ulong u: w.WriteNumberValue(u); break;
            case uint ui: w.WriteNumberValue(ui); break;
            case double d: w.WriteNumberValue(d); break;
            case float f: w.WriteNumberValue(f); break;
            case byte[] bytes: w.WriteBase64StringValue(bytes); break;
            case IDictionary<string, object?> map:
                w.WriteStartObject();
                foreach (var kv in map)
                {
                    w.WritePropertyName(kv.Key);
                    WriteValue(w, kv.Value);
                }
                w.WriteEndObject();
                break;
            case System.Collections.IEnumerable items:
                w.WriteStartArray();
                foreach (object? item in items)
                {
                    WriteValue(w, item);
                }
                w.WriteEndArray();
                break;
            default:
                w.WriteStringValue(value.ToString());
                break;
        }
    }
}
