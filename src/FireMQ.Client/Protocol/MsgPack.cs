using System.Buffers;
using System.Collections;
using System.Globalization;
using System.Text;
using System.Text.Json;
using MessagePack;

namespace FireMQ.Protocol;

/// <summary>
/// Converts between MessagePack and plain .NET values: <c>null</c>, <see cref="bool"/>,
/// <see cref="long"/> (or <see cref="ulong"/> above <see cref="long.MaxValue"/>),
/// <see cref="double"/>, <see cref="float"/>, <see cref="string"/>, <see cref="byte"/>[],
/// <see cref="object"/>[] and <see cref="Dictionary{TKey,TValue}"/> with string keys.
/// </summary>
internal static class MsgPack
{
    private const int MaxDepth = 64;

    /// <summary>Reads any MessagePack value.</summary>
    public static object? Read(ref MessagePackReader r) => Read(ref r, 0);

    private static object? Read(ref MessagePackReader r, int depth)
    {
        if (depth > MaxDepth)
        {
            throw new MessagePackSerializationException("MessagePack value nested too deeply");
        }
        switch (r.NextMessagePackType)
        {
            case MessagePackType.Nil:
                r.ReadNil();
                return null;
            case MessagePackType.Boolean:
                return r.ReadBoolean();
            case MessagePackType.Integer:
                if (r.NextCode == MessagePackCode.UInt64)
                {
                    ulong u = r.ReadUInt64();
                    return u <= long.MaxValue ? (long)u : u;
                }
                return r.ReadInt64();
            case MessagePackType.Float:
                if (r.NextCode == MessagePackCode.Float32)
                {
                    return r.ReadSingle();
                }
                return r.ReadDouble();
            case MessagePackType.String:
                return r.ReadString();
            case MessagePackType.Binary:
                return r.ReadBytes()?.ToArray() ?? Array.Empty<byte>();
            case MessagePackType.Array:
            {
                int n = r.ReadArrayHeader();
                var items = new object?[n];
                for (int i = 0; i < n; i++)
                {
                    items[i] = Read(ref r, depth + 1);
                }
                return items;
            }
            case MessagePackType.Map:
            {
                int n = r.ReadMapHeader();
                var map = new Dictionary<string, object?>(n, StringComparer.Ordinal);
                for (int i = 0; i < n; i++)
                {
                    string key = ReadKey(ref r, depth + 1);
                    map[key] = Read(ref r, depth + 1);
                }
                return map;
            }
            case MessagePackType.Extension:
            {
                var ext = r.ReadExtensionFormat();
                return ext.Data.ToArray();
            }
            default:
                r.Skip();
                return null;
        }
    }

    private static string ReadKey(ref MessagePackReader r, int depth)
    {
        if (r.NextMessagePackType == MessagePackType.String)
        {
            return r.ReadString() ?? "";
        }
        object? key = Read(ref r, depth);
        return key switch
        {
            null => "",
            byte[] b => Encoding.UTF8.GetString(b),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => key.ToString() ?? "",
        };
    }

    /// <summary>Decodes a frame header. An empty header is an empty map.</summary>
    public static Dictionary<string, object?> ReadHeader(ReadOnlyMemory<byte> bytes)
    {
        if (bytes.IsEmpty)
        {
            return new Dictionary<string, object?>(StringComparer.Ordinal);
        }
        var r = new MessagePackReader(bytes);
        if (r.NextMessagePackType != MessagePackType.Map)
        {
            throw new MessagePackSerializationException("frame header is not a MessagePack map");
        }
        return (Dictionary<string, object?>)Read(ref r, 0)!;
    }

    /// <summary>Writes a .NET value as MessagePack.</summary>
    public static void Write(ref MessagePackWriter w, object? value) => Write(ref w, value, 0);

    private static void Write(ref MessagePackWriter w, object? value, int depth)
    {
        if (depth > MaxDepth)
        {
            throw new ArgumentException("header value nested too deeply");
        }
        switch (value)
        {
            case null:
                w.WriteNil();
                break;
            case bool b:
                w.Write(b);
                break;
            case string s:
                w.Write(s);
                break;
            case int i:
                w.Write(i);
                break;
            case long l:
                w.Write(l);
                break;
            case uint ui:
                w.Write(ui);
                break;
            case ulong ul:
                w.Write(ul);
                break;
            case short sh:
                w.Write(sh);
                break;
            case ushort us:
                w.Write(us);
                break;
            case sbyte sb:
                w.Write(sb);
                break;
            case byte by:
                w.Write(by);
                break;
            case double d:
                w.Write(d);
                break;
            case float f:
                w.Write(f);
                break;
            case decimal m:
                w.Write((double)m);
                break;
            case char c:
                w.Write(c.ToString());
                break;
            case byte[] bytes:
                w.Write(bytes.AsSpan());
                break;
            case ReadOnlyMemory<byte> rom:
                w.Write(rom.Span);
                break;
            case Memory<byte> mem:
                w.Write(mem.Span);
                break;
            case ArraySegment<byte> seg:
                w.Write(seg.AsSpan());
                break;
            case DateTimeOffset dto:
                w.Write(dto.ToUnixTimeSeconds());
                break;
            case DateTime dt:
                w.Write(new DateTimeOffset(dt.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(dt, DateTimeKind.Utc) : dt).ToUnixTimeSeconds());
                break;
            case Guid g:
                w.Write(g.ToString());
                break;
            case Enum e:
                w.Write(e.ToString());
                break;
            case JsonElement je:
                WriteJson(ref w, je, depth);
                break;
            case IDictionary<string, object?> dict:
                w.WriteMapHeader(dict.Count);
                foreach (var kv in dict)
                {
                    w.Write(kv.Key);
                    Write(ref w, kv.Value, depth + 1);
                }
                break;
            case IReadOnlyDictionary<string, object?> rdict:
                w.WriteMapHeader(rdict.Count);
                foreach (var kv in rdict)
                {
                    w.Write(kv.Key);
                    Write(ref w, kv.Value, depth + 1);
                }
                break;
            case IDictionary odict:
                w.WriteMapHeader(odict.Count);
                foreach (DictionaryEntry kv in odict)
                {
                    w.Write(Convert.ToString(kv.Key, CultureInfo.InvariantCulture) ?? "");
                    Write(ref w, kv.Value, depth + 1);
                }
                break;
            case IEnumerable seq:
            {
                var items = new List<object?>();
                foreach (object? item in seq)
                {
                    items.Add(item);
                }
                w.WriteArrayHeader(items.Count);
                foreach (object? item in items)
                {
                    Write(ref w, item, depth + 1);
                }
                break;
            }
            default:
                throw new ArgumentException(
                    $"cannot send a value of type {value.GetType()} in a FireMQ header; use null, bool, numbers, strings, byte[], lists or dictionaries");
        }
    }

    private static void WriteJson(ref MessagePackWriter w, JsonElement e, int depth)
    {
        switch (e.ValueKind)
        {
            case JsonValueKind.Null:
            case JsonValueKind.Undefined:
                w.WriteNil();
                break;
            case JsonValueKind.True:
                w.Write(true);
                break;
            case JsonValueKind.False:
                w.Write(false);
                break;
            case JsonValueKind.String:
                w.Write(e.GetString());
                break;
            case JsonValueKind.Number:
                if (e.TryGetInt64(out long l))
                {
                    w.Write(l);
                }
                else if (e.TryGetUInt64(out ulong ul))
                {
                    w.Write(ul);
                }
                else
                {
                    w.Write(e.GetDouble());
                }
                break;
            case JsonValueKind.Array:
                w.WriteArrayHeader(e.GetArrayLength());
                foreach (var item in e.EnumerateArray())
                {
                    WriteJson(ref w, item, depth + 1);
                }
                break;
            case JsonValueKind.Object:
            {
                int n = 0;
                foreach (var _ in e.EnumerateObject())
                {
                    n++;
                }
                w.WriteMapHeader(n);
                foreach (var p in e.EnumerateObject())
                {
                    w.Write(p.Name);
                    WriteJson(ref w, p.Value, depth + 1);
                }
                break;
            }
        }
    }

    /// <summary>Converts a JSON element to the same plain values <see cref="Read(ref MessagePackReader)"/> returns.</summary>
    public static object? FromJson(JsonElement e) => e.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.Undefined => null,
        JsonValueKind.True => true,
        JsonValueKind.False => false,
        JsonValueKind.String => e.GetString(),
        JsonValueKind.Number => e.TryGetInt64(out long l) ? (object)l
            : e.TryGetUInt64(out ulong ul) ? (object)ul
            : (object)e.GetDouble(),
        JsonValueKind.Array => e.EnumerateArray().Select(FromJson).ToArray(),
        JsonValueKind.Object => e.EnumerateObject().ToDictionary(p => p.Name, p => FromJson(p.Value), StringComparer.Ordinal),
        _ => null,
    };
}
