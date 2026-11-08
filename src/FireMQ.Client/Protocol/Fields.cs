using System.Globalization;
using System.Text;
using MessagePack;

namespace FireMQ.Protocol;

/// <summary>
/// Lenient reads from a decoded header map. A missing key, or a value of the wrong type,
/// reads as the zero value, as the spec asks: omitted fields take their zero value and
/// receivers ignore what they do not understand.
/// </summary>
internal static class FieldReader
{
    public static string Str(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) ? AsString(v) : "";

    public static string? OptStr(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) && v is not null ? AsString(v) : null;

    public static ulong U64(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) ? AsU64(v) ?? 0 : 0;

    public static ulong? OptU64(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) ? AsU64(v) : null;

    public static uint U32(this Dictionary<string, object?> h, string key)
    {
        ulong v = h.U64(key);
        return v > uint.MaxValue ? uint.MaxValue : (uint)v;
    }

    public static bool Bool(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) && v switch
        {
            bool b => b,
            long l => l != 0,
            ulong u => u != 0,
            _ => false,
        };

    public static Dictionary<string, object?> Map(this Dictionary<string, object?> h, string key) =>
        h.TryGetValue(key, out object? v) && v is Dictionary<string, object?> m
            ? m
            : new Dictionary<string, object?>(StringComparer.Ordinal);

    public static List<string> StrList(this Dictionary<string, object?> h, string key)
    {
        var list = new List<string>();
        if (h.TryGetValue(key, out object? v) && v is object?[] items)
        {
            foreach (object? item in items)
            {
                if (item is not null)
                {
                    list.Add(AsString(item));
                }
            }
        }
        return list;
    }

    private static string AsString(object? v) => v switch
    {
        null => "",
        string s => s,
        byte[] b => Encoding.UTF8.GetString(b),
        bool b => b ? "true" : "false",
        IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
        _ => "",
    };

    private static ulong? AsU64(object? v) => v switch
    {
        long l when l >= 0 => (ulong)l,
        ulong u => u,
        double d when d >= 0 && d <= ulong.MaxValue => (ulong)d,
        float f when f >= 0 && f <= ulong.MaxValue => (ulong)f,
        string s when ulong.TryParse(s, NumberStyles.None, CultureInfo.InvariantCulture, out ulong p) => p,
        _ => null,
    };
}

/// <summary>
/// Writes header maps. Entries with zero values are omitted, since receivers read a
/// missing field as its zero value. Every header map in the protocol has at most 15
/// entries, so the map is written as a fixmap whose size is patched in at the end.
/// </summary>
/// <remarks>
/// The map owns the only MessagePack writer on the buffer until <see cref="End"/>, which
/// flushes it. Nested maps (properties, arguments, headers) are written through
/// <see cref="W"/> with their size known up front.
/// </remarks>
internal ref struct MapWriter
{
    private readonly ByteBuffer _buffer;
    private readonly int _start;
    private int _count;

    public MessagePackWriter W;

    private MapWriter(ByteBuffer buffer)
    {
        _buffer = buffer;
        _start = buffer.Length;
        _count = 0;
        W = new MessagePackWriter(buffer);
        W.WriteMapHeader(0);
    }

    /// <summary>Starts a map at the current end of <paramref name="buffer"/>.</summary>
    public static MapWriter Begin(ByteBuffer buffer) => new(buffer);

    /// <summary>Flushes the map into the buffer and patches in its size.</summary>
    public void End()
    {
        if (_count > 15)
        {
            throw new InvalidOperationException("header map has more than 15 entries");
        }
        W.Flush();
        _buffer[_start] = (byte)(0x80 | _count);
    }

    public void Key(string key)
    {
        W.Write(key);
        _count++;
    }

    public void Str(string key, string? value, bool always = false)
    {
        if (always || !string.IsNullOrEmpty(value))
        {
            Key(key);
            W.Write(value ?? "");
        }
    }

    public void U64(string key, ulong value, bool always = false)
    {
        if (always || value != 0)
        {
            Key(key);
            W.Write(value);
        }
    }

    public void OptU64(string key, ulong? value)
    {
        if (value.HasValue)
        {
            Key(key);
            W.Write(value.Value);
        }
    }

    public void Bool(string key, bool value, bool always = false)
    {
        if (always || value)
        {
            Key(key);
            W.Write(value);
        }
    }

    public void Map(string key, IDictionary<string, object?>? value)
    {
        if (value is { Count: > 0 })
        {
            Key(key);
            MsgPack.Write(ref W, value);
        }
    }

    public void StrList(string key, IReadOnlyList<string>? values)
    {
        if (values is { Count: > 0 })
        {
            Key(key);
            W.WriteArrayHeader(values.Count);
            foreach (string v in values)
            {
                W.Write(v);
            }
        }
    }
}
