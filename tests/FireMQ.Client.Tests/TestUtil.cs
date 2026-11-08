using System.Collections;
using System.Globalization;
using MessagePack;
using FireMQ.Protocol;

namespace FireMQ.Tests;

internal static class TestUtil
{
    public static byte[] FromHex(string hex)
    {
        var bytes = new byte[hex.Length / 2];
        for (int i = 0; i < bytes.Length; i++)
        {
            bytes[i] = byte.Parse(hex.AsSpan(i * 2, 2), NumberStyles.HexNumber, CultureInfo.InvariantCulture);
        }
        return bytes;
    }

    public static string ToHex(ReadOnlySpan<byte> bytes) => Convert.ToHexString(bytes).ToLowerInvariant();

    /// <summary>Decodes a frame's header bytes into plain values.</summary>
    public static Dictionary<string, object?> HeaderOf(byte[] frame)
    {
        Assert.True(RawFrame.TryRead(frame, uint.MaxValue, out var raw, out int consumed));
        Assert.Equal(frame.Length, consumed);
        return MsgPack.ReadHeader(raw.Header);
    }

    /// <summary>Encodes a plain value as a MessagePack header.</summary>
    public static byte[] Pack(object? value)
    {
        var buffer = new ByteBuffer();
        var w = new MessagePackWriter(buffer);
        MsgPack.Write(ref w, value);
        w.Flush();
        return buffer.ToArray();
    }

    /// <summary>
    /// Drops entries holding a zero value (null, false, 0, "", empty map or list), recursively,
    /// because the protocol treats an omitted field and its zero value the same.
    /// </summary>
    public static object? WithoutZeros(object? value)
    {
        switch (value)
        {
            case IDictionary<string, object?> map:
            {
                var result = new SortedDictionary<string, object?>(StringComparer.Ordinal);
                foreach (var kv in map)
                {
                    var v = WithoutZeros(kv.Value);
                    if (!IsZero(v))
                    {
                        result[kv.Key] = v;
                    }
                }
                return result;
            }
            case object?[] items:
                return items.Select(WithoutZeros).ToArray();
            default:
                return value;
        }
    }

    private static bool IsZero(object? v) => v switch
    {
        null => true,
        false => true,
        long l => l == 0,
        ulong u => u == 0,
        double d => d == 0,
        string s => s.Length == 0,
        ICollection c => c.Count == 0,
        _ => false,
    };

    /// <summary>Compares plain value trees. Integers compare by value whatever their .NET type.</summary>
    public static void AssertSame(object? expected, object? actual, string path = "$")
    {
        switch (expected)
        {
            case IDictionary<string, object?> em:
            {
                var am = Assert.IsAssignableFrom<IDictionary<string, object?>>(actual);
                Assert.True(
                    em.Keys.OrderBy(k => k, StringComparer.Ordinal).SequenceEqual(am.Keys.OrderBy(k => k, StringComparer.Ordinal)),
                    $"{path}: keys differ: expected [{string.Join(", ", em.Keys.OrderBy(k => k))}], got [{string.Join(", ", am.Keys.OrderBy(k => k))}]");
                foreach (var kv in em)
                {
                    AssertSame(kv.Value, am[kv.Key], $"{path}.{kv.Key}");
                }
                break;
            }
            case object?[] ea:
            {
                var aa = Assert.IsType<object?[]>(actual);
                Assert.True(ea.Length == aa.Length, $"{path}: expected {ea.Length} items, got {aa.Length}");
                for (int i = 0; i < ea.Length; i++)
                {
                    AssertSame(ea[i], aa[i], $"{path}[{i}]");
                }
                break;
            }
            case long or ulong or int or uint:
                Assert.True(actual is long or ulong or int or uint, $"{path}: expected an integer, got {actual?.GetType().Name ?? "null"}");
                Assert.True(Convert.ToDecimal(expected, CultureInfo.InvariantCulture) == Convert.ToDecimal(actual, CultureInfo.InvariantCulture), $"{path}: expected {expected}, got {actual}");
                break;
            default:
                Assert.True(Equals(expected, actual), $"{path}: expected {expected ?? "null"}, got {actual ?? "null"}");
                break;
        }
    }
}
