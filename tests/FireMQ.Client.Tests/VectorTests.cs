using System.Text.Json;
using FireMQ.Protocol;

namespace FireMQ.Tests;

/// <summary>Decodes every frame in the spec's test vectors (TestVectors/frames.json).</summary>
public class VectorTests
{
    public sealed record Vector(string Name, byte Op, string OpName, string FrameHex, string PayloadHex, string HeaderJson)
    {
        public override string ToString() => Name;
    }

    public static IEnumerable<object[]> Vectors()
    {
        string path = Path.Combine(AppContext.BaseDirectory, "TestVectors", "frames.json");
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        foreach (var v in doc.RootElement.GetProperty("vectors").EnumerateArray())
        {
            yield return new object[]
            {
                new Vector(
                    v.GetProperty("name").GetString()!,
                    v.GetProperty("op").GetByte(),
                    v.GetProperty("op_name").GetString()!,
                    v.GetProperty("frame_hex").GetString()!,
                    v.GetProperty("payload_hex").GetString()!,
                    v.GetProperty("header").GetRawText()),
            };
        }
    }

    [Fact]
    public void EveryOpHasAVector()
    {
        var ops = Vectors().Select(v => ((Vector)v[0]).Op).ToHashSet();
        byte[] all =
        {
            Op.Hello, Op.Welcome, Op.Ping, Op.Pong, Op.Close, Op.Ok, Op.Error, Op.Flow,
            Op.DeclareExchange, Op.DeleteExchange, Op.DeclareQueue, Op.DeleteQueue, Op.PurgeQueue,
            Op.Bind, Op.Unbind, Op.Publish, Op.Confirm, Op.Return, Op.Subscribe, Op.Unsubscribe,
            Op.Credit, Op.Deliver, Op.Cancelled, Op.Ack, Op.Nack, Op.Get, Op.GetOk,
        };
        Assert.All(all, op => Assert.Contains(op, ops));
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void FrameSplitsIntoOpHeaderAndPayload(Vector v)
    {
        byte[] bytes = TestUtil.FromHex(v.FrameHex);
        Assert.True(RawFrame.TryRead(bytes, Proto.DefaultMaxFrame, out var raw, out int consumed));
        Assert.Equal(bytes.Length, consumed);
        Assert.Equal(v.Op, raw.Op);
        Assert.Equal(v.OpName, Op.Name(raw.Op));
        Assert.Equal(v.PayloadHex, TestUtil.ToHex(raw.Payload.Span));
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void HeaderDecodesToTheListedMap(Vector v)
    {
        Assert.True(RawFrame.TryRead(TestUtil.FromHex(v.FrameHex), Proto.DefaultMaxFrame, out var raw, out _));
        var header = MsgPack.ReadHeader(raw.Header);
        using var expected = JsonDocument.Parse(v.HeaderJson);
        TestUtil.AssertSame(MsgPack.FromJson(expected.RootElement), header);
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void TypedFrameKeepsEveryField(Vector v)
    {
        Assert.True(RawFrame.TryRead(TestUtil.FromHex(v.FrameHex), Proto.DefaultMaxFrame, out var raw, out _));
        var frame = Frame.Decode(raw);
        Assert.NotNull(frame);
        Assert.Equal(v.Op, frame!.Op);
        Assert.Equal(v.PayloadHex, TestUtil.ToHex(frame.Payload.Span));

        // Re-encode the typed frame. MessagePack allows several encodings of one map, so
        // compare decoded headers, treating omitted fields as their zero values.
        byte[] again = frame.Encode();
        using var expected = JsonDocument.Parse(v.HeaderJson);
        TestUtil.AssertSame(
            TestUtil.WithoutZeros(MsgPack.FromJson(expected.RootElement)),
            TestUtil.WithoutZeros(TestUtil.HeaderOf(again)));

        Assert.True(RawFrame.TryRead(again, Proto.DefaultMaxFrame, out var raw2, out _));
        Assert.Equal(v.PayloadHex, TestUtil.ToHex(raw2.Payload.Span));
    }

    [Fact]
    public void TypedFieldsMatchTheVectors()
    {
        var frames = Vectors()
            .Select(o => (Vector)o[0])
            .ToDictionary(v => v.Name, v =>
            {
                Assert.True(RawFrame.TryRead(TestUtil.FromHex(v.FrameHex), Proto.DefaultMaxFrame, out var raw, out _));
                return Frame.Decode(raw)!;
            });

        var hello = Assert.IsType<HelloFrame>(frames["hello"]);
        Assert.Equal(1u, hello.Ver);
        Assert.Equal("/", hello.Vhost);
        Assert.Equal(("plain", "guest", "guest"), (hello.Auth.Mech, hello.Auth.User, hello.Auth.Pass));
        Assert.Equal(30u, hello.Heartbeat);
        Assert.Equal(new[] { "confirms", "credit" }, hello.Caps);

        var welcome = Assert.IsType<WelcomeFrame>(frames["welcome"]);
        Assert.Equal(("firemq", "0.1.0"), (welcome.ServerName, welcome.ServerVersion));
        Assert.Equal(16_777_216u, welcome.MaxFrame);
        Assert.Equal(30u, welcome.Heartbeat);
        Assert.Equal("127.0.0.1:50000 -> 127.0.0.1:5673", welcome.ConnId);

        var ok = Assert.IsType<OkFrame>(frames["ok"]);
        Assert.Equal((2u, "jobs", 0ul, 0ul), (ok.Rid, ok.QueueName, ok.Messages, ok.Consumers));

        var error = Assert.IsType<ErrorFrame>(frames["error"]);
        Assert.Equal((3u, (ushort)404, false), (error.Rid, error.Code, error.Fatal));

        Assert.True(Assert.IsType<FlowFrame>(frames["flow"]).Pause);

        var publish = Assert.IsType<PublishFrame>(frames["publish"]);
        Assert.Equal((1ul, "orders", "order.created", true, true), (publish.Seq, publish.Exchange, publish.Key, publish.Mandatory, publish.Persistent));
        Assert.Equal("application/json", publish.Props!.ContentType);
        Assert.Equal("req-1", publish.Props.CorrelationId);
        Assert.Equal("amq.gen-reply", publish.Props.ReplyTo);
        Assert.Equal(TimeSpan.FromSeconds(30), publish.Props.Expiration);
        Assert.Equal(DateTimeOffset.FromUnixTimeSeconds(1_767_225_600), publish.Props.Timestamp);
        Assert.Equal("abc", publish.Headers!["trace"]);
        Assert.Equal(2L, publish.Headers["x-attempt"]);
        Assert.Equal("{\"id\":42}", System.Text.Encoding.UTF8.GetString(publish.Body.Span));

        Assert.Equal(1ul, Assert.IsType<ConfirmFrame>(frames["confirm"]).Upto);
        var nack = Assert.IsType<ConfirmFrame>(frames["confirm_nack"]);
        Assert.Equal((2ul, true, (ushort)406, "queue 'jobs' is full"), (nack.Seq, nack.Nack, nack.Code, nack.Msg));

        var ret = Assert.IsType<ReturnFrame>(frames["return"]);
        Assert.Equal((3ul, (ushort)312, "orders", "order.unknown"), (ret.Seq, ret.Code, ret.Exchange, ret.Key));

        var deliver = Assert.IsType<DeliverFrame>(frames["deliver"]);
        Assert.Equal((1u, 1ul, false), (deliver.Sub, deliver.Tag, deliver.Redelivered));

        var unbind = Assert.IsType<BindFrame>(frames["unbind"]);
        Assert.True(unbind.Unbind);
        Assert.Equal(("orders", "audit", "#", ""), (unbind.Exchange, unbind.ToExchange, unbind.Key, unbind.Queue));

        var getOk = Assert.IsType<GetOkFrame>(frames["get_ok"]);
        Assert.Equal((8u, 9ul, 3ul, "jobs"), (getOk.Rid, getOk.Tag, getOk.Remaining, getOk.Key));
    }
}
