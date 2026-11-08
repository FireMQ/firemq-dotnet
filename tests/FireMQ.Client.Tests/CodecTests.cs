using System.Text;
using FireMQ.Protocol;

namespace FireMQ.Tests;

public class CodecTests
{
    private static T RoundTrip<T>(T frame) where T : Frame
    {
        byte[] bytes = frame.Encode();
        Assert.True(RawFrame.TryRead(bytes, uint.MaxValue, out var raw, out int consumed));
        Assert.Equal(bytes.Length, consumed);
        return Assert.IsType<T>(Frame.Decode(raw));
    }

    [Fact]
    public void PublishRoundTripsWithEveryPropertyAndHeaderType()
    {
        var headers = new Dictionary<string, object?>
        {
            ["nil"] = null,
            ["bool"] = true,
            ["int"] = 42,
            ["negative"] = -7L,
            ["big"] = ulong.MaxValue,
            ["float"] = 1.5,
            ["string"] = "s",
            ["bytes"] = new byte[] { 0, 255 },
            ["list"] = new List<object?> { 1, "two" },
            ["map"] = new Dictionary<string, object?> { ["k"] = "v" },
        };
        var props = new MessageProperties
        {
            ContentType = "application/json",
            ContentEncoding = "gzip",
            CorrelationId = "c-1",
            ReplyTo = "replies",
            Expiration = TimeSpan.FromMinutes(10),
            MessageId = "m-1",
            Timestamp = DateTimeOffset.FromUnixTimeSeconds(1_767_225_600),
            Type = "order.created",
            UserId = "guest",
            AppId = "shop",
            Priority = 3,
        };
        var decoded = RoundTrip(new PublishFrame
        {
            Seq = 42,
            Exchange = "orders",
            Key = "order.created",
            Mandatory = true,
            Persistent = true,
            Props = props,
            Headers = headers,
            Body = Encoding.UTF8.GetBytes("{\"id\":1}"),
        });

        Assert.Equal(42ul, decoded.Seq);
        Assert.Equal(("orders", "order.created", true, true), (decoded.Exchange, decoded.Key, decoded.Mandatory, decoded.Persistent));
        var p = decoded.Props!;
        Assert.Equal(
            (props.ContentType, props.ContentEncoding, props.CorrelationId, props.ReplyTo, props.Expiration, props.MessageId, props.Timestamp, props.Type, props.UserId, props.AppId, props.Priority),
            (p.ContentType, p.ContentEncoding, p.CorrelationId, p.ReplyTo, p.Expiration, p.MessageId, p.Timestamp, p.Type, p.UserId, p.AppId, p.Priority));

        var h = decoded.Headers!;
        Assert.Null(h["nil"]);
        Assert.Equal(true, h["bool"]);
        Assert.Equal(42L, h["int"]);
        Assert.Equal(-7L, h["negative"]);
        Assert.Equal(ulong.MaxValue, h["big"]);
        Assert.Equal(1.5, h["float"]);
        Assert.Equal("s", h["string"]);
        Assert.Equal(new byte[] { 0, 255 }, h["bytes"]);
        Assert.Equal(new object?[] { 1L, "two" }, h["list"]);
        Assert.Equal("v", Assert.IsType<Dictionary<string, object?>>(h["map"])["k"]);
        Assert.Equal("{\"id\":1}", Encoding.UTF8.GetString(decoded.Body.Span));
    }

    [Fact]
    public void ServerFramesRoundTrip()
    {
        var deliver = RoundTrip(new DeliverFrame { Sub = 1, Tag = 7, Redelivered = true, Key = "q", Body = new byte[] { 1, 2, 3 } });
        Assert.Equal((1u, 7ul, true, "", "q"), (deliver.Sub, deliver.Tag, deliver.Redelivered, deliver.Exchange, deliver.Key));
        Assert.Equal(new byte[] { 1, 2, 3 }, deliver.Body.ToArray());
        Assert.True(deliver.Props!.IsEmpty);

        var ok = RoundTrip(new OkFrame { Rid = 3, QueueName = "amq.gen-x", Messages = 0, Consumers = 0 });
        Assert.Equal((3u, "amq.gen-x", 0ul, 0ul, false), (ok.Rid, ok.QueueName, ok.Messages, ok.Consumers, ok.Empty));

        var error = RoundTrip(new ErrorFrame { Code = 406, Msg = "unknown delivery tag 9" });
        Assert.Equal((0u, (ushort)406, false), (error.Rid, error.Code, error.Fatal));

        var confirm = RoundTrip(new ConfirmFrame { Upto = 10 });
        Assert.Equal((10ul, 0ul, false), (confirm.Upto, confirm.Seq, confirm.Nack));

        Assert.Same(PingFrame.Instance, RoundTrip(PingFrame.Instance));
        Assert.Equal(new byte[] { 0, 0, 0, 5, Op.Ping, 0, 0, 0, 0 }, PingFrame.Instance.Encode());
    }

    [Fact]
    public void OmittedFieldsTakeZeroValues()
    {
        byte[] header = TestUtil.Pack(new Dictionary<string, object?> { ["queue"] = "jobs" });
        var s = Assert.IsType<SubscribeFrame>(Frame.Decode(new RawFrame(Op.Subscribe, header, default)));
        Assert.Equal(("jobs", 0u, 0u, false, ""), (s.Queue, s.Credit, s.Sub, s.NoAck, s.Tag));

        var welcome = Assert.IsType<WelcomeFrame>(Frame.Decode(new RawFrame(Op.Welcome, default, default)));
        Assert.Equal((0u, 0u, ""), (welcome.Heartbeat, welcome.MaxFrame, welcome.ServerName));
    }

    [Fact]
    public void UnknownKeysAndWrongTypesAreIgnored()
    {
        byte[] header = TestUtil.Pack(new Dictionary<string, object?>
        {
            ["tag"] = 5,
            ["future_field"] = new object?[] { 1, 2 },
            ["multiple"] = "not a bool",
        });
        var ack = Assert.IsType<AckFrame>(Frame.Decode(new RawFrame(Op.Ack, header, default)));
        Assert.Equal((5ul, false), (ack.Tag, ack.Multiple));
    }

    [Fact]
    public void UnknownOpsDecodeToNull()
    {
        Assert.Null(Frame.Decode(new RawFrame(0x7f, default, default)));
    }

    [Fact]
    public void HeaderThatIsNotAMapIsAProtocolError()
    {
        byte[] header = TestUtil.Pack(new object?[] { 1 });
        Assert.Throws<ProtocolException>(() => Frame.Decode(new RawFrame(Op.Ok, header, default)));
    }

    [Fact]
    public void FramesSplitAcrossReads()
    {
        byte[] stream = new PublishFrame { Key = "q", Body = Encoding.UTF8.GetBytes("hello") }.Encode()
            .Concat(PingFrame.Instance.Encode())
            .ToArray();
        var buffer = new List<byte>();
        var frames = new List<RawFrame>();
        foreach (byte b in stream)
        {
            buffer.Add(b);
            while (RawFrame.TryRead(buffer.ToArray(), 1024, out var frame, out int consumed))
            {
                frames.Add(frame);
                buffer.RemoveRange(0, consumed);
            }
        }
        Assert.Empty(buffer);
        Assert.Equal(new[] { Op.Publish, Op.Ping }, frames.Select(f => f.Op));
        Assert.Equal("hello", Encoding.UTF8.GetString(frames[0].Payload.Span));
        Assert.True(frames[1].Header.IsEmpty);
    }

    [Fact]
    public void OversizedAndMalformedFramesAreRejected()
    {
        byte[] big = new PublishFrame { Body = new byte[100] }.Encode();
        Assert.Throws<ProtocolException>(() => RawFrame.TryRead(big, 50, out _, out _));
        Assert.Throws<ProtocolException>(() => RawFrame.Parse(new byte[] { Op.Publish, 0, 0, 0, 9, 1 }));
        Assert.Throws<ProtocolException>(() => RawFrame.TryRead(new byte[] { 0, 0, 0, 3, 1, 2, 3 }, 1024, out _, out _));
    }

    [Fact]
    public void UnsupportedHeaderValuesAreRejected()
    {
        var frame = new PublishFrame { Headers = new Dictionary<string, object?> { ["x"] = new object() } };
        Assert.Throws<ArgumentException>(() => frame.Encode());
    }
}

public class UrlTests
{
    [Fact]
    public void DefaultsFillIn()
    {
        var u = ConnectUrl.Parse("fire://localhost");
        Assert.Equal(Transport.Tcp, u.Transport);
        Assert.Equal(5673, u.Port);
        Assert.Equal("/", u.Vhost);
        Assert.Equal(("guest", "guest"), (u.User, u.Password));
    }

    [Fact]
    public void CredentialsAndVhostAreDecoded()
    {
        var u = ConnectUrl.Parse("fires://app:p%40ss@mq.example.com:7000/prod%2Feu");
        Assert.Equal(Transport.Tls, u.Transport);
        Assert.Equal("mq.example.com", u.Host);
        Assert.Equal(7000, u.Port);
        Assert.Equal(("app", "p@ss"), (u.User, u.Password));
        Assert.Equal("prod/eu", u.Vhost);
    }

    [Fact]
    public void TlsDefaultsToPort5674()
    {
        Assert.Equal(5674, ConnectUrl.Parse("fires://mq.example.com").Port);
    }

    [Fact]
    public void UserWithoutPasswordKeepsDefaultPassword()
    {
        var u = ConnectUrl.Parse("fire://alice@localhost/dev");
        Assert.Equal(("alice", "guest", "dev"), (u.User, u.Password, u.Vhost));
    }

    [Fact]
    public void VhostCanComeFromTheQuery()
    {
        Assert.Equal("dev", ConnectUrl.Parse("fire://localhost?vhost=dev").Vhost);
    }

    [Fact]
    public void WebSocketUrlsUseQueryVhost()
    {
        var u = ConnectUrl.Parse("ws://localhost:15672/fire?vhost=dev");
        Assert.Equal((Transport.Ws, "/fire", "dev"), (u.Transport, u.Path, u.Vhost));
        var s = ConnectUrl.Parse("wss://mq.example.com");
        Assert.Equal((15671, "/fire"), (s.Port, s.Path));
    }

    [Fact]
    public void OtherSchemesAreRejected()
    {
        Assert.Throws<ArgumentException>(() => ConnectUrl.Parse("amqp://localhost"));
        Assert.Throws<ArgumentException>(() => ConnectUrl.Parse("not a url"));
    }

    [Fact]
    public void Ipv6HostsAreBracketedInAuthority()
    {
        var u = ConnectUrl.Parse("fire://[::1]:5673");
        Assert.Equal("::1", u.Host);
        Assert.Equal("[::1]:5673", u.Authority);
    }

    [Fact]
    public async Task WebSocketUrlsAreNotSupportedByThisClient()
    {
        await Assert.ThrowsAsync<NotSupportedException>(() => FireMQClient.ConnectAsync("ws://localhost:15672/fire"));
    }
}
