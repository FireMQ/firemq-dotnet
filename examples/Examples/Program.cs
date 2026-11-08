// The RabbitMQ tutorials, written with FireMQ.Client. Each example runs its producer and
// its consumers in one process and exits when they are done.
//
//   dotnet run --project examples/Examples -- hello_world
//   FIREMQ_URL=fire://guest:guest@localhost:5673 dotnet run --project examples/Examples -- all

using FireMQ.Examples;

var examples = new Dictionary<string, Func<string, CancellationToken, Task>>
{
    ["hello_world"] = HelloWorld.RunAsync,
    ["work_queues"] = WorkQueues.RunAsync,
    ["publish_subscribe"] = PublishSubscribe.RunAsync,
    ["routing"] = Routing.RunAsync,
    ["topics"] = Topics.RunAsync,
    ["rpc"] = Rpc.RunAsync,
};

string name = args.Length > 0 ? args[0] : "";
if (name != "all" && !examples.ContainsKey(name))
{
    Console.Error.WriteLine($"usage: Examples <{string.Join("|", examples.Keys)}|all>");
    return 2;
}

string url = Environment.GetEnvironmentVariable("FIREMQ_URL") ?? "fire://guest:guest@localhost:5673";
foreach (var (key, run) in examples.Where(e => name == "all" || e.Key == name))
{
    Console.WriteLine($"== {key}");
    using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
    await run(url, timeout.Token);
}
return 0;
