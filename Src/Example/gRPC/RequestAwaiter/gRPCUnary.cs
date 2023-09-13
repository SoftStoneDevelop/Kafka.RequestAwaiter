using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    public class gRPCUnary
    {
        public async Task Run()
        {
            Console.WriteLine($"Unary");
            var channel0 = GrpcChannel.ForAddress(
                "http://127.0.0.1:4400",
                new GrpcChannelOptions()
                {
                    Credentials = ChannelCredentials.Insecure,
                    HttpHandler = new SocketsHttpHandler
                    {
                        EnableMultipleHttp2Connections = true,
                        PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                        KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                        KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                    }
                }
                );

            await channel0.ConnectAsync();
            var client0 = new GrcpService.RespondService.RespondServiceClient(channel0);

            var channel1 = GrpcChannel.ForAddress(
                "http://localhost:4401",
                new GrpcChannelOptions()
                {
                    Credentials = ChannelCredentials.Insecure,
                    HttpHandler = new SocketsHttpHandler
                    {
                        EnableMultipleHttp2Connections = true,
                        PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                        KeepAlivePingDelay = TimeSpan.FromSeconds(60),
                        KeepAlivePingTimeout = TimeSpan.FromSeconds(30),
                    }
                }
                );

            await channel1.ConnectAsync();
            var client1 = new GrcpService.RespondService.RespondServiceClient(channel1);

            int requests = 0;
            while (true)
            {
                Console.WriteLine($"Write 'exit' for exit or press write 'requests count' for new pack");
                var read = Console.ReadLine();
                if (read == "exit")
                {
                    break;
                }

                requests = int.Parse(read);
                Console.WriteLine($"Start {requests} reqests");
                var iterationTimes = new long[20];
                for (int iteration = 0; iteration < 20; iteration++)
                {
                    Console.WriteLine($"Iteration {iteration}");
                    Stopwatch sw = Stopwatch.StartNew();
                    var tasks = new Task<long>[requests];

                    Parallel.For(0, requests, (index) =>
                    {
                        tasks[index] = Produce(client0, client1);
                    });

                    Console.WriteLine($"Create tasks: {sw.ElapsedMilliseconds} ms");
                    Task.WaitAll(tasks);
                    sw.Stop();
                    iterationTimes[iteration] = sw.ElapsedMilliseconds;
                    Console.WriteLine($"Requests sended: {requests}");
                    Console.WriteLine($"Pack Time: {sw.ElapsedMilliseconds} ms");

                    var hashSet = new Dictionary<long, int>();
                    foreach (var task in tasks)
                    {
                        var executedTime = task.Result;
                        if (hashSet.TryGetValue(executedTime, out var internalCount))
                        {
                            hashSet[executedTime] = ++internalCount;
                        }
                        else
                        {
                            hashSet[executedTime] = 1;
                        }
                    }
                    Console.WriteLine($"Times:");

                    var pairs = hashSet.OrderBy(or => or.Key).ToList();
                    long startRange = pairs.First().Key;
                    long last = pairs.First().Key;
                    int count = pairs.First().Value;
                    var sb = new StringBuilder();

                    for (int i = 0; i < pairs.Count; i++)
                    {
                        KeyValuePair<long, int> item = pairs[i];
                        if (item.Key > startRange + 100)
                        {
                            sb.AppendLine($"({startRange} - {last}) ms: count {count}");
                            count = item.Value;
                            startRange = item.Key;
                            last = item.Key;
                        }
                        else if (i != 0)
                        {
                            count += item.Value;
                            last = item.Key;
                        }

                        if (i == pairs.Count - 1)
                        {
                            sb.AppendLine($"({startRange} - {last}) ms: count {count}");
                        }
                    }

                    Console.Write(sb.ToString());
                }

                Console.WriteLine("Iterations:");
                for (int i = 0; i < iterationTimes.Length; i++)
                {
                    var number = i + 1;
                    var time = iterationTimes[i];
                    Console.WriteLine($"Iteration {number}: {time}");
                }
            }
        }

        private async Task<long> Produce(
            GrcpService.RespondService.RespondServiceClient client0,
            GrcpService.RespondService.RespondServiceClient client1
            )
        {
            Stopwatch sb = Stopwatch.StartNew();
            var request = new GrcpService.HelloRequest()
            {
                Text = "Hello"
            };

            var req0 = client0.HelloAsync(request).ConfigureAwait(false);
            var req1 = client1.HelloAsync(request).ConfigureAwait(false);

            await req0;
            await req1;

            return sb.ElapsedMilliseconds;
        }
    }
}
