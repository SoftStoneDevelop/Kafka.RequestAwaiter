using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    public class gRPCStream
    {
        private class Operation
        {
            public Operation(GrcpService.HelloStreamRequest helloStreamRequest) 
            {
                HelloStreamRequest = helloStreamRequest;
            }

            public Stopwatch Sw = Stopwatch.StartNew();
            public GrcpService.HelloStreamRequest HelloStreamRequest;

            public TaskCompletionSource Response0 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            public TaskCompletionSource Response1 = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public async Task Run()
        {
            Console.WriteLine($"Stream");
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
                    var responseDic = new ConcurrentDictionary<string, Operation>();
                    var cts = new CancellationTokenSource();


                    var stream0 = client0.HelloStream(cancellationToken: cts.Token);
                    var read0Routine = Reponse0Routine(stream0.ResponseStream, responseDic, cts.Token);
                    var writeChannel0 = Channel.CreateUnbounded<Operation>(new UnboundedChannelOptions
                    {
                        SingleWriter = false,
                        SingleReader = true,
                        AllowSynchronousContinuations = false
                    });
                    var write0Routine = WriteRoutine(writeChannel0.Reader, stream0.RequestStream, cts.Token);

                    var stream1 = client1.HelloStream(cancellationToken: cts.Token);
                    var read1Routine = Reponse1Routine(stream1.ResponseStream, responseDic, cts.Token);
                    var writeChannel1 = Channel.CreateUnbounded<Operation>(new UnboundedChannelOptions
                    {
                        SingleWriter = false,
                        SingleReader = true,
                        AllowSynchronousContinuations = false
                    });
                    var write1Routine = WriteRoutine(writeChannel1.Reader, stream1.RequestStream, cts.Token);

                    Console.WriteLine($"Iteration {iteration}");
                    Stopwatch sw = Stopwatch.StartNew();
                    var tasks = new Task[requests];

                    Parallel.For(0, requests, (index) =>
                    {
                        tasks[index] = Produce(writeChannel0.Writer, writeChannel1.Writer, responseDic);
                    });

                    Console.WriteLine($"Create tasks: {sw.ElapsedMilliseconds} ms");
                    Task.WaitAll(tasks);
                    sw.Stop();
                    iterationTimes[iteration] = sw.ElapsedMilliseconds;
                    Console.WriteLine($"Requests sended: {requests}");
                    Console.WriteLine($"Pack Time: {sw.ElapsedMilliseconds} ms");

                    var hashSet = new Dictionary<long, int>();
                    foreach (var operation in responseDic.Values)
                    {
                        var executedTime = operation.Sw.ElapsedMilliseconds;
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
                    cts.Cancel();

                    try
                    {
                        stream0.Dispose();
                    }
                    catch
                    {
                        //ignore
                    }

                    try
                    {
                        await read0Routine;
                    }
                    catch
                    {
                        //ignore
                    }

                    try
                    {
                        await write0Routine;
                    }
                    catch
                    {
                        //ignore
                    }

                    try
                    {
                        stream1.Dispose();
                    }
                    catch
                    {
                        //ignore
                    }

                    try
                    {
                        await read1Routine;
                    }
                    catch
                    {
                        //ignore
                    }

                    try
                    {
                        await write1Routine;
                    }
                    catch
                    {
                        //ignore
                    }
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

        private async Task Produce(
            ChannelWriter<Operation> writer0,
            ChannelWriter<Operation> writer1,
            ConcurrentDictionary<string, Operation> dic
            )
        {
            var guid = Guid.NewGuid().ToString("D");
            var request = new GrcpService.HelloStreamRequest()
            {
                Guid = guid,
                Text = "Hello"
            };

            var operation = new Operation(request);
            dic.TryAdd(guid, operation);
            var write0 = writer0.WriteAsync(operation).ConfigureAwait(false);
            var write1 = writer1.WriteAsync(operation).ConfigureAwait(false);

            await write0;
            await write1;

            await operation.Response0.Task.ConfigureAwait(false);
            await operation.Response1.Task.ConfigureAwait(false);

            operation.Sw.Stop();
        }

        private async Task WriteRoutine(
            ChannelReader<Operation> reader,
            IClientStreamWriter<GrcpService.HelloStreamRequest> writer,
            CancellationToken cancellationToken = default
            )
        {
            while (!cancellationToken.IsCancellationRequested) 
            {
                var operation = await reader.ReadAsync(cancellationToken);
                await writer.WriteAsync(operation.HelloStreamRequest).ConfigureAwait(false);
            }
        }

        private async Task Reponse0Routine(
            IAsyncStreamReader<GrcpService.HelloStreamResponse> reader,
            ConcurrentDictionary<string, Operation> dic,
            CancellationToken cancellationToken = default
            )
        {
            await foreach (var response in reader.ReadAllAsync(cancellationToken))
            {
                if(dic.TryGetValue(response.Guid, out var operation))
                {
                    operation.Response0.SetResult();
                }
            }
        }

        private async Task Reponse1Routine(
            IAsyncStreamReader<GrcpService.HelloStreamResponse> reader,
            ConcurrentDictionary<string, Operation> dic,
            CancellationToken cancellationToken = default
            )
        {
            await foreach (var response in reader.ReadAllAsync(cancellationToken))
            {
                if (dic.TryGetValue(response.Guid, out var operation))
                {
                    operation.Response1.SetResult();
                }
            }
        }
    }
}