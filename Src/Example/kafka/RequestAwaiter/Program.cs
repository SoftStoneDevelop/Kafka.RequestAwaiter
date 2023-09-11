using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaExchanger.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(GrcpService.HelloResponse), new string[] { "RAResponder1" }),
        Input(keyType: typeof(Null), valueType: typeof(GrcpService.HelloResponse), new string[] { "RAResponder2" }),
        Output(keyType: typeof(Null), valueType: typeof(GrcpService.HelloRequest))
        ]
    public partial class RequestAwaiter
    {

    }

    internal class Program
    {
        private static async Task ReCreateTopic(IAdminClient adminClient, string topicName)
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
            if (metadata.Topics.Any(an => an.Topic == topicName))
            {
                await adminClient.DeleteTopicsAsync(
                    new string[]
                    {
                        topicName
                    });

                await Task.Delay(300);
            }

            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                                new TopicSpecification
                                {
                                    Name = topicName,
                                    ReplicationFactor = -1,
                                    NumPartitions = 3,
                                    Configs = new System.Collections.Generic.Dictionary<string, string>
                                    {
                                        { "min.insync.replicas", "1" }
                                    }
                                }
                    }
                    );
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine($"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }

        static async Task Main(string[] args)
        {
            var bootstrapServers = "localhost:9194, localhost:9294, localhost:9394";
            var input0Name = "RAInputSimple1";
            var input1Name = "RAInputSimple2";
            var outputName = "RAOutputSimple";

            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = bootstrapServers
            }).Build())
            {
                await ReCreateTopic(adminClient, input0Name);
                await ReCreateTopic(adminClient, input1Name);
                await ReCreateTopic(adminClient, outputName);
            }

            var pool = new KafkaExchanger.Common.ProducerPoolNullProto(
                3,
                bootstrapServers,
                static (config) =>
                {
                    config.LingerMs = 5;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );

            await using var reqAwaiter = await Scenario1(bootstrapServers, input0Name, input1Name, outputName, pool);

            int requests = 0;
            while ( true )
            {
                Console.WriteLine($"Write 'exit' for exit or press write 'requests count' for new pack");
                var read = Console.ReadLine();
                if(read == "exit")
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

                    long sleepTime = 0;
                    if (true)//with pause
                    {
                        var pack = 0;
                        for (int i = 0; i < requests; i++)
                        {
                            tasks[i] = Produce(reqAwaiter);
                            pack++;

                            if (pack == 1000)
                            {
                                pack = 0;
                                var swSleep = Stopwatch.StartNew();
                                Thread.Sleep(1);
                                sleepTime += swSleep.ElapsedMilliseconds;
                            }
                        }

                        Console.WriteLine($"Sleep time: {sleepTime} ms");
                    }

                    if(false)//parralel
                    {
                        Parallel.For(0, requests, (index) =>
                        {
                            tasks[index] = Produce(reqAwaiter);
                        });
                    }

                    Console.WriteLine($"Create tasks: {sw.ElapsedMilliseconds - sleepTime} ms");
                    Task.WaitAll(tasks);
                    sw.Stop();
                    iterationTimes[iteration] = sw.ElapsedMilliseconds - sleepTime;
                    Console.WriteLine($"Requests sended: {requests}");
                    Console.WriteLine($"Pack Time: {sw.ElapsedMilliseconds - sleepTime} ms");

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

        private static async Task<RequestAwaiter> Scenario1(
            string bootstrapServers,
            string input0Name,
            string input1Name,
            string outputName,
            KafkaExchanger.Common.ProducerPoolNullProto pool
            )
        {
            var reqAwaiter = new RequestAwaiter();
            var reqAwaiterConfitg =
                new RequestAwaiter.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: bootstrapServers,
                    itemsInBucket: 1000,
                    inFlyBucketsLimit: 5,
                    addNewBucket: (bucketId, partitions0, topic0Name, partitions1, topic1Name) =>
                    {
                        return Task.CompletedTask;
                    },
                    bucketsCount: async (partitions0, topic0Name, partitions1, topic1Name) =>
                    {
                        return await Task.FromResult(2);
                    },
                    processors: new RequestAwaiter.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiter.ProcessorConfig(
                            input0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 0 }
                                ),
                            input1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                partitions: new int[] { 0 }
                                ),
                            output0: new RequestAwaiter.ProducerInfo(outputName)
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            input0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 1 }
                                ),
                            input1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                partitions: new int[] { 1 }
                                ),
                            output0:new RequestAwaiter.ProducerInfo(outputName)
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            input0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 2 }
                                ),
                            input1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                partitions: new int[] { 2 }
                                ),
                            output0: new RequestAwaiter.ProducerInfo(outputName)
                            )
                    }
                    );
            Console.WriteLine("Start ReqAwaiter");
            await reqAwaiter.Setup(
                reqAwaiterConfitg,
                producerPool0: pool,
                currentBucketsCount: reqAwaiterConfitg.BucketsCount
                );

            reqAwaiter.Start(
                static (config) =>
                {
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("ReqAwaiter started");

            return reqAwaiter;
        }

        private static async Task<long> Produce(RequestAwaiter reqAwaiter)
        {
            Stopwatch sb = Stopwatch.StartNew();
            using var result = await reqAwaiter.Produce(new GrcpService.HelloRequest { Text = "Hello" }).ConfigureAwait(false);
            return sb.ElapsedMilliseconds;
        }
    }
}