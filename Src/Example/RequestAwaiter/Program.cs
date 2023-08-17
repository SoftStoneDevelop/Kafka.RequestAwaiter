using Confluent.Kafka;
using KafkaExchanger.Attributes;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RequestAwaiterConsole
{
    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiter
    {

    }

    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string), new string[] { "RAResponder1", "RAResponder2" }),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiter2
    {

    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            var bootstrapServers = "localhost:9194, localhost:9294, localhost:9394";
            var input0Name = "RAInputSimple1";
            var input1Name = "RAInputSimple2";
            var outputName = "RAOutputSimple";

            var pool = new KafkaExchanger.Common.ProducerPoolNullString(
                20,
                bootstrapServers,
                static (config) =>
                {
                    config.LingerMs = 1;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );

            //using var reqAwaiter = Scenario1(bootstrapServers, input0Name, input1Name, outputName, pool);
            await using var reqAwaiter = Scenario2(bootstrapServers, input0Name, outputName, pool);

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
                Stopwatch sw = Stopwatch.StartNew();
                //var tasks = new Task<(RequestAwaiter.Response, long)>[requests];
                var tasks = new Task<(RequestAwaiter2.Response, long)>[requests];
                for (int i = 0; i < requests; i++)
                {
                    //tasks[i] = Produce(reqAwaiter);
                    tasks[i] = Produce2(reqAwaiter);
                }
                Console.WriteLine($"Create tasks: {sw.ElapsedMilliseconds} ms");
                Task.WaitAll(tasks);
                sw.Stop();
                Console.WriteLine($"Requests sended: {requests}");
                Console.WriteLine($"First pack Time: {sw.ElapsedMilliseconds} ms");
                Console.WriteLine($"Per request: {sw.ElapsedMilliseconds / requests} ms");

                var hashSet = new Dictionary<long, int>();
                foreach (var task in tasks)
                {
                    var executedTime = task.Result.Item2;
                    if(hashSet.TryGetValue(executedTime, out var internalCount))
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
                    else if(i != 0)
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
        }

        private static RequestAwaiter Scenario1(
            string bootstrapServers,
            string input0Name,
            string input1Name,
            string outputName,
            KafkaExchanger.Common.ProducerPoolNullString pool
            )
        {
            var reqAwaiter = new RequestAwaiter();
            var reqAwaiterConfitg =
                new RequestAwaiter.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: bootstrapServers,
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
                            output0: new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
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
                            output0:new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
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
                            output0: new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            )
                    }
                    );
            Console.WriteLine("Start ReqAwaiter");
            reqAwaiter.Setup(
                reqAwaiterConfitg,
                producerPool0: pool
                );

            reqAwaiter.Start(
                static (config) =>
                {
                    //config.MaxPollIntervalMs = 5_000;
                    //config.SessionTimeoutMs = 2_000;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("ReqAwaiter started");

            return reqAwaiter;
        }

        private static RequestAwaiter2 Scenario2(
            string bootstrapServers,
            string input0Name,
            string outputName,
            KafkaExchanger.Common.ProducerPoolNullString pool
            )
        {
            var reqAwaiter = new RequestAwaiter2();
            var reqAwaiterConfitg =
                new RequestAwaiter2.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: bootstrapServers,
                    processors: new RequestAwaiter2.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiter2.ProcessorConfig(
                            input0: new RequestAwaiter2.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 0 }
                                ),
                            output0: new RequestAwaiter2.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter2.ProcessorConfig(
                            input0: new RequestAwaiter2.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 1 }
                                ),
                            output0:new RequestAwaiter2.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter2.ProcessorConfig(
                            input0: new RequestAwaiter2.ConsumerInfo(
                                topicName: input0Name,
                                partitions: new int[] { 2 }
                                ),
                            output0: new RequestAwaiter2.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            )
                    }
                    );
            Console.WriteLine("Start ReqAwaiter");
            reqAwaiter.Setup(
                reqAwaiterConfitg,
                producerPool0: pool
                );

            reqAwaiter.Start(
                static (config) =>
                {
                    //config.MaxPollIntervalMs = 5_000;
                    //config.SessionTimeoutMs = 2_000;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("ReqAwaiter started");

            return reqAwaiter;
        }

        private static async Task<(RequestAwaiter.Response, long)> Produce(RequestAwaiter reqAwaiter)
        {
            Stopwatch sb = Stopwatch.StartNew();
            using var result = await reqAwaiter.Produce("Hello").ConfigureAwait(false);
            return (result, sb.ElapsedMilliseconds);
        }

        private static async Task<(RequestAwaiter2.Response, long)> Produce2(RequestAwaiter2 reqAwaiter)
        {
            Stopwatch sb = Stopwatch.StartNew();
            using var result = await reqAwaiter.Produce("Hello").ConfigureAwait(false);
            return (result, sb.ElapsedMilliseconds);
        }
    }
}