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
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiter
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

            var responderName0 = "RAResponder1";
            var responderName1 = "RAResponder2";

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

            using var reqAwaiter = new RequestAwaiter();
            var reqAwaiterConfitg =
                new RequestAwaiter.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: bootstrapServers,
                    processors: new RequestAwaiter.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiter.ProcessorConfig(
                            income0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                canAnswerService: new [] { responderName0 },
                                partitions: new int[] { 0 }
                                ),
                            income1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                canAnswerService: new [] { responderName1 },
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            income0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                canAnswerService: new [] { responderName0 },
                                partitions: new int[] { 1 }
                                ),
                            income1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                canAnswerService: new [] { responderName1 },
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            ),
                        new RequestAwaiter.ProcessorConfig(
                            income0: new RequestAwaiter.ConsumerInfo(
                                topicName: input0Name,
                                canAnswerService: new [] { responderName0 },
                                partitions: new int[] { 2 }
                                ),
                            income1: new RequestAwaiter.ConsumerInfo(
                                topicName: input1Name,
                                canAnswerService: new [] { responderName1 },
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiter.ProducerInfo(outputName),
                            buckets: 2,
                            maxInFly: 100
                            )
                    }
                    );
            Console.WriteLine("Start ReqAwaiter");
            reqAwaiter.Start(
                reqAwaiterConfitg, 
                producerPool0: pool,
                static (config) =>
                {
                    //config.MaxPollIntervalMs = 5_000;
                    //config.SessionTimeoutMs = 2_000;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("ReqAwaiter started");

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
                var tasks = new Task<(RequestAwaiterConsole.BaseResponse[], long)>[requests];
                for (int i = 0; i < requests; i++)
                {
                    tasks[i] = Produce(reqAwaiter);
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
                    var result = task.Result.Item2;
                    if(hashSet.TryGetValue(result, out var internalCount))
                    {
                        hashSet[result] = ++internalCount;
                    }
                    else
                    {
                        hashSet[result] = 1;
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

        private static async Task<(RequestAwaiterConsole.BaseResponse[], long)> Produce(RequestAwaiter reqAwaiter)
        {
            Stopwatch sb = Stopwatch.StartNew();
            using var result = await reqAwaiter.Produce("Hello").ConfigureAwait(false);
            return (result.Result, sb.ElapsedMilliseconds);
        }
    }
}