using Confluent.Kafka;
using KafkaExchanger.Attributes;
using System;
using System.Diagnostics;
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

            var pool = new KafkaExchanger.Common.ProducerPoolNullString(5, bootstrapServers);
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
                            maxInFly: 50
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
                            maxInFly: 50
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
                            maxInFly: 50
                            )
                    }
                    );
            reqAwaiter.Start(reqAwaiterConfitg, producerPool0: pool);

            Stopwatch sb = Stopwatch.StartNew();
            for (int i = 0; i < 1000; i++)
            {
                var result = await reqAwaiter.Produce("Hello");
                Console.WriteLine("Result " + i + ":");
                Console.WriteLine(((ResponseItem<RequestAwaiter.Income0Message>)result.Result[0]).Result.Value);
                Console.WriteLine(((ResponseItem<RequestAwaiter.Income1Message>)result.Result[1]).Result.Value);
                Console.WriteLine();
                Console.WriteLine();
            }
            sb.Stop();
            Console.WriteLine($"Time: {sb.Elapsed}");

            while ( true )
            {
                var read = Console.ReadLine();
                if(read == "exit")
                {
                    break;
                }
            }
        }
    }
}