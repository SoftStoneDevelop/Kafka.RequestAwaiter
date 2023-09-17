using Confluent.Kafka;
using KafkaExchanger.Attributes;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Responder1Console
{
    [Responder(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(GrcpService.HelloRequest)),
        Output(keyType: typeof(Null), valueType: typeof(GrcpService.HelloResponse))
        ]
    public partial class ResponderOneToOneSimple
    {

    }

    internal class Program
    {
        static async Task Main(string[] args)
        {
            var bootstrapServers = "localhost:9194, localhost:9294, localhost:9394";
            var inputName = "RAOutputSimple";
            var responderName = "RAResponder2";

            var responder2 = new ResponderOneToOneSimple();
            var partitions = 3;
            var processors = new ResponderOneToOneSimple.ProcessorConfig[partitions];
            for (int i = 0; i < processors.Length; i++)
            {
                processors[i] =
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Input: {input.Input0Message.Value}");
                            var result = new ResponderOneToOneSimple.OutputMessage()
                            {
                                Output0Message = new ResponderOneToOneSimple.Output0Message()
                                {
                                    Value = new GrcpService.HelloResponse { Text = $"1: Answer {input.Input0Message.Value}" }
                                }
                            };

                            return Task.FromResult(result);
                        },
                        input0: new ResponderOneToOneSimple.ConsumerInfo(inputName, new int[] { i })
                        );
            }

            var responder1Config =
                new ResponderOneToOneSimple.Config(
                groupId: responderName,
                serviceName: responderName,
                bootstrapServers: bootstrapServers,
                itemsInBucket: 1000,
                inFlyLimit: 5,
                addNewBucket: static (bucketId, partitions, topicName) => { return ValueTask.CompletedTask; },
                bucketsCount: (partitions, topicName) => { return ValueTask.FromResult(5); },
                processors: processors
                );

            var pool = new KafkaExchanger.ProducerPool<Confluent.Kafka.Null, byte[]>(
                new HashSet<string> { "Responder1Console0", "Responder1Console1" },
                bootstrapServers,
                changeConfig: static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("Start Responder");
            await responder2.Setup(config: responder1Config, output0Pool: pool);
            responder2.Start();
            Console.WriteLine("Responder started");

            while (true)
            {
                var read = Console.ReadLine();
                if (read == "exit")
                {
                    break;
                }
            }

            await responder2.StopAsync();
            await pool.DisposeAsync();
        }
    }
}