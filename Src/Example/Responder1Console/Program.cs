using Confluent.Kafka;
using KafkaExchanger.Attributes;
using System;
using System.Threading.Tasks;

namespace Responder0Console
{
    [Responder(useLogger: false),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
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
            var responder1Config =
                new ResponderOneToOneSimple.ConfigResponder(
                groupId: responderName,
                serviceName: responderName,
                bootstrapServers: bootstrapServers,
                new ResponderOneToOneSimple.ConsumerResponderConfig[]
                {
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Income: {input.Value}");
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            {
                                Value = $"2: Answer {input.Value}"
                            };

                            return Task.FromResult(result);
                        },
                        incomeTopicName: inputName,
                        partitions: new int[] { 0 }
                        ),
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Income: {input.Value}");
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            {
                                Value = $"2: Answer {input.Value}"
                            };

                            return Task.FromResult(result);
                        },
                        incomeTopicName: inputName,
                        partitions: new int[] { 1 }
                        ),
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) =>
                        {
                            Console.WriteLine($"Income: {input.Value}");
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            {
                                Value = $"2: Answer {input.Value}"
                            };

                            return Task.FromResult(result);
                        },
                        incomeTopicName: inputName,
                        partitions: new int[] { 2 }
                        )
                }
                );

            var pool = new KafkaExchanger.Common.ProducerPoolNullString(
                5,
                bootstrapServers,
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            Console.WriteLine("Start Responder");
            responder2.Start(config: responder1Config, producerPool: pool);
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
        }
    }
}