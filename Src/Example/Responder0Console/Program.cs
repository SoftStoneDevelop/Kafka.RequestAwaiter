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
            var responderName = "RAResponder1";

            var responder1 = new ResponderOneToOneSimple();
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
                    var result = new ResponderOneToOneSimple.OutcomeMessage()
                    {
                        Value = $"1: Answer {input.Value}"
                    };

                    return Task.FromResult(result);
                },
                        incomeTopicName: inputName,
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );

            var pool = new KafkaExchanger.Common.ProducerPoolNullString(5, bootstrapServers);
            responder1.Start(config: responder1Config, producerPool: pool);

            while (true)
            {
                var read = Console.ReadLine();
                if (read == "exit")
                {
                    break;
                }
            }

            await responder1.StopAsync();
        }
    }
}