using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class ProtobuffExchangeRunner
    {
        internal static async Task RunExchange(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = CreateAwaiter(loggerFactory);
            var simpleResponder = CreateResponder(loggerFactory);

            var consoleLog = loggerFactory.CreateLogger<Program>();
            for (var i = 0; i < 10; i++)
            {
                var result = await simpleAwaiter.Produce(
                    new protobuff.SimpleKey() { Id = i  },
                    new protobuff.SimpleValue() { Id = i, Priority = protobuff.Priority.Unspecified, Message = $"Value {i}" });
                consoleLog.LogWarning($@"


Get result: key:{result.Result.Key}, value:{result.Result.Value}
AllResults: {i + 1}

");
                result.FinishProcessing();
            }

            await simpleResponder.StopAsync();
            await simpleAwaiter.StopAsync();
        }

        private static TestProtobuffAwaiter CreateAwaiter(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = new TestProtobuffAwaiter(loggerFactory);
            var consumerConfigs = new KafkaExchanger.Common.ConsumerConfig[]
            {
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseProtobuffTopic,
                        new int[] { 0 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseProtobuffTopic,
                        new int[] { 1 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseProtobuffTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new KafkaExchanger.Common.ConfigRequestAwaiter(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                TopicNames.TestRequestProtobuffTopic,
                consumerConfigs
                );

            simpleAwaiter.Start(configKafka);

            return simpleAwaiter;
        }

        private static TestProtobuffResponder CreateResponder(ILoggerFactory loggerFactory)
        {
            var simpleResponder = new TestProtobuffResponder(loggerFactory);
            var consumerConfigs = new TestProtobuffResponder.ConsumerResponderConfig[]
            {
                    new TestProtobuffResponder.ConsumerResponderConfig(
                        (income) =>
                        new TestProtobuffResponder.OutcomeMessage()
                        {
                            Key = new protobuff.SimpleKey()
                            {
                                Id = income.Key.Id
                            },
                            Value =
                            new protobuff.SimpleValue()
                            {
                                Id = income.Key.Id,
                                Priority = protobuff.Priority.White,
                                Message = $"'{income.Value.Message}' back from 0"
                            }
                        },
                        TopicNames.TestRequestProtobuffTopic,
                        new int[] { 0 }
                        ),
                    new TestProtobuffResponder.ConsumerResponderConfig(
                        (income) =>
                        new TestProtobuffResponder.OutcomeMessage()
                        {
                            Key = new protobuff.SimpleKey()
                            {
                                Id = income.Key.Id
                            },
                            Value =
                            new protobuff.SimpleValue()
                            {
                                Id = income.Key.Id,
                                Priority = protobuff.Priority.Yellow,
                                Message = $"'{income.Value.Message}' back from 1"
                            }
                        },
                        TopicNames.TestRequestProtobuffTopic,
                        new int[] { 1 }
                        ),
                    new TestProtobuffResponder.ConsumerResponderConfig(
                        (income) => 
                        new TestProtobuffResponder.OutcomeMessage()
                        { 
                            Key = new protobuff.SimpleKey() 
                            { 
                                Id = income.Key.Id 
                            }, 
                            Value = 
                            new protobuff.SimpleValue() 
                            { 
                                Id = income.Key.Id, 
                                Priority = protobuff.Priority.Red, 
                                Message = $"'{income.Value.Message}' back from 2" 
                            }
                        },
                        TopicNames.TestRequestProtobuffTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestProtobuffResponder.ConfigResponder(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                consumerConfigs
                );

            simpleResponder.Start(configKafka);

            return simpleResponder;
        }
    }
}
