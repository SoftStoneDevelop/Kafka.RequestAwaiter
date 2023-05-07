using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class SimpleExchangeRunner
    {
        internal static async Task RunExchange(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = CreateAwaiter(loggerFactory);
            var simpleResponder = CreateResponder(loggerFactory);

            var consoleLog = loggerFactory.CreateLogger<Program>();
            for (var i = 0; i < 10; i++)
            {
                var result = await simpleAwaiter.Produce($"Key {i}", $"Value {i}");
                consoleLog.LogWarning($@"


Get result: key:{result.Result.Key}, value:{result.Result.Value}
AllResults: {i + 1}

");
                result.FinishProcessing();
            }

            await simpleResponder.StopAsync();
            await simpleAwaiter.StopAsync();
        }

        private static TestSimpleAwaiter CreateAwaiter(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = new TestSimpleAwaiter(loggerFactory);
            var consumerConfigs = new KafkaExchanger.Common.ConsumerConfig[]
            {
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 0, 1, 2 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 1 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new KafkaExchanger.Common.ConfigRequestAwaiter(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                TopicNames.TestRequestSimpleTopic,
                consumerConfigs
                );

            simpleAwaiter.Start(configKafka);

            return simpleAwaiter;
        }

        private static TestSimpleResponder CreateResponder(ILoggerFactory loggerFactory)
        {
            var simpleResponder = new TestSimpleResponder(loggerFactory);
            var consumerConfigs = new TestSimpleResponder.ConsumerResponderConfig[]
            {
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 0"},
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 0 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 1"},
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 1 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 2"},
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestSimpleResponder.ConfigResponder(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                consumerConfigs
                );

            simpleResponder.Start(configKafka);

            return simpleResponder;
        }
    }
}
