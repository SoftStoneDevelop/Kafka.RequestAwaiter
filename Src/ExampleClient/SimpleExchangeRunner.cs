using KafkaExchanger.Common;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class SimpleExchangeRunner
    {
        internal static async Task RunExchange(ILoggerFactory loggerFactory)
        {
            var producerPool = new ProducerPoolNullString(3, "localhost:9194, localhost:9294, localhost:9394");
            var simpleAwaiter = CreateAwaiter(loggerFactory, producerPool);
            var simpleResponder = CreateResponder(loggerFactory, producerPool);

            var consoleLog = loggerFactory.CreateLogger<Program>();
            for (var i = 0; i < 10; i++)
            {
                var result = await simpleAwaiter.Produce($"Value {i}");
                consoleLog.LogWarning($@"


Get result: value:{result.Result.Value}
AllResults: {i + 1}

");
                result.FinishProcessing();
            }

            await simpleResponder.StopAsync();
            await simpleAwaiter.StopAsync();
            producerPool.Dispose();
        }

        private static TestSimpleAwaiter CreateAwaiter(ILoggerFactory loggerFactory, IProducerPoolNullString producerPool)
        {
            var simpleAwaiter = new TestSimpleAwaiter(loggerFactory);
            var consumerConfigs = new TestSimpleAwaiter.ConsumerRequestAwaiterConfig[]
            {
                    new TestSimpleAwaiter.ConsumerRequestAwaiterConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 0 }
                        ),
                    new TestSimpleAwaiter.ConsumerRequestAwaiterConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 1 }
                        ),
                    new TestSimpleAwaiter.ConsumerRequestAwaiterConfig(
                        TopicNames.TestResponseSimpleTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestSimpleAwaiter.ConfigRequestAwaiter(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                TopicNames.TestRequestSimpleTopic,
                consumerConfigs
                );

            simpleAwaiter.Start(configKafka, producerPool);

            return simpleAwaiter;
        }

        private static TestSimpleResponder CreateResponder(ILoggerFactory loggerFactory, IProducerPoolNullString producerPool)
        {
            var simpleResponder = new TestSimpleResponder(loggerFactory);
            var consumerConfigs = new TestSimpleResponder.ConsumerResponderConfig[]
            {
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income, currentState) => Task.FromResult(new TestSimpleResponder.OutcomeMessage(){ Value = $"'{income.Value}' back from 0"}),
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 0 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income, currentState) => Task.FromResult(new TestSimpleResponder.OutcomeMessage(){ Value = $"'{income.Value}' back from 1"}),
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 1 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income, currentState) => Task.FromResult(new TestSimpleResponder.OutcomeMessage(){ Value = $"'{income.Value}' back from 2"}),
                        TopicNames.TestRequestSimpleTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestSimpleResponder.ConfigResponder(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                consumerConfigs
                );

            simpleResponder.Start(configKafka, producerPool);

            return simpleResponder;
        }
    }
}
