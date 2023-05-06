using Confluent.Kafka.Admin;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Threading.Tasks;
using System.Linq;

namespace ExampleClient
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var loggerFactory = LoggerFactory.Create(builder => builder.AddSimpleConsole());

            await CreateSimpleTopics();
            await RunSimpleExchange(loggerFactory);

            for (int i = 0; i < 30; i++)
            {
                Console.WriteLine();
            }

            //RunProtobufExchange(loggerFactory);
        }

        private static async Task RunSimpleExchange(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = CreateSimpleAwaiter(loggerFactory);
            var simpleResponder = CreateSimpleResponder(loggerFactory);

            var consoleLog = loggerFactory.CreateLogger<Program>();
            for (var i = 0; i < 1; i++)
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

        private static TestSimpleAwaiter CreateSimpleAwaiter(ILoggerFactory loggerFactory)
        {
            var simpleAwaiter = new TestSimpleAwaiter(loggerFactory);
            var consumerConfigs = new KafkaExchanger.Common.ConsumerConfig[]
            {
                    new KafkaExchanger.Common.ConsumerConfig(
                        "TestResponseSimpleTopic",
                        new int[] { 0 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        "TestResponseSimpleTopic",
                        new int[] { 1 }
                        ),
                    new KafkaExchanger.Common.ConsumerConfig(
                        "TestResponseSimpleTopic",
                        new int[] { 2 }
                        )
            };

            var configKafka = new KafkaExchanger.Common.ConfigRequestAwaiter(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                "TestRequestSimpleTopic",
                consumerConfigs
                );

            simpleAwaiter.Start(configKafka);

            return simpleAwaiter;
        }

        private static TestSimpleResponder CreateSimpleResponder(ILoggerFactory loggerFactory)
        {
            var simpleResponder = new TestSimpleResponder(loggerFactory);
            var consumerConfigs = new TestSimpleResponder.ConsumerResponderConfig[]
            {
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 0"},
                        "TestRequestSimpleTopic",
                        new int[] { 0 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 1"},
                        "TestRequestSimpleTopic",
                        new int[] { 1 }
                        ),
                    new TestSimpleResponder.ConsumerResponderConfig(
                        (income) => new TestSimpleResponder.OutcomeMessage(){ Key = $"{income.Key}", Value = $"'{income.Value}' back from 2"},
                        "TestRequestSimpleTopic",
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

        private static async Task CreateSimpleTopics()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = "localhost:9194, localhost:9294, localhost:9394"
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(5));
                if (metadata.Topics.All(an => an.Topic != "TestRequestSimpleTopic"))
                {
                    try
                    {
                        await adminClient.CreateTopicsAsync(new TopicSpecification[]
                            {
                                new TopicSpecification
                                {
                                    Name = "TestRequestSimpleTopic",
                                    ReplicationFactor = 3,
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

                if (metadata.Topics.All(an => an.Topic != "TestResponseSimpleTopic"))
                {
                    try
                    {
                        await adminClient.CreateTopicsAsync(new TopicSpecification[]
                            {
                                new TopicSpecification
                                {
                                    Name = "TestResponseSimpleTopic",
                                    ReplicationFactor = 3,
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
            }
        }
    }
}