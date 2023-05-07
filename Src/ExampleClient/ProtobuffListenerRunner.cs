using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class ProtobuffListenerRunner
    {
        internal static async Task RunListener(ILoggerFactory loggerFactory)
        {
            var counter = new CountdownEvent(10);
            var simpleListener = CreateListener(loggerFactory, (input) => counter.Signal());

            await ProduceMassages();
            var consoleLog = loggerFactory.CreateLogger<Program>();
            consoleLog.LogInformation("Wait 10 events");
            counter.Wait();
            consoleLog.LogInformation("Waiting done");

            await simpleListener.StopAsync();
        }

        private static TestProtobuffListener CreateListener(ILoggerFactory loggerFactory, Action<TestProtobuffListener.IncomeMessage> action)
        {
            var simpleListener = new TestProtobuffListener(loggerFactory);
            var consumerConfigs = new TestProtobuffListener.ConsumerListenerConfig[]
            {
                    new TestProtobuffListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerProtobuffTopic,
                        new int[] { 0 }
                        ),
                    new TestProtobuffListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerProtobuffTopic,
                        new int[] { 1 }
                        ),
                    new TestProtobuffListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerProtobuffTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestProtobuffListener.ConfigListener(
                "TestGroup",
                "localhost:9194, localhost:9294, localhost:9394",
                consumerConfigs
                );

            simpleListener.Start(configKafka);

            return simpleListener;
        }

        private static async Task ProduceMassages()
        {
            var config = new ProducerConfig
            {
                BootstrapServers = "localhost:9194, localhost:9294, localhost:9394",
                AllowAutoCreateTopics = false
            };

            var producer =
                new ProducerBuilder<byte[], byte[]>(config)
                .Build()
                ;

            var headerInfo = new kafka.ResponseHeader()
            {
            };

            for (int i = 0; i < 10; i++)
            {
                Message<byte[], byte[]> message = new Message<byte[], byte[]>();
                message.Key = new protobuff.SimpleKey() { Id = i }.ToByteArray();
                message.Value = new protobuff.SimpleValue() { Id = i, Priority = protobuff.Priority.Red, Message = $"Hello protobuff {i}" }.ToByteArray();
                message.Headers = new Headers
                {
                    { "Info", headerInfo.ToByteArray() }
                };

                var dr = await producer.ProduceAsync(TopicNames.TestListenerProtobuffTopic, message);
            }
        }
    }
}
