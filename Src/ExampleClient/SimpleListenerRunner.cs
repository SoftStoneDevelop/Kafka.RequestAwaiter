using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class SimpleListenerRunner
    {
        internal static async Task RunListener(ILoggerFactory loggerFactory)
        {
            var counter = new CountdownEvent(10);
            var simpleListener = CreateListener(loggerFactory, (input) => 
            {
                if(counter.CurrentCount != 0)
                {
                    counter.Signal();
                }
            });

            await ProduceMassages();
            var consoleLog = loggerFactory.CreateLogger<Program>();
            consoleLog.LogInformation("Wait 10 events");
            counter.Wait();
            consoleLog.LogInformation("Waiting done");

            await simpleListener.StopAsync();
        }

        private static TestSimpleListener CreateListener(ILoggerFactory loggerFactory, Action<TestSimpleListener.IncomeMessage> action)
        {
            var simpleListener = new TestSimpleListener(loggerFactory);
            var consumerConfigs = new TestSimpleListener.ConsumerListenerConfig[]
            {
                    new TestSimpleListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerSimpleTopic,
                        new int[] { 0 }
                        ),
                    new TestSimpleListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerSimpleTopic,
                        new int[] { 1 }
                        ),
                    new TestSimpleListener.ConsumerListenerConfig(
                        action,
                        TopicNames.TestListenerSimpleTopic,
                        new int[] { 2 }
                        )
            };

            var configKafka = new TestSimpleListener.ConfigListener(
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
                new ProducerBuilder<string, string>(config)
                .Build()
                ;

            var headerInfo = new ResponseHeader()
            {
            };

            for (int i = 0; i < 10; i++)
            {
                Message<string, string> message = new Message<string, string>();
                message.Headers = new Headers
                {
                    { "Info", headerInfo.ToByteArray() }
                };

                var dr = await producer.ProduceAsync(TopicNames.TestListenerSimpleTopic, message);
            }
        }
    }
}
