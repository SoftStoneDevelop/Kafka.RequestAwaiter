using Confluent.Kafka;
using Confluent.Kafka.Admin;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace ExampleClient
{
    internal static class TopicCreators
    {
        internal static async Task CreateSimpleTopics()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = "localhost:9194, localhost:9294, localhost:9394"
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                await CreateTopic(adminClient, TopicNames.TestRequestSimpleTopic);
                await CreateTopic(adminClient, TopicNames.TestResponseSimpleTopic);
                await CreateTopic(adminClient, TopicNames.TestListenerSimpleTopic);
            }
        }

        internal static async Task CreateProtobuffTopics()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = "localhost:9194, localhost:9294, localhost:9394"
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                await CreateTopic(adminClient, TopicNames.TestRequestProtobuffTopic);
                await CreateTopic(adminClient, TopicNames.TestResponseProtobuffTopic);
                await CreateTopic(adminClient, TopicNames.TestListenerProtobuffTopic);
            }
        }

        private static async Task CreateTopic(IAdminClient adminClient, string topicName)
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
            if (metadata.Topics.All(an => an.Topic != topicName))
            {
                try
                {
                    await adminClient.CreateTopicsAsync(new TopicSpecification[]
                        {
                                new TopicSpecification
                                {
                                    Name = topicName,
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
