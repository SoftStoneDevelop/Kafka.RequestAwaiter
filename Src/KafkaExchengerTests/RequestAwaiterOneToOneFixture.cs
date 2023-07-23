using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaExchanger.Common;
using NUnit.Framework;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaExchengerTests
{
    [TestFixture]
    [Parallelizable(ParallelScope.Self)]
    internal class RequestAwaiterOneToOneFixture
    {
        private static string _inputSimpleTopic = "RAOneToOneInputSimple";
        private static string _outputSimpleTopic = "RAOneToOneOutputSimple";

        private static string _inputProtobuffTopic = "RAOneToOneInputProtobuff";
        private static string _outputProtobuffTopic = "RAOneToOneOutputProtobuff";

        [SetUp]
        public async Task Setup()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = GlobalSetUp.Configuration["BootstrapServers"]
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                await CreateTopic(adminClient, _inputSimpleTopic);
                await CreateTopic(adminClient, _outputSimpleTopic);

                await CreateTopic(adminClient, _inputProtobuffTopic);
                await CreateTopic(adminClient, _outputProtobuffTopic);
            }
        }

        private async Task CreateTopic(IAdminClient adminClient, string topicName)
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
            if(metadata.Topics.Any(an => an.Topic == topicName))
            {
                throw new Exception("Duplicate, test result corrupted");
            }

            try
            {
                await adminClient.CreateTopicsAsync(new TopicSpecification[]
                    {
                                new TopicSpecification
                                {
                                    Name = topicName,
                                    ReplicationFactor = 1,
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

        [TearDown]
        public async Task TearDown()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = GlobalSetUp.Configuration["BootstrapServers"]
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                await adminClient.DeleteTopicsAsync(
                    new string[] 
                    { 
                        _inputSimpleTopic,
                        _outputSimpleTopic,

                        _inputProtobuffTopic, 
                        _outputProtobuffTopic,
                    });
            }
        }

        [Test]
        public async Task SimpleProduce()
        {
            var pool = new ProducerPoolNullString(2, GlobalSetUp.Configuration["BootstrapServers"]);
            var reqAwaiter = new RequestAwaiterOneToOneSimple();
            var reqAwaiterConfitg = 
                new RequestAwaiterOneToOneSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                    processors: new RequestAwaiterOneToOneSimple.ProcessorConfig[] 
                    {
                        new RequestAwaiterOneToOneSimple.ProcessorConfig(
                            groupName: "SimpleProduce", 
                            income0: new RequestAwaiterOneToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic, 
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterOneToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterOneToOneSimple.ProcessorConfig(
                            groupName: "SimpleProduce",
                            income0: new RequestAwaiterOneToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterOneToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterOneToOneSimple.ProcessorConfig(
                            groupName: "SimpleProduce",
                            income0: new RequestAwaiterOneToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterOneToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            )
                    }
                    );
            reqAwaiter.Start(reqAwaiterConfitg, producerPool0: pool);

            var responder = new ResponderOneToOneSimple();
            ResponderOneToOneSimple.OutcomeMessage expectAnswer = null;
            var responderConfig = new ResponderOneToOneSimple.ConfigResponder(
                groupId: "SimpleProduce", 
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                new ResponderOneToOneSimple.ConsumerResponderConfig[] 
                {
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) => 
                        {
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            { 
                                Value = $"Answer from {input.Partition.Value} {input.Value}" 
                            };
                            expectAnswer = result;

                            return Task.FromResult(result); 
                        },
                        incomeTopicName: _outputSimpleTopic, 
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );
            responder.Start(config: responderConfig, producerPool: pool);

            var answer = await reqAwaiter.Produce("Hello");
            Assert.That(answer.Result, Has.Length.EqualTo(1));
            Assert.That(answer.Result[0].TopicName, Is.EqualTo(_inputSimpleTopic));
            var result = answer.Result[0] as ResponseItem<RequestAwaiterOneToOneSimple.Income0Message>;
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Result, Is.Not.Null);
            Assert.That(result.Result.Value, Is.EqualTo(expectAnswer.Value));

            answer = await reqAwaiter.Produce("Hello again");
            Assert.That(answer.Result, Has.Length.EqualTo(1));
            Assert.That(answer.Result[0].TopicName, Is.EqualTo(_inputSimpleTopic));
            result = answer.Result[0] as ResponseItem<RequestAwaiterOneToOneSimple.Income0Message>;
            Assert.That(result, Is.Not.Null);
            Assert.That(result.Result, Is.Not.Null);
            Assert.That(result.Result.Value, Is.EqualTo(expectAnswer.Value));

            await reqAwaiter.StopAsync();
            await responder.StopAsync();
        }
    }
}