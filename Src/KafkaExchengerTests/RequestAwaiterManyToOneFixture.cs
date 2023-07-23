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
    internal class RequestAwaiterManyToOneFixture
    {
        private static string _inputSimpleTopic1 = "RAManyToOneInputSimple1";
        private static string _inputSimpleTopic2 = "RAManyToOneInputSimple2";
        private static string _outputSimpleTopic = "RAManyToOneOutputSimple";

        private static string _inputProtobuffTopic1 = "RAManyToOneInputProtobuff1";
        private static string _inputProtobuffTopic2 = "RAManyToOneInputProtobuff2";
        private static string _outputProtobuffTopic = "RAManyToOneOutputProtobuff";

        [SetUp]
        public async Task Setup()
        {
            var config = new AdminClientConfig
            {
                BootstrapServers = GlobalSetUp.Configuration["BootstrapServers"]
            };

            using (var adminClient = new AdminClientBuilder(config).Build())
            {
                await CreateTopic(adminClient, _inputSimpleTopic1);
                await CreateTopic(adminClient, _inputSimpleTopic2);
                await CreateTopic(adminClient, _outputSimpleTopic);

                await CreateTopic(adminClient, _inputProtobuffTopic1);
                await CreateTopic(adminClient, _inputProtobuffTopic2);
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
                        _inputSimpleTopic1,
                        _inputSimpleTopic2,
                        _outputSimpleTopic,

                        _inputProtobuffTopic1,
                        _inputProtobuffTopic2,
                        _outputProtobuffTopic,
                    });
            }
        }

        [Test]
        public async Task SimpleProduce()
        {
            var pool = new ProducerPoolNullString(3, GlobalSetUp.Configuration["BootstrapServers"]);
            var reqAwaiter = new RequestAwaiterManyToOneSimple();
            var reqAwaiterConfitg = 
                new RequestAwaiterManyToOneSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                    outcomeTopicName: _outputSimpleTopic,
                    consumers: new RequestAwaiterManyToOneSimple.Consumer[] 
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterManyToOneSimple.Consumer(
                            groupName: "SimpleProduce", 
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 0 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 0 }
                                ),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterManyToOneSimple.Consumer(
                            groupName: "SimpleProduce",
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 1 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 1 }
                                ),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterManyToOneSimple.Consumer(
                            groupName: "SimpleProduce",
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 2 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new string[] { "ResponderOneToOne" },
                                partitions: new int[] { 2 }
                                ),
                            buckets: 2,
                            maxInFly: 10
                            )
                    }
                    );
            reqAwaiter.Start(reqAwaiterConfitg, producerPool0: pool);

            var responder1 = new ResponderOneToOneSimple();
            ResponderOneToOneSimple.OutcomeMessage expect1Answer = null;
            var responder1Config = new ResponderOneToOneSimple.ConfigResponder(
                groupId: "SimpleProduce1", 
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                new ResponderOneToOneSimple.ConsumerResponderConfig[] 
                {
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) => 
                        {
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            { 
                                Value = $"1: Answer from {input.Partition.Value} {input.Value}" 
                            };
                            expect1Answer = result;

                            return Task.FromResult(result); 
                        },
                        incomeTopicName: _outputSimpleTopic, 
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );
            responder1.Start(config: responder1Config, producerPool: pool);

            var responder2 = new ResponderOneToOneSimple();
            ResponderOneToOneSimple.OutcomeMessage expect2Answer = null;
            var responder2Config = new ResponderOneToOneSimple.ConfigResponder(
                groupId: "SimpleProduce2",
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                new ResponderOneToOneSimple.ConsumerResponderConfig[]
                {
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: (input, s) =>
                        {
                            var result = new ResponderOneToOneSimple.OutcomeMessage()
                            {
                                Value = $"2: Answer from {input.Partition.Value} {input.Value}"
                            };
                            expect2Answer = result;

                            return Task.FromResult(result);
                        },
                        incomeTopicName: _outputSimpleTopic,
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );
            responder2.Start(config: responder2Config, producerPool: pool);

            var answer = await reqAwaiter.Produce("Hello");
            Assert.That(answer.Result, Has.Length.EqualTo(2));

            ResponseItem<RequestAwaiterManyToOneSimple.ResponseTopic0Message> answerFrom1 = null;
            ResponseItem<RequestAwaiterManyToOneSimple.ResponseTopic1Message> answerFrom2 = null;
            for (int i = 0; i < answer.Result.Length; i++)
            {
                Assert.That(answerFrom1 == null || answerFrom2 == null, Is.True);
                var result = answer.Result[i];
                if(result is ResponseItem<RequestAwaiterManyToOneSimple.ResponseTopic0Message> resultFrom1)
                {
                    answerFrom1 = resultFrom1;
                }

                if (result is ResponseItem<RequestAwaiterManyToOneSimple.ResponseTopic1Message> resultFrom2)
                {
                    answerFrom2 = resultFrom2;
                }
            }

            Assert.That(answerFrom1, Is.Not.Null);
            Assert.That(answerFrom1.Result, Is.Not.Null);
            Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
            Assert.That(answerFrom1.Result.Value, Is.EqualTo(expect1Answer.Value));

            Assert.That(answerFrom2, Is.Not.Null);
            Assert.That(answerFrom2.Result, Is.Not.Null);
            Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
            Assert.That(answerFrom2.Result.Value, Is.EqualTo(expect2Answer.Value));

            await reqAwaiter.StopAsync();
            await responder1.StopAsync();
            await responder2.StopAsync();
        }
    }
}