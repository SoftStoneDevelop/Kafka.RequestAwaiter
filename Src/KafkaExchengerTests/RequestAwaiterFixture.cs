using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaExchanger.Attributes.Enums;
using KafkaExchanger.Common;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace KafkaExchengerTests
{
    [TestFixture]
    [Parallelizable(ParallelScope.Self)]
    internal class RequestAwaiterFixture
    {
        private static string _inputSimpleTopic1 = "RAInputSimple1";
        private static string _inputSimpleTopic2 = "RAInputSimple2";
        private static string _outputSimpleTopic = "RAOutputSimple";

        private static string _inputProtobuffTopic1 = "RAInputProtobuff1";
        private static string _inputProtobuffTopic2 = "RAInputProtobuff2";
        private static string _outputProtobuffTopic = "RAOutputProtobuff";

        private static string _responderService1 = "RAResponder1";
        private static string _responderService2 = "RAResponder2";

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

        private async Task CreateTopic(IAdminClient adminClient, string topicName)
        {
            var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
            if(metadata.Topics.Any(an => an.Topic == topicName))
            {
                return;
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

        private ResponderOneToOneSimple.ConfigResponder CreateResponderConfig(
            string groupId,
            string serviceName,
            Func<ResponderOneToOneSimple.IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderOneToOneSimple.OutcomeMessage>> createAnswer
            )
        {
            return
                new ResponderOneToOneSimple.ConfigResponder(
                groupId: groupId,
                serviceName: serviceName,
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                new ResponderOneToOneSimple.ConsumerResponderConfig[]
                {
                    new ResponderOneToOneSimple.ConsumerResponderConfig(
                        createAnswer: createAnswer,
                        incomeTopicName: _outputSimpleTopic,
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );
        }

        [Test]
        public void CancelByTimeout()
        {
            var pool = new ProducerPoolNullString(3, GlobalSetUp.Configuration["BootstrapServers"],
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            using var reqAwaiter = new RequestAwaiterManyToOneSimple();
            var reqAwaiterConfitg =
                new RequestAwaiterManyToOneSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                    processors: new RequestAwaiterManyToOneSimple.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 0 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new [] { _responderService2 },
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 1 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new[] { _responderService2 },
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            ),
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 2 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new [] { _responderService2 },
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10
                            )
                    }
                    );
            reqAwaiter.Start(
                reqAwaiterConfitg, 
                producerPool0: pool,
                static (config) =>
                {
                    config.MaxPollIntervalMs = 10_000;
                    config.SessionTimeoutMs = 5_000;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            
            var sw = Stopwatch.StartNew();
            var tasks = new Task[120];
            for (int i = 0; i < 120; i++)
            {
                tasks[i] = reqAwaiter.Produce("Hello", 1_000);
            }

            foreach (var task in tasks)
            {
                Assert.ThrowsAsync<TaskCanceledException>(async () => { await task; });
            }
        }

        [Test]
        public async Task SimpleProduce()
        {
            var pool = new ProducerPoolNullString(5, GlobalSetUp.Configuration["BootstrapServers"],
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                });
            using var reqAwaiter = new RequestAwaiterManyToOneSimple();
            var reqAwaiterConfitg = 
                new RequestAwaiterManyToOneSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                    processors: new RequestAwaiterManyToOneSimple.ProcessorConfig[] 
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 0 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new [] { _responderService2 },
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50
                            ),
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 1 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new [] { _responderService2 },
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50
                            ),
                        new RequestAwaiterManyToOneSimple.ProcessorConfig(
                            income0: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                canAnswerService: new [] { _responderService1 },
                                partitions: new int[] { 2 }
                                ),
                            income1: new RequestAwaiterManyToOneSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                canAnswerService: new [] { _responderService2 },
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterManyToOneSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50
                            )
                    }
                    );
            reqAwaiter.Start(
                reqAwaiterConfitg, 
                producerPool0: pool,
                static (config) =>
                {
                    config.MaxPollIntervalMs = 10_000;
                    config.SessionTimeoutMs = 5_000;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );

            var responder1 = new ResponderOneToOneSimple();
            var responder1Config = CreateResponderConfig(
                "RAResponder1",
                "RAResponder1",
                (input, s) =>
                {
                    var result = new ResponderOneToOneSimple.OutcomeMessage()
                    {
                        Value = $"1: Answer {input.Value}"
                    };

                    return Task.FromResult(result);
                });
            responder1.Start(config: responder1Config, producerPool: pool);

            var responder2 = new ResponderOneToOneSimple();
            var responder2Config = CreateResponderConfig(
                "RAResponder2",
                "RAResponder2",
                (input, s) =>
                {
                    var result = new ResponderOneToOneSimple.OutcomeMessage()
                    {
                        Value = $"2: Answer {input.Value}"
                    };

                    return Task.FromResult(result);
                });
            responder2.Start(config: responder2Config, producerPool: pool);

            var requestsCount = 1000;
            var answers = new Task<(BaseResponse[], CurrentState)>[requestsCount];
            for (int i = 0; i < requestsCount; i++)
            {
                var message = $"Hello{i}";
                answers[i] = produce(message);

                async Task<(BaseResponse[], CurrentState)> produce(string message)
                {
                    using var result = await reqAwaiter.Produce(message).ConfigureAwait(false);
                    var state = result.CurrentState;
                    var response = result.Result;

                    return (response, state);
                }
            }

            var unique1 = new HashSet<string>(requestsCount);
            var unique2 = new HashSet<string>(requestsCount);

            Task.WaitAll(answers);
            for (int i = 0; i < requestsCount; i++)
            {
                (BaseResponse[] result, CurrentState state) result = await answers[i];
                Assert.Multiple(() =>
                {
                    Assert.That(result.result.Count, Is.EqualTo(2));
                    Assert.That(result.state, Is.EqualTo(CurrentState.NewMessage));
                });

                var answerFrom1 = result.result[0] as ResponseItem<RequestAwaiterManyToOneSimple.Income0Message>;
                Assert.That(answerFrom1 != null, Is.True);
                Assert.That(answerFrom1.Result != null, Is.True);
                Assert.Multiple(() => 
                {
                    Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                    Assert.That(answerFrom1.Result.Value, Is.EqualTo($"1: Answer Hello{i}"));
                    Assert.That(unique1.Add(answerFrom1.Result.Value), Is.True);
                });

                var answerFrom2 = result.result[1] as ResponseItem<RequestAwaiterManyToOneSimple.Income1Message>;
                Assert.That(answerFrom2 != null, Is.True);
                Assert.That(answerFrom2.Result != null, Is.True);
                Assert.Multiple(() =>
                {
                    Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
                    Assert.That(answerFrom2.Result.Value, Is.EqualTo($"2: Answer Hello{i}"));
                    Assert.That(unique2.Add(answerFrom2.Result.Value), Is.True);
                });
            }

            Assert.That(unique1.Count, Is.EqualTo(requestsCount));
            Assert.That(unique2.Count, Is.EqualTo(requestsCount));

            await responder1.StopAsync();
            await responder2.StopAsync();
        }
    }
}