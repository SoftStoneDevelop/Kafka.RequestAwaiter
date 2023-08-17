using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaExchanger.Attributes.Enums;
using KafkaExchanger.Common;
using NUnit.Framework;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
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
                throw new Exception("Duplicate");
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
            Func<ResponderOneToOneSimple.InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderOneToOneSimple.OutputMessage>> createAnswer
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
                        inputTopicName: _outputSimpleTopic,
                        partitions: new int[] { 0, 1, 2 }
                        )
                }
                );
        }

        [Test]
        public async Task CancelByTimeout()
        {
            var pool = new ProducerPoolNullString(3, GlobalSetUp.Configuration["BootstrapServers"],
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                }
                );
            await using var reqAwaiter = new RequestAwaiterSimple();
            var reqAwaiterConfitg =
                new RequestAwaiterSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                    processors: new RequestAwaiterSimple.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 0 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 1 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 2 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 10,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            )
                    }
                    );
            reqAwaiter.Setup(
                config: reqAwaiterConfitg,
                producerPool0: pool
                );

            reqAwaiter.Start(
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
                tasks[i] = reqAwaiter.Produce("Hello", 1_000).AsTask();
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
            await using var reqAwaiter = new RequestAwaiterSimple();
            var reqAwaiterConfitg = 
                new RequestAwaiterSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"], 
                    processors: new RequestAwaiterSimple.ProcessorConfig[] 
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 0 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 1 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 2 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50,
                            afterSendOutput0: (header, message) =>
                            {
                                return Task.CompletedTask;
                            }
                            )
                    }
                    );

            reqAwaiter.Setup(
                config: reqAwaiterConfitg,
                producerPool0: pool
                );

            reqAwaiter.Start(
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
                    var result = new ResponderOneToOneSimple.OutputMessage()
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
                    var result = new ResponderOneToOneSimple.OutputMessage()
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

                var answerFrom1 = result.result[0] as ResponseItem<RequestAwaiterSimple.Input0Message>;
                Assert.That(answerFrom1 != null, Is.True);
                Assert.That(answerFrom1.Result != null, Is.True);
                Assert.Multiple(() => 
                {
                    Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                    Assert.That(answerFrom1.Result.Value, Is.EqualTo($"1: Answer Hello{i}"));
                    Assert.That(unique1.Add(answerFrom1.Result.Value), Is.True);
                });

                var answerFrom2 = result.result[1] as ResponseItem<RequestAwaiterSimple.Input1Message>;
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

        private class AddAwaiterInfo
        {
            public KafkaExchengerTests.RequestHeader Header;
            public string Value;
        }

        [Test]
        public async Task AddAwaiter()
        {
            var pool = new ProducerPoolNullString(5, GlobalSetUp.Configuration["BootstrapServers"],
                static (config) =>
                {
                    config.LingerMs = 2;
                    config.SocketKeepaliveEnable = true;
                    config.AllowAutoCreateTopics = false;
                });
            var awaitersGuids = new ConcurrentDictionary<string, AddAwaiterInfo>();

            var reqAwaiterConfitg =
                new RequestAwaiterSimple.Config(
                    groupId: "SimpleProduce",
                    bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                    processors: new RequestAwaiterSimple.ProcessorConfig[]
                    {
                        //From _inputSimpleTopic1
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 0 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 0 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50, 
                            afterSendOutput0: (header, message) => 
                            {
                                var info = new AddAwaiterInfo()
                                {
                                    Value = message.Value,
                                    Header = header,
                                };
                                awaitersGuids.TryAdd(header.MessageGuid, info);
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 1 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 1 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50,
                            afterSendOutput0: (header, message) =>
                            {
                                var info = new AddAwaiterInfo()
                                {
                                    Value = message.Value,
                                    Header = header,
                                };
                                awaitersGuids.TryAdd(header.MessageGuid, info);
                                return Task.CompletedTask;
                            }
                            ),
                        new RequestAwaiterSimple.ProcessorConfig(
                            input0: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic1,
                                partitions: new int[] { 2 }
                                ),
                            input1: new RequestAwaiterSimple.ConsumerInfo(
                                topicName: _inputSimpleTopic2,
                                partitions: new int[] { 2 }
                                ),
                            new RequestAwaiterSimple.ProducerInfo(_outputSimpleTopic),
                            buckets: 2,
                            maxInFly: 50,
                            afterSendOutput0: (header, message) =>
                            {
                                var info = new AddAwaiterInfo()
                                {
                                    Value = message.Value,
                                    Header = header,
                                };

                                awaitersGuids.TryAdd(header.MessageGuid, info);
                                return Task.CompletedTask;
                            }
                            )
                    }
                    );

            var requestsCount = 300;
            await using (var reqAwaiterPrepare = new RequestAwaiterSimple())
            {
                reqAwaiterPrepare.Setup(
                    config: reqAwaiterConfitg,
                    producerPool0: pool
                    );

                reqAwaiterPrepare.Start(
                    static (config) =>
                    {
                        config.MaxPollIntervalMs = 10_000;
                        config.SessionTimeoutMs = 5_000;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }
                    );

                for (int i = 0; i < requestsCount; i++)
                {
                    _ = reqAwaiterPrepare.Produce(i.ToString());
                }

                while(true)
                {
                    if(awaitersGuids.Count == requestsCount)
                    {
                        break;
                    }

                    Thread.Sleep(50);
                }

                await reqAwaiterPrepare.StopAsync();
            }

            #region responders

            var responder1 = new ResponderOneToOneSimple();
            var responder1Config = CreateResponderConfig(
                "RAResponder1",
                "RAResponder1",
                (input, s) =>
                {
                    var result = new ResponderOneToOneSimple.OutputMessage()
                    {
                        Value = $"{input.Value} Answer from 1"
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
                    var result = new ResponderOneToOneSimple.OutputMessage()
                    {
                        Value = $"{input.Value} Answer from 2"
                    };

                    return Task.FromResult(result);
                });
            responder2.Start(config: responder2Config, producerPool: pool);

            #endregion

            var answers = new List<Task<(BaseResponse[], CurrentState, string)>>(awaitersGuids.Count);
            await using (var reqAwaiter = new RequestAwaiterSimple())
            {
                reqAwaiter.Setup(
                    config: reqAwaiterConfitg,
                    producerPool0: pool
                    );

                foreach (var pair in awaitersGuids)
                {
                    answers.Add(
                    AddAwaiter(
                            pair.Value.Header.MessageGuid,
                            pair.Value.Header.Bucket,
                            pair.Value.Header.TopicsForAnswer[0].Partitions.ToArray(),
                            pair.Value.Header.TopicsForAnswer[1].Partitions.ToArray(),
                            pair.Value.Value
                            )
                        );

                    async Task<(BaseResponse[], CurrentState, string)> AddAwaiter(
                        string messageGuid,
                        int bucket,
                        int[] input0Partitions,
                        int[] input1Partitions,
                        string requestValue
                        )
                    {
                        using var result =
                            await reqAwaiter.AddAwaiter(
                                messageGuid,
                                bucket,
                                input0Partitions,
                                input1Partitions
                                )
                            .ConfigureAwait(false);
                        var state = result.CurrentState;
                        var response = result.Result;

                        return (response, state, requestValue);
                    }
                }

                reqAwaiter.Start(
                    static (config) =>
                    {
                        config.MaxPollIntervalMs = 10_000;
                        config.SessionTimeoutMs = 5_000;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }
                    );

                var unique1 = new HashSet<string>(requestsCount);
                var unique2 = new HashSet<string>(requestsCount);

                Task.WaitAll(answers.ToArray());
                for (int i = 0; i < answers.Count; i++)
                {
                    (BaseResponse[] result, CurrentState state, string requestValue) result = await answers[i];
                    Assert.Multiple(() =>
                    {
                        Assert.That(result.result.Count, Is.EqualTo(2));
                        Assert.That(result.state, Is.EqualTo(CurrentState.NewMessage));
                    });

                    var answerFrom1 = result.result[0] as ResponseItem<RequestAwaiterSimple.Input0Message>;
                    Assert.That(answerFrom1 != null, Is.True);
                    Assert.That(answerFrom1.Result != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                        Assert.That(answerFrom1.Result.Value, Is.EqualTo($"{result.requestValue} Answer from 1"));
                        Assert.That(unique1.Add(answerFrom1.Result.Value), Is.True);
                    });

                    var answerFrom2 = result.result[1] as ResponseItem<RequestAwaiterSimple.Input1Message>;
                    Assert.That(answerFrom2 != null, Is.True);
                    Assert.That(answerFrom2.Result != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
                        Assert.That(answerFrom2.Result.Value, Is.EqualTo($"{result.requestValue} Answer from 2"));
                        Assert.That(unique2.Add(answerFrom2.Result.Value), Is.True);
                    });
                }

                Assert.That(unique1.Count, Is.EqualTo(requestsCount));
                Assert.That(unique2.Count, Is.EqualTo(requestsCount));
            }

            await responder1.StopAsync();
            await responder2.StopAsync();
        }
    }
}