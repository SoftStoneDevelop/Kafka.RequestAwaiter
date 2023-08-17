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

        private ResponderOneToOneSimple _responder1;
        private ResponderOneToOneSimple.ConfigResponder _responder1Config;

        private ResponderOneToOneSimple _responder2;
        private ResponderOneToOneSimple.ConfigResponder _responder2Config;

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

            _responder1 = new ResponderOneToOneSimple();
            _responder1Config = CreateResponderConfig(
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

            _responder2 = new ResponderOneToOneSimple();
            _responder2Config = CreateResponderConfig(
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
        }

        [TearDown]
        public async Task TearDown()
        {
            if(_responder1 != null)
            {
                await _responder1.StopAsync();
                _responder1 = null;
            }

            if (_responder2 != null)
            {
                await _responder2.StopAsync();
                _responder2 = null;
            }

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
            RequestAwaiterSimple.Config reqAwaiterConfitg;
            CreateConfig();
            
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

            void CreateConfig()
            {
                reqAwaiterConfitg =
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
                            )
                    }
                    );
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
            RequestAwaiterSimple.Config reqAwaiterConfitg;
            CreateConfig();

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

            _responder1.Start(config: _responder1Config, producerPool: pool);
            _responder2.Start(config: _responder2Config, producerPool: pool);

            var requestsCount = 1000;
            var answers = new Task<(BaseResponse[], CurrentState, string)>[requestsCount];
            for (int i = 0; i < requestsCount; i++)
            {
                var message = $"Hello{i}";
                answers[i] = produce(message);

                async Task<(BaseResponse[], CurrentState, string)> produce(string message)
                {
                    using var result = await reqAwaiter.Produce(message).ConfigureAwait(false);
                    var state = result.CurrentState;
                    var response = result.Result;

                    return (response, state, message);
                }
            }

            var unique1 = new HashSet<string>(requestsCount);
            var unique2 = new HashSet<string>(requestsCount);

            Task.WaitAll(answers);
            for (int i = 0; i < requestsCount; i++)
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

            void CreateConfig()
            {
                reqAwaiterConfitg =
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            },
                            loadOutput0Message: static (_, _, _, _) => { return Task.FromResult((RequestAwaiterSimple.Output0Message)null); },
                            addAwaiterCheckStatus: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
                            )
                    }
                    );
            }
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
            var sended = new ConcurrentDictionary<string, AddAwaiterInfo>();
            var sendedFromAwaiter = new ConcurrentDictionary<string, AddAwaiterInfo>();

            RequestAwaiterSimple.Config reqAwaiterConfitg;
            CreateConfig();

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

                var sendedCount = 0;
                for (int i = 0; i < requestsCount; i++)
                {
                    using var delayProduce = reqAwaiterPrepare.ProduceDelay(i.ToString());
                    if (i % 2 == 0)
                    {
                        sendedCount++;
                        _ = delayProduce.Produce();
                    }
                    else
                    {
                        var info = new AddAwaiterInfo()
                        {
                            Value = delayProduce.Output0Message.Value + "LOM",
                            Header = delayProduce.Output0Header,
                        };
                        sendedFromAwaiter.TryAdd(delayProduce.MessageGuid, info);
                    }
                }

                while (true)
                {
                    if (sended.Count == sendedCount)
                    {
                        break;
                    }

                    Thread.Sleep(50);
                }

                await reqAwaiterPrepare.StopAsync();

                Assert.That(sended.Count, Is.EqualTo(sendedCount));
            }
            _responder1.Start(config: _responder1Config, producerPool: pool);
            _responder2.Start(config: _responder2Config, producerPool: pool);

            var answers = new List<Task<(string, BaseResponse[], CurrentState, string)>>(requestsCount);
            await using (var reqAwaiter = new RequestAwaiterSimple())
            {
                reqAwaiter.Setup(
                    config: reqAwaiterConfitg,
                    producerPool0: pool
                    );

                foreach (var pair in sended)
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
                }

                foreach (var pair in sendedFromAwaiter)
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
                }

                async Task<(string, BaseResponse[], CurrentState, string)> AddAwaiter(
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

                    return (messageGuid, response, state, requestValue);
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
                    (string messageGuid, BaseResponse[] result, CurrentState state, string requestValue) result = await answers[i];
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
                        if(sendedFromAwaiter.ContainsKey(result.messageGuid))
                        {
                            Assert.That(answerFrom1.Result.Value, Is.EqualTo($"{result.requestValue}LOM Answer from 1"));
                        }
                        else
                        {
                            Assert.That(answerFrom1.Result.Value, Is.EqualTo($"{result.requestValue} Answer from 1"));
                        }
                        Assert.That(unique1.Add(answerFrom1.Result.Value), Is.True);
                    });

                    var answerFrom2 = result.result[1] as ResponseItem<RequestAwaiterSimple.Input1Message>;
                    Assert.That(answerFrom2 != null, Is.True);
                    Assert.That(answerFrom2.Result != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
                        if (sendedFromAwaiter.ContainsKey(result.messageGuid))
                        {
                            Assert.That(answerFrom2.Result.Value, Is.EqualTo($"{result.requestValue}LOM Answer from 2"));
                        }
                        else
                        {
                            Assert.That(answerFrom2.Result.Value, Is.EqualTo($"{result.requestValue} Answer from 2"));
                        }
                        Assert.That(unique2.Add(answerFrom2.Result.Value), Is.True);
                    });
                }

                Assert.That(unique1.Count, Is.EqualTo(requestsCount));
                Assert.That(unique2.Count, Is.EqualTo(requestsCount));
            }

            void CreateConfig()
            {
                reqAwaiterConfitg =
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
                            afterSendOutput0: AfterSend,
                            loadOutput0Message: LoadOutputMessage,
                            addAwaiterCheckStatus: AddAwaiterCheckStatus
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
                            afterSendOutput0: AfterSend,
                            loadOutput0Message: LoadOutputMessage,
                            addAwaiterCheckStatus: AddAwaiterCheckStatus
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
                            afterSendOutput0: AfterSend,
                            loadOutput0Message: LoadOutputMessage,
                            addAwaiterCheckStatus: AddAwaiterCheckStatus
                            )
                    }
                    );
            }

            Task AfterSend(KafkaExchengerTests.RequestHeader header, RequestAwaiterSimple.Output0Message message)
            {
                var info = new AddAwaiterInfo()
                {
                    Value = message.Value,
                    Header = header,
                };

                if (!sendedFromAwaiter.ContainsKey(header.MessageGuid))
                {
                    sended.TryAdd(header.MessageGuid, info);
                }

                return Task.CompletedTask;
            }

            Task<RequestAwaiterSimple.Output0Message> LoadOutputMessage(string messageGuid, int bucket, int[] input0Partitions, int[] input1Partitions)
            {

                return Task.FromResult(new RequestAwaiterSimple.Output0Message(
                    new Message<Null, string>()
                    {
                        Value = sendedFromAwaiter[messageGuid].Value + "LOM",
                    }
                    ));
            }

            Task<KafkaExchanger.Attributes.Enums.RAState> AddAwaiterCheckStatus(string messageGuid, int bucket, int[] input0Partitions, int[] input1Partitions)
            {
                if(sended.ContainsKey(messageGuid))
                {
                    return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended);
                }
                else
                {
                    return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.NotSended);
                }
            }
        }
    }
}