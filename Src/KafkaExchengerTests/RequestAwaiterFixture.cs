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
        private ResponderOneToOneSimple.Config _responder1Config;

        private ResponderOneToOneSimple _responder2;
        private ResponderOneToOneSimple.Config _responder2Config;

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

            await Task.Delay(500);

            _responder1 = new ResponderOneToOneSimple();
            _responder1Config = CreateResponderConfig(
                groupId: "RAResponder1",
                serviceName: "RAResponder1",
                createAnswer: static (input, s) =>
                {
                    var result = new ResponderOneToOneSimple.OutputMessage()
                    {
                        Output0Message = new ResponderOneToOneSimple.Output0Message() 
                        { 
                            Value = $"{input.Input0Message.Value} Answer from 1" 
                        }
                    };

                    return Task.FromResult(result);
                },
                loadCurrentHorizon: static async (input0partitions) => { return await Task.FromResult(1L); }
                );

            _responder2 = new ResponderOneToOneSimple();
            _responder2Config = CreateResponderConfig(
                groupId: "RAResponder2",
                serviceName: "RAResponder2",
                createAnswer: static (input, s) =>
                {
                    var result = new ResponderOneToOneSimple.OutputMessage()
                    {
                        Output0Message = new ResponderOneToOneSimple.Output0Message()
                        {
                            Value = $"{input.Input0Message.Value} Answer from 2"
                        }
                    };

                    return Task.FromResult(result);
                },
                loadCurrentHorizon: static async (input0partitions) => { return await Task.FromResult(1L); }
                );
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

            await Task.Delay(500);
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
                                    ReplicationFactor = -1,
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

        private ResponderOneToOneSimple.Config CreateResponderConfig(
            string groupId,
            string serviceName,
            Func<ResponderOneToOneSimple.InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderOneToOneSimple.OutputMessage>> createAnswer,
            Func<int[], ValueTask<long>> loadCurrentHorizon
            )
        {
            return
                new ResponderOneToOneSimple.Config(
                groupId: groupId,
                serviceName: serviceName,
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                maxBuckets: 5,
                itemsInBucket: 100,
                addNewBucket: static async (bucketId) => { await Task.CompletedTask; },
                processors: new ResponderOneToOneSimple.ProcessorConfig[]
                {
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: createAnswer,
                        new ResponderOneToOneSimple.ConsumerInfo(
                            _outputSimpleTopic, 
                            new int[] { 0, 1, 2 }
                            )
                        )
                }
                );
        }

        [Test]
        public async Task CancelByTimeout()
        {
            using (var pool = new ProducerPoolNullString(3, GlobalSetUp.Configuration["BootstrapServers"],
                    static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }
                    ))
            {
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
                            )
                        }
                        );
                }
            }
        }

        [Test]
        public async Task SimpleProduce()
        {
            using (var pool = new ProducerPoolNullString(5, GlobalSetUp.Configuration["BootstrapServers"],
                    static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }))
            {
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

                _responder1.Start(config: _responder1Config, output0Pool: pool);
                _responder2.Start(config: _responder2Config, output0Pool: pool);

                var requestsCount = 1000;
                var answers = new Task<(RequestAwaiterSimple.Response, string)>[requestsCount];
                for (int i = 0; i < requestsCount; i++)
                {
                    var message = $"Hello{i}";
                    answers[i] = produce(message);

                    async Task<(RequestAwaiterSimple.Response, string)> produce(string message)
                    {
                        using var result = await reqAwaiter.Produce(message).ConfigureAwait(false);
                        var state = result.CurrentState;

                        return (result, message);
                    }
                }

                var unique1 = new HashSet<string>(requestsCount);
                var unique2 = new HashSet<string>(requestsCount);

                Task.WaitAll(answers);
                for (int i = 0; i < requestsCount; i++)
                {
                    (RequestAwaiterSimple.Response result, string requestValue) result = await answers[i];
                    Assert.Multiple(() =>
                    {
                        Assert.That(result.result.CurrentState, Is.EqualTo(RAState.Sended));
                    });

                    var answerFrom1 = result.result.Input0Message0;
                    Assert.That(answerFrom1 != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                        Assert.That(answerFrom1.Value, Is.EqualTo($"{result.requestValue} Answer from 1"));
                        Assert.That(unique1.Add(answerFrom1.Value), Is.True);
                    });

                    var answerFrom2 = result.result.Input1Message0;
                    Assert.That(answerFrom2 != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
                        Assert.That(answerFrom2.Value, Is.EqualTo($"{result.requestValue} Answer from 2"));
                        Assert.That(unique2.Add(answerFrom2.Value), Is.True);
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
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
                            checkOutput0Status: static (_, _, _, _) => { return Task.FromResult(KafkaExchanger.Attributes.Enums.RAState.Sended); }
                            )
                        }
                        );
                }
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
            using (var pool = new ProducerPoolNullString(5, GlobalSetUp.Configuration["BootstrapServers"],
                    static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }))
            {
                var sended = new ConcurrentDictionary<string, AddAwaiterInfo>();
                var sendedFromAwaiter = new ConcurrentDictionary<string, AddAwaiterInfo>();
                TaskCompletionSource waitSended = new();

                RequestAwaiterSimple.Config reqAwaiterConfitg;
                CreateConfig();

                var requestsCount = 300;
                var reqAwaiterPrepare = new RequestAwaiterSimple();
                try
                {
                    reqAwaiterPrepare.Setup(
                        config: reqAwaiterConfitg,
                        producerPool0: pool
                        );

                    var sendedCount = 0;
                    for (int i = 0; i < requestsCount; i++)
                    {
                        using var delayProduce = await reqAwaiterPrepare.ProduceDelay(i.ToString(), 10).ConfigureAwait(false);
                        if (i % 2 == 0)
                        {
                            sendedCount++;
                            Volatile.Write(ref waitSended, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                            _ = delayProduce.Produce();
                            await Volatile.Read(ref waitSended).Task.ConfigureAwait(false);
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

                    Assert.That(sended.Count, Is.EqualTo(sendedCount));
                }
                finally
                {
                    using var cts = new CancellationTokenSource(0);
                    await reqAwaiterPrepare.StopAsync(cts.Token);
                    await reqAwaiterPrepare.DisposeAsync();
                }
                
                _responder1.Start(config: _responder1Config, output0Pool: pool);
                _responder2.Start(config: _responder2Config, output0Pool: pool);

                var answers = new List<Task<(string, RequestAwaiterSimple.Response, string)>>(requestsCount);
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

                    async Task<(string, RequestAwaiterSimple.Response, string)> AddAwaiter(
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

                        return (messageGuid, result, requestValue);
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
                        (string messageGuid, RequestAwaiterSimple.Response result, string requestValue) result = await answers[i];
                        Assert.Multiple(() =>
                        {
                            Assert.That(result.result.CurrentState, Is.EqualTo(RAState.Sended));
                        });

                        var answerFrom1 = result.result.Input0Message0;
                        Assert.That(answerFrom1 != null, Is.True);
                        Assert.Multiple(() =>
                        {
                            Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                            if (sendedFromAwaiter.ContainsKey(result.messageGuid))
                            {
                                Assert.That(answerFrom1.Value, Is.EqualTo($"{result.requestValue}LOM Answer from 1"));
                            }
                            else
                            {
                                Assert.That(answerFrom1.Value, Is.EqualTo($"{result.requestValue} Answer from 1"));
                            }
                            Assert.That(unique1.Add(answerFrom1.Value), Is.True);
                        });

                        var answerFrom2 = result.result.Input1Message0;
                        Assert.That(answerFrom2 != null, Is.True);
                        Assert.Multiple(() =>
                        {
                            Assert.That(answerFrom2.TopicName, Is.EqualTo(_inputSimpleTopic2));
                            if (sendedFromAwaiter.ContainsKey(result.messageGuid))
                            {
                                Assert.That(answerFrom2.Value, Is.EqualTo($"{result.requestValue}LOM Answer from 2"));
                            }
                            else
                            {
                                Assert.That(answerFrom2.Value, Is.EqualTo($"{result.requestValue} Answer from 2"));
                            }
                            Assert.That(unique2.Add(answerFrom2.Value), Is.True);
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
                            checkOutput0Status: AddAwaiterCheckStatus
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
                            checkOutput0Status: AddAwaiterCheckStatus
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
                            checkOutput0Status: AddAwaiterCheckStatus
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

                    Volatile.Read(ref waitSended).TrySetResult();
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
                    if (sended.ContainsKey(messageGuid))
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
}