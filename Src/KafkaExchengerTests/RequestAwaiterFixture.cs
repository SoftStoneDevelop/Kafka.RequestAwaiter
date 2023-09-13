using Confluent.Kafka;
using Confluent.Kafka.Admin;
using KafkaExchanger.Attributes.Enums;
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
                }
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
                }
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
            Func<ResponderOneToOneSimple.InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderOneToOneSimple.OutputMessage>> createAnswer
            )
        {
            return
                new ResponderOneToOneSimple.Config(
                groupId: groupId,
                serviceName: serviceName,
                bootstrapServers: GlobalSetUp.Configuration["BootstrapServers"],
                itemsInBucket: 100,
                inFlyLimit: 5,
                addNewBucket: static async (bucketId, partitions, topicName) => { await Task.CompletedTask; },
                bucketsCount: async (partitions, topicName) => { return await Task.FromResult(5); },
                processors: new ResponderOneToOneSimple.ProcessorConfig[]
                {
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: createAnswer,
                        new ResponderOneToOneSimple.ConsumerInfo(
                            _outputSimpleTopic, 
                            new int[] { 0 }
                            )
                        ),
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: createAnswer,
                        new ResponderOneToOneSimple.ConsumerInfo(
                            _outputSimpleTopic,
                            new int[] { 1 }
                            )
                        ),
                    new ResponderOneToOneSimple.ProcessorConfig(
                        createAnswer: createAnswer,
                        new ResponderOneToOneSimple.ConsumerInfo(
                            _outputSimpleTopic,
                            new int[] { 2 }
                            )
                        )
                }
                );
        }

        [Test]
        public async Task CancelByTimeout()
        {
            await using (var pool = new ProducerPoolNullString(
                new HashSet<string> { "Test0", "Test1" },
                GlobalSetUp.Configuration["BootstrapServers"],
                messagesInTransaction: 100,
                changeConfig: static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }
                    ))
            {
                int currentBuckets = 0;
                await using var reqAwaiter = new RequestAwaiterSimple();
                RequestAwaiterSimple.Config reqAwaiterConfitg;
                CreateConfig();

                await reqAwaiter.Setup(
                    config: reqAwaiterConfitg,
                    producerPool0: pool,
                    currentBucketsCount: reqAwaiterConfitg.BucketsCount
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
                    tasks[i] = reqAwaiter.Produce("Hello", 1_000);
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
                        itemsInBucket: 50,
                        inFlyBucketsLimit: 5,
                        addNewBucket: (bucketId, partitions0, topic0Name, partitions1, topic1Name) => 
                        {
                            currentBuckets++;
                            return Task.CompletedTask; 
                        },
                        bucketsCount: async (partitions0, topic0Name, partitions1, topic1Name) => 
                        { 
                            return await Task.FromResult(currentBuckets); 
                        },
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
                            afterSendOutput0: (bucketId, header, message) =>
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
                            afterSendOutput0: (bucketId, header, message) =>
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
                            afterSendOutput0: (bucketId, header, message) =>
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
            await using (var pool = new ProducerPoolNullString(
                new HashSet<string> { "Test0", "Test1" },
                GlobalSetUp.Configuration["BootstrapServers"],
                messagesInTransaction: 100,
                changeConfig: static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }))
            {
                int currentBuckets = 0;
                await using var reqAwaiter = new RequestAwaiterSimple();
                RequestAwaiterSimple.Config reqAwaiterConfitg;
                CreateConfig();

                await _responder1.Setup(config: _responder1Config, output0Pool: pool);
                await _responder2.Setup(config: _responder2Config, output0Pool: pool);
                await reqAwaiter.Setup(
                    config: reqAwaiterConfitg,
                    producerPool0: pool, 
                    currentBucketsCount: reqAwaiterConfitg.BucketsCount
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

                _responder1.Start();
                _responder2.Start();

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

                    var answerFrom1 = result.result.Input00;
                    Assert.That(answerFrom1 != null, Is.True);
                    Assert.Multiple(() =>
                    {
                        Assert.That(answerFrom1.TopicName, Is.EqualTo(_inputSimpleTopic1));
                        Assert.That(answerFrom1.Value, Is.EqualTo($"{result.requestValue} Answer from 1"));
                        Assert.That(unique1.Add(answerFrom1.Value), Is.True);
                    });

                    var answerFrom2 = result.result.Input10;
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
                        itemsInBucket: 50,
                        inFlyBucketsLimit: 5,
                        addNewBucket: (bucketId, partitions0, topic0Name, partitions1, topic1Name) =>
                        {
                            currentBuckets++;
                            return Task.CompletedTask;
                        },
                        bucketsCount: async (partitions0, topic0Name, partitions1, topic1Name) =>
                        {
                            return await Task.FromResult(currentBuckets);
                        },
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
                            afterSendOutput0: (bucketId, header, message) =>
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
                            afterSendOutput0: (bucketId, header, message) =>
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
                            afterSendOutput0: (bucketId, header, message) =>
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
            public string Guid;
            public int Bucket;
            public string Value;
            public KafkaExchengerTests.RequestHeader Header;
        }

        [Test]
        public async Task AddAwaiter()
        {
            await using (var pool = new ProducerPoolNullString(
                new HashSet<string> { "Test0", "Test1" },
                GlobalSetUp.Configuration["BootstrapServers"],
                messagesInTransaction: 100,
                changeConfig: static (config) =>
                    {
                        config.LingerMs = 2;
                        config.SocketKeepaliveEnable = true;
                        config.AllowAutoCreateTopics = false;
                    }))
            {
                int currentBuckets = 0;
                var sended = new ConcurrentDictionary<string, AddAwaiterInfo>();
                var sendedFromAwaiter = new ConcurrentDictionary<string, AddAwaiterInfo>();
                TaskCompletionSource waitSended = new();

                RequestAwaiterSimple.Config reqAwaiterConfitg;
                CreateConfig();
                await _responder1.Setup(config: _responder1Config, output0Pool: pool);
                await _responder2.Setup(config: _responder2Config, output0Pool: pool);

                var requestsCount = 300;
                var reqAwaiterPrepare = new RequestAwaiterSimple();
                try
                {
                    await reqAwaiterPrepare.Setup(
                        config: reqAwaiterConfitg,
                        producerPool0: pool,
                        currentBucketsCount: reqAwaiterConfitg.BucketsCount
                        );

                    reqAwaiterPrepare.Start();

                    var sendedCount = 0;
                    var tempStoreLink = new List<RequestAwaiterSimple.DelayProduce>();
                    for (int i = 0; i < requestsCount; i++)
                    {
                        if (i % 2 == 0)
                        {
                            using var delayProduce = await reqAwaiterPrepare.ProduceDelay(i.ToString(), 10).ConfigureAwait(false);
                            sendedCount++;
                            Volatile.Write(ref waitSended, new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
                            _ = delayProduce.Produce();
                            await Volatile.Read(ref waitSended).Task.ConfigureAwait(false);
                        }
                        else
                        {
                            var delayProduce = await reqAwaiterPrepare.ProduceDelay(i.ToString(), 10).ConfigureAwait(false);
                            tempStoreLink.Add(delayProduce);
                            var info = new AddAwaiterInfo()
                            {
                                Value = delayProduce.OutputRequest.Output0.Value + "LOM",
                                Guid = delayProduce.Guid,
                                Bucket = delayProduce.Bucket,
                                Header = delayProduce.OutputRequest.Output0Header
                            };
                            sendedFromAwaiter.TryAdd(delayProduce.Guid, info);
                        }
                    }

                    foreach(var dp in tempStoreLink)
                    {
                        dp.Dispose();
                    }
                    tempStoreLink.Clear();
                    Assert.That(sended.Count, Is.EqualTo(sendedCount));
                }
                finally
                {
                    using var cts = new CancellationTokenSource(0);
                    await reqAwaiterPrepare.StopAsync(cts.Token);
                    await reqAwaiterPrepare.DisposeAsync();
                }

                _responder1.Start();
                _responder2.Start();

                var answers = new List<Task<(string, RequestAwaiterSimple.Response, string)>>(requestsCount);
                await using (var reqAwaiter = new RequestAwaiterSimple())
                {
                    await reqAwaiter.Setup(
                        config: reqAwaiterConfitg,
                        producerPool0: pool,
                        currentBucketsCount: reqAwaiterConfitg.BucketsCount
                        );

                    foreach (var pair in sended)
                    {
                        answers.Add(
                        AddAwaiter(
                                pair.Value.Guid,
                                pair.Value.Bucket,
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
                                pair.Value.Guid,
                                pair.Value.Bucket,
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

                        var answerFrom1 = result.result.Input00;
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

                        var answerFrom2 = result.result.Input10;
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
                        itemsInBucket: 50,
                        inFlyBucketsLimit: 5,
                        addNewBucket: (bucketId, partitions0, topic0Name, partitions1, topic1Name) =>
                        {
                            currentBuckets++;
                            return Task.CompletedTask;
                        },
                        bucketsCount: async (partitions0, topic0Name, partitions1, topic1Name) =>
                        {
                            return await Task.FromResult(currentBuckets);
                        },
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
                            afterSendOutput0: AfterSend,
                            loadOutput0Message: LoadOutputMessage,
                            checkOutput0Status: AddAwaiterCheckStatus
                            )
                        }
                        );
                }

                Task AfterSend(int bucketId, RequestAwaiterSimple.Output0Message message, KafkaExchengerTests.RequestHeader header)
                {
                    var info = new AddAwaiterInfo()
                    {
                        Value = message.Value,
                        Guid = header.MessageGuid,
                        Bucket = bucketId,
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