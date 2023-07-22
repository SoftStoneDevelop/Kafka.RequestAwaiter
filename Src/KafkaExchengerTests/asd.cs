
using Confluent.Kafka;

using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Reflection.PortableExecutable;

namespace KafkaExchengerTests2
{

    public interface IRequestAwaiterManyToOneSimpleRequestAwaiter
    {

        public Task<KafkaExchengerTests.Response> Produce(

            System.String value0,

            int waitResponseTimeout = 0
            );

        public void Start(
            RequestAwaiterManyToOneSimple.Config config
,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0

            )
            ;

        public Task StopAsync();

    }

    public partial class RequestAwaiterManyToOneSimple : IRequestAwaiterManyToOneSimpleRequestAwaiter
    {

        private PartitionItem[] _items;

        public RequestAwaiterManyToOneSimple()
        {

        }

        public void Start(
            RequestAwaiterManyToOneSimple.Config config
,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0

            )
        {
            BuildPartitionItems(
                config
,
                producerPool0

                );

            foreach (var item in _items)
            {
                item.Start(
                    config.BootstrapServers,
                    config.GroupId
                    );
            }
        }

        private void BuildPartitionItems(
            RequestAwaiterManyToOneSimple.Config config
,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0

            )
        {
            _items = new PartitionItem[config.Consumers.Length];
            for (int i = 0; i < config.Consumers.Length; i++)
            {
                _items[i] =
                    new PartitionItem(

                        config.Consumers[i].Income0.TopicName,
                        config.Consumers[i].Income0.Partitions,
                        config.Consumers[i].Income0.CanAnswerService
,
                        config.Consumers[i].Income1.TopicName,
                        config.Consumers[i].Income1.Partitions,
                        config.Consumers[i].Income1.CanAnswerService
,
                        config.OutcomeTopicName
,
                        producerPool0




                        );
            }
        }

        public async Task StopAsync()
        {
            foreach (var item in _items)
            {
                await item.Stop();
            }

            _items = null;
        }

        public class Config
        {
            public Config(
                string groupId,
                string bootstrapServers,
                string outcomeTopicName,
                Consumers[] consumers
                )
            {
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                OutcomeTopicName = outcomeTopicName;
                Consumers = consumers;
            }

            public string GroupId { get; init; }

            public string BootstrapServers { get; init; }

            public string OutcomeTopicName { get; init; }

            public Consumers[] Consumers { get; init; }
        }

        public class Consumers
        {
            private Consumers() { }

            public Consumers(
                string groupName
,
                ConsumerInfo income0
,
                ConsumerInfo income1

                )
            {
                GroupName = groupName;

                Income0 = income0;

                Income1 = income1;

            }

            /// <summary>
            /// To identify the logger
            /// </summary>
            public string GroupName { get; init; }

            public ConsumerInfo Income0 { get; init; }

            public ConsumerInfo Income1 { get; init; }

        }

        public class ConsumerInfo
        {
            private ConsumerInfo() { }

            public ConsumerInfo(
                string topicName,
                string[] canAnswerService,
                int[] partitions
                )
            {
                CanAnswerService = canAnswerService;
                TopicName = topicName;
                Partitions = partitions;
            }

            public string TopicName { get; init; }

            public string[] CanAnswerService { get; init; }

            public int[] Partitions { get; init; }
        }

        public Task<KafkaExchengerTests.Response> Produce(

            System.String value0,

            int waitResponseTimeout = 0
            )
        {
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            var item = _items[index];
            return
                item.Produce(

                    value0,

                    waitResponseTimeout
                );
        }
        private uint _currentItemIndex = 0;

        public abstract class BaseResponseMessage
        {
            public Confluent.Kafka.Partition Partition { get; set; }
        }

        public class ResponseTopic0Message : BaseResponseMessage
        {
            public KafkaExchengerTests.ResponseHeader HeaderInfo { get; set; }

            public Message<Confluent.Kafka.Null, System.String> OriginalMessage { get; set; }

            public System.String Value { get; set; }
        }

        public class ResponseTopic1Message : BaseResponseMessage
        {
            public KafkaExchengerTests.ResponseHeader HeaderInfo { get; set; }

            public Message<Confluent.Kafka.Null, System.String> OriginalMessage { get; set; }

            public System.String Value { get; set; }
        }

        public class TopicResponse : IDisposable
        {
            private TaskCompletionSource<bool> _responseProcess = new();
            private CancellationTokenSource _cts;

            public Task<KafkaExchengerTests.Response> _response;

            private TaskCompletionSource<ResponseTopic0Message> _responseTopic0 = new();

            private TaskCompletionSource<ResponseTopic1Message> _responseTopic1 = new();

            public TopicResponse(

                string topic0Name,

                string topic1Name,

                string guid,
                Action<string> removeAction,
                int waitResponseTimeout = 0
                )
            {
                _responseProcess.Task.ContinueWith(
                    (t) => 
                    {
                        if(t.IsCompleted && t.Result)
                        {
                            removeAction(guid);
                        }
                    }
                    );
                _response = CreateGetResponse(

                topic0Name,
                topic1Name);
                if (waitResponseTimeout != 0)
                {
                    _cts = new CancellationTokenSource(waitResponseTimeout);
                    _cts.Token.Register(() =>
                    {
                        var canceled =

                            _responseTopic0.TrySetCanceled()
 |
                            _responseTopic1.TrySetCanceled()

                            ;
                        if (canceled)
                        {
                            removeAction(guid);
                        }
                    },
                    useSynchronizationContext: false
                    );
                }
            }

            private async Task<KafkaExchengerTests.Response> CreateGetResponse(

                string topic0Name
,
                string topic1Name

                )

            {

                var topic0 = await _responseTopic0.Task;

                var topic1 = await _responseTopic1.Task;

                var response = new KafkaExchengerTests.Response(
                    new KafkaExchengerTests.BaseResponse[]
                    {

                        new KafkaExchengerTests.ResponseItem<ResponseTopic0Message>(topic0Name, topic0)
,
                        new KafkaExchengerTests.ResponseItem<ResponseTopic1Message>(topic1Name, topic1)

                    },
                    _responseProcess
                    );

                return response;
            }

            public Task<bool> GetProcessStatus()
            {
                return _responseProcess.Task;
            }

            public Task<KafkaExchengerTests.Response> GetResponse()
            {
                return _response;
            }

            public bool TrySetResponse(int topicNumber, BaseResponseMessage response)
            {
                switch (topicNumber)
                {

                    case 0:
                    {
                        return _responseTopic0.TrySetResult((ResponseTopic0Message)response);
                    }

                    case 1:
                    {
                        return _responseTopic1.TrySetResult((ResponseTopic1Message)response);
                    }

                    default:
                    {
                        return false;
                    }
                }
            }

            public void SetException(int topicNumber, Exception exception)
            {
                switch (topicNumber)
                {

                    case 0:
                    {
                        _responseTopic0.SetException(exception);
                        break;
                    }

                    case 1:
                    {
                        _responseTopic1.SetException(exception);
                        break;
                    }

                    default:
                    {
                        break;
                    }
                }
            }

            public void Dispose()
            {
                _cts?.Dispose();
            }

        }

        private class PartitionItem
        {
            private class Bucket
            {
                private readonly int _bucketId;
                private readonly int _maxInFly = 100;
                private ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
                private int _addedCount;
                private readonly Dictionary<string, RequestAwaiterManyToOneSimple.TopicResponse> _responseAwaiters;
                
                private CancellationTokenSource _ctsConsume;
                private Task[] _consumeRoutines;

                public Bucket(

                string incomeTopic0Name,
                int[] incomeTopic0Partitions,
                string[] incomeTopic0CanAnswerService,

                string incomeTopic1Name,
                int[] incomeTopic1Partitions,
                string[] incomeTopic1CanAnswerService,

                string outcomeTopic0Name,
                KafkaExchanger.Common.IProducerPoolNullString producerPool0,
                int bucketId
                )
                {
                    _bucketId = bucketId;
                    _responseAwaiters = new(_maxInFly);

                    _incomeTopic0Name = incomeTopic0Name;
                    _incomeTopic0Partitions = incomeTopic0Partitions;
                    _incomeTopic0CanAnswerService = incomeTopic0CanAnswerService;

                    _incomeTopic1Name = incomeTopic1Name;
                    _incomeTopic1Partitions = incomeTopic1Partitions;
                    _incomeTopic1CanAnswerService = incomeTopic1CanAnswerService;

                    _outcomeTopic0Name = outcomeTopic0Name;
                    _producerPool0 = producerPool0;

                }

                private readonly string _incomeTopic0Name;
                private readonly int[] _incomeTopic0Partitions;
                private readonly string[] _incomeTopic0CanAnswerService;

                private readonly string _incomeTopic1Name;
                private readonly int[] _incomeTopic1Partitions;
                private readonly string[] _incomeTopic1CanAnswerService;

                private readonly string _outcomeTopic0Name;
                private readonly KafkaExchanger.Common.IProducerPoolNullString _producerPool0;

                public void Start(
                string bootstrapServers,
                string groupId
                )
                {
                    _consumeRoutines = new Task[2];
                    _ctsConsume = new CancellationTokenSource();

                    _consumeRoutines[0] = StartTopic0Consume(bootstrapServers, groupId);

                    _consumeRoutines[1] = StartTopic1Consume(bootstrapServers, groupId);

                }

                private Task StartTopic0Consume(
                string bootstrapServers,
                string groupId
                )
                {
                    return Task.Factory.StartNew(async () =>
                    {
                        var conf = new Confluent.Kafka.ConsumerConfig
                        {
                            GroupId = $"{groupId}Bucket{_bucketId}",
                            BootstrapServers = bootstrapServers,
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            AllowAutoCreateTopics = false,
                            EnableAutoCommit = false
                        };

                        var consumer =
                            new ConsumerBuilder<Confluent.Kafka.Null, System.String>(conf)
                            .Build()
                            ;

                        consumer.Assign(_incomeTopic0Partitions.Select(sel => new TopicPartition(_incomeTopic0Name, sel)));

                        try
                        {
                            int? leaderEpoch = null;
                            Offset offset = default;
                            TopicPartition topicPartition = null;
                            while (!_ctsConsume.Token.IsCancellationRequested)
                            {
                                try
                                {
                                    _lock.EnterUpgradeableReadLock();
                                    try
                                    {
                                        if(_addedCount == _maxInFly)
                                        {
                                            _lock.EnterWriteLock();
                                            try
                                            {
                                                var process = false;
                                                foreach (var response in _responseAwaiters.Values)
                                                {
                                                    try
                                                    {
                                                        process = response.GetProcessStatus().Result;
                                                    }
                                                    catch { /*ignore*/ }

                                                    if(!process)
                                                    {
                                                        //todo something??? Exception??
                                                    }
                                                }

                                                _addedCount = 0;
                                                consumer.Commit(new[] { new TopicPartitionOffset(topicPartition, offset + 1, leaderEpoch) });
                                            }
                                            finally
                                            {
                                                _lock.ExitWriteLock();
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        _lock.ExitUpgradeableReadLock();
                                    }
                                    var consumeResult = consumer.Consume(_ctsConsume.Token);
                                    leaderEpoch = consumeResult.LeaderEpoch;
                                    offset = consumeResult.Offset;
                                    topicPartition = consumeResult.TopicPartition;

                                    var incomeMessage = new RequestAwaiterManyToOneSimple.ResponseTopic0Message()
                                    {
                                        OriginalMessage = consumeResult.Message,

                                        Value = consumeResult.Message.Value,
                                        Partition = consumeResult.Partition
                                    };


                                    if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                    {
                                        continue;
                                    }

                                    incomeMessage.HeaderInfo = KafkaExchengerTests.ResponseHeader.Parser.ParseFrom(infoBytes);

                                    TopicResponse topicResponse;
                                    _lock.EnterReadLock();
                                    try
                                    {
                                        if (!_responseAwaiters.TryGetValue(incomeMessage.HeaderInfo.AnswerToMessageGuid, out topicResponse))
                                        {
                                            continue;
                                        }

                                        topicResponse.TrySetResponse(0, incomeMessage);
                                    }
                                    finally
                                    {
                                        _lock.ExitReadLock();
                                    }
                                }
                                catch (ConsumeException)
                                {
                                    //ignore
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // Ensure the consumer leaves the group cleanly and final offsets are committed.
                            consumer.Close();
                        }
                        finally
                        {
                            consumer.Dispose();
                        }
                    },
                _ctsConsume.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
                );
                }

                private Task StartTopic1Consume(
                    string bootstrapServers,
                    string groupId
                    )
                {
                    return Task.Factory.StartNew(async () =>
                    {
                        var conf = new Confluent.Kafka.ConsumerConfig
                        {
                            GroupId = $"{groupId}Bucket{_bucketId}",
                            BootstrapServers = bootstrapServers,
                            AutoOffsetReset = AutoOffsetReset.Earliest,
                            AllowAutoCreateTopics = false,
                            EnableAutoCommit = false
                        };

                        var consumer =
                            new ConsumerBuilder<Confluent.Kafka.Null, System.String>(conf)
                            .Build()
                            ;

                        consumer.Assign(_incomeTopic1Partitions.Select(sel => new TopicPartition(_incomeTopic1Name, sel)));

                        try
                        {
                            int? leaderEpoch = null;
                            Offset offset = default;
                            TopicPartition topicPartition = null;
                            while (!_ctsConsume.Token.IsCancellationRequested)
                            {
                                try
                                {
                                    _lock.EnterUpgradeableReadLock();
                                    try
                                    {
                                        if (_addedCount == _maxInFly)
                                        {
                                            _lock.EnterWriteLock();
                                            try
                                            {
                                                var process = false;
                                                foreach (var response in _responseAwaiters.Values)
                                                {
                                                    try
                                                    {
                                                        process = await response.GetProcessStatus();
                                                    }
                                                    catch { /*ignore*/ }

                                                    if (!process)
                                                    {
                                                        //todo something??? Exception??
                                                    }
                                                }

                                                _addedCount = 0;
                                                consumer.Commit(new[] { new TopicPartitionOffset(topicPartition, offset + 1, leaderEpoch) });
                                            }
                                            finally
                                            {
                                                _lock.ExitWriteLock();
                                            }
                                        }
                                    }
                                    finally
                                    {
                                        _lock.ExitUpgradeableReadLock();
                                    }
                                    var consumeResult = consumer.Consume(_ctsConsume.Token);
                                    leaderEpoch = consumeResult.LeaderEpoch;
                                    offset = consumeResult.Offset;
                                    topicPartition = consumeResult.TopicPartition;

                                    var incomeMessage = new RequestAwaiterManyToOneSimple.ResponseTopic1Message()
                                    {
                                        OriginalMessage = consumeResult.Message,

                                        Value = consumeResult.Message.Value,
                                        Partition = consumeResult.Partition
                                    };


                                    if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                    {
                                        continue;
                                    }

                                    incomeMessage.HeaderInfo = KafkaExchengerTests.ResponseHeader.Parser.ParseFrom(infoBytes);

                                    TopicResponse topicResponse;
                                    _lock.EnterReadLock();
                                    try
                                    {
                                        if (!_responseAwaiters.TryGetValue(incomeMessage.HeaderInfo.AnswerToMessageGuid, out topicResponse))
                                        {
                                            continue;
                                        }

                                        topicResponse.TrySetResponse(1, incomeMessage);
                                    }
                                    finally
                                    {
                                        _lock.ExitReadLock();
                                    }
                                }
                                catch (ConsumeException)
                                {
                                    //ignore
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            // Ensure the consumer leaves the group cleanly and final offsets are committed.
                            consumer.Close();
                        }
                        finally
                        {
                            consumer.Dispose();
                        }
                    },
                _ctsConsume.Token,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
                );
                }

                public async Task StopConsume()
                {
                    _ctsConsume?.Cancel();
                    foreach (var consumeRoutine in _consumeRoutines)
                    {
                        await consumeRoutine;
                    }

                    _ctsConsume?.Dispose();
                }

                public struct TryProduceResult
                {
                    public bool Succsess;
                    public KafkaExchengerTests.Response Response;
                }

                public async Task<TryProduceResult> TryProduce(

                System.String value0,

                int waitResponseTimeout = 0
                )
                {
                    var messageGuid = Guid.NewGuid().ToString("D");

                    var message0 = new Message<Confluent.Kafka.Null, System.String>()
                    {

                        Value = value0
                    };

                    var header = CreateOutcomeHeader();

                    header.MessageGuid = messageGuid;
                    message0.Headers = new Headers
                {
                    { "Info", header.ToByteArray() }
                };



                    var awaiter =
                        new RequestAwaiterManyToOneSimple.TopicResponse(

                            _incomeTopic0Name,

                            _incomeTopic1Name,

                            header.MessageGuid,
                            RemoveAwaiter,
                            waitResponseTimeout
                            );

                    _lock.EnterUpgradeableReadLock();
                    try
                    {
                        if(_responseAwaiters.Count == _maxInFly)
                        {
                            return new TryProduceResult { Succsess = false };
                        }
                        else
                        {
                            _lock.EnterWriteLock();
                            try
                            {
                                if (!_responseAwaiters.TryAdd(header.MessageGuid, awaiter))
                                {
                                    awaiter.Dispose();
                                    throw new Exception();
                                }
                                else
                                {
                                    _addedCount++;
                                }
                            }
                            finally
                            {
                                _lock.ExitWriteLock();
                            }
                        }
                    }
                    finally
                    {
                        _lock.ExitUpgradeableReadLock();
                    }

                    var producer = _producerPool0.Rent();
                    try
                    {
                        var deliveryResult = await producer.ProduceAsync(_outcomeTopic0Name, message0);
                    }
                    catch (ProduceException<Confluent.Kafka.Null, System.String>)
                    {
                        _lock.EnterWriteLock();
                        try
                        {
                            _responseAwaiters.Remove(header.MessageGuid, out _);
                        }
                        finally
                        {
                            _lock.ExitWriteLock();
                        }
                        awaiter.Dispose();

                        throw;
                    }
                    finally
                    {
                        _producerPool0.Return(producer);
                    }

                    var response = await awaiter.GetResponse();
                    return new TryProduceResult() { Succsess = true, Response = response };
                }

                private void RemoveAwaiter(string guid)
                {
                    _lock.EnterWriteLock();
                    try
                    {
                        if(_responseAwaiters.Remove(guid, out var value))
                        {
                            value.Dispose();
                        }
                    }
                    finally
                    {
                        _lock.ExitWriteLock();
                    }
                }

                private KafkaExchengerTests.RequestHeader CreateOutcomeHeader()
                {
                    var headerInfo = new KafkaExchengerTests.RequestHeader();

                    var topic = new KafkaExchengerTests.Topic()
                    {
                        Name = _incomeTopic0Name
                    };
                    topic.Partitions.Add(_incomeTopic0Partitions);
                    topic.CanAnswerFrom.Add(_incomeTopic0CanAnswerService);
                    headerInfo.TopicsForAnswer.Add(topic);

                    topic = new KafkaExchengerTests.Topic()
                    {
                        Name = _incomeTopic1Name
                    };
                    topic.Partitions.Add(_incomeTopic1Partitions);
                    topic.CanAnswerFrom.Add(_incomeTopic1CanAnswerService);
                    headerInfo.TopicsForAnswer.Add(topic);

                    return headerInfo;
                }
            }

            private readonly Bucket[] _buckets;

            public PartitionItem(

                string incomeTopic0Name,
                int[] incomeTopic0Partitions,
                string[] incomeTopic0CanAnswerService,

                string incomeTopic1Name,
                int[] incomeTopic1Partitions,
                string[] incomeTopic1CanAnswerService,

                string outcomeTopic0Name,
                KafkaExchanger.Common.IProducerPoolNullString producerPool0,
                int buckets



                )
            {
                _buckets = new Bucket[buckets];
                for (int i = 0; i < buckets; i++)
                {
                    _buckets[i] = new Bucket(
                        incomeTopic0Name,
                        incomeTopic0Partitions,
                        incomeTopic0CanAnswerService,

                        incomeTopic1Name,
                        incomeTopic1Partitions,
                        incomeTopic1CanAnswerService, 
                        
                        outcomeTopic0Name,
                        producerPool0,

                        i
                        );
                }
            }

            public void Start(
                string bootstrapServers,
                string groupId
                )
            {
                for (int i = 0; i < _buckets.Length; i++)
                {
                    _buckets[i].Start(bootstrapServers, groupId);
                }
            }

            public async Task Stop()
            {
                for (int i = 0; i < _buckets.Length; i++)
                {
                    await _buckets[i].StopConsume();
                }
            }

            public async Task<KafkaExchengerTests.Response> Produce(

                System.String value0,

                int waitResponseTimeout = 0
                )
            {
                //choose bucket and bucket.Produce
                return await awaiter.GetResponse();
            }

        }

    }

}
