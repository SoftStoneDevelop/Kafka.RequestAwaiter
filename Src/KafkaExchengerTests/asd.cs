
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Channels;
using KafkaExchanger;

namespace KafkaExchengerTests
{

    public partial interface IRequestAwaiterFullRequestAwaiter : IAsyncDisposable
    {

        public Task<RequestAwaiterFull.Response> Produce(
            System.String value0,
            int waitResponseTimeout = 0
            );

        public Task<RequestAwaiterFull.DelayProduce> ProduceDelay(
            System.String value0,
            int waitResponseTimeout = 0
            );

        public void Start(Action<Confluent.Kafka.ConsumerConfig> changeConfig = null);

        public Task Setup(
            RequestAwaiterFull.Config config,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0
            )
            ;

        public Task<RequestAwaiterFull.Response> AddAwaiter(
            string messageGuid,
            int bucket,
            int[] input0partitions,
            int[] input1partitions,
            int waitResponseTimeout = 0
            );

        public ValueTask StopAsync(CancellationToken token = default);

    }

    public partial class RequestAwaiterFull : IRequestAwaiterFullRequestAwaiter
    {
        private readonly ILoggerFactory _loggerFactory;
        private RequestAwaiterFull.PartitionItem[] _items;
        private string _bootstrapServers;
        private string _groupId;
        private volatile bool _isRun;

        public RequestAwaiterFull(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public class DelayProduce : IDisposable
        {
            private DelayProduce() { }
            public DelayProduce(
                int[] input0Partitions,
                int[] input1Partitions,
                int bucketId,
                string guid,
                KafkaExchengerTests.RequestHeader output0Header,
                Output0Message output0Message,
                RequestAwaiterFull.TopicResponse response,
                TaskCompletionSource endDelay
                )
            {
                Input0Partitions = input0Partitions;
                Input1Partitions = input1Partitions;
                Bucket = bucketId;
                Guid = guid;
                Output0Header = output0Header;
                Output0Message = output0Message;
                _endDelay = endDelay;
                _topicResponse = response;
            }

            private TaskCompletionSource _endDelay;
            private RequestAwaiterFull.TopicResponse _topicResponse;

            public int Bucket { get; init; }
            public string Guid { get; init; }

            public int[] Input0Partitions { get; init; }

            public int[] Input1Partitions { get; init; }

            public KafkaExchengerTests.RequestHeader Output0Header { get; init; }
            public Output0Message Output0Message { get; init; }

            public async Task<RequestAwaiterFull.Response> Produce()
            {
                _endDelay.TrySetResult();
                return
                    await _topicResponse.GetResponse();
            }

            private bool _disposedValue;
            private bool _produced;

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected void Dispose(bool disposed)
            {
                if (!_disposedValue)
                {
                    if (!_endDelay.Task.IsCompleted)
                    {
                        _endDelay.TrySetCanceled();
                    }

                    _endDelay = null;
                    _topicResponse = null;
                    _disposedValue = true;
                }
            }

            ~DelayProduce()
            {
                Dispose(false);
            }

        }

        public class TryDelayProduceResult
        {
            public bool Succsess;
            public RequestAwaiterFull.TopicResponse Response;

            public KafkaExchengerTests.RequestHeader Output0Header;
            public Output0Message Output0Message;

        }

        public class TryAddAwaiterResult
        {
            public bool Succsess;
            public RequestAwaiterFull.TopicResponse Response;
        }

        public class Response : IDisposable
        {

            public Response(
                int bucket,
                KafkaExchanger.Attributes.Enums.RAState currentState,
                TaskCompletionSource<bool> responseProcess
,
                int[] input0Partitions
,
                Input0Message input0Message0
,
                int[] input1Partitions
,
                Input1Message input1Message0

                )
            {
                Bucket = bucket;
                CurrentState = currentState;
                _responseProcess = responseProcess;

                Input0Partitions = input0Partitions;

                Input0Message0 = input0Message0;

                Input1Partitions = input1Partitions;

                Input1Message0 = input1Message0;

            }

            private TaskCompletionSource<bool> _responseProcess;

            public int Bucket { get; init; }

            public KafkaExchanger.Attributes.Enums.RAState CurrentState { get; init; }

            public int[] Input0Partitions { get; init; }

            public Input0Message Input0Message0 { get; init; }

            public int[] Input1Partitions { get; init; }

            public Input1Message Input1Message0 { get; init; }

            private bool _disposed;

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected virtual void Dispose(bool disposing)
            {
                if (_disposed)
                {
                    return;
                }

                if (disposing)
                {
                    _responseProcess.SetResult(disposing);
                }
                else
                {
                    try
                    {
                        _responseProcess.SetResult(disposing);
                    }
                    catch
                    {
                        //ignore
                    }
                }

                _disposed = true;
                _responseProcess = null;
            }

            ~Response()
            {
                Dispose(false);
            }

        }

        public class Config
        {
            public Config(
                string groupId,
                string bootstrapServers,
                ProcessorConfig[] processors
                )
            {
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                Processors = processors;
            }

            public string GroupId { get; init; }

            public string BootstrapServers { get; init; }

            public ProcessorConfig[] Processors { get; init; }
        }

        public class ProcessorConfig
        {
            private ProcessorConfig() { }

            public ProcessorConfig(
                ConsumerInfo input0,
                ConsumerInfo input1,
                ProducerInfo output0,
                Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> currentState,
                Func<int, int[], int[], Task> afterCommit,
                Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> afterSendOutput0,
                Func<string, int, int[], int[], Task<Output0Message>> loadOutput0Message,
                Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> checkOutput0Status,
                int buckets,
                int maxInFly
                )
            {
                Buckets = buckets;
                MaxInFly = maxInFly;

                CurrentState = currentState;
                AfterCommit = afterCommit;
                AfterSendOutput0 = afterSendOutput0;
                LoadOutput0Message = loadOutput0Message;
                CheckOutput0Status = checkOutput0Status;
                Input0 = input0;
                Input1 = input1;
                Output0 = output0;
            }


            public int Buckets { get; init; }

            public int MaxInFly { get; init; }

            public Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> CurrentState { get; init; }
            public Func<int, int[], int[], Task> AfterCommit { get; init; }
            public ConsumerInfo Input0 { get; init; }
            public ConsumerInfo Input1 { get; init; }
            public ProducerInfo Output0 { get; init; }
            public Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> AfterSendOutput0 { get; init; }
            public Func<string, int, int[], int[], Task<Output0Message>> LoadOutput0Message { get; init; }
            public Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> CheckOutput0Status { get; init; }
        }

        public class ConsumerInfo
        {
            private ConsumerInfo() { }

            public ConsumerInfo(
                string topicName,
                int[] partitions
                )
            {
                TopicName = topicName;
                Partitions = partitions;
            }

            public string TopicName { get; init; }

            public int[] Partitions { get; init; }
        }

        public class ProducerInfo
        {
            private ProducerInfo() { }

            public ProducerInfo(
                string topicName
                )
            {
                TopicName = topicName;
            }

            public string TopicName { get; init; }
        }

        public abstract class BaseInputMessage
        {
            public string TopicName { get; set; }
            public Confluent.Kafka.Partition Partition { get; set; }
        }

        public class Input0Message : RequestAwaiterFull.BaseInputMessage
        {
            public KafkaExchengerTests.ResponseHeader Header { get; set; }

            public Message<Confluent.Kafka.Null, System.String> OriginalMessage { get; set; }

            public Confluent.Kafka.Null Key => OriginalMessage.Key;

            public System.String Value => OriginalMessage.Value;

        }

        public class Input1Message : RequestAwaiterFull.BaseInputMessage
        {
            public KafkaExchengerTests.ResponseHeader Header { get; set; }

            public Message<Confluent.Kafka.Null, System.String> OriginalMessage { get; set; }

            public Confluent.Kafka.Null Key => OriginalMessage.Key;

            public System.String Value => OriginalMessage.Value;

        }

        public class Output0Message
        {
            private Output0Message() { }

            public Output0Message(
                Confluent.Kafka.Message<Confluent.Kafka.Null, System.String> message

            )
            {
                Message = message;

            }

            public Confluent.Kafka.Message<Confluent.Kafka.Null, System.String> Message;

            public Confluent.Kafka.Null Key => Message.Key;

            public System.String Value => Message.Value;

        }

        public class TopicResponse : IDisposable
        {
            private TaskCompletionSource<bool> _responseProcess = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task<RequestAwaiterFull.Response> _response;
            private CancellationTokenSource _cts;
            private readonly string _guid;
            private int _waitResponseTimeout;

            public string Guid => _guid;

            public int Bucket
            {
                get;
                set;
            }

            private TaskCompletionSource<Input0Message> _responseInput0RAResponder1 = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private TaskCompletionSource<Input1Message> _responseInput1RAResponder2 = new(TaskCreationOptions.RunContinuationsAsynchronously);

            private Action<string> _removeAction;

            private int[] _input0Partitions;
            private int[] _input1Partitions;
            private Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> _getCurrentState;

            public TopicResponse(
                Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> getCurrentState,
                string guid,
                Action<string> removeAction,
                int[] input0Partitions,
                int[] input1Partitions,
                int waitResponseTimeout = 0
                )
            {
                _guid = guid;
                _removeAction = removeAction;
                _input0Partitions = input0Partitions;
                _input1Partitions = input1Partitions;
                _getCurrentState = getCurrentState;
                _waitResponseTimeout = waitResponseTimeout;
            }

            public void Init()
            {
                _response = CreateGetResponse();
                if (_waitResponseTimeout != 0)
                {
                    _cts = new CancellationTokenSource(_waitResponseTimeout);
                    _cts.Token.Register(() =>
                    {
                        _responseInput0RAResponder1.TrySetCanceled();
                        _responseInput1RAResponder2.TrySetCanceled();
                    },
                    useSynchronizationContext: false
                    );
                }

                _response.ContinueWith(task =>
                {
                    if (task.IsFaulted)
                    {
                        _responseProcess.TrySetException(task.Exception);
                        return;
                    }

                    if (task.IsCanceled)
                    {
                        _responseProcess.TrySetCanceled();
                        return;
                    }
                });

                _responseProcess.Task.ContinueWith(task =>
                {
                    _removeAction(_guid);
                });
            }

            private async Task<RequestAwaiterFull.Response> CreateGetResponse()
            {

                var topic0RAResponder1 = await _responseInput0RAResponder1.Task.ConfigureAwait(false);
                var topic1RAResponder2 = await _responseInput1RAResponder2.Task.ConfigureAwait(false);
                var currentState = await _getCurrentState(
                    Bucket,
                    _input0Partitions,
                    topic0RAResponder1,
                    _input1Partitions,
                    topic1RAResponder2
                    );

                var response = new RequestAwaiterFull.Response(
                    Bucket,
                    currentState,
                    _responseProcess,
                    _input0Partitions,
                    topic0RAResponder1,
                    _input1Partitions,
                    topic1RAResponder2
                    );

                return response;
            }

            public Task<RequestAwaiterFull.Response> GetResponse()
            {
                return _response;
            }

            public bool TrySetResponse(int topicNumber, BaseInputMessage response, int serviceNumber = 0)
            {
                switch (topicNumber, serviceNumber)
                {

                    case (0, 0):
                    {
                        return _responseInput0RAResponder1.TrySetResult((Input0Message)response);
                    }

                    case (1, 0):
                    {
                        return _responseInput1RAResponder2.TrySetResult((Input1Message)response);
                    }

                    default:
                    {
                        return false;
                    }
                }
            }

            public bool TrySetException(int topicNumber, Exception exception, int serviceNumber = 0)
            {
                switch (topicNumber, serviceNumber)
                {

                    case (0, 0):
                    {
                        return _responseInput0RAResponder1.TrySetException(exception);
                    }

                    case (1, 0):
                    {
                        return _responseInput1RAResponder2.TrySetException(exception);
                    }

                    default:
                    {
                        return false;
                    }
                }
            }

            public void Dispose()
            {
                _cts?.Dispose();

                _responseInput0RAResponder1.TrySetCanceled();

                _responseInput1RAResponder2.TrySetCanceled();

                try
                {
                    _response.Wait();
                }
                catch { /* ignore */}
            }

        }

        public abstract class ChannelInfo
        {
        }

        public class StartResponse : RequestAwaiterFull.ChannelInfo
        {
            public RequestAwaiterFull.TopicResponse ResponseProcess { get; set; }
        }

        public class SetOffsetResponse : RequestAwaiterFull.ChannelInfo
        {
            public int BucketId { get; set; }

            public string Guid { get; set; }

            public int OffsetId { get; set; }

            public Confluent.Kafka.TopicPartitionOffset Offset { get; set; }
        }

        public class EndResponse : RequestAwaiterFull.ChannelInfo
        {

            public int BucketId { get; set; }

            public string Guid { get; set; }
        }

        public class PartitionItem
        {
            private uint _current;
            private int _needCommit;
            private Confluent.Kafka.TopicPartitionOffset[] _commitOffsets;
            private System.Threading.Tasks.TaskCompletionSource _tcsCommit = new();
            private System.Threading.CancellationTokenSource _cts;
            private System.Threading.Thread[] _consumeRoutines;
            private System.Threading.Tasks.Task _horizonRoutine;
            private System.Threading.Tasks.Task _initializeRoutine;

            private readonly ConcurrentDictionary<string, RequestAwaiterFull.TopicResponse> _responseAwaiters;
            private readonly Channel<RequestAwaiterFull.ChannelInfo> _channel = Channel.CreateUnbounded<RequestAwaiterFull.ChannelInfo>(
                new UnboundedChannelOptions()
                {
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = false
                });

            private readonly Channel<RequestAwaiterFull.TopicResponse> _initializeChannel = Channel.CreateUnbounded<RequestAwaiterFull.TopicResponse>(
                new UnboundedChannelOptions()
                {
                    AllowSynchronousContinuations = false,
                    SingleReader = true,
                    SingleWriter = true
                });

            private readonly KafkaExchanger.BucketStorage _storage;
            private readonly int _itemsInBucket;
            private readonly int _inFlyItemsLimit;

            private readonly ILogger _logger;

            private readonly Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> _currentState;

            private readonly Func<int, int[], int[], Task> _afterCommit;

            private readonly string _input0Name;
            public readonly int[] _input0Partitions;

            private readonly string _input1Name;
            public readonly int[] _input1Partitions;

            private readonly string _output0TopicName;
            private readonly KafkaExchanger.Common.IProducerPoolNullString _output0Pool;
            private readonly Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> _afterSendOutput0;
            private readonly Func<string, int, int[], int[], Task<Output0Message>> _loadOutput0Message;
            private readonly Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> _checkOutput0Status;

            public PartitionItem(
                int inFlyLimit,
                int itemsInBucket,
                Func<int, int[], string, Task> addNewBucket,

                string inputTopic0Name,
                int[] inputTopic0Partitions,
                string inputTopic1Name,
                int[] inputTopic1Partitions,
                ILogger logger,
                Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> currentState,
                Func<int, int[], int[], Task> afterCommit,
                string output0Name,
                KafkaExchanger.Common.IProducerPoolNullString producerPool0,
                Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> afterSendOutput0,
                Func<string, int, int[], int[], Task<Output0Message>> loadOutput0Message,
                Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> checkOutput0Status
                )
            {
                _input0Name = inputTopic0Name;
                _input0Partitions = inputTopic0Partitions;
                _input1Name = inputTopic1Name;
                _input1Partitions = inputTopic1Partitions;

                _logger = logger;
                _currentState = currentState;
                _afterCommit = afterCommit;
                _output0TopicName = output0Name;
                _output0Pool = producerPool0;
                _afterSendOutput0 = afterSendOutput0;
                _loadOutput0Message = loadOutput0Message;
                _checkOutput0Status = checkOutput0Status;

                _itemsInBucket = itemsInBucket;
                _inFlyItemsLimit = inFlyLimit * itemsInBucket;
                _storage = new KafkaExchanger.BucketStorage(
                    inFlyLimit: inFlyLimit,
                    inputs: 2,
                    itemsInBucket: _itemsInBucket,
                    addNewBucket: async (bucketId) =>
                    {
                        await addNewBucket(
                            bucketId,

                            _input0Partitions,
                            _input0Name

                        );
                    }
                    );
            }

            public void Start(
                string bootstrapServers,
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {
                _cts = new CancellationTokenSource();
                _storage.Validate();
                StartHorizonRoutine();
                StartInitializeRoutine();
                _consumeRoutines = new Thread[2];

                _consumeRoutines[0] = StartConsumeInput0(bootstrapServers, groupId);
                _consumeRoutines[0].Start();

                _consumeRoutines[1] = StartConsumeInput1(bootstrapServers, groupId);
                _consumeRoutines[1].Start();
            }

            public async Task Setup(
                Func<int[], string, int[], string, Task<int>> currentBucketsCount
                )
            {
                await _storage.Init(currentBucketsCount: async () =>
                {
                    return await currentBucketsCount(
                            _input0Partitions,
                            _input0Name,
                            _input1Partitions,
                            _input1Name
                            );
                }
                );
            }

            private void StartHorizonRoutine()
            {
                _horizonRoutine = Task.Factory.StartNew(async () =>
                {
                    var reader = _channel.Reader;
                    var writer = _initializeChannel.Writer;

                    var queue = new Queue<RequestAwaiterFull.StartResponse>();
                    var inTheFlyCount = 0;
                    try
                    {
                        while (!_cts.Token.IsCancellationRequested)
                        {
                            var info = await reader.ReadAsync(_cts.Token).ConfigureAwait(false);
                            if (info is RequestAwaiterFull.StartResponse startResponse)
                            {
                                if(queue.Count != 0 || inTheFlyCount == _inFlyItemsLimit)
                                {
                                    queue.Enqueue(startResponse);
                                }

                                var newMessage = new KafkaExchanger.MessageInfo(2);
                                var bucketId = await _storage.Push(startResponse.ResponseProcess.Guid, newMessage);
                                startResponse.ResponseProcess.Bucket = bucketId;
                                await writer.WriteAsync(startResponse.ResponseProcess);
                                inTheFlyCount++;
                            }
                            else if (info is RequestAwaiterFull.SetOffsetResponse setOffsetResponse)
                            {
                                _ = _storage.SetOffset(
                                    setOffsetResponse.BucketId,
                                    setOffsetResponse.Guid,
                                    setOffsetResponse.OffsetId,
                                    setOffsetResponse.Offset
                                    );
                            }
                            else if (info is RequestAwaiterFull.EndResponse endResponse)
                            {
                                _storage.Finish(endResponse.BucketId, endResponse.Guid);

                                var canFreeBuckets = _storage.CanFreeBuckets();
                                if (canFreeBuckets.Count == 0)
                                {
                                    continue;
                                }

                                inTheFlyCount -= canFreeBuckets.Count * _itemsInBucket;
                                while (queue.Count != 0)
                                {
                                    if (inTheFlyCount == _inFlyItemsLimit)
                                    {
                                        break;
                                    }

                                    startResponse = queue.Dequeue();
                                    var newMessage = new KafkaExchanger.MessageInfo(2);
                                    var bucketId = await _storage.Push(startResponse.ResponseProcess.Guid, newMessage);
                                    startResponse.ResponseProcess.Bucket = bucketId;
                                    await writer.WriteAsync(startResponse.ResponseProcess);
                                    inTheFlyCount++;
                                }

                                var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                                var offset = canFreeBuckets[^1].MaxOffset;
                                Volatile.Write(ref _commitOffsets, offset);
                                Interlocked.Exchange(ref _tcsCommit, commit);
                                Interlocked.Exchange(ref _needCommit, 1);

                                await commit.Task.ConfigureAwait(false);
                                for (int i = 0; i < canFreeBuckets.Count; i++)
                                {
                                    var freeBucket = canFreeBuckets[i];
                                    await _afterCommit(
                                        freeBucket.BucketId,
                                        _input0Partitions,
                                        _input1Partitions
                                        ).ConfigureAwait(false);
                                }
                            }
                            else
                            {
                                _logger.LogError("Unknown info type");
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        //ignore
                    }
                    catch (ChannelClosedException)
                    {
                        //ignore
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error commit task");
                        throw;
                    }
                });
            }

            private void StartInitializeRoutine()
            {
                _initializeRoutine = Task.Factory.StartNew(async () =>
                {
                    var reader = _initializeChannel.Reader;
                    try
                    {
                        while (!_cts.Token.IsCancellationRequested)
                        {
                            var propessResponse = await reader.ReadAsync(_cts.Token).ConfigureAwait(false);
                            propessResponse.Init();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error init task");
                        throw;
                    }
                });
            }

            public async Task Stop()
            {
                _cts?.Cancel();

                foreach (var consumeRoutine in _consumeRoutines)
                {
                    while (consumeRoutine.IsAlive)
                    {
                        await Task.Delay(15);
                    }
                }

                _tcsCommit.TrySetCanceled();
                _channel.Writer.Complete();
                _initializeChannel.Writer.Complete();

                try
                {
                    await _horizonRoutine;
                }
                catch
                {
                    //ignore
                }

                try
                {
                    await _initializeRoutine;
                }
                catch
                {
                    //ignore
                }

                _cts?.Dispose();
            }
        }

        public void Start(
            Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
            )
        {
            if (_isRun)
            {
                throw new System.Exception("Before starting, you need to stop the previous run: call StopAsync");
            }

            _isRun = true;
            foreach (var item in _items)
            {
                item.Start(
                    _bootstrapServers,
                    _groupId,
                    changeConfig
                    );
            }
        }

        public async Task Setup(
            RequestAwaiterFull.Config config,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0
            )
        {
            if (_items != null)
            {
                throw new System.Exception("Before setup new configuration, you need to stop the previous: call StopAsync");
            }

            _items = new RequestAwaiterFull.PartitionItem[config.Processors.Length];
            _bootstrapServers = config.BootstrapServers;
            _groupId = config.GroupId;
            for (int i = 0; i < config.Processors.Length; i++)
            {
                var processorConfig = config.Processors[i];
                _items[i] =
                    new RequestAwaiterFull.PartitionItem(
                        1,//TODO    
                        2,//TODO
                        null,//TODO
                        processorConfig.Input0.TopicName,
                        processorConfig.Input0.Partitions,
                        processorConfig.Input1.TopicName,
                        processorConfig.Input1.Partitions,
                        _loggerFactory.CreateLogger(config.GroupId),
                        processorConfig.CurrentState,
                        processorConfig.AfterCommit,
                        processorConfig.Output0.TopicName,
                        producerPool0,
                        processorConfig.AfterSendOutput0,
                        processorConfig.LoadOutput0Message,
                        processorConfig.CheckOutput0Status
                        );

                //TODO pass bucketsCount to Setup
                await _items[i].Setup(null);
            }
        }

        public async Task<RequestAwaiterFull.Response> Produce(

            System.String value0,

            int waitResponseTimeout = 0
            )
        {
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            var item = _items[index];
            return await item.Produce(
                value0,
                waitResponseTimeout
            );
        }
        private uint _currentItemIndex = 0;

        public async Task<RequestAwaiterFull.DelayProduce> ProduceDelay(

            System.String value0,

            int waitResponseTimeout = 0
            )
        {
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            var item = _items[index];
            return await item.DelayProduce(
                value0,
                waitResponseTimeout
                );
        }

        public async Task<RequestAwaiterFull.Response> AddAwaiter(
            string messageGuid,
            int bucket,
            int[] input0partitions,
            int[] input1partitions,
            int waitResponseTimeout = 0
            )
        {
            //TODO find PartitionItem
            return await _items[i].AddAwaiter(
                        messageGuid,
                        bucket,

                        input0partitions,

                        input1partitions,

                        waitResponseTimeout
                        );
        }

        public async ValueTask StopAsync(CancellationToken token = default)
        {
            var items = _items;
            if (items == null)
            {
                return;
            }

            _items = null;
            _bootstrapServers = null;
            _groupId = null;

            var disposeTasks = new Task[items.Length];
            for (var i = 0; i < items.Length; i++)
            {
                disposeTasks[i] = items[i].Stop();
            }

            await Task.WhenAll(disposeTasks);
            _isRun = false;
        }

        public async ValueTask DisposeAsync()
        {
            await StopAsync();
        }

    }

}
