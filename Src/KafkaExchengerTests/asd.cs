
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using Google.Protobuf;
using System.Linq;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace KafkaExchengerTests
{

    public partial interface IRequestAwaiterFullRequestAwaiter : IAsyncDisposable
    {

        public Task<RequestAwaiterFull.Response> Produce(
            System.String output0Value,
            int waitResponseTimeout = 0
            );

        public Task<RequestAwaiterFull.DelayProduce> ProduceDelay(
            System.String output0Value,
            int waitResponseTimeout = 0
            );

        public void Start(Action<Confluent.Kafka.ConsumerConfig> changeConfig = null);

        public Task Setup(
            RequestAwaiterFull.Config config
,
            KafkaExchanger.Common.IProducerPoolNullString producerPool0

            )
            ;

        public Task<RequestAwaiterFull.Response> AddAwaiter(
            string messageGuid,
            int bucket,
            int[] input0Partitions,
            int[] input1Partitions,
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
                RequestAwaiterFull.TopicResponse topicResponse,
                RequestAwaiterFull.OutputMessage outputRequest
                )
            {
                _topicResponse = topicResponse;
                OutputRequest = outputRequest;
            }
            private RequestAwaiterFull.TopicResponse _topicResponse;

            public RequestAwaiterFull.OutputMessage OutputRequest;
            public int Bucket => _topicResponse.Bucket;
            public string Guid => _topicResponse.Guid;
            public int[] Input0Partitions => _topicResponse.Input0Partitions;
            public int[] Input1Partitions => _topicResponse.Input1Partitions;
            public async Task<RequestAwaiterFull.Response> Produce()
            {
                _topicResponse.OutputTask.TrySetResult(OutputRequest);
                return
                    await _topicResponse.GetResponse();
            }

            private bool _disposedValue;

            public void Dispose()
            {
                Dispose(true);
                GC.SuppressFinalize(this);
            }

            protected void Dispose(bool disposed)
            {
                if (!_disposedValue)
                {
                    _topicResponse.OutputTask.TrySetCanceled();
                    _topicResponse = null;
                    _disposedValue = true;
                }
            }

            ~DelayProduce()
            {
                Dispose(false);
            }

        }

        public class Response : IDisposable
        {
            public Response(
                int bucket,
                KafkaExchanger.Attributes.Enums.RAState currentState,
                TaskCompletionSource<bool> responseProcess,
                int[] input0Partitions,
                RequestAwaiterFull.Input0Message input00,
                int[] input1Partitions,
                RequestAwaiterFull.Input1Message input10
                )
            {
                Bucket = bucket;
                CurrentState = currentState;
                _responseProcess = responseProcess;

                Input0Partitions = input0Partitions;
                Input00 = input00;

                Input1Partitions = input1Partitions;
                Input10 = input10;

            }

            private TaskCompletionSource<bool> _responseProcess;

            public int Bucket { get; init; }
            public KafkaExchanger.Attributes.Enums.RAState CurrentState { get; init; }
            public int[] Input0Partitions { get; init; }
            public Input0Message Input00 { get; init; }
            public int[] Input1Partitions { get; init; }
            public Input1Message Input10 { get; init; }
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

                ConsumerInfo input0
,
                ConsumerInfo input1
,
                ProducerInfo output0

                , Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> currentState
                , Func<int, int[], int[], Task> afterCommit
,
                Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> afterSendOutput0
,
                Func<string, int, int[], int[], Task<Output0Message>> loadOutput0Message,
                Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> checkOutput0Status
,
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
            public Confluent.Kafka.TopicPartitionOffset TopicPartitionOffset { get; set; }
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

        public class OutputMessage
        {

            public KafkaExchengerTests.RequestHeader Output0Header { get; set; }
            public RequestAwaiterFull.Output0Message Output0 { get; set; }
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

        public class TryAddAwaiterResult
        {
            public bool Succsess;
            public RequestAwaiterFull.TopicResponse Response;
        }

        public class TopicResponse : IDisposable
        {

            private TaskCompletionSource<RequestAwaiterFull.OutputMessage> _outputTask = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private TaskCompletionSource<bool> _responseProcess = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private Task<RequestAwaiterFull.Response> _response;
            private CancellationTokenSource _cts;
            private readonly string _guid;
            private int _waitResponseTimeout;
            ChannelWriter<RequestAwaiterFull.ChannelInfo> _writer;

            private Action<string> _removeAction;

            public string Guid => _guid;
            public TaskCompletionSource<RequestAwaiterFull.OutputMessage> OutputTask => _outputTask;
            public int Bucket;
            private Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> _getCurrentState;
            private TaskCompletionSource<Input0Message> _input0RAResponder1Task = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private TaskCompletionSource<Input1Message> _input1RAResponder2Task = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private int[] _input0Partitions;
            public int[] Input0Partitions => _input0Partitions;
            private int[] _input1Partitions;
            public int[] Input1Partitions => _input1Partitions;
            public TopicResponse(
                string guid,
                Action<string> removeAction,
                ChannelWriter<RequestAwaiterFull.ChannelInfo> writer,
                Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> getCurrentState,
                int[] input0Partitions,
                int[] input1Partitions,
                int waitResponseTimeout = 0
                )
            {
                _guid = guid;
                _removeAction = removeAction;
                _waitResponseTimeout = waitResponseTimeout;
                _writer = writer;
                _getCurrentState = getCurrentState;
                _input0Partitions = input0Partitions;
                _input1Partitions = input1Partitions;
            }

            public void Init(bool skipSend = false)
            {
                _response = CreateGetResponse(skipSend);
                if (_waitResponseTimeout != 0)
                {
                    _cts = new CancellationTokenSource(_waitResponseTimeout);
                    _cts.Token.Register(() =>
                    {
                        _input0RAResponder1Task.TrySetCanceled();
                        _input1RAResponder2Task.TrySetCanceled();
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

                _responseProcess.Task.ContinueWith(async task =>
                {
                    try
                    {
                        var endResponse = new RequestAwaiterFull.EndResponse
                        {
                            BucketId = this.Bucket,
                            Guid = this.Guid
                        };

                        await _writer.WriteAsync(endResponse).ConfigureAwait(false);
                    }
                    catch
                    {
                        //ignore
                    }

                    _removeAction(_guid);
                }
                );
            }

            private async Task<RequestAwaiterFull.Response> CreateGetResponse(bool skipSend)
            {
                if (!skipSend)
                {
                    var output = await OutputTask.Task;
                    //TODO send
                }

                var input0RAResponder1 = await _input0RAResponder1Task.Task.ConfigureAwait(false);
                await _writer.WriteAsync(
                    new RequestAwaiterFull.SetOffsetResponse
                    {
                        BucketId = this.Bucket,
                        Guid = this.Guid,
                        OffsetId = 0,
                        Offset = input0RAResponder1.TopicPartitionOffset
                    }
                    ).ConfigureAwait(false);
                var input1RAResponder2 = await _input1RAResponder2Task.Task.ConfigureAwait(false);
                await _writer.WriteAsync(
                    new RequestAwaiterFull.SetOffsetResponse
                    {
                        BucketId = this.Bucket,
                        Guid = this.Guid,
                        OffsetId = 1,
                        Offset = input1RAResponder2.TopicPartitionOffset
                    }
                    ).ConfigureAwait(false);
                var currentState = await _getCurrentState(
                    Bucket,
                    _input0Partitions,
                    input0RAResponder1,
                    _input1Partitions,
                    input1RAResponder2
                    );

                var response = new RequestAwaiterFull.Response(
                    Bucket,
                    currentState,
                    _responseProcess,
                    _input0Partitions,
                    input0RAResponder1,
                    _input1Partitions,
                    input1RAResponder2
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
                        return _input0RAResponder1Task.TrySetResult((Input0Message)response);
                    }

                    case (1, 0):
                    {
                        return _input1RAResponder2Task.TrySetResult((Input1Message)response);
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
                        return _input0RAResponder1Task.TrySetException(exception);
                    }

                    case (1, 0):
                    {
                        return _input1RAResponder2Task.TrySetException(exception);
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
                _input0RAResponder1Task.TrySetCanceled();
                _input1RAResponder2Task.TrySetCanceled();
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
            public RequestAwaiterFull.TopicResponse ResponseProcess;

            public readonly TaskCompletionSource BucketReserve = new(TaskCreationOptions.RunContinuationsAsynchronously);
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
            public readonly int[] Input0Partitions;
            private readonly string _input1Name;
            public readonly int[] Input1Partitions;
            private readonly string _output0Name;
            private readonly KafkaExchanger.Common.IProducerPoolNullString _output0Pool;
            private readonly Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> _afterSendOutput0;
            private readonly Func<string, int, int[], int[], Task<Output0Message>> _loadOutput0Message;
            private readonly Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> _checkOutput0Status;
            public PartitionItem(
                int inFlyBucketsLimit,
                int itemsInBucket,
                Func<int, int[], string, int[], string, ValueTask> addNewBucket,
                string input0Name,
                int[] input0Partitions,
                string input1Name,
                int[] input1Partitions,
                ILogger logger,
                Func<int, int[], Input0Message, int[], Input1Message, Task<KafkaExchanger.Attributes.Enums.RAState>> currentState,
                Func<int, int[], int[], Task> afterCommit,
                string output0Name,
                KafkaExchanger.Common.IProducerPoolNullString output0Pool,
                Func<KafkaExchengerTests.RequestHeader, Output0Message, Task> afterSendOutput0,
                Func<string, int, int[], int[], Task<Output0Message>> loadOutput0Message,
                Func<string, int, int[], int[], Task<KafkaExchanger.Attributes.Enums.RAState>> checkOutput0Status
                )
            {
                _itemsInBucket = itemsInBucket;
                _inFlyItemsLimit = inFlyBucketsLimit * itemsInBucket;

                _input0Name = input0Name;
                Input0Partitions = input0Partitions;
                _input1Name = input1Name;
                Input1Partitions = input1Partitions;
                _logger = logger;
                _currentState = currentState;
                _afterCommit = afterCommit;
                _output0Name = output0Name;
                _output0Pool = output0Pool;
                _afterSendOutput0 = afterSendOutput0;
                _loadOutput0Message = loadOutput0Message;
                _checkOutput0Status = checkOutput0Status;
                _storage = new KafkaExchanger.BucketStorage(
                    inFlyLimit: inFlyBucketsLimit,
                    inputs: 2,
                    itemsInBucket: _itemsInBucket,
                    addNewBucket: async (bucketId) =>
                    {
                        await addNewBucket(
                            bucketId,
                            Input0Partitions,
                            _input0Name,
                            Input1Partitions,
                            _input1Name
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
                StartCommitRoutine();
                StartInitializeRoutine();
                _consumeRoutines = new Thread[2];
                _consumeRoutines[0] = StartConsumeInput0(bootstrapServers, groupId, changeConfig);
                _consumeRoutines[0].Start();

                _consumeRoutines[1] = StartConsumeInput1(bootstrapServers, groupId, changeConfig);
                _consumeRoutines[1].Start();

            }

            public async Task Setup(
                Func<int[], string, int[], string, ValueTask<int>> currentBucketsCount
                )
            {
                await _storage.Init(currentBucketsCount: async () =>
                {
                    return await currentBucketsCount(
                            Input0Partitions,
                            _input0Name,
                            Input1Partitions,
                            _input1Name
                            );
                }
                );
            }

            private void StartCommitRoutine()
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
                                if (queue.Count != 0 || inTheFlyCount == _inFlyItemsLimit)
                                {
                                    queue.Enqueue(startResponse);
                                }

                                var newMessage = new KafkaExchanger.MessageInfo(2);
                                var bucketId = await _storage.Push(startResponse.ResponseProcess.Guid, newMessage);
                                startResponse.ResponseProcess.Bucket = bucketId;
                                startResponse.BucketReserve.SetResult();
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
                                Volatile.Write(ref _commitOffsets, canFreeBuckets[^1].MaxOffset);
                                Interlocked.Exchange(ref _tcsCommit, commit);
                                Interlocked.Exchange(ref _needCommit, 1);

                                await commit.Task.ConfigureAwait(false);
                                for (int i = 0; i < canFreeBuckets.Count; i++)
                                {
                                    var freeBucket = canFreeBuckets[i];
                                    await _afterCommit(
                                        freeBucket.BucketId,
                                        Input0Partitions,
                                        Input1Partitions
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

            private Thread StartConsumeInput0(
                string bootstrapServers,
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {
                return new Thread((param) =>
                {
                start:
                    if (_cts.Token.IsCancellationRequested)
                    {
                        return;
                    }

                    try
                    {
                        var conf = new Confluent.Kafka.ConsumerConfig();
                        if (changeConfig != null)
                        {
                            changeConfig(conf);
                        }

                        conf.GroupId = groupId;
                        conf.BootstrapServers = bootstrapServers;
                        conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                        conf.AllowAutoCreateTopics = false;
                        conf.EnableAutoCommit = false;

                        var consumer =
                            new ConsumerBuilder<Confluent.Kafka.Null, System.String>(conf)
                            .Build()
                            ;

                        consumer.Assign(Input0Partitions.Select(sel => new Confluent.Kafka.TopicPartition(_input0Name, sel)));

                        try
                        {
                            while (!_cts.Token.IsCancellationRequested)
                            {
                                try
                                {
                                    var consumeResult = consumer.Consume(50);
                                    if (Interlocked.CompareExchange(ref _needCommit, 0, 1) == 1)
                                    {
                                        var offsets = Volatile.Read(ref _commitOffsets);
                                        consumer.Commit(offsets);
                                        Volatile.Read(ref _tcsCommit).SetResult();
                                    }

                                    if (consumeResult == null)
                                    {
                                        continue;
                                    }

                                    var inputMessage = new Input0Message();
                                    inputMessage.TopicPartitionOffset = consumeResult.TopicPartitionOffset;
                                    inputMessage.OriginalMessage = consumeResult.Message;

                                    if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                    {
                                        continue;
                                    }

                                    inputMessage.Header = KafkaExchengerTests.ResponseHeader.Parser.ParseFrom(infoBytes);
                                    if (!_responseAwaiters.TryGetValue(inputMessage.Header.AnswerToMessageGuid, out var awaiter))
                                    {
                                        continue;
                                    }
                                    switch (inputMessage.Header.AnswerFrom)
                                    {
                                        default:
                                        {
                                            //ignore
                                            break;
                                        }

                                        case "RAResponder1":
                                        {
                                            awaiter.TrySetResponse(0, inputMessage, 0);
                                            break;
                                        }
                                    }
                                }
                                catch (ConsumeException)
                                {
                                    throw;
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            consumer.Close();
                        }
                        finally
                        {
                            consumer.Dispose();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"RequestAwaiterFull{groupId}_input0Name");
                        goto start;
                    }
                }
                    )
                {
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal,
                    Name = $"RequestAwaiterFull{groupId}_input0Name"
                };
            }

            private Thread StartConsumeInput1(
                string bootstrapServers,
                string groupId,
                Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
                )
            {
                return new Thread((param) =>
                {
                start:
                    if (_cts.Token.IsCancellationRequested)
                    {
                        return;
                    }

                    try
                    {
                        var conf = new Confluent.Kafka.ConsumerConfig();
                        if (changeConfig != null)
                        {
                            changeConfig(conf);
                        }

                        conf.GroupId = groupId;
                        conf.BootstrapServers = bootstrapServers;
                        conf.AutoOffsetReset = AutoOffsetReset.Earliest;
                        conf.AllowAutoCreateTopics = false;
                        conf.EnableAutoCommit = false;

                        var consumer =
                            new ConsumerBuilder<Confluent.Kafka.Null, System.String>(conf)
                            .Build()
                            ;

                        consumer.Assign(Input1Partitions.Select(sel => new Confluent.Kafka.TopicPartition(_input1Name, sel)));

                        try
                        {
                            while (!_cts.Token.IsCancellationRequested)
                            {
                                try
                                {
                                    var consumeResult = consumer.Consume(50);
                                    if (Interlocked.CompareExchange(ref _needCommit, 0, 1) == 1)
                                    {
                                        var offsets = Volatile.Read(ref _commitOffsets);
                                        consumer.Commit(offsets);
                                        Volatile.Read(ref _tcsCommit).SetResult();
                                    }

                                    if (consumeResult == null)
                                    {
                                        continue;
                                    }

                                    var inputMessage = new Input1Message();
                                    inputMessage.TopicPartitionOffset = consumeResult.TopicPartitionOffset;
                                    inputMessage.OriginalMessage = consumeResult.Message;

                                    if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                    {
                                        continue;
                                    }

                                    inputMessage.Header = KafkaExchengerTests.ResponseHeader.Parser.ParseFrom(infoBytes);
                                    if (!_responseAwaiters.TryGetValue(inputMessage.Header.AnswerToMessageGuid, out var awaiter))
                                    {
                                        continue;
                                    }
                                    switch (inputMessage.Header.AnswerFrom)
                                    {
                                        default:
                                        {
                                            //ignore
                                            break;
                                        }

                                        case "RAResponder2":
                                        {
                                            awaiter.TrySetResponse(1, inputMessage, 0);
                                            break;
                                        }
                                    }
                                }
                                catch (ConsumeException)
                                {
                                    throw;
                                }
                            }
                        }
                        catch (OperationCanceledException)
                        {
                            consumer.Close();
                        }
                        finally
                        {
                            consumer.Dispose();
                        }
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, $"RequestAwaiterFull{groupId}_input1Name");
                        goto start;
                    }
                }
                    )
                {
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal,
                    Name = $"RequestAwaiterFull{groupId}_input1Name"
                };
            }

            public async ValueTask StopAsync(CancellationToken token = default)
            {

            }

            public async Task<RequestAwaiterFull.DelayProduce> ProduceDelay(
                System.String output0Value,
                int waitResponseTimeout = 0
                )
            {
                var topicResponse = new RequestAwaiterFull.TopicResponse(
                        Guid.NewGuid().ToString("D"),
                        RemoveAwaiter,
                        _channel.Writer,
                        _currentState,
                        Input0Partitions,
                        Input1Partitions,
                        waitResponseTimeout
                        );

                var output = CreateOutput(
                    output0Value
                    );

                var startResponse = new RequestAwaiterFull.StartResponse
                {
                    ResponseProcess = topicResponse
                };

                if (!_responseAwaiters.TryAdd(topicResponse.Guid, topicResponse))
                {
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }

                await _channel.Writer.WriteAsync(startResponse).ConfigureAwait(false);
                await startResponse.BucketReserve.Task.ConfigureAwait(false);
                return
                    new RequestAwaiterFull.DelayProduce(
                        topicResponse,
                        output
                        );
            }

            public async Task<RequestAwaiterFull.Response> Produce(
                System.String output0Value,
                int waitResponseTimeout = 0
                )
            {
                var topicResponse = new RequestAwaiterFull.TopicResponse(
                        Guid.NewGuid().ToString("D"),
                        RemoveAwaiter,
                        _channel.Writer,
                        _currentState,
                        Input0Partitions,
                        Input1Partitions,
                        waitResponseTimeout
                        );

                topicResponse.OutputTask.SetResult(CreateOutput(
                    output0Value
                    )
                );

                var startResponse = new RequestAwaiterFull.StartResponse
                {
                    ResponseProcess = topicResponse
                };

                if (!_responseAwaiters.TryAdd(topicResponse.Guid, topicResponse))
                {
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }

                await _channel.Writer.WriteAsync(startResponse).ConfigureAwait(false);
                await startResponse.BucketReserve.Task.ConfigureAwait(false);
                return
                    await topicResponse.GetResponse().ConfigureAwait(false);
            }

            public RequestAwaiterFull.TopicResponse AddAwaiter(
                string guid,
                int bucket,
                int waitResponseTimeout = 0
                )
            {
                var newMessage = new KafkaExchanger.MessageInfo(2);
                _storage.Push(bucket, guid, newMessage);
                var topicResponse = new RequestAwaiterFull.TopicResponse(
                    guid,
                    RemoveAwaiter,
                    _channel.Writer,
                    _currentState,
                        Input0Partitions,
                        Input1Partitions,
                        waitResponseTimeout
                        );
                if (!_responseAwaiters.TryAdd(topicResponse.Guid, topicResponse))
                {
                    topicResponse.Dispose();
                    throw new InvalidOperationException();
                }
                topicResponse.Init(true);
                return topicResponse;
            }

            public void RemoveAwaiter(string guid)
            {
                if (_responseAwaiters.TryRemove(guid, out var awaiter))
                {
                    awaiter.Dispose();
                }
            }

            private RequestAwaiterFull.OutputMessage CreateOutput(
                System.String output0Value
                )
            {
                var output = new RequestAwaiterFull.OutputMessage
                {
                    Output0Header = CreateOutputHeader(),
                    Output0 = new RequestAwaiterFull.Output0Message(
                        new Confluent.Kafka.Message<Confluent.Kafka.Null, System.String>
                        {
                            Value = output0Value
                        }
                        )
                }
                ;
                return output;
            }

            private KafkaExchengerTests.RequestHeader CreateOutputHeader()
            {
                var header = new KafkaExchengerTests.RequestHeader()
                {
                };
                var topic = new KafkaExchengerTests.Topic()
                {
                    Name = _input0Name
                };
                topic.Partitions.Add(Input0Partitions);
                topic.CanAnswerFrom.Add(new string[] { "RAResponder1" });
                header.TopicsForAnswer.Add(topic);
                topic = new KafkaExchengerTests.Topic()
                {
                    Name = _input1Name
                };
                topic.Partitions.Add(Input1Partitions);
                topic.CanAnswerFrom.Add(new string[] { "RAResponder2" });
                header.TopicsForAnswer.Add(topic);
                return header;
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

        public void Setup(
            RequestAwaiterFull.Config config
,
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
                        processorConfig.Input0.TopicName,
                        processorConfig.Input0.Partitions,
                        processorConfig.Input1.TopicName,
                        processorConfig.Input1.Partitions,
                        processorConfig.Buckets,
                        processorConfig.MaxInFly,
                        _loggerFactory.CreateLogger(config.GroupId),
                        processorConfig.CurrentState,
                        processorConfig.AfterCommit,
                        processorConfig.Output0.TopicName,
                        producerPool0,
                        processorConfig.AfterSendOutput0,
                        processorConfig.LoadOutput0Message,
                        processorConfig.CheckOutput0Status
                        );
            }
        }

        public async Task<RequestAwaiterFull.Response> Produce(
            System.String output0Value,
            int waitResponseTimeout = 0
            )
        {
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            return await
                _items[index].Produce(

                    output0Value,

                    waitResponseTimeout
                );
        }
        private uint _currentItemIndex = 0;

        public async Task<RequestAwaiterFull.DelayProduce> ProduceDelay(
            System.String output0Value,
            int waitResponseTimeout = 0
            )
        {
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            return await
                _items[index].ProduceDelay(

                    output0Value,

                    waitResponseTimeout
                );
        }

        public async Task<RequestAwaiterFull.Response> AddAwaiter(
            string messageGuid,
            int bucket,
            int[] input0Partitions,
            int[] input1Partitions,
            int waitResponseTimeout = 0
            )
        {
            for (int i = 0; i < _items.Length; i++)
            {
                bool skipItem = false;
                var item = _items[i];
                if (input0Partitions == null || input0Partitions.Length == 0)
                {
                    throw new System.Exception("Partitions not specified");
                }

                for (int j = 0; j < input0Partitions.Length; j++)
                {
                    if (!item.Input0Partitions.Contains(input0Partitions[j]))
                    {
                        skipItem = true;
                        break;
                    }
                }

                if (skipItem)
                {
                    continue;
                }

                if (input1Partitions == null || input1Partitions.Length == 0)
                {
                    throw new System.Exception("Partitions not specified");
                }

                for (int j = 0; j < input1Partitions.Length; j++)
                {
                    if (!item.Input1Partitions.Contains(input1Partitions[j]))
                    {
                        skipItem = true;
                        break;
                    }
                }

                if (skipItem)
                {
                    continue;
                }

                return await item.AddAwaiter(
                    messageGuid,
                    bucket,
                    waitResponseTimeout
                    )
                    .GetResponse()
                    .ConfigureAwait(false)
                    ;
            }

            throw new System.Exception("No matching bucket found in combination with partitions");
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
                disposeTasks[i] = items[i].StopAsync(token).AsTask();
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
