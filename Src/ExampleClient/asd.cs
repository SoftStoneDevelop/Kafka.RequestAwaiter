
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;
using System.Collections.Generic;

namespace ExampleClient2
{
    public abstract record BaseResponse(string TopicName);
    public record ResponseItem<T>(string TopicName, T Result) : BaseResponse(TopicName);

    public class Response : IDisposable
    {

        public Response(
            BaseResponse[] response,
            TaskCompletionSource<bool> responseProcess
            )
        {
            Result = response;
            _responseProcess = responseProcess;
        }

        private TaskCompletionSource<bool> _responseProcess;

        public BaseResponse[] Result { get; private set; }

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

            if(disposing)
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
            Result = null;
        }

        ~Response()
        {
            Dispose(false);
        }
    }

    public interface ITestProtobuffAwaiterRequestAwaiter
    {

        public Task<ExampleClient2.Response> Produce(
            protobuff.SimpleKey key,
            protobuff.SimpleValue value,
            int waitResponceTimeout = 0
            );


        public void Start(TestProtobuffAwaiter.Config config, KafkaExchanger.Common.IProducerPoolProtoProto producerPool);

        public Task StopAsync();

    }

    public partial class TestProtobuffAwaiter : ExampleClient2.ITestProtobuffAwaiterRequestAwaiter
    {
        private readonly ILoggerFactory _loggerFactory;
        private PartitionItem[] _items;

        public TestProtobuffAwaiter(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory;
        }

        public void Start(TestProtobuffAwaiter.Config config, KafkaExchanger.Common.IProducerPoolProtoProto producerPool)
        {
            BuildPartitionItems(config, producerPool);

            foreach (var item in _items)
            {
                item.Start(
                    config.BootstrapServers,
                    config.GroupId
                    );
            }
        }

        private void BuildPartitionItems(TestProtobuffAwaiter.Config config, KafkaExchanger.Common.IProducerPoolProtoProto producerPool)
        {
            var items = new List<PartitionItem>();
            for (int i = 0; i < config.Consumers.Length; i++)
            {
                var consumer = config.Consumers[i];

                var item = new PartitionItem(
                    consumer.Income1.TopicName,
                    consumer.Income1.Partitions,
                    consumer.Income1.CanAnswerService,

                    consumer.Income2.TopicName,
                    consumer.Income2.Partitions,
                    consumer.Income2.CanAnswerService,
                    config.OutcomeTopicName,
                    producerPool,
                    _loggerFactory.CreateLogger($"{consumer.GroupName}")
                    );
            }

            _items = items.ToArray();
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
            private Consumers()
            {

            }

            public Consumers(
                string groupName,
                ConsumerInfo income1,
                ConsumerInfo income2
                )
            {
                GroupName = groupName;
                Income1 = income1;
                Income2 = income2;
            }

            /// <summary>
            /// To identify the logger
            /// </summary>
            public string GroupName { get; init; }

            public ConsumerInfo Income1 { get; init; }

            public ConsumerInfo Income2 { get; init; }
        }

        public class ConsumerInfo
        {
            public ConsumerInfo(
                string topicName,
                string [] canAnswerService,
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

        public Task<ExampleClient2.Response> Produce(

            protobuff.SimpleKey key,
            protobuff.SimpleValue value,
            int waitResponceTimeout = 0
            )
        {
            var item = _items[ChooseItemIndex()];
            return item.Produce(key, value, waitResponceTimeout);
        }

        private uint _currentItemIndex = 0;
        private uint ChooseItemIndex()
        {
            var index = Interlocked.Increment(ref _currentItemIndex);
            return index % (uint)_items.Length;
        }

        public abstract class BaseResponseMessage
        {
            public Confluent.Kafka.Partition Partition { get; set; }
        }

        public class ResponseTopic1Message : BaseResponseMessage
        {
            public ExampleClient.ResponseHeader HeaderInfo { get; set; }
            public Message<byte[], byte[]> OriginalMessage { get; set; }

            public protobuff.SimpleKey Key { get; set; }

            public protobuff.SimpleValue Value { get; set; }
        }

        public class ResponseTopic2Message : BaseResponseMessage
        {
            public ExampleClient.ResponseHeader HeaderInfo { get; set; }
            public Message<byte[], byte[]> OriginalMessage { get; set; }

            public protobuff.SimpleKey Key { get; set; }

            public protobuff.SimpleValue Value { get; set; }
        }

        private class TopicResponse : IDisposable
        {
            private TaskCompletionSource<bool> _responseProcess = new();

            private CancellationTokenSource _cts;

            private TaskCompletionSource<ResponseTopic1Message> _responseTopic1 = new();
            private TaskCompletionSource<ResponseTopic2Message> _responseTopic2 = new();

            public Task<ExampleClient2.Response> _response;

            public TopicResponse(
                string topic1Name,
                string topic2Name,
                string guid,
                Action<string> removeAction,
                int waitResponceTimeout = 0
                )
            {
                _response = GetResponse(topic1Name, topic2Name);
                if (waitResponceTimeout != 0)
                {
                    _cts = new CancellationTokenSource(waitResponceTimeout);
                    _cts.Token.Register(() =>
                    {
                        var canceled = _responseTopic1.TrySetCanceled() | _responseTopic2.TrySetCanceled();
                        if (canceled)
                        {
                            removeAction(guid);
                        }
                    },
                    useSynchronizationContext: false
                    );
                }
            }

            private async Task<Response> GetResponse(
                string topic1Name,
                string topic2Name
                )
            {
                var topic1 = await _responseTopic1.Task;
                var topic2 = await _responseTopic2.Task;

                var response = new ExampleClient2.Response(
                    new ExampleClient2.BaseResponse[]
                    {
                            new ResponseItem<ResponseTopic1Message>(topic1Name, topic1),
                            new ResponseItem<ResponseTopic2Message>(topic2Name, topic2)
                    },
                    _responseProcess
                    );

                return response;
            }

            public Task<bool> GetProcessStatus()
            {
                return _responseProcess.Task;
            }

            public Task<ExampleClient2.Response> GetResponce()
            {
                return _response;
            }

            public bool TrySetResponce(int topicNumber, BaseResponseMessage responce)
            {
                switch (topicNumber)
                {
                    case 1:
                    {
                        return _responseTopic1.TrySetResult((ResponseTopic1Message)responce);
                    }

                    case 2:
                    {
                        return _responseTopic2.TrySetResult((ResponseTopic2Message)responce);
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
                    case 1:
                    {
                        _responseTopic1.SetException(exception);
                        break;
                    }

                    case 2:
                    {
                        _responseTopic2.SetException(exception);
                        break;
                    }

                    default:
                        break;
                }
            }

            public void Dispose()
            {
                _cts?.Dispose();
            }
        }

        private class PartitionItem
        {
            public PartitionItem(
                string incomeTopic1Name,
                int[] incomeTopic1Partitions,
                string[] incomeTopic1CanAswerService,
                string incomeTopic2Name,
                int[] incomeTopic2Partitions,
                string[] incomeTopic2CanAswerService,
                string outcomeTopicName,
                KafkaExchanger.Common.IProducerPoolProtoProto producerPool
                , ILogger logger


                )
            {
                _incomeTopic1Name = incomeTopic1Name;
                _incomeTopic1Partitions = incomeTopic1Partitions;
                _incomeTopic1CanAswerService = incomeTopic1CanAswerService;

                _incomeTopic1Name = incomeTopic2Name;
                _incomeTopic2Partitions = incomeTopic2Partitions;
                _incomeTopic2CanAswerService = incomeTopic2CanAswerService;

                _logger = logger;
                _outcomeTopicName = outcomeTopicName;
                _producerPool = producerPool;
            }

            private readonly ILogger _logger;
            private readonly string _outcomeTopicName;

            private readonly string _incomeTopic1Name;
            private readonly int[] _incomeTopic1Partitions;
            private readonly string[] _incomeTopic1CanAswerService;

            private readonly string _incomeTopic2Name;
            private readonly int[] _incomeTopic2Partitions;
            private readonly string[] _incomeTopic2CanAswerService;

            private CancellationTokenSource _ctsConsume;
            private Task[] _consumeRoutines;

            public KafkaExchanger.Common.IProducerPoolProtoProto _producerPool;

            public ConcurrentDictionary<string, TestProtobuffAwaiter.TopicResponse> _responceAwaiters = new();

            public void Start(
                string bootstrapServers,
                string groupId
                )
            {
                _consumeRoutines = new Task[2];
                _consumeRoutines[0] = StartTopic1Consume(bootstrapServers, groupId, _incomeTopic1Partitions, _incomeTopic1Name);
                _consumeRoutines[1] = StartTopic2Consume(bootstrapServers, groupId, _incomeTopic2Partitions, _incomeTopic2Name);
            }

            private Task StartTopic1Consume(
                string bootstrapServers,
                string groupId,
                int[] partitions,
                string incomeTopicName
                )
            {
                _ctsConsume = new CancellationTokenSource();
                return Task.Factory.StartNew(async () =>
                {
                    var conf = new Confluent.Kafka.ConsumerConfig
                    {
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        AllowAutoCreateTopics = false,
                        EnableAutoCommit = false
                    };

                    var consumer =
                        new ConsumerBuilder<byte[], byte[]>(conf)
                        .Build()
                        ;

                    consumer.Assign(partitions.Select(sel => new TopicPartition(incomeTopicName, sel)));

                    try
                    {
                        while (!_ctsConsume.Token.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(_ctsConsume.Token);

                                var incomeMessage = new TestProtobuffAwaiter.ResponseTopic1Message()
                                {
                                    OriginalMessage = consumeResult.Message,
                                    Key = protobuff.SimpleKey.Parser.ParseFrom(consumeResult.Message.Key.AsSpan()),
                                    Value = protobuff.SimpleValue.Parser.ParseFrom(consumeResult.Message.Value.AsSpan()),
                                    Partition = consumeResult.Partition
                                };

                                _logger.LogInformation($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'.");
                                if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                {
                                    _logger.LogError($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }

                                incomeMessage.HeaderInfo = ExampleClient.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryGetValue(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {
                                    _logger.LogError($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}': no one wait results");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }

                                awaiter.TrySetResponce(1, incomeMessage);

                                bool isProcessed = false;
                                try
                                {
                                    isProcessed = await awaiter.GetProcessStatus();
                                }
                                catch (OperationCanceledException)
                                {
                                    isProcessed = true;
                                    //ignore
                                }
                                finally
                                {
                                    awaiter.Dispose();
                                }

                                if (!isProcessed)
                                {
                                    _logger.LogWarning("Message must be marked as processed, probably not called FinishProcessing");
                                }

                                consumer.Commit(consumeResult);
                            }
                            catch (ConsumeException e)
                            {
                                _logger.LogError($"Error occured: {e.Error.Reason}");
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

            private Task StartTopic2Consume(
                string bootstrapServers,
                string groupId,
                int[] partitions,
                string incomeTopicName
                )
            {
                _ctsConsume = new CancellationTokenSource();
                return Task.Factory.StartNew(async () =>
                {
                    var conf = new Confluent.Kafka.ConsumerConfig
                    {
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        AllowAutoCreateTopics = false,
                        EnableAutoCommit = false
                    };

                    var consumer =
                        new ConsumerBuilder<byte[], byte[]>(conf)
                        .Build()
                        ;

                    consumer.Assign(partitions.Select(sel => new TopicPartition(incomeTopicName, sel)));

                    try
                    {
                        while (!_ctsConsume.Token.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(_ctsConsume.Token);

                                var incomeMessage = new TestProtobuffAwaiter.ResponseTopic2Message()
                                {
                                    OriginalMessage = consumeResult.Message,
                                    Key = protobuff.SimpleKey.Parser.ParseFrom(consumeResult.Message.Key.AsSpan()),
                                    Value = protobuff.SimpleValue.Parser.ParseFrom(consumeResult.Message.Value.AsSpan()),
                                    Partition = consumeResult.Partition
                                };

                                _logger.LogInformation($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'.");
                                if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                {
                                    _logger.LogError($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }

                                incomeMessage.HeaderInfo = ExampleClient.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryGetValue(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {
                                    _logger.LogError($"Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}': no one wait results");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }

                                awaiter.TrySetResponce(2, incomeMessage);

                                bool isProcessed = false;
                                try
                                {
                                    isProcessed = await awaiter.GetProcessStatus();
                                }
                                catch (OperationCanceledException)
                                {
                                    isProcessed = true;
                                    //ignore
                                }
                                finally
                                {
                                    awaiter.Dispose();
                                }

                                if (!isProcessed)
                                {
                                    _logger.LogWarning("Message must be marked as processed, probably not called FinishProcessing");
                                }

                                consumer.Commit(consumeResult);
                            }
                            catch (ConsumeException e)
                            {
                                _logger.LogError($"Error occured: {e.Error.Reason}");
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

            private async Task StopConsume()
            {
                _ctsConsume?.Cancel();
                foreach (var consumeRoutine in _consumeRoutines)
                {
                    await consumeRoutine;
                }

                _ctsConsume?.Dispose();
            }

            public async Task Stop()
            {
                await StopConsume();
            }

            public async Task<ExampleClient2.Response> Produce(
                protobuff.SimpleKey key,
                protobuff.SimpleValue value,
                int waitResponceTimeout = 0
                )
            {

                var message = new Message<byte[], byte[]>()
                {
                    Key = key.ToByteArray(),
                    Value = value.ToByteArray()
                };

                var header = CreateOutcomeHeader();
                message.Headers = new Headers
                {
                    { "Info", header.ToByteArray() }
                };



                var awaiter = 
                    new TestProtobuffAwaiter.TopicResponse(
                        _incomeTopic1Name, 
                        _incomeTopic2Name,
                        header.MessageGuid, 
                        RemoveAwaiter, 
                        waitResponceTimeout
                        );
                if (!_responceAwaiters.TryAdd(header.MessageGuid, awaiter))
                {
                    awaiter.Dispose();
                    throw new Exception();
                }

                var producer = _producerPool.Rent();
                try
                {
                    var deliveryResult = await producer.ProduceAsync(_outcomeTopicName, message);
                }
                catch (ProduceException<byte[], byte[]> e)
                {
                    _logger.LogError($"Delivery failed: {e.Error.Reason}");
                    _responceAwaiters.TryRemove(header.MessageGuid, out _);
                    awaiter.Dispose();

                    throw;
                }
                finally
                {
                    _producerPool.Return(producer);
                }

                return await awaiter.GetResponce();
            }

            private void RemoveAwaiter(string guid)
            {
                if (_responceAwaiters.TryRemove(guid, out var value))
                {
                    value.Dispose();
                }
            }

            private ExampleClient.RequestHeader CreateOutcomeHeader()
            {
                var guid = Guid.NewGuid();
                var headerInfo = new ExampleClient.RequestHeader()
                {
                    MessageGuid = guid.ToString("D")
                };

                var topic1 = new ExampleClient.Topic()
                {
                    Name = _incomeTopic1Name
                };

                topic1.Partitions.Add(_incomeTopic1Partitions);
                topic1.CanAnswerFrom.Add(_incomeTopic1CanAswerService);

                headerInfo.TopicsForAnswer.Add(topic1);

                var topic2 = new ExampleClient.Topic()
                {
                    Name = _incomeTopic2Name
                };

                topic2.Partitions.Add(_incomeTopic2Partitions);
                topic2.CanAnswerFrom.Add(_incomeTopic2CanAswerService);

                headerInfo.TopicsForAnswer.Add(topic2);

                return headerInfo;
            }

        }

    }

}
