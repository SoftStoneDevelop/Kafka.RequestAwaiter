
using Confluent.Kafka;
using Google.Protobuf;

using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Channels;
using System.Collections.Concurrent;

namespace KafkaExchengerTests
{

    public interface IResponderNewResponder
    {

        public Task Start(ResponderNew.ConfigResponder config, KafkaExchanger.Common.IProducerPoolNullString producerPool);

        public Task StopAsync();

    }

    public partial class ResponderNew : IResponderNewResponder
    {

        private PartitionItem[] _items;

        public ResponderNew()
        {

        }

        public async Task Start(
            ResponderNew.ConfigResponder config, 
            KafkaExchanger.Common.IProducerPoolNullString producerPool
            )
        {
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {
                _items[i] =
                    new PartitionItem(
                        config.ServiceName,
                        config.ConsumerConfigs[i].InputTopicName,
                        config.ConsumerConfigs[i].CreateAnswer,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        );

                await _items[i].Start(
                    config.BootstrapServers,
                    config.GroupId,
                    config.ConsumerConfigs[i].LoadCurrentHorizon
                    );
            }
        }

        public async Task StopAsync()
        {
            if (_items == null)
            {
                return;
            }

            foreach (var item in _items)
            {
                await item.Stop();
            }

            _items = null;
        }

        public class ConfigResponder
        {
            public ConfigResponder(
                string groupId,
                string serviceName,
                string bootstrapServers,
                ConsumerResponderConfig[] consumerConfigs
                )
            {
                GroupId = groupId;
                ServiceName = serviceName;
                BootstrapServers = bootstrapServers;
                ConsumerConfigs = consumerConfigs;
            }

            public string GroupId { get; init; }

            public string ServiceName { get; init; }

            public string BootstrapServers { get; init; }

            public ConsumerResponderConfig[] ConsumerConfigs { get; init; }
        }

        public class ConsumerResponderConfig
        {
            public ConsumerResponderConfig(
                Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> createAnswer,
                Func<int[], ValueTask<long>> loadCurrentHorizon,


                string inputTopicName,
                params int[] partitions
                )
            {
                CreateAnswer = createAnswer;
                LoadCurrentHorizon = loadCurrentHorizon;
                InputTopicName = inputTopicName;
                Partitions = partitions;




            }

            public string InputTopicName { get; init; }

            public int[] Partitions { get; init; }

            public Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> CreateAnswer { get; init; }

            public Func<int[], ValueTask<long>> LoadCurrentHorizon { get; init; }

        }

        public class InputMessage
        {
            public Input0Message Input0Message { get; set; }
        }

        public class Input0Message : BaseInputMessage
        {
            public Message<Confluent.Kafka.Null, System.String> OriginalMessage { get; set; }
            public Confluent.Kafka.Null Key { get; set; }
            public System.String Value { get; set; }
            public KafkaExchengerTests.RequestHeader Header { get; set; }
        }

        public abstract class BaseInputMessage
        {
            public Confluent.Kafka.TopicPartitionOffset TopicPartitionOffset { get; set; }
        }

        public class OutputMessage
        {
            public Confluent.Kafka.Null Key { get; set; }
            public System.String Value { get; set; }
        }

        private class ResponseProcess
        {
            private Task _response;
            private TaskCompletionSource<Input0Message> _input0 = new(TaskCreationOptions.RunContinuationsAsynchronously);

            public ResponseProcess(
                string guid,
                long horizonId,
                Func<ResponderNew.InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderNew.OutputMessage>> createAnswer,
                Func<ResponderNew.OutputMessage, ResponderNew.InputMessage, Task> produce,
                Action<string> removeAction,
                ChannelWriter<(int, long, Confluent.Kafka.TopicPartitionOffset)> writer
                )
            {
                _response = Response(
                    guid,
                    horizonId,
                    createAnswer,
                    produce,
                    removeAction,
                    writer
                    );
            }

            private async Task Response(
                string guid,
                long horizonId,
                Func<ResponderNew.InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<ResponderNew.OutputMessage>> createAnswer,
                Func<ResponderNew.OutputMessage, ResponderNew.InputMessage, Task> produce,
                Action<string> removeAction,
                ChannelWriter<(int, long, Confluent.Kafka.TopicPartitionOffset)> writer
                )
            {
                var input0 = await _input0.Task.ConfigureAwait(false);
                var input = new ResponderNew.InputMessage()
                {
                    Input0Message = input0
                };

                var currentState = KafkaExchanger.Attributes.Enums.CurrentState.NewMessage;
                if(currentState == KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                {
                    return;
                }

                var answer = await createAnswer(input, currentState).ConfigureAwait(false);
                await produce(answer, input).ConfigureAwait(false);
                await writer.WriteAsync((0, horizonId, input.Input0Message.TopicPartitionOffset)).ConfigureAwait(false);

                removeAction(guid);
            }

            public bool TrySetResponse(int topicNumber, BaseInputMessage response, int serviceNumber = 0)
            {
                switch (topicNumber, serviceNumber)
                {
                    case (0, 0):
                    {
                        return _input0.TrySetResult((Input0Message)response);
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
                        return _input0.TrySetException(exception);
                    }

                    default:
                    {
                        return false;
                    }
                }
            }
        }

        private class PartitionItem
        {
            public PartitionItem(
                string serviceName,
                string inputTopicName,
                Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> createAnswer,
                int[] partitions,
                KafkaExchanger.Common.IProducerPoolNullString producerPool




                )
            {
                Partitions = partitions;

                _serviceName = serviceName;
                _inputTopicName = inputTopicName;
                _createAnswer = createAnswer;
                _producerPool = producerPool;



            }

            private long _horizonId;
            private int _needCommit;
            private TaskCompletionSource _tcsCommit;

            private readonly string _inputTopicName;
            private readonly string _serviceName;
            private readonly Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> _createAnswer;
            private readonly ConcurrentDictionary<string, ResponderNew.ResponseProcess> _responseProcesses;
            private readonly Channel<(int, long, Confluent.Kafka.TopicPartitionOffset)> _channel = Channel.CreateUnbounded<(int, long, Confluent.Kafka.TopicPartitionOffset)>(
                new UnboundedChannelOptions() 
                {
                    AllowSynchronousContinuations = false, 
                    SingleReader = true,
                    SingleWriter = false
                });



            private CancellationTokenSource _cts;
            private Thread[] _consumeRoutines;
            private Task _horizonRoutine;

            private KafkaExchanger.Common.IProducerPoolNullString _producerPool;

            public int[] Partitions { get; init; }

            public async ValueTask Start(
                string bootstrapServers,
                string groupId,
                Func<int[], ValueTask<long>> loadCurrentHorizon
                )
            {
                _horizonId = await loadCurrentHorizon(Partitions);
                StartConsume(bootstrapServers, groupId);
            }

            private void StartConsume(
                string bootstrapServers,
                string groupId
                )
            {
                _cts = new CancellationTokenSource();
                StartHorizonRoutine();
                _consumeRoutines[0] = StartConsumeInput0(bootstrapServers, groupId);
            }

            private Thread StartConsumeInput0(
                string bootstrapServers,
                string groupId
                )
            {
                return new Thread((param) =>
                {
                    var conf = new ConsumerConfig
                    {
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        AllowAutoCreateTopics = false,
                        EnableAutoCommit = false
                    };

                    var consumer =
                        new ConsumerBuilder<Confluent.Kafka.Null, System.String>(conf)
                        .Build()
                        ;

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_inputTopicName, sel)));

                    try
                    {


                        while (!_cts.Token.IsCancellationRequested)
                        {
                            try
                            {
                                var consumeResult = consumer.Consume(50);

                                var needCommit = Interlocked.CompareExchange(ref _needCommit, 0, 1);
                                if (needCommit == 1)
                                {
                                    
                                }

                                if (consumeResult == null)
                                {
                                    continue;
                                }
                                var inputMessage = new Input0Message();
                                inputMessage.TopicPartitionOffset = consumeResult.TopicPartitionOffset;
                                inputMessage.OriginalMessage = consumeResult.Message;
                                inputMessage.Key = consumeResult.Message.Key;
                                inputMessage.Value = consumeResult.Message.Value;


                                if (!consumeResult.Message.Headers.TryGetLastBytes("Info", out var infoBytes))
                                {

                                    continue;
                                }

                                inputMessage.Header = KafkaExchengerTests.RequestHeader.Parser.ParseFrom(infoBytes);
                                if (!inputMessage.Header.TopicsForAnswer.Any(wh => !wh.CanAnswerFrom.Any() || wh.CanAnswerFrom.Contains(_serviceName)))
                                {
                                    continue;
                                }

                                var responseProcess = _responseProcesses.GetOrAdd(
                                    inputMessage.Header.MessageGuid, 
                                    (key) => 
                                    {
                                        var horizonId = Interlocked.Increment(ref _horizonId);
                                        return new ResponseProcess(
                                            key,
                                            horizonId,
                                            _createAnswer, 
                                            Produce, 
                                            (key) => _responseProcesses.TryRemove(key, out _),
                                            _channel.Writer
                                            ); 
                                    }
                                    );

                                responseProcess.TrySetResponse(0, inputMessage);
                            }
                            catch (ConsumeException)
                            {
                                //ignore
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
                )
                {
                    IsBackground = true,
                    Priority = ThreadPriority.AboveNormal,
                    Name = $"{groupId}Topic0"
                }; ;
            }

            private void StartHorizonRoutine()
            {
                _horizonRoutine = Task.Factory.StartNew(async () => 
                {
                    var reader = _channel.Reader;
                    var completedResponse = 0;
                    try
                    {
                        while (!_cts.Token.IsCancellationRequested)
                        {
                            (int inputId, long horizonId, TopicPartitionOffset offset) = await reader.ReadAsync(_cts.Token).ConfigureAwait(false);
                            //TODO DO DO DO
                            //calculate completedResponse

                            if (completedResponse < 100)
                            {
                                continue;
                            }

                            var commit = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                            //TODO write info for commit
                            Volatile.Write(ref _tcsCommit, commit);
                            Volatile.Write(ref _needCommit, 1);

                            await commit.Task.ConfigureAwait(false);
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
                });
            }

            private async Task StopConsume()
            {
                _cts?.Cancel();

                foreach (var consumeRoutine in _consumeRoutines)
                {
                    while (consumeRoutine.IsAlive)
                    {
                        await Task.Delay(50);
                    }
                }

                _cts?.Dispose();
            }

            public async Task Stop()
            {
                await StopConsume();
            }

            private async Task Produce(
                OutputMessage outputMessage,
                ResponderNew.InputMessage inputMessage
                )
            {
                if (!inputMessage.Input0Message.Header.TopicsForAnswer.Any())
                {
                    return;
                }

                var message = new Message<Confluent.Kafka.Null, System.String>()
                {
                    Key = outputMessage.Key,
                    Value = outputMessage.Value
                };

                var header = CreateOutputHeader(inputMessage);
                message.Headers = new Headers
                {
                    { "Info", header.ToByteArray() }
                };

                foreach (var topicForAnswer in inputMessage.Input0Message.Header.TopicsForAnswer.Where(wh => !wh.CanAnswerFrom.Any() || wh.CanAnswerFrom.Contains(_serviceName)))
                {
                    var topicPartition = new TopicPartition(topicForAnswer.Name, topicForAnswer.Partitions.First());
                    var producer = _producerPool.Rent();
                    try
                    {
                        var deliveryResult = await producer.ProduceAsync(topicPartition, message);
                    }
                    catch (ProduceException<Confluent.Kafka.Null, System.String> e)
                    {
                        //ignore
                    }
                    finally
                    {
                        _producerPool.Return(producer);
                    }
                }
            }

            private KafkaExchengerTests.ResponseHeader CreateOutputHeader(ResponderNew.InputMessage inputMessage)
            {
                var headerInfo = new KafkaExchengerTests.ResponseHeader()
                {
                    AnswerToMessageGuid = inputMessage.Input0Message.Header.MessageGuid,
                    AnswerFrom = _serviceName,
                    Bucket = inputMessage.Input0Message.Header.Bucket
                };

                return headerInfo;
            }

        }

    }

}
