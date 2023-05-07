using KafkaExchanger.AttributeDatas;
using Microsoft.CodeAnalysis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class RequestAwaiterGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateRequestAwaiter(RequestAwaiterData ra, GeneratorExecutionContext context)
        {
            _builder.Clear();

            Start(ra);

            Interface(ra);

            StartClass(ra);
            StartMethod(ra);
            BuildPartitionItems(ra);
            StopAsync(ra);
            Produce(ra);
            ChooseItemIndex(ra);

            ResponseMessage(ra);
            PartitionItem(ra);

            EndInterfaceOrClass(ra);

            End();

            context.AddSource($"{ra.TypeSymbol.Name}RequesterAwaiter.g.cs", _builder.ToString());
        }

        private void Start(RequestAwaiterData ra)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;

namespace {ra.TypeSymbol.ContainingNamespace}
{{
");
        }

        #region Interface

        private void Interface(RequestAwaiterData ra)
        {
            StartInterface(ra);
            InterfaceMethods(ra);
            EndInterfaceOrClass(ra);
        }

        private void StartInterface(RequestAwaiterData ra)
        {
            _builder.Append($@"
    public interface I{ra.TypeSymbol.Name}RequestAwaiter
    {{
");
        }

        private void InterfaceMethods(RequestAwaiterData ra)
        {
            _builder.Append($@"
        public Task<KafkaExchanger.Common.Response<{ra.TypeSymbol.Name}.ResponseMessage>> Produce(
            {ra.OutcomeKeyType.GetFullTypeName(true, true)} key,
            {ra.OutcomeValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            );

        public void Start(KafkaExchanger.Common.ConfigRequestAwaiter config);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(RequestAwaiterData ra)
        {
            _builder.Append($@"
    }}
");
        }

        #endregion

        private void StartClass(RequestAwaiterData ra)
        {
            _builder.Append($@"
    public partial class {ra.TypeSymbol.Name} : I{ra.TypeSymbol.Name}RequestAwaiter
    {{
        private readonly ILoggerFactory _loggerFactory;
        private PartitionItem[] _items;
        
        public {ra.TypeSymbol.Name}(ILoggerFactory loggerFactory)
        {{
            _loggerFactory = loggerFactory;
        }}
");
        }

        private void StartMethod(RequestAwaiterData ra)
        {
            _builder.Append($@"
        public void Start(KafkaExchanger.Common.ConfigRequestAwaiter config)
        {{
            BuildPartitionItems(config);

            foreach (var item in _items)
            {{
                item.Start(
                    config.BootstrapServers,
                    config.GroupId
                    );
            }}
        }}
");
        }

        private void BuildPartitionItems(RequestAwaiterData ra)
        {
            _builder.Append($@"
        private void BuildPartitionItems(KafkaExchanger.Common.ConfigRequestAwaiter config)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.OutcomeTopicName,
                        config.ConsumerConfigs[i],
                        _loggerFactory.CreateLogger($""{{config.ConsumerConfigs[i].TopicName}}:Partition{{string.Join(',',config.ConsumerConfigs[i].Partitions)}}"")
                        );
            }}
        }}
");
        }

        private void StopAsync(RequestAwaiterData ra)
        {
            _builder.Append($@"
        public async Task StopAsync()
        {{
            foreach (var item in _items)
            {{
                await item.Stop();
            }}
            
            _items = null;
        }}
");
        }

        private void Produce(RequestAwaiterData ra)
        {
            _builder.Append($@"
        public Task<KafkaExchanger.Common.Response<{ra.TypeSymbol.Name}.ResponseMessage>> Produce(
            {ra.OutcomeKeyType.GetFullTypeName(true, true)} key,
            {ra.OutcomeValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            )
        {{
            var item = _items[ChooseItemIndex()];
            return item.Produce(key, value, waitResponceTimeout);
        }}
");
        }

        private void ChooseItemIndex(RequestAwaiterData ra)
        {
            _builder.Append($@"
        private uint _currentItemIndex = 0;
        private uint ChooseItemIndex()
        {{
            var index = Interlocked.Increment(ref _currentItemIndex);
            return index % (uint)_items.Length;
        }}
");
        }

        private void ResponseMessage(RequestAwaiterData ra)
        {
            _builder.Append($@"
        public class ResponseMessage
        {{
            public Message<{GetConsumerTType(ra)}> OriginalMessage {{ get; set; }}
            public {ra.IncomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {ra.IncomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public kafka.ResponseHeader HeaderInfo {{ get; set; }}
        }}
");
        }

        #region PartitionItem

        private void PartitionItem(RequestAwaiterData ra)
        {
            StartPartitionItem(ra);

            StartConsumePartitionItem(ra);
            StopConsumePartitionItem(ra);

            StartProducePartitionItem(ra);
            StopProducePartitionItem(ra);

            StopPartitionItem(ra);
            ProducePartitionItem(ra);
            RemoveAwaiter(ra);
            CreateOutcomeHeader(ra);

            EndPartitionItem();
        }

        private void StartPartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string outcomeTopicName,
                KafkaExchanger.Common.ConsumerConfig consumerConfig,
                ILogger logger
                )
            {{
                Partitions = consumerConfig.Partitions;
                _logger = logger;
                _outcomeTopicName = outcomeTopicName;
                _incomeTopicName = consumerConfig.TopicName;
            }}

            private readonly ILogger _logger;
            private readonly string _outcomeTopicName;
            private readonly string _incomeTopicName;

            private CancellationTokenSource _ctsConsume;
            private Task _routineConsume;

            public IProducer<{GetProducerTType(ra)}> _producer;

            public int[] Partitions {{ get; init; }}

            public ConcurrentDictionary<string, KafkaExchanger.Common.TopicResponse<{ra.TypeSymbol.Name}.ResponseMessage>> _responceAwaiters = new();

            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                StartConsume(bootstrapServers, groupId);
                StartProduce(bootstrapServers);
            }}
");
        }

        private void StartConsumePartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private void StartConsume(
                string bootstrapServers,
                string groupId
                )
            {{
                _ctsConsume = new CancellationTokenSource();
                _routineConsume = Task.Factory.StartNew(async () =>
                {{
                    var conf = new ConsumerConfig
                    {{
                        GroupId = groupId,
                        BootstrapServers = bootstrapServers,
                        AutoOffsetReset = AutoOffsetReset.Earliest,
                        AllowAutoCreateTopics = false,
                        EnableAutoCommit = false
                    }};

                    var consumer =
                        new ConsumerBuilder<{GetConsumerTType(ra)}>(conf)
                        .Build()
                        ;

                    consumer.Assign(Partitions.Select(partitionId => new TopicPartition(_incomeTopicName, partitionId)));

                    try
                    {{
                        while (!_ctsConsume.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_ctsConsume.Token);
                                
                                var incomeMessage = new ResponseMessage();
                                incomeMessage.OriginalMessage = consumeResult.Message;
                                incomeMessage.Key = {GetResponseKey(ra)};
                                incomeMessage.Value = {GetResponseValue(ra)};

                                _logger.LogInformation($""Consumed incomeMessage 'Key: {{consumeResult.Message.Key}}, Value: {{consumeResult.Message.Value}}'."");
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    _logger.LogError($""Consumed incomeMessage 'Key: {{consumeResult.Message.Key}}, Value: {{consumeResult.Message.Value}}' not contain Info header"");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = kafka.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryRemove(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {{
                                    _logger.LogError($""Consumed incomeMessage 'Key: {{consumeResult.Message.Key}}, Value: {{consumeResult.Message.Value}}': no one wait results"");
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                awaiter.TrySetResponce(incomeMessage);

                                bool isProcessed = false;
                                try
                                {{
                                    isProcessed = await awaiter.GetProcessStatus();
                                }}
                                catch (OperationCanceledException)
                                {{
                                    isProcessed = true;
                                    //ignore
                                }}
                                finally
                                {{
                                    awaiter.Dispose();
                                }}

                                if (!isProcessed)
                                {{
                                    _logger.LogWarning(""Message must be marked as processed, probably not called FinishProcessing"");
                                }}

                                consumer.Commit(consumeResult);
                            }}
                            catch (ConsumeException e)
                            {{
                                _logger.LogError($""Error occured: {{e.Error.Reason}}"");
                            }}
                        }}
                    }}
                    catch (OperationCanceledException)
                    {{
                        // Ensure the consumer leaves the group cleanly and final offsets are committed.
                        consumer.Close();
                    }}
                    finally
                    {{
                        consumer.Dispose();
                    }}
                }},
            _ctsConsume.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
            );
            }}
");
        }

        private string GetProducerTType(RequestAwaiterData ra)
        {
            return $@"{(ra.OutcomeKeyType.IsProtobuffType() ? "byte[]" : ra.OutcomeKeyType.GetFullTypeName(true))}, {(ra.OutcomeValueType.IsProtobuffType() ? "byte[]" : ra.OutcomeValueType.GetFullTypeName(true))}";
        }

        private string GetConsumerTType(RequestAwaiterData ra)
        {
            return $@"{(ra.IncomeKeyType.IsProtobuffType() ? "byte[]" : ra.IncomeKeyType.GetFullTypeName(true))}, {(ra.IncomeValueType.IsProtobuffType() ? "byte[]" : ra.IncomeValueType.GetFullTypeName(true))}";
        }

        private string GetResponseKey(RequestAwaiterData ra)
        {
            if(ra.IncomeKeyType.IsProtobuffType())
            {
                return $"{ra.IncomeKeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetResponseValue(RequestAwaiterData ra)
        {
            if (ra.IncomeValueType.IsProtobuffType())
            {
                return $"{ra.IncomeValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void StopConsumePartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private async Task StopConsume()
            {{
                _ctsConsume?.Cancel();
                if (_routineConsume != null)
                {{
                    await _routineConsume;
                }}

                _ctsConsume?.Dispose();
            }}
");
        }

        private void StartProducePartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private void StartProduce(string bootstrapServers)
            {{
                var config = new ProducerConfig
                {{
                    BootstrapServers = bootstrapServers,
                    AllowAutoCreateTopics = false
                }};

                _producer =
                    new ProducerBuilder<{GetProducerTType(ra)}>(config)
                    .Build()
                    ;
            }}
");
        }

        private void StopProducePartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private void StopProduce()
            {{
                _producer?.Flush();
                _producer?.Dispose();
            }}
");
        }

        private void StopPartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                StopProduce();
                await StopConsume();
            }}
");
        }

        private void ProducePartitionItem(RequestAwaiterData ra)
        {
            _builder.Append($@"
            public async Task<KafkaExchanger.Common.Response<ResponseMessage>> Produce(
                {ra.OutcomeKeyType.GetFullTypeName(true)} key,
                {ra.OutcomeValueType.GetFullTypeName(true)} value,
                int waitResponceTimeout = 0
                )
            {{
");
            CreateOutcomeMessage(ra);

            _builder.Append($@"
                var header = CreateOutcomeHeader();
                message.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                var awaiter = new KafkaExchanger.Common.TopicResponse<ResponseMessage>(header.MessageGuid, RemoveAwaiter, waitResponceTimeout);
                if (!_responceAwaiters.TryAdd(header.MessageGuid, awaiter))
                {{
                    awaiter.Dispose();
                    throw new Exception();
                }}

                try
                {{
                    var deliveryResult = await _producer.ProduceAsync(_outcomeTopicName, message);
                }}
                catch (ProduceException<{GetProducerTType(ra)}> e)
                {{
                    _logger.LogError($""Delivery failed: {{e.Error.Reason}}"");
                    _responceAwaiters.TryRemove(header.MessageGuid, out _);
                    awaiter.Dispose();

                    throw;
                }}

                return await awaiter.GetResponce();
            }}
");
        }

        private void CreateOutcomeMessage(RequestAwaiterData ra)
        {
            _builder.Append($@"
                var message = new Message<{GetProducerTType(ra)}>()
                {{
                    Key = {(ra.OutcomeKeyType.IsProtobuffType() ? "key.ToByteArray()" : "key")},
                    Value = {(ra.OutcomeValueType.IsProtobuffType() ? "value.ToByteArray()" : "value")}
                }};
");
        }

        private void RemoveAwaiter(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private void RemoveAwaiter(string guid)
            {{
                if (_responceAwaiters.TryRemove(guid, out var value))
                {{
                    value.Dispose();
                }}
            }}
");
        }

        private void CreateOutcomeHeader(RequestAwaiterData ra)
        {
            _builder.Append($@"
            private kafka.RequestHeader CreateOutcomeHeader()
            {{
                var guid = Guid.NewGuid();
                var headerInfo = new kafka.RequestHeader()
                {{
                    MessageGuid = guid.ToString(""D"")
                }};

                var topic = new kafka.Topic()
                {{
                    Name = _incomeTopicName
                }};
                topic.Partitions.Add(Partitions);

                headerInfo.TopicsForAnswer.Add(topic);

                return headerInfo;
            }}
");
        }

        private void EndPartitionItem()
        {
            _builder.Append($@"
        }}
");
        }

        #endregion

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}
