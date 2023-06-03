using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Datas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
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

        public void GenerateRequestAwaiter(RequestAwaiterData data, SourceProductionContext context)
        {
            _builder.Clear();
            var producerPair = new ProducerPair(data.OutcomeKeyType, data.OutcomeValueType);

            Start(data);

            Interface(data, producerPair);

            StartClass(data);
            StartMethod(data, producerPair);
            BuildPartitionItems(data, producerPair);
            StopAsync(data);
            Produce(data);
            ChooseItemIndex(data);

            ResponseMessage(data);
            PartitionItem(data, producerPair);

            EndInterfaceOrClass(data);

            End();

            context.AddSource($"{data.TypeSymbol.Name}RequesterAwaiter.g.cs", _builder.ToString());
        }

        private void Start(RequestAwaiterData data)
        {
            _builder.Append($@"
using Confluent.Kafka;
{(data.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;

namespace {data.TypeSymbol.ContainingNamespace}
{{
");
        }

        #region Interface

        private void Interface(RequestAwaiterData data, ProducerPair producerPair)
        {
            StartInterface(data);
            InterfaceMethods(data, producerPair);
            EndInterfaceOrClass(data);
        }

        private void StartInterface(RequestAwaiterData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{data.TypeSymbol.Name}RequestAwaiter
    {{
");
        }

        private void InterfaceMethods(RequestAwaiterData data, ProducerPair producerPair)
        {
            if(data.OutcomeKeyType.IsKafkaNull())
            {
                _builder.Append($@"
        public Task<KafkaExchanger.Common.Response<{data.TypeSymbol.Name}.ResponseMessage>> Produce(
            {data.OutcomeValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            );
");
            }
            else
            {
                _builder.Append($@"
        public Task<KafkaExchanger.Common.Response<{data.TypeSymbol.Name}.ResponseMessage>> Produce(
            {data.OutcomeKeyType.GetFullTypeName(true, true)} key,
            {data.OutcomeValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            );
");
            }

            _builder.Append($@"

        public void Start(KafkaExchanger.Common.ConfigRequestAwaiter config, {producerPair.FullPoolInterfaceName} producerPool);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(RequestAwaiterData data)
        {
            _builder.Append($@"
    }}
");
        }

        #endregion

        private void StartClass(RequestAwaiterData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {data.TypeSymbol.Name} : I{data.TypeSymbol.Name}RequestAwaiter
    {{
        {(data.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;

        public {data.TypeSymbol.Name}({(data.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(data.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartMethod(RequestAwaiterData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        public void Start(KafkaExchanger.Common.ConfigRequestAwaiter config, {producerPair.FullPoolInterfaceName} producerPool)
        {{
            BuildPartitionItems(config, producerPool);

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

        private void BuildPartitionItems(RequestAwaiterData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        private void BuildPartitionItems(KafkaExchanger.Common.ConfigRequestAwaiter config, {producerPair.FullPoolInterfaceName} producerPool)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.OutcomeTopicName,
                        config.ConsumerConfigs[i],
                        producerPool
                        {(data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].TopicName}:Partition{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        );
            }}
        }}
");
        }

        private void StopAsync(RequestAwaiterData data)
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

        private void Produce(RequestAwaiterData data)
        {
            _builder.Append($@"
        public Task<KafkaExchanger.Common.Response<{data.TypeSymbol.Name}.ResponseMessage>> Produce(
");

            if (data.OutcomeKeyType.IsKafkaNull())
            {
                _builder.Append($@"
            {data.OutcomeValueType.GetFullTypeName(true)} value,
            int waitResponceTimeout = 0
            )
        {{
            var item = _items[ChooseItemIndex()];
            return item.Produce(value, waitResponceTimeout);
        }}
");
            }
            else
            {
                _builder.Append($@"
            {data.OutcomeKeyType.GetFullTypeName(true)} key,
            {data.OutcomeValueType.GetFullTypeName(true)} value,
            int waitResponceTimeout = 0
            )
        {{
            var item = _items[ChooseItemIndex()];
            return item.Produce(key, value, waitResponceTimeout);
        }}
");
            }
        }

        private void ChooseItemIndex(RequestAwaiterData data)
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

        private void ResponseMessage(RequestAwaiterData data)
        {
            _builder.Append($@"
        public class ResponseMessage
        {{
            public kafka.ResponseHeader HeaderInfo {{ get; set; }}
            public Message<{GetConsumerTType(data)}> OriginalMessage {{ get; set; }}
");
            if(!data.IncomeKeyType.IsKafkaNull())
            {
                _builder.Append($@"
            public {data.IncomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
            }

            _builder.Append($@"
            public {data.IncomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        #region PartitionItem

        private void PartitionItem(RequestAwaiterData data, ProducerPair producerPair)
        {
            StartPartitionItem(data, producerPair);

            StartConsumePartitionItem(data);
            StopConsumePartitionItem(data);

            StopPartitionItem(data);
            ProducePartitionItem(data, producerPair);
            RemoveAwaiter(data);
            CreateOutcomeHeader(data);

            EndPartitionItem();
        }

        private void StartPartitionItem(RequestAwaiterData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string outcomeTopicName,
                KafkaExchanger.Common.ConsumerConfig consumerConfig,
                {producerPair.FullPoolInterfaceName} producerPool
                {(data.UseLogger ? @",ILogger logger" : "")}
                )
            {{
                Partitions = consumerConfig.Partitions;
                {(data.UseLogger ? @"_logger = logger;" : "")}
                _outcomeTopicName = outcomeTopicName;
                _incomeTopicName = consumerConfig.TopicName;
                _producerPool = producerPool;
            }}

            {(data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _outcomeTopicName;
            private readonly string _incomeTopicName;

            private CancellationTokenSource _ctsConsume;
            private Task _routineConsume;

            public {producerPair.FullPoolInterfaceName} _producerPool;

            public int[] Partitions {{ get; init; }}

            public ConcurrentDictionary<string, KafkaExchanger.Common.TopicResponse<{data.TypeSymbol.Name}.ResponseMessage>> _responceAwaiters = new();

            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                StartConsume(bootstrapServers, groupId);
            }}
");
        }

        private void StartConsumePartitionItem(RequestAwaiterData data)
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
                        new ConsumerBuilder<{GetConsumerTType(data)}>(conf)
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
                                
                                var incomeMessage = new ResponseMessage()
                                {{
                                    OriginalMessage = consumeResult.Message,
                                    {(data.IncomeKeyType.IsKafkaNull() ? "" : $"Key = {GetResponseKey(data)},")}
                                    Value = {GetResponseValue(data)},
                                    Partition = consumeResult.Partition
                                }};

                                {(data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = kafka.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryRemove(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {{
                                    {(data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}': no one wait results"");" : "")}
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
                                    {(data.UseLogger ? @"_logger.LogWarning(""Message must be marked as processed, probably not called FinishProcessing"");" : "")}
                                }}

                                consumer.Commit(consumeResult);
                            }}
                            catch (ConsumeException e)
                            {{
                                {(data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "")}
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

        private string GetConsumerTType(RequestAwaiterData data)
        {
            return $@"{(data.IncomeKeyType.IsProtobuffType() ? "byte[]" : data.IncomeKeyType.GetFullTypeName(true))}, {(data.IncomeValueType.IsProtobuffType() ? "byte[]" : data.IncomeValueType.GetFullTypeName(true))}";
        }

        private string GetResponseKey(RequestAwaiterData data)
        {
            if(data.IncomeKeyType.IsProtobuffType())
            {
                return $"{data.IncomeKeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetResponseValue(RequestAwaiterData data)
        {
            if (data.IncomeValueType.IsProtobuffType())
            {
                return $"{data.IncomeValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void StopConsumePartitionItem(RequestAwaiterData data)
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

        private void StopPartitionItem(RequestAwaiterData data)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private void ProducePartitionItem(RequestAwaiterData data, ProducerPair producerPair)
        {
            _builder.Append($@"
            public async Task<KafkaExchanger.Common.Response<ResponseMessage>> Produce(
");

            if (!data.OutcomeKeyType.IsKafkaNull())
            {
                _builder.Append($@"
                {data.OutcomeKeyType.GetFullTypeName(true)} key,
");
            }

            _builder.Append($@"
                {data.OutcomeValueType.GetFullTypeName(true)} value,
                int waitResponceTimeout = 0
                )
            {{
");
            CreateOutcomeMessage(data, producerPair);

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

                var producer = _producerPool.Rent();
                try
                {{
                    var deliveryResult = await producer.ProduceAsync(_outcomeTopicName, message);
                }}
                catch (ProduceException<{producerPair.TypesPair}> e)
                {{
                    {(data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
                    _responceAwaiters.TryRemove(header.MessageGuid, out _);
                    awaiter.Dispose();

                    throw;
                }}
                finally
                {{
                    _producerPool.Return(producer);
                }}

                return await awaiter.GetResponce();
            }}
");
        }

        private void CreateOutcomeMessage(RequestAwaiterData data, ProducerPair producerPair)
        {
            _builder.Append($@"
                var message = new Message<{producerPair.TypesPair}>()
                {{
");

            if (!data.OutcomeKeyType.IsKafkaNull())
            {
                _builder.Append($@"
                    Key = {(data.OutcomeKeyType.IsProtobuffType() ? "key.ToByteArray()" : "key")},
");
            }

            _builder.Append($@"
                    Value = {(data.OutcomeValueType.IsProtobuffType() ? "value.ToByteArray()" : "value")}
                }};
");
        }

        private void RemoveAwaiter(RequestAwaiterData data)
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

        private void CreateOutcomeHeader(RequestAwaiterData data)
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
