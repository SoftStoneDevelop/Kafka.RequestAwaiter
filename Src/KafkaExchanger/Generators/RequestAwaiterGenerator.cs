using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class RequestAwaiterGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateRequestAwaiter(
            string assemblyName,
            RequestAwaiter requestAwaiter,
            SourceProductionContext context
            )
        {
            _builder.Clear();

            Start(requestAwaiter);

            Interface(assemblyName, requestAwaiter);

            StartClass(requestAwaiter);
            StartMethod(requestAwaiter);
            BuildPartitionItems(requestAwaiter);
            StopAsync();

            ConfigResponder();
            ConsumerResponderConfig(assemblyName, requestAwaiter);

            Produce(assemblyName, requestAwaiter);
            ChooseItemIndex();

            ResponseMessage(assemblyName, requestAwaiter);
            PartitionItem(assemblyName, requestAwaiter);

            EndInterfaceOrClass(requestAwaiter);

            End();

            context.AddSource($"{requestAwaiter.Data.TypeSymbol.Name}RequesterAwaiter.g.cs", _builder.ToString());
        }

        private void Start(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
using Confluent.Kafka;
{(requestAwaiter.Data.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Linq;

namespace {requestAwaiter.Data.TypeSymbol.ContainingNamespace}
{{
");
        }

        #region Interface

        private void Interface(string assemblyName, RequestAwaiter requestAwaiter)
        {
            StartInterface(requestAwaiter);
            InterfaceMethods(assemblyName, requestAwaiter);
            EndInterfaceOrClass(requestAwaiter);
        }

        private void StartInterface(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{requestAwaiter.Data.TypeSymbol.Name}RequestAwaiter
    {{
");
        }

        private void InterfaceMethods(string assemblyName, RequestAwaiter requestAwaiter)
        {
            if (requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
            {
                _builder.Append($@"
        public Task<{assemblyName}.Response<{requestAwaiter.Data.TypeSymbol.Name}.ResponseMessage>> Produce(
            {requestAwaiter.OutcomeDatas[0].ValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            );
");
            }
            else
            {
                _builder.Append($@"
        public Task<{assemblyName}.Response<{requestAwaiter.Data.TypeSymbol.Name}.ResponseMessage>> Produce(
            {requestAwaiter.OutcomeDatas[0].KeyType.GetFullTypeName(true, true)} key,
            {requestAwaiter.OutcomeDatas[0].ValueType.GetFullTypeName(true, true)} value,
            int waitResponceTimeout = 0
            );
");
            }

            _builder.Append($@"

        public void Start({requestAwaiter.Data.TypeSymbol.Name}.ConfigRequestAwaiter config, {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} producerPool);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(RequestAwaiter data)
        {
            _builder.Append($@"
    }}
");
        }

        #endregion

        private void StartClass(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {requestAwaiter.Data.TypeSymbol.Name} : I{requestAwaiter.Data.TypeSymbol.Name}RequestAwaiter
    {{
        {(requestAwaiter.Data.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;

        public {requestAwaiter.Data.TypeSymbol.Name}({(requestAwaiter.Data.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(requestAwaiter.Data.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartMethod(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public void Start({requestAwaiter.Data.TypeSymbol.Name}.ConfigRequestAwaiter config, {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} producerPool)
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

        private void BuildPartitionItems(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        private void BuildPartitionItems({requestAwaiter.Data.TypeSymbol.Name}.ConfigRequestAwaiter config, {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} producerPool)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ConsumerConfigs[i].IncomeTopicName,
                        config.OutcomeTopicName,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        {(requestAwaiter.Data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].IncomeTopicName}:Partition{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? @",config.ConsumerConfigs[i].CreateOutcomeHeader" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",config.ConsumerConfigs[i].SetHeaders" : "")}
                        );
            }}
        }}
");
        }

        private void StopAsync()
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

        private void ConfigResponder()
        {
            _builder.Append($@"
        public class ConfigRequestAwaiter
        {{
            public ConfigRequestAwaiter(
                string groupId,
                string bootstrapServers,
                string outcomeTopicName,
                ConsumerRequestAwaiterConfig[] consumerConfigs
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                OutcomeTopicName = outcomeTopicName;
                ConsumerConfigs = consumerConfigs;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public string OutcomeTopicName {{ get; init; }}

            public ConsumerRequestAwaiterConfig[] ConsumerConfigs {{ get; init; }}
        }}
");
        }

        private void ConsumerResponderConfig(string assemblyName, RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public class ConsumerRequestAwaiterConfig
        {{
            public ConsumerRequestAwaiterConfig(
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@"Func<Task<{assemblyName}.ResponseHeader>> createOutcomeHeader," : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @"Func<Headers, Task> setHeaders," : "")}
                string incomeTopicName,
                params int[] partitions
                )
            {{
                IncomeTopicName = incomeTopicName;
                Partitions = partitions;
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? @"CreateOutcomeHeader = createOutcomeHeader;" : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
            }}

            {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $"public Func<Task<{assemblyName}.ResponseHeader>> CreateOutcomeHeader {{ get; init; }}" : "")}
            {(requestAwaiter.Data.ProducerData.CustomHeaders ? "public Func<Headers, Task> SetHeaders { get; init; }" : "")}

            public string IncomeTopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}
        }}
");
        }

        private void Produce(string assemblyName, RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public Task<{assemblyName}.Response<{requestAwaiter.Data.TypeSymbol.Name}.ResponseMessage>> Produce(
");

            if (requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
            {
                _builder.Append($@"
            {requestAwaiter.OutcomeDatas[0].ValueType.GetFullTypeName(true)} value,
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
            {requestAwaiter.OutcomeDatas[0].KeyType.GetFullTypeName(true)} key,
            {requestAwaiter.OutcomeDatas[0].ValueType.GetFullTypeName(true)} value,
            int waitResponceTimeout = 0
            )
        {{
            var item = _items[ChooseItemIndex()];
            return item.Produce(key, value, waitResponceTimeout);
        }}
");
            }
        }

        private void ChooseItemIndex()
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

        private void ResponseMessage(string assemblyName, RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public class ResponseMessage
        {{
            public {assemblyName}.ResponseHeader HeaderInfo {{ get; set; }}
            public Message<{GetConsumerTType(requestAwaiter)}> OriginalMessage {{ get; set; }}
");
            if (!requestAwaiter.IncomeDatas[0].KeyType.IsKafkaNull())
            {
                _builder.Append($@"
            public {requestAwaiter.IncomeDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
            }

            _builder.Append($@"
            public {requestAwaiter.IncomeDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        #region PartitionItem

        private void PartitionItem(string assemblyName, RequestAwaiter requestAwaiter)
        {
            StartPartitionItem(assemblyName, requestAwaiter);

            StartConsumePartitionItem(assemblyName, requestAwaiter);
            StopConsumePartitionItem();

            StopPartitionItem();
            ProducePartitionItem(assemblyName, requestAwaiter);
            RemoveAwaiter();
            CreateOutcomeHeader(assemblyName, requestAwaiter);

            EndPartitionItem();
        }

        private void StartPartitionItem(string assemblyName, RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                string outcomeTopicName,
                int[] partitions,
                {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} producerPool
                {(requestAwaiter.Data.UseLogger ? @",ILogger logger" : "")}
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@",Func<Task<{assemblyName}.ResponseHeader>> createOutcomeHeader" : "")}
                {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",Func<Headers, Task> setHeaders" : "")}
                )
            {{
                Partitions = partitions;
                {(requestAwaiter.Data.UseLogger ? @"_logger = logger;" : "")}
                _outcomeTopicName = outcomeTopicName;
                _incomeTopicName = incomeTopicName;
                _producerPool = producerPool;
            }}

            {(requestAwaiter.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _outcomeTopicName;
            private readonly string _incomeTopicName;

            {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? $@"private readonly Func<Task<{assemblyName}.ResponseHeader>> _createOutcomeHeader;" : "")}
            {(requestAwaiter.Data.ProducerData.CustomHeaders ? @"private readonly Func<Headers, Task> _setHeaders;" : "")}

            private CancellationTokenSource _ctsConsume;
            private Task _routineConsume;

            public {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} _producerPool;

            public int[] Partitions {{ get; init; }}

            public ConcurrentDictionary<string, {assemblyName}.TopicResponse<{requestAwaiter.Data.TypeSymbol.Name}.ResponseMessage>> _responceAwaiters = new();

            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                StartConsume(bootstrapServers, groupId);
            }}
");
        }

        private void StartConsumePartitionItem(string assemblyName, RequestAwaiter requestAwaiter)
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
                        new ConsumerBuilder<{GetConsumerTType(requestAwaiter)}>(conf)
                        .Build()
                        ;

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_incomeTopicName, sel)));

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
                                    {(requestAwaiter.IncomeDatas[0].KeyType.IsKafkaNull() ? "" : $"Key = {GetResponseKey(requestAwaiter)},")}
                                    Value = {GetResponseValue(requestAwaiter)},
                                    Partition = consumeResult.Partition
                                }};

                                {(requestAwaiter.Data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = {assemblyName}.ResponseHeader.Parser.ParseFrom(infoBytes);

                                if (!_responceAwaiters.TryRemove(incomeMessage.HeaderInfo.AnswerToMessageGuid, out var awaiter))
                                {{
                                    {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}': no one wait results"");" : "")}
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
                                    {(requestAwaiter.Data.UseLogger ? @"_logger.LogWarning(""Message must be marked as processed, probably not called FinishProcessing"");" : "")}
                                }}

                                consumer.Commit(consumeResult);
                            }}
                            catch (ConsumeException e)
                            {{
                                {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "")}
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

        private string GetConsumerTType(RequestAwaiter requestAwaiter)
        {
            return $@"{(requestAwaiter.IncomeDatas[0].KeyType.IsProtobuffType() ? "byte[]" : requestAwaiter.IncomeDatas[0].KeyType.GetFullTypeName(true))}, {(requestAwaiter.IncomeDatas[0].ValueType.IsProtobuffType() ? "byte[]" : requestAwaiter.IncomeDatas[0].ValueType.GetFullTypeName(true))}";
        }

        private string GetResponseKey(RequestAwaiter requestAwaiter)
        {
            if (requestAwaiter.IncomeDatas[0].KeyType.IsProtobuffType())
            {
                return $"{requestAwaiter.IncomeDatas[0].KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetResponseValue(RequestAwaiter requestAwaiter)
        {
            if (requestAwaiter.IncomeDatas[0].ValueType.IsProtobuffType())
            {
                return $"{requestAwaiter.IncomeDatas[0].ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void StopConsumePartitionItem()
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

        private void StopPartitionItem()
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private void ProducePartitionItem(string assemblyName, RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
            public async Task<{assemblyName}.Response<ResponseMessage>> Produce(
");

            if (!requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
            {
                _builder.Append($@"
                {requestAwaiter.OutcomeDatas[0].KeyType.GetFullTypeName(true)} key,
");
            }

            _builder.Append($@"
                {requestAwaiter.OutcomeDatas[0].ValueType.GetFullTypeName(true)} value,
                int waitResponceTimeout = 0
                )
            {{
");
            CreateOutcomeMessage(requestAwaiter);

            _builder.Append($@"
                {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? "var header = await _createOutcomeHeader();" : "var header = CreateOutcomeHeader();")}
                message.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                {(requestAwaiter.Data.ProducerData.CustomHeaders ? "await _setHeaders(message.Headers);" : "")}

                var awaiter = new {assemblyName}.TopicResponse<ResponseMessage>(header.MessageGuid, RemoveAwaiter, waitResponceTimeout);
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
                catch (ProduceException<{requestAwaiter.OutcomeDatas[0].TypesPair}> e)
                {{
                    {(requestAwaiter.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "")}
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

        private void CreateOutcomeMessage(RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
                var message = new Message<{requestAwaiter.OutcomeDatas[0].TypesPair}>()
                {{
");

            if (!requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
            {
                _builder.Append($@"
                    Key = {(requestAwaiter.OutcomeDatas[0].KeyType.IsProtobuffType() ? "key.ToByteArray()" : "key")},
");
            }

            _builder.Append($@"
                    Value = {(requestAwaiter.OutcomeDatas[0].ValueType.IsProtobuffType() ? "value.ToByteArray()" : "value")}
                }};
");
        }

        private void RemoveAwaiter()
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

        private void CreateOutcomeHeader(string assemblyName, RequestAwaiter requestAwaiter)
        {
            if (requestAwaiter.Data.ProducerData.CustomOutcomeHeader)
            {
                //nothing
            }
            else
            {
                _builder.Append($@"
            private {assemblyName}.RequestHeader CreateOutcomeHeader()
            {{
                var guid = Guid.NewGuid();
                var headerInfo = new {assemblyName}.RequestHeader()
                {{
                    MessageGuid = guid.ToString(""D"")
                }};

                var topic = new {assemblyName}.Topic()
                {{
                    Name = _incomeTopicName
                }};

                topic.Partitions.Add(Partitions);

                headerInfo.TopicsForAnswer.Add(topic);

                return headerInfo;
            }}
");
            }
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