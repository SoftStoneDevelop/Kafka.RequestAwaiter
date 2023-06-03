using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Datas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Reflection.Metadata;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class ResponderGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateResponder(ResponderData data, SourceProductionContext context)
        {
            _builder.Clear();

            var producerPair = new ProducerPair(data.OutcomeKeyType, data.OutcomeValueType);
            Start(data);

            Interface(data, producerPair);

            ResponderClass(data, producerPair);

            End();

            context.AddSource($"{data.TypeSymbol.Name}Responder.g.cs", _builder.ToString());
        }

        private void ResponderClass(ResponderData data, ProducerPair producerPair)
        {
            StartClass(data);

            StartResponderMethod(data, producerPair);
            BuildPartitionItems(data, producerPair);
            StopAsync(data);

            ConfigResponder(data);
            ConsumerResponderConfig(data);

            IncomeMessage(data);
            OutcomeMessage(data);

            PartitionItem(data, producerPair);

            EndInterfaceOrClass(data);
        }

        private void PartitionItem(ResponderData data, ProducerPair producerPair)
        {
            PartitionItemStartClass(data, producerPair);
            PartitionItemStartMethod(data);

            PartitionItemStartConsume(data);
            PartitionItemStopConsume(data);

            PartitionItemStop(data);
            PartitionItemProduce(data, producerPair);
            CreateOutcomeHeader(data);

            _builder.Append($@"
        }}
");
        }

        private void Start(ResponderData data)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
{(data.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {data.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(ResponderData data, ProducerPair producerPair)
        {
            StartInterface(data);
            InterfaceMethods(data, producerPair);
            EndInterfaceOrClass(data);
        }

        private void StartInterface(ResponderData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{data.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigResponder config, {producerPair.FullPoolInterfaceName} producerPool);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(ResponderData data)
        {
            _builder.Append($@"
    }}
");
        }

        private void StartClass(ResponderData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {data.TypeSymbol.Name} : I{data.TypeSymbol.Name}Responder
    {{
        {(data.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {data.TypeSymbol.Name}({(data.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(data.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartResponderMethod(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigResponder config, {producerPair.FullPoolInterfaceName} producerPool)
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

        private void BuildPartitionItems(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        private void BuildPartitionItems({data.TypeSymbol.Name}.ConfigResponder config, {producerPair.FullPoolInterfaceName} producerPool)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ConsumerConfigs[i].IncomeTopicName,
                        config.ConsumerConfigs[i].CreateAnswer,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        {(data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].IncomeTopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        {(data.ConsumerData.CheckCurrentState ? @",config.ConsumerConfigs[i].GetCurrentState" : "")}
                        {(data.ConsumerData.UseAfterCommit ? @",config.ConsumerConfigs[i].AfterCommit" : "")}
                        {(data.ProducerData.AfterSendResponse ? @",config.ConsumerConfigs[i].AfterSendResponse" : "")}
                        {(data.ProducerData.CustomOutcomeHeader ? @",config.ConsumerConfigs[i].CreateOutcomeHeader" : "")}
                        {(data.ProducerData.CustomHeaders ? @",config.ConsumerConfigs[i].SetHeaders" : "")}
                        );
            }}
        }}
");
        }

        private void StopAsync(ResponderData data)
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

        private void ConfigResponder(ResponderData data)
        {
            _builder.Append($@"
        public class ConfigResponder
        {{
            public ConfigResponder(
                string groupId,
                string bootstrapServers,
                ConsumerResponderConfig[] consumerConfigs
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                ConsumerConfigs = consumerConfigs;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public ConsumerResponderConfig[] ConsumerConfigs {{ get; init; }}
        }}
");
        }

        private void ConsumerResponderConfig(ResponderData data)
        {
            _builder.Append($@"
        public class ConsumerResponderConfig
        {{
            public ConsumerResponderConfig(
                Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> createAnswer,
                {(data.ConsumerData.CheckCurrentState ? "Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState," : "")}
                {(data.ConsumerData.UseAfterCommit ? "Func<HashSet<int>,Task> afterCommit," : "")}
                {(data.ProducerData.AfterSendResponse ? @"Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> afterSendResponse," : "")}
                {(data.ProducerData.CustomOutcomeHeader ? @"Func<kafka.RequestHeader, Task<kafka.ResponseHeader>> createOutcomeHeader," : "")}
                {(data.ProducerData.CustomHeaders ? @"Func<Headers, Task> setHeaders," : "")}
                string incomeTopicName,
                params int[] partitions
                )
            {{
                CreateAnswer = createAnswer;
                IncomeTopicName = incomeTopicName;
                Partitions = partitions;

                {(data.ConsumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(data.ConsumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(data.ProducerData.AfterSendResponse ? @"AfterSendResponse = afterSendResponse;" : "")}
                {(data.ProducerData.CustomOutcomeHeader ? @"CreateOutcomeHeader = createOutcomeHeader;" : "")}
                {(data.ProducerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
            }}

            public string IncomeTopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}

            public Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> CreateAnswer {{ get; init; }}
            {(data.ConsumerData.CheckCurrentState ? "public Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> GetCurrentState { get; init; }" : "")}
            {(data.ConsumerData.UseAfterCommit ? "public Func<HashSet<int>,Task> AfterCommit { get; init; }" : "")}
            {(data.ProducerData.AfterSendResponse ? "public Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> AfterSendResponse { get; init; }" : "")}
            {(data.ProducerData.CustomOutcomeHeader ? "public Func<kafka.RequestHeader, Task<kafka.ResponseHeader>> CreateOutcomeHeader { get; init; }" : "")}
            {(data.ProducerData.CustomHeaders ? "public Func<Headers, Task> SetHeaders { get; init; }" : "")}
        }}
");
        }

        private void IncomeMessage(ResponderData data)
        {
            _builder.Append($@"
        public class IncomeMessage
        {{
            public Message<{GetConsumerTType(data)}> OriginalMessage {{ get; set; }}
            public {data.IncomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {data.IncomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public kafka.RequestHeader HeaderInfo {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private void OutcomeMessage(ResponderData data)
        {
            _builder.Append($@"
        public class OutcomeMessage
        {{
            public {data.OutcomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {data.OutcomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
        }

        private void PartitionItemStartClass(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> createAnswer,
                int[] partitions,
                {producerPair.FullPoolInterfaceName} producerPool
                {(data.UseLogger ? @",ILogger logger" : "")}
                {(data.ConsumerData.CheckCurrentState ? @",Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState" : "")}
                {(data.ConsumerData.UseAfterCommit ? @",Func<HashSet<int>, Task> afterCommit" : "")}
                {(data.ProducerData.AfterSendResponse ? @",Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> afterSendResponse" : "")}
                {(data.ProducerData.CustomOutcomeHeader ? @",Func<kafka.RequestHeader, Task<kafka.ResponseHeader>> createOutcomeHeader" : "")}
                {(data.ProducerData.CustomHeaders ? @",Func<Headers, Task> setHeaders" : "")}
                )
            {{
                Partitions = partitions;
                {(data.UseLogger ? @"_logger = logger;" : "")}
                _incomeTopicName = incomeTopicName;
                _createAnswer = createAnswer;
                _producerPool = producerPool;
                {(data.ConsumerData.CheckCurrentState ? @"_getCurrentState = getCurrentState;" : "")}
                {(data.ConsumerData.UseAfterCommit ? @"_afterCommit = afterCommit;" : "")}
                {(data.ProducerData.AfterSendResponse ? @"_afterSendResponse = afterSendResponse;" : "")}
                {(data.ProducerData.CustomOutcomeHeader ? @"_createOutcomeHeader = createOutcomeHeader;" : "")}
                {(data.ProducerData.CustomHeaders ? @"_setHeaders = setHeaders;" : "")}
            }}

            {(data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> _createAnswer;
            {(data.ConsumerData.CheckCurrentState ? @"private readonly Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> _getCurrentState;" : "")}
            {(data.ConsumerData.UseAfterCommit ? @"private readonly Func<HashSet<int>, Task> _afterCommit;" : "")}
            {(data.ProducerData.AfterSendResponse ? @"private readonly Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> _afterSendResponse;" : "")}
            {(data.ProducerData.CustomOutcomeHeader ? @"private readonly Func<kafka.RequestHeader, Task<kafka.ResponseHeader>> _createOutcomeHeader;" : "")}
            {(data.ProducerData.CustomHeaders ? @"private readonly Func<Headers, Task> _setHeaders;" : "")}

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            private {producerPair.FullPoolInterfaceName} _producerPool;

            public int[] Partitions {{ get; init; }}
");
        }

        private void PartitionItemStartMethod(ResponderData data)
        {
            _builder.Append($@"
            public void Start(
                string bootstrapServers,
                string groupId
                )
            {{
                StartConsume(bootstrapServers, groupId);
            }}
");
        }

        private void PartitionItemStartConsume(ResponderData data)
        {
            PartitionItemStartStartConsume(data);
            if(data.ConsumerData.CommitAfter <= 1 || 
                (data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForProcess) && data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForResponse)))
            {
                StartConsumeBody(data);
            }
            else if(data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForProcess))
            {
                throw new NotSupportedException();
            }
            else if (data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForResponse))
            {
                throw new NotSupportedException();
            }
            else//NotMatters
            {
                StartConsumeBodyNotMatters(data);
            }

            PartitionItemEndStartConsume(data);
        }

        private void PartitionItemStartStartConsume(ResponderData data)
        {
            _builder.Append($@"
            private void StartConsume(
                string bootstrapServers,
                string groupId
                )
            {{
                _cts = new CancellationTokenSource();
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

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_incomeTopicName, sel)));
");
        }

        private void PartitionItemEndStartConsume(ResponderData data)
        {
            _builder.Append($@"
                }},
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
            );
            }}
");
        }

        private void StartConsumeBody(ResponderData data)
        {
            _builder.Append($@"
                    try
                    {{
                        {(data.ConsumerData.CommitAfter > 1 ? "int mesaggesPast = 0;" : "")}
                        {(data.ConsumerData.UseAfterCommit ? "var partitionsInPackage = new HashSet<int>();" : "")}
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);
                                {(data.ConsumerData.CommitAfter > 1 ? "mesaggesPast++;" : "")}
                                {(data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Add(consumeResult.Partition.Value);" : "")}
                                var incomeMessage = new IncomeMessage();
                                incomeMessage.Partition = consumeResult.Partition;
                                incomeMessage.OriginalMessage = consumeResult.Message;
                                incomeMessage.Key = {GetIncomeMessageKey(data)};
                                incomeMessage.Value = {GetIncomeMessageValue(data)};

                                {(data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = kafka.RequestHeader.Parser.ParseFrom(infoBytes);
                                var currentState = {(data.ConsumerData.CheckCurrentState ? "await _getCurrentState(incomeMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    var answer = await _createAnswer(incomeMessage, currentState);
                                    await Produce(answer, incomeMessage.HeaderInfo);
                                    {(data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, currentState, answer);" : "")}
                                }}
");
            if(data.ConsumerData.CommitAfter > 1)
            {
                _builder.Append($@"
                                if (mesaggesPast == {data.ConsumerData.CommitAfter})
                                {{
                                    consumer.Commit(consumeResult);
                                    {(data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                    {(data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
                                    mesaggesPast = 0;
                                }}
");
            }
            else
            {
                _builder.Append($@"
                                consumer.Commit(consumeResult);
                                {(data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                {(data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
");
            }
            _builder.Append($@"
                            }}
                            catch (ConsumeException e)
                            {{
                                {(data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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
");
        }

        private void StartConsumeBodyNotMatters(ResponderData data)
        {
            _builder.Append($@"
                    var package = new List<Task<Task>>({data.ConsumerData.CommitAfter});
                    {(data.ConsumerData.UseAfterCommit ? "var partitionsInPackage = new HashSet<int>();" : "")}
                    try
                    {{
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);

                                var incomeMessage = new IncomeMessage();
                                {(data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Add(consumeResult.Partition.Value);" : "")}
                                incomeMessage.Partition = consumeResult.Partition;
                                incomeMessage.OriginalMessage = consumeResult.Message;
                                incomeMessage.Key = {GetIncomeMessageKey(data)};
                                incomeMessage.Value = {GetIncomeMessageValue(data)};

                                {(data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = kafka.RequestHeader.Parser.ParseFrom(infoBytes);
                                var currentState = {(data.ConsumerData.CheckCurrentState ? "await _getCurrentState(incomeMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    package.Add(
                                            _createAnswer(incomeMessage, currentState)
                                        .ContinueWith
                                        (async (task) =>
                                        {{
                                            await Produce(task.Result, incomeMessage.HeaderInfo);
                                            {(data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, currentState, task.Result);" : "")}
                                        }},
                                        continuationOptions: TaskContinuationOptions.RunContinuationsAsynchronously
                                        )
                                        );
                                }}
                                else
                                {{
                                    package.Add(Task.FromResult(Task.CompletedTask));
                                }}

                                if (package.Count == {data.ConsumerData.CommitAfter})
                                {{
                                    await Task.WhenAll(await Task.WhenAll(package));
                                    package.Clear();
                                    consumer.Commit(consumeResult);
                                    {(data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                    {(data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
                                }}
                            }}
                            catch (ConsumeException e)
                            {{
                                {(data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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
                        if(package.Count != 0)
                        {{
                            try
                            {{
                                await Task.WhenAll(await Task.WhenAll(package));
                            }}
                            catch
                            {{
                                //ignore
                            }}
                        }}

                        consumer.Dispose();
                    }}
");
        }

        private string GetIncomeMessageKey(ResponderData data)
        {
            if (data.IncomeKeyType.IsProtobuffType())
            {
                return $"{data.IncomeKeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetIncomeMessageValue(ResponderData data)
        {
            if (data.IncomeValueType.IsProtobuffType())
            {
                return $"{data.IncomeValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void PartitionItemStopConsume(ResponderData data)
        {
            _builder.Append($@"
            private async Task StopConsume()
            {{
                _cts?.Cancel();
                if (_routineConsume != null)
                {{
                    await _routineConsume;
                }}

                _cts?.Dispose();
            }}
");
        }

        private void PartitionItemStop(ResponderData data)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private void PartitionItemProduce(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
            private async Task Produce(
                OutcomeMessage outcomeMessage,
                kafka.RequestHeader headerInfo
                )
            {{
");

            CreateOutcomeMessage(data, producerPair);

            _builder.Append($@"
                {(data.ProducerData.CustomOutcomeHeader ? "var header = await _createOutcomeHeader(headerInfo);" : "var header = CreateOutcomeHeader(headerInfo);")}
                message.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                {(data.ProducerData.CustomHeaders ? "await _setHeaders(message.Headers);" : "")}
                
                try
                {{
                    if (!headerInfo.TopicsForAnswer.Any())
                    {{
                        return;
                    }}

                    var topicsForAnswer = headerInfo.TopicsForAnswer.First();
                    var topicPartition = new TopicPartition(topicsForAnswer.Name, topicsForAnswer.Partitions.First());
                    
                    var producer = _producerPool.Rent();
                    try
                    {{
                        var deliveryResult = await producer.ProduceAsync(topicPartition, message);
                    }}
                    finally
                    {{
                        _producerPool.Return(producer);
                    }}
                }}
                catch (ProduceException<{producerPair.TypesPair}> e)
                {{
                    {(data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "//ignore")}
                }}
            }}
");
        }

        private void CreateOutcomeHeader(ResponderData data)
        {
            if(data.ProducerData.CustomOutcomeHeader)
            {
                //nothing
            }
            else
            {
                _builder.Append($@"
            private kafka.ResponseHeader CreateOutcomeHeader(kafka.RequestHeader requestHeaderInfo)
            {{
                var headerInfo = new kafka.ResponseHeader()
                {{
                    AnswerToMessageGuid = requestHeaderInfo.MessageGuid
                }};
                
                return headerInfo;
            }}
");
            }
        }

        private void CreateOutcomeMessage(ResponderData data, ProducerPair producerPair)
        {
            _builder.Append($@"
                var message = new Message<{producerPair.TypesPair}>()
                {{
                    Key = {(data.OutcomeKeyType.IsProtobuffType() ? "outcomeMessage.Key.ToByteArray()" : "outcomeMessage.Key")},
                    Value = {(data.OutcomeValueType.IsProtobuffType() ? "outcomeMessage.Value.ToByteArray()" : "outcomeMessage.Value")}
                }};
");
        }

        private string GetConsumerTType(ResponderData data)
        {
            return $@"{(data.IncomeKeyType.IsProtobuffType() ? "byte[]" : data.IncomeKeyType.GetFullTypeName(true))}, {(data.IncomeValueType.IsProtobuffType() ? "byte[]" : data.IncomeValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}
