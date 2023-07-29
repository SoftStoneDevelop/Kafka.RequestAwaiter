using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class ResponderGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateResponder(string assemblyName, Responder responder, SourceProductionContext context)
        {
            _builder.Clear();

            Start(responder);

            Interface(responder);

            ResponderClass(assemblyName, responder);

            End();

            context.AddSource($"{responder.Data.TypeSymbol.Name}Responder.g.cs", _builder.ToString());
        }

        private void ResponderClass(string assemblyName, Responder responder)
        {
            StartClass(responder);

            StartResponderMethod(responder);
            BuildPartitionItems(responder);
            StopAsync();

            ConfigResponder();
            ConsumerResponderConfig(assemblyName, responder);

            IncomeMessage(assemblyName, responder);
            OutcomeMessage(responder);

            PartitionItem(assemblyName, responder);

            EndInterfaceOrClass();
        }

        private void PartitionItem(string assemblyName, Responder responder)
        {
            PartitionItemStartClass(assemblyName, responder);
            PartitionItemStartMethod();

            PartitionItemStartConsume(assemblyName, responder);
            PartitionItemStopConsume();

            PartitionItemStop();
            PartitionItemProduce(assemblyName, responder);
            CreateOutcomeHeader(assemblyName, responder);

            _builder.Append($@"
        }}
");
        }

        private void Start(Responder responder)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
{(responder.Data.UseLogger ? @"using Microsoft.Extensions.Logging;" : "")}
using System.Collections.Generic;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {responder.Data.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(Responder responder)
        {
            StartInterface(responder);
            InterfaceMethods(responder);
            EndInterfaceOrClass();
        }

        private void StartInterface(Responder responder)
        {
            _builder.Append($@"
    {responder.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{responder.Data.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(Responder responder)
        {
            _builder.Append($@"
        public void Start({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutcomeDatas[0].FullPoolInterfaceName} producerPool);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass()
        {
            _builder.Append($@"
    }}
");
        }

        private void StartClass(Responder responder)
        {
            _builder.Append($@"
    {responder.Data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {responder.Data.TypeSymbol.Name} : I{responder.Data.TypeSymbol.Name}Responder
    {{
        {(responder.Data.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {responder.Data.TypeSymbol.Name}({(responder.Data.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(responder.Data.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartResponderMethod(Responder responder)
        {
            _builder.Append($@"
        public void Start({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutcomeDatas[0].FullPoolInterfaceName} producerPool)
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

        private void BuildPartitionItems(Responder responder)
        {
            _builder.Append($@"
        private void BuildPartitionItems({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutcomeDatas[0].FullPoolInterfaceName} producerPool)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ServiceName,
                        config.ConsumerConfigs[i].IncomeTopicName,
                        config.ConsumerConfigs[i].CreateAnswer,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        {(responder.Data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].IncomeTopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        {(responder.Data.ConsumerData.CheckCurrentState ? @",config.ConsumerConfigs[i].GetCurrentState" : "")}
                        {(responder.Data.ConsumerData.UseAfterCommit ? @",config.ConsumerConfigs[i].AfterCommit" : "")}
                        {(responder.Data.ProducerData.AfterSendResponse ? @",config.ConsumerConfigs[i].AfterSendResponse" : "")}
                        {(responder.Data.ProducerData.CustomOutcomeHeader ? @",config.ConsumerConfigs[i].CreateOutcomeHeader" : "")}
                        {(responder.Data.ProducerData.CustomHeaders ? @",config.ConsumerConfigs[i].SetHeaders" : "")}
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
        public class ConfigResponder
        {{
            public ConfigResponder(
                string groupId,
                string serviceName,
                string bootstrapServers,
                ConsumerResponderConfig[] consumerConfigs
                )
            {{
                GroupId = groupId;
                ServiceName = serviceName;
                BootstrapServers = bootstrapServers;
                ConsumerConfigs = consumerConfigs;
            }}

            public string GroupId {{ get; init; }}

            public string ServiceName {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public ConsumerResponderConfig[] ConsumerConfigs {{ get; init; }}
        }}
");
        }

        private void ConsumerResponderConfig(string assemblyName, Responder responder)
        {
            _builder.Append($@"
        public class ConsumerResponderConfig
        {{
            public ConsumerResponderConfig(
                Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> createAnswer,
                {(responder.Data.ConsumerData.CheckCurrentState ? "Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState," : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? "Func<HashSet<int>,Task> afterCommit," : "")}
                {(responder.Data.ProducerData.AfterSendResponse ? @"Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> afterSendResponse," : "")}
                {(responder.Data.ProducerData.CustomOutcomeHeader ? $@"Func<{assemblyName}.RequestHeader, Task<{assemblyName}.ResponseHeader>> createOutcomeHeader," : "")}
                {(responder.Data.ProducerData.CustomHeaders ? @"Func<Headers, Task> setHeaders," : "")}
                string incomeTopicName,
                params int[] partitions
                )
            {{
                CreateAnswer = createAnswer;
                IncomeTopicName = incomeTopicName;
                Partitions = partitions;

                {(responder.Data.ConsumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(responder.Data.ProducerData.AfterSendResponse ? @"AfterSendResponse = afterSendResponse;" : "")}
                {(responder.Data.ProducerData.CustomOutcomeHeader ? @"CreateOutcomeHeader = createOutcomeHeader;" : "")}
                {(responder.Data.ProducerData.CustomHeaders ? @"SetHeaders = setHeaders;" : "")}
            }}

            public string IncomeTopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}

            public Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> CreateAnswer {{ get; init; }}
            {(responder.Data.ConsumerData.CheckCurrentState ? "public Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> GetCurrentState { get; init; }" : "")}
            {(responder.Data.ConsumerData.UseAfterCommit ? "public Func<HashSet<int>,Task> AfterCommit { get; init; }" : "")}
            {(responder.Data.ProducerData.AfterSendResponse ? "public Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> AfterSendResponse { get; init; }" : "")}
            {(responder.Data.ProducerData.CustomOutcomeHeader ? $"public Func<{assemblyName}.RequestHeader, Task<{assemblyName}.ResponseHeader>> CreateOutcomeHeader {{ get; init; }}" : "")}
            {(responder.Data.ProducerData.CustomHeaders ? "public Func<Headers, Task> SetHeaders { get; init; }" : "")}
        }}
");
        }

        private void IncomeMessage(string assemblyName, Responder responder)
        {
            _builder.Append($@"
        public class IncomeMessage
        {{
            public Message<{GetConsumerTType(responder)}> OriginalMessage {{ get; set; }}
            public {responder.IncomeDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {responder.IncomeDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public {assemblyName}.RequestHeader HeaderInfo {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private void OutcomeMessage(Responder responder)
        {
            _builder.Append($@"
        public class OutcomeMessage
        {{
            public {responder.OutcomeDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {responder.OutcomeDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
        }

        private void PartitionItemStartClass(string assemblyName, Responder responder)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string serviceName,
                string incomeTopicName,
                Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> createAnswer,
                int[] partitions,
                {responder.OutcomeDatas[0].FullPoolInterfaceName} producerPool
                {(responder.Data.UseLogger ? @",ILogger logger" : "")}
                {(responder.Data.ConsumerData.CheckCurrentState ? @",Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? @",Func<HashSet<int>, Task> afterCommit" : "")}
                {(responder.Data.ProducerData.AfterSendResponse ? @",Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> afterSendResponse" : "")}
                {(responder.Data.ProducerData.CustomOutcomeHeader ? $@",Func<{assemblyName}.RequestHeader, Task<{assemblyName}.ResponseHeader>> createOutcomeHeader" : "")}
                {(responder.Data.ProducerData.CustomHeaders ? @",Func<Headers, Task> setHeaders" : "")}
                )
            {{
                Partitions = partitions;
                {(responder.Data.UseLogger ? @"_logger = logger;" : "")}
                _serviceName = serviceName;
                _incomeTopicName = incomeTopicName;
                _createAnswer = createAnswer;
                _producerPool = producerPool;
                {(responder.Data.ConsumerData.CheckCurrentState ? @"_getCurrentState = getCurrentState;" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? @"_afterCommit = afterCommit;" : "")}
                {(responder.Data.ProducerData.AfterSendResponse ? @"_afterSendResponse = afterSendResponse;" : "")}
                {(responder.Data.ProducerData.CustomOutcomeHeader ? @"_createOutcomeHeader = createOutcomeHeader;" : "")}
                {(responder.Data.ProducerData.CustomHeaders ? @"_setHeaders = setHeaders;" : "")}
            }}

            {(responder.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly string _serviceName;
            private readonly Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutcomeMessage>> _createAnswer;
            {(responder.Data.ConsumerData.CheckCurrentState ? @"private readonly Func<IncomeMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> _getCurrentState;" : "")}
            {(responder.Data.ConsumerData.UseAfterCommit ? @"private readonly Func<HashSet<int>, Task> _afterCommit;" : "")}
            {(responder.Data.ProducerData.AfterSendResponse ? @"private readonly Func<IncomeMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutcomeMessage, Task> _afterSendResponse;" : "")}
            {(responder.Data.ProducerData.CustomOutcomeHeader ? $@"private readonly Func<{assemblyName}.RequestHeader, Task<{assemblyName}.ResponseHeader>> _createOutcomeHeader;" : "")}
            {(responder.Data.ProducerData.CustomHeaders ? @"private readonly Func<Headers, Task> _setHeaders;" : "")}

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            private {responder.OutcomeDatas[0].FullPoolInterfaceName} _producerPool;

            public int[] Partitions {{ get; init; }}
");
        }

        private void PartitionItemStartMethod()
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

        private void PartitionItemStartConsume(string assemblyName, Responder responder)
        {
            PartitionItemStartStartConsume(responder);
            if(responder.Data.ConsumerData.CommitAfter <= 1 || 
                (responder.Data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForProcess) && responder.Data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForResponse)))
            {
                StartConsumeBody(assemblyName, responder);
            }
            else if(responder.Data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForProcess))
            {
                throw new NotSupportedException();
            }
            else if (responder.Data.ConsumerData.OrderMatters.HasFlag(Enums.OrderMatters.ForResponse))
            {
                throw new NotSupportedException();
            }
            else//NotMatters
            {
                StartConsumeBodyNotMatters(assemblyName, responder);
            }

            PartitionItemEndStartConsume();
        }

        private void PartitionItemStartStartConsume(Responder responder)
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
                        new ConsumerBuilder<{GetConsumerTType(responder)}>(conf)
                        .Build()
                        ;

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_incomeTopicName, sel)));
");
        }

        private void PartitionItemEndStartConsume()
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

        private void StartConsumeBody(string assemblyName, Responder responder)
        {
            _builder.Append($@"
                    try
                    {{
                        {(responder.Data.ConsumerData.CommitAfter > 1 ? "int mesaggesPast = 0;" : "")}
                        {(responder.Data.ConsumerData.UseAfterCommit ? "var partitionsInPackage = new HashSet<int>();" : "")}
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);
                                {(responder.Data.ConsumerData.CommitAfter > 1 ? "mesaggesPast++;" : "")}
                                {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Add(consumeResult.Partition.Value);" : "")}
                                var incomeMessage = new IncomeMessage();
                                incomeMessage.Partition = consumeResult.Partition;
                                incomeMessage.OriginalMessage = consumeResult.Message;
                                incomeMessage.Key = {GetIncomeMessageKey(responder)};
                                incomeMessage.Value = {GetIncomeMessageValue(responder)};

                                {(responder.Data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(responder.Data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    continue;
                                }}
                                
                                incomeMessage.HeaderInfo = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);
                                if(!incomeMessage.HeaderInfo.TopicsForAnswer.Any(wh => wh.CanAnswerFrom.Contains(_serviceName)))
                                {{
                                    continue;
                                }}

                                var currentState = {(responder.Data.ConsumerData.CheckCurrentState ? "await _getCurrentState(incomeMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    var answer = await _createAnswer(incomeMessage, currentState);
                                    await Produce(answer, incomeMessage.HeaderInfo);
                                    {(responder.Data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, currentState, answer);" : "")}
                                }}
");
            if(responder.Data.ConsumerData.CommitAfter > 1)
            {
                _builder.Append($@"
                                if (mesaggesPast == {responder.Data.ConsumerData.CommitAfter})
                                {{
                                    consumer.Commit(consumeResult);
                                    {(responder.Data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                    {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
                                    mesaggesPast = 0;
                                }}
");
            }
            else
            {
                _builder.Append($@"
                                consumer.Commit(consumeResult);
                                {(responder.Data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
");
            }
            _builder.Append($@"
                            }}
                            catch (ConsumeException e)
                            {{
                                {(responder.Data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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

        private void StartConsumeBodyNotMatters(string assemblyName, Responder responder)
        {
            _builder.Append($@"
                    var package = new List<Task<Task>>({responder.Data.ConsumerData.CommitAfter});
                    {(responder.Data.ConsumerData.UseAfterCommit ? "var partitionsInPackage = new HashSet<int>();" : "")}
                    try
                    {{
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);

                                var incomeMessage = new IncomeMessage();
                                {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Add(consumeResult.Partition.Value);" : "")}
                                incomeMessage.Partition = consumeResult.Partition;
                                incomeMessage.OriginalMessage = consumeResult.Message;
                                incomeMessage.Key = {GetIncomeMessageKey(responder)};
                                incomeMessage.Value = {GetIncomeMessageValue(responder)};

                                {(responder.Data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(responder.Data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);
                                var currentState = {(responder.Data.ConsumerData.CheckCurrentState ? "await _getCurrentState(incomeMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    package.Add(
                                            _createAnswer(incomeMessage, currentState)
                                        .ContinueWith
                                        (async (task) =>
                                        {{
                                            await Produce(task.Result, incomeMessage.HeaderInfo);
                                            {(responder.Data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, currentState, task.Result);" : "")}
                                        }},
                                        continuationOptions: TaskContinuationOptions.RunContinuationsAsynchronously
                                        )
                                        );
                                }}
                                else
                                {{
                                    package.Add(Task.FromResult(Task.CompletedTask));
                                }}

                                if (package.Count == {responder.Data.ConsumerData.CommitAfter})
                                {{
                                    await Task.WhenAll(await Task.WhenAll(package));
                                    package.Clear();
                                    consumer.Commit(consumeResult);
                                    {(responder.Data.ConsumerData.UseAfterCommit ? "await _afterCommit(partitionsInPackage);" : "")}
                                    {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Clear();" : "")}
                                }}
                            }}
                            catch (ConsumeException e)
                            {{
                                {(responder.Data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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

        private string GetIncomeMessageKey(Responder responder)
        {
            if (responder.IncomeDatas[0].KeyType.IsProtobuffType())
            {
                return $"{responder.IncomeDatas[0].KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetIncomeMessageValue(Responder responder)
        {
            if (responder.IncomeDatas[0].ValueType.IsProtobuffType())
            {
                return $"{responder.IncomeDatas[0].ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void PartitionItemStopConsume()
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

        private void PartitionItemStop()
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private void PartitionItemProduce(string assemblyName, Responder responder)
        {
            _builder.Append($@"
            private async Task Produce(
                OutcomeMessage outcomeMessage,
                {assemblyName}.RequestHeader headerInfo
                )
            {{
");

            CreateOutcomeMessage(responder);

            _builder.Append($@"
                {(responder.Data.ProducerData.CustomOutcomeHeader ? "var header = await _createOutcomeHeader(headerInfo);" : "var header = CreateOutcomeHeader(headerInfo);")}
                message.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};

                {(responder.Data.ProducerData.CustomHeaders ? "await _setHeaders(message.Headers);" : "")}
                
                try
                {{
                    if (!headerInfo.TopicsForAnswer.Any())
                    {{
                        return;
                    }}

                    foreach (var topicForAnswer in headerInfo.TopicsForAnswer.Where(wh => wh.CanAnswerFrom.Contains(_serviceName)))
                    {{
                        var topicPartition = new TopicPartition(topicForAnswer.Name, topicForAnswer.Partitions.First());
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
                }}
                catch (ProduceException<{responder.OutcomeDatas[0].TypesPair}> e)
                {{
                    {(responder.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "//ignore")}
                }}
            }}
");
        }

        private void CreateOutcomeHeader(string assemblyName, Responder responder)
        {
            if(responder.Data.ProducerData.CustomOutcomeHeader)
            {
                //nothing
            }
            else
            {
                _builder.Append($@"
            private {assemblyName}.ResponseHeader CreateOutcomeHeader({assemblyName}.RequestHeader requestHeaderInfo)
            {{
                var headerInfo = new {assemblyName}.ResponseHeader()
                {{
                    AnswerToMessageGuid = requestHeaderInfo.MessageGuid
                }};
                
                return headerInfo;
            }}
");
            }
        }

        private void CreateOutcomeMessage(Responder responder)
        {
            _builder.Append($@"
                var message = new Message<{responder.OutcomeDatas[0].TypesPair}>()
                {{
                    Key = {(responder.OutcomeDatas[0].KeyType.IsProtobuffType() ? "outcomeMessage.Key.ToByteArray()" : "outcomeMessage.Key")},
                    Value = {(responder.OutcomeDatas[0].ValueType.IsProtobuffType() ? "outcomeMessage.Value.ToByteArray()" : "outcomeMessage.Value")}
                }};
");
        }

        private string GetConsumerTType(Responder responder)
        {
            return $@"{(responder.IncomeDatas[0].KeyType.IsProtobuffType() ? "byte[]" : responder.IncomeDatas[0].KeyType.GetFullTypeName(true))}, {(responder.IncomeDatas[0].ValueType.IsProtobuffType() ? "byte[]" : responder.IncomeDatas[0].ValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}