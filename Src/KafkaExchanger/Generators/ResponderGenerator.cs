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

            InputMessage(assemblyName, responder);
            OutputMessage(responder);

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
            CreateOutputHeader(assemblyName, responder);

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
        public void Start({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutputDatas[0].FullPoolInterfaceName} producerPool);

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
        public void Start({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutputDatas[0].FullPoolInterfaceName} producerPool)
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
        private void BuildPartitionItems({responder.Data.TypeSymbol.Name}.ConfigResponder config, {responder.OutputDatas[0].FullPoolInterfaceName} producerPool)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ServiceName,
                        config.ConsumerConfigs[i].InputTopicName,
                        config.ConsumerConfigs[i].CreateAnswer,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        {(responder.Data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].InputTopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        {(responder.Data.ConsumerData.CheckCurrentState ? @",config.ConsumerConfigs[i].GetCurrentState" : "")}
                        {(responder.Data.ConsumerData.UseAfterCommit ? @",config.ConsumerConfigs[i].AfterCommit" : "")}
                        {(responder.Data.AfterSend ? @",config.ConsumerConfigs[i].AfterSendResponse" : "")}
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
                Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> createAnswer,
                {(responder.Data.ConsumerData.CheckCurrentState ? "Func<InputMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState," : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? "Func<HashSet<int>,Task> afterCommit," : "")}
                {(responder.Data.AfterSend ? @"Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutputMessage, Task> afterSendResponse," : "")}
                string inputTopicName,
                params int[] partitions
                )
            {{
                CreateAnswer = createAnswer;
                InputTopicName = inputTopicName;
                Partitions = partitions;

                {(responder.Data.ConsumerData.CheckCurrentState ? "GetCurrentState = getCurrentState;" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? "AfterCommit = afterCommit;" : "")}
                {(responder.Data.AfterSend ? @"AfterSendResponse = afterSendResponse;" : "")}
            }}

            public string InputTopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}

            public Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> CreateAnswer {{ get; init; }}
            {(responder.Data.ConsumerData.CheckCurrentState ? "public Func<InputMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> GetCurrentState { get; init; }" : "")}
            {(responder.Data.ConsumerData.UseAfterCommit ? "public Func<HashSet<int>,Task> AfterCommit { get; init; }" : "")}
            {(responder.Data.AfterSend ? "public Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutputMessage, Task> AfterSendResponse { get; init; }" : "")}
        }}
");
        }

        private void InputMessage(string assemblyName, Responder responder)
        {
            _builder.Append($@"
        public class InputMessage
        {{
            public Message<{GetConsumerTType(responder)}> OriginalMessage {{ get; set; }}
            public {responder.InputDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {responder.InputDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public {assemblyName}.RequestHeader HeaderInfo {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private void OutputMessage(Responder responder)
        {
            _builder.Append($@"
        public class OutputMessage
        {{
            public {responder.OutputDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {responder.OutputDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
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
                string inputTopicName,
                Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> createAnswer,
                int[] partitions,
                {responder.OutputDatas[0].FullPoolInterfaceName} producerPool
                {(responder.Data.UseLogger ? @",ILogger logger" : "")}
                {(responder.Data.ConsumerData.CheckCurrentState ? @",Func<InputMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> getCurrentState" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? @",Func<HashSet<int>, Task> afterCommit" : "")}
                {(responder.Data.AfterSend ? @",Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutputMessage, Task> afterSendResponse" : "")}
                )
            {{
                Partitions = partitions;
                {(responder.Data.UseLogger ? @"_logger = logger;" : "")}
                _serviceName = serviceName;
                _inputTopicName = inputTopicName;
                _createAnswer = createAnswer;
                _producerPool = producerPool;
                {(responder.Data.ConsumerData.CheckCurrentState ? @"_getCurrentState = getCurrentState;" : "")}
                {(responder.Data.ConsumerData.UseAfterCommit ? @"_afterCommit = afterCommit;" : "")}
                {(responder.Data.AfterSend ? @"_afterSendResponse = afterSendResponse;" : "")}
            }}

            {(responder.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _inputTopicName;
            private readonly string _serviceName;
            private readonly Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, Task<OutputMessage>> _createAnswer;
            {(responder.Data.ConsumerData.CheckCurrentState ? @"private readonly Func<InputMessage, Task<KafkaExchanger.Attributes.Enums.CurrentState>> _getCurrentState;" : "")}
            {(responder.Data.ConsumerData.UseAfterCommit ? @"private readonly Func<HashSet<int>, Task> _afterCommit;" : "")}
            {(responder.Data.AfterSend ? @"private readonly Func<InputMessage, KafkaExchanger.Attributes.Enums.CurrentState, OutputMessage, Task> _afterSendResponse;" : "")}

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            private {responder.OutputDatas[0].FullPoolInterfaceName} _producerPool;

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

                    consumer.Assign(Partitions.Select(sel => new TopicPartition(_inputTopicName, sel)));
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
                                var inputMessage = new InputMessage();
                                inputMessage.Partition = consumeResult.Partition;
                                inputMessage.OriginalMessage = consumeResult.Message;
                                inputMessage.Key = {GetInputMessageKey(responder)};
                                inputMessage.Value = {GetInputMessageValue(responder)};

                                {(responder.Data.UseLogger ? @"_logger.LogInformation($""Consumed inputMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(responder.Data.UseLogger ? @"_logger.LogError($""Consumed inputMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    continue;
                                }}
                                
                                inputMessage.HeaderInfo = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);
                                if(!inputMessage.HeaderInfo.TopicsForAnswer.Any(wh => !wh.CanAnswerFrom.Any() || wh.CanAnswerFrom.Contains(_serviceName)))
                                {{
                                    continue;
                                }}

                                var currentState = {(responder.Data.ConsumerData.CheckCurrentState ? "await _getCurrentState(inputMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    var answer = await _createAnswer(inputMessage, currentState);
                                    await Produce(answer, inputMessage.HeaderInfo);
                                    {(responder.Data.AfterSend ? "await _afterSendResponse(inputMessage, currentState, answer);" : "")}
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

                                var inputMessage = new InputMessage();
                                {(responder.Data.ConsumerData.UseAfterCommit ? "partitionsInPackage.Add(consumeResult.Partition.Value);" : "")}
                                inputMessage.Partition = consumeResult.Partition;
                                inputMessage.OriginalMessage = consumeResult.Message;
                                inputMessage.Key = {GetInputMessageKey(responder)};
                                inputMessage.Value = {GetInputMessageValue(responder)};

                                {(responder.Data.UseLogger ? @"_logger.LogInformation($""Consumed inputMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(responder.Data.UseLogger ? @"_logger.LogError($""Consumed inputMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    continue;
                                }}

                                inputMessage.HeaderInfo = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);
                                var currentState = {(responder.Data.ConsumerData.CheckCurrentState ? "await _getCurrentState(inputMessage)" : "KafkaExchanger.Attributes.Enums.CurrentState.NewMessage")};
                                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                                {{
                                    package.Add(
                                            _createAnswer(inputMessage, currentState)
                                        .ContinueWith
                                        (async (task) =>
                                        {{
                                            await Produce(task.Result, inputMessage.HeaderInfo);
                                            {(responder.Data.AfterSend ? "await _afterSendResponse(inputMessage, currentState, task.Result);" : "")}
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

        private string GetInputMessageKey(Responder responder)
        {
            if (responder.InputDatas[0].KeyType.IsProtobuffType())
            {
                return $"{responder.InputDatas[0].KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetInputMessageValue(Responder responder)
        {
            if (responder.InputDatas[0].ValueType.IsProtobuffType())
            {
                return $"{responder.InputDatas[0].ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
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
                OutputMessage outputMessage,
                {assemblyName}.RequestHeader headerInfo
                )
            {{
");

            CreateOutputMessage(responder);

            _builder.Append($@"
                var header = CreateOutputHeader(headerInfo);
                message.Headers = new Headers
                {{
                    {{ ""Info"", header.ToByteArray() }}
                }};
                
                try
                {{
                    if (!headerInfo.TopicsForAnswer.Any())
                    {{
                        return;
                    }}

                    foreach (var topicForAnswer in headerInfo.TopicsForAnswer.Where(wh => !wh.CanAnswerFrom.Any() || wh.CanAnswerFrom.Contains(_serviceName)))
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
                catch (ProduceException<{responder.OutputDatas[0].TypesPair}> e)
                {{
                    {(responder.Data.UseLogger ? @"_logger.LogError($""Delivery failed: {e.Error.Reason}"");" : "//ignore")}
                }}
            }}
");
        }

        private void CreateOutputHeader(string assemblyName, Responder responder)
        {
            _builder.Append($@"
            private {assemblyName}.ResponseHeader CreateOutputHeader({assemblyName}.RequestHeader requestHeaderInfo)
            {{
                var headerInfo = new {assemblyName}.ResponseHeader()
                {{
                    AnswerToMessageGuid = requestHeaderInfo.MessageGuid,
                    AnswerFrom = _serviceName,
                    Bucket = requestHeaderInfo.Bucket
                }};
                
                return headerInfo;
            }}
");
        }

        private void CreateOutputMessage(Responder responder)
        {
            _builder.Append($@"
                var message = new Message<{responder.OutputDatas[0].TypesPair}>()
                {{
                    Key = {(responder.OutputDatas[0].KeyType.IsProtobuffType() ? "outputMessage.Key.ToByteArray()" : "outputMessage.Key")},
                    Value = {(responder.OutputDatas[0].ValueType.IsProtobuffType() ? "outputMessage.Value.ToByteArray()" : "outputMessage.Value")}
                }};
");
        }

        private string GetConsumerTType(Responder responder)
        {
            return $@"{(responder.InputDatas[0].KeyType.IsProtobuffType() ? "byte[]" : responder.InputDatas[0].KeyType.GetFullTypeName(true))}, {(responder.InputDatas[0].ValueType.IsProtobuffType() ? "byte[]" : responder.InputDatas[0].ValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}