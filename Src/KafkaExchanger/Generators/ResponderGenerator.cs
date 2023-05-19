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

        public void GenerateResponder(ResponderData data, GeneratorExecutionContext context)
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
                        config.ConsumerConfigs[i].TopicName,
                        config.ConsumerConfigs[i].CreateAnswerDelegate,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        {(data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].TopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        {(data.ConsumerData.CheckDuplicate ? @",config.ConsumerConfigs[i].CheckDuplicateDelegate" : "")}
                        {(data.ConsumerData.UseAfterCommit ? @",config.ConsumerConfigs[i].AfterCommitDelegate" : "")}
                        {(data.ProducerData.BeforeSendResponse ? @",config.ConsumerConfigs[i].BeforeSendResponseDelegate" : "")}
                        {(data.ProducerData.AfterSendResponse ? @",config.ConsumerConfigs[i].AfterSendResponseDelegate" : "")}
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
        public class ConsumerResponderConfig : KafkaExchanger.Common.ConsumerConfig
        {{
            public ConsumerResponderConfig(
                Func<IncomeMessage, Task<OutcomeMessage>> createAnswerDelegate,
                {(data.ConsumerData.CheckDuplicate ? "Func<IncomeMessage, Task<bool>> checkDuplicateDelegate," : "")}
                {(data.ConsumerData.UseAfterCommit ? "Func<Task<bool>> afterCommitDelegate," : "")}
                {(data.ProducerData.BeforeSendResponse ? @"Func<IncomeMessage, OutcomeMessage, Task> beforeSendResponseDelegate," : "")}
                {(data.ProducerData.AfterSendResponse ? @"Func<IncomeMessage, OutcomeMessage, Task> afterSendResponseDelegate," : "")}
                string topicName,
                params int[] partitions
                ) : base(topicName, partitions)
            {{
                CreateAnswerDelegate = createAnswerDelegate;
                {(data.ConsumerData.CheckDuplicate ? "CheckDuplicateDelegate = checkDuplicateDelegate;" : "")}
                {(data.ConsumerData.UseAfterCommit ? "AfterCommitDelegate = afterCommitDelegate;" : "")}
                {(data.ProducerData.BeforeSendResponse ? @"BeforeSendResponseDelegate = beforeSendResponseDelegate;" : "")}
                {(data.ProducerData.AfterSendResponse ? @"AfterSendResponseDelegate = afterSendResponseDelegate;" : "")}
            }}

            public Func<IncomeMessage, Task<OutcomeMessage>> CreateAnswerDelegate {{ get; init; }}
            {(data.ConsumerData.CheckDuplicate ? "public Func<IncomeMessage, Task<bool>> CheckDuplicateDelegate { get; init; }" : "")}
            {(data.ConsumerData.UseAfterCommit ? "public Func<Task> AfterCommitDelegate { get; init; }" : "")}
            {(data.ProducerData.BeforeSendResponse ? "public Func<IncomeMessage, OutcomeMessage, Task> BeforeSendResponseDelegate { get; init; }" : "")}
            {(data.ProducerData.AfterSendResponse ? "public Func<IncomeMessage, OutcomeMessage, Task> AfterSendResponseDelegate { get; init; }" : "")}
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
                Func<IncomeMessage, Task<OutcomeMessage>> createAnswer,
                int[] partitions,
                {producerPair.FullPoolInterfaceName} producerPool
                {(data.UseLogger ? @",ILogger logger" : "")}
                {(data.ConsumerData.CheckDuplicate ? @",Func<IncomeMessage, Task<bool>> checkDuplicate" : "")}
                {(data.ConsumerData.UseAfterCommit ? @",Func<Task> afterCommit" : "")}
                {(data.ProducerData.BeforeSendResponse ? @",Func<IncomeMessage, OutcomeMessage, Task> beforeSendResponse" : "")}
                {(data.ProducerData.AfterSendResponse ? @",Func<IncomeMessage, OutcomeMessage, Task> afterSendResponse" : "")}
                )
            {{
                Partitions = partitions;
                {(data.UseLogger ? @"_logger = logger;" : "")}
                _incomeTopicName = incomeTopicName;
                _createAnswer = createAnswer;
                _producerPool = producerPool;
                {(data.ConsumerData.CheckDuplicate ? @"_checkDuplicate = checkDuplicate;" : "")}
                {(data.ConsumerData.UseAfterCommit ? @"_afterCommit = afterCommit;" : "")}
                {(data.ProducerData.BeforeSendResponse ? @"_beforeSendResponse = beforeSendResponse;" : "")}
                {(data.ProducerData.AfterSendResponse ? @"_afterSendResponse = afterSendResponse;" : "")}
            }}

            {(data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly Func<IncomeMessage, Task<OutcomeMessage>> _createAnswer;
            {(data.ConsumerData.CheckDuplicate ? @"private readonly Func<IncomeMessage, Task<bool>> _checkDuplicate;" : "")}
            {(data.ConsumerData.UseAfterCommit ? @"private readonly Func<Task> _afterCommit;" : "")}
            {(data.ProducerData.BeforeSendResponse ? @"private readonly Func<IncomeMessage, OutcomeMessage, Task> _beforeSendResponse;" : "")}
            {(data.ProducerData.AfterSendResponse ? @"private readonly Func<IncomeMessage, OutcomeMessage, Task> _afterSendResponse;" : "")}

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
            if(data.ConsumerData.CommitAfter == 1 || 
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
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);

                                var incomeMessage = new IncomeMessage();
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
");
            if(data.ConsumerData.CheckDuplicate)
            {
                _builder.Append($@"
                                if(!await _checkDuplicate(incomeMessage))
                                {{
");
            }
            _builder.Append($@"
                                var answer = await _createAnswer(incomeMessage);
                                {(data.ProducerData.BeforeSendResponse ? "await _beforeSendResponse(incomeMessage, answer);" : "")}
                                await Produce(answer, incomeMessage.HeaderInfo);
                                {(data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, answer);" : "")}
");
            if (data.ConsumerData.CheckDuplicate)
            {
                _builder.Append($@"
                                }}
");
            }
            _builder.Append($@"
                                consumer.Commit(consumeResult);
                                {(data.ConsumerData.UseAfterCommit ? "await _afterCommit();" : "")}
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
                    try
                    {{
                        while (!_cts.Token.IsCancellationRequested)
                        {{
                            try
                            {{
                                var consumeResult = consumer.Consume(_cts.Token);

                                var incomeMessage = new IncomeMessage();
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

");
            if (data.ConsumerData.CheckDuplicate)
            {
                _builder.Append($@"
                                if(!await _checkDuplicate(incomeMessage))
                                {{
");
            }
            _builder.Append($@"
                                package.Add(
                                    _createAnswer(incomeMessage)
                                    .ContinueWith
                                    (async (task) =>
                                    {{
                                        {(data.ProducerData.BeforeSendResponse ? "await _beforeSendResponse(incomeMessage, task.Result);" : "")}
                                        await Produce(task.Result, incomeMessage.HeaderInfo);
                                        {(data.ProducerData.AfterSendResponse ? "await _afterSendResponse(incomeMessage, task.Result);" : "")}
                                    }},
                                    continuationOptions: TaskContinuationOptions.RunContinuationsAsynchronously
                                    )
                                    );
");
            if (data.ConsumerData.CheckDuplicate)
            {
                _builder.Append($@"
                                }}
                                else
                                {{
                                    package.Add(Task.FromResult(Task.CompletedTask));
                                }}
");
            }
            _builder.Append($@"
                                if (package.Count == {data.ConsumerData.CommitAfter})
                                {{
                                    await Task.WhenAll(await Task.WhenAll(package));
                                    package.Clear();
                                    consumer.Commit(consumeResult);
                                    {(data.ConsumerData.UseAfterCommit ? "await _afterCommit();" : "")}
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
                var header = CreateOutcomeHeader(headerInfo);
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
