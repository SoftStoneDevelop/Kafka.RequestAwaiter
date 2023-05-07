﻿using KafkaExchanger.AttributeDatas;
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

            Start(data);

            Interface(data);


            ResponderClass(data);

            End();

            context.AddSource($"{data.TypeSymbol.Name}Responder.g.cs", _builder.ToString());
        }

        private void ResponderClass(ResponderData data)
        {
            StartClass(data);

            StartResponderMethod(data);
            BuildPartitionItems(data);
            StopAsync(data);

            ConfigResponder(data);
            ConsumerResponderConfig(data);

            IncomeMessage(data);
            OutcomeMessage(data);

            PartitionItem(data);

            EndInterfaceOrClass(data);
        }

        private void PartitionItem(ResponderData data)
        {
            PartitionItemStartClass(data);
            PartitionItemStartMethod(data);

            PartitionItemStartConsume(data);
            PartitionItemStopConsume(data);

            PartitionItemStartProduce(data);
            PartitionItemStopProduce(data);
            PartitionItemStop(data);
            PartitionItemProduce(data);
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
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {data.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(ResponderData data)
        {
            StartInterface(data);
            InterfaceMethods(data);
            EndInterfaceOrClass(data);
        }

        private void StartInterface(ResponderData data)
        {
            _builder.Append($@"
    public interface I{data.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(ResponderData data)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigResponder config);

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
    public partial class {data.TypeSymbol.Name} : I{data.TypeSymbol.Name}Responder
    {{
        {(data.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {data.TypeSymbol.Name}({(data.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(data.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartResponderMethod(ResponderData data)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigResponder config)
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

        private void BuildPartitionItems(ResponderData data)
        {
            _builder.Append($@"
        private void BuildPartitionItems({data.TypeSymbol.Name}.ConfigResponder config)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ConsumerConfigs[i].TopicName,
                        config.ConsumerConfigs[i].CreateAnswerDelegate,
                        config.ConsumerConfigs[i].Partitions
                        {(data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].TopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
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
                Func<IncomeMessage, OutcomeMessage> createAnswerDelegate,
                string topicName,
                params int[] partitions
                ) : base(topicName, partitions)
            {{
                {{
                    CreateAnswerDelegate = createAnswerDelegate;
                }}
            }}

            public Func<IncomeMessage, OutcomeMessage> CreateAnswerDelegate {{ get; init; }}
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

        private void PartitionItemStartClass(ResponderData data)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                Func<IncomeMessage, OutcomeMessage> createAnswer,
                int[] partitions
                {(data.UseLogger ? @",ILogger logger" : "")}
                )
            {{
                Partitions = partitions;
                {(data.UseLogger ? @"_logger = logger;" : "")}
                _incomeTopicName = incomeTopicName;
                _createAnswer = createAnswer;
            }}

            {(data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly Func<IncomeMessage, OutcomeMessage> _createAnswer;

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            private IProducer<{GetProducerTType(data)}> _producer;

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
                StartProduce(bootstrapServers);
            }}
");
        }

        private void PartitionItemStartConsume(ResponderData data)
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

                                var asnwer = _createAnswer(incomeMessage);
                                consumer.Commit(consumeResult);

                                await Produce(asnwer, incomeMessage.HeaderInfo);
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
                }},
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
            );
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

        private void PartitionItemStartProduce(ResponderData data)
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
                    new ProducerBuilder<{GetProducerTType(data)}>(config)
                    .Build()
                    ;
            }}
");
        }

        private void PartitionItemStopProduce(ResponderData data)
        {
            _builder.Append($@"
            private void StopProduce()
            {{
                _producer?.Flush();
                _producer?.Dispose();
            }}
");
        }

        private void PartitionItemStop(ResponderData data)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                StopProduce();
                await StopConsume();
            }}
");
        }

        private void PartitionItemProduce(ResponderData data)
        {
            _builder.Append($@"
            private async Task Produce(
                OutcomeMessage outcomeMessage,
                kafka.RequestHeader headerInfo
                )
            {{
");

            CreateOutcomeMessage(data);

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
                    var deliveryResult = await _producer.ProduceAsync(topicPartition, message);
                }}
                catch (ProduceException<{GetProducerTType(data)}> e)
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

        private void CreateOutcomeMessage(ResponderData data)
        {
            _builder.Append($@"
                var message = new Message<{GetProducerTType(data)}>()
                {{
                    Key = {(data.OutcomeKeyType.IsProtobuffType() ? "outcomeMessage.Key.ToByteArray()" : "outcomeMessage.Key")},
                    Value = {(data.OutcomeValueType.IsProtobuffType() ? "outcomeMessage.Value.ToByteArray()" : "outcomeMessage.Value")}
                }};
");
        }

        private string GetProducerTType(ResponderData data)
        {
            return $@"{(data.OutcomeKeyType.IsProtobuffType() ? "byte[]" : data.OutcomeKeyType.GetFullTypeName(true))}, {(data.OutcomeValueType.IsProtobuffType() ? "byte[]" : data.OutcomeValueType.GetFullTypeName(true))}";
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
