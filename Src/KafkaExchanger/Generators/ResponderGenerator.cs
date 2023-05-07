using KafkaExchanger.AttributeDatas;
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

        public void GenerateResponder(ResponderData ra, GeneratorExecutionContext context)
        {
            _builder.Clear();

            Start(ra);

            Interface(ra);


            ResponderClass(ra);

            End();

            context.AddSource($"{ra.TypeSymbol.Name}Responder.g.cs", _builder.ToString());
        }

        private void ResponderClass(ResponderData ra)
        {
            StartClass(ra);

            StartResponderMethod(ra);
            BuildPartitionItems(ra);
            StopAsync(ra);

            ConfigResponder(ra);
            ConsumerResponderConfig(ra);

            IncomeMessage(ra);
            OutcomeMessage(ra);

            PartitionItem(ra);

            EndInterfaceOrClass(ra);
        }

        private void PartitionItem(ResponderData ra)
        {
            PartitionItemStartClass(ra);
            PartitionItemStartMethod(ra);

            PartitionItemStartConsume(ra);
            PartitionItemStopConsume(ra);

            PartitionItemStartProduce(ra);
            PartitionItemStopProduce(ra);
            PartitionItemStop(ra);
            PartitionItemProduce(ra);
            CreateOutcomeHeader(ra);

            _builder.Append($@"
        }}
");
        }

        private void Start(ResponderData ra)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {ra.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(ResponderData ra)
        {
            StartInterface(ra);
            InterfaceMethods(ra);
            EndInterfaceOrClass(ra);
        }

        private void StartInterface(ResponderData ra)
        {
            _builder.Append($@"
    public interface I{ra.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(ResponderData ra)
        {
            _builder.Append($@"
        public void Start({ra.TypeSymbol.Name}.ConfigResponder config);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(ResponderData ra)
        {
            _builder.Append($@"
    }}
");
        }

        private void StartClass(ResponderData ra)
        {
            _builder.Append($@"
    public partial class {ra.TypeSymbol.Name} : I{ra.TypeSymbol.Name}Responder
    {{
        private readonly ILoggerFactory _loggerFactory;
        private PartitionItem[] _items;
        
        public {ra.TypeSymbol.Name}(ILoggerFactory loggerFactory)
        {{
            _loggerFactory = loggerFactory;
        }}
");
        }

        private void StartResponderMethod(ResponderData ra)
        {
            _builder.Append($@"
        public void Start({ra.TypeSymbol.Name}.ConfigResponder config)
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

        private void BuildPartitionItems(ResponderData ra)
        {
            _builder.Append($@"
        private void BuildPartitionItems({ra.TypeSymbol.Name}.ConfigResponder config)
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
                        _loggerFactory.CreateLogger($""{{config.ConsumerConfigs[i].TopicName}}:Partitions:{{string.Join(',',config.ConsumerConfigs[i].Partitions)}}"")
                        );
            }}
        }}
");
        }

        private void StopAsync(ResponderData ra)
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

        private void ConfigResponder(ResponderData ra)
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

        private void ConsumerResponderConfig(ResponderData ra)
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

        private void IncomeMessage(ResponderData ra)
        {
            _builder.Append($@"
        public class IncomeMessage
        {{
            public Message<{GetConsumerTType(ra)}> OriginalMessage {{ get; set; }}
            public {ra.IncomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {ra.IncomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public kafka.RequestHeader HeaderInfo {{ get; set; }}
        }}
");
        }

        private void OutcomeMessage(ResponderData ra)
        {
            _builder.Append($@"
        public class OutcomeMessage
        {{
            public {ra.OutcomeKeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {ra.OutcomeValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
        }

        private void PartitionItemStartClass(ResponderData ra)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                Func<IncomeMessage, OutcomeMessage> createAnswer,
                int[] partitions,
                ILogger logger
                )
            {{
                Partitions = partitions;
                _logger = logger;
                _incomeTopicName = incomeTopicName;
                _createAnswer = createAnswer;
            }}

            private readonly ILogger _logger;
            private readonly string _incomeTopicName;
            private readonly Func<IncomeMessage, OutcomeMessage> _createAnswer;

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            private IProducer<{GetProducerTType(ra)}> _producer;

            public int[] Partitions {{ get; init; }}
");
        }

        private void PartitionItemStartMethod(ResponderData ra)
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

        private void PartitionItemStartConsume(ResponderData ra)
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
                        new ConsumerBuilder<{GetConsumerTType(ra)}>(conf)
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
                                incomeMessage.Key = {GetIncomeMessageKey(ra)};
                                incomeMessage.Value = {GetIncomeMessageValue(ra)};

                                _logger.LogInformation($""Consumed incomeMessage 'Key: {{consumeResult.Message.Key}}, Value: {{consumeResult.Message.Value}}'."");
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    _logger.LogError($""Consumed incomeMessage 'Key: {{consumeResult.Message.Key}}, Value: {{consumeResult.Message.Value}}' not contain Info header"");
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
            _cts.Token,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
            );
            }}
");
        }

        private string GetIncomeMessageKey(ResponderData ra)
        {
            if (ra.IncomeKeyType.IsProtobuffType())
            {
                return $"{ra.IncomeKeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetIncomeMessageValue(ResponderData ra)
        {
            if (ra.IncomeValueType.IsProtobuffType())
            {
                return $"{ra.IncomeValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void PartitionItemStopConsume(ResponderData ra)
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

        private void PartitionItemStartProduce(ResponderData ra)
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

        private void PartitionItemStopProduce(ResponderData ra)
        {
            _builder.Append($@"
            private void StopProduce()
            {{
                _producer?.Flush();
                _producer?.Dispose();
            }}
");
        }

        private void PartitionItemStop(ResponderData ra)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                StopProduce();
                await StopConsume();
            }}
");
        }

        private void PartitionItemProduce(ResponderData ra)
        {
            _builder.Append($@"
            private async Task Produce(
                OutcomeMessage outcomeMessage,
                kafka.RequestHeader headerInfo
                )
            {{
");

            CreateOutcomeMessage(ra);

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
                catch (ProduceException<{GetProducerTType(ra)}> e)
                {{
                    _logger.LogError($""Delivery failed: {{e.Error.Reason}}"");
                }}
            }}
");
        }

        private void CreateOutcomeHeader(ResponderData ra)
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

        private void CreateOutcomeMessage(ResponderData ra)
        {
            _builder.Append($@"
                var message = new Message<{GetProducerTType(ra)}>()
                {{
                    Key = {(ra.OutcomeKeyType.IsProtobuffType() ? "outcomeMessage.Key.ToByteArray()" : "outcomeMessage.Key")},
                    Value = {(ra.OutcomeValueType.IsProtobuffType() ? "outcomeMessage.Value.ToByteArray()" : "outcomeMessage.Value")}
                }};
");
        }

        private string GetProducerTType(ResponderData ra)
        {
            return $@"{(ra.OutcomeKeyType.IsProtobuffType() ? "byte[]" : ra.OutcomeKeyType.GetFullTypeName(true))}, {(ra.OutcomeValueType.IsProtobuffType() ? "byte[]" : ra.OutcomeValueType.GetFullTypeName(true))}";
        }

        private string GetConsumerTType(ResponderData ra)
        {
            return $@"{(ra.IncomeKeyType.IsProtobuffType() ? "byte[]" : ra.IncomeKeyType.GetFullTypeName(true))}, {(ra.IncomeValueType.IsProtobuffType() ? "byte[]" : ra.IncomeValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}
