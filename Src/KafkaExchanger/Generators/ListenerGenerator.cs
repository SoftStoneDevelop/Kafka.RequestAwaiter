using KafkaExchanger.AttributeDatas;
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
    internal class ListenerGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateListener(ListenerData ld, SourceProductionContext context)
        {
            _builder.Clear();

            Start(ld);

            Interface(ld);


            ResponderClass(ld);

            End();

            context.AddSource($"{ld.TypeSymbol.Name}Listener.g.cs", _builder.ToString());
        }

        private void ResponderClass(ListenerData data)
        {
            StartClass(data);

            StartResponderMethod(data);
            BuildPartitionItems(data);
            StopAsync(data);

            ConfigListener(data);
            ConsumerListenerConfig(data);

            IncomeMessage(data);

            PartitionItem(data);

            EndInterfaceOrClass(data);
        }

        private void PartitionItem(ListenerData data)
        {
            PartitionItemStartClass(data);
            PartitionItemStartMethod(data);

            PartitionItemStartConsume(data);
            PartitionItemStopConsume(data);

            PartitionItemStop(data);

            _builder.Append($@"
        }}
");
        }

        private void Start(ListenerData data)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
{(data.UseLogger ? "using Microsoft.Extensions.Logging;" : "")}
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {data.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(ListenerData data)
        {
            StartInterface(data);
            InterfaceMethods(data);
            EndInterfaceOrClass(data);
        }

        private void StartInterface(ListenerData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{data.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(ListenerData data)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigListener config);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(ListenerData data)
        {
            _builder.Append($@"
    }}
");
        }

        private void StartClass(ListenerData data)
        {
            _builder.Append($@"
    {data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {data.TypeSymbol.Name} : I{data.TypeSymbol.Name}Responder
    {{
        {(data.UseLogger ? "private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {data.TypeSymbol.Name}({(data.UseLogger ? "ILoggerFactory loggerFactory" : "")})
        {{
            {(data.UseLogger ? "_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartResponderMethod(ListenerData data)
        {
            _builder.Append($@"
        public void Start({data.TypeSymbol.Name}.ConfigListener config)
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

        private void BuildPartitionItems(ListenerData data)
        {
            _builder.Append($@"
        private void BuildPartitionItems({data.TypeSymbol.Name}.ConfigListener config)
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            var items = _items.AsSpan();
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                items[i] =
                    new PartitionItem(
                        config.ConsumerConfigs[i].TopicName,
                        config.ConsumerConfigs[i].ProcessDelegate,
                        config.ConsumerConfigs[i].Partitions
                        {(data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].TopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
                        );
            }}
        }}
");
        }

        private void StopAsync(ListenerData data)
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

        private void ConfigListener(ListenerData data)
        {
            _builder.Append($@"
        public class ConfigListener
        {{
            public ConfigListener(
                string groupId,
                string bootstrapServers,
                ConsumerListenerConfig[] consumerConfigs
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                ConsumerConfigs = consumerConfigs;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public ConsumerListenerConfig[] ConsumerConfigs {{ get; init; }}
        }}
");
        }

        private void ConsumerListenerConfig(ListenerData data)
        {
            _builder.Append($@"
        public class ConsumerListenerConfig : KafkaExchanger.Common.ConsumerConfig
        {{
            public ConsumerListenerConfig(
                Action<IncomeMessage> processDelegate,
                string topicName,
                params int[] partitions
                ) : base(topicName, partitions)
            {{
                {{
                    ProcessDelegate = processDelegate;
                }}
            }}

            public Action<IncomeMessage> ProcessDelegate {{ get; init; }}
        }}
");
        }

        private void IncomeMessage(ListenerData data)
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

        private void PartitionItemStartClass(ListenerData data)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                Action<IncomeMessage> processDelegate,
                int[] partitions
                {(data.UseLogger ? @",ILogger logger" : "")}
                )
            {{
                Partitions = partitions;
                {(data.UseLogger ? @"_logger = logger;" : "")}
                _incomeTopicName = incomeTopicName;
                _processDelegate = processDelegate;
            }}

            {(data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly Action<IncomeMessage> _processDelegate;

            private CancellationTokenSource _cts;
            private Task _routineConsume;

            public int[] Partitions {{ get; init; }}
");
        }

        private void PartitionItemStartMethod(ListenerData data)
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

        private void PartitionItemStartConsume(ListenerData data)
        {
            _builder.Append($@"
            private void StartConsume(
                string bootstrapServers,
                string groupId
                )
            {{
                _cts = new CancellationTokenSource();
                _routineConsume = Task.Factory.StartNew(() =>
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

                                _processDelegate(incomeMessage);
                                consumer.Commit(consumeResult);
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

        private string GetIncomeMessageKey(ListenerData data)
        {
            if (data.IncomeKeyType.IsProtobuffType())
            {
                return $"{data.IncomeKeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetIncomeMessageValue(ListenerData data)
        {
            if (data.IncomeValueType.IsProtobuffType())
            {
                return $"{data.IncomeValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
            }

            return "consumeResult.Message.Value";
        }

        private void PartitionItemStopConsume(ListenerData data)
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

        private void PartitionItemStop(ListenerData data)
        {
            _builder.Append($@"
            public async Task Stop()
            {{
                await StopConsume();
            }}
");
        }

        private string GetConsumerTType(ListenerData data)
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
