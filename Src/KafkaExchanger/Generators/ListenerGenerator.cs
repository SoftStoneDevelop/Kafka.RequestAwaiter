using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System.Text;

namespace KafkaExchanger.Generators
{
    internal class ListenerGenerator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateListener(string assemblyName, Listener listener, SourceProductionContext context)
        {
            _builder.Clear();

            Start(listener);

            Interface(listener);


            ResponderClass(assemblyName, listener);

            End();

            context.AddSource($"{listener.Data.TypeSymbol.Name}Listener.g.cs", _builder.ToString());
        }

        private void ResponderClass(string assemblyName, Listener listener)
        {
            StartClass(listener);

            StartResponderMethod(listener);
            BuildPartitionItems(listener);
            StopAsync();

            ConfigListener();
            ConsumerListenerConfig();

            IncomeMessage(assemblyName, listener);

            PartitionItem(assemblyName, listener);

            EndInterfaceOrClass(listener);
        }

        private void PartitionItem(string assemblyName, Listener listener)
        {
            PartitionItemStartClass(listener);
            PartitionItemStartMethod();

            PartitionItemStartConsume(assemblyName, listener);
            PartitionItemStopConsume();

            PartitionItemStop();

            _builder.Append($@"
        }}
");
        }

        private void Start(Listener listener)
        {
            _builder.Append($@"
using Confluent.Kafka;
using Google.Protobuf;
{(listener.Data.UseLogger ? "using Microsoft.Extensions.Logging;" : "")}
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace {listener.Data.TypeSymbol.ContainingNamespace}
{{
");
        }

        private void Interface(Listener listener)
        {
            StartInterface(listener);
            InterfaceMethods(listener);
            EndInterfaceOrClass(listener);
        }

        private void StartInterface(Listener listener)
        {
            _builder.Append($@"
    {listener.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{listener.Data.TypeSymbol.Name}Responder
    {{
");
        }

        private void InterfaceMethods(Listener listener)
        {
            _builder.Append($@"
        public void Start({listener.Data.TypeSymbol.Name}.ConfigListener config);

        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(Listener listener)
        {
            _builder.Append($@"
    }}
");
        }

        private void StartClass(Listener listener)
        {
            _builder.Append($@"
    {listener.Data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {listener.Data.TypeSymbol.Name} : I{listener.Data.TypeSymbol.Name}Responder
    {{
        {(listener.Data.UseLogger ? "private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {listener.Data.TypeSymbol.Name}({(listener.Data.UseLogger ? "ILoggerFactory loggerFactory" : "")})
        {{
            {(listener.Data.UseLogger ? "_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private void StartResponderMethod(Listener listener)
        {
            _builder.Append($@"
        public void Start({listener.Data.TypeSymbol.Name}.ConfigListener config)
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

        private void BuildPartitionItems(Listener listener)
        {
            _builder.Append($@"
        private void BuildPartitionItems({listener.Data.TypeSymbol.Name}.ConfigListener config)
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
                        {(listener.Data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.ConsumerConfigs[i].TopicName}:Partitions:{string.Join(',',config.ConsumerConfigs[i].Partitions)}"")" : "")}
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

        private void ConfigListener()
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

        private void ConsumerListenerConfig()
        {
            _builder.Append($@"
        public class ConsumerListenerConfig
        {{
            public ConsumerListenerConfig(
                Action<IncomeMessage> processDelegate,
                string topicName,
                params int[] partitions
                )
            {{
                {{
                    ProcessDelegate = processDelegate;
                }}
            }}

            public string TopicName {{ get; init; }}

            public int[] Partitions {{ get; init; }}

            public Action<IncomeMessage> ProcessDelegate {{ get; init; }}
        }}
");
        }

        private void IncomeMessage(string assemblyName, Listener listener)
        {
            _builder.Append($@"
        public class IncomeMessage
        {{
            public Message<{GetConsumerTType(listener)}> OriginalMessage {{ get; set; }}
            public {listener.IncomeDatas[0].KeyType.GetFullTypeName(true)} Key {{ get; set; }}
            public {listener.IncomeDatas[0].ValueType.GetFullTypeName(true)} Value {{ get; set; }}
            public {assemblyName}.RequestHeader HeaderInfo {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private void PartitionItemStartClass(Listener listener)
        {
            _builder.Append($@"
        private class PartitionItem
        {{
            public PartitionItem(
                string incomeTopicName,
                Action<IncomeMessage> processDelegate,
                int[] partitions
                {(listener.Data.UseLogger ? @",ILogger logger" : "")}
                )
            {{
                Partitions = partitions;
                {(listener.Data.UseLogger ? @"_logger = logger;" : "")}
                _incomeTopicName = incomeTopicName;
                _processDelegate = processDelegate;
            }}

            {(listener.Data.UseLogger ? @"private readonly ILogger _logger;" : "")}
            private readonly string _incomeTopicName;
            private readonly Action<IncomeMessage> _processDelegate;

            private CancellationTokenSource _cts;
            private Task _routineConsume;

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

        private void PartitionItemStartConsume(string assemblyName, Listener listener)
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
                        new ConsumerBuilder<{GetConsumerTType(listener)}>(conf)
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
                                incomeMessage.Key = {GetIncomeMessageKey(listener)};
                                incomeMessage.Value = {GetIncomeMessageValue(listener)};

                                {(listener.Data.UseLogger ? @"_logger.LogInformation($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}'."");" : "")}
                                if (!consumeResult.Message.Headers.TryGetLastBytes(""Info"", out var infoBytes))
                                {{
                                    {(listener.Data.UseLogger ? @"_logger.LogError($""Consumed incomeMessage 'Key: {consumeResult.Message.Key}, Value: {consumeResult.Message.Value}' not contain Info header"");" : "")}
                                    consumer.Commit(consumeResult);
                                    continue;
                                }}

                                incomeMessage.HeaderInfo = {assemblyName}.RequestHeader.Parser.ParseFrom(infoBytes);

                                _processDelegate(incomeMessage);
                                consumer.Commit(consumeResult);
                            }}
                            catch (ConsumeException e)
                            {{
                                {(listener.Data.UseLogger ? @"_logger.LogError($""Error occured: {e.Error.Reason}"");" : "//ignore")}
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

        private string GetIncomeMessageKey(Listener listener)
        {
            if (listener.IncomeDatas[0].KeyType.IsProtobuffType())
            {
                return $"{listener.IncomeDatas[0].KeyType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Key.AsSpan())";
            }

            return "consumeResult.Message.Key";
        }

        private string GetIncomeMessageValue(Listener listener)
        {
            if (listener.IncomeDatas[0].ValueType.IsProtobuffType())
            {
                return $"{listener.IncomeDatas[0].ValueType.GetFullTypeName(true)}.Parser.ParseFrom(consumeResult.Message.Value.AsSpan())";
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

        private string GetConsumerTType(Listener listener)
        {
            return $@"{(listener.IncomeDatas[0].KeyType.IsProtobuffType() ? "byte[]" : listener.IncomeDatas[0].KeyType.GetFullTypeName(true))}, {(listener.IncomeDatas[0].ValueType.IsProtobuffType() ? "byte[]" : listener.IncomeDatas[0].ValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}