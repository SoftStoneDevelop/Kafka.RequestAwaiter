using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System.IO;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal class Generator
    {
        StringBuilder _builder = new StringBuilder();

        public void GenerateRequestAwaiter(
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter,
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

            Config();
            Consumers(assemblyName, requestAwaiter);
            ConsumerInfo(assemblyName, requestAwaiter);

            Produce(assemblyName, requestAwaiter);
            BaseResponseMessage();
            ResponseMessages(assemblyName, requestAwaiter);

            TopicResponse.Append(_builder, assemblyName, requestAwaiter);
            PartitionItem.Append(_builder, assemblyName, requestAwaiter);

            EndInterfaceOrClass(requestAwaiter);

            End();

            context.AddSource($"{requestAwaiter.Data.TypeSymbol.Name}RequesterAwaiter.g.cs", _builder.ToString());
        }

        private void Start(AttributeDatas.RequestAwaiter requestAwaiter)
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
using System.Collections.Generic;

namespace {requestAwaiter.Data.TypeSymbol.ContainingNamespace}
{{
");
        }

        #region Interface

        private void Interface(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            StartInterface(requestAwaiter);

            InterfaceProduceMethod(assemblyName, requestAwaiter);
            InterfaceStartMethod(assemblyName, requestAwaiter);
            InterfaceStopMethod(assemblyName, requestAwaiter);

            EndInterfaceOrClass(requestAwaiter);
        }

        private void StartInterface(AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} interface I{requestAwaiter.Data.TypeSymbol.Name}RequestAwaiter
    {{
");
        }

        private void InterfaceProduceMethod(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeData = requestAwaiter.OutcomeDatas[i];
                if(!outcomeData.KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
            {outcomeData.KeyType.GetFullTypeName(true, true)} key{i},
");
                }

                _builder.Append($@"
            {outcomeData.ValueType.GetFullTypeName(true, true)} value{i},
");
            }

            _builder.Append($@"
            int waitResponseTimeout = 0
            );
");
        }

        private void InterfaceStartMethod(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public void Start(
            {requestAwaiter.Data.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
            {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            _builder.Append($@"
            )
            ;
");
        }

        private void InterfaceStopMethod(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public Task StopAsync();
");
        }

        private void EndInterfaceOrClass(AttributeDatas.RequestAwaiter data)
        {
            _builder.Append($@"
    }}
");
        }

        #endregion

        private void StartClass(AttributeDatas.RequestAwaiter requestAwaiter)
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

        private void StartMethod(AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public void Start(
            {requestAwaiter.Data.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
            {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            _builder.Append($@"
            )
        {{
            BuildPartitionItems(
                config
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
                producerPool{i}
");
                _builder.Append($@"
                );

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
        }

        private void BuildPartitionItems(AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        private void BuildPartitionItems(
            {requestAwaiter.Data.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
            {requestAwaiter.OutcomeDatas[0].FullPoolInterfaceName} producerPool{i}
");
            }
            _builder.Append($@"
            )
        {{
            _items = new PartitionItem[config.Consumers.Length];
            for (int i = 0; i < config.Consumers.Length; i++)
            {{
                _items[i] =
                    new PartitionItem(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if (i != 0)
                {
                    _builder.Append(',');
                }

                _builder.Append($@"
                        config.Consumers[i].Income{i}.TopicName,
                        config.Consumers[i].Income{i}.Partitions,
                        config.Consumers[i].Income{i}.CanAnswerService
");
            }
            _builder.Append($@",
                        config.OutcomeTopicName
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
                        producerPool{i}
");
            }
            _builder.Append($@"
                        {(requestAwaiter.Data.UseLogger ? @",_loggerFactory.CreateLogger($""{config.Consumers[i].GroupName}"")" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? @",config.Consumers[i].CreateOutcomeHeader" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",config.Consumers[i].SetHeaders" : "")}
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

        private void Config()
        {
            _builder.Append($@"
        public class Config
        {{
            public Config(
                string groupId,
                string bootstrapServers,
                string outcomeTopicName,
                Consumers[] consumers
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                OutcomeTopicName = outcomeTopicName;
                Consumers = consumers;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public string OutcomeTopicName {{ get; init; }}

            public Consumers[] Consumers {{ get; init; }}
        }}
");
        }

        private void Consumers(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public class Consumers
        {{
            private Consumers() {{ }}

            public Consumers(
                string groupName
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                _builder.Append($@",
                ConsumerInfo income{i}
");
            }
            _builder.Append($@"
                )
            {{
                GroupName = groupName;
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                _builder.Append($@"
                Income{i} = income{i};
");
            }
            _builder.Append($@"
            }}
            
            /// <summary>
            /// To identify the logger
            /// </summary>
            public string GroupName {{ get; init; }}
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                _builder.Append($@"
            public ConsumerInfo Income{i} {{ get; init; }}
");
            }
            _builder.Append($@"
        }}
");
        }

        private void ConsumerInfo(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public class ConsumerInfo
        {{
            private ConsumerInfo() {{ }}

            public ConsumerInfo(
                string topicName,
                string[] canAnswerService,
                int[] partitions
                )
            {{
                CanAnswerService = canAnswerService;
                TopicName = topicName;
                Partitions = partitions;
            }}

            public string TopicName {{ get; init; }}

            public string[] CanAnswerService {{ get; init; }}

            public int[] Partitions {{ get; init; }}
        }}
");
        }

        private void Produce(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                if (!requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
            {requestAwaiter.OutcomeDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                _builder.Append($@"
            {requestAwaiter.OutcomeDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            _builder.Append($@"
            int waitResponseTimeout = 0
            )
        {{
            var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
            var item = _items[index];
            return 
                item.Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                if (!requestAwaiter.OutcomeDatas[0].KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
                    key{i},
");
                }

                _builder.Append($@"
                    value{i},
");
            }
            _builder.Append($@"
                    waitResponseTimeout
                );
        }}
        private uint _currentItemIndex = 0;
");
        }

        private void BaseResponseMessage()
        {
            _builder.Append($@"
        public abstract class BaseResponseMessage
        {{
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private void ResponseMessages(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                var incomeData = requestAwaiter.IncomeDatas[i];
                _builder.Append($@"
        public class ResponseTopic{i}Message : BaseResponseMessage
        {{
            public {assemblyName}.ResponseHeader HeaderInfo {{ get; set; }}

            public Message<{GetConsumerTType(incomeData)}> OriginalMessage {{ get; set; }}
");
                if (!incomeData.KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
            public {incomeData.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                _builder.Append($@"
            public {incomeData.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }

        private string GetConsumerTType(IncomeData incomeData)
        {
            return $@"{(incomeData.KeyType.IsProtobuffType() ? "byte[]" : incomeData.KeyType.GetFullTypeName(true))}, {(incomeData.ValueType.IsProtobuffType() ? "byte[]" : incomeData.ValueType.GetFullTypeName(true))}";
        }

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}