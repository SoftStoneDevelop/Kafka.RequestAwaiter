using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Enums;
using KafkaExchanger.Extensions;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System.IO;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal class Generator
    {
        StringBuilder _builder = new StringBuilder(1200);

        public void GenerateRequestAwaiter(
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter,
            SourceProductionContext context
            )
        {
            _builder.Clear();

            Start(requestAwaiter);//start file

            Interface.Append(_builder, assemblyName, requestAwaiter);

            StartClass(requestAwaiter);

            //inner classes
            Config.Append(_builder);
            ProcessorConfig.Append(_builder, assemblyName, requestAwaiter);
            ConsumerInfo.Append(_builder);
            ProducerInfo.Append(_builder, assemblyName, requestAwaiter);

            IncomeMessages.Append(_builder, assemblyName, requestAwaiter);
            if (requestAwaiter.Data is ResponderData)
            { 
                OutcomeMessages.Append(_builder, assemblyName, requestAwaiter);
                ResponseResult.Append(_builder, assemblyName, requestAwaiter);
            }
            TopicResponse.Append(_builder, assemblyName, requestAwaiter);
            PartitionItem.Append(_builder, assemblyName, requestAwaiter);

            //methods
            StartMethod(requestAwaiter);
            BuildPartitionItems(requestAwaiter);

            if(requestAwaiter.Data is RequestAwaiterData)
                Produce(assemblyName, requestAwaiter);
            StopAsync();

            EndClass(requestAwaiter);

            End();//end file

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

        private string DataToPostfix(AttributeDatas.RequestAwaiter requestAwaiter)
        {
            if(requestAwaiter.Data is RequestAwaiterData)
            {
                return "RequestAwaiter";
            }

            if (requestAwaiter.Data is ResponderData)
            {
                return "Responder";
            }

            if (requestAwaiter.Data is ListenerData)
            {
                return "Listener";
            }

            throw new System.NotImplementedException();
        }

        private void StartClass(AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
    {requestAwaiter.Data.TypeSymbol.DeclaredAccessibility.ToName()} partial class {requestAwaiter.Data.TypeSymbol.Name} : I{requestAwaiter.Data.TypeSymbol.Name}{DataToPostfix(requestAwaiter)}
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

        private void EndClass(AttributeDatas.RequestAwaiter data)
        {
            _builder.Append($@"
    }}
");
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
            {requestAwaiter.OutcomeDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            _builder.Append($@"
            )
        {{
            _items = new PartitionItem[config.Processors.Length];
            for (int i = 0; i < config.Processors.Length; i++)
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
                        config.Processors[i].Income{i}.TopicName,
                        config.Processors[i].Income{i}.Partitions,
                        config.Processors[i].Income{i}.CanAnswerService
");
            }
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                _builder.Append($@",
                        config.Processors[i].Outcome{i}.TopicName,
                        producerPool{i}
");
            }
            _builder.Append($@",
                        config.Processors[i].Buckets,
                        config.Processors[i].MaxInFly
                        {(requestAwaiter.Data.UseLogger ? @",_loggerFactory.CreateLogger(config.Processors[i].GroupName)" : "")}
                        {(requestAwaiter.Data.ConsumerData.CheckCurrentState ? @",config.Processors[i].GetCurrentState" : "")}
                        {(requestAwaiter.Data.ConsumerData.UseAfterCommit ? @",config.Processors[i].AfterCommit" : "")}
                        {(requestAwaiter.Data.ProducerData.AfterSendResponse ? @",config.Processors[i].AfterSendResponse" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomOutcomeHeader ? @",config.Processors[i].CreateOutcomeHeader" : "")}
                        {(requestAwaiter.Data.ProducerData.CustomHeaders ? @",config.Processors[i].SetHeaders" : "")}
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

        private void Produce(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public Task<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                if (!requestAwaiter.OutcomeDatas[i].KeyType.IsKafkaNull())
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
                if (!requestAwaiter.OutcomeDatas[i].KeyType.IsKafkaNull())
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

        private void End()
        {
            _builder.Append($@"
}}
");
        }
    }
}