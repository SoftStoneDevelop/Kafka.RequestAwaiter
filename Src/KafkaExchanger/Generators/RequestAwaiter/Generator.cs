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
        StringBuilder _builder = new StringBuilder(1300);

        public void Generate(
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
            DelayProduce.Append(_builder, assemblyName, requestAwaiter);
            TryDelayProduceResult.Append(_builder, assemblyName, requestAwaiter);
            Config.Append(_builder);
            ProcessorConfig.Append(_builder, assemblyName, requestAwaiter);
            ConsumerInfo.Append(_builder);
            ProducerInfo.Append(_builder);

            InputMessages.Append(_builder, assemblyName, requestAwaiter);
            TopicResponse.Append(_builder, assemblyName, requestAwaiter);
            PartitionItem.Append(_builder, assemblyName, requestAwaiter);

            //methods
            StartMethod(requestAwaiter);
            BuildPartitionItems(requestAwaiter);
            Produce(assemblyName, requestAwaiter);
            ProduceDelay(assemblyName, requestAwaiter);
            StopAsync();
            Dispose();

            EndClass();

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
using System.Diagnostics;

namespace {requestAwaiter.Data.TypeSymbol.ContainingNamespace}
{{
");
        }

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
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                _builder.Append($@",
            {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
");
            }
            _builder.Append($@",
            Action<Confluent.Kafka.ConsumerConfig> changeConfig = null
            )
        {{
            BuildPartitionItems(
                config
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                _builder.Append($@",
                producerPool{i}
");
            }
            _builder.Append($@"
                );

            foreach (var item in _items)
            {{
                item.Start(
                    config.BootstrapServers,
                    config.GroupId,
                    changeConfig
                    );
            }}
        }}
");
        }

        private void EndClass()
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
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                _builder.Append($@",
            {requestAwaiter.OutputDatas[i].FullPoolInterfaceName} producerPool{i}
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
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                if (i != 0)
                {
                    _builder.Append(',');
                }

                _builder.Append($@"
                        config.Processors[i].Input{i}.TopicName,
                        config.Processors[i].Input{i}.Partitions
");
            }
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                _builder.Append($@",
                        config.Processors[i].Output{i}.TopicName,
                        producerPool{i}
");
            }
            _builder.Append($@",
                        config.Processors[i].Buckets,
                        config.Processors[i].MaxInFly
                        {(requestAwaiter.Data.UseLogger ? @",_loggerFactory.CreateLogger(config.GroupId)" : "")}
                        {(requestAwaiter.Data.ConsumerData.CheckCurrentState ? @",config.Processors[i].GetCurrentState" : "")}
                        {(requestAwaiter.Data.ConsumerData.UseAfterCommit ? @",config.Processors[i].AfterCommit" : "")}
");
            if(requestAwaiter.Data.AfterSend)
            {
                for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
                {
                    _builder.Append($@",
                        config.Processors[i].AfterSendOutput{i}
");
                }
            }
            _builder.Append($@"
                        );
            }}
        }}
");
        }

        private void StopAsync()
        {
            _builder.Append($@"
        public void StopAsync()
        {{
            foreach (var item in _items)
            {{
                item.Stop();
            }}
            
            _items = null;
        }}
");
        }

        private void Produce(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public async ValueTask<{assemblyName}.Response> Produce(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
            {requestAwaiter.OutputDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                _builder.Append($@"
            {requestAwaiter.OutputDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            _builder.Append($@"
            int waitResponseTimeout = 0
            )
        {{

            while(true)
            {{
                var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
                var item = _items[index];
                var tp =
                    await item.TryProduce(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
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
                ).ConfigureAwait(false);

                if(tp.Succsess)
                {{
                    return tp.Response;
                }}
            }}
        }}
        private uint _currentItemIndex = 0;
");
        }

        private void ProduceDelay(string assemblyName, AttributeDatas.RequestAwaiter requestAwaiter)
        {
            _builder.Append($@"
        public async ValueTask<{requestAwaiter.Data.TypeSymbol.Name}.DelayProduce> ProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
                {
                    _builder.Append($@"
            {requestAwaiter.OutputDatas[i].KeyType.GetFullTypeName(true)} key{i},
");
                }

                _builder.Append($@"
            {requestAwaiter.OutputDatas[i].ValueType.GetFullTypeName(true)} value{i},
");
            }
            _builder.Append($@"
            int waitResponseTimeout = 0
            )
        {{

            while(true)
            {{
                var index = Interlocked.Increment(ref _currentItemIndex) % (uint)_items.Length;
                var item = _items[index];
                var tp =
                    await item.TryProduceDelay(
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (!requestAwaiter.OutputDatas[i].KeyType.IsKafkaNull())
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
                ).ConfigureAwait(false);

                if(tp.Succsess)
                {{
                    return new {requestAwaiter.Data.TypeSymbol.Name}.DelayProduce(tp);
                }}
            }}
        }}
");
        }

        private void Dispose()
        {
            _builder.Append($@"
        public void Dispose()
        {{
            StopAsync();
        }}
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