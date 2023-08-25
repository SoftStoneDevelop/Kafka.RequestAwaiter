using KafkaExchanger.Extensions;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class Responder
    {
        public static void Append(
            string assemblyName,
            KafkaExchanger.Datas.Responder responder,
            StringBuilder builder
            )
        {
            Start(builder, responder);

            Config.Append(builder, responder);
            ProcessorConfig.Append(builder, assemblyName, responder);

            InputMessage.Append(builder, assemblyName, responder);
            InputMessages.Append(builder, assemblyName, responder);

            OutputMessage.Append(builder, assemblyName, responder);
            OutputMessages.Append(builder, assemblyName, responder);

            ChannelInfo.Append(builder, assemblyName, responder);
            StartResponse.Append(builder, assemblyName, responder);
            EndResponse.Append(builder, assemblyName, responder);

            ResponseProcess.Append(builder, assemblyName, responder);

            StartMethod(builder, responder);
            StopAsync(builder);

            End(builder);
        }

        private static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
    {responder.TypeSymbol.DeclaredAccessibility.ToName()} partial class {responder.TypeSymbol.Name} : I{responder.TypeSymbol.Name}Responder
    {{
        {(responder.UseLogger ? @"private readonly ILoggerFactory _loggerFactory;" : "")}
        private PartitionItem[] _items;
        
        public {responder.TypeSymbol.Name}({(responder.UseLogger ? @"ILoggerFactory loggerFactory" : "")})
        {{
            {(responder.UseLogger ? @"_loggerFactory = loggerFactory;" : "")}
        }}
");
        }

        private static void StartMethod(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
        public async Task Start(
            {responder.TypeSymbol.Name}.Config config
");
            for (int i = 0; i < responder.OutputDatas.Count; i++)
            {
                var outputData = responder.OutputDatas[i];
                builder.Append($@",
            {outputData.FullPoolInterfaceName} {outputData.NameCamelCase}Pool
");
            }
            builder.Append($@"
            )
        {{
            _items = new PartitionItem[config.ConsumerConfigs.Length];
            for (int i = 0; i < config.ConsumerConfigs.Length; i++)
            {{
                _items[i] =
                    new PartitionItem(
                        config.ServiceName,
                        config.ConsumerConfigs[i].InputTopicName,
                        config.ConsumerConfigs[i].CreateAnswer,
                        config.ConsumerConfigs[i].Partitions,
                        producerPool
                        );

                await _items[i].Start(
                    config.BootstrapServers,
                    config.GroupId,
                    config.ConsumerConfigs[i].LoadCurrentHorizon
                    );
            }}
        }}
");
        }

        private static void StopAsync(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public async Task StopAsync()
        {{
            if (_items == null)
            {{
                return;
            }}

            foreach (var item in _items)
            {{
                await item.Stop();
            }}

            _items = null;
        }}
");
        }

        private static void End(StringBuilder builder)
        {
            builder.Append($@"
}}
");
        }
    }
}
