using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Config
    {
        public static void Append(StringBuilder builder)
        {
            builder.Append($@"
        public class Config
        {{
            public Config(
                string groupId,
                string bootstrapServers,
                ProcessorConfig[] processors
                )
            {{
                GroupId = groupId;
                BootstrapServers = bootstrapServers;
                Processors = processors;
            }}

            public string GroupId {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public ProcessorConfig[] Processors {{ get; init; }}
        }}
");
        }
    }
}