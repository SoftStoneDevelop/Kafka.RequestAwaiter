using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class Config
    {
        public static void Append(
            StringBuilder builder,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}()
            {{
            }}

            public {TypeName()}(
                string groupId,
                string serviceName,
                string bootstrapServers,
                {ProcessorConfig.TypeFullName(responder)}[] processors
                )
            {{
                GroupId = groupId;
                ServiceName = serviceName;
                BootstrapServers = bootstrapServers;
                Processors = processors;
            }}

            public string GroupId {{ get; init; }}

            public string ServiceName {{ get; init; }}

            public string BootstrapServers {{ get; init; }}

            public {ProcessorConfig.TypeFullName(responder)}[] Processors {{ get; init; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "Config";
        }
    }
}