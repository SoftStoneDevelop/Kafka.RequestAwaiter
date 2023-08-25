using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ProducerInfo
    {
        public static void Append(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}() {{ }}

            public ProducerInfo(
                string topicName
                )
            {{
                TopicName = topicName;
            }}

            public string TopicName {{ get; init; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "ProducerInfo";
        }
    }
}