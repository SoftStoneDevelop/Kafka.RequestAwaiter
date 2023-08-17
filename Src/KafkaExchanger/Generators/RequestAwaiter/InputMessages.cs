using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class InputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            BaseInputMessage(builder);
            AppendInputMessages(builder, assemblyName, requestAwaiter);
        }

        private static void BaseInputMessage(StringBuilder builder)
        {
            builder.Append($@"
        public abstract class BaseInputMessage
        {{
            public string TopicName {{ get; set; }}
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private static void AppendInputMessages(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
        public class {inputData.MessageTypeName} : BaseInputMessage
        {{
            public {assemblyName}.ResponseHeader HeaderInfo {{ get; set; }}

            public Message<{inputData.TypesPair}> OriginalMessage {{ get; set; }}
");
                if (!inputData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            public {inputData.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                builder.Append($@"
            public {inputData.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }
    }
}