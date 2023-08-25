using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class InputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            BaseInputMessage(builder);
            AppendInputMessages(builder, assemblyName, responder);
        }

        private static void BaseInputMessage(StringBuilder builder)
        {
            builder.Append($@"
        public abstract class BaseInputMessage
        {{
            public long HorizonId {{ get; set; }}

            public Confluent.Kafka.TopicPartitionOffset TopicPartitionOffset {{ get; set; }}
        }}
");
        }

        private static void AppendInputMessages(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
        public class {inputData.MessageTypeName} : {responder.TypeSymbol.Name}.BaseInputMessage
        {{
            public {assemblyName}.RequestHeader Header {{ get; set; }}

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