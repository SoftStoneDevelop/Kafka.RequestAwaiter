using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ResponseMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            BaseResponseMessage(builder);
            AppendResponseMessages(builder, assemblyName, requestAwaiter);
        }

        private static void BaseResponseMessage(StringBuilder builder)
        {
            builder.Append($@"
        public abstract class BaseResponseMessage
        {{
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private static void AppendResponseMessages(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                var incomeData = requestAwaiter.IncomeDatas[i];
                builder.Append($@"
        public class ResponseTopic{i}Message : BaseResponseMessage
        {{
            public {assemblyName}.ResponseHeader HeaderInfo {{ get; set; }}

            public Message<{incomeData.TypesPair}> OriginalMessage {{ get; set; }}
");
                if (!incomeData.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            public {incomeData.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                builder.Append($@"
            public {incomeData.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }
    }
}