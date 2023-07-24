using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class IncomeMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            BaseIncomeMessage(builder);
            AppendIncomeMessages(builder, assemblyName, requestAwaiter);
        }

        private static void BaseIncomeMessage(StringBuilder builder)
        {
            builder.Append($@"
        public abstract class BaseIncomeMessage
        {{
            public Confluent.Kafka.Partition Partition {{ get; set; }}
        }}
");
        }

        private static void AppendIncomeMessages(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.GenerateData requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                var incomeData = requestAwaiter.IncomeDatas[i];
                builder.Append($@"
        public class Income{i}Message : BaseIncomeMessage
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