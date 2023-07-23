using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ResponseResult
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class ResponseResult
        {{
            private ResponseResult() {{ }}            

            public ResponseResult(
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeDatas = requestAwaiter.OutcomeDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                Outcome{i}Message outcome{i}
");
            }
            builder.Append($@"
                )
            {{
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeDatas = requestAwaiter.OutcomeDatas[i];
                builder.Append($@"
                Outcome{i} = outcome{i};
");
            }
            builder.Append($@"
            }}
");
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeDatas = requestAwaiter.OutcomeDatas[i];
                builder.Append($@"
            Outcome{i}Message Outcome{i} {{ get; init; }}
");
            }

            builder.Append($@"
        }}
");
        }
    }

    internal static class OutcomeMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.OutcomeDatas.Count; i++)
            {
                var outcomeDatas = requestAwaiter.OutcomeDatas[i];
                builder.Append($@"
        public class Outcome{i}Message
        {{
");
                if (!outcomeDatas.KeyType.IsKafkaNull())
                {
                    builder.Append($@"
            public {outcomeDatas.KeyType.GetFullTypeName(true)} Key {{ get; set; }}
");
                }

                builder.Append($@"
            public {outcomeDatas.ValueType.GetFullTypeName(true)} Value {{ get; set; }}
        }}
");
            }
        }
    }

    internal static class IncomeMessages
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
        public class Income{i}Message : BaseResponseMessage
        {{
            public {assemblyName}.{(requestAwaiter.Data is RequestAwaiterData ? "ResponseHeader" : "RequestHeader")} HeaderInfo {{ get; set; }}

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