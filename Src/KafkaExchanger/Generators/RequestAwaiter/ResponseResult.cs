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
            AttributeDatas.GenerateData requestAwaiter
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
                if (i != 0)
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
}