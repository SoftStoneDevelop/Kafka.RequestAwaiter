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
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                Output{i}Message output{i}
");
            }
            builder.Append($@"
                )
            {{
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
                Output{i} = output{i};
");
            }
            builder.Append($@"
            }}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                builder.Append($@"
            Output{i}Message Output{i} {{ get; init; }}
");
            }

            builder.Append($@"
        }}
");
        }
    }
}