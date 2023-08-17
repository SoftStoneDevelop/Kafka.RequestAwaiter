using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TryDelayProduceResult
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class TryDelayProduceResult
        {{
            public bool Succsess;
            public {requestAwaiter.Data.TypeSymbol.Name}.TopicResponse Response;
            public {requestAwaiter.Data.TypeSymbol.Name}.PartitionItem.Bucket Bucket;
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public {assemblyName}.RequestHeader Output{i}Header;
            public Output{i}Message Output{i}Message;
");
            }

            builder.Append($@"
        }}
");
        }
    }
}