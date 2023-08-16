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
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public {assemblyName}.RequestHeader Message{i}Header;
            public Confluent.Kafka.Message<{inputData.TypesPair}> Message{i};
");
            }

            builder.Append($@"
        }}
");
        }
    }
}