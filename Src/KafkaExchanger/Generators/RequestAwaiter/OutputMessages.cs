using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class OutputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
        public class Output{i}Message
        {{
            public Confluent.Kafka.Message<{outputData.TypesPair}> Message;
");
                if (outputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@"
            public {outputData.KeyType.GetFullTypeName()} Key;
");
                }
                else
                {
                    builder.Append($@"
            public {outputData.KeyType.GetFullTypeName()} Key => Message.Key;
");
                }

                if (outputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@"
            public {outputData.ValueType.GetFullTypeName()} Value;
");
                }
                else
                {
                    builder.Append($@"
            public {outputData.ValueType.GetFullTypeName()} Value => Message.Value;
");
                }

                builder.Append($@"
        }}
");
            }
        }
    }
}