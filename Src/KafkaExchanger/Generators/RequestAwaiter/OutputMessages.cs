using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class OutputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
        public class {outputData.MessageTypeName}
        {{
            private {outputData.MessageTypeName}() {{ }}

            public {outputData.MessageTypeName}(
                Confluent.Kafka.Message<{outputData.TypesPair}> message
");
                if (outputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@",
                {outputData.KeyType.GetFullTypeName()} key
");
                }

                if (outputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@",
                {outputData.ValueType.GetFullTypeName()} value
");
                }
                builder.Append($@"
            ) 
            {{
                Message = message;
");
                if (outputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@"
                Key = key;
");
                }

                if (outputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@"
                Value = value;
");
                }

                builder.Append($@"
            }}

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