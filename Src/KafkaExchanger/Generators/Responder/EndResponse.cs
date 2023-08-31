using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class EndResponse
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()} : {ChannelInfo.TypeFullName(responder)}
        {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"

            public int {BucketId()} {{ get; set; }}

            public int {MessageId()} {{ get; set; }}

            public Confluent.Kafka.TopicPartitionOffset {Offset(inputData)} {{ get; set; }}
");
            }
            builder.Append($@"
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "EndResponse";
        }

        public static string Offset(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        public static string BucketId()
        {
            return "BucketId";
        }

        public static string MessageId()
        {
            return "MessageId";
        }
    }
}