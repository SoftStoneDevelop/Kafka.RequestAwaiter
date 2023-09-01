using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class SetOffsetResponse
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
            public int {BucketId()} {{ get; set; }}

            public string {Guid()} {{ get; set; }}

            public int {OffsetId()} {{ get; set; }}

            public Confluent.Kafka.TopicPartitionOffset {Offset()} {{ get; set; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "SetOffsetResponse";
        }

        public static string Offset()
        {
            return "Offset";
        }

        public static string BucketId()
        {
            return "BucketId";
        }

        public static string Guid()
        {
            return "Guid";
        }

        public static string OffsetId()
        {
            return "OffsetId";
        }
    }
}