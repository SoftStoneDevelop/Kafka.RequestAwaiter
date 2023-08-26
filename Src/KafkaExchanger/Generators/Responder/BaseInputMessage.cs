using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class BaseInputMessage
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public abstract class BaseInputMessage
        {{
            public Confluent.Kafka.TopicPartitionOffset {TopicPartitionOffset()} {{ get; set; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "BaseInputMessage";
        }

        public static string TopicPartitionOffset()
        {
            return "TopicPartitionOffset";
        }
    }
}