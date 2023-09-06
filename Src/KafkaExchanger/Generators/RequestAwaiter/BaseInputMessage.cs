using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class BaseInputMessage
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public abstract class BaseInputMessage
        {{
            public string {TopicName()} {{ get; set; }}
            public Confluent.Kafka.TopicPartitionOffset {TopicPartitionOffset()} {{ get; set; }}
        }}
");
        }

        public static string TypeFullName(Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "BaseInputMessage";
        }

        public static string TopicPartitionOffset()
        {
            return "TopicPartitionOffset";
        }

        public static string TopicName()
        {
            return "TopicName";
        }
    }
}