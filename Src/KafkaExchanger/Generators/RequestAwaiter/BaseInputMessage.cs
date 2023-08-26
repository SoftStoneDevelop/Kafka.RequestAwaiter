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
            public Confluent.Kafka.Partition {Partition()} {{ get; set; }}
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

        public static string Partition()
        {
            return "Partition";
        }

        public static string TopicName()
        {
            return "TopicName";
        }
    }
}