using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ProducerInfo
    {
        public static void Append(
            StringBuilder builder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}() {{ }}

            public ProducerInfo(
                string topicName
                )
            {{
                {TopicName()} = topicName;
            }}

            public string {TopicName()} {{ get; init; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "ProducerInfo";
        }

        public static string TopicName()
        {
            return "TopicName";
        }
    }
}