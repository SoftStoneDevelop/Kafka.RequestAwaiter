using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Pool
{
    internal static class ByTopicPartition
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {

            builder.Append($@"
        private class {TypeName()} : {ProduceInfo.TypeFullName(assemblyName, outputData)}
        {{
            public Confluent.Kafka.TopicPartition {TopicPartition()};
        }}
");
        }

        public static string TypeFullName(
            string assemblyName,
            OutputData outputData
            )
        {
            return $"{Pool.TypeFullName(assemblyName, outputData)}.{TypeName()}";
        }

        public static string TypeName()
        {
            return $"ByTopicPartition";
        }

        public static string TopicPartition()
        {
            return $"TopicPartition";
        }
    }
}