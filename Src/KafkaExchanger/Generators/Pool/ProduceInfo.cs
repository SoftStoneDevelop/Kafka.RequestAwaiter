using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Pool
{
    internal static class ProduceInfo
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            OutputData outputData
            )
        {

            builder.Append($@"
        private abstract class {TypeName()}
        {{
            public Confluent.Kafka.Message<{outputData.TypesPair}> {Message()};
            public TaskCompletionSource {CompletionSource()} = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
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
            return $"ProduceInfo";
        }

        public static string Message()
        {
            return $"Message";
        }

        public static string CompletionSource()
        {
            return $"CompletionSource";
        }
    }
}