using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Pool
{
    internal static class Interface
    {
        public static  void Append(
            StringBuilder builder,
            OutputData outputData
            )
        {

            Start(builder, outputData);

            ProduceByName(builder, outputData);
            ProduceByPartition(builder, outputData);

            End(builder, outputData);
        }

        public static string TypeFullName(
            string assemblyName,
            OutputData outputData
            )
        {
            return $"{assemblyName}.{TypeName(outputData)}";
        }

        public static string TypeName(OutputData outputData)
        {
            return $"IProducerPool{outputData.KeyTypeAlias}{outputData.ValueTypeAlias}";
        }

        private static void Start(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
    public partial interface {TypeName(outputData)}
    {{
");
        }

        private static void ProduceByName(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
        public Task {Produce()}(string topicName, Confluent.Kafka.Message<{outputData.TypesPair}> message);
");
        }

        private static void ProduceByPartition(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
        public Task {Produce()}(Confluent.Kafka.TopicPartition topicPartition, Confluent.Kafka.Message<{outputData.TypesPair}> message);
");
        }

        public static string Produce()
        {
            return "Produce";
        }

        private static void End(
            StringBuilder builder,
            OutputData outputData
            )
        {
            builder.Append($@"
    }}
");
        }
    }
}
