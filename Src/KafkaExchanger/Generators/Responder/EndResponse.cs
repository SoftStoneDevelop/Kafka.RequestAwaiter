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
        private class {TypeName()} : {ChannelInfo.TypeFullName(responder)}
        {{
            public Confluent.Kafka.TopicPartitionOffset Input0 {{ get; set; }}
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            public Confluent.Kafka.TopicPartitionOffset {inputData.NamePascalCase} {{ get; set; }}
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
    }
}