using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class Config
    {
        public static void Append(
            StringBuilder builder,
            Datas.Responder responder
            )
        {
            Start(builder, responder);

            Constructor(builder, responder);
            PropertiesAndFields(builder, responder);
            Validate(builder, responder);

            End(builder, responder);
        }

        private static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}()
            {{
            }}
");
        }

        private static void End(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
        }}
");
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public {TypeName()}(
                string groupId,
                string serviceName,
                string bootstrapServers,
                int itemsInBucket,
                int inFlyLimit,
                {responder.AddNewBucketFuncType()} addNewBucket,
                {responder.BucketsCountFuncType()} bucketsCount,
                {ProcessorConfig.TypeFullName(responder)}[] processors
                )
            {{
                {GroupId()} = groupId;
                {ServiceName()} = serviceName;
                {BootstrapServers()} = bootstrapServers;
                {ItemsInBucket()} = itemsInBucket;
                {InFlyLimit()} = inFlyLimit;
                {AddNewBucket()} = addNewBucket;
                {BucketsCount()} = bucketsCount;
                {Processors()} = processors;
            }}
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public string {GroupId()} {{ get; init; }}

            public string {ServiceName()} {{ get; init; }}

            public string {BootstrapServers()} {{ get; init; }}

            public int {ItemsInBucket()} {{ get; init; }}

            public int {InFlyLimit()} {{ get; init; }}

            public {responder.AddNewBucketFuncType()} {AddNewBucket()} {{ get; init; }}

            public {responder.BucketsCountFuncType()} {BucketsCount()} {{ get; init; }}

            public {ProcessorConfig.TypeFullName(responder)}[] {Processors()} {{ get; init; }}
");
        }

        private static void Validate(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public void Validate()
            {{
                var topicPartition = new Dictionary<string, HashSet<int>>();
                foreach(var processor in {Processors()})
                {{");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                var consumerName = $"{ProcessorConfig.ConsumerInfoName(inputData).ToLowerInvariant()}";
                var definePartitions = i == 0 ? "var" : "";
                builder.Append($@"
                    var {consumerName} = processor.{ProcessorConfig.ConsumerInfoName(inputData)};
                    if(!topicPartition.TryGetValue({consumerName}.{ConsumerInfo.TopicName()}, out {definePartitions} partitions))
                    {{
                        partitions = new HashSet<int>();
                        topicPartition[{consumerName}.{ConsumerInfo.TopicName()}] = partitions;
                    }}

                    for(int i = 0; i < {consumerName}.{ConsumerInfo.Partitions()}.Length; i++)
                    {{
                        var partition = {consumerName}.{ConsumerInfo.Partitions()}[i];
                        if(!partitions.Add(partition))
                        {{
                            throw new Exception($@""The configurations overlap each other: topic '{{{consumerName}.{ConsumerInfo.TopicName()}}}', partition '{{partition}}'."");
                        }}
                    }}
");
            }
            builder.Append($@"
                }}
            }}
");
        }

        public static string ItemsInBucket()
        {
            return "ItemsInBucket";
        }

        public static string InFlyLimit()
        {
            return "InFlyLimit";
        }

        public static string AddNewBucket()
        {
            return "AddNewBucket";
        }

        public static string BucketsCount()
        {
            return "BucketsCount";
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "Config";
        }

        public static string Processors()
        {
            return "Processors";
        }

        public static string BootstrapServers()
        {
            return "BootstrapServers";
        }

        public static string ServiceName()
        {
            return "ServiceName";
        }

        public static string GroupId()
        {
            return "GroupId";
        }

        public static string Validate()
        {
            return "Validate";
        }
    }
}