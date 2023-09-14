using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Config
    {
        public static void Append(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            Start(builder, requestAwaiter);

            Constructor(builder, requestAwaiter);
            PropertiesAndFields(builder, requestAwaiter);
            Validate(builder, requestAwaiter);

            End(builder, requestAwaiter);
        }

        private static void Start(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
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
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        }}
");
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {TypeName()}(
                string groupId,
                string bootstrapServers,
                int itemsInBucket,
                int inFlyBucketsLimit,
                {requestAwaiter.AddNewBucketFuncType()} addNewBucket,
                {requestAwaiter.BucketsCountFuncType()} bucketsCount,
                {ProcessorConfig.TypeFullName(requestAwaiter)}[] processors
                )
            {{
                {GroupId()} = groupId;
                {BootstrapServers()} = bootstrapServers;
                {ItemsInBucket()} = itemsInBucket;
                {InFlyBucketsLimit()} = inFlyBucketsLimit;
                {AddNewBucket()} = addNewBucket;
                {BucketsCount()} = bucketsCount;
                {Processors()} = processors;
            }}
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public string {GroupId()} {{ get; init; }}

            public string {BootstrapServers()} {{ get; init; }}

            public int {ItemsInBucket()} {{ get; init; }}

            public int {InFlyBucketsLimit()} {{ get; init; }}

            public {requestAwaiter.AddNewBucketFuncType()} {AddNewBucket()} {{ get; init; }}

            public {requestAwaiter.BucketsCountFuncType()} {BucketsCount()} {{ get; init; }}

            public {ProcessorConfig.TypeFullName(requestAwaiter)}[] {Processors()} {{ get; init; }}
");
        }

        private static void Validate(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Validate()
            {{
                var topicPartition = new Dictionary<string, HashSet<int>>();
                foreach(var processor in {Processors()})
                {{");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                var consumerName = $"{ProcessorConfig.ConsumerInfo(inputData).ToLowerInvariant()}";
                var definePartitions = i == 0 ? "var" : "";
                builder.Append($@"
                    var {consumerName} = processor.{ProcessorConfig.ConsumerInfo(inputData)};
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

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "Config";
        }

        public static string GroupId()
        {
            return "GroupId";
        }

        public static string BootstrapServers()
        {
            return "BootstrapServers";
        }

        public static string Processors()
        {
            return "Processors";
        }

        public static string ItemsInBucket()
        {
            return "ItemsInBucket";
        }

        public static string InFlyBucketsLimit()
        {
            return "InFlyBucketsLimit";
        }

        public static string AddNewBucket()
        {
            return "AddNewBucket";
        }

        public static string BucketsCount()
        {
            return "BucketsCount";
        }

        public static string Validate()
        {
            return "Validate";
        }
    }
}