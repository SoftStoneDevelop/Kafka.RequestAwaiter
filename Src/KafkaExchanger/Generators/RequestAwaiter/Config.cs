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
            builder.Append($@"
        public class {TypeName()}
        {{
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

            public string {GroupId()} {{ get; init; }}

            public string {BootstrapServers()} {{ get; init; }}

            public int {ItemsInBucket()} {{ get; init; }}

            public int {InFlyBucketsLimit()} {{ get; init; }}

            public {requestAwaiter.AddNewBucketFuncType()} {AddNewBucket()} {{ get; init; }}

            public {requestAwaiter.BucketsCountFuncType()} {BucketsCount()} {{ get; init; }}

            public ProcessorConfig[] {Processors()} {{ get; init; }}
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
    }
}