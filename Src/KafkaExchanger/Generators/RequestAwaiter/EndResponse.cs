using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class EndResponse
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()} : {ChannelInfo.TypeFullName(requestAwaiter)}
        {{

            public int {BucketId()} {{ get; set; }}

            public string {Guid()} {{ get; set; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "EndResponse";
        }

        public static string BucketId()
        {
            return "BucketId";
        }

        public static string Guid()
        {
            return "Guid";
        }
    }
}