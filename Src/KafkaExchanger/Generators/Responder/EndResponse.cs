using KafkaExchanger.Datas;
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
        public class {TypeName()} : {ChannelInfo.TypeFullName(responder)}
        {{

            public int {BucketId()} {{ get; set; }}

            public string {Guid()} {{ get; set; }}
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