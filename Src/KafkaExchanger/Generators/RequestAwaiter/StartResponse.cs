using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class StartResponse
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
            public {KafkaExchanger.Generators.RequestAwaiter.TopicResponse.TypeFullName(requestAwaiter)} ResponseProcess {{ get; set; }}
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "StartResponse";
        }

        public static string ResponseProcess()
        {
            return "ResponseProcess";
        }
    }
}