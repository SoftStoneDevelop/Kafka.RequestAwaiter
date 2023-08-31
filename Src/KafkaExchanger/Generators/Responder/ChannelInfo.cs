using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class ChannelInfo
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public abstract class {TypeName()}
        {{
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "ChannelInfo";
        }
    }
}