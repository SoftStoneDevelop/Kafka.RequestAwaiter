using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class InputMessage
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            public {inputData.MessageTypeName} {Message(inputData)} {{ get; set; }}
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
            return "InputMessage";
        }

        public static string Message(InputData inputData)
        {
            return inputData.MessageTypeName;
        }
    }
}