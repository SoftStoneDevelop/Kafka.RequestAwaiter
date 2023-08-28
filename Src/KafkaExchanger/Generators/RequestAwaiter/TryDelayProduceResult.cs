using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TryDelayProduceResult
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            public bool {Succsess()};
            public {TopicResponse.TypeFullName(requestAwaiter)} {Response()};
            public {KafkaExchanger.Generators.RequestAwaiter.Bucket.TypeFullName(requestAwaiter)} {Bucket()};
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public {assemblyName}.RequestHeader {Header(outputData)};
            public {outputData.MessageTypeName} {Message(outputData)};
");
            }

            builder.Append($@"
        }}
");
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "TryDelayProduceResult";
        }

        public static string Succsess()
        {
            return "Succsess";
        }

        public static string Bucket()
        {
            return "Bucket";
        }

        public static string Response()
        {
            return "Response";
        }

        public static string Header(OutputData outputData)
        {
            return $"{outputData.NamePascalCase}Header";
        }

        public static string Message(OutputData outputData)
        {
            return $"{outputData.MessageTypeName}";
        }
    }
}