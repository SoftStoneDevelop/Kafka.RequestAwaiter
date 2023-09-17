using KafkaExchanger.Datas;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class OutputMessage
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
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public KafkaExchanger.RequestHeader {Header(outputData)} {{ get; set; }}
            public {OutputMessages.TypeFullName(requestAwaiter, outputData)} {Message(outputData)} {{ get; set; }}");
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
            return "OutputMessage";
        }

        public static string Message(OutputData outputData)
        {
            return outputData.NamePascalCase;
        }

        public static string Header(OutputData outputData)
        {
            return $"{outputData.NamePascalCase}Header";
        }
    }
}