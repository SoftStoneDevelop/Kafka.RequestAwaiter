using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class InputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            AppendInputMessages(builder, assemblyName, requestAwaiter);
        }

        private static void AppendInputMessages(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
        public class {inputData.MessageTypeName} : {BaseInputMessage.TypeFullName(requestAwaiter)}
        {{
            public KafkaExchanger.ResponseHeader {Header()} {{ get; set; }}

            public Message<{inputData.TypesPair}> {OriginalMessage()} {{ get; set; }}
");
                if (inputData.KeyType.IsProtobuffType())
                {
                    builder.Append($@"
            public {inputData.KeyType.GetFullTypeName(true)} {Key()} {{ get; set; }}
");
                }
                else
                {
                    builder.Append($@"
            public {inputData.KeyType.GetFullTypeName(true)} {Key()} => {OriginalMessage()}.Key;
");
                }

                if (inputData.ValueType.IsProtobuffType())
                {
                    builder.Append($@"
            public {inputData.ValueType.GetFullTypeName(true)} {Value()} {{ get; set; }}
");
                }
                else
                {
                    builder.Append($@"
            public {inputData.ValueType.GetFullTypeName(true)} {Value()} => {OriginalMessage()}.Value;
");
                }

                builder.Append($@"
        }}
");
            }
        }

        public static string TypeFullName(Datas.RequestAwaiter requestAwaiter, InputData inputData)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName(inputData)}";
        }

        public static string TypeName(InputData inputData)
        {
            return inputData.MessageTypeName;
        }

        public static string OriginalMessage()
        {
            return "OriginalMessage";
        }

        public static string Key()
        {
            return "Key";
        }

        public static string Value()
        {
            return "Value";
        }

        public static string Header()
        {
            return "Header";
        }
    }
}