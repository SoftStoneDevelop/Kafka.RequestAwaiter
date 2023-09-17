using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class InputMessages
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            AppendInputMessages(builder, assemblyName, responder);
        }

        private static void AppendInputMessages(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
        public class {TypeName(inputData)} : {BaseInputMessage.TypeFullName(responder)}
        {{
            public KafkaExchanger.RequestHeader {Header()} {{ get; set; }}

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

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder, InputData inputData)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName(inputData)}";
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