using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.Responder
{
    internal static class ProcessorConfig
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            Class(builder, assemblyName, responder);
        }

        public static string TypeName()
        {
            return "ProcessorConfig";
        }

        public static string CreateAnswer()
        {
            return "CreateAnswer";
        }

        public static string LoadCurrentHorizon()
        {
            return "LoadCurrentHorizon";
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        private static void Class(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            public {TypeName()}(
                {CreateAnswerFuncType(responder)} createAnswer,
                {LoadCurrentHorizonFuncType(responder)} loadCurrentHorizon
");
            for ( var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@",
                {ConsumerInfo.TypeFullName(responder)} {ConsumerInfoNameCamel(inputData)}
");
            }
            builder.Append($@"
                )
            {{
                {CreateAnswer()} = createAnswer;
                {LoadCurrentHorizon()} = loadCurrentHorizon;
");
            for (var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                {ConsumerInfoName(inputData)} = {ConsumerInfoNameCamel(inputData)};
");
            }
            builder.Append($@"
            }}

            public {CreateAnswerFuncType(responder)} {CreateAnswer()} {{ get; init; }}

            public {LoadCurrentHorizonFuncType(responder)} {LoadCurrentHorizon()} {{ get; init; }}
");
            for (var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            public {ConsumerInfo.TypeFullName(responder)} {ConsumerInfoName(inputData)} {{ get; init; }}
");
            }
            builder.Append($@"
        }}
");
        }

        public static string CreateAnswerFuncType(KafkaExchanger.Datas.Responder responder)
        {
            return $"Func<{InputMessage.TypeFullName(responder)}, KafkaExchanger.Attributes.Enums.CurrentState, Task<{OutputMessage.TypeFullName(responder)}>>";
        }

        public static string LoadCurrentHorizonFuncType(KafkaExchanger.Datas.Responder responder)
        {
            var builder = new StringBuilder();
            builder.Append("Func<");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append("int[],");
            }
            builder.Append("ValueTask<long>>");

            return builder.ToString();
        }

        public static string ConsumerInfoName(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        private static string ConsumerInfoNameCamel(InputData inputData)
        {
            return ConsumerInfoName(inputData).ToCamel();
        }
    }
}