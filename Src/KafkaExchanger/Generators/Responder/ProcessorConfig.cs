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
            Start(builder, assemblyName, responder);

            Constructor(builder, assemblyName, responder);
            PropertiesAndFields(builder, assemblyName, responder);

            End(builder, assemblyName, responder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
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

        public static string AfterCommit()
        {
            return "AfterCommit";
        }

        public static string CheckCurrentState()
        {
            return "CheckCurrentState";
        }

        public static string AfterSend()
        {
            return "AfterSend";
        }

        public static string ConsumerInfoName(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        private static void Start(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
");
        }

        private static void End(
            StringBuilder builder,
            string assemblyName,
            Datas.Responder responder
            )
        {
            builder.Append($@"
        }}
");
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            string consumerInfo(InputData inputData)
            {
                return inputData.NameCamelCase;
            }

            builder.Append($@"
            public {TypeName()}(
                {responder.CreateAnswerFuncType()} createAnswer,
                {responder.LoadCurrentHorizonFuncType()} loadCurrentHorizon");
            if (responder.AfterCommit)
            {
                builder.Append($@",
                {responder.AfterCommitFuncType()} afterCommit");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@",
                {responder.CheckCurrentStateFuncType()} checkState");
            }

            if (responder.AfterSend)
            {
                builder.Append($@",
                {responder.AfterSendFuncType()} afterSend");
            }

            for ( var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@",
                {ConsumerInfo.TypeFullName(responder)} {consumerInfo(inputData)}");
            }
            builder.Append($@"
                )
            {{
                {CreateAnswer()} = createAnswer;
                {LoadCurrentHorizon()} = loadCurrentHorizon;");

            if(responder.AfterCommit)
            {
                builder.Append($@"
                {AfterCommit()} = afterCommit;");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@"
                {CheckCurrentState()} = checkState;");
            }

            if (responder.AfterSend)
            {
                builder.Append($@"
                {AfterSend()} = afterSend;");
            }

            for (var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                {ConsumerInfoName(inputData)} = {consumerInfo(inputData)};");
            }
            builder.AppendLine($@"
            }}
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public {responder.CreateAnswerFuncType()} {CreateAnswer()} {{ get; init; }}

            public {responder.LoadCurrentHorizonFuncType()} {LoadCurrentHorizon()} {{ get; init; }}");

            if (responder.AfterCommit)
            {
                builder.Append($@"
            public {responder.AfterCommitFuncType()} {AfterCommit()} {{ get; init; }}");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@"
            public {responder.CheckCurrentStateFuncType()} {CheckCurrentState()} {{ get; init; }}");
            }

            if (responder.AfterSend)
            {
                builder.Append($@"
            public {responder.AfterSendFuncType()} {AfterSend()} {{ get; init; }}");
            }

            for (var i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            public {ConsumerInfo.TypeFullName(responder)} {ConsumerInfoName(inputData)} {{ get; init; }}");
            }
        }
    }
}