using KafkaExchanger.Datas;
using KafkaExchanger.Generators.Responder;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class ProcessorConfig
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            Start(builder, requestAwaiter);

            Constructor(builder, assemblyName, requestAwaiter);
            FieldsAndProperties(builder, assemblyName, requestAwaiter);

            End(builder, requestAwaiter);
        }

        public static void Start(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}() {{ }}
");
        }

        public static void End(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        }}
");
        }

        public static void Constructor(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            string afterSendFunc(OutputData outputData)
            {
                return $"afterSend{outputData.NamePascalCase}";
            }

            string checkOutputStatusFunc(OutputData outputData)
            {
                return $"check{outputData.NamePascalCase}Status";
            }

            string loadOutputFunc(OutputData outputData)
            {
                return $"load{outputData.MessageTypeName}";
            }

            string producerInfo(OutputData outputData)
            {
                return outputData.NameCamelCase;
            }

            string consumerInfo(InputData inputData)
            {
                return inputData.NameCamelCase;
            }

            builder.Append($@"
            public {TypeName()}(");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ConsumerInfo {consumerInfo(inputData)}");
            }

            if (requestAwaiter.InputDatas.Count > 0)
            {
                builder.Append(',');
            }
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ProducerInfo {producerInfo(outputData)}");
            }

            var currentStateParam = "currentState";
            var afterCommitParam = "afterCommit";
            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {currentStateParam}");
            }
            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@",
                {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {afterCommitParam}");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {afterSendFunc(outputData)}");

                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {loadOutputFunc(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {checkOutputStatusFunc(outputData)}");

                }
            }

            builder.Append($@"
                )
            {{");

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {CurrentState()} = {currentStateParam};");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                {AfterCommit()} = {afterCommitParam};");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                {AfterSend(outputData)} = {afterSendFunc(outputData)};");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                {LoadOutput(outputData)} = {loadOutputFunc(outputData)};
                {CheckOutputStatus(outputData)} = {checkOutputStatusFunc(outputData)};");

                }
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {ConsumerInfo(inputData)} = {consumerInfo(inputData)};");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                {ProducerInfo(outputData)} = {producerInfo(outputData)};");
            }

            builder.Append($@"
            }}
");
        }

        public static void FieldsAndProperties(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
            public {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {CurrentState()} {{ get; init; }}");

            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
            public {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {AfterCommit()} {{ get; init; }}");

            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public {KafkaExchanger.Generators.RequestAwaiter.ConsumerInfo.TypeFullName(requestAwaiter)} {ConsumerInfo(inputData)} {{ get; init; }}");

            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public ProducerInfo {ProducerInfo(outputData)} {{ get; init; }}");

                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
            public {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {AfterSend(outputData)} {{ get; init; }}");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
            public {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {LoadOutput(outputData)} {{ get; init; }}
            public {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {CheckOutputStatus(outputData)} {{ get; init; }}");
                }
            }
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "ProcessorConfig";
        }

        public static string ConsumerInfo(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        public static string ProducerInfo(OutputData outputData)
        {
            return outputData.NamePascalCase;
        }

        public static string LoadOutput(OutputData outputData)
        {
            return $"Load{outputData.MessageTypeName}";
        }

        public static string CheckOutputStatus(OutputData outputData)
        {
            return $"Check{outputData.NamePascalCase}Status";
        }

        public static string AfterSend(OutputData outputData)
        {
            return $"AfterSend{outputData.NamePascalCase}";
        }

        public static string AfterCommit()
        {
            return $"AfterCommit";
        }

        public static string CurrentState()
        {
            return $"CurrentState";
        }
    }
}