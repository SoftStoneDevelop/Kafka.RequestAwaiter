using KafkaExchanger.Datas;
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
            public {TypeName()}(
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ConsumerInfo {consumerInfo(inputData)}
");
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
                ProducerInfo {producerInfo(outputData)}
");
            }

            var currentStateParam = "currentState";
            var afterCommitParam = "afterCommit";
            builder.Append($@"
                {(requestAwaiter.CheckCurrentState ? $",{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {currentStateParam}" : "")}
                {(requestAwaiter.AfterCommit ? $",{requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {afterCommitParam}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {afterSendFunc(outputData)}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {loadOutputFunc(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {checkOutputStatusFunc(outputData)}
");
                }
            }

            var maxInFlyParam = "maxInFly";
            var bucketsParam = "buckets";
            builder.Append($@",
                int {bucketsParam},
                int {maxInFlyParam}
                )
            {{
                {Buckets()} = {bucketsParam};
                {MaxInFly()} = {maxInFlyParam};
");
            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {CurrentStateFunc()} = {currentStateParam};
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                {AfterCommitFunc()} = {afterCommitParam};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                {AfterSendFunc(outputData)} = {afterSendFunc(outputData)};
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                {LoadOutputFunc(outputData)} = {loadOutputFunc(outputData)};
                {CheckOutputStatusFunc(outputData)} = {checkOutputStatusFunc(outputData)};
");
                }
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {ConsumerInfo(inputData)} = {consumerInfo(inputData)};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                {ProducerInfo(outputData)} = {producerInfo(outputData)};
");
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
            builder.Append($@"

            public int {Buckets()} {{ get; init; }}
            
            public int {MaxInFly()} {{ get; init; }}

            {(requestAwaiter.CheckCurrentState ? $"public {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {CurrentStateFunc()} {{ get; init; }}" : "")}
            {(requestAwaiter.AfterCommit ? $"public {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {AfterCommitFunc()} {{ get; init; }}" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public ConsumerInfo {ConsumerInfo(inputData)} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public ProducerInfo {ProducerInfo(outputData)} {{ get; init; }}
");
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
            public {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {AfterSendFunc(outputData)} {{ get; init; }}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
            public {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {LoadOutputFunc(outputData)} {{ get; init; }}
            public {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {CheckOutputStatusFunc(outputData)} {{ get; init; }}
");
                }
            }
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
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

        public static string LoadOutputFunc(OutputData outputData)
        {
            return $"Load{outputData.MessageTypeName}";
        }

        public static string CheckOutputStatusFunc(OutputData outputData)
        {
            return $"Check{outputData.NamePascalCase}Status";
        }

        public static string AfterSendFunc(OutputData outputData)
        {
            return $"AfterSend{outputData.NamePascalCase}";
        }

        public static string Buckets()
        {
            return $"Buckets";
        }

        public static string MaxInFly()
        {
            return $"MaxInFly";
        }

        public static string AfterCommitFunc()
        {
            return $"AfterCommit";
        }

        public static string CurrentStateFunc()
        {
            return $"CurrentState";
        }
    }
}