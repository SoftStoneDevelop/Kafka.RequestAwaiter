using KafkaExchanger.Datas;
using KafkaExchanger.Helpers;
using System;
using System.Collections.Generic;
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
            builder.Append($@"
        public class {TypeName()}
        {{
            private {TypeName()}() {{ }}

            public {TypeName()}(
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(i != 0)
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

            builder.Append($@"
                {(requestAwaiter.CheckCurrentState ? $",{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {currentStateFunc()}" : "")}
                {(requestAwaiter.AfterCommit ? $",{requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {afterCommitFunc()}" : "")}
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

            builder.Append($@",
                int {buckets()},
                int {maxInFly()}
                )
            {{
                {Buckets()} = {buckets()};
                {MaxInFly()} = {maxInFly()};
");
            if(requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {CurrentStateFunc()} = {currentStateFunc()};
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                {AfterCommitFunc()} = {afterCommitFunc()};
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
                if(requestAwaiter.AfterSend)
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
            return "ProcessorConfig";
        }

        public static string ConsumerInfo(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        private static string consumerInfo(InputData inputData)
        {
            return inputData.NameCamelCase;
        }

        public static string ProducerInfo(OutputData outputData)
        {
            return outputData.NamePascalCase;
        }

        private static string producerInfo(OutputData outputData)
        {
            return outputData.NameCamelCase;
        }

        public static string LoadOutputFunc(OutputData outputData)
        {
            return $"Load{outputData.MessageTypeName}";
        }

        private static string loadOutputFunc(OutputData outputData)
        {
            return $"load{outputData.MessageTypeName}";
        }

        public static string CheckOutputStatusFunc(OutputData outputData)
        {
            return $"Check{outputData.NamePascalCase}Status";
        }

        private static string checkOutputStatusFunc(OutputData outputData)
        {
            return $"check{outputData.NamePascalCase}Status";
        }

        public static string AfterSendFunc(OutputData outputData)
        {
            return $"AfterSend{outputData.NamePascalCase}";
        }

        private static string afterSendFunc(OutputData outputData)
        {
            return $"afterSend{outputData.NamePascalCase}";
        }

        public static string Buckets()
        {
            return $"Buckets";
        }

        private static string buckets()
        {
            return $"buckets";
        }

        public static string MaxInFly()
        {
            return $"MaxInFly";
        }

        private static string maxInFly()
        {
            return $"maxInFly";
        }

        public static string AfterCommitFunc()
        {
            return $"AfterCommit";
        }

        private static string afterCommitFunc()
        {
            return $"afterCommit";
        }

        public static string CurrentStateFunc()
        {
            return $"CurrentState";
        }

        private static string currentStateFunc()
        {
            return $"currentState";
        }
    }
}