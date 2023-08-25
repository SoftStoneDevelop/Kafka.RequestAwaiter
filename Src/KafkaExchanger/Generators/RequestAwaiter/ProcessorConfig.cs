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
        public class ProcessorConfig
        {{
            private ProcessorConfig() {{ }}

            public ProcessorConfig(
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                ConsumerInfo {ConsumerInfoNameCamel(inputData)}
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
                ProducerInfo {ProducerInfoNameCamel(outputData)}
");
            }

            builder.Append($@"
                {(requestAwaiter.CheckCurrentState ? $",{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {CurrentStateFuncNameCamel()}" : "")}
                {(requestAwaiter.AfterCommit ? $",{requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {AfterCommitFuncNameCamel()}" : "")}
");
            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@",
                {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {AfterSendFuncNameCamel(outputData)}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@",
                {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {LoadOutputFuncNameCamel(outputData)},
                {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {CheckOutputStatusFuncNameCamel(outputData)}
");
                }
            }

            builder.Append($@",
                int {BucketsNameCamel()},
                int {MaxInFlyNameCamel()}
                )
            {{
                {BucketsName()} = {BucketsNameCamel()};
                {MaxInFlyName()} = {MaxInFlyNameCamel()};
");
            if(requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {CurrentStateFuncName()} = {CurrentStateFuncNameCamel()};
");
            }

            if (requestAwaiter.AfterCommit)
            {
                builder.Append($@"
                {AfterCommitFuncName()} = {AfterCommitFuncNameCamel()};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                if (requestAwaiter.AfterSend)
                {
                    builder.Append($@"
                {AfterSendFuncName(outputData)} = {AfterSendFuncNameCamel(outputData)};
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
                {LoadOutputFuncName(outputData)} = {LoadOutputFuncNameCamel(outputData)};
                {CheckOutputStatusFuncName(outputData)} = {CheckOutputStatusFuncNameCamel(outputData)};
");
                }
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {ConsumerInfoName(inputData)} = {ConsumerInfoNameCamel(inputData)};
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
                {ProducerInfoName(outputData)} = {ProducerInfoNameCamel(outputData)};
");
            }

            builder.Append($@"
            }}

            public int {BucketsName()} {{ get; init; }}
            
            public int {MaxInFlyName()} {{ get; init; }}

            {(requestAwaiter.CheckCurrentState ? $"public {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {CurrentStateFuncName()} {{ get; init; }}" : "")}
            {(requestAwaiter.AfterCommit ? $"public {requestAwaiter.AfterCommitFunc(requestAwaiter.InputDatas)} {AfterCommitFuncName()} {{ get; init; }}" : "")}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public ConsumerInfo {ConsumerInfoName(inputData)} {{ get; init; }}
");
            }

            for (int i = 0; i < requestAwaiter.OutputDatas.Count; i++)
            {
                var outputData = requestAwaiter.OutputDatas[i];
                builder.Append($@"
            public ProducerInfo {ProducerInfoName(outputData)} {{ get; init; }}
");
                if(requestAwaiter.AfterSend)
                {
                    builder.Append($@"
            public {requestAwaiter.AfterSendFunc(assemblyName, outputData, i)} {AfterSendFuncName(outputData)} {{ get; init; }}
");
                }

                if (requestAwaiter.AddAwaiterCheckStatus)
                {
                    builder.Append($@"
            public {requestAwaiter.LoadOutputMessageFunc(assemblyName, outputData, requestAwaiter.InputDatas)} {LoadOutputFuncName(outputData)} {{ get; init; }}
            public {requestAwaiter.AddAwaiterStatusFunc(assemblyName, requestAwaiter.InputDatas)} {CheckOutputStatusFuncName(outputData)} {{ get; init; }}
");
                }
            }

            builder.Append($@"
        }}
");
        }

        public static string ConsumerInfoName(InputData inputData)
        {
            return inputData.NamePascalCase;
        }

        public static string ConsumerInfoNameCamel(InputData inputData)
        {
            return ConsumerInfoName(inputData).ToCamel();
        }

        public static string ProducerInfoName(OutputData outputData)
        {
            return outputData.NamePascalCase;
        }

        public static string ProducerInfoNameCamel(OutputData outputData)
        {
            return ProducerInfoName(outputData).ToCamel();
        }

        public static string LoadOutputFuncName(OutputData outputData)
        {
            return $"Load{outputData.MessageTypeName}";
        }

        public static string LoadOutputFuncNameCamel(OutputData outputData)
        {
            return LoadOutputFuncName(outputData).ToCamel();
        }

        public static string CheckOutputStatusFuncName(OutputData outputData)
        {
            return $"Check{outputData.NamePascalCase}Status";
        }

        public static string CheckOutputStatusFuncNameCamel(OutputData outputData)
        {
            return CheckOutputStatusFuncName(outputData).ToCamel();
        }

        public static string AfterSendFuncName(OutputData outputData)
        {
            return $"AfterSend{outputData.NamePascalCase}";
        }

        public static string AfterSendFuncNameCamel(OutputData outputData)
        {
            return AfterSendFuncName(outputData).ToCamel();
        }

        public static string BucketsName()
        {
            return $"Buckets";
        }

        public static string BucketsNameCamel()
        {
            return BucketsName().ToCamel();
        }

        public static string MaxInFlyName()
        {
            return $"MaxInFly";
        }

        public static string MaxInFlyNameCamel()
        {
            return MaxInFlyName().ToCamel();
        }

        public static string AfterCommitFuncName()
        {
            return $"AfterCommit";
        }

        public static string AfterCommitFuncNameCamel()
        {
            return AfterCommitFuncName().ToCamel();
        }

        public static string CurrentStateFuncName()
        {
            return $"CurrentState";
        }

        public static string CurrentStateFuncNameCamel()
        {
            return CurrentStateFuncName().ToCamel();
        }
    }
}