using KafkaExchanger.Enums;
using KafkaExchanger.Generators.RequestAwaiter;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Datas
{
    internal class RequestAwaiter : Exchange
    {
        public static RequestAwaiter Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new RequestAwaiter();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 5)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!result.SetUseLogger(namedArguments[0]))
            {
                throw new Exception($"Fail create {nameof(RequestAwaiter)}");
            }

            if (!result.SetCheckCurrentState(namedArguments[1]))
            {
                throw new Exception($"Fail create {nameof(RequestAwaiter)}");
            }

            if (!result.SetAfterCommit(namedArguments[2]))
            {
                throw new Exception($"Fail create {nameof(RequestAwaiter)}");
            }

            if (!result.SetAfterSend(namedArguments[3]))
            {
                throw new Exception($"Fail create {nameof(RequestAwaiter)}");
            }

            if (!result.SetAddAwaiterCheckStatus(namedArguments[4]))
            {
                throw new Exception($"Fail create {nameof(RequestAwaiter)}");
            }

            return result;
        }

        public bool CheckCurrentState { get; private set; }

        public string GetCurrentStateFunc(List<InputData> inputDatas)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int,");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                var inputData = inputDatas[i];
                tempSb.Append($" int[], {inputData.MessageTypeName},");
            }
            tempSb.Append(" Task<KafkaExchanger.Attributes.Enums.RAState>>");

            return tempSb.ToString();
        }

        internal bool SetCheckCurrentState(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CheckCurrentState = (bool)argument.Value;
            return true;
        }

        public bool AfterSend { get; private set; }

        public string AfterSendFunc(
            string assemblyName,
            OutputData outputData
            )
        {
            return $"Func<int, {OutputMessages.TypeFullName(this, outputData)}, KafkaExchanger.RequestHeader, Task>";
        }

        internal bool SetAfterSend(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AfterSend = (bool)argument.Value;
            return true;
        }

        public bool AddAwaiterCheckStatus { get; private set; }

        public string AddAwaiterStatusFunc(
            string assemblyName
            )
        {
            var builder = new StringBuilder(200);
            builder.Append($"Func<string, int,");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                builder.Append($@" int[],");
            }

            builder.Append($" Task<KafkaExchanger.Attributes.Enums.RAState>>");
            return builder.ToString();
        }

        public string LoadOutputMessageFunc(
            string assemblyName,
            OutputData outputData
            )
        {
            var builder = new StringBuilder(200);
            builder.Append($"Func<string, int,");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                builder.Append($@" int[],");
            }

            builder.Append($" Task<{outputData.MessageTypeName}>>");
            return builder.ToString();
        }

        internal bool SetAddAwaiterCheckStatus(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AddAwaiterCheckStatus = (bool)argument.Value;
            return true;
        }

        public bool AfterCommit { get; private set; }

        public string AfterCommitFunc(List<InputData> inputDatas)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int, ");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                tempSb.Append($" int[],");
            }
            tempSb.Append(" Task>");

            return tempSb.ToString();
        }

        internal bool SetAfterCommit(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AfterCommit = (bool)argument.Value;
            return true;
        }

        public string AddNewBucketFuncType()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int,");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                var inputData = InputDatas[i];
                tempSb.Append($"int[], string,");
            }
            tempSb.Append("Task>");

            return tempSb.ToString();
        }

        public string BucketsCountFuncType()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                var inputData = InputDatas[i];
                tempSb.Append($"int[], string,");
            }
            tempSb.Append("Task<int>>");

            return tempSb.ToString();
        }
    }
}
