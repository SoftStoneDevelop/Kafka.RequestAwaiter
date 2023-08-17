using KafkaExchanger.Enums;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class RequestAwaiter : Exchange
    {
        public RequestAwaiterData Data { get; set; }

        public override INamedTypeSymbol TypeSymbol => Data.TypeSymbol;

        public ConsumerData ConsumerData => Data.ConsumerData;

        public bool IsEmpty()
        {
            return Data == null && OutputDatas.Count == 0 && InputDatas.Count == 0;
        }
    }

    internal class RequestAwaiterData : BaseServiceData
    {
        public bool AfterSend { get; private set; }

        public string AfterSendFunc(
            string assemblyName,
            OutputData outputData,
            int outputIndex
            )
        {
            return $"Func<{assemblyName}.RequestHeader, Output{outputIndex}Message, Task>";
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

        public string AddAwaiterCheckStatusFunc(
            string assemblyName,
            List<InputData> inputDatas
            )
        {
            var builder = new StringBuilder(200);
            builder.Append($"Func<string, int,");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                builder.Append($@" int[],");
            }

            builder.Append($" Task<KafkaExchanger.Attributes.Enums.RAState>>");
            return builder.ToString();
        }

        public string LoadOutputMessageFunc(
            string assemblyName,
            OutputData outputData,
            int outputIndex,
            List<InputData> inputDatas
            )
        {
            var builder = new StringBuilder(200);
            builder.Append($"Func<string, int,");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                builder.Append($@" int[],");
            }

            builder.Append($" Task<Output{outputIndex}Message>>");
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

        public static RequestAwaiterData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new RequestAwaiterData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 5)
            {
                throw new Exception("Unknown attribute constructor");
            }
            
            if (!SetUseLogger(namedArguments[0], result))
            {
                throw new Exception("Fail create RequestAwaiter data: UseLogger");
            }

            if (!result.ConsumerData.SetCheckCurrentState(namedArguments[1]))
            {
                throw new Exception("Fail create RequestAwaiter data: SetCheckCurrentState");
            }

            if (!result.ConsumerData.SetUseAfterCommit(namedArguments[2]))
            {
                throw new Exception("Fail create RequestAwaiter data: UseAfterCommit");
            }

            if (!result.SetAfterSend(namedArguments[3]))
            {
                throw new Exception("Fail create RequestAwaiter data: AfterSend");
            }

            if (!result.SetAddAwaiterCheckStatus(namedArguments[4]))
            {
                throw new Exception("Fail create RequestAwaiter data: AddAwaiterCheckStatus");
            }

            return result;
        }
    }
}
