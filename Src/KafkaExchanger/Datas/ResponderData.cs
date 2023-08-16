using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class Responder
    {
        public ResponderData Data { get; set; }
        public List<InputData> InputDatas { get; } = new List<InputData>();

        public List<OutputData> OutputDatas { get; } = new List<OutputData>();

        public bool IsEmpty()
        {
            return Data == null && OutputDatas.Count == 0 && InputDatas.Count == 0;
        }
    }

    internal class ResponderData : BaseServiceData
    {
        public bool AfterSend { get; private set; }

        public string AfterSendFunc(
            string assemblyName,
            List<InputData> inputDatas,
            INamedTypeSymbol typeSymbol
            )
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int,");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                tempSb.Append($"Input{i}Message,");
            }
            tempSb.Append($"{typeSymbol.Name}.ResponseResult, Task>");

            return tempSb.ToString();
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

        public static ResponderData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new ResponderData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 5)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetUseLogger(namedArguments[0], result))
            {
                throw new Exception("Fail create ResponderData data: UseLogger");
            }

            if (!result.ConsumerData.SetCommitAfter(namedArguments[1]))
            {
                throw new Exception("Fail create ResponderData data: CommitAfter");
            }

            if (!result.ConsumerData.SetCheckCurrentState(namedArguments[2]))
            {
                throw new Exception("Fail create ResponderData data: SetCheckCurrentState");
            }

            if (!result.SetAfterSend(namedArguments[3]))
            {
                throw new Exception("Fail create ResponderData data: AfterSend");
            }

            if (!result.ConsumerData.SetUseAfterCommit(namedArguments[4]))
            {
                throw new Exception("Fail create ResponderData data: UseAfterCommit");
            }

            return result;
        }
    }
}