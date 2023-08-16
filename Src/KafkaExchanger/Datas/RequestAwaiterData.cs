using KafkaExchanger.Enums;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class RequestAwaiter
    {
        public RequestAwaiterData Data { get; set; }

        public ConsumerData ConsumerData => Data.ConsumerData;

        public List<InputData> InputDatas { get; } = new List<InputData>();

        public List<OutputData> OutputDatas { get; } = new List<OutputData>();

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
            OutputData outputData
            )
        {
            return $"Func<{assemblyName}.RequestHeader, Confluent.Kafka.Message<{outputData.TypesPair}>, Task>";
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

        public static RequestAwaiterData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new RequestAwaiterData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 4)
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

            return result;
        }
    }
}
