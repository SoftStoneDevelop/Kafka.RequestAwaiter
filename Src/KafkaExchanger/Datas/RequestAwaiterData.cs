using KafkaExchanger.Enums;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class GenerateData
    {
        public BaseServiceData Data { get; set; }

        public ConsumerData ConsumerData => Data.ConsumerData;

        public ProducerData ProducerData => Data.ProducerData;

        public List<InputData> InputDatas { get; } = new List<InputData>();

        public List<OutputData> OutputDatas { get; } = new List<OutputData>();

        public bool IsEmpty()
        {
            return Data == null && OutputDatas.Count == 0 && InputDatas.Count == 0;
        }
    }

    internal class RequestAwaiterData : BaseServiceData
    {
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

            if (!result.ProducerData.SetCustomOutputHeader(namedArguments[3]))
            {
                throw new Exception("Fail create RequestAwaiter data: CustomOutputHeader");
            }

            if (!result.ProducerData.SetCustomHeaders(namedArguments[4]))
            {
                throw new Exception("Fail create RequestAwaiter data: CustomHeaders");
            }

            return result;
        }
    }
}
