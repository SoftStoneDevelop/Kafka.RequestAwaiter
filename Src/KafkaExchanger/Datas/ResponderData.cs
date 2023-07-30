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
        public static ResponderData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new ResponderData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 7)
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

            if (!result.ProducerData.SetAfterSendResponse(namedArguments[3]))
            {
                throw new Exception("Fail create ResponderData data: AfterSendResponse");
            }

            if (!result.ConsumerData.SetUseAfterCommit(namedArguments[4]))
            {
                throw new Exception("Fail create ResponderData data: UseAfterCommit");
            }

            if (!result.ProducerData.SetCustomOutputHeader(namedArguments[5]))
            {
                throw new Exception("Fail create ResponderData data: CustomOutputHeader");
            }

            if (!result.ProducerData.SetCustomHeaders(namedArguments[6]))
            {
                throw new Exception("Fail create ResponderData data: CustomHeaders");
            }

            return result;
        }
    }
}