using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class Responder
    {
        public ResponderData Data { get; set; }
        public List<IncomeData> IncomeDatas { get; } = new List<IncomeData>();

        public List<OutcomeData> OutcomeDatas { get; } = new List<OutcomeData>();

        public bool IsEmpty()
        {
            return Data == null && OutcomeDatas.Count == 0 && IncomeDatas.Count == 0;
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

            if (!result.ProducerData.SetCustomOutcomeHeader(namedArguments[5]))
            {
                throw new Exception("Fail create ResponderData data: CustomOutcomeHeader");
            }

            if (!result.ProducerData.SetCustomHeaders(namedArguments[6]))
            {
                throw new Exception("Fail create ResponderData data: CustomHeaders");
            }

            return result;
        }
    }
}