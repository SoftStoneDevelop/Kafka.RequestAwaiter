using KafkaExchanger.Enums;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class RequestAwaiter
    {
        public BaseServiceData Data { get; set; }

        public ConsumerData ConsumerData => Data.ConsumerData;

        public ProducerData ProducerData => Data.ProducerData;

        public List<IncomeData> IncomeDatas { get; } = new List<IncomeData>();

        public List<OutcomeData> OutcomeDatas { get; } = new List<OutcomeData>();

        public bool IsEmpty()
        {
            return Data == null && OutcomeDatas.Count == 0 && IncomeDatas.Count == 0;
        }
    }

    internal class RequestAwaiterData : BaseServiceData
    {
        public static RequestAwaiterData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new RequestAwaiterData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 6)
            {
                throw new Exception("Unknown attribute constructor");
            }
            
            if (!SetUseLogger(namedArguments[0], result))
            {
                throw new Exception("Fail create RequestAwaiter data: UseLogger");
            }

            if (!result.ConsumerData.SetCommitAfter(namedArguments[1]))
            {
                throw new Exception("Fail create RequestAwaiter data: CommitAfter");
            }

            if (!result.ConsumerData.SetCheckCurrentState(namedArguments[2]))
            {
                throw new Exception("Fail create RequestAwaiter data: SetCheckCurrentState");
            }

            if (!result.ConsumerData.SetUseAfterCommit(namedArguments[3]))
            {
                throw new Exception("Fail create RequestAwaiter data: UseAfterCommit");
            }

            if (!result.ProducerData.SetCustomOutcomeHeader(namedArguments[4]))
            {
                throw new Exception("Fail create RequestAwaiter data: CustomOutcomeHeader");
            }

            if (!result.ProducerData.SetCustomHeaders(namedArguments[5]))
            {
                throw new Exception("Fail create RequestAwaiter data: CustomHeaders");
            }

            return result;
        }
    }
}
