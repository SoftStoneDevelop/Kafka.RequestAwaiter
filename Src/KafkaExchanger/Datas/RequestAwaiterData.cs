using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class RequestAwaiter
    {
        public RequestAwaiterData Data { get; set; }
        public List<IncomeData> IncomeDatas { get; } = new List<IncomeData>();

        public List<OutcomeData> OutcomeDatas { get; } = new List<OutcomeData>();

        public bool IsEmpty()
        {
            return Data == null && OutcomeDatas.Count == 0 && IncomeDatas.Count == 0;
        }
    }

    internal class RequestAwaiterData : BaseServiceData
    {
        public ConsumerData ConsumerData { get; } = new ConsumerData();

        public ProducerData ProducerData { get; } = new ProducerData();

        public static RequestAwaiterData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new RequestAwaiterData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 3)
            {
                throw new Exception("Unknown attribute constructor");
            }
            
            if (!SetUseLogger(namedArguments[0], result))
            {
                throw new Exception("Fail create RequestAwaiter data: UseLogger");
            }

            if (!result.ProducerData.SetCustomOutcomeHeader(namedArguments[1]))
            {
                throw new Exception("Fail create ResponderData data: CustomOutcomeHeader");
            }

            if (!result.ProducerData.SetCustomHeaders(namedArguments[2]))
            {
                throw new Exception("Fail create ResponderData data: CustomHeaders");
            }

            return result;
        }
    }
}
