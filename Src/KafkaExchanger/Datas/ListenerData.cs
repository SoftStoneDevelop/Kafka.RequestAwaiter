using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class Listener
    {
        public ListenerData Data { get; set; }
        public List<InputData> InputDatas { get; } = new List<InputData>();

        public bool IsEmpty()
        {
            return Data == null && InputDatas.Count == 0;
        }
    }

    internal class ListenerData : BaseServiceData
    {
        public static ListenerData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new ListenerData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 1)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetUseLogger(namedArguments[0], result))
            {
                throw new Exception("Fail create ListenerData data: UseLogger");
            }

            return result;
        }
    }
}