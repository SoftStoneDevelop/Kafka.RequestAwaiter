using KafkaExchanger.Datas;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;

namespace KafkaExchanger.Datas
{
    internal abstract class Exchange
    {
        public INamedTypeSymbol TypeSymbol { get; protected set; }

        public List<InputData> InputDatas { get; } = new List<InputData>();

        public List<OutputData> OutputDatas { get; } = new List<OutputData>();

        public bool UseLogger { get; set; }

        protected bool SetUseLogger(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            UseLogger = (bool)argument.Value;
            return true;
        }
    }
}