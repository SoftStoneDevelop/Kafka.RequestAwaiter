using KafkaExchanger.Enums;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class ConsumerData
    {
        public uint CommitAfter { get; private set; }
        public OrderMatters OrderMatters { get; private set; }

        public bool CheckCurrentState { get; private set; }

        public string GetCurrentStateFunc(List<IncomeData> incomeDatas)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<");
            for (int i = 0; i < incomeDatas.Count; i++)
            {
                tempSb.Append($"Income{i}Message,");
            }
            tempSb.Append(" Task<KafkaExchanger.Attributes.Enums.CurrentState>>");

            return tempSb.ToString();
        }

        public bool UseAfterCommit { get; private set; }

        public string AfterCommitFunc()
        {
            return "Func<int, HashSet<int>,Task>";
        }

        internal bool SetCommitAfter(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(UInt32)
                )
            {
                return false;
            }

            CommitAfter = (uint)argument.Value;
            return true;
        }

        internal bool SetCheckCurrentState(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CheckCurrentState = (bool)argument.Value;
            return true;
        }

        internal bool SetUseAfterCommit(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            UseAfterCommit = (bool)argument.Value;
            return true;
        }

        internal bool SetOrderMatters(TypedConstant argument)
        {
            if (argument.Kind != TypedConstantKind.Enum ||
                !(argument.Type is INamedTypeSymbol directionParam) ||
                !directionParam.IsAssignableFrom("KafkaExchanger.Attributes.Enums", "OrderMatters")
                )
            {
                return false;
            }

            OrderMatters = (OrderMatters)argument.Value;
            return true;
        }
    }
}