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

        public string GetCurrentStateFunc(List<InputData> inputDatas)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                tempSb.Append($"Input{i}Message,");
            }
            tempSb.Append(" Task<KafkaExchanger.Attributes.Enums.RAState>>");

            return tempSb.ToString();
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

        public bool UseAfterCommit { get; private set; }

        public string AfterCommitFunc(List<InputData> inputDatas)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int, ");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                tempSb.Append($" HashSet<Confluent.Kafka.Partition>,");
            }
            tempSb.Append(" Task>");

            return tempSb.ToString();
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

        public string CreateResponseFunc(List<InputData> inputDatas, INamedTypeSymbol typeSymbol)
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<KafkaExchanger.Attributes.Enums.CurrentState,");
            for (int i = 0; i < inputDatas.Count; i++)
            {
                tempSb.Append($" Input{i}Message,");
            }

            tempSb.Append($" Task<{typeSymbol.Name}.ResponseResult>>");

            return tempSb.ToString();
        }
    }
}