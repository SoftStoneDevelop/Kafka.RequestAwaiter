using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class ProducerData
    {
        public bool AfterSendResponse { get; private set; }

        public string AfterSendResponseFunc(
            List<IncomeData> incomeDatas,
            List<OutcomeData> outcomeDatas
            )
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<");
            for (int i = 0; i < incomeDatas.Count; i++)
            {
                tempSb.Append($"Income{i}Message,");
            }
            tempSb.Append("KafkaExchanger.Attributes.Enums.CurrentState,");
            for (int i = 0; i < outcomeDatas.Count; i++)
            {
                tempSb.Append($"Outcome{i}Message,");
            }

            tempSb.Append(" Task>");

            return tempSb.ToString();
        }

        internal bool SetAfterSendResponse(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AfterSendResponse = (bool)argument.Value;
            return true;
        }

        public bool CustomOutcomeHeader { get; private set; }

        public string CustomOutcomeHeaderFunc(
            string assemblyName
            )
        {
            return $"Func<Task<{assemblyName}.RequestHeader>>";
        }

        internal bool SetCustomOutcomeHeader(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CustomOutcomeHeader = (bool)argument.Value;
            return true;
        }

        public bool CustomHeaders { get; private set; }

        public string CustomHeadersFunc()
        {
            return $"Func<Headers, Task>";
        }

        internal bool SetCustomHeaders(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CustomHeaders = (bool)argument.Value;
            return true;
        }
    }
}
