using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class BaseServiceData
    {
        public INamedTypeSymbol TypeSymbol { get; set; }

        public bool UseLogger { get; set; }

        protected static bool SetUseLogger(TypedConstant argument, BaseServiceData result)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            result.UseLogger = (bool)argument.Value;
            return true;
        }
    }
}