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
        public int CommitAfter { get; private set; }
        public OrderMatters OrderMatters { get; private set; }

        public bool CheckDuplicate { get; private set; }

        public bool UseAfterCommit { get; private set; }

        internal bool SetCommitAfter(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Int32)
                )
            {
                return false;
            }

            CommitAfter = (int)argument.Value;
            return true;
        }

        internal bool SetCheckDuplicate(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CheckDuplicate = (bool)argument.Value;
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

    internal class ProducerData
    {
        public bool BeforeSendResponse { get; private set; }

        public bool AfterSendResponse { get; private set; }

        internal bool SetBeforeSendResponse(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            BeforeSendResponse = (bool)argument.Value;
            return true;
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
    }

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