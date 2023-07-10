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

        public bool UseAfterCommit { get; private set; }

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

    internal class ProducerData
    {
        public bool AfterSendResponse { get; private set; }

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

    internal class BaseServiceData : BaseData
    {
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

    internal abstract class BaseData
    {
        public INamedTypeSymbol TypeSymbol { get; set; }
    }
}