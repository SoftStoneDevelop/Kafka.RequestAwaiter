using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class ResponderData
    {
        public INamedTypeSymbol TypeSymbol { get; set; }

        public ITypeSymbol OutcomeKeyType { get; set; }

        public ITypeSymbol OutcomeValueType { get; set; }

        public ITypeSymbol IncomeKeyType { get; set; }

        public ITypeSymbol IncomeValueType { get; set; }

        public static ResponderData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new ResponderData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 4)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetOutcomeKeyType(namedArguments[0], result))
            {
                throw new Exception("Fail create ResponderData data: OutcomeKeyType");
            }

            if (!SetOutcomeValueType(namedArguments[1], result))
            {
                throw new Exception("Fail create ResponderData data: OutcomeValueType");
            }

            if (!SetIncomeKeyType(namedArguments[2], result))
            {
                throw new Exception("Fail create ResponderData data: IncomeKeyType");
            }

            if (!SetIncomeValueType(namedArguments[3], result))
            {
                throw new Exception("Fail create ResponderData data: IncomeValueType");
            }

            return result;
        }

        private static bool SetIncomeKeyType(TypedConstant argument, ResponderData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.IncomeKeyType = keyType;
            return true;
        }

        private static bool SetIncomeValueType(TypedConstant argument, ResponderData result)
        {
            if (!(argument.Value is INamedTypeSymbol valueType))
            {
                return false;
            }

            result.IncomeValueType = valueType;
            return true;
        }

        private static bool SetOutcomeKeyType(TypedConstant argument, ResponderData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.OutcomeKeyType = keyType;
            return true;
        }

        private static bool SetOutcomeValueType(TypedConstant argument, ResponderData result)
        {
            if (!(argument.Value is INamedTypeSymbol valueType))
            {
                return false;
            }

            result.OutcomeValueType = valueType;
            return true;
        }
    }
}
