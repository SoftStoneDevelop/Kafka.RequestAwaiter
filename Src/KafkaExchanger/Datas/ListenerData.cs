using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class ListenerData : BaseServiceData
    {

        public ITypeSymbol IncomeKeyType { get; set; }

        public ITypeSymbol IncomeValueType { get; set; }

        public static ListenerData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new ListenerData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 3)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetIncomeKeyType(namedArguments[0], result))
            {
                throw new Exception("Fail create ListenerData data: IncomeKeyType");
            }

            if (!SetIncomeValueType(namedArguments[1], result))
            {
                throw new Exception("Fail create ListenerData data: IncomeValueType");
            }

            if (!SetUseLogger(namedArguments[2], result))
            {
                throw new Exception("Fail create ListenerData data: UseLogger");
            }

            return result;
        }

        private static bool SetIncomeKeyType(TypedConstant argument, ListenerData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.IncomeKeyType = keyType;
            return true;
        }

        private static bool SetIncomeValueType(TypedConstant argument, ListenerData result)
        {
            if (!(argument.Value is INamedTypeSymbol valueType))
            {
                return false;
            }

            result.IncomeValueType = valueType;
            return true;
        }
    }
}