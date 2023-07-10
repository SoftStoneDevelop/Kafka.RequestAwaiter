using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class IncomeData : BaseData
    {
        public ITypeSymbol KeyType { get; set; }

        public ITypeSymbol ValueType { get; set; }

        public static IncomeData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new IncomeData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 2)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetKeyType(namedArguments[0], result))
            {
                throw new Exception("Fail create IncomeData: KeyType");
            }

            if (!SetValueType(namedArguments[1], result))
            {
                throw new Exception("Fail create IncomeData: ValueType");
            }

            return result;
        }

        private static bool SetKeyType(TypedConstant argument, IncomeData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.KeyType = keyType;
            return true;
        }

        private static bool SetValueType(TypedConstant argument, IncomeData result)
        {
            if (!(argument.Value is INamedTypeSymbol valueType))
            {
                return false;
            }

            result.ValueType = valueType;
            return true;
        }
    }
}