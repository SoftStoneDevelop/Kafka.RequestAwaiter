using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class InputData : BaseData
    {
        public ITypeSymbol KeyType { get; set; }

        public ITypeSymbol ValueType { get; set; }

        public string FullKeyTypeName => KeyType.IsProtobuffType() ? "byte[]" : KeyType.GetFullTypeName(true);

        public string FullValueTypeName => ValueType.IsProtobuffType() ? "byte[]" : ValueType.GetFullTypeName(true);

        public string TypesPair => $"{FullKeyTypeName}, {FullValueTypeName}";

        public static InputData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new InputData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 2)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetKeyType(namedArguments[0], result))
            {
                throw new Exception("Fail create InputData: KeyType");
            }

            if (!SetValueType(namedArguments[1], result))
            {
                throw new Exception("Fail create InputData: ValueType");
            }

            return result;
        }

        private static bool SetKeyType(TypedConstant argument, InputData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.KeyType = keyType;
            return true;
        }

        private static bool SetValueType(TypedConstant argument, InputData result)
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