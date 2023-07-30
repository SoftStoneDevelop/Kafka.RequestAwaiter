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

        public string[] AcceptedService { get; private set; }

        public bool AcceptFromAny => AcceptedService == null || AcceptedService.Length == 0;

        public static InputData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new InputData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 3)
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

            if (!SetAcceptedService(namedArguments[2], result))
            {
                throw new Exception("Fail create InputData: AcceptedService");
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

        internal static bool SetAcceptedService(TypedConstant argument, InputData result)
        {
            if (argument.Kind != TypedConstantKind.Array)
            {
                return false;
            }

            if (argument.IsNull)
            {
                result.AcceptedService = null;
                return true;
            }

            var values = argument.Values;
            var tempArr = new string[values.Length];
            for (int i = 0; i < tempArr.Length; i++)
            {
                var value = values[i];
                if (!(value.Type is INamedTypeSymbol useLogger) ||
                    useLogger.Name != nameof(String) || value.IsNull
                    )
                {
                    return false;
                }

                tempArr[i] = (string)value.Value;
            }

            result.AcceptedService = tempArr;
            return true;
        }
    }
}