using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.AttributeDatas
{
    internal class OutputData : BaseData
    {
        public ITypeSymbol KeyType { get; set; }

        public ITypeSymbol ValueType { get; set; }

        public string KeyTypeAlias => KeyType.IsProtobuffType() ? "Proto" : KeyType.GetTypeAliasName();

        public string FullKeyTypeName => KeyType.IsProtobuffType() ? "byte[]" : KeyType.GetFullTypeName(true);

        public string ValueTypeAlias => ValueType.IsProtobuffType() ? "Proto" : ValueType.GetTypeAliasName();

        public string FullValueTypeName => ValueType.IsProtobuffType() ? "byte[]" : ValueType.GetFullTypeName(true);

        public string PoolInterfaceName => $"IProducerPool{KeyTypeAlias}{ValueTypeAlias}";

        public string FullPoolInterfaceName => $"KafkaExchanger.Common.{PoolInterfaceName}";

        public string TypesPair => $"{FullKeyTypeName}, {FullValueTypeName}";

        public static OutputData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new OutputData();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 2)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!SetKeyType(namedArguments[0], result))
            {
                throw new Exception("Fail create OutputData: KeyType");
            }

            if (!SetValueType(namedArguments[1], result))
            {
                throw new Exception("Fail create OutputData: ValueType");
            }

            return result;
        }

        private static bool SetKeyType(TypedConstant argument, OutputData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.KeyType = keyType;
            return true;
        }

        private static bool SetValueType(TypedConstant argument, OutputData result)
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