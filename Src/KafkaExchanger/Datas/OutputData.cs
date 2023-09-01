using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;

namespace KafkaExchanger.Datas
{
    internal class OutputData : BaseTopicData
    {
        public string KeyTypeAlias => KeyType.IsProtobuffType() ? "Proto" : KeyType.GetTypeAliasName();

        public string ValueTypeAlias => ValueType.IsProtobuffType() ? "Proto" : ValueType.GetTypeAliasName();

        public string PoolInterfaceName => $"IProducerPool{KeyTypeAlias}{ValueTypeAlias}";

        public string FullPoolInterfaceName => $"KafkaExchanger.Common.{PoolInterfaceName}";

        public static OutputData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new OutputData();

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

        public override void SetName(int index)
        {
            if (_hold)
            {
                return;
            }

            _id = index;
            _namePascalCase = $"Output{index}";
            _nameCamelCase = $"{char.ToLowerInvariant(_namePascalCase[0])}{_namePascalCase.Substring(1)}";
        }
    }
}