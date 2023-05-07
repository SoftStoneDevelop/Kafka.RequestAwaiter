using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Datas
{
    internal class ProducerPairComparer : IEqualityComparer<ProducerPair>
    {
        public static readonly ProducerPairComparer Default = new ProducerPairComparer();

        public bool Equals(ProducerPair x, ProducerPair y)
        {
            return SymbolEqualityComparer.Default.Equals(x.KeyType, y.KeyType) && SymbolEqualityComparer.Default.Equals(x.ValueType, y.ValueType);
        }

        public int GetHashCode(ProducerPair obj)
        {
            return SymbolEqualityComparer.Default.GetHashCode(obj.KeyType) + SymbolEqualityComparer.Default.GetHashCode(obj.ValueType);
        }
    }


    internal class ProducerPair
    {
        internal ProducerPair(ITypeSymbol keyType, ITypeSymbol valueType)
        {
            KeyType = keyType;
            ValueType = valueType;
        }

        public ITypeSymbol KeyType { get; private set; }

        public string KeyTypeAlias => KeyType.IsProtobuffType() ? "Proto" : KeyType.GetTypeAliasName();

        public string FullKeyTypeName => KeyType.IsProtobuffType() ? "byte[]" : KeyType.GetFullTypeName(true);

        public ITypeSymbol ValueType { get; private set; }

        public string ValueTypeAlias => KeyType.IsProtobuffType() ? "Proto" : KeyType.GetTypeAliasName();

        public string FullValueTypeName => KeyType.IsProtobuffType() ? "byte[]" : KeyType.GetFullTypeName(true);

        public string PoolInterfaceName => $"IProducerPool{KeyTypeAlias}{ValueTypeAlias}";

        public string FullPoolInterfaceName => $"KafkaExchanger.Common.{PoolInterfaceName}";

        public string TypesPair => $"{FullKeyTypeName}, {FullValueTypeName}";
    }
}