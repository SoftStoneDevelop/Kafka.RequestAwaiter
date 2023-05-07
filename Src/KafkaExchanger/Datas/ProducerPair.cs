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
            return 
                (SymbolEqualityComparer.Default.Equals(x.KeyType, y.KeyType) || (x.KeyType.IsProtobuffType() && y.KeyType.IsProtobuffType())) 
                &&
                (SymbolEqualityComparer.Default.Equals(x.ValueType, y.ValueType) || (x.ValueType.IsProtobuffType() && y.ValueType.IsProtobuffType()))
                ;
        }

        public int GetHashCode(ProducerPair obj)
        {
            var keyHash = obj.KeyType.IsProtobuffType() ? 1 : SymbolEqualityComparer.Default.GetHashCode(obj.KeyType);
            var valueHash = obj.ValueType.IsProtobuffType() ? 1 : SymbolEqualityComparer.Default.GetHashCode(obj.ValueType);
            return keyHash + valueHash;
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