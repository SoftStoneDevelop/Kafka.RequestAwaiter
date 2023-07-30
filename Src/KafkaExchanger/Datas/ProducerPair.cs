using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Datas
{
    internal class ProducerPairComparer : IEqualityComparer<OutputData>
    {
        public static readonly ProducerPairComparer Default = new ProducerPairComparer();

        public bool Equals(OutputData x, OutputData y)
        {
            return 
                (SymbolEqualityComparer.Default.Equals(x.KeyType, y.KeyType) || (x.KeyType.IsProtobuffType() && y.KeyType.IsProtobuffType())) 
                &&
                (SymbolEqualityComparer.Default.Equals(x.ValueType, y.ValueType) || (x.ValueType.IsProtobuffType() && y.ValueType.IsProtobuffType()))
                ;
        }

        public int GetHashCode(OutputData obj)
        {
            var keyHash = obj.KeyType.IsProtobuffType() ? 1 : SymbolEqualityComparer.Default.GetHashCode(obj.KeyType);
            var valueHash = obj.ValueType.IsProtobuffType() ? 1 : SymbolEqualityComparer.Default.GetHashCode(obj.ValueType);
            return keyHash + valueHash;
        }
    }
}