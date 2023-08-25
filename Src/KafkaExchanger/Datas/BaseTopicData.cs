using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;

namespace KafkaExchanger.Datas
{
    internal abstract class BaseTopicData
    {
        public ITypeSymbol KeyType
        {
            get => _keyType;
            set
            {
                if (_hold)
                {
                    return;
                }

                _keyType = value;
            }
        }
        private ITypeSymbol _keyType;

        protected static bool SetKeyType(TypedConstant argument, BaseTopicData result)
        {
            if (!(argument.Value is INamedTypeSymbol keyType))
            {
                return false;
            }

            result.KeyType = keyType;
            return true;
        }

        public string FullKeyTypeName => KeyType.IsProtobuffType() ? "byte[]" : KeyType.GetFullTypeName(true);

        public ITypeSymbol ValueType
        {
            get => _valueType;
            set
            {
                if (_hold)
                {
                    return;
                }

                _valueType = value;
            }
        }
        private ITypeSymbol _valueType;

        protected static bool SetValueType(TypedConstant argument, BaseTopicData result)
        {
            if (!(argument.Value is INamedTypeSymbol valueType))
            {
                return false;
            }

            result.ValueType = valueType;
            return true;
        }

        public string FullValueTypeName => ValueType.IsProtobuffType() ? "byte[]" : ValueType.GetFullTypeName(true);

        public string TypesPair => $"{FullKeyTypeName}, {FullValueTypeName}";

        protected bool _hold = false;

        public void Hold()
        {
            _hold = true;
        }

        public string NameCamelCase
        {
            get => _nameCamelCase;
        }
        protected string _nameCamelCase;

        public string NamePascalCase
        {
            get => _namePascalCase;
        }
        protected string _namePascalCase;

        public abstract void SetName(int index);

        public string MessageTypeName => $"{NamePascalCase}Message";

        public string MessageTypeNameCamel => $"{char.ToLowerInvariant(MessageTypeName[0])}{MessageTypeName.Substring(1)}";
    }
}