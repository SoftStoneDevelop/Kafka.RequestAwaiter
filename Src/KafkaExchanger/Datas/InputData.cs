using Microsoft.CodeAnalysis;
using System;

namespace KafkaExchanger.Datas
{
    internal class InputData : BaseTopicData
    {
        public string[] AcceptedService 
        { 
            get => _acceptedService;
            private set 
            {
                if (_hold)
                {
                    return;
                }

                _acceptedService = value; 
            }
        }
        private string[] _acceptedService;

        public bool AcceptFromAny => AcceptedService == null || AcceptedService.Length == 0;

        public static InputData Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new InputData();

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

        public override void SetName(int index)
        {
            if (_hold)
            {
                return;
            }

            _namePascalCase = $"Input{index}";
            _nameCamelCase = $"{char.ToLowerInvariant(_namePascalCase[0])}{_namePascalCase.Substring(1)}";
        }
    }
}