using KafkaExchanger.Generators.Responder;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Datas
{
    internal class Responder : Exchange
    {
        public static Responder Create(INamedTypeSymbol type, AttributeData attribute)
        {
            var result = new Responder();
            result.TypeSymbol = type;

            var namedArguments = attribute.ConstructorArguments;
            if (namedArguments.Length != 5)
            {
                throw new Exception("Unknown attribute constructor");
            }

            if (!result.SetUseLogger(namedArguments[0]))
            {
                throw new Exception($"Fail create {nameof(Responder)}");
            }

            if (!result.SetCheckCurrentState(namedArguments[2]))
            {
                throw new Exception($"Fail create {nameof(Responder)}");
            }

            if (!result.SetAfterSend(namedArguments[3]))
            {
                throw new Exception($"Fail create {nameof(Responder)}");
            }

            if (!result.SetAfterCommit(namedArguments[4]))
            {
                throw new Exception($"Fail create {nameof(Responder)}");
            }

            return result;
        }

        public bool CheckCurrentState { get; private set; }

        public string GetCurrentStateFunc()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int,");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                tempSb.Append($" int[], Input{i}Message,");
            }
            tempSb.Append(" Task<KafkaExchanger.Attributes.Enums.RAState>>");

            return tempSb.ToString();
        }

        internal bool SetCheckCurrentState(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            CheckCurrentState = (bool)argument.Value;
            return true;
        }

        public bool AfterSend { get; private set; }

        public string AfterSendFunc()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int,");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                tempSb.Append($"Input{i}Message,");
            }
            tempSb.Append($"{TypeSymbol.Name}.ResponseResult, Task>");

            return tempSb.ToString();
        }

        internal bool SetAfterSend(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AfterSend = (bool)argument.Value;
            return true;
        }

        public bool AfterCommit { get; private set; }

        public string AfterCommitFunc()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<int, ");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                tempSb.Append($" HashSet<Confluent.Kafka.Partition>,");
            }
            tempSb.Append(" Task>");

            return tempSb.ToString();
        }

        internal bool SetAfterCommit(TypedConstant argument)
        {
            if (!(argument.Type is INamedTypeSymbol useLogger) ||
                useLogger.Name != nameof(Boolean)
                )
            {
                return false;
            }

            AfterCommit = (bool)argument.Value;
            return true;
        }

        public string CreateAnswerFuncType()
        {
            return $"Func<{InputMessage.TypeFullName(this)}, KafkaExchanger.Attributes.Enums.CurrentState, Task<{OutputMessage.TypeFullName(this)}>>";
        }

        public string LoadCurrentHorizonFuncType()
        {
            var tempSb = new StringBuilder(100);
            tempSb.Append("Func<");
            for (int i = 0; i < InputDatas.Count; i++)
            {
                tempSb.Append($"int[], ");
            }
            tempSb.Append("ValueTask<long>>");

            return tempSb.ToString();
        }
    }
}