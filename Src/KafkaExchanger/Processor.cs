using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Generators;
using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace KafkaExchanger
{
    internal class Processor
    {
        private readonly List<RequestAwaiterData> _requestAwaiterDatas = new List<RequestAwaiterData>();
        private readonly List<ResponderData> _responderDatas = new List<ResponderData>();
        private readonly List<ListenerData> _listenerDatas = new List<ListenerData>();

        public void FillTypes(INamespaceOrTypeSymbol symbol)
        {
            var queue = new Queue<INamespaceOrTypeSymbol>();
            queue.Enqueue(symbol);

            //Naming conflict escapes: one class one generator
            var set = new HashSet<INamedTypeSymbol>(SymbolEqualityComparer.Default);

            while (queue.Count != 0)
            {
                var current = queue.Dequeue();
                if (current is INamedTypeSymbol type)
                {
                    if (!set.Add(type))
                    {
                        continue;
                    }

                    ProcessAttributes(type);
                }

                foreach (var child in current.GetMembers())
                {
                    if (child is INamespaceOrTypeSymbol symbolChild)
                    {
                        queue.Enqueue(symbolChild);
                    }
                }
            }
        }

        private void ProcessAttributes(
            INamedTypeSymbol type
            )
        {
            foreach (var attribute in type.GetAttributes())
            {
                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "RequestAwaiterAttribute"))
                {
                    _requestAwaiterDatas.Add(RequestAwaiterData.Create(type, attribute));
                    break;
                }

                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ResponderAttribute"))
                {
                    _responderDatas.Add(ResponderData.Create(type, attribute));
                    break;
                }

                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ListenerAttribute"))
                {
                    _listenerDatas.Add(ListenerData.Create(type, attribute));
                    break;
                }
            }
        }

        public void Generate(
            GeneratorExecutionContext context
            )
        {
            if (_requestAwaiterDatas.Any() || _responderDatas.Any() || _listenerDatas.Any())
            {
                var commonGenerator = new CommonGenarator();
                commonGenerator.Generate(context);

                var headerGenerator = new HeaderGenarator();
                headerGenerator.Generate(context);
            }

            var producerPoolsGenerator = new ProducerPoolsGenerator();
            producerPoolsGenerator.FillProducerTypes(_requestAwaiterDatas, _responderDatas);
            producerPoolsGenerator.GenerateProducerPools(context);

            var requestAwaiterGenerator = new RequestAwaiterGenerator();
            foreach (var requestAwaiterData in _requestAwaiterDatas)
            {
                requestAwaiterGenerator.GenerateRequestAwaiter(requestAwaiterData, context);
            }

            var responderGenerator = new ResponderGenerator();
            foreach (var responderData in _responderDatas)
            {
                responderGenerator.GenerateResponder(responderData, context);
            }

            var listenerGenerator = new ListenerGenerator();
            foreach (var listenerData in _listenerDatas)
            {
                listenerGenerator.GenerateListener(listenerData, context);
            }
        }
    }
}