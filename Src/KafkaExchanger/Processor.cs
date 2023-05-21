using KafkaExchanger.AttributeDatas;
using KafkaExchanger.Generators;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.Linq;

namespace KafkaExchanger
{
    internal class Processor
    {
        private readonly List<RequestAwaiterData> _requestAwaiterDatas = new List<RequestAwaiterData>();
        private readonly List<ResponderData> _responderDatas = new List<ResponderData>();
        private readonly List<ListenerData> _listenerDatas = new List<ListenerData>();

        public void TryFillFrom(INamedTypeSymbol type)
        {
            ProcessAttributes(type);
        }

        private void ProcessAttributes(
            INamedTypeSymbol type
            )
        {
            foreach (var attribute in type.GetAttributes())
            {
                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "RequestAwaiterAttribute"))
                {
                    CheckGeneralTypeRequirements(type);
                    _requestAwaiterDatas.Add(RequestAwaiterData.Create(type, attribute));
                    break;
                }

                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ResponderAttribute"))
                {
                    CheckGeneralTypeRequirements(type);
                    _responderDatas.Add(ResponderData.Create(type, attribute));
                    break;
                }

                if (attribute.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ListenerAttribute"))
                {
                    CheckGeneralTypeRequirements(type);
                    _listenerDatas.Add(ListenerData.Create(type, attribute));
                    break;
                }
            }
        }

        private void CheckGeneralTypeRequirements(INamedTypeSymbol type)
        {
            if(type.DeclaredAccessibility == Accessibility.Private)
            {
                throw new Exception($"Class '{type.Name}' can not be declared as private");
            }

            foreach (var item in type.DeclaringSyntaxReferences)
            {
                var syntax = item.GetSyntax();
                if (!(syntax is ClassDeclarationSyntax classDeclarationSyntax))
                {
                    continue;
                }

                if (classDeclarationSyntax.Modifiers.Any(m => m.IsKind(SyntaxKind.StaticKeyword)))
                {
                    throw new Exception($"Class '{type.Name}' can not be declared as static");
                }

                if (!classDeclarationSyntax.Modifiers.Any(m => m.IsKind(SyntaxKind.PartialKeyword)))
                {
                    throw new Exception($"Class '{type.Name}' must be declared as partial");
                }
            }
        }

        public void Generate(
            SourceProductionContext context
            )
        {
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