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
        private readonly List<Listener> _listeners = new List<Listener>();
        private readonly List<Responder> _responders = new List<Responder>();
        private readonly List<GenerateData> _requestAwaiters = new List<GenerateData>();

        List<InputData> _inputsTemp = new List<InputData>();
        List<OutputData> _outputsTemp = new List<OutputData>();

        public void ProcessAttributes(
            ClassDeclarationSyntax classDeclarationSyntax,
            Compilation compilation
            )
        {
            var type = compilation.GetSemanticModel(classDeclarationSyntax.SyntaxTree).GetDeclaredSymbol(classDeclarationSyntax);
            CheckGeneralTypeRequirements(type);
            foreach (var attributeListSyntax in classDeclarationSyntax.AttributeLists)
            {
                var parentSymbol = attributeListSyntax.Parent.GetDeclaredSymbol(compilation);
                var parentAttributes = parentSymbol.GetAttributes();

                _inputsTemp.Clear();
                _outputsTemp.Clear();
                ListenerData listenerData = null;
                ResponderData responderData = null;
                RequestAwaiterData requestAwaiterData = null;

                foreach (var attributeSyntax in attributeListSyntax.Attributes)
                {
                    var attributeData = parentAttributes.First(f => f.ApplicationSyntaxReference.GetSyntax() == attributeSyntax);

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "InputAttribute"))
                    {
                        _inputsTemp.Add(InputData.Create(type, attributeData));
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "OutputAttribute"))
                    {
                        _outputsTemp.Add(OutputData.Create(type, attributeData));
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "RequestAwaiterAttribute"))
                    {
                        requestAwaiterData = RequestAwaiterData.Create(type, attributeData);
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ResponderAttribute"))
                    {
                        responderData = ResponderData.Create(type, attributeData);
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ListenerAttribute"))
                    {
                        listenerData = ListenerData.Create(type, attributeData);
                        continue;
                    }
                }

                TryAddListener(listenerData);
                TryAddResponder(responderData);
                TryAddRequestAwaiter(requestAwaiterData);
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

        private void TryAddRequestAwaiter(
            RequestAwaiterData requestAwaiterData
            )
        {
            if (requestAwaiterData == null)
            {
                return;
            }

            var newRA = new GenerateData();
            newRA.Data = requestAwaiterData;
            newRA.InputDatas.AddRange(_inputsTemp);
            newRA.OutputDatas.AddRange(_outputsTemp);

            _requestAwaiters.Add(newRA);
            _inputsTemp.Clear();
            _outputsTemp.Clear();
        }

        private void TryAddResponder(
            ResponderData responderData
            )
        {
            if (responderData == null)
            {
                return;
            }

            var newRes = new Responder();
            newRes.Data = responderData;
            newRes.InputDatas.AddRange(_inputsTemp);
            newRes.OutputDatas.AddRange(_outputsTemp);

            _responders.Add(newRes);
            _inputsTemp.Clear();
            _outputsTemp.Clear();
        }

        private void TryAddListener(
            ListenerData listenerData
            )
        {
            if (listenerData == null)
            {
                return;
            }

            var newLis = new Listener();
            newLis.Data = listenerData;
            newLis.InputDatas.AddRange(_inputsTemp);

            _listeners.Add(newLis);
            _inputsTemp.Clear();
        }

        public void Generate(
            string assemblyName,
            SourceProductionContext context
            )
        {
            var producerPoolsGenerator = new ProducerPoolsGenerator();
            producerPoolsGenerator.FillProducerTypes(_requestAwaiters, _responders);
            producerPoolsGenerator.GenerateProducerPools(context);

            var requestAwaiterGenerator = new KafkaExchanger.Generators.RequestAwaiter.Generator();
            foreach (var requestAwaiter in _requestAwaiters)
            {
                requestAwaiterGenerator.Generate(assemblyName, requestAwaiter, context);
            }
            _requestAwaiters.Clear();

            var responderGenerator = new ResponderGenerator();
            foreach (var responder in _responders)
            {
                responderGenerator.GenerateResponder(assemblyName, responder, context);
            }
            _responders.Clear();

            var listenerGenerator = new ListenerGenerator();
            foreach (var listener in _listeners)
            {
                listenerGenerator.GenerateListener(assemblyName, listener, context);
            }
            _listeners.Clear();
        }
    }
}