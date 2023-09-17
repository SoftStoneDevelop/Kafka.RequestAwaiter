using KafkaExchanger.Datas;
using KafkaExchanger.Generators;
using KafkaExchanger.Helpers;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace KafkaExchanger
{
    internal class Processor
    {
        private readonly List<Responder> _responders = new List<Responder>();
        private readonly List<RequestAwaiter> _requestAwaiters = new List<RequestAwaiter>();

        List<InputData> _inputsTemp = new List<InputData>();
        List<OutputData> _outputsTemp = new List<OutputData>();

        public void ProcessAttributes(
            ClassDeclarationSyntax classDeclarationSyntax,
            Compilation compilation,
            CancellationToken cancellationToken
            )
        {
            var type = compilation.GetSemanticModel(classDeclarationSyntax.SyntaxTree).GetDeclaredSymbol(classDeclarationSyntax);
            CheckGeneralTypeRequirements(type);
            foreach (var attributeListSyntax in classDeclarationSyntax.AttributeLists)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var parentSymbol = attributeListSyntax.Parent.GetDeclaredSymbol(compilation);
                var parentAttributes = parentSymbol.GetAttributes();

                _inputsTemp.Clear();
                _outputsTemp.Clear();
                Responder responder = null;
                RequestAwaiter requestAwaiter = null;

                foreach (var attributeSyntax in attributeListSyntax.Attributes)
                {
                    cancellationToken.ThrowIfCancellationRequested();

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
                        requestAwaiter = RequestAwaiter.Create(type, attributeData);
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ResponderAttribute"))
                    {
                        responder = Responder.Create(type, attributeData);
                        continue;
                    }

                    if (attributeData.AttributeClass.IsAssignableFrom("KafkaExchanger.Attributes", "ListenerAttribute"))
                    {
                        //TODO
                        continue;
                    }
                }

                TryAddResponder(responder);
                TryAddRequestAwaiter(requestAwaiter);
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
            RequestAwaiter requestAwaiter
            )
        {
            if (requestAwaiter == null)
            {
                return;
            }

            SetDatas(requestAwaiter);

            _requestAwaiters.Add(requestAwaiter);
            _inputsTemp.Clear();
            _outputsTemp.Clear();
        }

        private void TryAddResponder(
            Responder responder
            )
        {
            if (responder == null)
            {
                return;
            }

            SetDatas(responder);

            _responders.Add(responder);
            _inputsTemp.Clear();
            _outputsTemp.Clear();
        }

        private void SetDatas(Exchange exchange)
        {
            for (var i = 0; i < _inputsTemp.Count; i++)
            {
                var data = _inputsTemp[i];
                data.SetName(i);
                data.Hold();
                exchange.InputDatas.Add(data);
            }

            for (var i = 0; i < _outputsTemp.Count; i++)
            {
                var data = _outputsTemp[i];
                data.SetName(i);
                data.Hold();
                exchange.OutputDatas.Add(data);
            }
        }

        public void Generate(
            string assemblyName,
            SourceProductionContext context,
            CancellationToken cancellationToken
            )
        {
            cancellationToken.ThrowIfCancellationRequested();

            var requestAwaiterGenerator = new KafkaExchanger.Generators.RequestAwaiter.Generator();
            foreach (var requestAwaiter in _requestAwaiters)
            {
                cancellationToken.ThrowIfCancellationRequested();
                requestAwaiterGenerator.Generate(assemblyName, requestAwaiter, context);
            }
            _requestAwaiters.Clear();

            var responderGenerator = new KafkaExchanger.Generators.Responder.Generator();
            foreach (var responder in _responders)
            {
                cancellationToken.ThrowIfCancellationRequested();
                responderGenerator.GenerateResponder(assemblyName, responder, context);
            }
            _responders.Clear();
        }
    }
}