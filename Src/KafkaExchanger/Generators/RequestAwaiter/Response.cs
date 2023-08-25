using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class Response
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            Start(builder, assemblyName, requestAwaiter);

            Constructor(builder, assemblyName, requestAwaiter);
            PropertiesAndFields(builder, assemblyName, requestAwaiter);
            Dispose(builder, assemblyName, requestAwaiter);
            
            Finalizer(builder, assemblyName, requestAwaiter);
            End(builder, assemblyName, requestAwaiter);
        }

        private static void Start(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class Response : IDisposable
        {{
");
        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Response(
                int bucket,
                KafkaExchanger.Attributes.Enums.RAState currentState,
                TaskCompletionSource<bool> responseProcess
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {inputData.NameCamelCase}Partitions
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@",
                {inputData.MessageTypeName} {inputData.MessageTypeNameCamel}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@",
                {inputData.MessageTypeName} {inputData.MessageTypeNameCamel}{j}
");
                    }
                }
            }

            builder.Append($@"
                )
            {{
                Bucket = bucket;
                CurrentState = currentState;
                _responseProcess = responseProcess;
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {inputData.NamePascalCase}Partitions = {inputData.NameCamelCase}Partitions;
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                {inputData.MessageTypeName} = {inputData.MessageTypeNameCamel};
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                {inputData.MessageTypeName}{j} = {inputData.MessageTypeNameCamel}{j};
");
                    }
                }
            }
            builder.Append($@"
            }}
");
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private TaskCompletionSource<bool> _responseProcess;

            public int Bucket {{ get; init; }}

            public KafkaExchanger.Attributes.Enums.RAState CurrentState {{ get; init; }}
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                public int[] {inputData.NamePascalCase}Partitions {{ get; init; }}
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
            public {inputData.MessageTypeName} {inputData.MessageTypeName} {{ get; init; }}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
            public {inputData.MessageTypeName} {inputData.MessageTypeName}{j} {{ get; init; }}
");
                    }
                }
            }
        }

        private static void Dispose(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private bool _disposed;

            public void Dispose()
            {{
                Dispose(true);
                GC.SuppressFinalize(this);
            }}

            protected virtual void Dispose(bool disposing)
            {{  
                if (_disposed)
                {{
                    return;
                }}

                if(disposing)
                {{
                    _responseProcess.SetResult(disposing);
                }}
                else
                {{
                    try
                    {{
                        _responseProcess.SetResult(disposing);
                    }}
                    catch
                    {{
                        //ignore
                    }}
                }}

                _disposed = true;
                _responseProcess = null;
            }}
");
        }

        private static void Finalizer(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            ~Response()
            {{
                Dispose(false);
            }}
");
        }

        private static void End(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        }}
");
        }
    }
}