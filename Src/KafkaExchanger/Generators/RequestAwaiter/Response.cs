using KafkaExchanger.Datas;
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

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "Response";
        }

        public static string CurrentState()
        {
            return "CurrentState";
        }

        public static string Bucket()
        {
            return "Bucket";
        }

        public static string Partitions(InputData inputData)
        {
            return $"{inputData.NamePascalCase}Partitions";
        }

        public static string _responseProcess()
        {
            return $"_responseProcess";
        }

        private static void Start(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()} : IDisposable
        {{");

        }

        private static void Constructor(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string message(InputData inputData, int serviceIndex = -1)
            {
                if(serviceIndex == -1)
                {
                    return $@"{inputData.NamePascalCase}";
                }
                else
                {
                    return $@"{inputData.NamePascalCase}{serviceIndex}";
                }
            }

            builder.Append($@"
            public {TypeName()}(
                int bucket,
                KafkaExchanger.Attributes.Enums.RAState currentState,
                TaskCompletionSource<bool> responseProcess
");
            string partitions(InputData inputData)
            {
                return $"{inputData.NameCamelCase}Partitions";
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {partitions(inputData)}");

                if (inputData.AcceptFromAny)
                {
                    builder.Append($@",
                {InputMessages.TypeFullName(requestAwaiter, inputData)} {message(inputData)}");

                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@",
                {InputMessages.TypeFullName(requestAwaiter, inputData)} {message(inputData, j)}");

                    }
                }
            }

            builder.Append($@"
                )
            {{
                {Bucket()} = bucket;
                {CurrentState()} = currentState;
                {_responseProcess()} = responseProcess;
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {Partitions(inputData)} = {partitions(inputData)};");

                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                {Message(inputData)} = {message(inputData)};");

                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                {Message(inputData, j)} = {message(inputData, j)};
");
                    }
                }
            }
            builder.Append($@"
            }}
");
        }

        public static string Message(InputData inputData, int serviceIndex = -1)
        {
            if (serviceIndex == -1)
            {
                return $@"{inputData.NamePascalCase}";
            }
            else
            {
                return $@"{inputData.NamePascalCase}{serviceIndex}";
            }
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private TaskCompletionSource<bool> {_responseProcess()};

            public int {Bucket()} {{ get; init; }}
            public KafkaExchanger.Attributes.Enums.RAState {CurrentState()} {{ get; init; }}");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                public int[] {Partitions(inputData)} {{ get; init; }}");

                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
            public {inputData.MessageTypeName} {Message(inputData)} {{ get; init; }}");

                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
            public {inputData.MessageTypeName} {Message(inputData, j)} {{ get; init; }}");

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
                    {_responseProcess()}.SetResult(disposing);
                }}
                else
                {{
                    try
                    {{
                        {_responseProcess()}.SetResult(disposing);
                    }}
                    catch
                    {{
                        //ignore
                    }}
                }}

                _disposed = true;
                {_responseProcess()} = null;
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