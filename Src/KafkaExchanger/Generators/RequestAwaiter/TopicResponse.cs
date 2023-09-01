using KafkaExchanger.Datas;
using System.Net.Sockets;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TopicResponse
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter, assemblyName);
            TCS(builder, requestAwaiter);
            Constructor(builder, requestAwaiter);
            CreateGetResponse(builder, assemblyName, requestAwaiter);
            GetResponse(builder, requestAwaiter);
            TrySetResponse(builder, requestAwaiter);
            TrySetException(builder, requestAwaiter);
            Dispose(builder, requestAwaiter);
            End(builder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "TopicResponse";
        }

        private static string _response()
        {
            return "_response";
        }

        private static string _responseProcess()
        {
            return "_responseProcess";
        }

        public static string MessageGuid()
        {
            return "MessageGuid";
        }

        private static string _guid()
        {
            return "_guid";
        }

        private static string _cts()
        {
            return "_cts";
        }

        public static string _responseTopic(InputData inputData, int serviceId = -1)
        {
            if(serviceId == -1)
            {
                return $"_response{inputData.NamePascalCase}";
            }
            else
            {
                return $"_response{inputData.NamePascalCase}{inputData.AcceptedService[serviceId]}";
            }
        }

        private static void StartClass(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter,
            string assemblyName
            )
        {
            builder.Append($@"
        public class {TypeName()} : IDisposable
        {{
            private TaskCompletionSource<bool> {_responseProcess()} = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task<{Response.TypeFullName(requestAwaiter)}> {_response()};
            private CancellationTokenSource {_cts()};
            private readonly string {_guid()};
            public string {MessageGuid()} => {_guid()};
");
        }

        private static void TCS(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(inputData.AcceptFromAny)
                {
                    builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> {_responseTopic(inputData)} = new(TaskCreationOptions.RunContinuationsAsynchronously);
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> {_responseTopic(inputData, j)} = new(TaskCreationOptions.RunContinuationsAsynchronously);
");
                    }
                }
            }
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public {TypeName()}(
                int bucket,
                {(requestAwaiter.CheckCurrentState ? $"{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState," : "")}
                string guid,
                Action<string> removeAction
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {inputData.NameCamelCase}Partitions
");
            }
            
            builder.Append($@",
                int waitResponseTimeout = 0
                )
            {{
                {_guid()} = guid;
                {_response()} = CreateGetResponse(
                                bucket
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                                {inputData.NameCamelCase}Partitions
");
            }
            builder.Append($@"
                                {(requestAwaiter.CheckCurrentState ? ",getCurrentState" : "")}
                                );

                {_response()}.ContinueWith(task => 
                {{
                    if (task.IsFaulted)
                    {{
                        {_responseProcess()}.TrySetException(task.Exception);
                        return;
                    }}

                    if (task.IsCanceled)
                    {{
                        {_responseProcess()}.TrySetCanceled();
                        return;
                    }}
                }});

                {_responseProcess()}.Task.ContinueWith(task => 
                {{
                    removeAction(guid);
                }});

                if (waitResponseTimeout != 0)
                {{
                    {_cts()} = new CancellationTokenSource(waitResponseTimeout);
                    {_cts()}.Token.Register(() =>
                    {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        {_responseTopic(inputData)}.TrySetCanceled();
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        {_responseTopic(inputData, j)}.TrySetCanceled();
");
                    }
                }
            }
            builder.Append($@"
                            ;
                    }},
                    useSynchronizationContext: false
                    );
                }}
            }}
");
        }

        private static void CreateGetResponse(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            var bucketParam = "bucket";
            builder.Append($@"
            private async Task<{requestAwaiter.TypeSymbol.Name}.Response> CreateGetResponse(
                int {bucketParam}");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {inputData.NameCamelCase}Partitions");
            }
            builder.Append($@"
                {(requestAwaiter.CheckCurrentState ? $",{requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState" : "")}
                )
            {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                var topic{i} = await {_responseTopic(inputData)}.Task.ConfigureAwait(false);");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                var topic{i}{inputData.AcceptedService[j]} = await {_responseTopic(inputData, j)}.Task.ConfigureAwait(false);");
                    }
                }
            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = await getCurrentState(
                    {bucketParam},");
                for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
                {
                    var inputData = requestAwaiter.InputDatas[i];
                    if (i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                    {inputData.NameCamelCase}Partitions,");

                    if (inputData.AcceptFromAny)
                    {
                        builder.Append($@"
                    topic{i}");
                    }
                    else
                    {
                        for (int j = 0; j < inputData.AcceptedService.Length; j++)
                        {
                            if(j != 0)
                            {
                                builder.Append(',');
                            }

                            builder.Append($@"
                    topic{i}{inputData.AcceptedService[j]}");
                        }
                    }
                }
                builder.Append($@"
                    );
");
            }
            else
            {
                builder.Append($@"
                var currentState = KafkaExchanger.Attributes.Enums.RAState.Sended;");
            }

            builder.Append($@"
                var response = new {requestAwaiter.TypeSymbol.Name}.Response(
                    {bucketParam},
                    currentState,
                    {_responseProcess()}");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                    {inputData.NameCamelCase}Partitions");

                if (inputData.AcceptFromAny)
                {
                    builder.Append($@",
                    topic{i}");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@",
                    topic{i}{inputData.AcceptedService[j]}");
                    }
                }
            }
            builder.Append($@"
                    );
                
                return response;
            }}
");
        }

        private static void GetResponse(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Task<{requestAwaiter.TypeSymbol.Name}.Response> GetResponse()
            {{
                return {_response()};
            }}
");
        }

        private static void TrySetResponse(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public bool TrySetResponse(int topicNumber, BaseInputMessage response, int serviceNumber = 0)
            {{
                switch (topicNumber, serviceNumber)
                {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(inputData.AcceptFromAny)
                {
                    builder.Append($@"
                    case ({inputData.Id}, 0):
                    {{
                        return {_responseTopic(inputData)}.TrySetResult(({inputData.MessageTypeName})response);
                    }}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                    case ({inputData.Id}, {j}):
                    {{
                        return {_responseTopic(inputData, j)}.TrySetResult(({inputData.MessageTypeName})response);
                    }}
");
                    }
                }
            }

            builder.Append($@"
                    default:
                    {{
                        return false;
                    }}
                }}
            }}
");
        }

        private static void TrySetException(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public bool TrySetException(int topicNumber, Exception exception, int serviceNumber = 0)
            {{
                switch (topicNumber, serviceNumber)
                {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                    case ({inputData.Id}, 0):
                    {{
                        return {_responseTopic(inputData)}.TrySetException(exception);
                    }}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                    case ({inputData.Id}, {j}):
                    {{
                        return {_responseTopic(inputData, j)}.TrySetException(exception);
                    }}
");
                    }
                }
            }

            builder.Append($@"
                    default:
                    {{
                        return false;
                    }}
                }}
            }}
");
        }

        private static void Dispose(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Dispose()
            {{
                {_cts()}?.Cancel();
                {_cts()}?.Dispose();
"); 
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        {_responseTopic(inputData)}.TrySetCanceled();
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        {_responseTopic(inputData, j)}.TrySetCanceled();
");
                    }
                }
            }
            builder.Append($@"
                try
                {{
                    {_response()}.Wait();
                }}
                catch{{ /* ignore */}}
            }}
");
        }

        private static void End(
            StringBuilder builder
            )
        {
            builder.Append($@"    
        }}
");
        }
    }
}
