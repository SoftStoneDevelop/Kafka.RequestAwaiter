using KafkaExchanger.AttributeDatas;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class TopicResponse
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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

        private static void StartClass(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter,
            string assemblyName
            )
        {
            builder.Append($@"
        public class TopicResponse : IDisposable
        {{
            private TaskCompletionSource<bool> _responseProcess = new(TaskCreationOptions.RunContinuationsAsynchronously);
            public Task<{requestAwaiter.TypeSymbol.Name}.Response> _response;
            private CancellationTokenSource _cts;
            private string _guid;
            public string MessageGuid => _guid;
");
        }

        private static void TCS(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(inputData.AcceptFromAny)
                {
                    builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> _responseTopic{i} = new(TaskCreationOptions.RunContinuationsAsynchronously);
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> _responseTopic{i}{inputData.AcceptedService[j]} = new(TaskCreationOptions.RunContinuationsAsynchronously);
");
                    }
                }
            }
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.ConsumerData;
            builder.Append($@"
            public TopicResponse(
                int bucket,
                {(consumerData.CheckCurrentState ? $"{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState," : "")}
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
                _guid = guid;
                _response = CreateGetResponse(
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
                                {(consumerData.CheckCurrentState ? ",getCurrentState" : "")}
                                );

                _response.ContinueWith(task => 
                {{
                    if (task.IsFaulted)
                    {{
                        _responseProcess.TrySetException(task.Exception);
                        return;
                    }}

                    if (task.IsCanceled)
                    {{
                        _responseProcess.TrySetCanceled();
                        return;
                    }}
                }});

                _responseProcess.Task.ContinueWith(task => 
                {{
                    removeAction(guid);
                }});

                if (waitResponseTimeout != 0)
                {{
                    _cts = new CancellationTokenSource(waitResponseTimeout);
                    _cts.Token.Register(() =>
                    {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        _responseTopic{i}.TrySetCanceled();
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        _responseTopic{i}{inputData.AcceptedService[j]}.TrySetCanceled();
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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;

            builder.Append($@"
            private async Task<{requestAwaiter.TypeSymbol.Name}.Response> CreateGetResponse(
                int bucket
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {inputData.NameCamelCase}Partitions
");
            }
            builder.Append($@"
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.InputDatas)} getCurrentState" : "")}
                )
            {{
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                var topic{i} = await _responseTopic{i}.Task.ConfigureAwait(false);
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                var topic{i}{inputData.AcceptedService[j]} = await _responseTopic{i}{inputData.AcceptedService[j]}.Task.ConfigureAwait(false);
");
                    }
                }
            }

            if (consumerData.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = await getCurrentState(
                    
");
                for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
                {
                    var inputData = requestAwaiter.InputDatas[i];
                    if (i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                    {inputData.NameCamelCase}Partitions
");

                    if (inputData.AcceptFromAny)
                    {
                        builder.Append($@"
                    topic{i}
");
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
                    topic{i}{inputData.AcceptedService[j]}
");
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
                var currentState = KafkaExchanger.Attributes.Enums.RAState.Sended;
");
            }

            builder.Append($@"
                var response = new {requestAwaiter.TypeSymbol.Name}.Response(
                    bucket,
                    currentState,
                    _responseProcess
");
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                    {inputData.NameCamelCase}Partitions
");
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@",
                        topic{i}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@",
                        topic{i}{inputData.AcceptedService[j]}
");
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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public Task<{requestAwaiter.TypeSymbol.Name}.Response> GetResponse()
            {{
                return _response;
            }}
");
        }

        private static void TrySetResponse(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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
                    case ({i}, 0):
                    {{
                        return _responseTopic{i}.TrySetResult(({inputData.MessageTypeName})response);
                    }}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                    case ({i}, {j}):
                    {{
                        return _responseTopic{i}{inputData.AcceptedService[j]}.TrySetResult(({inputData.MessageTypeName})response);
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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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
                    case ({i}, 0):
                    {{
                        return _responseTopic{i}.TrySetException(exception);
                    }}
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                    case ({i}, {j}):
                    {{
                        return _responseTopic{i}{inputData.AcceptedService[j]}.TrySetException(exception);
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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Dispose()
            {{
                _cts?.Cancel();
                _cts?.Dispose();
"); 
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        _responseTopic{i}.TrySetCanceled();
");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        _responseTopic{i}{inputData.AcceptedService[j]}.TrySetCanceled();
");
                    }
                }
            }
            builder.Append($@"
                try
                {{
                    _response.Wait();
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
