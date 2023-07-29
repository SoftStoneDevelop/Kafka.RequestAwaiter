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
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter, assemblyName);
            TCS(builder, requestAwaiter);
            Constructor(builder, requestAwaiter);
            CreateGetResponse(builder, assemblyName, requestAwaiter);
            GetResponse(builder, assemblyName);
            IsCompleted(builder, requestAwaiter);
            TrySetResponse(builder, requestAwaiter);
            TrySetException(builder, requestAwaiter);
            Dispose(builder);
            End(builder);
        }

        private static void StartClass(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter,
            string assemblyName
            )
        {
            builder.Append($@"
        public class TopicResponse : IDisposable
        {{
            private TaskCompletionSource<bool> _responseProcess = new();
            public Task<{assemblyName}.Response> _response;
            private CancellationTokenSource _cts;
");
        }

        private static void TCS(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
            private TaskCompletionSource<Income{i}Message> _responseTopic{i} = new();
");
            }
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            var consumerData = requestAwaiter.ConsumerData;
            var producerData = requestAwaiter.ProducerData;

            builder.Append($@"
            public TopicResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                string topic{i}Name,
");
            }

            builder.Append($@"
                {(consumerData.CheckCurrentState ? $"{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState," : "")}
                string guid,
                Action<string> removeAction,
                int waitResponseTimeout = 0
                )
            {{
                _response = CreateGetResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                    topic{i}Name
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
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                        _responseTopic{i}.TrySetCanceled();
");
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
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.ProducerData;

            builder.Append($@"
            private async Task<{assemblyName}.Response> CreateGetResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                string topic{i}Name
");
            }
            builder.Append($@"
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
                )
            {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                var topic{i} = await _responseTopic{i}.Task;
");
            }

            if (consumerData.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = await getCurrentState(
                    
");
                for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
                {
                    if(i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                    topic{i}
");
                }
                builder.Append($@"
                    );
");
            }
            else
            {
                builder.Append($@"
                var currentState = KafkaExchanger.Attributes.Enums.CurrentState.NewMessage;
");
            }

            builder.Append($@"
                var response = new {assemblyName}.Response(
                    currentState,
                    new {assemblyName}.BaseResponse[]
                    {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                        new ResponseItem<Income{i}Message>(topic{i}Name, topic{i})
");
                if (i != requestAwaiter.IncomeDatas.Count - 1)
                {
                    builder.Append(',');
                }
            }
            builder.Append($@"    
                    }},
                    _responseProcess
                    );
                
                return response;
            }}
");
        }

        private static void GetResponse(
            StringBuilder builder,
            string assemblyName
            )
        {
            builder.Append($@"
            public Task<{assemblyName}.Response> GetResponse()
            {{
                return _response;
            }}
");
        }

        private static void IsCompleted(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            TaskCompletionSource<bool> taskCompletionSource = new TaskCompletionSource<bool>();
            var ss = taskCompletionSource.Task.IsCompleted;
            builder.Append($@"
            public bool IsCompleted()
            {{
                return _responseProcess.Task.IsCompleted;
            }}
");
        }

        private static void TrySetResponse(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
            public bool TrySetResponse(int topicNumber, BaseIncomeMessage response)
            {{
                switch (topicNumber)
                {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                    case {i}:
                    {{
                        return _responseTopic{i}.TrySetResult((Income{i}Message)response);
                    }}
");
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
            KafkaExchanger.AttributeDatas.GenerateData requestAwaiter
            )
        {
            builder.Append($@"
            public bool TrySetException(int topicNumber, Exception exception)
            {{
                switch (topicNumber)
                {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                    case {i}:
                    {{
                        return _responseTopic{i}.TrySetException(exception);
                    }}
");
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
            StringBuilder builder
            )
        {
            builder.Append($@"
            public void Dispose()
            {{
                _cts?.Dispose();
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
