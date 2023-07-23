using KafkaExchanger.AttributeDatas;
using System;
using System.Collections.Generic;
using System.Text;

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
            StartClass(builder, assemblyName);
            TCS(builder, requestAwaiter);
            Constructor(builder, requestAwaiter);

            CreateGetResponse(builder, assemblyName, requestAwaiter);
            GetProcessStatus(builder);
            GetResponse(builder, assemblyName);
            TrySetResponse(builder, requestAwaiter);
            SetException(builder, requestAwaiter);
            Dispose(builder);
            End(builder);
        }

        private static void StartClass(
            StringBuilder builder,
            string assemblyName
            )
        {
            builder.Append($@"
        public class TopicResponse : IDisposable
        {{
            private TaskCompletionSource<bool> _responseProcess = new();
            private CancellationTokenSource _cts;

            public Task<{assemblyName}.Response> _response;
");
        }

        private static void TCS(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
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
                string guid,
                Action<string> removeAction,
                int waitResponseTimeout = 0
                )
            {{
                _response = CreateGetResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                topic{i}Name");
                if (i != requestAwaiter.IncomeDatas.Count - 1)
                {
                    builder.Append(',');
                }
                else
                {
                    builder.Append(");");
                }
            }
            builder.Append($@"
                if (waitResponseTimeout != 0)
                {{
                    _cts = new CancellationTokenSource(waitResponseTimeout);
                    _cts.Token.Register(() =>
                    {{
                        var canceled =
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                if(i != 0)
                {
                    builder.Append(" | ");
                }
                builder.Append($@"
                            _responseTopic{i}.TrySetCanceled()
");
            }
            builder.Append($@"
                            ;
                        if (canceled)
                        {{
                            removeAction(guid);
                        }}
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
            builder.Append($@"
            private async Task<Response> CreateGetResponse(
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                string topic{i}Name
");
                if (i != requestAwaiter.IncomeDatas.Count - 1)
                {
                    builder.Append(',');
                }
                else
                {
                    builder.Append($@"
                )
");
                }
            }
            builder.Append($@"
            {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                var topic{i} = await _responseTopic{i}.Task;
");
            }
            builder.Append($@"
                var response = new {assemblyName}.Response(
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

        private static void GetProcessStatus(
            StringBuilder builder
            )
        {
            builder.Append($@"
            public Task<bool> GetProcessStatus()
            {{
                return _responseProcess.Task;
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

        private static void TrySetResponse(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public bool TrySetResponse(int topicNumber, BaseResponseMessage response)
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

        private static void SetException(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void SetException(int topicNumber, Exception exception)
            {{
                switch (topicNumber)
                {{
");
            for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
            {
                builder.Append($@"
                    case {i}:
                    {{
                        _responseTopic{i}.SetException(exception);
                        break;
                    }}
");
            }

            builder.Append($@"
                    default:
                    {{
                        break;
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
