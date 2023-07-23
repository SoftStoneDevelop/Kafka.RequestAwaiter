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
            StartClass(builder, requestAwaiter, assemblyName);
            TCS(builder, requestAwaiter);
            Constructor(builder, requestAwaiter);

            CreateGetResponse(builder, assemblyName, requestAwaiter);

            if(requestAwaiter.Data is RequestAwaiterData)
            {
                GetProcessStatus(builder);
                GetResponse(builder, assemblyName);
            }
            
            TrySetResponse(builder, requestAwaiter);
            TrySetException(builder, requestAwaiter);
            Dispose(builder);
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
");
            if(requestAwaiter.Data is RequestAwaiterData)
            {
                builder.Append($@"
            private TaskCompletionSource<bool> _responseProcess = new();
            private CancellationTokenSource _cts;
            public Task<{assemblyName}.Response> _response;
");
            }
            else
            {
                builder.Append($@"
            public Task _response;
");
            }
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
                {(requestAwaiter.Data is ResponderData ? $"{consumerData.CreateResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} createResponse," : "")}
                {(requestAwaiter.Data is ResponderData ? $"{producerData.SendResponseFunc(requestAwaiter.Data.TypeSymbol)} sendResponse," : "")}
                {(requestAwaiter.Data is ResponderData ? $"{producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} afterSendResponse," : "")}
                {(consumerData.CheckCurrentState ? $"{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState," : "")}
");

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
                if (i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                    topic{i}Name
");
            }

            builder.Append($@"
                {(requestAwaiter.Data is ResponderData ? $",createResponse" : "")}
                {(requestAwaiter.Data is ResponderData ? $",sendResponse" : "")}
                {(requestAwaiter.Data is ResponderData ? $",afterSendResponse" : "")}
                {(consumerData.CheckCurrentState ? ",getCurrentState" : "")}
");

            builder.Append($@"
                    );
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
            var consumerData = requestAwaiter.Data.ConsumerData;
            var producerData = requestAwaiter.ProducerData;

            builder.Append($@"
            private async Task<Response> CreateGetResponse(
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
                {(requestAwaiter.Data is ResponderData ? $",{consumerData.CreateResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} createResponse" : "")}
                {(requestAwaiter.Data is ResponderData ? $",{producerData.SendResponseFunc(requestAwaiter.Data.TypeSymbol)} sendResponse" : "")}
                {(requestAwaiter.Data is ResponderData ? $",{producerData.AfterSendResponseFunc(requestAwaiter.IncomeDatas, requestAwaiter.Data.TypeSymbol)} afterSendResponse" : "")}
                {(consumerData.CheckCurrentState ? $",{consumerData.GetCurrentStateFunc(requestAwaiter.IncomeDatas)} getCurrentState" : "")}
");

            builder.Append($@"
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

            if(requestAwaiter.Data is RequestAwaiterData)
            {
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
            else
            {
                builder.Append($@"
                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                {{
                    var answer = await createResponse(
                        currentState,
");
                for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
                {
                    if (i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                        topic{i}
");
                }
                builder.Append($@"
                        );
                    await sendResponse(answer);
                    await afterSendResponse(
");
                for (int i = 0; i < requestAwaiter.IncomeDatas.Count; i++)
                {
                    if (i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                        topic{i}
");
                }
                builder.Append($@",
                        answer
                }}
            }}
");
            }
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

        private static void TrySetException(
            StringBuilder builder,
            KafkaExchanger.AttributeDatas.RequestAwaiter requestAwaiter
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
