using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators
{
    public class CommonGenarator
    {
        private readonly StringBuilder _builder = new StringBuilder();

        public void Generate(IncrementalGeneratorPostInitializationContext context)
        {
            _builder.Clear();

            Start();

            ResponseT();
            TopicResponseT();

            End();

            context.AddSource($"KafkaExchangerCommon.g.cs", _builder.ToString());
        }

        public void Start()
        {
            _builder.Append($@"
using System.Threading.Tasks;
using System.Threading;
using System;

namespace KafkaExchanger.Common
{{
");
        }

        public void End()
        {
            _builder.Append($@"
}}
");
        }

        public void ResponseT()
        {
            _builder.Append($@"
    public class Response<T>
    {{
        public Response(
            T result,
            TaskCompletionSource<bool> responseProcess
            )
        {{
            Result = result;
            _responseProcess = responseProcess;
        }}

        private TaskCompletionSource<bool> _responseProcess;

        public T Result {{ get; init; }}

        public void FinishProcessing()
        {{
            _responseProcess.SetResult(true);
#pragma warning disable CA1816 // Dispose methods should call SuppressFinalize
            GC.SuppressFinalize(this);
#pragma warning restore CA1816 // Dispose methods should call SuppressFinalize
        }}

        ~Response() 
        {{
            _responseProcess.SetResult(false);
        }}
    }}
");
        }

        public void TopicResponseT()
        {
            _builder.Append($@"
    public class TopicResponse<T> : IDisposable
    {{
        public TaskCompletionSource<Response<T>> _response = new();
        private TaskCompletionSource<bool> _responseProcess = new();
        private CancellationTokenSource _cts;

        public TopicResponse(string guid, Action<string> removeAction, int waitResponceTimeout = 0)
        {{
            if (waitResponceTimeout != 0)
            {{
                _cts = new CancellationTokenSource(waitResponceTimeout);
                _cts.Token.Register(() =>
                {{
                    if(_response.TrySetCanceled())
                    {{
                        removeAction(guid);
                    }}
                }},
                useSynchronizationContext: false
                );
            }}
        }}

        public Task<bool> GetProcessStatus()
        {{
            return _responseProcess.Task;
        }}

        public Task<Response<T>> GetResponce()
        {{
            return _response.Task;
        }}

        public bool TrySetResponce(T responce)
        {{
            return _response.TrySetResult(new Response<T>(responce, _responseProcess));
        }}

        public void SetException(Exception exception)
        {{
            _response.SetException(exception);
        }}

        public void Dispose()
        {{
            _cts?.Dispose();
        }}
    }}
");
        }
    }
}