using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace KafkaExchanger.Generators
{
    public class CommonGenarator
    {
        private readonly StringBuilder _builder = new StringBuilder();

        public void Generate(string assemblyName, SourceProductionContext context)
        {
            _builder.Clear();

            Start(assemblyName);
            BaseResponse();
            ResponseItem();
            Response();
            AsyncManualResetEvent();
            TryProduceResult(assemblyName);

            End();

            context.AddSource($"KafkaExchangerCommon.g.cs", _builder.ToString());
        }

        public void Start(string assemblyName)
        {
            _builder.Append($@"
using System.Threading.Tasks;
using System.Threading;
using System;

namespace {assemblyName}
{{
");
        }

        public void End()
        {
            _builder.Append($@"
}}
");
        }

        public void BaseResponse()
        {
            _builder.Append($@"
    public abstract class BaseResponse
    {{
        private BaseResponse()
        {{

        }}

        public BaseResponse(string topicName) 
        {{
            TopicName = topicName;
        }}

        public string TopicName {{ get; init; }}
    }}
");
        }

        public void ResponseItem()
        {
            _builder.Append($@"
    public class ResponseItem<T> : BaseResponse
    {{
        private ResponseItem(string topicName) : base(topicName)
        {{

        }}

        public ResponseItem(string topicName, T result) : base(topicName)
        {{
            Result = result;
        }}

        public T Result {{ get; init; }}
    }}
");
        }

        public void Response()
        {
            _builder.Append($@"
    public class Response : IDisposable
    {{

        public Response(
            KafkaExchanger.Attributes.Enums.CurrentState currentState,
            BaseResponse[] response,
            TaskCompletionSource<bool> responseProcess
            )
        {{
            CurrentState = currentState;
            Result = response;
            _responseProcess = responseProcess;
        }}

        private TaskCompletionSource<bool> _responseProcess;

        public KafkaExchanger.Attributes.Enums.CurrentState CurrentState {{ get; private set; }}

        public BaseResponse[] Result {{ get; private set; }}

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
            Result = null;
        }}

        ~Response()
        {{
            Dispose(false);
        }}
    }}
");
        }

        public void TryProduceResult(string assemblyName)
        {
            _builder.Append($@"
    public class TryProduceResult
    {{
        public bool Succsess;
        public {assemblyName}.Response Response;
    }}
");
        }

        public void AsyncManualResetEvent()
        {
            _builder.Append($@"
    public class AsyncManualResetEvent
    {{
        private volatile TaskCompletionSource<bool> m_tcs = new TaskCompletionSource<bool>();

        public Task WaitAsync() {{ return m_tcs.Task; }}

        public void Set() {{ m_tcs.TrySetResult(true); }}

        public void Reset()
        {{
            while (true)
            {{
                var tcs = m_tcs;
                if (!tcs.Task.IsCompleted || Interlocked.CompareExchange(ref m_tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
                    return;
            }}
        }}
    }}
");
        }
    }
}