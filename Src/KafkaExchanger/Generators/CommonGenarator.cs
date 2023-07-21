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

            Response();

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

        public void Response()
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

    public class Response : IDisposable
    {{

        public Response(
            BaseResponse[] response,
            TaskCompletionSource<bool> responseProcess
            )
        {{
            Result = response;
            _responseProcess = responseProcess;
        }}

        private TaskCompletionSource<bool> _responseProcess;

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
    }
}