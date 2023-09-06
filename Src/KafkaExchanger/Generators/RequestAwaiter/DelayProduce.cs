using KafkaExchanger.Datas;
using System.Text;

namespace KafkaExchanger.Generators.RequestAwaiter
{
    internal static class DelayProduce
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            StartClass(builder, requestAwaiter);

            Constructors(builder, requestAwaiter);
            PropertiesAndFields(builder, assemblyName, requestAwaiter);
            Produce(builder, requestAwaiter);
            Dispose(builder, assemblyName);

            EndClass(builder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"{requestAwaiter.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "DelayProduce";
        }

        public static string Bucket()
        {
            return "Bucket";
        }

        public static string Partitions(InputData inputData)
        {
            return $"{inputData.NamePascalCase}Partitions";
        }

        public static string Header(OutputData outputData)
        {
            return $"{outputData.NamePascalCase}Header";
        }

        public static string Message(OutputData outputData)
        {
            return $"{outputData.NamePascalCase}Message";
        }

        private static void StartClass(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
        public class {TypeName()} : IDisposable
        {{");
        }

        private static void Constructors(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {TypeName()}(){{}}
");

            var topicResponseParam = "topicResponse";
            var outputRequestParam = "outputRequest";
            builder.Append($@"
            public {TypeName()}(
                {TopicResponse.TypeFullName(requestAwaiter)} {topicResponseParam},
                {OutputMessage.TypeFullName(requestAwaiter)} {outputRequestParam}
                )
            {{
                {_topicResponse()} = {topicResponseParam};
                {OutputRequest()} = {outputRequestParam};
            }}");
        }

        private static void EndClass(
            StringBuilder builder
            )
        {
            builder.Append($@"
        }}
");
        }

        private static string OutputRequest()
        {
            return "OutputRequest";
        }

        private static string _topicResponse()
        {
            return "_topicResponse";
        }

        private static void PropertiesAndFields(
            StringBuilder builder,
            string assemblyName,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private {TopicResponse.TypeFullName(requestAwaiter)} {_topicResponse()};

            public {OutputMessage.TypeFullName(requestAwaiter)} {OutputRequest()};
            public int {Bucket()} => {_topicResponse()}.{TopicResponse.Bucket()};
            public string Guid => {_topicResponse()}.{TopicResponse.Guid()};");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            public int[] {Partitions(inputData)} => {_topicResponse()}.{TopicResponse.InputPartition(inputData)};");
            }
        }

        private static void Produce(
            StringBuilder builder,
            Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public async Task<{Response.TypeFullName(requestAwaiter)}> Produce()
            {{
                {_topicResponse()}.{TopicResponse.OutputTask()}.TrySetResult({OutputRequest()});
                return
                    await {_topicResponse()}.GetResponse();
            }}
");
        }

        private static void Dispose(
            StringBuilder builder,
            string assemblyName
            )
        {
            builder.Append($@"
            private bool _disposedValue;

            public void Dispose()
            {{
                Dispose(true);
                GC.SuppressFinalize(this);
            }}

            protected void Dispose(bool disposed)
            {{
                if (!_disposedValue)
                {{
                    {_topicResponse()}.{TopicResponse.OutputTask()}.TrySetCanceled();
                    {_topicResponse()} = null;
                    _disposedValue = true;
                }}
            }}

            ~{TypeName()}()
            {{
                Dispose(false);
            }}
");
        }
    }
}