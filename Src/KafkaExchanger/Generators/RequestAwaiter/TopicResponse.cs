using KafkaExchanger.Datas;
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
            FieldsAndProperties(builder, requestAwaiter);
            Constructor(builder, requestAwaiter);

            Init(builder, assemblyName, requestAwaiter);
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

        private static string _outputTask()
        {
            return "_outputTask";
        }

        public static string OutputTask()
        {
            return "OutputTask";
        }

        public static string Guid()
        {
            return "Guid";
        }

        private static string _guid()
        {
            return "_guid";
        }

        private static string _cts()
        {
            return "_cts";
        }

        private static string _waitResponseTimeout()
        {
            return "_waitResponseTimeout";
        }

        public static string Bucket()
        {
            return "Bucket";
        }

        private static string _removeAction()
        {
            return "_removeAction";
        }

        private static string _writer()
        {
            return "_writer";
        }

        public static string _inputMessageTask(InputData inputData, int serviceId = -1)
        {
            if(serviceId == -1)
            {
                return $"_{inputData.NameCamelCase}Task";
            }
            else
            {
                return $"_{inputData.NameCamelCase}{inputData.AcceptedService[serviceId]}Task";
            }
        }

        private static string _getCurrentState()
        {
            return "_getCurrentState";
        }

        private static string _inputPartition(InputData inputData)
        {
            return $@"_{inputData.NameCamelCase}Partitions";
        }

        public static string InputPartition(InputData inputData)
        {
            return $@"{inputData.NamePascalCase}Partitions";
        }

        private static string ChannelWriterType(KafkaExchanger.Datas.RequestAwaiter requestAwaiter)
        {
            return $"ChannelWriter<{ChannelInfo.TypeFullName(requestAwaiter)}>";
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
");
        }

        private static void FieldsAndProperties(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private TaskCompletionSource<{OutputMessage.TypeFullName(requestAwaiter)}> {_outputTask()} = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private TaskCompletionSource<bool> {_responseProcess()} = new(TaskCreationOptions.RunContinuationsAsynchronously);
            private Task<{Response.TypeFullName(requestAwaiter)}> {_response()};
            private CancellationTokenSource {_cts()};
            private readonly string {_guid()};
            private int {_waitResponseTimeout()};
            {ChannelWriterType(requestAwaiter)} {_writer()};

            private Action<string> {_removeAction()};

            public string {Guid()} => {_guid()};
            public TaskCompletionSource<{OutputMessage.TypeFullName(requestAwaiter)}> {OutputTask()} => {_outputTask()};
            public int {Bucket()};");

            if(requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
            private {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {_getCurrentState()};");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if(inputData.AcceptFromAny)
                {
                    builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> {_inputMessageTask(inputData)} = new(TaskCreationOptions.RunContinuationsAsynchronously);");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> {_inputMessageTask(inputData, j)} = new(TaskCreationOptions.RunContinuationsAsynchronously);");
                    }
                }
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
            private int[] {_inputPartition(inputData)};
            public int[] {InputPartition(inputData)} => {_inputPartition(inputData)};");
            }
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            string inputPartition(InputData inputData)
            {
                return $@"{inputData.NameCamelCase}Partitions";
            }

            var removeActionParam = "removeAction";
            var guidParam = "guid";

            builder.Append($@"
            public {TypeName()}(
                string {guidParam},
                Action<string> {removeActionParam},
                {ChannelWriterType(requestAwaiter)} writer");
            var getCurrentStateParam = "getCurrentState";
            if(requestAwaiter.CheckCurrentState)
            {
                builder.Append($@",
                {requestAwaiter.GetCurrentStateFunc(requestAwaiter.InputDatas)} {getCurrentStateParam}");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                int[] {inputPartition(inputData)}");
            }

            var waitResponseTimeoutParam = "waitResponseTimeout";
            builder.Append($@",
                int {waitResponseTimeoutParam} = 0
                )
            {{
                {_guid()} = {guidParam};
                {_removeAction()} = {removeActionParam};
                {_waitResponseTimeout()} = {waitResponseTimeoutParam};
                {_writer()} = writer;");

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                {_getCurrentState()} = {getCurrentStateParam};");
            }

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@"
                {_inputPartition(inputData)} = {inputPartition(inputData)};");
            }

            builder.Append($@"
            }}
");
        }

        private static void Init(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            public void Init()
            {{
                {_response()} = CreateGetResponse();
                if ({_waitResponseTimeout()} != 0)
                {{
                    {_cts()} = new CancellationTokenSource({_waitResponseTimeout()});
                    {_cts()}.Token.Register(() =>
                    {{");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        {_inputMessageTask(inputData)}.TrySetCanceled();");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        {_inputMessageTask(inputData, j)}.TrySetCanceled();");
                    }
                }
            }

            builder.Append($@"
                    }},
                    useSynchronizationContext: false
                    );
                }}

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

                {_responseProcess()}.Task.ContinueWith(async task =>
                {{
                    try
                    {{
                        var endResponse = new {EndResponse.TypeFullName(requestAwaiter)}
                        {{
                            {EndResponse.BucketId()} = this.{Bucket()},
                            {EndResponse.Guid()} = this.{Guid()}
                        }};

                        await {_writer()}.WriteAsync(endResponse).ConfigureAwait(false);
                    }}
                    catch
                    {{
                        //ignore
                    }}

                    {_removeAction()}({_guid()});
                }}
                );
            }}
");
        }

        private static void CreateGetResponse(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.RequestAwaiter requestAwaiter
            )
        {
            builder.Append($@"
            private async Task<{requestAwaiter.TypeSymbol.Name}.Response> CreateGetResponse()
            {{
");
            var offsetIndex = 0;
            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                var {inputData.NameCamelCase} = await {_inputMessageTask(inputData)}.Task.ConfigureAwait(false);
                await {_writer()}.WriteAsync(
                    new {SetOffsetResponse.TypeFullName(requestAwaiter)}
                    {{
                        {SetOffsetResponse.BucketId()} = this.{Bucket()},
                        {SetOffsetResponse.Guid()} = this.{Guid()},
                        {SetOffsetResponse.OffsetId()} = {offsetIndex++},
                        {SetOffsetResponse.Offset()} = {inputData.NameCamelCase}.{BaseInputMessage.TopicPartitionOffset()}
                    }}
                    ).ConfigureAwait(false);");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                var {inputData.NameCamelCase}{inputData.AcceptedService[j]} = await {_inputMessageTask(inputData, j)}.Task.ConfigureAwait(false);
                await {_writer()}.WriteAsync(
                    new {SetOffsetResponse.TypeFullName(requestAwaiter)}
                    {{
                        {SetOffsetResponse.BucketId()} = this.{Bucket()},
                        {SetOffsetResponse.Guid()} = this.{Guid()},
                        {SetOffsetResponse.OffsetId()} = {offsetIndex++},
                        {SetOffsetResponse.Offset()} = {inputData.NameCamelCase}{inputData.AcceptedService[j]}.{BaseInputMessage.TopicPartitionOffset()}
                    }}
                    ).ConfigureAwait(false);");
                    }
                }


            }

            if (requestAwaiter.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = await {_getCurrentState()}(
                    {Bucket()},");
                for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
                {
                    var inputData = requestAwaiter.InputDatas[i];
                    if (i != 0)
                    {
                        builder.Append(',');
                    }

                    builder.Append($@"
                    {_inputPartition(inputData)},");

                    if (inputData.AcceptFromAny)
                    {
                        builder.Append($@"
                    {inputData.NameCamelCase}");
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
                    {inputData.NameCamelCase}{inputData.AcceptedService[j]}");
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
                    {Bucket()},
                    currentState,
                    {_responseProcess()}");

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                builder.Append($@",
                    {_inputPartition(inputData)}");

                if (inputData.AcceptFromAny)
                {
                    builder.Append($@",
                    {inputData.NameCamelCase}");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@",
                    {inputData.NameCamelCase}{inputData.AcceptedService[j]}");
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
            public Task<{Response.TypeFullName(requestAwaiter)}> GetResponse()
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
                        return {_inputMessageTask(inputData)}.TrySetResult(({inputData.MessageTypeName})response);
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
                        return {_inputMessageTask(inputData, j)}.TrySetResult(({inputData.MessageTypeName})response);
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
                        return {_inputMessageTask(inputData)}.TrySetException(exception);
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
                        return {_inputMessageTask(inputData, j)}.TrySetException(exception);
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
                {_cts()}?.Dispose();"); 

            for (int i = 0; i < requestAwaiter.InputDatas.Count; i++)
            {
                var inputData = requestAwaiter.InputDatas[i];
                if (inputData.AcceptFromAny)
                {
                    builder.Append($@"
                        {_inputMessageTask(inputData)}.TrySetCanceled();");
                }
                else
                {
                    for (int j = 0; j < inputData.AcceptedService.Length; j++)
                    {
                        builder.Append($@"
                        {_inputMessageTask(inputData, j)}.TrySetCanceled();");
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
