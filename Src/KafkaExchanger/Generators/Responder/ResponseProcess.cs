using KafkaExchanger.Datas;
using KafkaExchanger.Generators.RequestAwaiter;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace KafkaExchanger.Generators.Responder
{
    internal static class ResponseProcess
    {
        public static void Append(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            StartClass(builder, responder, assemblyName);

            Constructor(builder, responder);
            PropetiesAndFields(builder, responder, assemblyName);

            Init(builder);
            Dispose(builder, responder, assemblyName);
            Response(builder, assemblyName, responder);
            TrySetResponse(builder, responder);
            TrySetException(builder, responder);

            End(builder);
        }

        public static string TypeFullName(KafkaExchanger.Datas.Responder responder)
        {
            return $"{responder.TypeSymbol.Name}.{TypeName()}";
        }

        public static string TypeName()
        {
            return "ResponseProcess";
        }

        public static string BucketId()
        {
            return "BucketId";
        }

        public static string MessageId()
        {
            return "MessageId";
        }

        private static void StartClass(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder,
            string assemblyName
            )
        {
            builder.Append($@"
        public class {TypeName()} : IDisposable
        {{
");
        }

        private static string _inputTask(InputData inputData)
        {
            return $@"_{inputData.NameCamelCase}";
        }

        private static string _guid()
        {
            return "_guid";
        }

        private static string _createAnswer()
        {
            return "_createAnswer";
        }

        private static string _produce()
        {
            return "_produce";
        }

        private static string _removeAction()
        {
            return "_removeAction";
        }

        private static string _writer()
        {
            return "_writer";
        }

        private static string _afterSend()
        {
            return "_afterSend";
        }

        private static string _checkState()
        {
            return "_checkState";
        }

        private static string _partitions(InputData inputData)
        {
            return $"_{inputData.NameCamelCase}Partitions";
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            string partitions(InputData inputData)
            {
                return $"{inputData.NameCamelCase}Partitions";
            }

            builder.Append($@"
            public {TypeName()}(
                string guid,
                {responder.CreateAnswerFuncType()} createAnswer,
                {ProduceFuncType(responder)} produce,
                {RemoveActionType()} removeAction,
                {ChannelWriterType(responder)} writer");

            var afterSendParam = "afterSend";
            var needPartitions = false;
            if (responder.AfterSend)
            {
                needPartitions |= true;
                builder.Append($@",
                {responder.AfterSendFuncType()} {afterSendParam}");
            }

            var checkStateParam = "checkState";
            if (responder.CheckCurrentState)
            {
                needPartitions |= true;
                builder.Append($@",
                {responder.CheckCurrentStateFuncType()} {checkStateParam}");
            }

            if(needPartitions)
            {
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@",
                int[] {partitions(inputData)}");
                }
            }

            builder.Append($@"
                )
            {{
                {_guid()} = guid;
                {_createAnswer()} = createAnswer;
                {_produce()} = produce;
                {_removeAction()} = removeAction;
                {_writer()} = writer;");

            if (responder.AfterSend)
            {
                builder.Append($@"
                {_afterSend()} = {afterSendParam};");
            }

            if (responder.CheckCurrentState)
            {
                builder.Append($@"
                {_checkState()} = {checkStateParam};");
            }

            if (needPartitions)
            {
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@"
                {_partitions(inputData)} = {partitions(inputData)};");
                }
            }

            builder.Append($@"
            }}
");
        }

        private static void PropetiesAndFields(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder,
            string assemblyName
            )
        {
            builder.Append($@"
            public int {BucketId()} 
            {{ 
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set;
            }}

            public int {MessageId()} 
            {{ 
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get;

                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                set;
            }}

            private Task {_response()};
            private string {_guid()};
            private {responder.CreateAnswerFuncType()} {_createAnswer()};
            private {ProduceFuncType(responder)} {_produce()};
            {RemoveActionType()} {_removeAction()};
            {ChannelWriterType(responder)} {_writer()};");

            var needPartitions = false;
            if (responder.AfterSend)
            {
                needPartitions |= true;
                builder.Append($@"
                private {responder.AfterSendFuncType()} {_afterSend()};");
            }

            if (responder.CheckCurrentState)
            {
                needPartitions |= true;
                builder.Append($@"
                private {responder.CheckCurrentStateFuncType()} {_checkState()};");
            }

            if (needPartitions)
            {
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@"
                private int[] {_partitions(inputData)};");
                }
            }

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> {_inputTask(inputData)} = new(TaskCreationOptions.RunContinuationsAsynchronously);");
            }
        }

        private static void Dispose(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder,
            string assemblyName
            )
        {
            builder.Append($@"
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Dispose()
            {{
                if({_response()} != null)
                {{
                    return;
                }}
");
            
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                {_inputTask(inputData)}.TrySetCanceled();");
            }
            builder.Append($@"
                {_response()} = null;
                {BucketId()} = 0;
                {MessageId()} = 0;
                {_guid()} = null;
                {_createAnswer()} = null;
                {_produce()} = null;
                {_removeAction()} = null;
                {_writer()} = null;");

            var needPartitions = false;
            if (responder.AfterSend)
            {
                needPartitions |= true;
                builder.Append($@"
                {_afterSend()} = null;");
            }

            if (responder.CheckCurrentState)
            {
                needPartitions |= true;
                builder.Append($@"
                {_checkState()} = null;");
            }

            if (needPartitions)
            {
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@"
                {_partitions(inputData)} = null;");
                }
            }

            builder.Append($@"
            }}
");
        }

        private static void Init(StringBuilder builder)
        {
            builder.Append($@"
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void Init()
            {{
                {_response()} = 
                    Response()
                    .ContinueWith(
                        (task) => 
                        {{
                            try
                            {{
                                {_removeAction()}({_guid()});
                            }}
                            catch
                            {{
                                //ignore
                            }}
                            
                            Dispose();
                        }});
            }}");
        }

        private static string _response()
        {
            return "_response";
        }

        private static string ChannelWriterType(KafkaExchanger.Datas.Responder responder)
        {
            return $"ChannelWriter<{ChannelInfo.TypeFullName(responder)}>";
        }

        private static string RemoveActionType()
        {
            return $"Action<string>";
        }

        private static string ProduceFuncType(KafkaExchanger.Datas.Responder responder)
        {
            return $"Func<{OutputMessage.TypeFullName(responder)}, {InputMessage.TypeFullName(responder)}, Task>";
        }

        private static void Response(
            StringBuilder builder,
            string assemblyName,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            private async Task Response()
            {{");

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                var {inputData.NameCamelCase} = await {_inputTask(inputData)}.Task.ConfigureAwait(false);");
            }

            builder.Append($@"
                var inputMessage = new {InputMessage.TypeFullName(responder)}()
                {{");

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                    {InputMessage.Message(inputData)} = {inputData.NameCamelCase}");
            }
            builder.Append($@"
                }};");

            if(responder.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = await {_checkState()}(");
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@"
                    {_partitions(inputData)},
                    {inputData.NameCamelCase}");
                }
                builder.Append($@"
                    ).ConfigureAwait(false);");
            }
            else
            {
                builder.Append($@"
                var currentState = KafkaExchanger.Attributes.Enums.CurrentState.NewMessage;");
            }

            builder.Append($@"
                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                {{
                    var outputMessage = await {_createAnswer()}(inputMessage, currentState).ConfigureAwait(false);
                    await {_produce()}(outputMessage, inputMessage).ConfigureAwait(false);");
            
            if(responder.AfterSend)
            {
                builder.Append($@"
                await {_afterSend()}(");
                for (int i = 0; i < responder.InputDatas.Count; i++)
                {
                    var inputData = responder.InputDatas[i];
                    builder.Append($@"
                    {_partitions(inputData)},
                    {inputData.NameCamelCase}");
                }
                builder.Append($@"
                    ).ConfigureAwait(false);");
            }

            builder.Append($@"
                }}

                var endResponse = new {EndResponse.TypeFullName(responder)}() 
                {{
                    {EndResponse.BucketId()} = this.{BucketId()},
                    {EndResponse.MessageId()} = this.{MessageId()}");

            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@",
                    {inputData.NamePascalCase} = {inputData.NameCamelCase}.TopicPartitionOffset");
            }
            builder.Append($@"
                }};

                await {_writer()}.WriteAsync(endResponse).ConfigureAwait(false);
            }}
");
        }

        private static void TrySetResponse(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public bool TrySetResponse(int topicNumber, BaseInputMessage response, int serviceNumber = 0)
            {{
                switch (topicNumber, serviceNumber)
                {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                    case ({i}, 0):
                    {{
                        return {_inputTask(inputData)}.TrySetResult(({inputData.MessageTypeName})response);
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
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public bool TrySetException(int topicNumber, Exception exception, int serviceNumber = 0)
            {{
                switch (topicNumber, serviceNumber)
                {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                    case ({i}, 0):
                    {{
                        return {_inputTask(inputData)}.TrySetException(exception);
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
