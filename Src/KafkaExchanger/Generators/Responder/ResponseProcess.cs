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
            TCS(builder, responder);
            Constructor(builder, responder);
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

        public static string HorizonId()
        {
            return "HorizonId";
        }

        private static void StartClass(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder,
            string assemblyName
            )
        {
            builder.Append($@"
        public class {TypeName()}
        {{
            public Task _response;
            public long {HorizonId()} {{ get; init; }}
");
        }

        private static void TCS(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
            private TaskCompletionSource<{inputData.MessageTypeName}> _{inputData.NameCamelCase} = new(TaskCreationOptions.RunContinuationsAsynchronously);
");
            }
        }

        private static void Constructor(
            StringBuilder builder,
            KafkaExchanger.Datas.Responder responder
            )
        {
            builder.Append($@"
            public {TypeName()}(
                string guid,
                long horizonId,
                {ProcessorConfig.CreateAnswerFuncType(responder)} createAnswer,
                {ProduceFuncType(responder)} produce,
                Action<string> removeAction,
                ChannelWriter<{ChannelInfo.TypeFullName(responder)}> writer
                )
            {{
                HorizonId = horizonId;
                _response = Response(
                    guid,
                    createAnswer,
                    produce,
                    removeAction,
                    writer
                    );
            }}
");
        }

        public static string ProduceFuncType(KafkaExchanger.Datas.Responder responder)
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
            private async Task Response(
                string guid,
                {ProcessorConfig.CreateAnswerFuncType(responder)} createAnswer,
                {ProduceFuncType(responder)} produce,
                Action<string> removeAction,
                ChannelWriter<{ChannelInfo.TypeFullName(responder)}> writer
                )
            {{
                
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@"
                var {inputData.NameCamelCase} = await _{inputData.NameCamelCase}.Task.ConfigureAwait(false);
");
            }

            builder.Append($@"
                var inputMessage = new {InputMessage.TypeFullName(responder)}()
                {{
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                if(i != 0)
                {
                    builder.Append(',');
                }

                builder.Append($@"
                {inputData.MessageTypeName} = {inputData.NameCamelCase}
");
            }
            builder.Append($@"
                }};
");
            if(responder.CheckCurrentState)
            {
                builder.Append($@"
                var currentState = _checkCurrentState();//TODO
");
            }
            else
            {
                builder.Append($@"
                var currentState = KafkaExchanger.Attributes.Enums.CurrentState.NewMessage;
");
            }

            builder.Append($@"
                if(currentState != KafkaExchanger.Attributes.Enums.CurrentState.AnswerSended)
                {{
                    var outputMessage = await createAnswer(inputMessage, currentState).ConfigureAwait(false);
                    await produce(outputMessage, inputMessage).ConfigureAwait(false);
                }}

                var endResponse = new {EndResponse.TypeFullName(responder)}() 
                {{
                    HorizonId = this.HorizonId
");
            for (int i = 0; i < responder.InputDatas.Count; i++)
            {
                var inputData = responder.InputDatas[i];
                builder.Append($@",
                    {inputData.NamePascalCase} = {inputData.NameCamelCase}.TopicPartitionOffset
");
            }
            builder.Append($@"
                }};

                await writer.WriteAsync(endResponse).ConfigureAwait(false);
                removeAction(guid);
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
                        return _{inputData.NameCamelCase}.TrySetResult(({inputData.MessageTypeName})response);
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
                        return _{inputData.NameCamelCase}.TrySetException(exception);
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
