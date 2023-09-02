using Confluent.Kafka;
using KafkaExchanger.Attributes;

namespace KafkaExchengerTests
{
    [RequestAwaiter(useLogger: false, afterSend: true, AddAwaiterCheckStatus: true),
        Input(keyType: typeof(Null), valueType: typeof(string), new string[] { "RAResponder1" }),
        Input(keyType: typeof(Null), valueType: typeof(string), new string[] { "RAResponder2" }),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiterSimple
    {

    }

    //[RequestAwaiter(
    //    useLogger: true, 
    //    afterSend: true, 
    //    AddAwaiterCheckStatus: true, 
    //    useAfterCommit: true, 
    //    checkCurrentState: true
    //    ),
    //    Input(keyType: typeof(Null), valueType: typeof(string), new string[] { "RAResponder1" }),
    //    Input(keyType: typeof(Null), valueType: typeof(string), new string[] { "RAResponder2" }),
    //    Output(keyType: typeof(Null), valueType: typeof(string))
    //    ]
    //public partial class RequestAwaiterFull
    //{

    //}

    [Responder(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class ResponderOneToOneSimple
    {

    }

    [Responder(
        useLogger: true,
        checkCurrentState: true,
        afterSend: true,
        afterCommit: true
        ),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class ResponderFull
    {

    }
}
