using Confluent.Kafka;
using KafkaExchanger.Attributes;

namespace KafkaExchengerTests
{
    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiterOneToOneSimple
    {

    }

    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class RequestAwaiterManyToOneSimple
    {

    }

    [Responder(useLogger: false),
        Input(keyType: typeof(Null), valueType: typeof(string)),
        Output(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class ResponderOneToOneSimple
    {

    }

    [RequestAwaiter(useLogger: false),
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class RequestAwaiterOneToOneProtobuff
    {

    }

    [Responder(useLogger: false),
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class ResponderOneToOneProtobuff
    {

    }
}
