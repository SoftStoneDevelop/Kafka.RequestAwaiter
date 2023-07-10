using Confluent.Kafka;
using KafkaExchanger.Attributes;

namespace ExampleClient
{
    [RequestAwaiter(),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class TestSimpleAwaiter
    {

    }

    [Responder(),
        Income(keyType: typeof(Null), valueType: typeof(string)),
        Outcome(keyType: typeof(Null), valueType: typeof(string))
        ]
    public partial class TestSimpleResponder
    {

    }

    [RequestAwaiter(),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Outcome(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class TestProtobuffAwaiter
    {

    }

    [Responder(),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Outcome(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class TestProtobuffResponder
    {

    }

    [Listener(),
        Income(keyType: typeof(string), valueType: typeof(string))
        ]
    public partial class TestSimpleListener
    {

    }

    [Listener(),
        Income(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
        ]
    public partial class TestProtobuffListener
    {

    }
}