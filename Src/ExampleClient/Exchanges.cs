using Confluent.Kafka;
using KafkaExchanger.Attributes;

namespace ExampleClient
{
    [RequestAwaiter
        (
        incomeKeyType: typeof(Null),
        incomeValueType: typeof(string),

        outcomeKeyType: typeof(Null),
        outcomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleAwaiter
    {

    }

    [Responder
        (
        incomeKeyType: typeof(Null),
        incomeValueType: typeof(string),

        outcomeKeyType: typeof(Null),
        outcomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleResponder
    {

    }

    [RequestAwaiter
        (
        incomeKeyType: typeof(protobuff.SimpleKey),
        incomeValueType: typeof(protobuff.SimpleValue),

        outcomeKeyType: typeof(protobuff.SimpleKey),
        outcomeValueType: typeof(protobuff.SimpleValue)
        )
        ]
    public partial class TestProtobuffAwaiter
    {

    }

    [Responder
        (
        incomeKeyType: typeof(protobuff.SimpleKey),
        incomeValueType: typeof(protobuff.SimpleValue),

        outcomeKeyType: typeof(protobuff.SimpleKey),
        outcomeValueType: typeof(protobuff.SimpleValue)
        )
        ]
    public partial class TestProtobuffResponder
    {

    }

    [Listener
        (
        incomeKeyType: typeof(string),
        incomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleListener
    {

    }

    [Listener
        (
        incomeKeyType: typeof(protobuff.SimpleKey),
        incomeValueType: typeof(protobuff.SimpleValue)
        )
        ]
    public partial class TestProtobuffListener
    {

    }
}