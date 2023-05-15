using Confluent.Kafka;

namespace ExampleClient
{
    [KafkaExchanger.Attributes.RequestAwaiter
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

    [KafkaExchanger.Attributes.Responder
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

    [KafkaExchanger.Attributes.RequestAwaiter
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

    [KafkaExchanger.Attributes.Responder
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

    [KafkaExchanger.Attributes.Listener
        (
        incomeKeyType: typeof(string),
        incomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleListener
    {

    }

    [KafkaExchanger.Attributes.Listener
        (
        incomeKeyType: typeof(protobuff.SimpleKey),
        incomeValueType: typeof(protobuff.SimpleValue)
        )
        ]
    public partial class TestProtobuffListener
    {

    }
}