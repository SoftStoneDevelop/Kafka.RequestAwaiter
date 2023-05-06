namespace ExampleClient
{
    [KafkaExchanger.Attributes.RequestAwaiter
        (
        incomeKeyType: typeof(string),
        incomeValueType: typeof(string),
        outcomeKeyType: typeof(string),
        outcomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleAwaiter
    {

    }

    [KafkaExchanger.Attributes.Responder
        (
        incomeKeyType: typeof(string),
        incomeValueType: typeof(string),
        outcomeKeyType: typeof(string),
        outcomeValueType: typeof(string)
        )
        ]
    public partial class TestSimpleResponder
    {

    }
}