Constructors:

```C#

public ResponderAttribute(
  Type outcomeKeyType,
  Type outcomeValueType,
  Type incomeKeyType,
  Type incomeValueType,
  bool useLogger = true
  )

```

Usage:

```C#

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


var simpleResponder = new TestProtobuffResponder(loggerFactory);
var consumerConfigs = new TestProtobuffResponder.ConsumerResponderConfig[]
{
  new TestProtobuffResponder.ConsumerResponderConfig(
    funcCreateAnswer,
    "IncomeTopicName",
    new int[] { 0 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
    funcCreateAnswer,
    "IncomeTopicName",
    new int[] { 1 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
    funcCreateAnswer,
    "IncomeTopicName",
    new int[] { 2 }
  )
};

var configKafka = new TestProtobuffResponder.ConfigResponder(
  "grouId",
  "localhost:9194, localhost:9294, localhost:9394",//bootstrapServers
  consumerConfigs
  );

simpleResponder.Start(configKafka);
```
