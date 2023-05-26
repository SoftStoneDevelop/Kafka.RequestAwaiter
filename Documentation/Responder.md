Constructors:

```C#

public ResponderAttribute(
  Type outcomeKeyType,
  Type outcomeValueType,
  Type incomeKeyType,
  Type incomeValueType,
  bool useLogger = true,
  uint commitAfter = 1,
  OrderMatters orderMatters = OrderMatters.NotMatters,
  bool checkCurrentState = false,
  bool useAfterSendResponse = false,
  bool useAfterCommit = false
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

//Implement IProducerPoolProtoProto for producerPool self or use delault generated pool ProducerPoolProtoProto

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

simpleResponder.Start(configKafka, producerPool);
```
