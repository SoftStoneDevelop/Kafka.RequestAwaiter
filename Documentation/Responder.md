Constructors:

```C#

public ResponderAttribute(
  bool useLogger = true,
  uint commitAfter = 1u,
  OrderMatters orderMatters = OrderMatters.NotMatters,
  bool checkCurrentState = false,
  bool useAfterSendResponse = false,
  bool useAfterCommit = false,
  bool customOutputHeader = false,
  bool customHeaders = false
  )

```

Usage:

```C#

[Responder(),
        Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),
        Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))
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
    "InputTopicName",
    new int[] { 0 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
    funcCreateAnswer,
    "InputTopicName",
    new int[] { 1 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
    funcCreateAnswer,
    "InputTopicName",
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
