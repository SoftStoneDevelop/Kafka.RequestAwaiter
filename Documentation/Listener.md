Constructors:

```C#

public ListenerAttribute(
  Type incomeKeyType,
  Type incomeValueType,
  bool useLogger = true
  )

```

Types can be any type or generated from Protobuff types(The main thing is that it should be inherited from IMessage<T>)

Usage:

```C#

[KafkaExchanger.Attributes.Listener
        (
        incomeKeyType: typeof(protobuff.SimpleKey),
        incomeValueType: typeof(protobuff.SimpleValue)
        )
        ]
    public partial class TestProtobuffListener
    {

    }

//action is you custom action
  
var simpleListener = new TestProtobuffListener(loggerFactory);
var consumerConfigs = new TestProtobuffListener.ConsumerListenerConfig[]
{
  new TestProtobuffListener.ConsumerListenerConfig(
    action,//Called on incoming messages
    "IncomeTopicName",
    new int[] { 0 }
  ),
  new TestProtobuffListener.ConsumerListenerConfig(
    action,//Called on incoming messages
    "IncomeTopicName",
    new int[] { 1 }
  ),
  new TestProtobuffListener.ConsumerListenerConfig(
    action,//Called on incoming messages
    "IncomeTopicName",
    new int[] { 2 }
  )
};

var configKafka = new TestProtobuffListener.ConfigListener(
  "grouId",
  "localhost:9194, localhost:9294, localhost:9394",//bootstrapServers
  consumerConfigs
  );

simpleListener.Start(configKafka);
```
