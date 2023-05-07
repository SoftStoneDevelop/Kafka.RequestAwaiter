Constructors:

```C#

public RequestAwaiterAttribute(
  Type outcomeKeyType, 
  Type outcomeValueType, 
  Type incomeKeyType, 
  Type incomeValueType, 
  bool useLogger = true
  )

```

Types can be any type or generated from Protobuff types(The main thing is that it should be inherited from IMessage<T>)

Usage:

```C#

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

//Implement IProducerPoolProtoProto for producerPool self or use delault generated pool ProducerPoolProtoProto
var simpleAwaiter = new TestProtobuffAwaiter(loggerFactory);
var consumerConfigs = new KafkaExchanger.Common.ConsumerConfig[]
{
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 0 }
  ),
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 1 }
  ),
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 2 }
  )
  };

  var configKafka = new KafkaExchanger.Common.ConfigRequestAwaiter(
    "groupId",
    "localhost:9194, localhost:9294, localhost:9394",//bootstrapServers
    "OutComeTopicName",
    consumerConfigs
    );

  simpleAwaiter.Start(configKafka, producerPool);
  
  var answer = await simpleAwaiter.Produce(
    new protobuff.SimpleKey() { Id = 12  },
    new protobuff.SimpleValue() { Id = 12, Message = "Hello" }
    );
    
    //process answer.Result
    
    answer.FinishProcessing();
```
