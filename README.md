<h1 align="center">
  <a>KafkaExchanger</a>
</h1>

<h3 align="center">

  [![Nuget](https://img.shields.io/nuget/v/KafkaExchanger?logo=KafkaExchanger)](https://www.nuget.org/packages/KafkaExchanger/)
  [![Downloads](https://img.shields.io/nuget/dt/KafkaExchanger.svg)](https://www.nuget.org/packages/KafkaExchanger/)
  [![Stars](https://img.shields.io/github/stars/SoftStoneDevelop/KafkaExchanger?color=brightgreen)](https://github.com/SoftStoneDevelop/KafkaExchanger/stargazers)
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

</h3>

<h3 align="center">
  <a href="https://github.com/SoftStoneDevelop/KafkaExchanger/tree/main/Documentation/Readme.md">Documentation</a>
</h3>

A Kafka broker message processing service generator to simplify communication in a microservices environment.

Usage with Protobuff key/value:

```proto

syntax = "proto3";
option csharp_namespace = "protobuff";
package protobuffKeys;

message SimpleKey
{
    int32 Id = 1;
}

```

```proto

syntax = "proto3";
option csharp_namespace = "protobuff";
package protobuffValues;

message SimpleValue
{
    int32 Id = 1;
    Priority Priority = 2;
    string Message = 3;
}

enum Priority
{
    Priority_UNSPECIFIED = 0;
    Priority_WHITE = 1;
    Priority_YELLOW = 2;
    Priority_RED = 3;
}

```
Declare partial classes with attributes:
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

```

Pass configs to Start methods. It's all what you need to do.
```C#

var producerPool = new ProducerPoolProtoProto(3, "localhost:9194, localhost:9294, localhost:9394");
var awaitService = new TestProtobuffAwaiter(loggerFactory);
//for each ConsumerConfig created own consumer and thread.
//In this case simpleAwaiter create 3 thread/consumer/producer
var consumerConfigs = new KafkaExchanger.Common.ConsumerConfig[]
{
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 0 }//one consumer can response for many partitions or ane partition in this case
    ),
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 1 }
    ),
  new KafkaExchanger.Common.ConsumerConfig(
    "IncomeTopicName",
    new int[] { 2 }                    )
};

var configKafka = new KafkaExchanger.Common.ConfigRequestAwaiter(
  "TestGroup",
  "localhost:9194, localhost:9294, localhost:9394",
  "OutcomeTopicName,
  consumerConfigs
  );

awaitService.Start(configKafka, producerPool);

var response = await awaitService.Produce(
  new protobuff.SimpleKey() { Id = 459  },
  new protobuff.SimpleValue() { Id = 459, Priority = protobuff.Priority.Unspecified, Message = "Hello world!" }
  );
//response.Result.Key is incomeKeyType(protobuff.SimpleKey)
//response.Result.Value is incomeValueType(protobuff.SimpleValue)
            
response.FinishProcessing();
```

```C#
var producerPool = new ProducerPoolProtoProto(3, "localhost:9194, localhost:9294, localhost:9394");
var responderService = new TestProtobuffResponder(loggerFactory);
var consumerConfigs = new TestProtobuffResponder.ConsumerResponderConfig[]
{
  new TestProtobuffResponder.ConsumerResponderConfig(
  (income) =>
    new TestProtobuffResponder.OutcomeMessage()
    {
      Key = new protobuff.SimpleKey()
      {
        Id = income.Key.Id
      },
      Value =
        new protobuff.SimpleValue()
        {
          Id = income.Key.Id,
          Priority = protobuff.Priority.White,
          Message = $"'{income.Value.Message}' back from 0"
        }
    },
  "IncomeTopicName",
  new int[] { 0 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
    (income) =>
    new TestProtobuffResponder.OutcomeMessage()
    {
      Key = new protobuff.SimpleKey()
      {
        Id = income.Key.Id
      },
      Value =
        new protobuff.SimpleValue()
        {
          Id = income.Key.Id,
          Priority = protobuff.Priority.Yellow,
          Message = $"'{income.Value.Message}' back from 1"
        }
    },
    "IncomeTopicName",
  new int[] { 1 }
  ),
  new TestProtobuffResponder.ConsumerResponderConfig(
  (income) => 
    new TestProtobuffResponder.OutcomeMessage()
    { 
      Key = new protobuff.SimpleKey() 
      { 
        Id = income.Key.Id 
      }, 
      Value = 
        new protobuff.SimpleValue() 
        { 
          Id = income.Key.Id, 
          Priority = protobuff.Priority.Red, 
          Message = $"'{income.Value.Message}' back from 2" 
        }
      },
      "IncomeTopicName",
      new int[] { 2 }
    )
};

var configKafka = new TestProtobuffResponder.ConfigResponder(
  "TestGroup",
  "localhost:9194, localhost:9294, localhost:9394",
  consumerConfigs
  );

responderService.Start(configKafka, producerPool);
```
