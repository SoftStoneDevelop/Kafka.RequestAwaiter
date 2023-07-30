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

[RequestAwaiter(useLogger: false),
  Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),//input0
  Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))//output0
]
public partial class TestProtobuffAwaiter
{

}

```

Pass configs to Start methods. It's all what you need to do.
```C#

using var producerPool = new ProducerPoolProtoProto(3, "localhost:9194, localhost:9294, localhost:9394");
using var awaitService = new TestProtobuffAwaiter();
awaitService.Start(configKafka, producerPool);

using var response = await awaitService.Produce(
  new protobuff.SimpleKey() { Id = 459  },
  new protobuff.SimpleValue() { Id = 459, Priority = protobuff.Priority.Unspecified, Message = "Hello world!" }
  );
var responseInput0 = (ResponseItem<TestProtobuffAwaiter.Input0Message>)response.Result[0];
//responseInput0.Result is TestProtobuffAwaiter.Input0Message
//where responseInput0.Result.Key is protobuff.SimpleKey
//and responseInput0.Result.Value is protobuff.SimpleValue
            
```

You can have many input or output topics(two in the example):
```C#

[RequestAwaiter(useLogger: false),
  Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),//input0
  Input(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),//input1

  Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue)),//output0
  Output(keyType: typeof(protobuff.SimpleKey), valueType: typeof(protobuff.SimpleValue))//output1
]
public partial class TestProtobuffAwaiter
{

}

using var producerPool = new ProducerPoolProtoProto(3, "localhost:9194, localhost:9294, localhost:9394");
using var awaitService = new TestProtobuffAwaiter();
awaitService.Start(configKafka, producerPool, producerPool);

using var response = await awaitService.Produce(
  //to output0
  new protobuff.SimpleKey() { Id = 459  },
  new protobuff.SimpleValue() { Id = 459, Priority = protobuff.Priority.Unspecified, Message = "Hello world!" },

  //to output1
  new protobuff.SimpleKey() { Id = 123  },
  new protobuff.SimpleValue() { Id = 123, Priority = protobuff.Priority.Unspecified, Message = "Hello world! 2" }
  );

var responseInput0 = (ResponseItem<TestProtobuffAwaiter.Input0Message>)response.Result[0];
//responseInput0.Result is TestProtobuffAwaiter.Input0Message

var responseInput1 = (ResponseItem<TestProtobuffAwaiter.Input1Message>)response.Result[1];
//responseInput1.Result is TestProtobuffAwaiter.Input1Message
```
