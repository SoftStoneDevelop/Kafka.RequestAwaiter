If your clients/consumers are not written in C#, they need to add this information to the header, where the key is "Information" and the value is the protobuff byte array.

```proto
syntax = "proto3";

option csharp_namespace = "kafka";
package headers;

message RequestHeader
{
	//8-4-4-4-12
	string MessageGuid = 1;

	repeated Topic TopicsForAnswer = 2;
}

message ResponseHeader
{
	//8-4-4-4-12
	string AnswerToMessageGuid = 1;

	//Service Name
	string AnswerFrom = 2;
}

message Topic
{
	string Name = 1;

	//-1 or emty array - Any
	repeated int32 Partitions = 2;
}
```
