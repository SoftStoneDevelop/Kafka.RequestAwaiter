﻿using Microsoft.CodeAnalysis;
using System;
using System.Collections.Generic;
using System.Text;

namespace KafkaExchanger.Generators
{
    public class HeaderGenarator
    {
        public void Generate(GeneratorExecutionContext context)
        {
            context.AddSource($"Headers.g.cs", GetHeaderCode());
        }

        private string GetHeaderCode()
        {
            return @"
// <auto-generated>
//     Generated by the protocol buffer compiler.  DO NOT EDIT!
//     source: Proto/headers.proto
// </auto-generated>
#pragma warning disable 1591, 0612, 3021, 8981
#region Designer generated code

using pb = global::Google.Protobuf;
using pbc = global::Google.Protobuf.Collections;
using pbr = global::Google.Protobuf.Reflection;
using scg = global::System.Collections.Generic;
namespace kafka {

  /// <summary>Holder for reflection information generated from Proto/headers.proto</summary>
  public static partial class HeadersReflection {

    #region Descriptor
    /// <summary>File descriptor for Proto/headers.proto</summary>
    public static pbr::FileDescriptor Descriptor {
      get { return descriptor; }
    }
    private static pbr::FileDescriptor descriptor;

    static HeadersReflection() {
      byte[] descriptorData = global::System.Convert.FromBase64String(
          string.Concat(
            ""ChNQcm90by9oZWFkZXJzLnByb3RvEgdoZWFkZXJzIk0KDVJlcXVlc3RIZWFk"",
            ""ZXISEwoLTWVzc2FnZUd1aWQYASABKAkSJwoPVG9waWNzRm9yQW5zd2VyGAIg"",
            ""AygLMg4uaGVhZGVycy5Ub3BpYyItCg5SZXNwb25zZUhlYWRlchIbChNBbnN3"",
            ""ZXJUb01lc3NhZ2VHdWlkGAEgASgJIikKBVRvcGljEgwKBE5hbWUYASABKAkS"",
            ""EgoKUGFydGl0aW9ucxgCIAMoBUIIqgIFa2Fma2FiBnByb3RvMw==""));
      descriptor = pbr::FileDescriptor.FromGeneratedCode(descriptorData,
          new pbr::FileDescriptor[] { },
          new pbr::GeneratedClrTypeInfo(null, null, new pbr::GeneratedClrTypeInfo[] {
            new pbr::GeneratedClrTypeInfo(typeof(global::kafka.RequestHeader), global::kafka.RequestHeader.Parser, new[]{ ""MessageGuid"", ""TopicsForAnswer"" }, null, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::kafka.ResponseHeader), global::kafka.ResponseHeader.Parser, new[]{ ""AnswerToMessageGuid"" }, null, null, null, null),
            new pbr::GeneratedClrTypeInfo(typeof(global::kafka.Topic), global::kafka.Topic.Parser, new[]{ ""Name"", ""Partitions"" }, null, null, null, null)
          }));
    }
    #endregion

  }
  #region Messages
  public sealed partial class RequestHeader : pb::IMessage<RequestHeader>
  #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      , pb::IBufferMessage
  #endif
  {
    private static readonly pb::MessageParser<RequestHeader> _parser = new pb::MessageParser<RequestHeader>(() => new RequestHeader());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pb::MessageParser<RequestHeader> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::kafka.HeadersReflection.Descriptor.MessageTypes[0]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public RequestHeader() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public RequestHeader(RequestHeader other) : this() {
      messageGuid_ = other.messageGuid_;
      topicsForAnswer_ = other.topicsForAnswer_.Clone();
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public RequestHeader Clone() {
      return new RequestHeader(this);
    }

    /// <summary>Field number for the ""MessageGuid"" field.</summary>
    public const int MessageGuidFieldNumber = 1;
    private string messageGuid_ = """";
    /// <summary>
    ///8-4-4-4-12
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public string MessageGuid {
      get { return messageGuid_; }
      set {
        messageGuid_ = pb::ProtoPreconditions.CheckNotNull(value, ""value"");
      }
    }

    /// <summary>Field number for the ""TopicsForAnswer"" field.</summary>
    public const int TopicsForAnswerFieldNumber = 2;
    private static readonly pb::FieldCodec<global::kafka.Topic> _repeated_topicsForAnswer_codec
        = pb::FieldCodec.ForMessage(18, global::kafka.Topic.Parser);
    private readonly pbc::RepeatedField<global::kafka.Topic> topicsForAnswer_ = new pbc::RepeatedField<global::kafka.Topic>();
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public pbc::RepeatedField<global::kafka.Topic> TopicsForAnswer {
      get { return topicsForAnswer_; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override bool Equals(object other) {
      return Equals(other as RequestHeader);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public bool Equals(RequestHeader other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (MessageGuid != other.MessageGuid) return false;
      if(!topicsForAnswer_.Equals(other.topicsForAnswer_)) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override int GetHashCode() {
      int hash = 1;
      if (MessageGuid.Length != 0) hash ^= MessageGuid.GetHashCode();
      hash ^= topicsForAnswer_.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void WriteTo(pb::CodedOutputStream output) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      output.WriteRawMessage(this);
    #else
      if (MessageGuid.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(MessageGuid);
      }
      topicsForAnswer_.WriteTo(output, _repeated_topicsForAnswer_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalWriteTo(ref pb::WriteContext output) {
      if (MessageGuid.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(MessageGuid);
      }
      topicsForAnswer_.WriteTo(ref output, _repeated_topicsForAnswer_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(ref output);
      }
    }
    #endif

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public int CalculateSize() {
      int size = 0;
      if (MessageGuid.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(MessageGuid);
      }
      size += topicsForAnswer_.CalculateSize(_repeated_topicsForAnswer_codec);
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(RequestHeader other) {
      if (other == null) {
        return;
      }
      if (other.MessageGuid.Length != 0) {
        MessageGuid = other.MessageGuid;
      }
      topicsForAnswer_.Add(other.topicsForAnswer_);
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(pb::CodedInputStream input) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      input.ReadRawMessage(this);
    #else
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            MessageGuid = input.ReadString();
            break;
          }
          case 18: {
            topicsForAnswer_.AddEntriesFrom(input, _repeated_topicsForAnswer_codec);
            break;
          }
        }
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalMergeFrom(ref pb::ParseContext input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, ref input);
            break;
          case 10: {
            MessageGuid = input.ReadString();
            break;
          }
          case 18: {
            topicsForAnswer_.AddEntriesFrom(ref input, _repeated_topicsForAnswer_codec);
            break;
          }
        }
      }
    }
    #endif

  }

  public sealed partial class ResponseHeader : pb::IMessage<ResponseHeader>
  #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      , pb::IBufferMessage
  #endif
  {
    private static readonly pb::MessageParser<ResponseHeader> _parser = new pb::MessageParser<ResponseHeader>(() => new ResponseHeader());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pb::MessageParser<ResponseHeader> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::kafka.HeadersReflection.Descriptor.MessageTypes[1]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public ResponseHeader() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public ResponseHeader(ResponseHeader other) : this() {
      answerToMessageGuid_ = other.answerToMessageGuid_;
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public ResponseHeader Clone() {
      return new ResponseHeader(this);
    }

    /// <summary>Field number for the ""AnswerToMessageGuid"" field.</summary>
    public const int AnswerToMessageGuidFieldNumber = 1;
    private string answerToMessageGuid_ = """";
    /// <summary>
    ///8-4-4-4-12
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public string AnswerToMessageGuid {
      get { return answerToMessageGuid_; }
      set {
        answerToMessageGuid_ = pb::ProtoPreconditions.CheckNotNull(value, ""value"");
      }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override bool Equals(object other) {
      return Equals(other as ResponseHeader);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public bool Equals(ResponseHeader other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (AnswerToMessageGuid != other.AnswerToMessageGuid) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override int GetHashCode() {
      int hash = 1;
      if (AnswerToMessageGuid.Length != 0) hash ^= AnswerToMessageGuid.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void WriteTo(pb::CodedOutputStream output) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      output.WriteRawMessage(this);
    #else
      if (AnswerToMessageGuid.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(AnswerToMessageGuid);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalWriteTo(ref pb::WriteContext output) {
      if (AnswerToMessageGuid.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(AnswerToMessageGuid);
      }
      if (_unknownFields != null) {
        _unknownFields.WriteTo(ref output);
      }
    }
    #endif

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public int CalculateSize() {
      int size = 0;
      if (AnswerToMessageGuid.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(AnswerToMessageGuid);
      }
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(ResponseHeader other) {
      if (other == null) {
        return;
      }
      if (other.AnswerToMessageGuid.Length != 0) {
        AnswerToMessageGuid = other.AnswerToMessageGuid;
      }
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(pb::CodedInputStream input) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      input.ReadRawMessage(this);
    #else
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            AnswerToMessageGuid = input.ReadString();
            break;
          }
        }
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalMergeFrom(ref pb::ParseContext input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, ref input);
            break;
          case 10: {
            AnswerToMessageGuid = input.ReadString();
            break;
          }
        }
      }
    }
    #endif

  }

  public sealed partial class Topic : pb::IMessage<Topic>
  #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      , pb::IBufferMessage
  #endif
  {
    private static readonly pb::MessageParser<Topic> _parser = new pb::MessageParser<Topic>(() => new Topic());
    private pb::UnknownFieldSet _unknownFields;
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pb::MessageParser<Topic> Parser { get { return _parser; } }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public static pbr::MessageDescriptor Descriptor {
      get { return global::kafka.HeadersReflection.Descriptor.MessageTypes[2]; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    pbr::MessageDescriptor pb::IMessage.Descriptor {
      get { return Descriptor; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public Topic() {
      OnConstruction();
    }

    partial void OnConstruction();

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public Topic(Topic other) : this() {
      name_ = other.name_;
      partitions_ = other.partitions_.Clone();
      _unknownFields = pb::UnknownFieldSet.Clone(other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public Topic Clone() {
      return new Topic(this);
    }

    /// <summary>Field number for the ""Name"" field.</summary>
    public const int NameFieldNumber = 1;
    private string name_ = """";
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public string Name {
      get { return name_; }
      set {
        name_ = pb::ProtoPreconditions.CheckNotNull(value, ""value"");
      }
    }

    /// <summary>Field number for the ""Partitions"" field.</summary>
    public const int PartitionsFieldNumber = 2;
    private static readonly pb::FieldCodec<int> _repeated_partitions_codec
        = pb::FieldCodec.ForInt32(18);
    private readonly pbc::RepeatedField<int> partitions_ = new pbc::RepeatedField<int>();
    /// <summary>
    ///-1 or emty array - Any
    /// </summary>
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public pbc::RepeatedField<int> Partitions {
      get { return partitions_; }
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override bool Equals(object other) {
      return Equals(other as Topic);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public bool Equals(Topic other) {
      if (ReferenceEquals(other, null)) {
        return false;
      }
      if (ReferenceEquals(other, this)) {
        return true;
      }
      if (Name != other.Name) return false;
      if(!partitions_.Equals(other.partitions_)) return false;
      return Equals(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override int GetHashCode() {
      int hash = 1;
      if (Name.Length != 0) hash ^= Name.GetHashCode();
      hash ^= partitions_.GetHashCode();
      if (_unknownFields != null) {
        hash ^= _unknownFields.GetHashCode();
      }
      return hash;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public override string ToString() {
      return pb::JsonFormatter.ToDiagnosticString(this);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void WriteTo(pb::CodedOutputStream output) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      output.WriteRawMessage(this);
    #else
      if (Name.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(Name);
      }
      partitions_.WriteTo(output, _repeated_partitions_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(output);
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalWriteTo(ref pb::WriteContext output) {
      if (Name.Length != 0) {
        output.WriteRawTag(10);
        output.WriteString(Name);
      }
      partitions_.WriteTo(ref output, _repeated_partitions_codec);
      if (_unknownFields != null) {
        _unknownFields.WriteTo(ref output);
      }
    }
    #endif

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public int CalculateSize() {
      int size = 0;
      if (Name.Length != 0) {
        size += 1 + pb::CodedOutputStream.ComputeStringSize(Name);
      }
      size += partitions_.CalculateSize(_repeated_partitions_codec);
      if (_unknownFields != null) {
        size += _unknownFields.CalculateSize();
      }
      return size;
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(Topic other) {
      if (other == null) {
        return;
      }
      if (other.Name.Length != 0) {
        Name = other.Name;
      }
      partitions_.Add(other.partitions_);
      _unknownFields = pb::UnknownFieldSet.MergeFrom(_unknownFields, other._unknownFields);
    }

    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    public void MergeFrom(pb::CodedInputStream input) {
    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
      input.ReadRawMessage(this);
    #else
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, input);
            break;
          case 10: {
            Name = input.ReadString();
            break;
          }
          case 18:
          case 16: {
            partitions_.AddEntriesFrom(input, _repeated_partitions_codec);
            break;
          }
        }
      }
    #endif
    }

    #if !GOOGLE_PROTOBUF_REFSTRUCT_COMPATIBILITY_MODE
    [global::System.Diagnostics.DebuggerNonUserCodeAttribute]
    [global::System.CodeDom.Compiler.GeneratedCode(""protoc"", null)]
    void pb::IBufferMessage.InternalMergeFrom(ref pb::ParseContext input) {
      uint tag;
      while ((tag = input.ReadTag()) != 0) {
        switch(tag) {
          default:
            _unknownFields = pb::UnknownFieldSet.MergeFieldFrom(_unknownFields, ref input);
            break;
          case 10: {
            Name = input.ReadString();
            break;
          }
          case 18:
          case 16: {
            partitions_.AddEntriesFrom(ref input, _repeated_partitions_codec);
            break;
          }
        }
      }
    }
    #endif

  }

  #endregion

}

#endregion Designer generated code

";
        }
    }
}