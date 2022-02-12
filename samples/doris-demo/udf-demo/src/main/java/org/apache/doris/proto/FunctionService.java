// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.proto;

public final class FunctionService {
  private FunctionService() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface PRequestContextOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PRequestContext)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string id = 1;</code>
     * @return Whether the id field is set.
     */
    boolean hasId();
    /**
     * <code>optional string id = 1;</code>
     * @return The id.
     */
    java.lang.String getId();
    /**
     * <code>optional string id = 1;</code>
     * @return The bytes for id.
     */
    com.google.protobuf.ByteString
        getIdBytes();

    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     * @return Whether the functionContext field is set.
     */
    boolean hasFunctionContext();
    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     * @return The functionContext.
     */
    org.apache.doris.proto.Types.PFunctionContext getFunctionContext();
    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     */
    org.apache.doris.proto.Types.PFunctionContextOrBuilder getFunctionContextOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PRequestContext}
   */
  public  static final class PRequestContext extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PRequestContext)
      PRequestContextOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PRequestContext.newBuilder() to construct.
    private PRequestContext(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PRequestContext() {
      id_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PRequestContext();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PRequestContext(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000001;
              id_ = bs;
              break;
            }
            case 18: {
              org.apache.doris.proto.Types.PFunctionContext.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = functionContext_.toBuilder();
              }
              functionContext_ = input.readMessage(org.apache.doris.proto.Types.PFunctionContext.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(functionContext_);
                functionContext_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PRequestContext_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PRequestContext_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.FunctionService.PRequestContext.class, org.apache.doris.proto.FunctionService.PRequestContext.Builder.class);
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 1;
    private volatile java.lang.Object id_;
    /**
     * <code>optional string id = 1;</code>
     * @return Whether the id field is set.
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string id = 1;</code>
     * @return The id.
     */
    public java.lang.String getId() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          id_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string id = 1;</code>
     * @return The bytes for id.
     */
    public com.google.protobuf.ByteString
        getIdBytes() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        id_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int FUNCTION_CONTEXT_FIELD_NUMBER = 2;
    private org.apache.doris.proto.Types.PFunctionContext functionContext_;
    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     * @return Whether the functionContext field is set.
     */
    public boolean hasFunctionContext() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     * @return The functionContext.
     */
    public org.apache.doris.proto.Types.PFunctionContext getFunctionContext() {
      return functionContext_ == null ? org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance() : functionContext_;
    }
    /**
     * <code>optional .doris.PFunctionContext function_context = 2;</code>
     */
    public org.apache.doris.proto.Types.PFunctionContextOrBuilder getFunctionContextOrBuilder() {
      return functionContext_ == null ? org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance() : functionContext_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (hasFunctionContext()) {
        if (!getFunctionContext().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, id_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(2, getFunctionContext());
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, id_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, getFunctionContext());
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.doris.proto.FunctionService.PRequestContext)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.FunctionService.PRequestContext other = (org.apache.doris.proto.FunctionService.PRequestContext) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (!getId()
            .equals(other.getId())) return false;
      }
      if (hasFunctionContext() != other.hasFunctionContext()) return false;
      if (hasFunctionContext()) {
        if (!getFunctionContext()
            .equals(other.getFunctionContext())) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasId()) {
        hash = (37 * hash) + ID_FIELD_NUMBER;
        hash = (53 * hash) + getId().hashCode();
      }
      if (hasFunctionContext()) {
        hash = (37 * hash) + FUNCTION_CONTEXT_FIELD_NUMBER;
        hash = (53 * hash) + getFunctionContext().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PRequestContext parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.doris.proto.FunctionService.PRequestContext prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code doris.PRequestContext}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PRequestContext)
        org.apache.doris.proto.FunctionService.PRequestContextOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PRequestContext_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PRequestContext_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.FunctionService.PRequestContext.class, org.apache.doris.proto.FunctionService.PRequestContext.Builder.class);
      }

      // Construct using org.apache.doris.proto.FunctionService.PRequestContext.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getFunctionContextFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        id_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        if (functionContextBuilder_ == null) {
          functionContext_ = null;
        } else {
          functionContextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PRequestContext_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PRequestContext getDefaultInstanceForType() {
        return org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PRequestContext build() {
        org.apache.doris.proto.FunctionService.PRequestContext result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PRequestContext buildPartial() {
        org.apache.doris.proto.FunctionService.PRequestContext result = new org.apache.doris.proto.FunctionService.PRequestContext(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.id_ = id_;
        if (((from_bitField0_ & 0x00000002) != 0)) {
          if (functionContextBuilder_ == null) {
            result.functionContext_ = functionContext_;
          } else {
            result.functionContext_ = functionContextBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.doris.proto.FunctionService.PRequestContext) {
          return mergeFrom((org.apache.doris.proto.FunctionService.PRequestContext)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.FunctionService.PRequestContext other) {
        if (other == org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance()) return this;
        if (other.hasId()) {
          bitField0_ |= 0x00000001;
          id_ = other.id_;
          onChanged();
        }
        if (other.hasFunctionContext()) {
          mergeFunctionContext(other.getFunctionContext());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (hasFunctionContext()) {
          if (!getFunctionContext().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.FunctionService.PRequestContext parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.FunctionService.PRequestContext) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object id_ = "";
      /**
       * <code>optional string id = 1;</code>
       * @return Whether the id field is set.
       */
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional string id = 1;</code>
       * @return The id.
       */
      public java.lang.String getId() {
        java.lang.Object ref = id_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            id_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string id = 1;</code>
       * @return The bytes for id.
       */
      public com.google.protobuf.ByteString
          getIdBytes() {
        java.lang.Object ref = id_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          id_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string id = 1;</code>
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        id_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string id = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = getDefaultInstance().getId();
        onChanged();
        return this;
      }
      /**
       * <code>optional string id = 1;</code>
       * @param value The bytes for id to set.
       * @return This builder for chaining.
       */
      public Builder setIdBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        id_ = value;
        onChanged();
        return this;
      }

      private org.apache.doris.proto.Types.PFunctionContext functionContext_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PFunctionContext, org.apache.doris.proto.Types.PFunctionContext.Builder, org.apache.doris.proto.Types.PFunctionContextOrBuilder> functionContextBuilder_;
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       * @return Whether the functionContext field is set.
       */
      public boolean hasFunctionContext() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       * @return The functionContext.
       */
      public org.apache.doris.proto.Types.PFunctionContext getFunctionContext() {
        if (functionContextBuilder_ == null) {
          return functionContext_ == null ? org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance() : functionContext_;
        } else {
          return functionContextBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public Builder setFunctionContext(org.apache.doris.proto.Types.PFunctionContext value) {
        if (functionContextBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          functionContext_ = value;
          onChanged();
        } else {
          functionContextBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public Builder setFunctionContext(
          org.apache.doris.proto.Types.PFunctionContext.Builder builderForValue) {
        if (functionContextBuilder_ == null) {
          functionContext_ = builderForValue.build();
          onChanged();
        } else {
          functionContextBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public Builder mergeFunctionContext(org.apache.doris.proto.Types.PFunctionContext value) {
        if (functionContextBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0) &&
              functionContext_ != null &&
              functionContext_ != org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance()) {
            functionContext_ =
              org.apache.doris.proto.Types.PFunctionContext.newBuilder(functionContext_).mergeFrom(value).buildPartial();
          } else {
            functionContext_ = value;
          }
          onChanged();
        } else {
          functionContextBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public Builder clearFunctionContext() {
        if (functionContextBuilder_ == null) {
          functionContext_ = null;
          onChanged();
        } else {
          functionContextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public org.apache.doris.proto.Types.PFunctionContext.Builder getFunctionContextBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getFunctionContextFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      public org.apache.doris.proto.Types.PFunctionContextOrBuilder getFunctionContextOrBuilder() {
        if (functionContextBuilder_ != null) {
          return functionContextBuilder_.getMessageOrBuilder();
        } else {
          return functionContext_ == null ?
              org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance() : functionContext_;
        }
      }
      /**
       * <code>optional .doris.PFunctionContext function_context = 2;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PFunctionContext, org.apache.doris.proto.Types.PFunctionContext.Builder, org.apache.doris.proto.Types.PFunctionContextOrBuilder> 
          getFunctionContextFieldBuilder() {
        if (functionContextBuilder_ == null) {
          functionContextBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PFunctionContext, org.apache.doris.proto.Types.PFunctionContext.Builder, org.apache.doris.proto.Types.PFunctionContextOrBuilder>(
                  getFunctionContext(),
                  getParentForChildren(),
                  isClean());
          functionContext_ = null;
        }
        return functionContextBuilder_;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:doris.PRequestContext)
    }

    // @@protoc_insertion_point(class_scope:doris.PRequestContext)
    private static final org.apache.doris.proto.FunctionService.PRequestContext DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.FunctionService.PRequestContext();
    }

    public static org.apache.doris.proto.FunctionService.PRequestContext getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PRequestContext>
        PARSER = new com.google.protobuf.AbstractParser<PRequestContext>() {
      @java.lang.Override
      public PRequestContext parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PRequestContext(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PRequestContext> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PRequestContext> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.FunctionService.PRequestContext getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PFunctionCallRequestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PFunctionCallRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string function_name = 1;</code>
     * @return Whether the functionName field is set.
     */
    boolean hasFunctionName();
    /**
     * <code>optional string function_name = 1;</code>
     * @return The functionName.
     */
    java.lang.String getFunctionName();
    /**
     * <code>optional string function_name = 1;</code>
     * @return The bytes for functionName.
     */
    com.google.protobuf.ByteString
        getFunctionNameBytes();

    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PValues> 
        getArgsList();
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    org.apache.doris.proto.Types.PValues getArgs(int index);
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    int getArgsCount();
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PValuesOrBuilder> 
        getArgsOrBuilderList();
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    org.apache.doris.proto.Types.PValuesOrBuilder getArgsOrBuilder(
        int index);

    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     * @return Whether the context field is set.
     */
    boolean hasContext();
    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     * @return The context.
     */
    org.apache.doris.proto.FunctionService.PRequestContext getContext();
    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     */
    org.apache.doris.proto.FunctionService.PRequestContextOrBuilder getContextOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PFunctionCallRequest}
   */
  public  static final class PFunctionCallRequest extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PFunctionCallRequest)
      PFunctionCallRequestOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PFunctionCallRequest.newBuilder() to construct.
    private PFunctionCallRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PFunctionCallRequest() {
      functionName_ = "";
      args_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PFunctionCallRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PFunctionCallRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000001;
              functionName_ = bs;
              break;
            }
            case 18: {
              if (!((mutable_bitField0_ & 0x00000002) != 0)) {
                args_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValues>();
                mutable_bitField0_ |= 0x00000002;
              }
              args_.add(
                  input.readMessage(org.apache.doris.proto.Types.PValues.PARSER, extensionRegistry));
              break;
            }
            case 26: {
              org.apache.doris.proto.FunctionService.PRequestContext.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = context_.toBuilder();
              }
              context_ = input.readMessage(org.apache.doris.proto.FunctionService.PRequestContext.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(context_);
                context_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        if (((mutable_bitField0_ & 0x00000002) != 0)) {
          args_ = java.util.Collections.unmodifiableList(args_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.FunctionService.PFunctionCallRequest.class, org.apache.doris.proto.FunctionService.PFunctionCallRequest.Builder.class);
    }

    private int bitField0_;
    public static final int FUNCTION_NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object functionName_;
    /**
     * <code>optional string function_name = 1;</code>
     * @return Whether the functionName field is set.
     */
    public boolean hasFunctionName() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string function_name = 1;</code>
     * @return The functionName.
     */
    public java.lang.String getFunctionName() {
      java.lang.Object ref = functionName_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          functionName_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string function_name = 1;</code>
     * @return The bytes for functionName.
     */
    public com.google.protobuf.ByteString
        getFunctionNameBytes() {
      java.lang.Object ref = functionName_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        functionName_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int ARGS_FIELD_NUMBER = 2;
    private java.util.List<org.apache.doris.proto.Types.PValues> args_;
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PValues> getArgsList() {
      return args_;
    }
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PValuesOrBuilder> 
        getArgsOrBuilderList() {
      return args_;
    }
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    public int getArgsCount() {
      return args_.size();
    }
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    public org.apache.doris.proto.Types.PValues getArgs(int index) {
      return args_.get(index);
    }
    /**
     * <code>repeated .doris.PValues args = 2;</code>
     */
    public org.apache.doris.proto.Types.PValuesOrBuilder getArgsOrBuilder(
        int index) {
      return args_.get(index);
    }

    public static final int CONTEXT_FIELD_NUMBER = 3;
    private org.apache.doris.proto.FunctionService.PRequestContext context_;
    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     * @return Whether the context field is set.
     */
    public boolean hasContext() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     * @return The context.
     */
    public org.apache.doris.proto.FunctionService.PRequestContext getContext() {
      return context_ == null ? org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance() : context_;
    }
    /**
     * <code>optional .doris.PRequestContext context = 3;</code>
     */
    public org.apache.doris.proto.FunctionService.PRequestContextOrBuilder getContextOrBuilder() {
      return context_ == null ? org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance() : context_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      for (int i = 0; i < getArgsCount(); i++) {
        if (!getArgs(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasContext()) {
        if (!getContext().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, functionName_);
      }
      for (int i = 0; i < args_.size(); i++) {
        output.writeMessage(2, args_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(3, getContext());
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, functionName_);
      }
      for (int i = 0; i < args_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, args_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, getContext());
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.doris.proto.FunctionService.PFunctionCallRequest)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.FunctionService.PFunctionCallRequest other = (org.apache.doris.proto.FunctionService.PFunctionCallRequest) obj;

      if (hasFunctionName() != other.hasFunctionName()) return false;
      if (hasFunctionName()) {
        if (!getFunctionName()
            .equals(other.getFunctionName())) return false;
      }
      if (!getArgsList()
          .equals(other.getArgsList())) return false;
      if (hasContext() != other.hasContext()) return false;
      if (hasContext()) {
        if (!getContext()
            .equals(other.getContext())) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasFunctionName()) {
        hash = (37 * hash) + FUNCTION_NAME_FIELD_NUMBER;
        hash = (53 * hash) + getFunctionName().hashCode();
      }
      if (getArgsCount() > 0) {
        hash = (37 * hash) + ARGS_FIELD_NUMBER;
        hash = (53 * hash) + getArgsList().hashCode();
      }
      if (hasContext()) {
        hash = (37 * hash) + CONTEXT_FIELD_NUMBER;
        hash = (53 * hash) + getContext().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.doris.proto.FunctionService.PFunctionCallRequest prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code doris.PFunctionCallRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PFunctionCallRequest)
        org.apache.doris.proto.FunctionService.PFunctionCallRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.FunctionService.PFunctionCallRequest.class, org.apache.doris.proto.FunctionService.PFunctionCallRequest.Builder.class);
      }

      // Construct using org.apache.doris.proto.FunctionService.PFunctionCallRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getArgsFieldBuilder();
          getContextFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        functionName_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        if (argsBuilder_ == null) {
          args_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          argsBuilder_.clear();
        }
        if (contextBuilder_ == null) {
          context_ = null;
        } else {
          contextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallRequest getDefaultInstanceForType() {
        return org.apache.doris.proto.FunctionService.PFunctionCallRequest.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallRequest build() {
        org.apache.doris.proto.FunctionService.PFunctionCallRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallRequest buildPartial() {
        org.apache.doris.proto.FunctionService.PFunctionCallRequest result = new org.apache.doris.proto.FunctionService.PFunctionCallRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.functionName_ = functionName_;
        if (argsBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)) {
            args_ = java.util.Collections.unmodifiableList(args_);
            bitField0_ = (bitField0_ & ~0x00000002);
          }
          result.args_ = args_;
        } else {
          result.args_ = argsBuilder_.build();
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          if (contextBuilder_ == null) {
            result.context_ = context_;
          } else {
            result.context_ = contextBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.doris.proto.FunctionService.PFunctionCallRequest) {
          return mergeFrom((org.apache.doris.proto.FunctionService.PFunctionCallRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.FunctionService.PFunctionCallRequest other) {
        if (other == org.apache.doris.proto.FunctionService.PFunctionCallRequest.getDefaultInstance()) return this;
        if (other.hasFunctionName()) {
          bitField0_ |= 0x00000001;
          functionName_ = other.functionName_;
          onChanged();
        }
        if (argsBuilder_ == null) {
          if (!other.args_.isEmpty()) {
            if (args_.isEmpty()) {
              args_ = other.args_;
              bitField0_ = (bitField0_ & ~0x00000002);
            } else {
              ensureArgsIsMutable();
              args_.addAll(other.args_);
            }
            onChanged();
          }
        } else {
          if (!other.args_.isEmpty()) {
            if (argsBuilder_.isEmpty()) {
              argsBuilder_.dispose();
              argsBuilder_ = null;
              args_ = other.args_;
              bitField0_ = (bitField0_ & ~0x00000002);
              argsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getArgsFieldBuilder() : null;
            } else {
              argsBuilder_.addAllMessages(other.args_);
            }
          }
        }
        if (other.hasContext()) {
          mergeContext(other.getContext());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        for (int i = 0; i < getArgsCount(); i++) {
          if (!getArgs(i).isInitialized()) {
            return false;
          }
        }
        if (hasContext()) {
          if (!getContext().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.FunctionService.PFunctionCallRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.FunctionService.PFunctionCallRequest) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object functionName_ = "";
      /**
       * <code>optional string function_name = 1;</code>
       * @return Whether the functionName field is set.
       */
      public boolean hasFunctionName() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional string function_name = 1;</code>
       * @return The functionName.
       */
      public java.lang.String getFunctionName() {
        java.lang.Object ref = functionName_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            functionName_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string function_name = 1;</code>
       * @return The bytes for functionName.
       */
      public com.google.protobuf.ByteString
          getFunctionNameBytes() {
        java.lang.Object ref = functionName_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          functionName_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string function_name = 1;</code>
       * @param value The functionName to set.
       * @return This builder for chaining.
       */
      public Builder setFunctionName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        functionName_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string function_name = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearFunctionName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        functionName_ = getDefaultInstance().getFunctionName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string function_name = 1;</code>
       * @param value The bytes for functionName to set.
       * @return This builder for chaining.
       */
      public Builder setFunctionNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        functionName_ = value;
        onChanged();
        return this;
      }

      private java.util.List<org.apache.doris.proto.Types.PValues> args_ =
        java.util.Collections.emptyList();
      private void ensureArgsIsMutable() {
        if (!((bitField0_ & 0x00000002) != 0)) {
          args_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValues>(args_);
          bitField0_ |= 0x00000002;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder> argsBuilder_;

      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValues> getArgsList() {
        if (argsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(args_);
        } else {
          return argsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public int getArgsCount() {
        if (argsBuilder_ == null) {
          return args_.size();
        } else {
          return argsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public org.apache.doris.proto.Types.PValues getArgs(int index) {
        if (argsBuilder_ == null) {
          return args_.get(index);
        } else {
          return argsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder setArgs(
          int index, org.apache.doris.proto.Types.PValues value) {
        if (argsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureArgsIsMutable();
          args_.set(index, value);
          onChanged();
        } else {
          argsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder setArgs(
          int index, org.apache.doris.proto.Types.PValues.Builder builderForValue) {
        if (argsBuilder_ == null) {
          ensureArgsIsMutable();
          args_.set(index, builderForValue.build());
          onChanged();
        } else {
          argsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder addArgs(org.apache.doris.proto.Types.PValues value) {
        if (argsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureArgsIsMutable();
          args_.add(value);
          onChanged();
        } else {
          argsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder addArgs(
          int index, org.apache.doris.proto.Types.PValues value) {
        if (argsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureArgsIsMutable();
          args_.add(index, value);
          onChanged();
        } else {
          argsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder addArgs(
          org.apache.doris.proto.Types.PValues.Builder builderForValue) {
        if (argsBuilder_ == null) {
          ensureArgsIsMutable();
          args_.add(builderForValue.build());
          onChanged();
        } else {
          argsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder addArgs(
          int index, org.apache.doris.proto.Types.PValues.Builder builderForValue) {
        if (argsBuilder_ == null) {
          ensureArgsIsMutable();
          args_.add(index, builderForValue.build());
          onChanged();
        } else {
          argsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder addAllArgs(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PValues> values) {
        if (argsBuilder_ == null) {
          ensureArgsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, args_);
          onChanged();
        } else {
          argsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder clearArgs() {
        if (argsBuilder_ == null) {
          args_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
          onChanged();
        } else {
          argsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public Builder removeArgs(int index) {
        if (argsBuilder_ == null) {
          ensureArgsIsMutable();
          args_.remove(index);
          onChanged();
        } else {
          argsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public org.apache.doris.proto.Types.PValues.Builder getArgsBuilder(
          int index) {
        return getArgsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public org.apache.doris.proto.Types.PValuesOrBuilder getArgsOrBuilder(
          int index) {
        if (argsBuilder_ == null) {
          return args_.get(index);  } else {
          return argsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PValuesOrBuilder> 
           getArgsOrBuilderList() {
        if (argsBuilder_ != null) {
          return argsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(args_);
        }
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public org.apache.doris.proto.Types.PValues.Builder addArgsBuilder() {
        return getArgsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PValues.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public org.apache.doris.proto.Types.PValues.Builder addArgsBuilder(
          int index) {
        return getArgsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PValues.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValues args = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValues.Builder> 
           getArgsBuilderList() {
        return getArgsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder> 
          getArgsFieldBuilder() {
        if (argsBuilder_ == null) {
          argsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder>(
                  args_,
                  ((bitField0_ & 0x00000002) != 0),
                  getParentForChildren(),
                  isClean());
          args_ = null;
        }
        return argsBuilder_;
      }

      private org.apache.doris.proto.FunctionService.PRequestContext context_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.FunctionService.PRequestContext, org.apache.doris.proto.FunctionService.PRequestContext.Builder, org.apache.doris.proto.FunctionService.PRequestContextOrBuilder> contextBuilder_;
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       * @return Whether the context field is set.
       */
      public boolean hasContext() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       * @return The context.
       */
      public org.apache.doris.proto.FunctionService.PRequestContext getContext() {
        if (contextBuilder_ == null) {
          return context_ == null ? org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance() : context_;
        } else {
          return contextBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public Builder setContext(org.apache.doris.proto.FunctionService.PRequestContext value) {
        if (contextBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          context_ = value;
          onChanged();
        } else {
          contextBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public Builder setContext(
          org.apache.doris.proto.FunctionService.PRequestContext.Builder builderForValue) {
        if (contextBuilder_ == null) {
          context_ = builderForValue.build();
          onChanged();
        } else {
          contextBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public Builder mergeContext(org.apache.doris.proto.FunctionService.PRequestContext value) {
        if (contextBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0) &&
              context_ != null &&
              context_ != org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance()) {
            context_ =
              org.apache.doris.proto.FunctionService.PRequestContext.newBuilder(context_).mergeFrom(value).buildPartial();
          } else {
            context_ = value;
          }
          onChanged();
        } else {
          contextBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public Builder clearContext() {
        if (contextBuilder_ == null) {
          context_ = null;
          onChanged();
        } else {
          contextBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public org.apache.doris.proto.FunctionService.PRequestContext.Builder getContextBuilder() {
        bitField0_ |= 0x00000004;
        onChanged();
        return getContextFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      public org.apache.doris.proto.FunctionService.PRequestContextOrBuilder getContextOrBuilder() {
        if (contextBuilder_ != null) {
          return contextBuilder_.getMessageOrBuilder();
        } else {
          return context_ == null ?
              org.apache.doris.proto.FunctionService.PRequestContext.getDefaultInstance() : context_;
        }
      }
      /**
       * <code>optional .doris.PRequestContext context = 3;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.FunctionService.PRequestContext, org.apache.doris.proto.FunctionService.PRequestContext.Builder, org.apache.doris.proto.FunctionService.PRequestContextOrBuilder> 
          getContextFieldBuilder() {
        if (contextBuilder_ == null) {
          contextBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.FunctionService.PRequestContext, org.apache.doris.proto.FunctionService.PRequestContext.Builder, org.apache.doris.proto.FunctionService.PRequestContextOrBuilder>(
                  getContext(),
                  getParentForChildren(),
                  isClean());
          context_ = null;
        }
        return contextBuilder_;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:doris.PFunctionCallRequest)
    }

    // @@protoc_insertion_point(class_scope:doris.PFunctionCallRequest)
    private static final org.apache.doris.proto.FunctionService.PFunctionCallRequest DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.FunctionService.PFunctionCallRequest();
    }

    public static org.apache.doris.proto.FunctionService.PFunctionCallRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PFunctionCallRequest>
        PARSER = new com.google.protobuf.AbstractParser<PFunctionCallRequest>() {
      @java.lang.Override
      public PFunctionCallRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PFunctionCallRequest(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PFunctionCallRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PFunctionCallRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.FunctionService.PFunctionCallRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PFunctionCallResponseOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PFunctionCallResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional .doris.PValues result = 1;</code>
     * @return Whether the result field is set.
     */
    boolean hasResult();
    /**
     * <code>optional .doris.PValues result = 1;</code>
     * @return The result.
     */
    org.apache.doris.proto.Types.PValues getResult();
    /**
     * <code>optional .doris.PValues result = 1;</code>
     */
    org.apache.doris.proto.Types.PValuesOrBuilder getResultOrBuilder();

    /**
     * <code>optional .doris.PStatus status = 2;</code>
     * @return Whether the status field is set.
     */
    boolean hasStatus();
    /**
     * <code>optional .doris.PStatus status = 2;</code>
     * @return The status.
     */
    org.apache.doris.proto.Types.PStatus getStatus();
    /**
     * <code>optional .doris.PStatus status = 2;</code>
     */
    org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PFunctionCallResponse}
   */
  public  static final class PFunctionCallResponse extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PFunctionCallResponse)
      PFunctionCallResponseOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PFunctionCallResponse.newBuilder() to construct.
    private PFunctionCallResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PFunctionCallResponse() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PFunctionCallResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PFunctionCallResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              org.apache.doris.proto.Types.PValues.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = result_.toBuilder();
              }
              result_ = input.readMessage(org.apache.doris.proto.Types.PValues.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(result_);
                result_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 18: {
              org.apache.doris.proto.Types.PStatus.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = status_.toBuilder();
              }
              status_ = input.readMessage(org.apache.doris.proto.Types.PStatus.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(status_);
                status_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.FunctionService.PFunctionCallResponse.class, org.apache.doris.proto.FunctionService.PFunctionCallResponse.Builder.class);
    }

    private int bitField0_;
    public static final int RESULT_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PValues result_;
    /**
     * <code>optional .doris.PValues result = 1;</code>
     * @return Whether the result field is set.
     */
    public boolean hasResult() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional .doris.PValues result = 1;</code>
     * @return The result.
     */
    public org.apache.doris.proto.Types.PValues getResult() {
      return result_ == null ? org.apache.doris.proto.Types.PValues.getDefaultInstance() : result_;
    }
    /**
     * <code>optional .doris.PValues result = 1;</code>
     */
    public org.apache.doris.proto.Types.PValuesOrBuilder getResultOrBuilder() {
      return result_ == null ? org.apache.doris.proto.Types.PValues.getDefaultInstance() : result_;
    }

    public static final int STATUS_FIELD_NUMBER = 2;
    private org.apache.doris.proto.Types.PStatus status_;
    /**
     * <code>optional .doris.PStatus status = 2;</code>
     * @return Whether the status field is set.
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PStatus status = 2;</code>
     * @return The status.
     */
    public org.apache.doris.proto.Types.PStatus getStatus() {
      return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
    }
    /**
     * <code>optional .doris.PStatus status = 2;</code>
     */
    public org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder() {
      return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (hasResult()) {
        if (!getResult().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasStatus()) {
        if (!getStatus().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getResult());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(2, getStatus());
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, getResult());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, getStatus());
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.doris.proto.FunctionService.PFunctionCallResponse)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.FunctionService.PFunctionCallResponse other = (org.apache.doris.proto.FunctionService.PFunctionCallResponse) obj;

      if (hasResult() != other.hasResult()) return false;
      if (hasResult()) {
        if (!getResult()
            .equals(other.getResult())) return false;
      }
      if (hasStatus() != other.hasStatus()) return false;
      if (hasStatus()) {
        if (!getStatus()
            .equals(other.getStatus())) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasResult()) {
        hash = (37 * hash) + RESULT_FIELD_NUMBER;
        hash = (53 * hash) + getResult().hashCode();
      }
      if (hasStatus()) {
        hash = (37 * hash) + STATUS_FIELD_NUMBER;
        hash = (53 * hash) + getStatus().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.doris.proto.FunctionService.PFunctionCallResponse prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code doris.PFunctionCallResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PFunctionCallResponse)
        org.apache.doris.proto.FunctionService.PFunctionCallResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.FunctionService.PFunctionCallResponse.class, org.apache.doris.proto.FunctionService.PFunctionCallResponse.Builder.class);
      }

      // Construct using org.apache.doris.proto.FunctionService.PFunctionCallResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getResultFieldBuilder();
          getStatusFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (resultBuilder_ == null) {
          result_ = null;
        } else {
          resultBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        if (statusBuilder_ == null) {
          status_ = null;
        } else {
          statusBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PFunctionCallResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallResponse getDefaultInstanceForType() {
        return org.apache.doris.proto.FunctionService.PFunctionCallResponse.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallResponse build() {
        org.apache.doris.proto.FunctionService.PFunctionCallResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PFunctionCallResponse buildPartial() {
        org.apache.doris.proto.FunctionService.PFunctionCallResponse result = new org.apache.doris.proto.FunctionService.PFunctionCallResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (resultBuilder_ == null) {
            result.result_ = result_;
          } else {
            result.result_ = resultBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          if (statusBuilder_ == null) {
            result.status_ = status_;
          } else {
            result.status_ = statusBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.doris.proto.FunctionService.PFunctionCallResponse) {
          return mergeFrom((org.apache.doris.proto.FunctionService.PFunctionCallResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.FunctionService.PFunctionCallResponse other) {
        if (other == org.apache.doris.proto.FunctionService.PFunctionCallResponse.getDefaultInstance()) return this;
        if (other.hasResult()) {
          mergeResult(other.getResult());
        }
        if (other.hasStatus()) {
          mergeStatus(other.getStatus());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (hasResult()) {
          if (!getResult().isInitialized()) {
            return false;
          }
        }
        if (hasStatus()) {
          if (!getStatus().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.FunctionService.PFunctionCallResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.FunctionService.PFunctionCallResponse) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PValues result_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder> resultBuilder_;
      /**
       * <code>optional .doris.PValues result = 1;</code>
       * @return Whether the result field is set.
       */
      public boolean hasResult() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       * @return The result.
       */
      public org.apache.doris.proto.Types.PValues getResult() {
        if (resultBuilder_ == null) {
          return result_ == null ? org.apache.doris.proto.Types.PValues.getDefaultInstance() : result_;
        } else {
          return resultBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public Builder setResult(org.apache.doris.proto.Types.PValues value) {
        if (resultBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          result_ = value;
          onChanged();
        } else {
          resultBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public Builder setResult(
          org.apache.doris.proto.Types.PValues.Builder builderForValue) {
        if (resultBuilder_ == null) {
          result_ = builderForValue.build();
          onChanged();
        } else {
          resultBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public Builder mergeResult(org.apache.doris.proto.Types.PValues value) {
        if (resultBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              result_ != null &&
              result_ != org.apache.doris.proto.Types.PValues.getDefaultInstance()) {
            result_ =
              org.apache.doris.proto.Types.PValues.newBuilder(result_).mergeFrom(value).buildPartial();
          } else {
            result_ = value;
          }
          onChanged();
        } else {
          resultBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public Builder clearResult() {
        if (resultBuilder_ == null) {
          result_ = null;
          onChanged();
        } else {
          resultBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public org.apache.doris.proto.Types.PValues.Builder getResultBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getResultFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      public org.apache.doris.proto.Types.PValuesOrBuilder getResultOrBuilder() {
        if (resultBuilder_ != null) {
          return resultBuilder_.getMessageOrBuilder();
        } else {
          return result_ == null ?
              org.apache.doris.proto.Types.PValues.getDefaultInstance() : result_;
        }
      }
      /**
       * <code>optional .doris.PValues result = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder> 
          getResultFieldBuilder() {
        if (resultBuilder_ == null) {
          resultBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PValues, org.apache.doris.proto.Types.PValues.Builder, org.apache.doris.proto.Types.PValuesOrBuilder>(
                  getResult(),
                  getParentForChildren(),
                  isClean());
          result_ = null;
        }
        return resultBuilder_;
      }

      private org.apache.doris.proto.Types.PStatus status_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder> statusBuilder_;
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       * @return Whether the status field is set.
       */
      public boolean hasStatus() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       * @return The status.
       */
      public org.apache.doris.proto.Types.PStatus getStatus() {
        if (statusBuilder_ == null) {
          return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
        } else {
          return statusBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public Builder setStatus(org.apache.doris.proto.Types.PStatus value) {
        if (statusBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          status_ = value;
          onChanged();
        } else {
          statusBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public Builder setStatus(
          org.apache.doris.proto.Types.PStatus.Builder builderForValue) {
        if (statusBuilder_ == null) {
          status_ = builderForValue.build();
          onChanged();
        } else {
          statusBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public Builder mergeStatus(org.apache.doris.proto.Types.PStatus value) {
        if (statusBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0) &&
              status_ != null &&
              status_ != org.apache.doris.proto.Types.PStatus.getDefaultInstance()) {
            status_ =
              org.apache.doris.proto.Types.PStatus.newBuilder(status_).mergeFrom(value).buildPartial();
          } else {
            status_ = value;
          }
          onChanged();
        } else {
          statusBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public Builder clearStatus() {
        if (statusBuilder_ == null) {
          status_ = null;
          onChanged();
        } else {
          statusBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public org.apache.doris.proto.Types.PStatus.Builder getStatusBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getStatusFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      public org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder() {
        if (statusBuilder_ != null) {
          return statusBuilder_.getMessageOrBuilder();
        } else {
          return status_ == null ?
              org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
        }
      }
      /**
       * <code>optional .doris.PStatus status = 2;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder> 
          getStatusFieldBuilder() {
        if (statusBuilder_ == null) {
          statusBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder>(
                  getStatus(),
                  getParentForChildren(),
                  isClean());
          status_ = null;
        }
        return statusBuilder_;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:doris.PFunctionCallResponse)
    }

    // @@protoc_insertion_point(class_scope:doris.PFunctionCallResponse)
    private static final org.apache.doris.proto.FunctionService.PFunctionCallResponse DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.FunctionService.PFunctionCallResponse();
    }

    public static org.apache.doris.proto.FunctionService.PFunctionCallResponse getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PFunctionCallResponse>
        PARSER = new com.google.protobuf.AbstractParser<PFunctionCallResponse>() {
      @java.lang.Override
      public PFunctionCallResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PFunctionCallResponse(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PFunctionCallResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PFunctionCallResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.FunctionService.PFunctionCallResponse getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PCheckFunctionRequestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PCheckFunctionRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional .doris.PFunction function = 1;</code>
     * @return Whether the function field is set.
     */
    boolean hasFunction();
    /**
     * <code>optional .doris.PFunction function = 1;</code>
     * @return The function.
     */
    org.apache.doris.proto.Types.PFunction getFunction();
    /**
     * <code>optional .doris.PFunction function = 1;</code>
     */
    org.apache.doris.proto.Types.PFunctionOrBuilder getFunctionOrBuilder();

    /**
     * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
     * @return Whether the matchType field is set.
     */
    boolean hasMatchType();
    /**
     * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
     * @return The matchType.
     */
    org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType getMatchType();
  }
  /**
   * Protobuf type {@code doris.PCheckFunctionRequest}
   */
  public  static final class PCheckFunctionRequest extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PCheckFunctionRequest)
      PCheckFunctionRequestOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PCheckFunctionRequest.newBuilder() to construct.
    private PCheckFunctionRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PCheckFunctionRequest() {
      matchType_ = 0;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PCheckFunctionRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PCheckFunctionRequest(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              org.apache.doris.proto.Types.PFunction.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = function_.toBuilder();
              }
              function_ = input.readMessage(org.apache.doris.proto.Types.PFunction.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(function_);
                function_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 16: {
              int rawValue = input.readEnum();
                @SuppressWarnings("deprecation")
              org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType value = org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                bitField0_ |= 0x00000002;
                matchType_ = rawValue;
              }
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.FunctionService.PCheckFunctionRequest.class, org.apache.doris.proto.FunctionService.PCheckFunctionRequest.Builder.class);
    }

    /**
     * Protobuf enum {@code doris.PCheckFunctionRequest.MatchType}
     */
    public enum MatchType
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>IDENTICAL = 0;</code>
       */
      IDENTICAL(0),
      /**
       * <code>INDISTINGUISHABLE = 1;</code>
       */
      INDISTINGUISHABLE(1),
      /**
       * <code>SUPERTYPE_OF = 2;</code>
       */
      SUPERTYPE_OF(2),
      /**
       * <code>NONSTRICT_SUPERTYPE_OF = 3;</code>
       */
      NONSTRICT_SUPERTYPE_OF(3),
      /**
       * <code>MATCHABLE = 4;</code>
       */
      MATCHABLE(4),
      ;

      /**
       * <code>IDENTICAL = 0;</code>
       */
      public static final int IDENTICAL_VALUE = 0;
      /**
       * <code>INDISTINGUISHABLE = 1;</code>
       */
      public static final int INDISTINGUISHABLE_VALUE = 1;
      /**
       * <code>SUPERTYPE_OF = 2;</code>
       */
      public static final int SUPERTYPE_OF_VALUE = 2;
      /**
       * <code>NONSTRICT_SUPERTYPE_OF = 3;</code>
       */
      public static final int NONSTRICT_SUPERTYPE_OF_VALUE = 3;
      /**
       * <code>MATCHABLE = 4;</code>
       */
      public static final int MATCHABLE_VALUE = 4;


      public final int getNumber() {
        return value;
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       * @deprecated Use {@link #forNumber(int)} instead.
       */
      @java.lang.Deprecated
      public static MatchType valueOf(int value) {
        return forNumber(value);
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       */
      public static MatchType forNumber(int value) {
        switch (value) {
          case 0: return IDENTICAL;
          case 1: return INDISTINGUISHABLE;
          case 2: return SUPERTYPE_OF;
          case 3: return NONSTRICT_SUPERTYPE_OF;
          case 4: return MATCHABLE;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<MatchType>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static final com.google.protobuf.Internal.EnumLiteMap<
          MatchType> internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<MatchType>() {
              public MatchType findValueByNumber(int number) {
                return MatchType.forNumber(number);
              }
            };

      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(ordinal());
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.PCheckFunctionRequest.getDescriptor().getEnumTypes().get(0);
      }

      private static final MatchType[] VALUES = values();

      public static MatchType valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int value;

      private MatchType(int value) {
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:doris.PCheckFunctionRequest.MatchType)
    }

    private int bitField0_;
    public static final int FUNCTION_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PFunction function_;
    /**
     * <code>optional .doris.PFunction function = 1;</code>
     * @return Whether the function field is set.
     */
    public boolean hasFunction() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional .doris.PFunction function = 1;</code>
     * @return The function.
     */
    public org.apache.doris.proto.Types.PFunction getFunction() {
      return function_ == null ? org.apache.doris.proto.Types.PFunction.getDefaultInstance() : function_;
    }
    /**
     * <code>optional .doris.PFunction function = 1;</code>
     */
    public org.apache.doris.proto.Types.PFunctionOrBuilder getFunctionOrBuilder() {
      return function_ == null ? org.apache.doris.proto.Types.PFunction.getDefaultInstance() : function_;
    }

    public static final int MATCH_TYPE_FIELD_NUMBER = 2;
    private int matchType_;
    /**
     * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
     * @return Whether the matchType field is set.
     */
    public boolean hasMatchType() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
     * @return The matchType.
     */
    public org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType getMatchType() {
      @SuppressWarnings("deprecation")
      org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType result = org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType.valueOf(matchType_);
      return result == null ? org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType.IDENTICAL : result;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (hasFunction()) {
        if (!getFunction().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getFunction());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeEnum(2, matchType_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, getFunction());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(2, matchType_);
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.doris.proto.FunctionService.PCheckFunctionRequest)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.FunctionService.PCheckFunctionRequest other = (org.apache.doris.proto.FunctionService.PCheckFunctionRequest) obj;

      if (hasFunction() != other.hasFunction()) return false;
      if (hasFunction()) {
        if (!getFunction()
            .equals(other.getFunction())) return false;
      }
      if (hasMatchType() != other.hasMatchType()) return false;
      if (hasMatchType()) {
        if (matchType_ != other.matchType_) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasFunction()) {
        hash = (37 * hash) + FUNCTION_FIELD_NUMBER;
        hash = (53 * hash) + getFunction().hashCode();
      }
      if (hasMatchType()) {
        hash = (37 * hash) + MATCH_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + matchType_;
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.doris.proto.FunctionService.PCheckFunctionRequest prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code doris.PCheckFunctionRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PCheckFunctionRequest)
        org.apache.doris.proto.FunctionService.PCheckFunctionRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.FunctionService.PCheckFunctionRequest.class, org.apache.doris.proto.FunctionService.PCheckFunctionRequest.Builder.class);
      }

      // Construct using org.apache.doris.proto.FunctionService.PCheckFunctionRequest.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getFunctionFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (functionBuilder_ == null) {
          function_ = null;
        } else {
          functionBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        matchType_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionRequest getDefaultInstanceForType() {
        return org.apache.doris.proto.FunctionService.PCheckFunctionRequest.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionRequest build() {
        org.apache.doris.proto.FunctionService.PCheckFunctionRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionRequest buildPartial() {
        org.apache.doris.proto.FunctionService.PCheckFunctionRequest result = new org.apache.doris.proto.FunctionService.PCheckFunctionRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (functionBuilder_ == null) {
            result.function_ = function_;
          } else {
            result.function_ = functionBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          to_bitField0_ |= 0x00000002;
        }
        result.matchType_ = matchType_;
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.doris.proto.FunctionService.PCheckFunctionRequest) {
          return mergeFrom((org.apache.doris.proto.FunctionService.PCheckFunctionRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.FunctionService.PCheckFunctionRequest other) {
        if (other == org.apache.doris.proto.FunctionService.PCheckFunctionRequest.getDefaultInstance()) return this;
        if (other.hasFunction()) {
          mergeFunction(other.getFunction());
        }
        if (other.hasMatchType()) {
          setMatchType(other.getMatchType());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (hasFunction()) {
          if (!getFunction().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.FunctionService.PCheckFunctionRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.FunctionService.PCheckFunctionRequest) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PFunction function_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PFunction, org.apache.doris.proto.Types.PFunction.Builder, org.apache.doris.proto.Types.PFunctionOrBuilder> functionBuilder_;
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       * @return Whether the function field is set.
       */
      public boolean hasFunction() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       * @return The function.
       */
      public org.apache.doris.proto.Types.PFunction getFunction() {
        if (functionBuilder_ == null) {
          return function_ == null ? org.apache.doris.proto.Types.PFunction.getDefaultInstance() : function_;
        } else {
          return functionBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public Builder setFunction(org.apache.doris.proto.Types.PFunction value) {
        if (functionBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          function_ = value;
          onChanged();
        } else {
          functionBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public Builder setFunction(
          org.apache.doris.proto.Types.PFunction.Builder builderForValue) {
        if (functionBuilder_ == null) {
          function_ = builderForValue.build();
          onChanged();
        } else {
          functionBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public Builder mergeFunction(org.apache.doris.proto.Types.PFunction value) {
        if (functionBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              function_ != null &&
              function_ != org.apache.doris.proto.Types.PFunction.getDefaultInstance()) {
            function_ =
              org.apache.doris.proto.Types.PFunction.newBuilder(function_).mergeFrom(value).buildPartial();
          } else {
            function_ = value;
          }
          onChanged();
        } else {
          functionBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public Builder clearFunction() {
        if (functionBuilder_ == null) {
          function_ = null;
          onChanged();
        } else {
          functionBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public org.apache.doris.proto.Types.PFunction.Builder getFunctionBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getFunctionFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      public org.apache.doris.proto.Types.PFunctionOrBuilder getFunctionOrBuilder() {
        if (functionBuilder_ != null) {
          return functionBuilder_.getMessageOrBuilder();
        } else {
          return function_ == null ?
              org.apache.doris.proto.Types.PFunction.getDefaultInstance() : function_;
        }
      }
      /**
       * <code>optional .doris.PFunction function = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PFunction, org.apache.doris.proto.Types.PFunction.Builder, org.apache.doris.proto.Types.PFunctionOrBuilder> 
          getFunctionFieldBuilder() {
        if (functionBuilder_ == null) {
          functionBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PFunction, org.apache.doris.proto.Types.PFunction.Builder, org.apache.doris.proto.Types.PFunctionOrBuilder>(
                  getFunction(),
                  getParentForChildren(),
                  isClean());
          function_ = null;
        }
        return functionBuilder_;
      }

      private int matchType_ = 0;
      /**
       * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
       * @return Whether the matchType field is set.
       */
      public boolean hasMatchType() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
       * @return The matchType.
       */
      public org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType getMatchType() {
        @SuppressWarnings("deprecation")
        org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType result = org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType.valueOf(matchType_);
        return result == null ? org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType.IDENTICAL : result;
      }
      /**
       * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
       * @param value The matchType to set.
       * @return This builder for chaining.
       */
      public Builder setMatchType(org.apache.doris.proto.FunctionService.PCheckFunctionRequest.MatchType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000002;
        matchType_ = value.getNumber();
        onChanged();
        return this;
      }
      /**
       * <code>optional .doris.PCheckFunctionRequest.MatchType match_type = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearMatchType() {
        bitField0_ = (bitField0_ & ~0x00000002);
        matchType_ = 0;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:doris.PCheckFunctionRequest)
    }

    // @@protoc_insertion_point(class_scope:doris.PCheckFunctionRequest)
    private static final org.apache.doris.proto.FunctionService.PCheckFunctionRequest DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.FunctionService.PCheckFunctionRequest();
    }

    public static org.apache.doris.proto.FunctionService.PCheckFunctionRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PCheckFunctionRequest>
        PARSER = new com.google.protobuf.AbstractParser<PCheckFunctionRequest>() {
      @java.lang.Override
      public PCheckFunctionRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PCheckFunctionRequest(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PCheckFunctionRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PCheckFunctionRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.FunctionService.PCheckFunctionRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PCheckFunctionResponseOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PCheckFunctionResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional .doris.PStatus status = 1;</code>
     * @return Whether the status field is set.
     */
    boolean hasStatus();
    /**
     * <code>optional .doris.PStatus status = 1;</code>
     * @return The status.
     */
    org.apache.doris.proto.Types.PStatus getStatus();
    /**
     * <code>optional .doris.PStatus status = 1;</code>
     */
    org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PCheckFunctionResponse}
   */
  public  static final class PCheckFunctionResponse extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PCheckFunctionResponse)
      PCheckFunctionResponseOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PCheckFunctionResponse.newBuilder() to construct.
    private PCheckFunctionResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PCheckFunctionResponse() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PCheckFunctionResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PCheckFunctionResponse(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      this();
      if (extensionRegistry == null) {
        throw new java.lang.NullPointerException();
      }
      int mutable_bitField0_ = 0;
      com.google.protobuf.UnknownFieldSet.Builder unknownFields =
          com.google.protobuf.UnknownFieldSet.newBuilder();
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              org.apache.doris.proto.Types.PStatus.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = status_.toBuilder();
              }
              status_ = input.readMessage(org.apache.doris.proto.Types.PStatus.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(status_);
                status_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            default: {
              if (!parseUnknownField(
                  input, unknownFields, extensionRegistry, tag)) {
                done = true;
              }
              break;
            }
          }
        }
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(this);
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(
            e).setUnfinishedMessage(this);
      } finally {
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.FunctionService.PCheckFunctionResponse.class, org.apache.doris.proto.FunctionService.PCheckFunctionResponse.Builder.class);
    }

    private int bitField0_;
    public static final int STATUS_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PStatus status_;
    /**
     * <code>optional .doris.PStatus status = 1;</code>
     * @return Whether the status field is set.
     */
    public boolean hasStatus() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional .doris.PStatus status = 1;</code>
     * @return The status.
     */
    public org.apache.doris.proto.Types.PStatus getStatus() {
      return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
    }
    /**
     * <code>optional .doris.PStatus status = 1;</code>
     */
    public org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder() {
      return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (hasStatus()) {
        if (!getStatus().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getStatus());
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, getStatus());
      }
      size += unknownFields.getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof org.apache.doris.proto.FunctionService.PCheckFunctionResponse)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.FunctionService.PCheckFunctionResponse other = (org.apache.doris.proto.FunctionService.PCheckFunctionResponse) obj;

      if (hasStatus() != other.hasStatus()) return false;
      if (hasStatus()) {
        if (!getStatus()
            .equals(other.getStatus())) return false;
      }
      if (!unknownFields.equals(other.unknownFields)) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasStatus()) {
        hash = (37 * hash) + STATUS_FIELD_NUMBER;
        hash = (53 * hash) + getStatus().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(org.apache.doris.proto.FunctionService.PCheckFunctionResponse prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code doris.PCheckFunctionResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PCheckFunctionResponse)
        org.apache.doris.proto.FunctionService.PCheckFunctionResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.FunctionService.PCheckFunctionResponse.class, org.apache.doris.proto.FunctionService.PCheckFunctionResponse.Builder.class);
      }

      // Construct using org.apache.doris.proto.FunctionService.PCheckFunctionResponse.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessageV3
                .alwaysUseFieldBuilders) {
          getStatusFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (statusBuilder_ == null) {
          status_ = null;
        } else {
          statusBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.FunctionService.internal_static_doris_PCheckFunctionResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionResponse getDefaultInstanceForType() {
        return org.apache.doris.proto.FunctionService.PCheckFunctionResponse.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionResponse build() {
        org.apache.doris.proto.FunctionService.PCheckFunctionResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.FunctionService.PCheckFunctionResponse buildPartial() {
        org.apache.doris.proto.FunctionService.PCheckFunctionResponse result = new org.apache.doris.proto.FunctionService.PCheckFunctionResponse(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (statusBuilder_ == null) {
            result.status_ = status_;
          } else {
            result.status_ = statusBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        result.bitField0_ = to_bitField0_;
        onBuilt();
        return result;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.doris.proto.FunctionService.PCheckFunctionResponse) {
          return mergeFrom((org.apache.doris.proto.FunctionService.PCheckFunctionResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.FunctionService.PCheckFunctionResponse other) {
        if (other == org.apache.doris.proto.FunctionService.PCheckFunctionResponse.getDefaultInstance()) return this;
        if (other.hasStatus()) {
          mergeStatus(other.getStatus());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (hasStatus()) {
          if (!getStatus().isInitialized()) {
            return false;
          }
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.FunctionService.PCheckFunctionResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.FunctionService.PCheckFunctionResponse) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PStatus status_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder> statusBuilder_;
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       * @return Whether the status field is set.
       */
      public boolean hasStatus() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       * @return The status.
       */
      public org.apache.doris.proto.Types.PStatus getStatus() {
        if (statusBuilder_ == null) {
          return status_ == null ? org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
        } else {
          return statusBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public Builder setStatus(org.apache.doris.proto.Types.PStatus value) {
        if (statusBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          status_ = value;
          onChanged();
        } else {
          statusBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public Builder setStatus(
          org.apache.doris.proto.Types.PStatus.Builder builderForValue) {
        if (statusBuilder_ == null) {
          status_ = builderForValue.build();
          onChanged();
        } else {
          statusBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public Builder mergeStatus(org.apache.doris.proto.Types.PStatus value) {
        if (statusBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              status_ != null &&
              status_ != org.apache.doris.proto.Types.PStatus.getDefaultInstance()) {
            status_ =
              org.apache.doris.proto.Types.PStatus.newBuilder(status_).mergeFrom(value).buildPartial();
          } else {
            status_ = value;
          }
          onChanged();
        } else {
          statusBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public Builder clearStatus() {
        if (statusBuilder_ == null) {
          status_ = null;
          onChanged();
        } else {
          statusBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public org.apache.doris.proto.Types.PStatus.Builder getStatusBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getStatusFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      public org.apache.doris.proto.Types.PStatusOrBuilder getStatusOrBuilder() {
        if (statusBuilder_ != null) {
          return statusBuilder_.getMessageOrBuilder();
        } else {
          return status_ == null ?
              org.apache.doris.proto.Types.PStatus.getDefaultInstance() : status_;
        }
      }
      /**
       * <code>optional .doris.PStatus status = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder> 
          getStatusFieldBuilder() {
        if (statusBuilder_ == null) {
          statusBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PStatus, org.apache.doris.proto.Types.PStatus.Builder, org.apache.doris.proto.Types.PStatusOrBuilder>(
                  getStatus(),
                  getParentForChildren(),
                  isClean());
          status_ = null;
        }
        return statusBuilder_;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:doris.PCheckFunctionResponse)
    }

    // @@protoc_insertion_point(class_scope:doris.PCheckFunctionResponse)
    private static final org.apache.doris.proto.FunctionService.PCheckFunctionResponse DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.FunctionService.PCheckFunctionResponse();
    }

    public static org.apache.doris.proto.FunctionService.PCheckFunctionResponse getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PCheckFunctionResponse>
        PARSER = new com.google.protobuf.AbstractParser<PCheckFunctionResponse>() {
      @java.lang.Override
      public PCheckFunctionResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PCheckFunctionResponse(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PCheckFunctionResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PCheckFunctionResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.FunctionService.PCheckFunctionResponse getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PRequestContext_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PRequestContext_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PFunctionCallRequest_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PFunctionCallRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PFunctionCallResponse_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PFunctionCallResponse_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PCheckFunctionRequest_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PCheckFunctionRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PCheckFunctionResponse_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PCheckFunctionResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\026function_service.proto\022\005doris\032\013types.p" +
      "roto\"P\n\017PRequestContext\022\n\n\002id\030\001 \001(\t\0221\n\020f" +
      "unction_context\030\002 \001(\0132\027.doris.PFunctionC" +
      "ontext\"t\n\024PFunctionCallRequest\022\025\n\rfuncti" +
      "on_name\030\001 \001(\t\022\034\n\004args\030\002 \003(\0132\016.doris.PVal" +
      "ues\022\'\n\007context\030\003 \001(\0132\026.doris.PRequestCon" +
      "text\"W\n\025PFunctionCallResponse\022\036\n\006result\030" +
      "\001 \001(\0132\016.doris.PValues\022\036\n\006status\030\002 \001(\0132\016." +
      "doris.PStatus\"\347\001\n\025PCheckFunctionRequest\022" +
      "\"\n\010function\030\001 \001(\0132\020.doris.PFunction\022:\n\nm" +
      "atch_type\030\002 \001(\0162&.doris.PCheckFunctionRe" +
      "quest.MatchType\"n\n\tMatchType\022\r\n\tIDENTICA" +
      "L\020\000\022\025\n\021INDISTINGUISHABLE\020\001\022\020\n\014SUPERTYPE_" +
      "OF\020\002\022\032\n\026NONSTRICT_SUPERTYPE_OF\020\003\022\r\n\tMATC" +
      "HABLE\020\004\"8\n\026PCheckFunctionResponse\022\036\n\006sta" +
      "tus\030\001 \001(\0132\016.doris.PStatus2\344\001\n\020PFunctionS" +
      "ervice\022D\n\007fn_call\022\033.doris.PFunctionCallR" +
      "equest\032\034.doris.PFunctionCallResponse\022G\n\010" +
      "check_fn\022\034.doris.PCheckFunctionRequest\032\035" +
      ".doris.PCheckFunctionResponse\022A\n\nhand_sh" +
      "ake\022\030.doris.PHandShakeRequest\032\031.doris.PH" +
      "andShakeResponseB\033\n\026org.apache.doris.pro" +
      "to\200\001\001"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          org.apache.doris.proto.Types.getDescriptor(),
        });
    internal_static_doris_PRequestContext_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_doris_PRequestContext_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PRequestContext_descriptor,
        new java.lang.String[] { "Id", "FunctionContext", });
    internal_static_doris_PFunctionCallRequest_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_doris_PFunctionCallRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PFunctionCallRequest_descriptor,
        new java.lang.String[] { "FunctionName", "Args", "Context", });
    internal_static_doris_PFunctionCallResponse_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_doris_PFunctionCallResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PFunctionCallResponse_descriptor,
        new java.lang.String[] { "Result", "Status", });
    internal_static_doris_PCheckFunctionRequest_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_doris_PCheckFunctionRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PCheckFunctionRequest_descriptor,
        new java.lang.String[] { "Function", "MatchType", });
    internal_static_doris_PCheckFunctionResponse_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_doris_PCheckFunctionResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PCheckFunctionResponse_descriptor,
        new java.lang.String[] { "Status", });
    org.apache.doris.proto.Types.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
