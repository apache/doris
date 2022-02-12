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

public final class Types {
  private Types() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface PStatusOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PStatus)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required int32 status_code = 1;</code>
     * @return Whether the statusCode field is set.
     */
    boolean hasStatusCode();
    /**
     * <code>required int32 status_code = 1;</code>
     * @return The statusCode.
     */
    int getStatusCode();

    /**
     * <code>repeated string error_msgs = 2;</code>
     * @return A list containing the errorMsgs.
     */
    java.util.List<java.lang.String>
        getErrorMsgsList();
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @return The count of errorMsgs.
     */
    int getErrorMsgsCount();
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @param index The index of the element to return.
     * @return The errorMsgs at the given index.
     */
    java.lang.String getErrorMsgs(int index);
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @param index The index of the value to return.
     * @return The bytes of the errorMsgs at the given index.
     */
    com.google.protobuf.ByteString
        getErrorMsgsBytes(int index);
  }
  /**
   * Protobuf type {@code doris.PStatus}
   */
  public  static final class PStatus extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PStatus)
      PStatusOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PStatus.newBuilder() to construct.
    private PStatus(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PStatus() {
      errorMsgs_ = com.google.protobuf.LazyStringArrayList.EMPTY;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PStatus();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PStatus(
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
            case 8: {
              bitField0_ |= 0x00000001;
              statusCode_ = input.readInt32();
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              if (!((mutable_bitField0_ & 0x00000002) != 0)) {
                errorMsgs_ = new com.google.protobuf.LazyStringArrayList();
                mutable_bitField0_ |= 0x00000002;
              }
              errorMsgs_.add(bs);
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
          errorMsgs_ = errorMsgs_.getUnmodifiableView();
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PStatus_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PStatus_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PStatus.class, org.apache.doris.proto.Types.PStatus.Builder.class);
    }

    private int bitField0_;
    public static final int STATUS_CODE_FIELD_NUMBER = 1;
    private int statusCode_;
    /**
     * <code>required int32 status_code = 1;</code>
     * @return Whether the statusCode field is set.
     */
    public boolean hasStatusCode() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required int32 status_code = 1;</code>
     * @return The statusCode.
     */
    public int getStatusCode() {
      return statusCode_;
    }

    public static final int ERROR_MSGS_FIELD_NUMBER = 2;
    private com.google.protobuf.LazyStringList errorMsgs_;
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @return A list containing the errorMsgs.
     */
    public com.google.protobuf.ProtocolStringList
        getErrorMsgsList() {
      return errorMsgs_;
    }
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @return The count of errorMsgs.
     */
    public int getErrorMsgsCount() {
      return errorMsgs_.size();
    }
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @param index The index of the element to return.
     * @return The errorMsgs at the given index.
     */
    public java.lang.String getErrorMsgs(int index) {
      return errorMsgs_.get(index);
    }
    /**
     * <code>repeated string error_msgs = 2;</code>
     * @param index The index of the value to return.
     * @return The bytes of the errorMsgs at the given index.
     */
    public com.google.protobuf.ByteString
        getErrorMsgsBytes(int index) {
      return errorMsgs_.getByteString(index);
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasStatusCode()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt32(1, statusCode_);
      }
      for (int i = 0; i < errorMsgs_.size(); i++) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, errorMsgs_.getRaw(i));
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
          .computeInt32Size(1, statusCode_);
      }
      {
        int dataSize = 0;
        for (int i = 0; i < errorMsgs_.size(); i++) {
          dataSize += computeStringSizeNoTag(errorMsgs_.getRaw(i));
        }
        size += dataSize;
        size += 1 * getErrorMsgsList().size();
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
      if (!(obj instanceof org.apache.doris.proto.Types.PStatus)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PStatus other = (org.apache.doris.proto.Types.PStatus) obj;

      if (hasStatusCode() != other.hasStatusCode()) return false;
      if (hasStatusCode()) {
        if (getStatusCode()
            != other.getStatusCode()) return false;
      }
      if (!getErrorMsgsList()
          .equals(other.getErrorMsgsList())) return false;
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
      if (hasStatusCode()) {
        hash = (37 * hash) + STATUS_CODE_FIELD_NUMBER;
        hash = (53 * hash) + getStatusCode();
      }
      if (getErrorMsgsCount() > 0) {
        hash = (37 * hash) + ERROR_MSGS_FIELD_NUMBER;
        hash = (53 * hash) + getErrorMsgsList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PStatus parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStatus parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStatus parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStatus parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PStatus prototype) {
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
     * Protobuf type {@code doris.PStatus}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PStatus)
        org.apache.doris.proto.Types.PStatusOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PStatus_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PStatus_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PStatus.class, org.apache.doris.proto.Types.PStatus.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PStatus.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        statusCode_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        errorMsgs_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PStatus_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStatus getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PStatus.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStatus build() {
        org.apache.doris.proto.Types.PStatus result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStatus buildPartial() {
        org.apache.doris.proto.Types.PStatus result = new org.apache.doris.proto.Types.PStatus(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.statusCode_ = statusCode_;
          to_bitField0_ |= 0x00000001;
        }
        if (((bitField0_ & 0x00000002) != 0)) {
          errorMsgs_ = errorMsgs_.getUnmodifiableView();
          bitField0_ = (bitField0_ & ~0x00000002);
        }
        result.errorMsgs_ = errorMsgs_;
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
        if (other instanceof org.apache.doris.proto.Types.PStatus) {
          return mergeFrom((org.apache.doris.proto.Types.PStatus)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PStatus other) {
        if (other == org.apache.doris.proto.Types.PStatus.getDefaultInstance()) return this;
        if (other.hasStatusCode()) {
          setStatusCode(other.getStatusCode());
        }
        if (!other.errorMsgs_.isEmpty()) {
          if (errorMsgs_.isEmpty()) {
            errorMsgs_ = other.errorMsgs_;
            bitField0_ = (bitField0_ & ~0x00000002);
          } else {
            ensureErrorMsgsIsMutable();
            errorMsgs_.addAll(other.errorMsgs_);
          }
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasStatusCode()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PStatus parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PStatus) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int statusCode_ ;
      /**
       * <code>required int32 status_code = 1;</code>
       * @return Whether the statusCode field is set.
       */
      public boolean hasStatusCode() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required int32 status_code = 1;</code>
       * @return The statusCode.
       */
      public int getStatusCode() {
        return statusCode_;
      }
      /**
       * <code>required int32 status_code = 1;</code>
       * @param value The statusCode to set.
       * @return This builder for chaining.
       */
      public Builder setStatusCode(int value) {
        bitField0_ |= 0x00000001;
        statusCode_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int32 status_code = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearStatusCode() {
        bitField0_ = (bitField0_ & ~0x00000001);
        statusCode_ = 0;
        onChanged();
        return this;
      }

      private com.google.protobuf.LazyStringList errorMsgs_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      private void ensureErrorMsgsIsMutable() {
        if (!((bitField0_ & 0x00000002) != 0)) {
          errorMsgs_ = new com.google.protobuf.LazyStringArrayList(errorMsgs_);
          bitField0_ |= 0x00000002;
         }
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @return A list containing the errorMsgs.
       */
      public com.google.protobuf.ProtocolStringList
          getErrorMsgsList() {
        return errorMsgs_.getUnmodifiableView();
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @return The count of errorMsgs.
       */
      public int getErrorMsgsCount() {
        return errorMsgs_.size();
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param index The index of the element to return.
       * @return The errorMsgs at the given index.
       */
      public java.lang.String getErrorMsgs(int index) {
        return errorMsgs_.get(index);
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param index The index of the value to return.
       * @return The bytes of the errorMsgs at the given index.
       */
      public com.google.protobuf.ByteString
          getErrorMsgsBytes(int index) {
        return errorMsgs_.getByteString(index);
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param index The index to set the value at.
       * @param value The errorMsgs to set.
       * @return This builder for chaining.
       */
      public Builder setErrorMsgs(
          int index, java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureErrorMsgsIsMutable();
        errorMsgs_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param value The errorMsgs to add.
       * @return This builder for chaining.
       */
      public Builder addErrorMsgs(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureErrorMsgsIsMutable();
        errorMsgs_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param values The errorMsgs to add.
       * @return This builder for chaining.
       */
      public Builder addAllErrorMsgs(
          java.lang.Iterable<java.lang.String> values) {
        ensureErrorMsgsIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, errorMsgs_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearErrorMsgs() {
        errorMsgs_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string error_msgs = 2;</code>
       * @param value The bytes of the errorMsgs to add.
       * @return This builder for chaining.
       */
      public Builder addErrorMsgsBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureErrorMsgsIsMutable();
        errorMsgs_.add(value);
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


      // @@protoc_insertion_point(builder_scope:doris.PStatus)
    }

    // @@protoc_insertion_point(class_scope:doris.PStatus)
    private static final org.apache.doris.proto.Types.PStatus DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PStatus();
    }

    public static org.apache.doris.proto.Types.PStatus getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PStatus>
        PARSER = new com.google.protobuf.AbstractParser<PStatus>() {
      @java.lang.Override
      public PStatus parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PStatus(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PStatus> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PStatus> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PStatus getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PScalarTypeOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PScalarType)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * TPrimitiveType, use int32 to avoid redefine Enum
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <pre>
     * TPrimitiveType, use int32 to avoid redefine Enum
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return The type.
     */
    int getType();

    /**
     * <pre>
     * Only set if type == CHAR or type == VARCHAR
     * </pre>
     *
     * <code>optional int32 len = 2;</code>
     * @return Whether the len field is set.
     */
    boolean hasLen();
    /**
     * <pre>
     * Only set if type == CHAR or type == VARCHAR
     * </pre>
     *
     * <code>optional int32 len = 2;</code>
     * @return The len.
     */
    int getLen();

    /**
     * <pre>
     * Only set for DECIMAL
     * </pre>
     *
     * <code>optional int32 precision = 3;</code>
     * @return Whether the precision field is set.
     */
    boolean hasPrecision();
    /**
     * <pre>
     * Only set for DECIMAL
     * </pre>
     *
     * <code>optional int32 precision = 3;</code>
     * @return The precision.
     */
    int getPrecision();

    /**
     * <code>optional int32 scale = 4;</code>
     * @return Whether the scale field is set.
     */
    boolean hasScale();
    /**
     * <code>optional int32 scale = 4;</code>
     * @return The scale.
     */
    int getScale();
  }
  /**
   * Protobuf type {@code doris.PScalarType}
   */
  public  static final class PScalarType extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PScalarType)
      PScalarTypeOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PScalarType.newBuilder() to construct.
    private PScalarType(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PScalarType() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PScalarType();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PScalarType(
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
            case 8: {
              bitField0_ |= 0x00000001;
              type_ = input.readInt32();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              len_ = input.readInt32();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              precision_ = input.readInt32();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              scale_ = input.readInt32();
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
      return org.apache.doris.proto.Types.internal_static_doris_PScalarType_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PScalarType_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PScalarType.class, org.apache.doris.proto.Types.PScalarType.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;
    private int type_;
    /**
     * <pre>
     * TPrimitiveType, use int32 to avoid redefine Enum
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <pre>
     * TPrimitiveType, use int32 to avoid redefine Enum
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return The type.
     */
    public int getType() {
      return type_;
    }

    public static final int LEN_FIELD_NUMBER = 2;
    private int len_;
    /**
     * <pre>
     * Only set if type == CHAR or type == VARCHAR
     * </pre>
     *
     * <code>optional int32 len = 2;</code>
     * @return Whether the len field is set.
     */
    public boolean hasLen() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <pre>
     * Only set if type == CHAR or type == VARCHAR
     * </pre>
     *
     * <code>optional int32 len = 2;</code>
     * @return The len.
     */
    public int getLen() {
      return len_;
    }

    public static final int PRECISION_FIELD_NUMBER = 3;
    private int precision_;
    /**
     * <pre>
     * Only set for DECIMAL
     * </pre>
     *
     * <code>optional int32 precision = 3;</code>
     * @return Whether the precision field is set.
     */
    public boolean hasPrecision() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <pre>
     * Only set for DECIMAL
     * </pre>
     *
     * <code>optional int32 precision = 3;</code>
     * @return The precision.
     */
    public int getPrecision() {
      return precision_;
    }

    public static final int SCALE_FIELD_NUMBER = 4;
    private int scale_;
    /**
     * <code>optional int32 scale = 4;</code>
     * @return Whether the scale field is set.
     */
    public boolean hasScale() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional int32 scale = 4;</code>
     * @return The scale.
     */
    public int getScale() {
      return scale_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt32(1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt32(2, len_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeInt32(3, precision_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeInt32(4, scale_);
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
          .computeInt32Size(1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, len_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, precision_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(4, scale_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PScalarType)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PScalarType other = (org.apache.doris.proto.Types.PScalarType) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (getType()
            != other.getType()) return false;
      }
      if (hasLen() != other.hasLen()) return false;
      if (hasLen()) {
        if (getLen()
            != other.getLen()) return false;
      }
      if (hasPrecision() != other.hasPrecision()) return false;
      if (hasPrecision()) {
        if (getPrecision()
            != other.getPrecision()) return false;
      }
      if (hasScale() != other.hasScale()) return false;
      if (hasScale()) {
        if (getScale()
            != other.getScale()) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType();
      }
      if (hasLen()) {
        hash = (37 * hash) + LEN_FIELD_NUMBER;
        hash = (53 * hash) + getLen();
      }
      if (hasPrecision()) {
        hash = (37 * hash) + PRECISION_FIELD_NUMBER;
        hash = (53 * hash) + getPrecision();
      }
      if (hasScale()) {
        hash = (37 * hash) + SCALE_FIELD_NUMBER;
        hash = (53 * hash) + getScale();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PScalarType parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PScalarType parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PScalarType parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PScalarType prototype) {
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
     * Protobuf type {@code doris.PScalarType}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PScalarType)
        org.apache.doris.proto.Types.PScalarTypeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PScalarType_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PScalarType_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PScalarType.class, org.apache.doris.proto.Types.PScalarType.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PScalarType.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        type_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        len_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        precision_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        scale_ = 0;
        bitField0_ = (bitField0_ & ~0x00000008);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PScalarType_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PScalarType getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PScalarType.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PScalarType build() {
        org.apache.doris.proto.Types.PScalarType result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PScalarType buildPartial() {
        org.apache.doris.proto.Types.PScalarType result = new org.apache.doris.proto.Types.PScalarType(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.type_ = type_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.len_ = len_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.precision_ = precision_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.scale_ = scale_;
          to_bitField0_ |= 0x00000008;
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
        if (other instanceof org.apache.doris.proto.Types.PScalarType) {
          return mergeFrom((org.apache.doris.proto.Types.PScalarType)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PScalarType other) {
        if (other == org.apache.doris.proto.Types.PScalarType.getDefaultInstance()) return this;
        if (other.hasType()) {
          setType(other.getType());
        }
        if (other.hasLen()) {
          setLen(other.getLen());
        }
        if (other.hasPrecision()) {
          setPrecision(other.getPrecision());
        }
        if (other.hasScale()) {
          setScale(other.getScale());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PScalarType parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PScalarType) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int type_ ;
      /**
       * <pre>
       * TPrimitiveType, use int32 to avoid redefine Enum
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <pre>
       * TPrimitiveType, use int32 to avoid redefine Enum
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return The type.
       */
      public int getType() {
        return type_;
      }
      /**
       * <pre>
       * TPrimitiveType, use int32 to avoid redefine Enum
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @param value The type to set.
       * @return This builder for chaining.
       */
      public Builder setType(int value) {
        bitField0_ |= 0x00000001;
        type_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * TPrimitiveType, use int32 to avoid redefine Enum
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearType() {
        bitField0_ = (bitField0_ & ~0x00000001);
        type_ = 0;
        onChanged();
        return this;
      }

      private int len_ ;
      /**
       * <pre>
       * Only set if type == CHAR or type == VARCHAR
       * </pre>
       *
       * <code>optional int32 len = 2;</code>
       * @return Whether the len field is set.
       */
      public boolean hasLen() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <pre>
       * Only set if type == CHAR or type == VARCHAR
       * </pre>
       *
       * <code>optional int32 len = 2;</code>
       * @return The len.
       */
      public int getLen() {
        return len_;
      }
      /**
       * <pre>
       * Only set if type == CHAR or type == VARCHAR
       * </pre>
       *
       * <code>optional int32 len = 2;</code>
       * @param value The len to set.
       * @return This builder for chaining.
       */
      public Builder setLen(int value) {
        bitField0_ |= 0x00000002;
        len_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Only set if type == CHAR or type == VARCHAR
       * </pre>
       *
       * <code>optional int32 len = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearLen() {
        bitField0_ = (bitField0_ & ~0x00000002);
        len_ = 0;
        onChanged();
        return this;
      }

      private int precision_ ;
      /**
       * <pre>
       * Only set for DECIMAL
       * </pre>
       *
       * <code>optional int32 precision = 3;</code>
       * @return Whether the precision field is set.
       */
      public boolean hasPrecision() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <pre>
       * Only set for DECIMAL
       * </pre>
       *
       * <code>optional int32 precision = 3;</code>
       * @return The precision.
       */
      public int getPrecision() {
        return precision_;
      }
      /**
       * <pre>
       * Only set for DECIMAL
       * </pre>
       *
       * <code>optional int32 precision = 3;</code>
       * @param value The precision to set.
       * @return This builder for chaining.
       */
      public Builder setPrecision(int value) {
        bitField0_ |= 0x00000004;
        precision_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * Only set for DECIMAL
       * </pre>
       *
       * <code>optional int32 precision = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearPrecision() {
        bitField0_ = (bitField0_ & ~0x00000004);
        precision_ = 0;
        onChanged();
        return this;
      }

      private int scale_ ;
      /**
       * <code>optional int32 scale = 4;</code>
       * @return Whether the scale field is set.
       */
      public boolean hasScale() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional int32 scale = 4;</code>
       * @return The scale.
       */
      public int getScale() {
        return scale_;
      }
      /**
       * <code>optional int32 scale = 4;</code>
       * @param value The scale to set.
       * @return This builder for chaining.
       */
      public Builder setScale(int value) {
        bitField0_ |= 0x00000008;
        scale_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 scale = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearScale() {
        bitField0_ = (bitField0_ & ~0x00000008);
        scale_ = 0;
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


      // @@protoc_insertion_point(builder_scope:doris.PScalarType)
    }

    // @@protoc_insertion_point(class_scope:doris.PScalarType)
    private static final org.apache.doris.proto.Types.PScalarType DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PScalarType();
    }

    public static org.apache.doris.proto.Types.PScalarType getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PScalarType>
        PARSER = new com.google.protobuf.AbstractParser<PScalarType>() {
      @java.lang.Override
      public PScalarType parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PScalarType(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PScalarType> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PScalarType> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PScalarType getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PStructFieldOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PStructField)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required string name = 1;</code>
     * @return Whether the name field is set.
     */
    boolean hasName();
    /**
     * <code>required string name = 1;</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>required string name = 1;</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>optional string comment = 2;</code>
     * @return Whether the comment field is set.
     */
    boolean hasComment();
    /**
     * <code>optional string comment = 2;</code>
     * @return The comment.
     */
    java.lang.String getComment();
    /**
     * <code>optional string comment = 2;</code>
     * @return The bytes for comment.
     */
    com.google.protobuf.ByteString
        getCommentBytes();
  }
  /**
   * <pre>
   * Represents a field in a STRUCT type.
   * TODO: Model column stats for struct fields.
   * </pre>
   *
   * Protobuf type {@code doris.PStructField}
   */
  public  static final class PStructField extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PStructField)
      PStructFieldOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PStructField.newBuilder() to construct.
    private PStructField(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PStructField() {
      name_ = "";
      comment_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PStructField();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PStructField(
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
              name_ = bs;
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              comment_ = bs;
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
      return org.apache.doris.proto.Types.internal_static_doris_PStructField_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PStructField_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PStructField.class, org.apache.doris.proto.Types.PStructField.Builder.class);
    }

    private int bitField0_;
    public static final int NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object name_;
    /**
     * <code>required string name = 1;</code>
     * @return Whether the name field is set.
     */
    public boolean hasName() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required string name = 1;</code>
     * @return The name.
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          name_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string name = 1;</code>
     * @return The bytes for name.
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int COMMENT_FIELD_NUMBER = 2;
    private volatile java.lang.Object comment_;
    /**
     * <code>optional string comment = 2;</code>
     * @return Whether the comment field is set.
     */
    public boolean hasComment() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string comment = 2;</code>
     * @return The comment.
     */
    public java.lang.String getComment() {
      java.lang.Object ref = comment_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          comment_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string comment = 2;</code>
     * @return The bytes for comment.
     */
    public com.google.protobuf.ByteString
        getCommentBytes() {
      java.lang.Object ref = comment_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        comment_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasName()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, name_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, comment_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, name_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, comment_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PStructField)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PStructField other = (org.apache.doris.proto.Types.PStructField) obj;

      if (hasName() != other.hasName()) return false;
      if (hasName()) {
        if (!getName()
            .equals(other.getName())) return false;
      }
      if (hasComment() != other.hasComment()) return false;
      if (hasComment()) {
        if (!getComment()
            .equals(other.getComment())) return false;
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
      if (hasName()) {
        hash = (37 * hash) + NAME_FIELD_NUMBER;
        hash = (53 * hash) + getName().hashCode();
      }
      if (hasComment()) {
        hash = (37 * hash) + COMMENT_FIELD_NUMBER;
        hash = (53 * hash) + getComment().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PStructField parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStructField parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStructField parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStructField parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PStructField prototype) {
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
     * <pre>
     * Represents a field in a STRUCT type.
     * TODO: Model column stats for struct fields.
     * </pre>
     *
     * Protobuf type {@code doris.PStructField}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PStructField)
        org.apache.doris.proto.Types.PStructFieldOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PStructField_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PStructField_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PStructField.class, org.apache.doris.proto.Types.PStructField.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PStructField.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        name_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        comment_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PStructField_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStructField getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PStructField.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStructField build() {
        org.apache.doris.proto.Types.PStructField result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStructField buildPartial() {
        org.apache.doris.proto.Types.PStructField result = new org.apache.doris.proto.Types.PStructField(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.name_ = name_;
        if (((from_bitField0_ & 0x00000002) != 0)) {
          to_bitField0_ |= 0x00000002;
        }
        result.comment_ = comment_;
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
        if (other instanceof org.apache.doris.proto.Types.PStructField) {
          return mergeFrom((org.apache.doris.proto.Types.PStructField)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PStructField other) {
        if (other == org.apache.doris.proto.Types.PStructField.getDefaultInstance()) return this;
        if (other.hasName()) {
          bitField0_ |= 0x00000001;
          name_ = other.name_;
          onChanged();
        }
        if (other.hasComment()) {
          bitField0_ |= 0x00000002;
          comment_ = other.comment_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasName()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PStructField parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PStructField) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object name_ = "";
      /**
       * <code>required string name = 1;</code>
       * @return Whether the name field is set.
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required string name = 1;</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       * @return The bytes for name.
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string name = 1;</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 1;</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        name_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object comment_ = "";
      /**
       * <code>optional string comment = 2;</code>
       * @return Whether the comment field is set.
       */
      public boolean hasComment() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional string comment = 2;</code>
       * @return The comment.
       */
      public java.lang.String getComment() {
        java.lang.Object ref = comment_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            comment_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string comment = 2;</code>
       * @return The bytes for comment.
       */
      public com.google.protobuf.ByteString
          getCommentBytes() {
        java.lang.Object ref = comment_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          comment_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string comment = 2;</code>
       * @param value The comment to set.
       * @return This builder for chaining.
       */
      public Builder setComment(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        comment_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string comment = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearComment() {
        bitField0_ = (bitField0_ & ~0x00000002);
        comment_ = getDefaultInstance().getComment();
        onChanged();
        return this;
      }
      /**
       * <code>optional string comment = 2;</code>
       * @param value The bytes for comment to set.
       * @return This builder for chaining.
       */
      public Builder setCommentBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        comment_ = value;
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


      // @@protoc_insertion_point(builder_scope:doris.PStructField)
    }

    // @@protoc_insertion_point(class_scope:doris.PStructField)
    private static final org.apache.doris.proto.Types.PStructField DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PStructField();
    }

    public static org.apache.doris.proto.Types.PStructField getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PStructField>
        PARSER = new com.google.protobuf.AbstractParser<PStructField>() {
      @java.lang.Override
      public PStructField parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PStructField(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PStructField> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PStructField> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PStructField getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PTypeNodeOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PTypeNode)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <pre>
     * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return The type.
     */
    int getType();

    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     * @return Whether the scalarType field is set.
     */
    boolean hasScalarType();
    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     * @return The scalarType.
     */
    org.apache.doris.proto.Types.PScalarType getScalarType();
    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     */
    org.apache.doris.proto.Types.PScalarTypeOrBuilder getScalarTypeOrBuilder();

    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PStructField> 
        getStructFieldsList();
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    org.apache.doris.proto.Types.PStructField getStructFields(int index);
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    int getStructFieldsCount();
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PStructFieldOrBuilder> 
        getStructFieldsOrBuilderList();
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    org.apache.doris.proto.Types.PStructFieldOrBuilder getStructFieldsOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code doris.PTypeNode}
   */
  public  static final class PTypeNode extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PTypeNode)
      PTypeNodeOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PTypeNode.newBuilder() to construct.
    private PTypeNode(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PTypeNode() {
      structFields_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PTypeNode();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PTypeNode(
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
            case 8: {
              bitField0_ |= 0x00000001;
              type_ = input.readInt32();
              break;
            }
            case 18: {
              org.apache.doris.proto.Types.PScalarType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = scalarType_.toBuilder();
              }
              scalarType_ = input.readMessage(org.apache.doris.proto.Types.PScalarType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(scalarType_);
                scalarType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            case 26: {
              if (!((mutable_bitField0_ & 0x00000004) != 0)) {
                structFields_ = new java.util.ArrayList<org.apache.doris.proto.Types.PStructField>();
                mutable_bitField0_ |= 0x00000004;
              }
              structFields_.add(
                  input.readMessage(org.apache.doris.proto.Types.PStructField.PARSER, extensionRegistry));
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
        if (((mutable_bitField0_ & 0x00000004) != 0)) {
          structFields_ = java.util.Collections.unmodifiableList(structFields_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PTypeNode_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PTypeNode_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PTypeNode.class, org.apache.doris.proto.Types.PTypeNode.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;
    private int type_;
    /**
     * <pre>
     * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <pre>
     * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
     * </pre>
     *
     * <code>required int32 type = 1;</code>
     * @return The type.
     */
    public int getType() {
      return type_;
    }

    public static final int SCALAR_TYPE_FIELD_NUMBER = 2;
    private org.apache.doris.proto.Types.PScalarType scalarType_;
    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     * @return Whether the scalarType field is set.
     */
    public boolean hasScalarType() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     * @return The scalarType.
     */
    public org.apache.doris.proto.Types.PScalarType getScalarType() {
      return scalarType_ == null ? org.apache.doris.proto.Types.PScalarType.getDefaultInstance() : scalarType_;
    }
    /**
     * <pre>
     * only set for scalar types
     * </pre>
     *
     * <code>optional .doris.PScalarType scalar_type = 2;</code>
     */
    public org.apache.doris.proto.Types.PScalarTypeOrBuilder getScalarTypeOrBuilder() {
      return scalarType_ == null ? org.apache.doris.proto.Types.PScalarType.getDefaultInstance() : scalarType_;
    }

    public static final int STRUCT_FIELDS_FIELD_NUMBER = 3;
    private java.util.List<org.apache.doris.proto.Types.PStructField> structFields_;
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PStructField> getStructFieldsList() {
      return structFields_;
    }
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PStructFieldOrBuilder> 
        getStructFieldsOrBuilderList() {
      return structFields_;
    }
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    public int getStructFieldsCount() {
      return structFields_.size();
    }
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    public org.apache.doris.proto.Types.PStructField getStructFields(int index) {
      return structFields_.get(index);
    }
    /**
     * <pre>
     * only used for structs; has struct_fields.size() corresponding child types
     * </pre>
     *
     * <code>repeated .doris.PStructField struct_fields = 3;</code>
     */
    public org.apache.doris.proto.Types.PStructFieldOrBuilder getStructFieldsOrBuilder(
        int index) {
      return structFields_.get(index);
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (hasScalarType()) {
        if (!getScalarType().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getStructFieldsCount(); i++) {
        if (!getStructFields(i).isInitialized()) {
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
        output.writeInt32(1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(2, getScalarType());
      }
      for (int i = 0; i < structFields_.size(); i++) {
        output.writeMessage(3, structFields_.get(i));
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
          .computeInt32Size(1, type_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, getScalarType());
      }
      for (int i = 0; i < structFields_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, structFields_.get(i));
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
      if (!(obj instanceof org.apache.doris.proto.Types.PTypeNode)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PTypeNode other = (org.apache.doris.proto.Types.PTypeNode) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (getType()
            != other.getType()) return false;
      }
      if (hasScalarType() != other.hasScalarType()) return false;
      if (hasScalarType()) {
        if (!getScalarType()
            .equals(other.getScalarType())) return false;
      }
      if (!getStructFieldsList()
          .equals(other.getStructFieldsList())) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType();
      }
      if (hasScalarType()) {
        hash = (37 * hash) + SCALAR_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getScalarType().hashCode();
      }
      if (getStructFieldsCount() > 0) {
        hash = (37 * hash) + STRUCT_FIELDS_FIELD_NUMBER;
        hash = (53 * hash) + getStructFieldsList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeNode parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PTypeNode prototype) {
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
     * Protobuf type {@code doris.PTypeNode}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PTypeNode)
        org.apache.doris.proto.Types.PTypeNodeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeNode_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeNode_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PTypeNode.class, org.apache.doris.proto.Types.PTypeNode.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PTypeNode.newBuilder()
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
          getScalarTypeFieldBuilder();
          getStructFieldsFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        type_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        if (scalarTypeBuilder_ == null) {
          scalarType_ = null;
        } else {
          scalarTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        if (structFieldsBuilder_ == null) {
          structFields_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
        } else {
          structFieldsBuilder_.clear();
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeNode_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeNode getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PTypeNode.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeNode build() {
        org.apache.doris.proto.Types.PTypeNode result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeNode buildPartial() {
        org.apache.doris.proto.Types.PTypeNode result = new org.apache.doris.proto.Types.PTypeNode(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.type_ = type_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          if (scalarTypeBuilder_ == null) {
            result.scalarType_ = scalarType_;
          } else {
            result.scalarType_ = scalarTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        if (structFieldsBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0)) {
            structFields_ = java.util.Collections.unmodifiableList(structFields_);
            bitField0_ = (bitField0_ & ~0x00000004);
          }
          result.structFields_ = structFields_;
        } else {
          result.structFields_ = structFieldsBuilder_.build();
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
        if (other instanceof org.apache.doris.proto.Types.PTypeNode) {
          return mergeFrom((org.apache.doris.proto.Types.PTypeNode)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PTypeNode other) {
        if (other == org.apache.doris.proto.Types.PTypeNode.getDefaultInstance()) return this;
        if (other.hasType()) {
          setType(other.getType());
        }
        if (other.hasScalarType()) {
          mergeScalarType(other.getScalarType());
        }
        if (structFieldsBuilder_ == null) {
          if (!other.structFields_.isEmpty()) {
            if (structFields_.isEmpty()) {
              structFields_ = other.structFields_;
              bitField0_ = (bitField0_ & ~0x00000004);
            } else {
              ensureStructFieldsIsMutable();
              structFields_.addAll(other.structFields_);
            }
            onChanged();
          }
        } else {
          if (!other.structFields_.isEmpty()) {
            if (structFieldsBuilder_.isEmpty()) {
              structFieldsBuilder_.dispose();
              structFieldsBuilder_ = null;
              structFields_ = other.structFields_;
              bitField0_ = (bitField0_ & ~0x00000004);
              structFieldsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getStructFieldsFieldBuilder() : null;
            } else {
              structFieldsBuilder_.addAllMessages(other.structFields_);
            }
          }
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        if (hasScalarType()) {
          if (!getScalarType().isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getStructFieldsCount(); i++) {
          if (!getStructFields(i).isInitialized()) {
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
        org.apache.doris.proto.Types.PTypeNode parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PTypeNode) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int type_ ;
      /**
       * <pre>
       * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <pre>
       * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return The type.
       */
      public int getType() {
        return type_;
      }
      /**
       * <pre>
       * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @param value The type to set.
       * @return This builder for chaining.
       */
      public Builder setType(int value) {
        bitField0_ |= 0x00000001;
        type_ = value;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * TTypeNodeType(SCALAR, ARRAY, MAP, STRUCT)
       * </pre>
       *
       * <code>required int32 type = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearType() {
        bitField0_ = (bitField0_ & ~0x00000001);
        type_ = 0;
        onChanged();
        return this;
      }

      private org.apache.doris.proto.Types.PScalarType scalarType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PScalarType, org.apache.doris.proto.Types.PScalarType.Builder, org.apache.doris.proto.Types.PScalarTypeOrBuilder> scalarTypeBuilder_;
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       * @return Whether the scalarType field is set.
       */
      public boolean hasScalarType() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       * @return The scalarType.
       */
      public org.apache.doris.proto.Types.PScalarType getScalarType() {
        if (scalarTypeBuilder_ == null) {
          return scalarType_ == null ? org.apache.doris.proto.Types.PScalarType.getDefaultInstance() : scalarType_;
        } else {
          return scalarTypeBuilder_.getMessage();
        }
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public Builder setScalarType(org.apache.doris.proto.Types.PScalarType value) {
        if (scalarTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          scalarType_ = value;
          onChanged();
        } else {
          scalarTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public Builder setScalarType(
          org.apache.doris.proto.Types.PScalarType.Builder builderForValue) {
        if (scalarTypeBuilder_ == null) {
          scalarType_ = builderForValue.build();
          onChanged();
        } else {
          scalarTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public Builder mergeScalarType(org.apache.doris.proto.Types.PScalarType value) {
        if (scalarTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0) &&
              scalarType_ != null &&
              scalarType_ != org.apache.doris.proto.Types.PScalarType.getDefaultInstance()) {
            scalarType_ =
              org.apache.doris.proto.Types.PScalarType.newBuilder(scalarType_).mergeFrom(value).buildPartial();
          } else {
            scalarType_ = value;
          }
          onChanged();
        } else {
          scalarTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public Builder clearScalarType() {
        if (scalarTypeBuilder_ == null) {
          scalarType_ = null;
          onChanged();
        } else {
          scalarTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public org.apache.doris.proto.Types.PScalarType.Builder getScalarTypeBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getScalarTypeFieldBuilder().getBuilder();
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      public org.apache.doris.proto.Types.PScalarTypeOrBuilder getScalarTypeOrBuilder() {
        if (scalarTypeBuilder_ != null) {
          return scalarTypeBuilder_.getMessageOrBuilder();
        } else {
          return scalarType_ == null ?
              org.apache.doris.proto.Types.PScalarType.getDefaultInstance() : scalarType_;
        }
      }
      /**
       * <pre>
       * only set for scalar types
       * </pre>
       *
       * <code>optional .doris.PScalarType scalar_type = 2;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PScalarType, org.apache.doris.proto.Types.PScalarType.Builder, org.apache.doris.proto.Types.PScalarTypeOrBuilder> 
          getScalarTypeFieldBuilder() {
        if (scalarTypeBuilder_ == null) {
          scalarTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PScalarType, org.apache.doris.proto.Types.PScalarType.Builder, org.apache.doris.proto.Types.PScalarTypeOrBuilder>(
                  getScalarType(),
                  getParentForChildren(),
                  isClean());
          scalarType_ = null;
        }
        return scalarTypeBuilder_;
      }

      private java.util.List<org.apache.doris.proto.Types.PStructField> structFields_ =
        java.util.Collections.emptyList();
      private void ensureStructFieldsIsMutable() {
        if (!((bitField0_ & 0x00000004) != 0)) {
          structFields_ = new java.util.ArrayList<org.apache.doris.proto.Types.PStructField>(structFields_);
          bitField0_ |= 0x00000004;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PStructField, org.apache.doris.proto.Types.PStructField.Builder, org.apache.doris.proto.Types.PStructFieldOrBuilder> structFieldsBuilder_;

      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PStructField> getStructFieldsList() {
        if (structFieldsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(structFields_);
        } else {
          return structFieldsBuilder_.getMessageList();
        }
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public int getStructFieldsCount() {
        if (structFieldsBuilder_ == null) {
          return structFields_.size();
        } else {
          return structFieldsBuilder_.getCount();
        }
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public org.apache.doris.proto.Types.PStructField getStructFields(int index) {
        if (structFieldsBuilder_ == null) {
          return structFields_.get(index);
        } else {
          return structFieldsBuilder_.getMessage(index);
        }
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder setStructFields(
          int index, org.apache.doris.proto.Types.PStructField value) {
        if (structFieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStructFieldsIsMutable();
          structFields_.set(index, value);
          onChanged();
        } else {
          structFieldsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder setStructFields(
          int index, org.apache.doris.proto.Types.PStructField.Builder builderForValue) {
        if (structFieldsBuilder_ == null) {
          ensureStructFieldsIsMutable();
          structFields_.set(index, builderForValue.build());
          onChanged();
        } else {
          structFieldsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder addStructFields(org.apache.doris.proto.Types.PStructField value) {
        if (structFieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStructFieldsIsMutable();
          structFields_.add(value);
          onChanged();
        } else {
          structFieldsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder addStructFields(
          int index, org.apache.doris.proto.Types.PStructField value) {
        if (structFieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStructFieldsIsMutable();
          structFields_.add(index, value);
          onChanged();
        } else {
          structFieldsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder addStructFields(
          org.apache.doris.proto.Types.PStructField.Builder builderForValue) {
        if (structFieldsBuilder_ == null) {
          ensureStructFieldsIsMutable();
          structFields_.add(builderForValue.build());
          onChanged();
        } else {
          structFieldsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder addStructFields(
          int index, org.apache.doris.proto.Types.PStructField.Builder builderForValue) {
        if (structFieldsBuilder_ == null) {
          ensureStructFieldsIsMutable();
          structFields_.add(index, builderForValue.build());
          onChanged();
        } else {
          structFieldsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder addAllStructFields(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PStructField> values) {
        if (structFieldsBuilder_ == null) {
          ensureStructFieldsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, structFields_);
          onChanged();
        } else {
          structFieldsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder clearStructFields() {
        if (structFieldsBuilder_ == null) {
          structFields_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
          onChanged();
        } else {
          structFieldsBuilder_.clear();
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public Builder removeStructFields(int index) {
        if (structFieldsBuilder_ == null) {
          ensureStructFieldsIsMutable();
          structFields_.remove(index);
          onChanged();
        } else {
          structFieldsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public org.apache.doris.proto.Types.PStructField.Builder getStructFieldsBuilder(
          int index) {
        return getStructFieldsFieldBuilder().getBuilder(index);
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public org.apache.doris.proto.Types.PStructFieldOrBuilder getStructFieldsOrBuilder(
          int index) {
        if (structFieldsBuilder_ == null) {
          return structFields_.get(index);  } else {
          return structFieldsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PStructFieldOrBuilder> 
           getStructFieldsOrBuilderList() {
        if (structFieldsBuilder_ != null) {
          return structFieldsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(structFields_);
        }
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public org.apache.doris.proto.Types.PStructField.Builder addStructFieldsBuilder() {
        return getStructFieldsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PStructField.getDefaultInstance());
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public org.apache.doris.proto.Types.PStructField.Builder addStructFieldsBuilder(
          int index) {
        return getStructFieldsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PStructField.getDefaultInstance());
      }
      /**
       * <pre>
       * only used for structs; has struct_fields.size() corresponding child types
       * </pre>
       *
       * <code>repeated .doris.PStructField struct_fields = 3;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PStructField.Builder> 
           getStructFieldsBuilderList() {
        return getStructFieldsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PStructField, org.apache.doris.proto.Types.PStructField.Builder, org.apache.doris.proto.Types.PStructFieldOrBuilder> 
          getStructFieldsFieldBuilder() {
        if (structFieldsBuilder_ == null) {
          structFieldsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PStructField, org.apache.doris.proto.Types.PStructField.Builder, org.apache.doris.proto.Types.PStructFieldOrBuilder>(
                  structFields_,
                  ((bitField0_ & 0x00000004) != 0),
                  getParentForChildren(),
                  isClean());
          structFields_ = null;
        }
        return structFieldsBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PTypeNode)
    }

    // @@protoc_insertion_point(class_scope:doris.PTypeNode)
    private static final org.apache.doris.proto.Types.PTypeNode DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PTypeNode();
    }

    public static org.apache.doris.proto.Types.PTypeNode getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PTypeNode>
        PARSER = new com.google.protobuf.AbstractParser<PTypeNode>() {
      @java.lang.Override
      public PTypeNode parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PTypeNode(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PTypeNode> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PTypeNode> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PTypeNode getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PTypeDescOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PTypeDesc)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PTypeNode> 
        getTypesList();
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    org.apache.doris.proto.Types.PTypeNode getTypes(int index);
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    int getTypesCount();
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PTypeNodeOrBuilder> 
        getTypesOrBuilderList();
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    org.apache.doris.proto.Types.PTypeNodeOrBuilder getTypesOrBuilder(
        int index);
  }
  /**
   * <pre>
   * A flattened representation of a tree of column types obtained by depth-first
   * traversal. Complex types such as map, array and struct have child types corresponding
   * to the map key/value, array item type, and struct fields, respectively.
   * For scalar types the list contains only a single node.
   * Note: We cannot rename this to TType because it conflicts with Thrift's internal TType
   * and the generated Python thrift files will not work.
   * Note: TTypeDesc in impala is TColumnType, but we already use TColumnType, so we name this
   * to TTypeDesc. In future, we merge these two to one
   * </pre>
   *
   * Protobuf type {@code doris.PTypeDesc}
   */
  public  static final class PTypeDesc extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PTypeDesc)
      PTypeDescOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PTypeDesc.newBuilder() to construct.
    private PTypeDesc(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PTypeDesc() {
      types_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PTypeDesc();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PTypeDesc(
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
              if (!((mutable_bitField0_ & 0x00000001) != 0)) {
                types_ = new java.util.ArrayList<org.apache.doris.proto.Types.PTypeNode>();
                mutable_bitField0_ |= 0x00000001;
              }
              types_.add(
                  input.readMessage(org.apache.doris.proto.Types.PTypeNode.PARSER, extensionRegistry));
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
        if (((mutable_bitField0_ & 0x00000001) != 0)) {
          types_ = java.util.Collections.unmodifiableList(types_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PTypeDesc_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PTypeDesc_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PTypeDesc.class, org.apache.doris.proto.Types.PTypeDesc.Builder.class);
    }

    public static final int TYPES_FIELD_NUMBER = 1;
    private java.util.List<org.apache.doris.proto.Types.PTypeNode> types_;
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PTypeNode> getTypesList() {
      return types_;
    }
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PTypeNodeOrBuilder> 
        getTypesOrBuilderList() {
      return types_;
    }
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    public int getTypesCount() {
      return types_.size();
    }
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    public org.apache.doris.proto.Types.PTypeNode getTypes(int index) {
      return types_.get(index);
    }
    /**
     * <code>repeated .doris.PTypeNode types = 1;</code>
     */
    public org.apache.doris.proto.Types.PTypeNodeOrBuilder getTypesOrBuilder(
        int index) {
      return types_.get(index);
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      for (int i = 0; i < getTypesCount(); i++) {
        if (!getTypes(i).isInitialized()) {
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
      for (int i = 0; i < types_.size(); i++) {
        output.writeMessage(1, types_.get(i));
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < types_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, types_.get(i));
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
      if (!(obj instanceof org.apache.doris.proto.Types.PTypeDesc)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PTypeDesc other = (org.apache.doris.proto.Types.PTypeDesc) obj;

      if (!getTypesList()
          .equals(other.getTypesList())) return false;
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
      if (getTypesCount() > 0) {
        hash = (37 * hash) + TYPES_FIELD_NUMBER;
        hash = (53 * hash) + getTypesList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PTypeDesc parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PTypeDesc prototype) {
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
     * <pre>
     * A flattened representation of a tree of column types obtained by depth-first
     * traversal. Complex types such as map, array and struct have child types corresponding
     * to the map key/value, array item type, and struct fields, respectively.
     * For scalar types the list contains only a single node.
     * Note: We cannot rename this to TType because it conflicts with Thrift's internal TType
     * and the generated Python thrift files will not work.
     * Note: TTypeDesc in impala is TColumnType, but we already use TColumnType, so we name this
     * to TTypeDesc. In future, we merge these two to one
     * </pre>
     *
     * Protobuf type {@code doris.PTypeDesc}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PTypeDesc)
        org.apache.doris.proto.Types.PTypeDescOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeDesc_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeDesc_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PTypeDesc.class, org.apache.doris.proto.Types.PTypeDesc.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PTypeDesc.newBuilder()
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
          getTypesFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (typesBuilder_ == null) {
          types_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          typesBuilder_.clear();
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PTypeDesc_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeDesc getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PTypeDesc.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeDesc build() {
        org.apache.doris.proto.Types.PTypeDesc result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PTypeDesc buildPartial() {
        org.apache.doris.proto.Types.PTypeDesc result = new org.apache.doris.proto.Types.PTypeDesc(this);
        int from_bitField0_ = bitField0_;
        if (typesBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)) {
            types_ = java.util.Collections.unmodifiableList(types_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.types_ = types_;
        } else {
          result.types_ = typesBuilder_.build();
        }
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
        if (other instanceof org.apache.doris.proto.Types.PTypeDesc) {
          return mergeFrom((org.apache.doris.proto.Types.PTypeDesc)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PTypeDesc other) {
        if (other == org.apache.doris.proto.Types.PTypeDesc.getDefaultInstance()) return this;
        if (typesBuilder_ == null) {
          if (!other.types_.isEmpty()) {
            if (types_.isEmpty()) {
              types_ = other.types_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureTypesIsMutable();
              types_.addAll(other.types_);
            }
            onChanged();
          }
        } else {
          if (!other.types_.isEmpty()) {
            if (typesBuilder_.isEmpty()) {
              typesBuilder_.dispose();
              typesBuilder_ = null;
              types_ = other.types_;
              bitField0_ = (bitField0_ & ~0x00000001);
              typesBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getTypesFieldBuilder() : null;
            } else {
              typesBuilder_.addAllMessages(other.types_);
            }
          }
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        for (int i = 0; i < getTypesCount(); i++) {
          if (!getTypes(i).isInitialized()) {
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
        org.apache.doris.proto.Types.PTypeDesc parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PTypeDesc) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.util.List<org.apache.doris.proto.Types.PTypeNode> types_ =
        java.util.Collections.emptyList();
      private void ensureTypesIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          types_ = new java.util.ArrayList<org.apache.doris.proto.Types.PTypeNode>(types_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PTypeNode, org.apache.doris.proto.Types.PTypeNode.Builder, org.apache.doris.proto.Types.PTypeNodeOrBuilder> typesBuilder_;

      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PTypeNode> getTypesList() {
        if (typesBuilder_ == null) {
          return java.util.Collections.unmodifiableList(types_);
        } else {
          return typesBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public int getTypesCount() {
        if (typesBuilder_ == null) {
          return types_.size();
        } else {
          return typesBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public org.apache.doris.proto.Types.PTypeNode getTypes(int index) {
        if (typesBuilder_ == null) {
          return types_.get(index);
        } else {
          return typesBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder setTypes(
          int index, org.apache.doris.proto.Types.PTypeNode value) {
        if (typesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureTypesIsMutable();
          types_.set(index, value);
          onChanged();
        } else {
          typesBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder setTypes(
          int index, org.apache.doris.proto.Types.PTypeNode.Builder builderForValue) {
        if (typesBuilder_ == null) {
          ensureTypesIsMutable();
          types_.set(index, builderForValue.build());
          onChanged();
        } else {
          typesBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder addTypes(org.apache.doris.proto.Types.PTypeNode value) {
        if (typesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureTypesIsMutable();
          types_.add(value);
          onChanged();
        } else {
          typesBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder addTypes(
          int index, org.apache.doris.proto.Types.PTypeNode value) {
        if (typesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureTypesIsMutable();
          types_.add(index, value);
          onChanged();
        } else {
          typesBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder addTypes(
          org.apache.doris.proto.Types.PTypeNode.Builder builderForValue) {
        if (typesBuilder_ == null) {
          ensureTypesIsMutable();
          types_.add(builderForValue.build());
          onChanged();
        } else {
          typesBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder addTypes(
          int index, org.apache.doris.proto.Types.PTypeNode.Builder builderForValue) {
        if (typesBuilder_ == null) {
          ensureTypesIsMutable();
          types_.add(index, builderForValue.build());
          onChanged();
        } else {
          typesBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder addAllTypes(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PTypeNode> values) {
        if (typesBuilder_ == null) {
          ensureTypesIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, types_);
          onChanged();
        } else {
          typesBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder clearTypes() {
        if (typesBuilder_ == null) {
          types_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          typesBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public Builder removeTypes(int index) {
        if (typesBuilder_ == null) {
          ensureTypesIsMutable();
          types_.remove(index);
          onChanged();
        } else {
          typesBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public org.apache.doris.proto.Types.PTypeNode.Builder getTypesBuilder(
          int index) {
        return getTypesFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public org.apache.doris.proto.Types.PTypeNodeOrBuilder getTypesOrBuilder(
          int index) {
        if (typesBuilder_ == null) {
          return types_.get(index);  } else {
          return typesBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PTypeNodeOrBuilder> 
           getTypesOrBuilderList() {
        if (typesBuilder_ != null) {
          return typesBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(types_);
        }
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public org.apache.doris.proto.Types.PTypeNode.Builder addTypesBuilder() {
        return getTypesFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PTypeNode.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public org.apache.doris.proto.Types.PTypeNode.Builder addTypesBuilder(
          int index) {
        return getTypesFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PTypeNode.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PTypeNode types = 1;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PTypeNode.Builder> 
           getTypesBuilderList() {
        return getTypesFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PTypeNode, org.apache.doris.proto.Types.PTypeNode.Builder, org.apache.doris.proto.Types.PTypeNodeOrBuilder> 
          getTypesFieldBuilder() {
        if (typesBuilder_ == null) {
          typesBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PTypeNode, org.apache.doris.proto.Types.PTypeNode.Builder, org.apache.doris.proto.Types.PTypeNodeOrBuilder>(
                  types_,
                  ((bitField0_ & 0x00000001) != 0),
                  getParentForChildren(),
                  isClean());
          types_ = null;
        }
        return typesBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PTypeDesc)
    }

    // @@protoc_insertion_point(class_scope:doris.PTypeDesc)
    private static final org.apache.doris.proto.Types.PTypeDesc DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PTypeDesc();
    }

    public static org.apache.doris.proto.Types.PTypeDesc getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PTypeDesc>
        PARSER = new com.google.protobuf.AbstractParser<PTypeDesc>() {
      @java.lang.Override
      public PTypeDesc parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PTypeDesc(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PTypeDesc> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PTypeDesc> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PTypeDesc getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PUniqueIdOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PUniqueId)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required int64 hi = 1;</code>
     * @return Whether the hi field is set.
     */
    boolean hasHi();
    /**
     * <code>required int64 hi = 1;</code>
     * @return The hi.
     */
    long getHi();

    /**
     * <code>required int64 lo = 2;</code>
     * @return Whether the lo field is set.
     */
    boolean hasLo();
    /**
     * <code>required int64 lo = 2;</code>
     * @return The lo.
     */
    long getLo();
  }
  /**
   * Protobuf type {@code doris.PUniqueId}
   */
  public  static final class PUniqueId extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PUniqueId)
      PUniqueIdOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PUniqueId.newBuilder() to construct.
    private PUniqueId(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PUniqueId() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PUniqueId();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PUniqueId(
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
            case 8: {
              bitField0_ |= 0x00000001;
              hi_ = input.readInt64();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              lo_ = input.readInt64();
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
      return org.apache.doris.proto.Types.internal_static_doris_PUniqueId_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PUniqueId_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PUniqueId.class, org.apache.doris.proto.Types.PUniqueId.Builder.class);
    }

    private int bitField0_;
    public static final int HI_FIELD_NUMBER = 1;
    private long hi_;
    /**
     * <code>required int64 hi = 1;</code>
     * @return Whether the hi field is set.
     */
    public boolean hasHi() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required int64 hi = 1;</code>
     * @return The hi.
     */
    public long getHi() {
      return hi_;
    }

    public static final int LO_FIELD_NUMBER = 2;
    private long lo_;
    /**
     * <code>required int64 lo = 2;</code>
     * @return Whether the lo field is set.
     */
    public boolean hasLo() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>required int64 lo = 2;</code>
     * @return The lo.
     */
    public long getLo() {
      return lo_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasHi()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasLo()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt64(1, hi_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt64(2, lo_);
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
          .computeInt64Size(1, hi_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(2, lo_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PUniqueId)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PUniqueId other = (org.apache.doris.proto.Types.PUniqueId) obj;

      if (hasHi() != other.hasHi()) return false;
      if (hasHi()) {
        if (getHi()
            != other.getHi()) return false;
      }
      if (hasLo() != other.hasLo()) return false;
      if (hasLo()) {
        if (getLo()
            != other.getLo()) return false;
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
      if (hasHi()) {
        hash = (37 * hash) + HI_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getHi());
      }
      if (hasLo()) {
        hash = (37 * hash) + LO_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getLo());
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PUniqueId parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PUniqueId prototype) {
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
     * Protobuf type {@code doris.PUniqueId}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PUniqueId)
        org.apache.doris.proto.Types.PUniqueIdOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PUniqueId_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PUniqueId_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PUniqueId.class, org.apache.doris.proto.Types.PUniqueId.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PUniqueId.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        hi_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000001);
        lo_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PUniqueId_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PUniqueId getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PUniqueId.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PUniqueId build() {
        org.apache.doris.proto.Types.PUniqueId result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PUniqueId buildPartial() {
        org.apache.doris.proto.Types.PUniqueId result = new org.apache.doris.proto.Types.PUniqueId(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.hi_ = hi_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.lo_ = lo_;
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
        if (other instanceof org.apache.doris.proto.Types.PUniqueId) {
          return mergeFrom((org.apache.doris.proto.Types.PUniqueId)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PUniqueId other) {
        if (other == org.apache.doris.proto.Types.PUniqueId.getDefaultInstance()) return this;
        if (other.hasHi()) {
          setHi(other.getHi());
        }
        if (other.hasLo()) {
          setLo(other.getLo());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasHi()) {
          return false;
        }
        if (!hasLo()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PUniqueId parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PUniqueId) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private long hi_ ;
      /**
       * <code>required int64 hi = 1;</code>
       * @return Whether the hi field is set.
       */
      public boolean hasHi() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required int64 hi = 1;</code>
       * @return The hi.
       */
      public long getHi() {
        return hi_;
      }
      /**
       * <code>required int64 hi = 1;</code>
       * @param value The hi to set.
       * @return This builder for chaining.
       */
      public Builder setHi(long value) {
        bitField0_ |= 0x00000001;
        hi_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 hi = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearHi() {
        bitField0_ = (bitField0_ & ~0x00000001);
        hi_ = 0L;
        onChanged();
        return this;
      }

      private long lo_ ;
      /**
       * <code>required int64 lo = 2;</code>
       * @return Whether the lo field is set.
       */
      public boolean hasLo() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>required int64 lo = 2;</code>
       * @return The lo.
       */
      public long getLo() {
        return lo_;
      }
      /**
       * <code>required int64 lo = 2;</code>
       * @param value The lo to set.
       * @return This builder for chaining.
       */
      public Builder setLo(long value) {
        bitField0_ |= 0x00000002;
        lo_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required int64 lo = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearLo() {
        bitField0_ = (bitField0_ & ~0x00000002);
        lo_ = 0L;
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


      // @@protoc_insertion_point(builder_scope:doris.PUniqueId)
    }

    // @@protoc_insertion_point(class_scope:doris.PUniqueId)
    private static final org.apache.doris.proto.Types.PUniqueId DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PUniqueId();
    }

    public static org.apache.doris.proto.Types.PUniqueId getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PUniqueId>
        PARSER = new com.google.protobuf.AbstractParser<PUniqueId>() {
      @java.lang.Override
      public PUniqueId parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PUniqueId(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PUniqueId> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PUniqueId> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PUniqueId getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PGenericTypeOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PGenericType)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType.TypeId id = 2;</code>
     * @return Whether the id field is set.
     */
    boolean hasId();
    /**
     * <code>required .doris.PGenericType.TypeId id = 2;</code>
     * @return The id.
     */
    org.apache.doris.proto.Types.PGenericType.TypeId getId();

    /**
     * <code>optional .doris.PList list_type = 11;</code>
     * @return Whether the listType field is set.
     */
    boolean hasListType();
    /**
     * <code>optional .doris.PList list_type = 11;</code>
     * @return The listType.
     */
    org.apache.doris.proto.Types.PList getListType();
    /**
     * <code>optional .doris.PList list_type = 11;</code>
     */
    org.apache.doris.proto.Types.PListOrBuilder getListTypeOrBuilder();

    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     * @return Whether the mapType field is set.
     */
    boolean hasMapType();
    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     * @return The mapType.
     */
    org.apache.doris.proto.Types.PMap getMapType();
    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     */
    org.apache.doris.proto.Types.PMapOrBuilder getMapTypeOrBuilder();

    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     * @return Whether the structType field is set.
     */
    boolean hasStructType();
    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     * @return The structType.
     */
    org.apache.doris.proto.Types.PStruct getStructType();
    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     */
    org.apache.doris.proto.Types.PStructOrBuilder getStructTypeOrBuilder();

    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     * @return Whether the decimalType field is set.
     */
    boolean hasDecimalType();
    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     * @return The decimalType.
     */
    org.apache.doris.proto.Types.PDecimal getDecimalType();
    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     */
    org.apache.doris.proto.Types.PDecimalOrBuilder getDecimalTypeOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PGenericType}
   */
  public  static final class PGenericType extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PGenericType)
      PGenericTypeOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PGenericType.newBuilder() to construct.
    private PGenericType(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PGenericType() {
      id_ = 0;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PGenericType();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PGenericType(
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
            case 16: {
              int rawValue = input.readEnum();
                @SuppressWarnings("deprecation")
              org.apache.doris.proto.Types.PGenericType.TypeId value = org.apache.doris.proto.Types.PGenericType.TypeId.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(2, rawValue);
              } else {
                bitField0_ |= 0x00000001;
                id_ = rawValue;
              }
              break;
            }
            case 90: {
              org.apache.doris.proto.Types.PList.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = listType_.toBuilder();
              }
              listType_ = input.readMessage(org.apache.doris.proto.Types.PList.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(listType_);
                listType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            case 98: {
              org.apache.doris.proto.Types.PMap.Builder subBuilder = null;
              if (((bitField0_ & 0x00000004) != 0)) {
                subBuilder = mapType_.toBuilder();
              }
              mapType_ = input.readMessage(org.apache.doris.proto.Types.PMap.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(mapType_);
                mapType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000004;
              break;
            }
            case 106: {
              org.apache.doris.proto.Types.PStruct.Builder subBuilder = null;
              if (((bitField0_ & 0x00000008) != 0)) {
                subBuilder = structType_.toBuilder();
              }
              structType_ = input.readMessage(org.apache.doris.proto.Types.PStruct.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(structType_);
                structType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000008;
              break;
            }
            case 114: {
              org.apache.doris.proto.Types.PDecimal.Builder subBuilder = null;
              if (((bitField0_ & 0x00000010) != 0)) {
                subBuilder = decimalType_.toBuilder();
              }
              decimalType_ = input.readMessage(org.apache.doris.proto.Types.PDecimal.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(decimalType_);
                decimalType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000010;
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
      return org.apache.doris.proto.Types.internal_static_doris_PGenericType_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PGenericType_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PGenericType.class, org.apache.doris.proto.Types.PGenericType.Builder.class);
    }

    /**
     * Protobuf enum {@code doris.PGenericType.TypeId}
     */
    public enum TypeId
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>UINT8 = 0;</code>
       */
      UINT8(0),
      /**
       * <code>UINT16 = 1;</code>
       */
      UINT16(1),
      /**
       * <code>UINT32 = 2;</code>
       */
      UINT32(2),
      /**
       * <code>UINT64 = 3;</code>
       */
      UINT64(3),
      /**
       * <code>UINT128 = 4;</code>
       */
      UINT128(4),
      /**
       * <code>UINT256 = 5;</code>
       */
      UINT256(5),
      /**
       * <code>INT8 = 6;</code>
       */
      INT8(6),
      /**
       * <code>INT16 = 7;</code>
       */
      INT16(7),
      /**
       * <code>INT32 = 8;</code>
       */
      INT32(8),
      /**
       * <code>INT64 = 9;</code>
       */
      INT64(9),
      /**
       * <code>INT128 = 10;</code>
       */
      INT128(10),
      /**
       * <code>INT256 = 11;</code>
       */
      INT256(11),
      /**
       * <code>FLOAT = 12;</code>
       */
      FLOAT(12),
      /**
       * <code>DOUBLE = 13;</code>
       */
      DOUBLE(13),
      /**
       * <code>BOOLEAN = 14;</code>
       */
      BOOLEAN(14),
      /**
       * <code>DATE = 15;</code>
       */
      DATE(15),
      /**
       * <code>DATETIME = 16;</code>
       */
      DATETIME(16),
      /**
       * <code>HLL = 17;</code>
       */
      HLL(17),
      /**
       * <code>BITMAP = 18;</code>
       */
      BITMAP(18),
      /**
       * <code>LIST = 19;</code>
       */
      LIST(19),
      /**
       * <code>MAP = 20;</code>
       */
      MAP(20),
      /**
       * <code>STRUCT = 21;</code>
       */
      STRUCT(21),
      /**
       * <code>STRING = 22;</code>
       */
      STRING(22),
      /**
       * <code>DECIMAL32 = 23;</code>
       */
      DECIMAL32(23),
      /**
       * <code>DECIMAL64 = 24;</code>
       */
      DECIMAL64(24),
      /**
       * <code>DECIMAL128 = 25;</code>
       */
      DECIMAL128(25),
      /**
       * <code>BYTES = 26;</code>
       */
      BYTES(26),
      /**
       * <code>NOTHING = 27;</code>
       */
      NOTHING(27),
      /**
       * <code>UNKNOWN = 999;</code>
       */
      UNKNOWN(999),
      ;

      /**
       * <code>UINT8 = 0;</code>
       */
      public static final int UINT8_VALUE = 0;
      /**
       * <code>UINT16 = 1;</code>
       */
      public static final int UINT16_VALUE = 1;
      /**
       * <code>UINT32 = 2;</code>
       */
      public static final int UINT32_VALUE = 2;
      /**
       * <code>UINT64 = 3;</code>
       */
      public static final int UINT64_VALUE = 3;
      /**
       * <code>UINT128 = 4;</code>
       */
      public static final int UINT128_VALUE = 4;
      /**
       * <code>UINT256 = 5;</code>
       */
      public static final int UINT256_VALUE = 5;
      /**
       * <code>INT8 = 6;</code>
       */
      public static final int INT8_VALUE = 6;
      /**
       * <code>INT16 = 7;</code>
       */
      public static final int INT16_VALUE = 7;
      /**
       * <code>INT32 = 8;</code>
       */
      public static final int INT32_VALUE = 8;
      /**
       * <code>INT64 = 9;</code>
       */
      public static final int INT64_VALUE = 9;
      /**
       * <code>INT128 = 10;</code>
       */
      public static final int INT128_VALUE = 10;
      /**
       * <code>INT256 = 11;</code>
       */
      public static final int INT256_VALUE = 11;
      /**
       * <code>FLOAT = 12;</code>
       */
      public static final int FLOAT_VALUE = 12;
      /**
       * <code>DOUBLE = 13;</code>
       */
      public static final int DOUBLE_VALUE = 13;
      /**
       * <code>BOOLEAN = 14;</code>
       */
      public static final int BOOLEAN_VALUE = 14;
      /**
       * <code>DATE = 15;</code>
       */
      public static final int DATE_VALUE = 15;
      /**
       * <code>DATETIME = 16;</code>
       */
      public static final int DATETIME_VALUE = 16;
      /**
       * <code>HLL = 17;</code>
       */
      public static final int HLL_VALUE = 17;
      /**
       * <code>BITMAP = 18;</code>
       */
      public static final int BITMAP_VALUE = 18;
      /**
       * <code>LIST = 19;</code>
       */
      public static final int LIST_VALUE = 19;
      /**
       * <code>MAP = 20;</code>
       */
      public static final int MAP_VALUE = 20;
      /**
       * <code>STRUCT = 21;</code>
       */
      public static final int STRUCT_VALUE = 21;
      /**
       * <code>STRING = 22;</code>
       */
      public static final int STRING_VALUE = 22;
      /**
       * <code>DECIMAL32 = 23;</code>
       */
      public static final int DECIMAL32_VALUE = 23;
      /**
       * <code>DECIMAL64 = 24;</code>
       */
      public static final int DECIMAL64_VALUE = 24;
      /**
       * <code>DECIMAL128 = 25;</code>
       */
      public static final int DECIMAL128_VALUE = 25;
      /**
       * <code>BYTES = 26;</code>
       */
      public static final int BYTES_VALUE = 26;
      /**
       * <code>NOTHING = 27;</code>
       */
      public static final int NOTHING_VALUE = 27;
      /**
       * <code>UNKNOWN = 999;</code>
       */
      public static final int UNKNOWN_VALUE = 999;


      public final int getNumber() {
        return value;
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       * @deprecated Use {@link #forNumber(int)} instead.
       */
      @java.lang.Deprecated
      public static TypeId valueOf(int value) {
        return forNumber(value);
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       */
      public static TypeId forNumber(int value) {
        switch (value) {
          case 0: return UINT8;
          case 1: return UINT16;
          case 2: return UINT32;
          case 3: return UINT64;
          case 4: return UINT128;
          case 5: return UINT256;
          case 6: return INT8;
          case 7: return INT16;
          case 8: return INT32;
          case 9: return INT64;
          case 10: return INT128;
          case 11: return INT256;
          case 12: return FLOAT;
          case 13: return DOUBLE;
          case 14: return BOOLEAN;
          case 15: return DATE;
          case 16: return DATETIME;
          case 17: return HLL;
          case 18: return BITMAP;
          case 19: return LIST;
          case 20: return MAP;
          case 21: return STRUCT;
          case 22: return STRING;
          case 23: return DECIMAL32;
          case 24: return DECIMAL64;
          case 25: return DECIMAL128;
          case 26: return BYTES;
          case 27: return NOTHING;
          case 999: return UNKNOWN;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<TypeId>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static final com.google.protobuf.Internal.EnumLiteMap<
          TypeId> internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<TypeId>() {
              public TypeId findValueByNumber(int number) {
                return TypeId.forNumber(number);
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
        return org.apache.doris.proto.Types.PGenericType.getDescriptor().getEnumTypes().get(0);
      }

      private static final TypeId[] VALUES = values();

      public static TypeId valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int value;

      private TypeId(int value) {
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:doris.PGenericType.TypeId)
    }

    private int bitField0_;
    public static final int ID_FIELD_NUMBER = 2;
    private int id_;
    /**
     * <code>required .doris.PGenericType.TypeId id = 2;</code>
     * @return Whether the id field is set.
     */
    public boolean hasId() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType.TypeId id = 2;</code>
     * @return The id.
     */
    public org.apache.doris.proto.Types.PGenericType.TypeId getId() {
      @SuppressWarnings("deprecation")
      org.apache.doris.proto.Types.PGenericType.TypeId result = org.apache.doris.proto.Types.PGenericType.TypeId.valueOf(id_);
      return result == null ? org.apache.doris.proto.Types.PGenericType.TypeId.UINT8 : result;
    }

    public static final int LIST_TYPE_FIELD_NUMBER = 11;
    private org.apache.doris.proto.Types.PList listType_;
    /**
     * <code>optional .doris.PList list_type = 11;</code>
     * @return Whether the listType field is set.
     */
    public boolean hasListType() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PList list_type = 11;</code>
     * @return The listType.
     */
    public org.apache.doris.proto.Types.PList getListType() {
      return listType_ == null ? org.apache.doris.proto.Types.PList.getDefaultInstance() : listType_;
    }
    /**
     * <code>optional .doris.PList list_type = 11;</code>
     */
    public org.apache.doris.proto.Types.PListOrBuilder getListTypeOrBuilder() {
      return listType_ == null ? org.apache.doris.proto.Types.PList.getDefaultInstance() : listType_;
    }

    public static final int MAP_TYPE_FIELD_NUMBER = 12;
    private org.apache.doris.proto.Types.PMap mapType_;
    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     * @return Whether the mapType field is set.
     */
    public boolean hasMapType() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     * @return The mapType.
     */
    public org.apache.doris.proto.Types.PMap getMapType() {
      return mapType_ == null ? org.apache.doris.proto.Types.PMap.getDefaultInstance() : mapType_;
    }
    /**
     * <code>optional .doris.PMap map_type = 12;</code>
     */
    public org.apache.doris.proto.Types.PMapOrBuilder getMapTypeOrBuilder() {
      return mapType_ == null ? org.apache.doris.proto.Types.PMap.getDefaultInstance() : mapType_;
    }

    public static final int STRUCT_TYPE_FIELD_NUMBER = 13;
    private org.apache.doris.proto.Types.PStruct structType_;
    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     * @return Whether the structType field is set.
     */
    public boolean hasStructType() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     * @return The structType.
     */
    public org.apache.doris.proto.Types.PStruct getStructType() {
      return structType_ == null ? org.apache.doris.proto.Types.PStruct.getDefaultInstance() : structType_;
    }
    /**
     * <code>optional .doris.PStruct struct_type = 13;</code>
     */
    public org.apache.doris.proto.Types.PStructOrBuilder getStructTypeOrBuilder() {
      return structType_ == null ? org.apache.doris.proto.Types.PStruct.getDefaultInstance() : structType_;
    }

    public static final int DECIMAL_TYPE_FIELD_NUMBER = 14;
    private org.apache.doris.proto.Types.PDecimal decimalType_;
    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     * @return Whether the decimalType field is set.
     */
    public boolean hasDecimalType() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     * @return The decimalType.
     */
    public org.apache.doris.proto.Types.PDecimal getDecimalType() {
      return decimalType_ == null ? org.apache.doris.proto.Types.PDecimal.getDefaultInstance() : decimalType_;
    }
    /**
     * <code>optional .doris.PDecimal decimal_type = 14;</code>
     */
    public org.apache.doris.proto.Types.PDecimalOrBuilder getDecimalTypeOrBuilder() {
      return decimalType_ == null ? org.apache.doris.proto.Types.PDecimal.getDefaultInstance() : decimalType_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasId()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (hasListType()) {
        if (!getListType().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasMapType()) {
        if (!getMapType().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasStructType()) {
        if (!getStructType().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasDecimalType()) {
        if (!getDecimalType().isInitialized()) {
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
        output.writeEnum(2, id_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(11, getListType());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeMessage(12, getMapType());
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeMessage(13, getStructType());
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeMessage(14, getDecimalType());
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
          .computeEnumSize(2, id_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(11, getListType());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(12, getMapType());
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(13, getStructType());
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(14, getDecimalType());
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
      if (!(obj instanceof org.apache.doris.proto.Types.PGenericType)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PGenericType other = (org.apache.doris.proto.Types.PGenericType) obj;

      if (hasId() != other.hasId()) return false;
      if (hasId()) {
        if (id_ != other.id_) return false;
      }
      if (hasListType() != other.hasListType()) return false;
      if (hasListType()) {
        if (!getListType()
            .equals(other.getListType())) return false;
      }
      if (hasMapType() != other.hasMapType()) return false;
      if (hasMapType()) {
        if (!getMapType()
            .equals(other.getMapType())) return false;
      }
      if (hasStructType() != other.hasStructType()) return false;
      if (hasStructType()) {
        if (!getStructType()
            .equals(other.getStructType())) return false;
      }
      if (hasDecimalType() != other.hasDecimalType()) return false;
      if (hasDecimalType()) {
        if (!getDecimalType()
            .equals(other.getDecimalType())) return false;
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
        hash = (53 * hash) + id_;
      }
      if (hasListType()) {
        hash = (37 * hash) + LIST_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getListType().hashCode();
      }
      if (hasMapType()) {
        hash = (37 * hash) + MAP_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getMapType().hashCode();
      }
      if (hasStructType()) {
        hash = (37 * hash) + STRUCT_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getStructType().hashCode();
      }
      if (hasDecimalType()) {
        hash = (37 * hash) + DECIMAL_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getDecimalType().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PGenericType parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PGenericType parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PGenericType parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PGenericType prototype) {
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
     * Protobuf type {@code doris.PGenericType}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PGenericType)
        org.apache.doris.proto.Types.PGenericTypeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PGenericType_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PGenericType_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PGenericType.class, org.apache.doris.proto.Types.PGenericType.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PGenericType.newBuilder()
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
          getListTypeFieldBuilder();
          getMapTypeFieldBuilder();
          getStructTypeFieldBuilder();
          getDecimalTypeFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        id_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        if (listTypeBuilder_ == null) {
          listType_ = null;
        } else {
          listTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        if (mapTypeBuilder_ == null) {
          mapType_ = null;
        } else {
          mapTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        if (structTypeBuilder_ == null) {
          structType_ = null;
        } else {
          structTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000008);
        if (decimalTypeBuilder_ == null) {
          decimalType_ = null;
        } else {
          decimalTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PGenericType_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PGenericType getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PGenericType.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PGenericType build() {
        org.apache.doris.proto.Types.PGenericType result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PGenericType buildPartial() {
        org.apache.doris.proto.Types.PGenericType result = new org.apache.doris.proto.Types.PGenericType(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.id_ = id_;
        if (((from_bitField0_ & 0x00000002) != 0)) {
          if (listTypeBuilder_ == null) {
            result.listType_ = listType_;
          } else {
            result.listType_ = listTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          if (mapTypeBuilder_ == null) {
            result.mapType_ = mapType_;
          } else {
            result.mapType_ = mapTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          if (structTypeBuilder_ == null) {
            result.structType_ = structType_;
          } else {
            result.structType_ = structTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          if (decimalTypeBuilder_ == null) {
            result.decimalType_ = decimalType_;
          } else {
            result.decimalType_ = decimalTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000010;
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
        if (other instanceof org.apache.doris.proto.Types.PGenericType) {
          return mergeFrom((org.apache.doris.proto.Types.PGenericType)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PGenericType other) {
        if (other == org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) return this;
        if (other.hasId()) {
          setId(other.getId());
        }
        if (other.hasListType()) {
          mergeListType(other.getListType());
        }
        if (other.hasMapType()) {
          mergeMapType(other.getMapType());
        }
        if (other.hasStructType()) {
          mergeStructType(other.getStructType());
        }
        if (other.hasDecimalType()) {
          mergeDecimalType(other.getDecimalType());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasId()) {
          return false;
        }
        if (hasListType()) {
          if (!getListType().isInitialized()) {
            return false;
          }
        }
        if (hasMapType()) {
          if (!getMapType().isInitialized()) {
            return false;
          }
        }
        if (hasStructType()) {
          if (!getStructType().isInitialized()) {
            return false;
          }
        }
        if (hasDecimalType()) {
          if (!getDecimalType().isInitialized()) {
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
        org.apache.doris.proto.Types.PGenericType parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PGenericType) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int id_ = 0;
      /**
       * <code>required .doris.PGenericType.TypeId id = 2;</code>
       * @return Whether the id field is set.
       */
      public boolean hasId() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType.TypeId id = 2;</code>
       * @return The id.
       */
      public org.apache.doris.proto.Types.PGenericType.TypeId getId() {
        @SuppressWarnings("deprecation")
        org.apache.doris.proto.Types.PGenericType.TypeId result = org.apache.doris.proto.Types.PGenericType.TypeId.valueOf(id_);
        return result == null ? org.apache.doris.proto.Types.PGenericType.TypeId.UINT8 : result;
      }
      /**
       * <code>required .doris.PGenericType.TypeId id = 2;</code>
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(org.apache.doris.proto.Types.PGenericType.TypeId value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000001;
        id_ = value.getNumber();
        onChanged();
        return this;
      }
      /**
       * <code>required .doris.PGenericType.TypeId id = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearId() {
        bitField0_ = (bitField0_ & ~0x00000001);
        id_ = 0;
        onChanged();
        return this;
      }

      private org.apache.doris.proto.Types.PList listType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PList, org.apache.doris.proto.Types.PList.Builder, org.apache.doris.proto.Types.PListOrBuilder> listTypeBuilder_;
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       * @return Whether the listType field is set.
       */
      public boolean hasListType() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       * @return The listType.
       */
      public org.apache.doris.proto.Types.PList getListType() {
        if (listTypeBuilder_ == null) {
          return listType_ == null ? org.apache.doris.proto.Types.PList.getDefaultInstance() : listType_;
        } else {
          return listTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public Builder setListType(org.apache.doris.proto.Types.PList value) {
        if (listTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          listType_ = value;
          onChanged();
        } else {
          listTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public Builder setListType(
          org.apache.doris.proto.Types.PList.Builder builderForValue) {
        if (listTypeBuilder_ == null) {
          listType_ = builderForValue.build();
          onChanged();
        } else {
          listTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public Builder mergeListType(org.apache.doris.proto.Types.PList value) {
        if (listTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0) &&
              listType_ != null &&
              listType_ != org.apache.doris.proto.Types.PList.getDefaultInstance()) {
            listType_ =
              org.apache.doris.proto.Types.PList.newBuilder(listType_).mergeFrom(value).buildPartial();
          } else {
            listType_ = value;
          }
          onChanged();
        } else {
          listTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public Builder clearListType() {
        if (listTypeBuilder_ == null) {
          listType_ = null;
          onChanged();
        } else {
          listTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public org.apache.doris.proto.Types.PList.Builder getListTypeBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getListTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      public org.apache.doris.proto.Types.PListOrBuilder getListTypeOrBuilder() {
        if (listTypeBuilder_ != null) {
          return listTypeBuilder_.getMessageOrBuilder();
        } else {
          return listType_ == null ?
              org.apache.doris.proto.Types.PList.getDefaultInstance() : listType_;
        }
      }
      /**
       * <code>optional .doris.PList list_type = 11;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PList, org.apache.doris.proto.Types.PList.Builder, org.apache.doris.proto.Types.PListOrBuilder> 
          getListTypeFieldBuilder() {
        if (listTypeBuilder_ == null) {
          listTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PList, org.apache.doris.proto.Types.PList.Builder, org.apache.doris.proto.Types.PListOrBuilder>(
                  getListType(),
                  getParentForChildren(),
                  isClean());
          listType_ = null;
        }
        return listTypeBuilder_;
      }

      private org.apache.doris.proto.Types.PMap mapType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PMap, org.apache.doris.proto.Types.PMap.Builder, org.apache.doris.proto.Types.PMapOrBuilder> mapTypeBuilder_;
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       * @return Whether the mapType field is set.
       */
      public boolean hasMapType() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       * @return The mapType.
       */
      public org.apache.doris.proto.Types.PMap getMapType() {
        if (mapTypeBuilder_ == null) {
          return mapType_ == null ? org.apache.doris.proto.Types.PMap.getDefaultInstance() : mapType_;
        } else {
          return mapTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public Builder setMapType(org.apache.doris.proto.Types.PMap value) {
        if (mapTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          mapType_ = value;
          onChanged();
        } else {
          mapTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public Builder setMapType(
          org.apache.doris.proto.Types.PMap.Builder builderForValue) {
        if (mapTypeBuilder_ == null) {
          mapType_ = builderForValue.build();
          onChanged();
        } else {
          mapTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public Builder mergeMapType(org.apache.doris.proto.Types.PMap value) {
        if (mapTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0) &&
              mapType_ != null &&
              mapType_ != org.apache.doris.proto.Types.PMap.getDefaultInstance()) {
            mapType_ =
              org.apache.doris.proto.Types.PMap.newBuilder(mapType_).mergeFrom(value).buildPartial();
          } else {
            mapType_ = value;
          }
          onChanged();
        } else {
          mapTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public Builder clearMapType() {
        if (mapTypeBuilder_ == null) {
          mapType_ = null;
          onChanged();
        } else {
          mapTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public org.apache.doris.proto.Types.PMap.Builder getMapTypeBuilder() {
        bitField0_ |= 0x00000004;
        onChanged();
        return getMapTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      public org.apache.doris.proto.Types.PMapOrBuilder getMapTypeOrBuilder() {
        if (mapTypeBuilder_ != null) {
          return mapTypeBuilder_.getMessageOrBuilder();
        } else {
          return mapType_ == null ?
              org.apache.doris.proto.Types.PMap.getDefaultInstance() : mapType_;
        }
      }
      /**
       * <code>optional .doris.PMap map_type = 12;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PMap, org.apache.doris.proto.Types.PMap.Builder, org.apache.doris.proto.Types.PMapOrBuilder> 
          getMapTypeFieldBuilder() {
        if (mapTypeBuilder_ == null) {
          mapTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PMap, org.apache.doris.proto.Types.PMap.Builder, org.apache.doris.proto.Types.PMapOrBuilder>(
                  getMapType(),
                  getParentForChildren(),
                  isClean());
          mapType_ = null;
        }
        return mapTypeBuilder_;
      }

      private org.apache.doris.proto.Types.PStruct structType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStruct, org.apache.doris.proto.Types.PStruct.Builder, org.apache.doris.proto.Types.PStructOrBuilder> structTypeBuilder_;
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       * @return Whether the structType field is set.
       */
      public boolean hasStructType() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       * @return The structType.
       */
      public org.apache.doris.proto.Types.PStruct getStructType() {
        if (structTypeBuilder_ == null) {
          return structType_ == null ? org.apache.doris.proto.Types.PStruct.getDefaultInstance() : structType_;
        } else {
          return structTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public Builder setStructType(org.apache.doris.proto.Types.PStruct value) {
        if (structTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          structType_ = value;
          onChanged();
        } else {
          structTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000008;
        return this;
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public Builder setStructType(
          org.apache.doris.proto.Types.PStruct.Builder builderForValue) {
        if (structTypeBuilder_ == null) {
          structType_ = builderForValue.build();
          onChanged();
        } else {
          structTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000008;
        return this;
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public Builder mergeStructType(org.apache.doris.proto.Types.PStruct value) {
        if (structTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000008) != 0) &&
              structType_ != null &&
              structType_ != org.apache.doris.proto.Types.PStruct.getDefaultInstance()) {
            structType_ =
              org.apache.doris.proto.Types.PStruct.newBuilder(structType_).mergeFrom(value).buildPartial();
          } else {
            structType_ = value;
          }
          onChanged();
        } else {
          structTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000008;
        return this;
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public Builder clearStructType() {
        if (structTypeBuilder_ == null) {
          structType_ = null;
          onChanged();
        } else {
          structTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000008);
        return this;
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public org.apache.doris.proto.Types.PStruct.Builder getStructTypeBuilder() {
        bitField0_ |= 0x00000008;
        onChanged();
        return getStructTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      public org.apache.doris.proto.Types.PStructOrBuilder getStructTypeOrBuilder() {
        if (structTypeBuilder_ != null) {
          return structTypeBuilder_.getMessageOrBuilder();
        } else {
          return structType_ == null ?
              org.apache.doris.proto.Types.PStruct.getDefaultInstance() : structType_;
        }
      }
      /**
       * <code>optional .doris.PStruct struct_type = 13;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PStruct, org.apache.doris.proto.Types.PStruct.Builder, org.apache.doris.proto.Types.PStructOrBuilder> 
          getStructTypeFieldBuilder() {
        if (structTypeBuilder_ == null) {
          structTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PStruct, org.apache.doris.proto.Types.PStruct.Builder, org.apache.doris.proto.Types.PStructOrBuilder>(
                  getStructType(),
                  getParentForChildren(),
                  isClean());
          structType_ = null;
        }
        return structTypeBuilder_;
      }

      private org.apache.doris.proto.Types.PDecimal decimalType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PDecimal, org.apache.doris.proto.Types.PDecimal.Builder, org.apache.doris.proto.Types.PDecimalOrBuilder> decimalTypeBuilder_;
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       * @return Whether the decimalType field is set.
       */
      public boolean hasDecimalType() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       * @return The decimalType.
       */
      public org.apache.doris.proto.Types.PDecimal getDecimalType() {
        if (decimalTypeBuilder_ == null) {
          return decimalType_ == null ? org.apache.doris.proto.Types.PDecimal.getDefaultInstance() : decimalType_;
        } else {
          return decimalTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public Builder setDecimalType(org.apache.doris.proto.Types.PDecimal value) {
        if (decimalTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          decimalType_ = value;
          onChanged();
        } else {
          decimalTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public Builder setDecimalType(
          org.apache.doris.proto.Types.PDecimal.Builder builderForValue) {
        if (decimalTypeBuilder_ == null) {
          decimalType_ = builderForValue.build();
          onChanged();
        } else {
          decimalTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public Builder mergeDecimalType(org.apache.doris.proto.Types.PDecimal value) {
        if (decimalTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000010) != 0) &&
              decimalType_ != null &&
              decimalType_ != org.apache.doris.proto.Types.PDecimal.getDefaultInstance()) {
            decimalType_ =
              org.apache.doris.proto.Types.PDecimal.newBuilder(decimalType_).mergeFrom(value).buildPartial();
          } else {
            decimalType_ = value;
          }
          onChanged();
        } else {
          decimalTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public Builder clearDecimalType() {
        if (decimalTypeBuilder_ == null) {
          decimalType_ = null;
          onChanged();
        } else {
          decimalTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public org.apache.doris.proto.Types.PDecimal.Builder getDecimalTypeBuilder() {
        bitField0_ |= 0x00000010;
        onChanged();
        return getDecimalTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      public org.apache.doris.proto.Types.PDecimalOrBuilder getDecimalTypeOrBuilder() {
        if (decimalTypeBuilder_ != null) {
          return decimalTypeBuilder_.getMessageOrBuilder();
        } else {
          return decimalType_ == null ?
              org.apache.doris.proto.Types.PDecimal.getDefaultInstance() : decimalType_;
        }
      }
      /**
       * <code>optional .doris.PDecimal decimal_type = 14;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PDecimal, org.apache.doris.proto.Types.PDecimal.Builder, org.apache.doris.proto.Types.PDecimalOrBuilder> 
          getDecimalTypeFieldBuilder() {
        if (decimalTypeBuilder_ == null) {
          decimalTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PDecimal, org.apache.doris.proto.Types.PDecimal.Builder, org.apache.doris.proto.Types.PDecimalOrBuilder>(
                  getDecimalType(),
                  getParentForChildren(),
                  isClean());
          decimalType_ = null;
        }
        return decimalTypeBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PGenericType)
    }

    // @@protoc_insertion_point(class_scope:doris.PGenericType)
    private static final org.apache.doris.proto.Types.PGenericType DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PGenericType();
    }

    public static org.apache.doris.proto.Types.PGenericType getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PGenericType>
        PARSER = new com.google.protobuf.AbstractParser<PGenericType>() {
      @java.lang.Override
      public PGenericType parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PGenericType(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PGenericType> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PGenericType> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PGenericType getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PListOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PList)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     * @return Whether the elementType field is set.
     */
    boolean hasElementType();
    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     * @return The elementType.
     */
    org.apache.doris.proto.Types.PGenericType getElementType();
    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getElementTypeOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PList}
   */
  public  static final class PList extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PList)
      PListOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PList.newBuilder() to construct.
    private PList(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PList() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PList();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PList(
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
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = elementType_.toBuilder();
              }
              elementType_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(elementType_);
                elementType_ = subBuilder.buildPartial();
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
      return org.apache.doris.proto.Types.internal_static_doris_PList_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PList_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PList.class, org.apache.doris.proto.Types.PList.Builder.class);
    }

    private int bitField0_;
    public static final int ELEMENT_TYPE_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PGenericType elementType_;
    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     * @return Whether the elementType field is set.
     */
    public boolean hasElementType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     * @return The elementType.
     */
    public org.apache.doris.proto.Types.PGenericType getElementType() {
      return elementType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : elementType_;
    }
    /**
     * <code>required .doris.PGenericType element_type = 1;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getElementTypeOrBuilder() {
      return elementType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : elementType_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasElementType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getElementType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getElementType());
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
          .computeMessageSize(1, getElementType());
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
      if (!(obj instanceof org.apache.doris.proto.Types.PList)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PList other = (org.apache.doris.proto.Types.PList) obj;

      if (hasElementType() != other.hasElementType()) return false;
      if (hasElementType()) {
        if (!getElementType()
            .equals(other.getElementType())) return false;
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
      if (hasElementType()) {
        hash = (37 * hash) + ELEMENT_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getElementType().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PList parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PList parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PList parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PList parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PList prototype) {
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
     * Protobuf type {@code doris.PList}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PList)
        org.apache.doris.proto.Types.PListOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PList_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PList_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PList.class, org.apache.doris.proto.Types.PList.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PList.newBuilder()
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
          getElementTypeFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (elementTypeBuilder_ == null) {
          elementType_ = null;
        } else {
          elementTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PList_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PList getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PList.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PList build() {
        org.apache.doris.proto.Types.PList result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PList buildPartial() {
        org.apache.doris.proto.Types.PList result = new org.apache.doris.proto.Types.PList(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (elementTypeBuilder_ == null) {
            result.elementType_ = elementType_;
          } else {
            result.elementType_ = elementTypeBuilder_.build();
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
        if (other instanceof org.apache.doris.proto.Types.PList) {
          return mergeFrom((org.apache.doris.proto.Types.PList)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PList other) {
        if (other == org.apache.doris.proto.Types.PList.getDefaultInstance()) return this;
        if (other.hasElementType()) {
          mergeElementType(other.getElementType());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasElementType()) {
          return false;
        }
        if (!getElementType().isInitialized()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PList parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PList) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PGenericType elementType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> elementTypeBuilder_;
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       * @return Whether the elementType field is set.
       */
      public boolean hasElementType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       * @return The elementType.
       */
      public org.apache.doris.proto.Types.PGenericType getElementType() {
        if (elementTypeBuilder_ == null) {
          return elementType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : elementType_;
        } else {
          return elementTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public Builder setElementType(org.apache.doris.proto.Types.PGenericType value) {
        if (elementTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          elementType_ = value;
          onChanged();
        } else {
          elementTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public Builder setElementType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (elementTypeBuilder_ == null) {
          elementType_ = builderForValue.build();
          onChanged();
        } else {
          elementTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public Builder mergeElementType(org.apache.doris.proto.Types.PGenericType value) {
        if (elementTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              elementType_ != null &&
              elementType_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            elementType_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(elementType_).mergeFrom(value).buildPartial();
          } else {
            elementType_ = value;
          }
          onChanged();
        } else {
          elementTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public Builder clearElementType() {
        if (elementTypeBuilder_ == null) {
          elementType_ = null;
          onChanged();
        } else {
          elementTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getElementTypeBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getElementTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getElementTypeOrBuilder() {
        if (elementTypeBuilder_ != null) {
          return elementTypeBuilder_.getMessageOrBuilder();
        } else {
          return elementType_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : elementType_;
        }
      }
      /**
       * <code>required .doris.PGenericType element_type = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getElementTypeFieldBuilder() {
        if (elementTypeBuilder_ == null) {
          elementTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getElementType(),
                  getParentForChildren(),
                  isClean());
          elementType_ = null;
        }
        return elementTypeBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PList)
    }

    // @@protoc_insertion_point(class_scope:doris.PList)
    private static final org.apache.doris.proto.Types.PList DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PList();
    }

    public static org.apache.doris.proto.Types.PList getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PList>
        PARSER = new com.google.protobuf.AbstractParser<PList>() {
      @java.lang.Override
      public PList parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PList(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PList> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PList> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PList getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PMapOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PMap)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     * @return Whether the keyType field is set.
     */
    boolean hasKeyType();
    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     * @return The keyType.
     */
    org.apache.doris.proto.Types.PGenericType getKeyType();
    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getKeyTypeOrBuilder();

    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     * @return Whether the valueType field is set.
     */
    boolean hasValueType();
    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     * @return The valueType.
     */
    org.apache.doris.proto.Types.PGenericType getValueType();
    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getValueTypeOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PMap}
   */
  public  static final class PMap extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PMap)
      PMapOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PMap.newBuilder() to construct.
    private PMap(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PMap() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PMap();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PMap(
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
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = keyType_.toBuilder();
              }
              keyType_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(keyType_);
                keyType_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 18: {
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = valueType_.toBuilder();
              }
              valueType_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(valueType_);
                valueType_ = subBuilder.buildPartial();
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
      return org.apache.doris.proto.Types.internal_static_doris_PMap_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PMap_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PMap.class, org.apache.doris.proto.Types.PMap.Builder.class);
    }

    private int bitField0_;
    public static final int KEY_TYPE_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PGenericType keyType_;
    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     * @return Whether the keyType field is set.
     */
    public boolean hasKeyType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     * @return The keyType.
     */
    public org.apache.doris.proto.Types.PGenericType getKeyType() {
      return keyType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : keyType_;
    }
    /**
     * <code>required .doris.PGenericType key_type = 1;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getKeyTypeOrBuilder() {
      return keyType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : keyType_;
    }

    public static final int VALUE_TYPE_FIELD_NUMBER = 2;
    private org.apache.doris.proto.Types.PGenericType valueType_;
    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     * @return Whether the valueType field is set.
     */
    public boolean hasValueType() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     * @return The valueType.
     */
    public org.apache.doris.proto.Types.PGenericType getValueType() {
      return valueType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : valueType_;
    }
    /**
     * <code>required .doris.PGenericType value_type = 2;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getValueTypeOrBuilder() {
      return valueType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : valueType_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasKeyType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasValueType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getKeyType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getValueType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getKeyType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(2, getValueType());
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
          .computeMessageSize(1, getKeyType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, getValueType());
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
      if (!(obj instanceof org.apache.doris.proto.Types.PMap)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PMap other = (org.apache.doris.proto.Types.PMap) obj;

      if (hasKeyType() != other.hasKeyType()) return false;
      if (hasKeyType()) {
        if (!getKeyType()
            .equals(other.getKeyType())) return false;
      }
      if (hasValueType() != other.hasValueType()) return false;
      if (hasValueType()) {
        if (!getValueType()
            .equals(other.getValueType())) return false;
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
      if (hasKeyType()) {
        hash = (37 * hash) + KEY_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getKeyType().hashCode();
      }
      if (hasValueType()) {
        hash = (37 * hash) + VALUE_TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getValueType().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PMap parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PMap parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PMap parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PMap parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PMap prototype) {
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
     * Protobuf type {@code doris.PMap}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PMap)
        org.apache.doris.proto.Types.PMapOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PMap_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PMap_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PMap.class, org.apache.doris.proto.Types.PMap.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PMap.newBuilder()
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
          getKeyTypeFieldBuilder();
          getValueTypeFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (keyTypeBuilder_ == null) {
          keyType_ = null;
        } else {
          keyTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        if (valueTypeBuilder_ == null) {
          valueType_ = null;
        } else {
          valueTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PMap_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PMap getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PMap.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PMap build() {
        org.apache.doris.proto.Types.PMap result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PMap buildPartial() {
        org.apache.doris.proto.Types.PMap result = new org.apache.doris.proto.Types.PMap(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (keyTypeBuilder_ == null) {
            result.keyType_ = keyType_;
          } else {
            result.keyType_ = keyTypeBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          if (valueTypeBuilder_ == null) {
            result.valueType_ = valueType_;
          } else {
            result.valueType_ = valueTypeBuilder_.build();
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
        if (other instanceof org.apache.doris.proto.Types.PMap) {
          return mergeFrom((org.apache.doris.proto.Types.PMap)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PMap other) {
        if (other == org.apache.doris.proto.Types.PMap.getDefaultInstance()) return this;
        if (other.hasKeyType()) {
          mergeKeyType(other.getKeyType());
        }
        if (other.hasValueType()) {
          mergeValueType(other.getValueType());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasKeyType()) {
          return false;
        }
        if (!hasValueType()) {
          return false;
        }
        if (!getKeyType().isInitialized()) {
          return false;
        }
        if (!getValueType().isInitialized()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PMap parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PMap) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PGenericType keyType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> keyTypeBuilder_;
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       * @return Whether the keyType field is set.
       */
      public boolean hasKeyType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       * @return The keyType.
       */
      public org.apache.doris.proto.Types.PGenericType getKeyType() {
        if (keyTypeBuilder_ == null) {
          return keyType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : keyType_;
        } else {
          return keyTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public Builder setKeyType(org.apache.doris.proto.Types.PGenericType value) {
        if (keyTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          keyType_ = value;
          onChanged();
        } else {
          keyTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public Builder setKeyType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (keyTypeBuilder_ == null) {
          keyType_ = builderForValue.build();
          onChanged();
        } else {
          keyTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public Builder mergeKeyType(org.apache.doris.proto.Types.PGenericType value) {
        if (keyTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              keyType_ != null &&
              keyType_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            keyType_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(keyType_).mergeFrom(value).buildPartial();
          } else {
            keyType_ = value;
          }
          onChanged();
        } else {
          keyTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public Builder clearKeyType() {
        if (keyTypeBuilder_ == null) {
          keyType_ = null;
          onChanged();
        } else {
          keyTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getKeyTypeBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getKeyTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getKeyTypeOrBuilder() {
        if (keyTypeBuilder_ != null) {
          return keyTypeBuilder_.getMessageOrBuilder();
        } else {
          return keyType_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : keyType_;
        }
      }
      /**
       * <code>required .doris.PGenericType key_type = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getKeyTypeFieldBuilder() {
        if (keyTypeBuilder_ == null) {
          keyTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getKeyType(),
                  getParentForChildren(),
                  isClean());
          keyType_ = null;
        }
        return keyTypeBuilder_;
      }

      private org.apache.doris.proto.Types.PGenericType valueType_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> valueTypeBuilder_;
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       * @return Whether the valueType field is set.
       */
      public boolean hasValueType() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       * @return The valueType.
       */
      public org.apache.doris.proto.Types.PGenericType getValueType() {
        if (valueTypeBuilder_ == null) {
          return valueType_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : valueType_;
        } else {
          return valueTypeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public Builder setValueType(org.apache.doris.proto.Types.PGenericType value) {
        if (valueTypeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          valueType_ = value;
          onChanged();
        } else {
          valueTypeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public Builder setValueType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (valueTypeBuilder_ == null) {
          valueType_ = builderForValue.build();
          onChanged();
        } else {
          valueTypeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public Builder mergeValueType(org.apache.doris.proto.Types.PGenericType value) {
        if (valueTypeBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0) &&
              valueType_ != null &&
              valueType_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            valueType_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(valueType_).mergeFrom(value).buildPartial();
          } else {
            valueType_ = value;
          }
          onChanged();
        } else {
          valueTypeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000002;
        return this;
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public Builder clearValueType() {
        if (valueTypeBuilder_ == null) {
          valueType_ = null;
          onChanged();
        } else {
          valueTypeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getValueTypeBuilder() {
        bitField0_ |= 0x00000002;
        onChanged();
        return getValueTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getValueTypeOrBuilder() {
        if (valueTypeBuilder_ != null) {
          return valueTypeBuilder_.getMessageOrBuilder();
        } else {
          return valueType_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : valueType_;
        }
      }
      /**
       * <code>required .doris.PGenericType value_type = 2;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getValueTypeFieldBuilder() {
        if (valueTypeBuilder_ == null) {
          valueTypeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getValueType(),
                  getParentForChildren(),
                  isClean());
          valueType_ = null;
        }
        return valueTypeBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PMap)
    }

    // @@protoc_insertion_point(class_scope:doris.PMap)
    private static final org.apache.doris.proto.Types.PMap DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PMap();
    }

    public static org.apache.doris.proto.Types.PMap getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PMap>
        PARSER = new com.google.protobuf.AbstractParser<PMap>() {
      @java.lang.Override
      public PMap parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PMap(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PMap> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PMap> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PMap getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PFieldOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PField)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    org.apache.doris.proto.Types.PGenericType getType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder();

    /**
     * <code>optional string name = 2;</code>
     * @return Whether the name field is set.
     */
    boolean hasName();
    /**
     * <code>optional string name = 2;</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>optional string name = 2;</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();

    /**
     * <code>optional string comment = 3;</code>
     * @return Whether the comment field is set.
     */
    boolean hasComment();
    /**
     * <code>optional string comment = 3;</code>
     * @return The comment.
     */
    java.lang.String getComment();
    /**
     * <code>optional string comment = 3;</code>
     * @return The bytes for comment.
     */
    com.google.protobuf.ByteString
        getCommentBytes();
  }
  /**
   * Protobuf type {@code doris.PField}
   */
  public  static final class PField extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PField)
      PFieldOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PField.newBuilder() to construct.
    private PField(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PField() {
      name_ = "";
      comment_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PField();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PField(
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
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = type_.toBuilder();
              }
              type_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(type_);
                type_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              name_ = bs;
              break;
            }
            case 26: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000004;
              comment_ = bs;
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
      return org.apache.doris.proto.Types.internal_static_doris_PField_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PField_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PField.class, org.apache.doris.proto.Types.PField.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PGenericType type_;
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    public org.apache.doris.proto.Types.PGenericType getType() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }

    public static final int NAME_FIELD_NUMBER = 2;
    private volatile java.lang.Object name_;
    /**
     * <code>optional string name = 2;</code>
     * @return Whether the name field is set.
     */
    public boolean hasName() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string name = 2;</code>
     * @return The name.
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          name_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string name = 2;</code>
     * @return The bytes for name.
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int COMMENT_FIELD_NUMBER = 3;
    private volatile java.lang.Object comment_;
    /**
     * <code>optional string comment = 3;</code>
     * @return Whether the comment field is set.
     */
    public boolean hasComment() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional string comment = 3;</code>
     * @return The comment.
     */
    public java.lang.String getComment() {
      java.lang.Object ref = comment_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          comment_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string comment = 3;</code>
     * @return The bytes for comment.
     */
    public com.google.protobuf.ByteString
        getCommentBytes() {
      java.lang.Object ref = comment_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        comment_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, name_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 3, comment_);
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
          .computeMessageSize(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, name_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, comment_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PField)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PField other = (org.apache.doris.proto.Types.PField) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (!getType()
            .equals(other.getType())) return false;
      }
      if (hasName() != other.hasName()) return false;
      if (hasName()) {
        if (!getName()
            .equals(other.getName())) return false;
      }
      if (hasComment() != other.hasComment()) return false;
      if (hasComment()) {
        if (!getComment()
            .equals(other.getComment())) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType().hashCode();
      }
      if (hasName()) {
        hash = (37 * hash) + NAME_FIELD_NUMBER;
        hash = (53 * hash) + getName().hashCode();
      }
      if (hasComment()) {
        hash = (37 * hash) + COMMENT_FIELD_NUMBER;
        hash = (53 * hash) + getComment().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PField parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PField parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PField parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PField parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PField prototype) {
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
     * Protobuf type {@code doris.PField}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PField)
        org.apache.doris.proto.Types.PFieldOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PField_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PField_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PField.class, org.apache.doris.proto.Types.PField.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PField.newBuilder()
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
          getTypeFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (typeBuilder_ == null) {
          type_ = null;
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        name_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        comment_ = "";
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PField_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PField getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PField.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PField build() {
        org.apache.doris.proto.Types.PField result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PField buildPartial() {
        org.apache.doris.proto.Types.PField result = new org.apache.doris.proto.Types.PField(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (typeBuilder_ == null) {
            result.type_ = type_;
          } else {
            result.type_ = typeBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          to_bitField0_ |= 0x00000002;
        }
        result.name_ = name_;
        if (((from_bitField0_ & 0x00000004) != 0)) {
          to_bitField0_ |= 0x00000004;
        }
        result.comment_ = comment_;
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
        if (other instanceof org.apache.doris.proto.Types.PField) {
          return mergeFrom((org.apache.doris.proto.Types.PField)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PField other) {
        if (other == org.apache.doris.proto.Types.PField.getDefaultInstance()) return this;
        if (other.hasType()) {
          mergeType(other.getType());
        }
        if (other.hasName()) {
          bitField0_ |= 0x00000002;
          name_ = other.name_;
          onChanged();
        }
        if (other.hasComment()) {
          bitField0_ |= 0x00000004;
          comment_ = other.comment_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        if (!getType().isInitialized()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PField parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PField) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PGenericType type_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> typeBuilder_;
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return The type.
       */
      public org.apache.doris.proto.Types.PGenericType getType() {
        if (typeBuilder_ == null) {
          return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        } else {
          return typeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          type_ = value;
          onChanged();
        } else {
          typeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (typeBuilder_ == null) {
          type_ = builderForValue.build();
          onChanged();
        } else {
          typeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder mergeType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              type_ != null &&
              type_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            type_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(type_).mergeFrom(value).buildPartial();
          } else {
            type_ = value;
          }
          onChanged();
        } else {
          typeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder clearType() {
        if (typeBuilder_ == null) {
          type_ = null;
          onChanged();
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getTypeBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
        if (typeBuilder_ != null) {
          return typeBuilder_.getMessageOrBuilder();
        } else {
          return type_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getTypeFieldBuilder() {
        if (typeBuilder_ == null) {
          typeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getType(),
                  getParentForChildren(),
                  isClean());
          type_ = null;
        }
        return typeBuilder_;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>optional string name = 2;</code>
       * @return Whether the name field is set.
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional string name = 2;</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string name = 2;</code>
       * @return The bytes for name.
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string name = 2;</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string name = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        bitField0_ = (bitField0_ & ~0x00000002);
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>optional string name = 2;</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        name_ = value;
        onChanged();
        return this;
      }

      private java.lang.Object comment_ = "";
      /**
       * <code>optional string comment = 3;</code>
       * @return Whether the comment field is set.
       */
      public boolean hasComment() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional string comment = 3;</code>
       * @return The comment.
       */
      public java.lang.String getComment() {
        java.lang.Object ref = comment_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            comment_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string comment = 3;</code>
       * @return The bytes for comment.
       */
      public com.google.protobuf.ByteString
          getCommentBytes() {
        java.lang.Object ref = comment_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          comment_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string comment = 3;</code>
       * @param value The comment to set.
       * @return This builder for chaining.
       */
      public Builder setComment(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        comment_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string comment = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearComment() {
        bitField0_ = (bitField0_ & ~0x00000004);
        comment_ = getDefaultInstance().getComment();
        onChanged();
        return this;
      }
      /**
       * <code>optional string comment = 3;</code>
       * @param value The bytes for comment to set.
       * @return This builder for chaining.
       */
      public Builder setCommentBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000004;
        comment_ = value;
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


      // @@protoc_insertion_point(builder_scope:doris.PField)
    }

    // @@protoc_insertion_point(class_scope:doris.PField)
    private static final org.apache.doris.proto.Types.PField DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PField();
    }

    public static org.apache.doris.proto.Types.PField getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PField>
        PARSER = new com.google.protobuf.AbstractParser<PField>() {
      @java.lang.Override
      public PField parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PField(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PField> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PField> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PField getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PStructOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PStruct)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PField> 
        getFieldsList();
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    org.apache.doris.proto.Types.PField getFields(int index);
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    int getFieldsCount();
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PFieldOrBuilder> 
        getFieldsOrBuilderList();
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    org.apache.doris.proto.Types.PFieldOrBuilder getFieldsOrBuilder(
        int index);

    /**
     * <code>required string name = 2;</code>
     * @return Whether the name field is set.
     */
    boolean hasName();
    /**
     * <code>required string name = 2;</code>
     * @return The name.
     */
    java.lang.String getName();
    /**
     * <code>required string name = 2;</code>
     * @return The bytes for name.
     */
    com.google.protobuf.ByteString
        getNameBytes();
  }
  /**
   * Protobuf type {@code doris.PStruct}
   */
  public  static final class PStruct extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PStruct)
      PStructOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PStruct.newBuilder() to construct.
    private PStruct(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PStruct() {
      fields_ = java.util.Collections.emptyList();
      name_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PStruct();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PStruct(
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
              if (!((mutable_bitField0_ & 0x00000001) != 0)) {
                fields_ = new java.util.ArrayList<org.apache.doris.proto.Types.PField>();
                mutable_bitField0_ |= 0x00000001;
              }
              fields_.add(
                  input.readMessage(org.apache.doris.proto.Types.PField.PARSER, extensionRegistry));
              break;
            }
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000001;
              name_ = bs;
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
        if (((mutable_bitField0_ & 0x00000001) != 0)) {
          fields_ = java.util.Collections.unmodifiableList(fields_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PStruct_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PStruct_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PStruct.class, org.apache.doris.proto.Types.PStruct.Builder.class);
    }

    private int bitField0_;
    public static final int FIELDS_FIELD_NUMBER = 1;
    private java.util.List<org.apache.doris.proto.Types.PField> fields_;
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PField> getFieldsList() {
      return fields_;
    }
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PFieldOrBuilder> 
        getFieldsOrBuilderList() {
      return fields_;
    }
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    public int getFieldsCount() {
      return fields_.size();
    }
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    public org.apache.doris.proto.Types.PField getFields(int index) {
      return fields_.get(index);
    }
    /**
     * <code>repeated .doris.PField fields = 1;</code>
     */
    public org.apache.doris.proto.Types.PFieldOrBuilder getFieldsOrBuilder(
        int index) {
      return fields_.get(index);
    }

    public static final int NAME_FIELD_NUMBER = 2;
    private volatile java.lang.Object name_;
    /**
     * <code>required string name = 2;</code>
     * @return Whether the name field is set.
     */
    public boolean hasName() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required string name = 2;</code>
     * @return The name.
     */
    public java.lang.String getName() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          name_ = s;
        }
        return s;
      }
    }
    /**
     * <code>required string name = 2;</code>
     * @return The bytes for name.
     */
    public com.google.protobuf.ByteString
        getNameBytes() {
      java.lang.Object ref = name_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        name_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasName()) {
        memoizedIsInitialized = 0;
        return false;
      }
      for (int i = 0; i < getFieldsCount(); i++) {
        if (!getFields(i).isInitialized()) {
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
      for (int i = 0; i < fields_.size(); i++) {
        output.writeMessage(1, fields_.get(i));
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, name_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < fields_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, fields_.get(i));
      }
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, name_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PStruct)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PStruct other = (org.apache.doris.proto.Types.PStruct) obj;

      if (!getFieldsList()
          .equals(other.getFieldsList())) return false;
      if (hasName() != other.hasName()) return false;
      if (hasName()) {
        if (!getName()
            .equals(other.getName())) return false;
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
      if (getFieldsCount() > 0) {
        hash = (37 * hash) + FIELDS_FIELD_NUMBER;
        hash = (53 * hash) + getFieldsList().hashCode();
      }
      if (hasName()) {
        hash = (37 * hash) + NAME_FIELD_NUMBER;
        hash = (53 * hash) + getName().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PStruct parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStruct parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStruct parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PStruct parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PStruct prototype) {
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
     * Protobuf type {@code doris.PStruct}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PStruct)
        org.apache.doris.proto.Types.PStructOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PStruct_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PStruct_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PStruct.class, org.apache.doris.proto.Types.PStruct.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PStruct.newBuilder()
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
          getFieldsFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (fieldsBuilder_ == null) {
          fields_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
        } else {
          fieldsBuilder_.clear();
        }
        name_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PStruct_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStruct getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PStruct.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStruct build() {
        org.apache.doris.proto.Types.PStruct result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PStruct buildPartial() {
        org.apache.doris.proto.Types.PStruct result = new org.apache.doris.proto.Types.PStruct(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (fieldsBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)) {
            fields_ = java.util.Collections.unmodifiableList(fields_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.fields_ = fields_;
        } else {
          result.fields_ = fieldsBuilder_.build();
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.name_ = name_;
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
        if (other instanceof org.apache.doris.proto.Types.PStruct) {
          return mergeFrom((org.apache.doris.proto.Types.PStruct)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PStruct other) {
        if (other == org.apache.doris.proto.Types.PStruct.getDefaultInstance()) return this;
        if (fieldsBuilder_ == null) {
          if (!other.fields_.isEmpty()) {
            if (fields_.isEmpty()) {
              fields_ = other.fields_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureFieldsIsMutable();
              fields_.addAll(other.fields_);
            }
            onChanged();
          }
        } else {
          if (!other.fields_.isEmpty()) {
            if (fieldsBuilder_.isEmpty()) {
              fieldsBuilder_.dispose();
              fieldsBuilder_ = null;
              fields_ = other.fields_;
              bitField0_ = (bitField0_ & ~0x00000001);
              fieldsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getFieldsFieldBuilder() : null;
            } else {
              fieldsBuilder_.addAllMessages(other.fields_);
            }
          }
        }
        if (other.hasName()) {
          bitField0_ |= 0x00000002;
          name_ = other.name_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasName()) {
          return false;
        }
        for (int i = 0; i < getFieldsCount(); i++) {
          if (!getFields(i).isInitialized()) {
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
        org.apache.doris.proto.Types.PStruct parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PStruct) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.util.List<org.apache.doris.proto.Types.PField> fields_ =
        java.util.Collections.emptyList();
      private void ensureFieldsIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          fields_ = new java.util.ArrayList<org.apache.doris.proto.Types.PField>(fields_);
          bitField0_ |= 0x00000001;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PField, org.apache.doris.proto.Types.PField.Builder, org.apache.doris.proto.Types.PFieldOrBuilder> fieldsBuilder_;

      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PField> getFieldsList() {
        if (fieldsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(fields_);
        } else {
          return fieldsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public int getFieldsCount() {
        if (fieldsBuilder_ == null) {
          return fields_.size();
        } else {
          return fieldsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public org.apache.doris.proto.Types.PField getFields(int index) {
        if (fieldsBuilder_ == null) {
          return fields_.get(index);
        } else {
          return fieldsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder setFields(
          int index, org.apache.doris.proto.Types.PField value) {
        if (fieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureFieldsIsMutable();
          fields_.set(index, value);
          onChanged();
        } else {
          fieldsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder setFields(
          int index, org.apache.doris.proto.Types.PField.Builder builderForValue) {
        if (fieldsBuilder_ == null) {
          ensureFieldsIsMutable();
          fields_.set(index, builderForValue.build());
          onChanged();
        } else {
          fieldsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder addFields(org.apache.doris.proto.Types.PField value) {
        if (fieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureFieldsIsMutable();
          fields_.add(value);
          onChanged();
        } else {
          fieldsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder addFields(
          int index, org.apache.doris.proto.Types.PField value) {
        if (fieldsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureFieldsIsMutable();
          fields_.add(index, value);
          onChanged();
        } else {
          fieldsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder addFields(
          org.apache.doris.proto.Types.PField.Builder builderForValue) {
        if (fieldsBuilder_ == null) {
          ensureFieldsIsMutable();
          fields_.add(builderForValue.build());
          onChanged();
        } else {
          fieldsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder addFields(
          int index, org.apache.doris.proto.Types.PField.Builder builderForValue) {
        if (fieldsBuilder_ == null) {
          ensureFieldsIsMutable();
          fields_.add(index, builderForValue.build());
          onChanged();
        } else {
          fieldsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder addAllFields(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PField> values) {
        if (fieldsBuilder_ == null) {
          ensureFieldsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, fields_);
          onChanged();
        } else {
          fieldsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder clearFields() {
        if (fieldsBuilder_ == null) {
          fields_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          fieldsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public Builder removeFields(int index) {
        if (fieldsBuilder_ == null) {
          ensureFieldsIsMutable();
          fields_.remove(index);
          onChanged();
        } else {
          fieldsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public org.apache.doris.proto.Types.PField.Builder getFieldsBuilder(
          int index) {
        return getFieldsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public org.apache.doris.proto.Types.PFieldOrBuilder getFieldsOrBuilder(
          int index) {
        if (fieldsBuilder_ == null) {
          return fields_.get(index);  } else {
          return fieldsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PFieldOrBuilder> 
           getFieldsOrBuilderList() {
        if (fieldsBuilder_ != null) {
          return fieldsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(fields_);
        }
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public org.apache.doris.proto.Types.PField.Builder addFieldsBuilder() {
        return getFieldsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PField.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public org.apache.doris.proto.Types.PField.Builder addFieldsBuilder(
          int index) {
        return getFieldsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PField.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PField fields = 1;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PField.Builder> 
           getFieldsBuilderList() {
        return getFieldsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PField, org.apache.doris.proto.Types.PField.Builder, org.apache.doris.proto.Types.PFieldOrBuilder> 
          getFieldsFieldBuilder() {
        if (fieldsBuilder_ == null) {
          fieldsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PField, org.apache.doris.proto.Types.PField.Builder, org.apache.doris.proto.Types.PFieldOrBuilder>(
                  fields_,
                  ((bitField0_ & 0x00000001) != 0),
                  getParentForChildren(),
                  isClean());
          fields_ = null;
        }
        return fieldsBuilder_;
      }

      private java.lang.Object name_ = "";
      /**
       * <code>required string name = 2;</code>
       * @return Whether the name field is set.
       */
      public boolean hasName() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>required string name = 2;</code>
       * @return The name.
       */
      public java.lang.String getName() {
        java.lang.Object ref = name_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            name_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>required string name = 2;</code>
       * @return The bytes for name.
       */
      public com.google.protobuf.ByteString
          getNameBytes() {
        java.lang.Object ref = name_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          name_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>required string name = 2;</code>
       * @param value The name to set.
       * @return This builder for chaining.
       */
      public Builder setName(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        name_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearName() {
        bitField0_ = (bitField0_ & ~0x00000002);
        name_ = getDefaultInstance().getName();
        onChanged();
        return this;
      }
      /**
       * <code>required string name = 2;</code>
       * @param value The bytes for name to set.
       * @return This builder for chaining.
       */
      public Builder setNameBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        name_ = value;
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


      // @@protoc_insertion_point(builder_scope:doris.PStruct)
    }

    // @@protoc_insertion_point(class_scope:doris.PStruct)
    private static final org.apache.doris.proto.Types.PStruct DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PStruct();
    }

    public static org.apache.doris.proto.Types.PStruct getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PStruct>
        PARSER = new com.google.protobuf.AbstractParser<PStruct>() {
      @java.lang.Override
      public PStruct parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PStruct(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PStruct> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PStruct> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PStruct getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PDecimalOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PDecimal)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required uint32 precision = 1;</code>
     * @return Whether the precision field is set.
     */
    boolean hasPrecision();
    /**
     * <code>required uint32 precision = 1;</code>
     * @return The precision.
     */
    int getPrecision();

    /**
     * <code>required uint32 scale = 2;</code>
     * @return Whether the scale field is set.
     */
    boolean hasScale();
    /**
     * <code>required uint32 scale = 2;</code>
     * @return The scale.
     */
    int getScale();
  }
  /**
   * Protobuf type {@code doris.PDecimal}
   */
  public  static final class PDecimal extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PDecimal)
      PDecimalOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PDecimal.newBuilder() to construct.
    private PDecimal(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PDecimal() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PDecimal();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PDecimal(
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
            case 8: {
              bitField0_ |= 0x00000001;
              precision_ = input.readUInt32();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              scale_ = input.readUInt32();
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
      return org.apache.doris.proto.Types.internal_static_doris_PDecimal_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PDecimal_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PDecimal.class, org.apache.doris.proto.Types.PDecimal.Builder.class);
    }

    private int bitField0_;
    public static final int PRECISION_FIELD_NUMBER = 1;
    private int precision_;
    /**
     * <code>required uint32 precision = 1;</code>
     * @return Whether the precision field is set.
     */
    public boolean hasPrecision() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required uint32 precision = 1;</code>
     * @return The precision.
     */
    public int getPrecision() {
      return precision_;
    }

    public static final int SCALE_FIELD_NUMBER = 2;
    private int scale_;
    /**
     * <code>required uint32 scale = 2;</code>
     * @return Whether the scale field is set.
     */
    public boolean hasScale() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>required uint32 scale = 2;</code>
     * @return The scale.
     */
    public int getScale() {
      return scale_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasPrecision()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!hasScale()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeUInt32(1, precision_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeUInt32(2, scale_);
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
          .computeUInt32Size(1, precision_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt32Size(2, scale_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PDecimal)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PDecimal other = (org.apache.doris.proto.Types.PDecimal) obj;

      if (hasPrecision() != other.hasPrecision()) return false;
      if (hasPrecision()) {
        if (getPrecision()
            != other.getPrecision()) return false;
      }
      if (hasScale() != other.hasScale()) return false;
      if (hasScale()) {
        if (getScale()
            != other.getScale()) return false;
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
      if (hasPrecision()) {
        hash = (37 * hash) + PRECISION_FIELD_NUMBER;
        hash = (53 * hash) + getPrecision();
      }
      if (hasScale()) {
        hash = (37 * hash) + SCALE_FIELD_NUMBER;
        hash = (53 * hash) + getScale();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDecimal parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDecimal parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDecimal parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PDecimal prototype) {
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
     * Protobuf type {@code doris.PDecimal}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PDecimal)
        org.apache.doris.proto.Types.PDecimalOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PDecimal_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PDecimal_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PDecimal.class, org.apache.doris.proto.Types.PDecimal.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PDecimal.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        precision_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        scale_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PDecimal_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDecimal getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PDecimal.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDecimal build() {
        org.apache.doris.proto.Types.PDecimal result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDecimal buildPartial() {
        org.apache.doris.proto.Types.PDecimal result = new org.apache.doris.proto.Types.PDecimal(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.precision_ = precision_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.scale_ = scale_;
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
        if (other instanceof org.apache.doris.proto.Types.PDecimal) {
          return mergeFrom((org.apache.doris.proto.Types.PDecimal)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PDecimal other) {
        if (other == org.apache.doris.proto.Types.PDecimal.getDefaultInstance()) return this;
        if (other.hasPrecision()) {
          setPrecision(other.getPrecision());
        }
        if (other.hasScale()) {
          setScale(other.getScale());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasPrecision()) {
          return false;
        }
        if (!hasScale()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PDecimal parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PDecimal) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int precision_ ;
      /**
       * <code>required uint32 precision = 1;</code>
       * @return Whether the precision field is set.
       */
      public boolean hasPrecision() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required uint32 precision = 1;</code>
       * @return The precision.
       */
      public int getPrecision() {
        return precision_;
      }
      /**
       * <code>required uint32 precision = 1;</code>
       * @param value The precision to set.
       * @return This builder for chaining.
       */
      public Builder setPrecision(int value) {
        bitField0_ |= 0x00000001;
        precision_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required uint32 precision = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearPrecision() {
        bitField0_ = (bitField0_ & ~0x00000001);
        precision_ = 0;
        onChanged();
        return this;
      }

      private int scale_ ;
      /**
       * <code>required uint32 scale = 2;</code>
       * @return Whether the scale field is set.
       */
      public boolean hasScale() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>required uint32 scale = 2;</code>
       * @return The scale.
       */
      public int getScale() {
        return scale_;
      }
      /**
       * <code>required uint32 scale = 2;</code>
       * @param value The scale to set.
       * @return This builder for chaining.
       */
      public Builder setScale(int value) {
        bitField0_ |= 0x00000002;
        scale_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>required uint32 scale = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearScale() {
        bitField0_ = (bitField0_ & ~0x00000002);
        scale_ = 0;
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


      // @@protoc_insertion_point(builder_scope:doris.PDecimal)
    }

    // @@protoc_insertion_point(class_scope:doris.PDecimal)
    private static final org.apache.doris.proto.Types.PDecimal DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PDecimal();
    }

    public static org.apache.doris.proto.Types.PDecimal getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PDecimal>
        PARSER = new com.google.protobuf.AbstractParser<PDecimal>() {
      @java.lang.Override
      public PDecimal parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PDecimal(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PDecimal> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PDecimal> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PDecimal getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PDateTimeOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PDateTime)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional int32 year = 1;</code>
     * @return Whether the year field is set.
     */
    boolean hasYear();
    /**
     * <code>optional int32 year = 1;</code>
     * @return The year.
     */
    int getYear();

    /**
     * <code>optional int32 month = 2;</code>
     * @return Whether the month field is set.
     */
    boolean hasMonth();
    /**
     * <code>optional int32 month = 2;</code>
     * @return The month.
     */
    int getMonth();

    /**
     * <code>optional int32 day = 3;</code>
     * @return Whether the day field is set.
     */
    boolean hasDay();
    /**
     * <code>optional int32 day = 3;</code>
     * @return The day.
     */
    int getDay();

    /**
     * <code>optional int32 hour = 4;</code>
     * @return Whether the hour field is set.
     */
    boolean hasHour();
    /**
     * <code>optional int32 hour = 4;</code>
     * @return The hour.
     */
    int getHour();

    /**
     * <code>optional int32 minute = 5;</code>
     * @return Whether the minute field is set.
     */
    boolean hasMinute();
    /**
     * <code>optional int32 minute = 5;</code>
     * @return The minute.
     */
    int getMinute();

    /**
     * <code>optional int32 second = 6;</code>
     * @return Whether the second field is set.
     */
    boolean hasSecond();
    /**
     * <code>optional int32 second = 6;</code>
     * @return The second.
     */
    int getSecond();

    /**
     * <code>optional int32 microsecond = 7;</code>
     * @return Whether the microsecond field is set.
     */
    boolean hasMicrosecond();
    /**
     * <code>optional int32 microsecond = 7;</code>
     * @return The microsecond.
     */
    int getMicrosecond();
  }
  /**
   * Protobuf type {@code doris.PDateTime}
   */
  public  static final class PDateTime extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PDateTime)
      PDateTimeOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PDateTime.newBuilder() to construct.
    private PDateTime(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PDateTime() {
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PDateTime();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PDateTime(
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
            case 8: {
              bitField0_ |= 0x00000001;
              year_ = input.readInt32();
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              month_ = input.readInt32();
              break;
            }
            case 24: {
              bitField0_ |= 0x00000004;
              day_ = input.readInt32();
              break;
            }
            case 32: {
              bitField0_ |= 0x00000008;
              hour_ = input.readInt32();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              minute_ = input.readInt32();
              break;
            }
            case 48: {
              bitField0_ |= 0x00000020;
              second_ = input.readInt32();
              break;
            }
            case 56: {
              bitField0_ |= 0x00000040;
              microsecond_ = input.readInt32();
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
      return org.apache.doris.proto.Types.internal_static_doris_PDateTime_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PDateTime_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PDateTime.class, org.apache.doris.proto.Types.PDateTime.Builder.class);
    }

    private int bitField0_;
    public static final int YEAR_FIELD_NUMBER = 1;
    private int year_;
    /**
     * <code>optional int32 year = 1;</code>
     * @return Whether the year field is set.
     */
    public boolean hasYear() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional int32 year = 1;</code>
     * @return The year.
     */
    public int getYear() {
      return year_;
    }

    public static final int MONTH_FIELD_NUMBER = 2;
    private int month_;
    /**
     * <code>optional int32 month = 2;</code>
     * @return Whether the month field is set.
     */
    public boolean hasMonth() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional int32 month = 2;</code>
     * @return The month.
     */
    public int getMonth() {
      return month_;
    }

    public static final int DAY_FIELD_NUMBER = 3;
    private int day_;
    /**
     * <code>optional int32 day = 3;</code>
     * @return Whether the day field is set.
     */
    public boolean hasDay() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional int32 day = 3;</code>
     * @return The day.
     */
    public int getDay() {
      return day_;
    }

    public static final int HOUR_FIELD_NUMBER = 4;
    private int hour_;
    /**
     * <code>optional int32 hour = 4;</code>
     * @return Whether the hour field is set.
     */
    public boolean hasHour() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional int32 hour = 4;</code>
     * @return The hour.
     */
    public int getHour() {
      return hour_;
    }

    public static final int MINUTE_FIELD_NUMBER = 5;
    private int minute_;
    /**
     * <code>optional int32 minute = 5;</code>
     * @return Whether the minute field is set.
     */
    public boolean hasMinute() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     * <code>optional int32 minute = 5;</code>
     * @return The minute.
     */
    public int getMinute() {
      return minute_;
    }

    public static final int SECOND_FIELD_NUMBER = 6;
    private int second_;
    /**
     * <code>optional int32 second = 6;</code>
     * @return Whether the second field is set.
     */
    public boolean hasSecond() {
      return ((bitField0_ & 0x00000020) != 0);
    }
    /**
     * <code>optional int32 second = 6;</code>
     * @return The second.
     */
    public int getSecond() {
      return second_;
    }

    public static final int MICROSECOND_FIELD_NUMBER = 7;
    private int microsecond_;
    /**
     * <code>optional int32 microsecond = 7;</code>
     * @return Whether the microsecond field is set.
     */
    public boolean hasMicrosecond() {
      return ((bitField0_ & 0x00000040) != 0);
    }
    /**
     * <code>optional int32 microsecond = 7;</code>
     * @return The microsecond.
     */
    public int getMicrosecond() {
      return microsecond_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt32(1, year_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeInt32(2, month_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeInt32(3, day_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeInt32(4, hour_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeInt32(5, minute_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        output.writeInt32(6, second_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        output.writeInt32(7, microsecond_);
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
          .computeInt32Size(1, year_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(2, month_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(3, day_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(4, hour_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(5, minute_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(6, second_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(7, microsecond_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PDateTime)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PDateTime other = (org.apache.doris.proto.Types.PDateTime) obj;

      if (hasYear() != other.hasYear()) return false;
      if (hasYear()) {
        if (getYear()
            != other.getYear()) return false;
      }
      if (hasMonth() != other.hasMonth()) return false;
      if (hasMonth()) {
        if (getMonth()
            != other.getMonth()) return false;
      }
      if (hasDay() != other.hasDay()) return false;
      if (hasDay()) {
        if (getDay()
            != other.getDay()) return false;
      }
      if (hasHour() != other.hasHour()) return false;
      if (hasHour()) {
        if (getHour()
            != other.getHour()) return false;
      }
      if (hasMinute() != other.hasMinute()) return false;
      if (hasMinute()) {
        if (getMinute()
            != other.getMinute()) return false;
      }
      if (hasSecond() != other.hasSecond()) return false;
      if (hasSecond()) {
        if (getSecond()
            != other.getSecond()) return false;
      }
      if (hasMicrosecond() != other.hasMicrosecond()) return false;
      if (hasMicrosecond()) {
        if (getMicrosecond()
            != other.getMicrosecond()) return false;
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
      if (hasYear()) {
        hash = (37 * hash) + YEAR_FIELD_NUMBER;
        hash = (53 * hash) + getYear();
      }
      if (hasMonth()) {
        hash = (37 * hash) + MONTH_FIELD_NUMBER;
        hash = (53 * hash) + getMonth();
      }
      if (hasDay()) {
        hash = (37 * hash) + DAY_FIELD_NUMBER;
        hash = (53 * hash) + getDay();
      }
      if (hasHour()) {
        hash = (37 * hash) + HOUR_FIELD_NUMBER;
        hash = (53 * hash) + getHour();
      }
      if (hasMinute()) {
        hash = (37 * hash) + MINUTE_FIELD_NUMBER;
        hash = (53 * hash) + getMinute();
      }
      if (hasSecond()) {
        hash = (37 * hash) + SECOND_FIELD_NUMBER;
        hash = (53 * hash) + getSecond();
      }
      if (hasMicrosecond()) {
        hash = (37 * hash) + MICROSECOND_FIELD_NUMBER;
        hash = (53 * hash) + getMicrosecond();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDateTime parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDateTime parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PDateTime parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PDateTime prototype) {
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
     * Protobuf type {@code doris.PDateTime}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PDateTime)
        org.apache.doris.proto.Types.PDateTimeOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PDateTime_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PDateTime_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PDateTime.class, org.apache.doris.proto.Types.PDateTime.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PDateTime.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        year_ = 0;
        bitField0_ = (bitField0_ & ~0x00000001);
        month_ = 0;
        bitField0_ = (bitField0_ & ~0x00000002);
        day_ = 0;
        bitField0_ = (bitField0_ & ~0x00000004);
        hour_ = 0;
        bitField0_ = (bitField0_ & ~0x00000008);
        minute_ = 0;
        bitField0_ = (bitField0_ & ~0x00000010);
        second_ = 0;
        bitField0_ = (bitField0_ & ~0x00000020);
        microsecond_ = 0;
        bitField0_ = (bitField0_ & ~0x00000040);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PDateTime_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDateTime getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PDateTime.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDateTime build() {
        org.apache.doris.proto.Types.PDateTime result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PDateTime buildPartial() {
        org.apache.doris.proto.Types.PDateTime result = new org.apache.doris.proto.Types.PDateTime(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.year_ = year_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.month_ = month_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.day_ = day_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.hour_ = hour_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.minute_ = minute_;
          to_bitField0_ |= 0x00000010;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.second_ = second_;
          to_bitField0_ |= 0x00000020;
        }
        if (((from_bitField0_ & 0x00000040) != 0)) {
          result.microsecond_ = microsecond_;
          to_bitField0_ |= 0x00000040;
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
        if (other instanceof org.apache.doris.proto.Types.PDateTime) {
          return mergeFrom((org.apache.doris.proto.Types.PDateTime)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PDateTime other) {
        if (other == org.apache.doris.proto.Types.PDateTime.getDefaultInstance()) return this;
        if (other.hasYear()) {
          setYear(other.getYear());
        }
        if (other.hasMonth()) {
          setMonth(other.getMonth());
        }
        if (other.hasDay()) {
          setDay(other.getDay());
        }
        if (other.hasHour()) {
          setHour(other.getHour());
        }
        if (other.hasMinute()) {
          setMinute(other.getMinute());
        }
        if (other.hasSecond()) {
          setSecond(other.getSecond());
        }
        if (other.hasMicrosecond()) {
          setMicrosecond(other.getMicrosecond());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PDateTime parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PDateTime) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private int year_ ;
      /**
       * <code>optional int32 year = 1;</code>
       * @return Whether the year field is set.
       */
      public boolean hasYear() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional int32 year = 1;</code>
       * @return The year.
       */
      public int getYear() {
        return year_;
      }
      /**
       * <code>optional int32 year = 1;</code>
       * @param value The year to set.
       * @return This builder for chaining.
       */
      public Builder setYear(int value) {
        bitField0_ |= 0x00000001;
        year_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 year = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearYear() {
        bitField0_ = (bitField0_ & ~0x00000001);
        year_ = 0;
        onChanged();
        return this;
      }

      private int month_ ;
      /**
       * <code>optional int32 month = 2;</code>
       * @return Whether the month field is set.
       */
      public boolean hasMonth() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional int32 month = 2;</code>
       * @return The month.
       */
      public int getMonth() {
        return month_;
      }
      /**
       * <code>optional int32 month = 2;</code>
       * @param value The month to set.
       * @return This builder for chaining.
       */
      public Builder setMonth(int value) {
        bitField0_ |= 0x00000002;
        month_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 month = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearMonth() {
        bitField0_ = (bitField0_ & ~0x00000002);
        month_ = 0;
        onChanged();
        return this;
      }

      private int day_ ;
      /**
       * <code>optional int32 day = 3;</code>
       * @return Whether the day field is set.
       */
      public boolean hasDay() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional int32 day = 3;</code>
       * @return The day.
       */
      public int getDay() {
        return day_;
      }
      /**
       * <code>optional int32 day = 3;</code>
       * @param value The day to set.
       * @return This builder for chaining.
       */
      public Builder setDay(int value) {
        bitField0_ |= 0x00000004;
        day_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 day = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearDay() {
        bitField0_ = (bitField0_ & ~0x00000004);
        day_ = 0;
        onChanged();
        return this;
      }

      private int hour_ ;
      /**
       * <code>optional int32 hour = 4;</code>
       * @return Whether the hour field is set.
       */
      public boolean hasHour() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional int32 hour = 4;</code>
       * @return The hour.
       */
      public int getHour() {
        return hour_;
      }
      /**
       * <code>optional int32 hour = 4;</code>
       * @param value The hour to set.
       * @return This builder for chaining.
       */
      public Builder setHour(int value) {
        bitField0_ |= 0x00000008;
        hour_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 hour = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearHour() {
        bitField0_ = (bitField0_ & ~0x00000008);
        hour_ = 0;
        onChanged();
        return this;
      }

      private int minute_ ;
      /**
       * <code>optional int32 minute = 5;</code>
       * @return Whether the minute field is set.
       */
      public boolean hasMinute() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <code>optional int32 minute = 5;</code>
       * @return The minute.
       */
      public int getMinute() {
        return minute_;
      }
      /**
       * <code>optional int32 minute = 5;</code>
       * @param value The minute to set.
       * @return This builder for chaining.
       */
      public Builder setMinute(int value) {
        bitField0_ |= 0x00000010;
        minute_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 minute = 5;</code>
       * @return This builder for chaining.
       */
      public Builder clearMinute() {
        bitField0_ = (bitField0_ & ~0x00000010);
        minute_ = 0;
        onChanged();
        return this;
      }

      private int second_ ;
      /**
       * <code>optional int32 second = 6;</code>
       * @return Whether the second field is set.
       */
      public boolean hasSecond() {
        return ((bitField0_ & 0x00000020) != 0);
      }
      /**
       * <code>optional int32 second = 6;</code>
       * @return The second.
       */
      public int getSecond() {
        return second_;
      }
      /**
       * <code>optional int32 second = 6;</code>
       * @param value The second to set.
       * @return This builder for chaining.
       */
      public Builder setSecond(int value) {
        bitField0_ |= 0x00000020;
        second_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 second = 6;</code>
       * @return This builder for chaining.
       */
      public Builder clearSecond() {
        bitField0_ = (bitField0_ & ~0x00000020);
        second_ = 0;
        onChanged();
        return this;
      }

      private int microsecond_ ;
      /**
       * <code>optional int32 microsecond = 7;</code>
       * @return Whether the microsecond field is set.
       */
      public boolean hasMicrosecond() {
        return ((bitField0_ & 0x00000040) != 0);
      }
      /**
       * <code>optional int32 microsecond = 7;</code>
       * @return The microsecond.
       */
      public int getMicrosecond() {
        return microsecond_;
      }
      /**
       * <code>optional int32 microsecond = 7;</code>
       * @param value The microsecond to set.
       * @return This builder for chaining.
       */
      public Builder setMicrosecond(int value) {
        bitField0_ |= 0x00000040;
        microsecond_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 microsecond = 7;</code>
       * @return This builder for chaining.
       */
      public Builder clearMicrosecond() {
        bitField0_ = (bitField0_ & ~0x00000040);
        microsecond_ = 0;
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


      // @@protoc_insertion_point(builder_scope:doris.PDateTime)
    }

    // @@protoc_insertion_point(class_scope:doris.PDateTime)
    private static final org.apache.doris.proto.Types.PDateTime DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PDateTime();
    }

    public static org.apache.doris.proto.Types.PDateTime getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PDateTime>
        PARSER = new com.google.protobuf.AbstractParser<PDateTime>() {
      @java.lang.Override
      public PDateTime parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PDateTime(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PDateTime> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PDateTime> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PDateTime getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PValueOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PValue)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    org.apache.doris.proto.Types.PGenericType getType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder();

    /**
     * <code>optional bool is_null = 2 [default = false];</code>
     * @return Whether the isNull field is set.
     */
    boolean hasIsNull();
    /**
     * <code>optional bool is_null = 2 [default = false];</code>
     * @return The isNull.
     */
    boolean getIsNull();

    /**
     * <code>optional double double_value = 3;</code>
     * @return Whether the doubleValue field is set.
     */
    boolean hasDoubleValue();
    /**
     * <code>optional double double_value = 3;</code>
     * @return The doubleValue.
     */
    double getDoubleValue();

    /**
     * <code>optional float float_value = 4;</code>
     * @return Whether the floatValue field is set.
     */
    boolean hasFloatValue();
    /**
     * <code>optional float float_value = 4;</code>
     * @return The floatValue.
     */
    float getFloatValue();

    /**
     * <code>optional int32 int32_value = 5;</code>
     * @return Whether the int32Value field is set.
     */
    boolean hasInt32Value();
    /**
     * <code>optional int32 int32_value = 5;</code>
     * @return The int32Value.
     */
    int getInt32Value();

    /**
     * <code>optional int64 int64_value = 6;</code>
     * @return Whether the int64Value field is set.
     */
    boolean hasInt64Value();
    /**
     * <code>optional int64 int64_value = 6;</code>
     * @return The int64Value.
     */
    long getInt64Value();

    /**
     * <code>optional uint32 uint32_value = 7;</code>
     * @return Whether the uint32Value field is set.
     */
    boolean hasUint32Value();
    /**
     * <code>optional uint32 uint32_value = 7;</code>
     * @return The uint32Value.
     */
    int getUint32Value();

    /**
     * <code>optional uint64 uint64_value = 8;</code>
     * @return Whether the uint64Value field is set.
     */
    boolean hasUint64Value();
    /**
     * <code>optional uint64 uint64_value = 8;</code>
     * @return The uint64Value.
     */
    long getUint64Value();

    /**
     * <code>optional bool bool_value = 9;</code>
     * @return Whether the boolValue field is set.
     */
    boolean hasBoolValue();
    /**
     * <code>optional bool bool_value = 9;</code>
     * @return The boolValue.
     */
    boolean getBoolValue();

    /**
     * <code>optional string string_value = 10;</code>
     * @return Whether the stringValue field is set.
     */
    boolean hasStringValue();
    /**
     * <code>optional string string_value = 10;</code>
     * @return The stringValue.
     */
    java.lang.String getStringValue();
    /**
     * <code>optional string string_value = 10;</code>
     * @return The bytes for stringValue.
     */
    com.google.protobuf.ByteString
        getStringValueBytes();

    /**
     * <code>optional bytes bytes_value = 11;</code>
     * @return Whether the bytesValue field is set.
     */
    boolean hasBytesValue();
    /**
     * <code>optional bytes bytes_value = 11;</code>
     * @return The bytesValue.
     */
    com.google.protobuf.ByteString getBytesValue();

    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     * @return Whether the datetimeValue field is set.
     */
    boolean hasDatetimeValue();
    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     * @return The datetimeValue.
     */
    org.apache.doris.proto.Types.PDateTime getDatetimeValue();
    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     */
    org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder();
  }
  /**
   * Protobuf type {@code doris.PValue}
   */
  public  static final class PValue extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PValue)
      PValueOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PValue.newBuilder() to construct.
    private PValue(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PValue() {
      stringValue_ = "";
      bytesValue_ = com.google.protobuf.ByteString.EMPTY;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PValue();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PValue(
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
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = type_.toBuilder();
              }
              type_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(type_);
                type_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              isNull_ = input.readBool();
              break;
            }
            case 25: {
              bitField0_ |= 0x00000004;
              doubleValue_ = input.readDouble();
              break;
            }
            case 37: {
              bitField0_ |= 0x00000008;
              floatValue_ = input.readFloat();
              break;
            }
            case 40: {
              bitField0_ |= 0x00000010;
              int32Value_ = input.readInt32();
              break;
            }
            case 48: {
              bitField0_ |= 0x00000020;
              int64Value_ = input.readInt64();
              break;
            }
            case 56: {
              bitField0_ |= 0x00000040;
              uint32Value_ = input.readUInt32();
              break;
            }
            case 64: {
              bitField0_ |= 0x00000080;
              uint64Value_ = input.readUInt64();
              break;
            }
            case 72: {
              bitField0_ |= 0x00000100;
              boolValue_ = input.readBool();
              break;
            }
            case 82: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000200;
              stringValue_ = bs;
              break;
            }
            case 90: {
              bitField0_ |= 0x00000400;
              bytesValue_ = input.readBytes();
              break;
            }
            case 98: {
              org.apache.doris.proto.Types.PDateTime.Builder subBuilder = null;
              if (((bitField0_ & 0x00000800) != 0)) {
                subBuilder = datetimeValue_.toBuilder();
              }
              datetimeValue_ = input.readMessage(org.apache.doris.proto.Types.PDateTime.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(datetimeValue_);
                datetimeValue_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000800;
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
      return org.apache.doris.proto.Types.internal_static_doris_PValue_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PValue_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PValue.class, org.apache.doris.proto.Types.PValue.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PGenericType type_;
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    public org.apache.doris.proto.Types.PGenericType getType() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }

    public static final int IS_NULL_FIELD_NUMBER = 2;
    private boolean isNull_;
    /**
     * <code>optional bool is_null = 2 [default = false];</code>
     * @return Whether the isNull field is set.
     */
    public boolean hasIsNull() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional bool is_null = 2 [default = false];</code>
     * @return The isNull.
     */
    public boolean getIsNull() {
      return isNull_;
    }

    public static final int DOUBLE_VALUE_FIELD_NUMBER = 3;
    private double doubleValue_;
    /**
     * <code>optional double double_value = 3;</code>
     * @return Whether the doubleValue field is set.
     */
    public boolean hasDoubleValue() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional double double_value = 3;</code>
     * @return The doubleValue.
     */
    public double getDoubleValue() {
      return doubleValue_;
    }

    public static final int FLOAT_VALUE_FIELD_NUMBER = 4;
    private float floatValue_;
    /**
     * <code>optional float float_value = 4;</code>
     * @return Whether the floatValue field is set.
     */
    public boolean hasFloatValue() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional float float_value = 4;</code>
     * @return The floatValue.
     */
    public float getFloatValue() {
      return floatValue_;
    }

    public static final int INT32_VALUE_FIELD_NUMBER = 5;
    private int int32Value_;
    /**
     * <code>optional int32 int32_value = 5;</code>
     * @return Whether the int32Value field is set.
     */
    public boolean hasInt32Value() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     * <code>optional int32 int32_value = 5;</code>
     * @return The int32Value.
     */
    public int getInt32Value() {
      return int32Value_;
    }

    public static final int INT64_VALUE_FIELD_NUMBER = 6;
    private long int64Value_;
    /**
     * <code>optional int64 int64_value = 6;</code>
     * @return Whether the int64Value field is set.
     */
    public boolean hasInt64Value() {
      return ((bitField0_ & 0x00000020) != 0);
    }
    /**
     * <code>optional int64 int64_value = 6;</code>
     * @return The int64Value.
     */
    public long getInt64Value() {
      return int64Value_;
    }

    public static final int UINT32_VALUE_FIELD_NUMBER = 7;
    private int uint32Value_;
    /**
     * <code>optional uint32 uint32_value = 7;</code>
     * @return Whether the uint32Value field is set.
     */
    public boolean hasUint32Value() {
      return ((bitField0_ & 0x00000040) != 0);
    }
    /**
     * <code>optional uint32 uint32_value = 7;</code>
     * @return The uint32Value.
     */
    public int getUint32Value() {
      return uint32Value_;
    }

    public static final int UINT64_VALUE_FIELD_NUMBER = 8;
    private long uint64Value_;
    /**
     * <code>optional uint64 uint64_value = 8;</code>
     * @return Whether the uint64Value field is set.
     */
    public boolean hasUint64Value() {
      return ((bitField0_ & 0x00000080) != 0);
    }
    /**
     * <code>optional uint64 uint64_value = 8;</code>
     * @return The uint64Value.
     */
    public long getUint64Value() {
      return uint64Value_;
    }

    public static final int BOOL_VALUE_FIELD_NUMBER = 9;
    private boolean boolValue_;
    /**
     * <code>optional bool bool_value = 9;</code>
     * @return Whether the boolValue field is set.
     */
    public boolean hasBoolValue() {
      return ((bitField0_ & 0x00000100) != 0);
    }
    /**
     * <code>optional bool bool_value = 9;</code>
     * @return The boolValue.
     */
    public boolean getBoolValue() {
      return boolValue_;
    }

    public static final int STRING_VALUE_FIELD_NUMBER = 10;
    private volatile java.lang.Object stringValue_;
    /**
     * <code>optional string string_value = 10;</code>
     * @return Whether the stringValue field is set.
     */
    public boolean hasStringValue() {
      return ((bitField0_ & 0x00000200) != 0);
    }
    /**
     * <code>optional string string_value = 10;</code>
     * @return The stringValue.
     */
    public java.lang.String getStringValue() {
      java.lang.Object ref = stringValue_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          stringValue_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string string_value = 10;</code>
     * @return The bytes for stringValue.
     */
    public com.google.protobuf.ByteString
        getStringValueBytes() {
      java.lang.Object ref = stringValue_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        stringValue_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int BYTES_VALUE_FIELD_NUMBER = 11;
    private com.google.protobuf.ByteString bytesValue_;
    /**
     * <code>optional bytes bytes_value = 11;</code>
     * @return Whether the bytesValue field is set.
     */
    public boolean hasBytesValue() {
      return ((bitField0_ & 0x00000400) != 0);
    }
    /**
     * <code>optional bytes bytes_value = 11;</code>
     * @return The bytesValue.
     */
    public com.google.protobuf.ByteString getBytesValue() {
      return bytesValue_;
    }

    public static final int DATETIME_VALUE_FIELD_NUMBER = 12;
    private org.apache.doris.proto.Types.PDateTime datetimeValue_;
    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     * @return Whether the datetimeValue field is set.
     */
    public boolean hasDatetimeValue() {
      return ((bitField0_ & 0x00000800) != 0);
    }
    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     * @return The datetimeValue.
     */
    public org.apache.doris.proto.Types.PDateTime getDatetimeValue() {
      return datetimeValue_ == null ? org.apache.doris.proto.Types.PDateTime.getDefaultInstance() : datetimeValue_;
    }
    /**
     * <code>optional .doris.PDateTime datetime_value = 12;</code>
     */
    public org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder() {
      return datetimeValue_ == null ? org.apache.doris.proto.Types.PDateTime.getDefaultInstance() : datetimeValue_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeBool(2, isNull_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeDouble(3, doubleValue_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeFloat(4, floatValue_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeInt32(5, int32Value_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        output.writeInt64(6, int64Value_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        output.writeUInt32(7, uint32Value_);
      }
      if (((bitField0_ & 0x00000080) != 0)) {
        output.writeUInt64(8, uint64Value_);
      }
      if (((bitField0_ & 0x00000100) != 0)) {
        output.writeBool(9, boolValue_);
      }
      if (((bitField0_ & 0x00000200) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 10, stringValue_);
      }
      if (((bitField0_ & 0x00000400) != 0)) {
        output.writeBytes(11, bytesValue_);
      }
      if (((bitField0_ & 0x00000800) != 0)) {
        output.writeMessage(12, getDatetimeValue());
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
          .computeMessageSize(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(2, isNull_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeDoubleSize(3, doubleValue_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeFloatSize(4, floatValue_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(5, int32Value_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(6, int64Value_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt32Size(7, uint32Value_);
      }
      if (((bitField0_ & 0x00000080) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeUInt64Size(8, uint64Value_);
      }
      if (((bitField0_ & 0x00000100) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(9, boolValue_);
      }
      if (((bitField0_ & 0x00000200) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(10, stringValue_);
      }
      if (((bitField0_ & 0x00000400) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(11, bytesValue_);
      }
      if (((bitField0_ & 0x00000800) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(12, getDatetimeValue());
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
      if (!(obj instanceof org.apache.doris.proto.Types.PValue)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PValue other = (org.apache.doris.proto.Types.PValue) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (!getType()
            .equals(other.getType())) return false;
      }
      if (hasIsNull() != other.hasIsNull()) return false;
      if (hasIsNull()) {
        if (getIsNull()
            != other.getIsNull()) return false;
      }
      if (hasDoubleValue() != other.hasDoubleValue()) return false;
      if (hasDoubleValue()) {
        if (java.lang.Double.doubleToLongBits(getDoubleValue())
            != java.lang.Double.doubleToLongBits(
                other.getDoubleValue())) return false;
      }
      if (hasFloatValue() != other.hasFloatValue()) return false;
      if (hasFloatValue()) {
        if (java.lang.Float.floatToIntBits(getFloatValue())
            != java.lang.Float.floatToIntBits(
                other.getFloatValue())) return false;
      }
      if (hasInt32Value() != other.hasInt32Value()) return false;
      if (hasInt32Value()) {
        if (getInt32Value()
            != other.getInt32Value()) return false;
      }
      if (hasInt64Value() != other.hasInt64Value()) return false;
      if (hasInt64Value()) {
        if (getInt64Value()
            != other.getInt64Value()) return false;
      }
      if (hasUint32Value() != other.hasUint32Value()) return false;
      if (hasUint32Value()) {
        if (getUint32Value()
            != other.getUint32Value()) return false;
      }
      if (hasUint64Value() != other.hasUint64Value()) return false;
      if (hasUint64Value()) {
        if (getUint64Value()
            != other.getUint64Value()) return false;
      }
      if (hasBoolValue() != other.hasBoolValue()) return false;
      if (hasBoolValue()) {
        if (getBoolValue()
            != other.getBoolValue()) return false;
      }
      if (hasStringValue() != other.hasStringValue()) return false;
      if (hasStringValue()) {
        if (!getStringValue()
            .equals(other.getStringValue())) return false;
      }
      if (hasBytesValue() != other.hasBytesValue()) return false;
      if (hasBytesValue()) {
        if (!getBytesValue()
            .equals(other.getBytesValue())) return false;
      }
      if (hasDatetimeValue() != other.hasDatetimeValue()) return false;
      if (hasDatetimeValue()) {
        if (!getDatetimeValue()
            .equals(other.getDatetimeValue())) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType().hashCode();
      }
      if (hasIsNull()) {
        hash = (37 * hash) + IS_NULL_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getIsNull());
      }
      if (hasDoubleValue()) {
        hash = (37 * hash) + DOUBLE_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            java.lang.Double.doubleToLongBits(getDoubleValue()));
      }
      if (hasFloatValue()) {
        hash = (37 * hash) + FLOAT_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + java.lang.Float.floatToIntBits(
            getFloatValue());
      }
      if (hasInt32Value()) {
        hash = (37 * hash) + INT32_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getInt32Value();
      }
      if (hasInt64Value()) {
        hash = (37 * hash) + INT64_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getInt64Value());
      }
      if (hasUint32Value()) {
        hash = (37 * hash) + UINT32_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getUint32Value();
      }
      if (hasUint64Value()) {
        hash = (37 * hash) + UINT64_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getUint64Value());
      }
      if (hasBoolValue()) {
        hash = (37 * hash) + BOOL_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getBoolValue());
      }
      if (hasStringValue()) {
        hash = (37 * hash) + STRING_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getStringValue().hashCode();
      }
      if (hasBytesValue()) {
        hash = (37 * hash) + BYTES_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getBytesValue().hashCode();
      }
      if (hasDatetimeValue()) {
        hash = (37 * hash) + DATETIME_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getDatetimeValue().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PValue parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValue parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValue parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValue parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PValue prototype) {
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
     * Protobuf type {@code doris.PValue}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PValue)
        org.apache.doris.proto.Types.PValueOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PValue_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PValue_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PValue.class, org.apache.doris.proto.Types.PValue.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PValue.newBuilder()
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
          getTypeFieldBuilder();
          getDatetimeValueFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (typeBuilder_ == null) {
          type_ = null;
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        isNull_ = false;
        bitField0_ = (bitField0_ & ~0x00000002);
        doubleValue_ = 0D;
        bitField0_ = (bitField0_ & ~0x00000004);
        floatValue_ = 0F;
        bitField0_ = (bitField0_ & ~0x00000008);
        int32Value_ = 0;
        bitField0_ = (bitField0_ & ~0x00000010);
        int64Value_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000020);
        uint32Value_ = 0;
        bitField0_ = (bitField0_ & ~0x00000040);
        uint64Value_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000080);
        boolValue_ = false;
        bitField0_ = (bitField0_ & ~0x00000100);
        stringValue_ = "";
        bitField0_ = (bitField0_ & ~0x00000200);
        bytesValue_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000400);
        if (datetimeValueBuilder_ == null) {
          datetimeValue_ = null;
        } else {
          datetimeValueBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000800);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PValue_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValue getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PValue.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValue build() {
        org.apache.doris.proto.Types.PValue result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValue buildPartial() {
        org.apache.doris.proto.Types.PValue result = new org.apache.doris.proto.Types.PValue(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (typeBuilder_ == null) {
            result.type_ = type_;
          } else {
            result.type_ = typeBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.isNull_ = isNull_;
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.doubleValue_ = doubleValue_;
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          result.floatValue_ = floatValue_;
          to_bitField0_ |= 0x00000008;
        }
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.int32Value_ = int32Value_;
          to_bitField0_ |= 0x00000010;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          result.int64Value_ = int64Value_;
          to_bitField0_ |= 0x00000020;
        }
        if (((from_bitField0_ & 0x00000040) != 0)) {
          result.uint32Value_ = uint32Value_;
          to_bitField0_ |= 0x00000040;
        }
        if (((from_bitField0_ & 0x00000080) != 0)) {
          result.uint64Value_ = uint64Value_;
          to_bitField0_ |= 0x00000080;
        }
        if (((from_bitField0_ & 0x00000100) != 0)) {
          result.boolValue_ = boolValue_;
          to_bitField0_ |= 0x00000100;
        }
        if (((from_bitField0_ & 0x00000200) != 0)) {
          to_bitField0_ |= 0x00000200;
        }
        result.stringValue_ = stringValue_;
        if (((from_bitField0_ & 0x00000400) != 0)) {
          to_bitField0_ |= 0x00000400;
        }
        result.bytesValue_ = bytesValue_;
        if (((from_bitField0_ & 0x00000800) != 0)) {
          if (datetimeValueBuilder_ == null) {
            result.datetimeValue_ = datetimeValue_;
          } else {
            result.datetimeValue_ = datetimeValueBuilder_.build();
          }
          to_bitField0_ |= 0x00000800;
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
        if (other instanceof org.apache.doris.proto.Types.PValue) {
          return mergeFrom((org.apache.doris.proto.Types.PValue)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PValue other) {
        if (other == org.apache.doris.proto.Types.PValue.getDefaultInstance()) return this;
        if (other.hasType()) {
          mergeType(other.getType());
        }
        if (other.hasIsNull()) {
          setIsNull(other.getIsNull());
        }
        if (other.hasDoubleValue()) {
          setDoubleValue(other.getDoubleValue());
        }
        if (other.hasFloatValue()) {
          setFloatValue(other.getFloatValue());
        }
        if (other.hasInt32Value()) {
          setInt32Value(other.getInt32Value());
        }
        if (other.hasInt64Value()) {
          setInt64Value(other.getInt64Value());
        }
        if (other.hasUint32Value()) {
          setUint32Value(other.getUint32Value());
        }
        if (other.hasUint64Value()) {
          setUint64Value(other.getUint64Value());
        }
        if (other.hasBoolValue()) {
          setBoolValue(other.getBoolValue());
        }
        if (other.hasStringValue()) {
          bitField0_ |= 0x00000200;
          stringValue_ = other.stringValue_;
          onChanged();
        }
        if (other.hasBytesValue()) {
          setBytesValue(other.getBytesValue());
        }
        if (other.hasDatetimeValue()) {
          mergeDatetimeValue(other.getDatetimeValue());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        if (!getType().isInitialized()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PValue parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PValue) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PGenericType type_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> typeBuilder_;
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return The type.
       */
      public org.apache.doris.proto.Types.PGenericType getType() {
        if (typeBuilder_ == null) {
          return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        } else {
          return typeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          type_ = value;
          onChanged();
        } else {
          typeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (typeBuilder_ == null) {
          type_ = builderForValue.build();
          onChanged();
        } else {
          typeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder mergeType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              type_ != null &&
              type_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            type_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(type_).mergeFrom(value).buildPartial();
          } else {
            type_ = value;
          }
          onChanged();
        } else {
          typeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder clearType() {
        if (typeBuilder_ == null) {
          type_ = null;
          onChanged();
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getTypeBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
        if (typeBuilder_ != null) {
          return typeBuilder_.getMessageOrBuilder();
        } else {
          return type_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getTypeFieldBuilder() {
        if (typeBuilder_ == null) {
          typeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getType(),
                  getParentForChildren(),
                  isClean());
          type_ = null;
        }
        return typeBuilder_;
      }

      private boolean isNull_ ;
      /**
       * <code>optional bool is_null = 2 [default = false];</code>
       * @return Whether the isNull field is set.
       */
      public boolean hasIsNull() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional bool is_null = 2 [default = false];</code>
       * @return The isNull.
       */
      public boolean getIsNull() {
        return isNull_;
      }
      /**
       * <code>optional bool is_null = 2 [default = false];</code>
       * @param value The isNull to set.
       * @return This builder for chaining.
       */
      public Builder setIsNull(boolean value) {
        bitField0_ |= 0x00000002;
        isNull_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool is_null = 2 [default = false];</code>
       * @return This builder for chaining.
       */
      public Builder clearIsNull() {
        bitField0_ = (bitField0_ & ~0x00000002);
        isNull_ = false;
        onChanged();
        return this;
      }

      private double doubleValue_ ;
      /**
       * <code>optional double double_value = 3;</code>
       * @return Whether the doubleValue field is set.
       */
      public boolean hasDoubleValue() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional double double_value = 3;</code>
       * @return The doubleValue.
       */
      public double getDoubleValue() {
        return doubleValue_;
      }
      /**
       * <code>optional double double_value = 3;</code>
       * @param value The doubleValue to set.
       * @return This builder for chaining.
       */
      public Builder setDoubleValue(double value) {
        bitField0_ |= 0x00000004;
        doubleValue_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional double double_value = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearDoubleValue() {
        bitField0_ = (bitField0_ & ~0x00000004);
        doubleValue_ = 0D;
        onChanged();
        return this;
      }

      private float floatValue_ ;
      /**
       * <code>optional float float_value = 4;</code>
       * @return Whether the floatValue field is set.
       */
      public boolean hasFloatValue() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional float float_value = 4;</code>
       * @return The floatValue.
       */
      public float getFloatValue() {
        return floatValue_;
      }
      /**
       * <code>optional float float_value = 4;</code>
       * @param value The floatValue to set.
       * @return This builder for chaining.
       */
      public Builder setFloatValue(float value) {
        bitField0_ |= 0x00000008;
        floatValue_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional float float_value = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearFloatValue() {
        bitField0_ = (bitField0_ & ~0x00000008);
        floatValue_ = 0F;
        onChanged();
        return this;
      }

      private int int32Value_ ;
      /**
       * <code>optional int32 int32_value = 5;</code>
       * @return Whether the int32Value field is set.
       */
      public boolean hasInt32Value() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <code>optional int32 int32_value = 5;</code>
       * @return The int32Value.
       */
      public int getInt32Value() {
        return int32Value_;
      }
      /**
       * <code>optional int32 int32_value = 5;</code>
       * @param value The int32Value to set.
       * @return This builder for chaining.
       */
      public Builder setInt32Value(int value) {
        bitField0_ |= 0x00000010;
        int32Value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 int32_value = 5;</code>
       * @return This builder for chaining.
       */
      public Builder clearInt32Value() {
        bitField0_ = (bitField0_ & ~0x00000010);
        int32Value_ = 0;
        onChanged();
        return this;
      }

      private long int64Value_ ;
      /**
       * <code>optional int64 int64_value = 6;</code>
       * @return Whether the int64Value field is set.
       */
      public boolean hasInt64Value() {
        return ((bitField0_ & 0x00000020) != 0);
      }
      /**
       * <code>optional int64 int64_value = 6;</code>
       * @return The int64Value.
       */
      public long getInt64Value() {
        return int64Value_;
      }
      /**
       * <code>optional int64 int64_value = 6;</code>
       * @param value The int64Value to set.
       * @return This builder for chaining.
       */
      public Builder setInt64Value(long value) {
        bitField0_ |= 0x00000020;
        int64Value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 int64_value = 6;</code>
       * @return This builder for chaining.
       */
      public Builder clearInt64Value() {
        bitField0_ = (bitField0_ & ~0x00000020);
        int64Value_ = 0L;
        onChanged();
        return this;
      }

      private int uint32Value_ ;
      /**
       * <code>optional uint32 uint32_value = 7;</code>
       * @return Whether the uint32Value field is set.
       */
      public boolean hasUint32Value() {
        return ((bitField0_ & 0x00000040) != 0);
      }
      /**
       * <code>optional uint32 uint32_value = 7;</code>
       * @return The uint32Value.
       */
      public int getUint32Value() {
        return uint32Value_;
      }
      /**
       * <code>optional uint32 uint32_value = 7;</code>
       * @param value The uint32Value to set.
       * @return This builder for chaining.
       */
      public Builder setUint32Value(int value) {
        bitField0_ |= 0x00000040;
        uint32Value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional uint32 uint32_value = 7;</code>
       * @return This builder for chaining.
       */
      public Builder clearUint32Value() {
        bitField0_ = (bitField0_ & ~0x00000040);
        uint32Value_ = 0;
        onChanged();
        return this;
      }

      private long uint64Value_ ;
      /**
       * <code>optional uint64 uint64_value = 8;</code>
       * @return Whether the uint64Value field is set.
       */
      public boolean hasUint64Value() {
        return ((bitField0_ & 0x00000080) != 0);
      }
      /**
       * <code>optional uint64 uint64_value = 8;</code>
       * @return The uint64Value.
       */
      public long getUint64Value() {
        return uint64Value_;
      }
      /**
       * <code>optional uint64 uint64_value = 8;</code>
       * @param value The uint64Value to set.
       * @return This builder for chaining.
       */
      public Builder setUint64Value(long value) {
        bitField0_ |= 0x00000080;
        uint64Value_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional uint64 uint64_value = 8;</code>
       * @return This builder for chaining.
       */
      public Builder clearUint64Value() {
        bitField0_ = (bitField0_ & ~0x00000080);
        uint64Value_ = 0L;
        onChanged();
        return this;
      }

      private boolean boolValue_ ;
      /**
       * <code>optional bool bool_value = 9;</code>
       * @return Whether the boolValue field is set.
       */
      public boolean hasBoolValue() {
        return ((bitField0_ & 0x00000100) != 0);
      }
      /**
       * <code>optional bool bool_value = 9;</code>
       * @return The boolValue.
       */
      public boolean getBoolValue() {
        return boolValue_;
      }
      /**
       * <code>optional bool bool_value = 9;</code>
       * @param value The boolValue to set.
       * @return This builder for chaining.
       */
      public Builder setBoolValue(boolean value) {
        bitField0_ |= 0x00000100;
        boolValue_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool bool_value = 9;</code>
       * @return This builder for chaining.
       */
      public Builder clearBoolValue() {
        bitField0_ = (bitField0_ & ~0x00000100);
        boolValue_ = false;
        onChanged();
        return this;
      }

      private java.lang.Object stringValue_ = "";
      /**
       * <code>optional string string_value = 10;</code>
       * @return Whether the stringValue field is set.
       */
      public boolean hasStringValue() {
        return ((bitField0_ & 0x00000200) != 0);
      }
      /**
       * <code>optional string string_value = 10;</code>
       * @return The stringValue.
       */
      public java.lang.String getStringValue() {
        java.lang.Object ref = stringValue_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            stringValue_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string string_value = 10;</code>
       * @return The bytes for stringValue.
       */
      public com.google.protobuf.ByteString
          getStringValueBytes() {
        java.lang.Object ref = stringValue_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          stringValue_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string string_value = 10;</code>
       * @param value The stringValue to set.
       * @return This builder for chaining.
       */
      public Builder setStringValue(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000200;
        stringValue_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string string_value = 10;</code>
       * @return This builder for chaining.
       */
      public Builder clearStringValue() {
        bitField0_ = (bitField0_ & ~0x00000200);
        stringValue_ = getDefaultInstance().getStringValue();
        onChanged();
        return this;
      }
      /**
       * <code>optional string string_value = 10;</code>
       * @param value The bytes for stringValue to set.
       * @return This builder for chaining.
       */
      public Builder setStringValueBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000200;
        stringValue_ = value;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString bytesValue_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes bytes_value = 11;</code>
       * @return Whether the bytesValue field is set.
       */
      public boolean hasBytesValue() {
        return ((bitField0_ & 0x00000400) != 0);
      }
      /**
       * <code>optional bytes bytes_value = 11;</code>
       * @return The bytesValue.
       */
      public com.google.protobuf.ByteString getBytesValue() {
        return bytesValue_;
      }
      /**
       * <code>optional bytes bytes_value = 11;</code>
       * @param value The bytesValue to set.
       * @return This builder for chaining.
       */
      public Builder setBytesValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000400;
        bytesValue_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes bytes_value = 11;</code>
       * @return This builder for chaining.
       */
      public Builder clearBytesValue() {
        bitField0_ = (bitField0_ & ~0x00000400);
        bytesValue_ = getDefaultInstance().getBytesValue();
        onChanged();
        return this;
      }

      private org.apache.doris.proto.Types.PDateTime datetimeValue_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder> datetimeValueBuilder_;
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       * @return Whether the datetimeValue field is set.
       */
      public boolean hasDatetimeValue() {
        return ((bitField0_ & 0x00000800) != 0);
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       * @return The datetimeValue.
       */
      public org.apache.doris.proto.Types.PDateTime getDatetimeValue() {
        if (datetimeValueBuilder_ == null) {
          return datetimeValue_ == null ? org.apache.doris.proto.Types.PDateTime.getDefaultInstance() : datetimeValue_;
        } else {
          return datetimeValueBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public Builder setDatetimeValue(org.apache.doris.proto.Types.PDateTime value) {
        if (datetimeValueBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          datetimeValue_ = value;
          onChanged();
        } else {
          datetimeValueBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000800;
        return this;
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public Builder setDatetimeValue(
          org.apache.doris.proto.Types.PDateTime.Builder builderForValue) {
        if (datetimeValueBuilder_ == null) {
          datetimeValue_ = builderForValue.build();
          onChanged();
        } else {
          datetimeValueBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000800;
        return this;
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public Builder mergeDatetimeValue(org.apache.doris.proto.Types.PDateTime value) {
        if (datetimeValueBuilder_ == null) {
          if (((bitField0_ & 0x00000800) != 0) &&
              datetimeValue_ != null &&
              datetimeValue_ != org.apache.doris.proto.Types.PDateTime.getDefaultInstance()) {
            datetimeValue_ =
              org.apache.doris.proto.Types.PDateTime.newBuilder(datetimeValue_).mergeFrom(value).buildPartial();
          } else {
            datetimeValue_ = value;
          }
          onChanged();
        } else {
          datetimeValueBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000800;
        return this;
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public Builder clearDatetimeValue() {
        if (datetimeValueBuilder_ == null) {
          datetimeValue_ = null;
          onChanged();
        } else {
          datetimeValueBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000800);
        return this;
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public org.apache.doris.proto.Types.PDateTime.Builder getDatetimeValueBuilder() {
        bitField0_ |= 0x00000800;
        onChanged();
        return getDatetimeValueFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      public org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder() {
        if (datetimeValueBuilder_ != null) {
          return datetimeValueBuilder_.getMessageOrBuilder();
        } else {
          return datetimeValue_ == null ?
              org.apache.doris.proto.Types.PDateTime.getDefaultInstance() : datetimeValue_;
        }
      }
      /**
       * <code>optional .doris.PDateTime datetime_value = 12;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder> 
          getDatetimeValueFieldBuilder() {
        if (datetimeValueBuilder_ == null) {
          datetimeValueBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder>(
                  getDatetimeValue(),
                  getParentForChildren(),
                  isClean());
          datetimeValue_ = null;
        }
        return datetimeValueBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PValue)
    }

    // @@protoc_insertion_point(class_scope:doris.PValue)
    private static final org.apache.doris.proto.Types.PValue DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PValue();
    }

    public static org.apache.doris.proto.Types.PValue getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PValue>
        PARSER = new com.google.protobuf.AbstractParser<PValue>() {
      @java.lang.Override
      public PValue parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PValue(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PValue> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PValue> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PValue getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PValuesOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PValues)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    org.apache.doris.proto.Types.PGenericType getType();
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder();

    /**
     * <code>optional bool has_null = 2 [default = false];</code>
     * @return Whether the hasNull field is set.
     */
    boolean hasHasNull();
    /**
     * <code>optional bool has_null = 2 [default = false];</code>
     * @return The hasNull.
     */
    boolean getHasNull();

    /**
     * <code>repeated bool null_map = 3;</code>
     * @return A list containing the nullMap.
     */
    java.util.List<java.lang.Boolean> getNullMapList();
    /**
     * <code>repeated bool null_map = 3;</code>
     * @return The count of nullMap.
     */
    int getNullMapCount();
    /**
     * <code>repeated bool null_map = 3;</code>
     * @param index The index of the element to return.
     * @return The nullMap at the given index.
     */
    boolean getNullMap(int index);

    /**
     * <code>repeated double double_value = 4;</code>
     * @return A list containing the doubleValue.
     */
    java.util.List<java.lang.Double> getDoubleValueList();
    /**
     * <code>repeated double double_value = 4;</code>
     * @return The count of doubleValue.
     */
    int getDoubleValueCount();
    /**
     * <code>repeated double double_value = 4;</code>
     * @param index The index of the element to return.
     * @return The doubleValue at the given index.
     */
    double getDoubleValue(int index);

    /**
     * <code>repeated float float_value = 5;</code>
     * @return A list containing the floatValue.
     */
    java.util.List<java.lang.Float> getFloatValueList();
    /**
     * <code>repeated float float_value = 5;</code>
     * @return The count of floatValue.
     */
    int getFloatValueCount();
    /**
     * <code>repeated float float_value = 5;</code>
     * @param index The index of the element to return.
     * @return The floatValue at the given index.
     */
    float getFloatValue(int index);

    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @return A list containing the int32Value.
     */
    java.util.List<java.lang.Integer> getInt32ValueList();
    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @return The count of int32Value.
     */
    int getInt32ValueCount();
    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @param index The index of the element to return.
     * @return The int32Value at the given index.
     */
    int getInt32Value(int index);

    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @return A list containing the int64Value.
     */
    java.util.List<java.lang.Long> getInt64ValueList();
    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @return The count of int64Value.
     */
    int getInt64ValueCount();
    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @param index The index of the element to return.
     * @return The int64Value at the given index.
     */
    long getInt64Value(int index);

    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @return A list containing the uint32Value.
     */
    java.util.List<java.lang.Integer> getUint32ValueList();
    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @return The count of uint32Value.
     */
    int getUint32ValueCount();
    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @param index The index of the element to return.
     * @return The uint32Value at the given index.
     */
    int getUint32Value(int index);

    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @return A list containing the uint64Value.
     */
    java.util.List<java.lang.Long> getUint64ValueList();
    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @return The count of uint64Value.
     */
    int getUint64ValueCount();
    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @param index The index of the element to return.
     * @return The uint64Value at the given index.
     */
    long getUint64Value(int index);

    /**
     * <code>repeated bool bool_value = 10;</code>
     * @return A list containing the boolValue.
     */
    java.util.List<java.lang.Boolean> getBoolValueList();
    /**
     * <code>repeated bool bool_value = 10;</code>
     * @return The count of boolValue.
     */
    int getBoolValueCount();
    /**
     * <code>repeated bool bool_value = 10;</code>
     * @param index The index of the element to return.
     * @return The boolValue at the given index.
     */
    boolean getBoolValue(int index);

    /**
     * <code>repeated string string_value = 11;</code>
     * @return A list containing the stringValue.
     */
    java.util.List<java.lang.String>
        getStringValueList();
    /**
     * <code>repeated string string_value = 11;</code>
     * @return The count of stringValue.
     */
    int getStringValueCount();
    /**
     * <code>repeated string string_value = 11;</code>
     * @param index The index of the element to return.
     * @return The stringValue at the given index.
     */
    java.lang.String getStringValue(int index);
    /**
     * <code>repeated string string_value = 11;</code>
     * @param index The index of the value to return.
     * @return The bytes of the stringValue at the given index.
     */
    com.google.protobuf.ByteString
        getStringValueBytes(int index);

    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @return A list containing the bytesValue.
     */
    java.util.List<com.google.protobuf.ByteString> getBytesValueList();
    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @return The count of bytesValue.
     */
    int getBytesValueCount();
    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @param index The index of the element to return.
     * @return The bytesValue at the given index.
     */
    com.google.protobuf.ByteString getBytesValue(int index);

    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PDateTime> 
        getDatetimeValueList();
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    org.apache.doris.proto.Types.PDateTime getDatetimeValue(int index);
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    int getDatetimeValueCount();
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PDateTimeOrBuilder> 
        getDatetimeValueOrBuilderList();
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder(
        int index);
  }
  /**
   * Protobuf type {@code doris.PValues}
   */
  public  static final class PValues extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PValues)
      PValuesOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PValues.newBuilder() to construct.
    private PValues(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PValues() {
      nullMap_ = emptyBooleanList();
      doubleValue_ = emptyDoubleList();
      floatValue_ = emptyFloatList();
      int32Value_ = emptyIntList();
      int64Value_ = emptyLongList();
      uint32Value_ = emptyIntList();
      uint64Value_ = emptyLongList();
      boolValue_ = emptyBooleanList();
      stringValue_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      bytesValue_ = java.util.Collections.emptyList();
      datetimeValue_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PValues();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PValues(
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
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000001) != 0)) {
                subBuilder = type_.toBuilder();
              }
              type_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(type_);
                type_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000001;
              break;
            }
            case 16: {
              bitField0_ |= 0x00000002;
              hasNull_ = input.readBool();
              break;
            }
            case 24: {
              if (!((mutable_bitField0_ & 0x00000004) != 0)) {
                nullMap_ = newBooleanList();
                mutable_bitField0_ |= 0x00000004;
              }
              nullMap_.addBoolean(input.readBool());
              break;
            }
            case 26: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000004) != 0) && input.getBytesUntilLimit() > 0) {
                nullMap_ = newBooleanList();
                mutable_bitField0_ |= 0x00000004;
              }
              while (input.getBytesUntilLimit() > 0) {
                nullMap_.addBoolean(input.readBool());
              }
              input.popLimit(limit);
              break;
            }
            case 33: {
              if (!((mutable_bitField0_ & 0x00000008) != 0)) {
                doubleValue_ = newDoubleList();
                mutable_bitField0_ |= 0x00000008;
              }
              doubleValue_.addDouble(input.readDouble());
              break;
            }
            case 34: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000008) != 0) && input.getBytesUntilLimit() > 0) {
                doubleValue_ = newDoubleList();
                mutable_bitField0_ |= 0x00000008;
              }
              while (input.getBytesUntilLimit() > 0) {
                doubleValue_.addDouble(input.readDouble());
              }
              input.popLimit(limit);
              break;
            }
            case 45: {
              if (!((mutable_bitField0_ & 0x00000010) != 0)) {
                floatValue_ = newFloatList();
                mutable_bitField0_ |= 0x00000010;
              }
              floatValue_.addFloat(input.readFloat());
              break;
            }
            case 42: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000010) != 0) && input.getBytesUntilLimit() > 0) {
                floatValue_ = newFloatList();
                mutable_bitField0_ |= 0x00000010;
              }
              while (input.getBytesUntilLimit() > 0) {
                floatValue_.addFloat(input.readFloat());
              }
              input.popLimit(limit);
              break;
            }
            case 48: {
              if (!((mutable_bitField0_ & 0x00000020) != 0)) {
                int32Value_ = newIntList();
                mutable_bitField0_ |= 0x00000020;
              }
              int32Value_.addInt(input.readInt32());
              break;
            }
            case 50: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000020) != 0) && input.getBytesUntilLimit() > 0) {
                int32Value_ = newIntList();
                mutable_bitField0_ |= 0x00000020;
              }
              while (input.getBytesUntilLimit() > 0) {
                int32Value_.addInt(input.readInt32());
              }
              input.popLimit(limit);
              break;
            }
            case 56: {
              if (!((mutable_bitField0_ & 0x00000040) != 0)) {
                int64Value_ = newLongList();
                mutable_bitField0_ |= 0x00000040;
              }
              int64Value_.addLong(input.readInt64());
              break;
            }
            case 58: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000040) != 0) && input.getBytesUntilLimit() > 0) {
                int64Value_ = newLongList();
                mutable_bitField0_ |= 0x00000040;
              }
              while (input.getBytesUntilLimit() > 0) {
                int64Value_.addLong(input.readInt64());
              }
              input.popLimit(limit);
              break;
            }
            case 64: {
              if (!((mutable_bitField0_ & 0x00000080) != 0)) {
                uint32Value_ = newIntList();
                mutable_bitField0_ |= 0x00000080;
              }
              uint32Value_.addInt(input.readUInt32());
              break;
            }
            case 66: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000080) != 0) && input.getBytesUntilLimit() > 0) {
                uint32Value_ = newIntList();
                mutable_bitField0_ |= 0x00000080;
              }
              while (input.getBytesUntilLimit() > 0) {
                uint32Value_.addInt(input.readUInt32());
              }
              input.popLimit(limit);
              break;
            }
            case 72: {
              if (!((mutable_bitField0_ & 0x00000100) != 0)) {
                uint64Value_ = newLongList();
                mutable_bitField0_ |= 0x00000100;
              }
              uint64Value_.addLong(input.readUInt64());
              break;
            }
            case 74: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000100) != 0) && input.getBytesUntilLimit() > 0) {
                uint64Value_ = newLongList();
                mutable_bitField0_ |= 0x00000100;
              }
              while (input.getBytesUntilLimit() > 0) {
                uint64Value_.addLong(input.readUInt64());
              }
              input.popLimit(limit);
              break;
            }
            case 80: {
              if (!((mutable_bitField0_ & 0x00000200) != 0)) {
                boolValue_ = newBooleanList();
                mutable_bitField0_ |= 0x00000200;
              }
              boolValue_.addBoolean(input.readBool());
              break;
            }
            case 82: {
              int length = input.readRawVarint32();
              int limit = input.pushLimit(length);
              if (!((mutable_bitField0_ & 0x00000200) != 0) && input.getBytesUntilLimit() > 0) {
                boolValue_ = newBooleanList();
                mutable_bitField0_ |= 0x00000200;
              }
              while (input.getBytesUntilLimit() > 0) {
                boolValue_.addBoolean(input.readBool());
              }
              input.popLimit(limit);
              break;
            }
            case 90: {
              com.google.protobuf.ByteString bs = input.readBytes();
              if (!((mutable_bitField0_ & 0x00000400) != 0)) {
                stringValue_ = new com.google.protobuf.LazyStringArrayList();
                mutable_bitField0_ |= 0x00000400;
              }
              stringValue_.add(bs);
              break;
            }
            case 98: {
              if (!((mutable_bitField0_ & 0x00000800) != 0)) {
                bytesValue_ = new java.util.ArrayList<com.google.protobuf.ByteString>();
                mutable_bitField0_ |= 0x00000800;
              }
              bytesValue_.add(input.readBytes());
              break;
            }
            case 106: {
              if (!((mutable_bitField0_ & 0x00001000) != 0)) {
                datetimeValue_ = new java.util.ArrayList<org.apache.doris.proto.Types.PDateTime>();
                mutable_bitField0_ |= 0x00001000;
              }
              datetimeValue_.add(
                  input.readMessage(org.apache.doris.proto.Types.PDateTime.PARSER, extensionRegistry));
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
        if (((mutable_bitField0_ & 0x00000004) != 0)) {
          nullMap_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000008) != 0)) {
          doubleValue_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000010) != 0)) {
          floatValue_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000020) != 0)) {
          int32Value_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000040) != 0)) {
          int64Value_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000080) != 0)) {
          uint32Value_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000100) != 0)) {
          uint64Value_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000200) != 0)) {
          boolValue_.makeImmutable(); // C
        }
        if (((mutable_bitField0_ & 0x00000400) != 0)) {
          stringValue_ = stringValue_.getUnmodifiableView();
        }
        if (((mutable_bitField0_ & 0x00000800) != 0)) {
          bytesValue_ = java.util.Collections.unmodifiableList(bytesValue_); // C
        }
        if (((mutable_bitField0_ & 0x00001000) != 0)) {
          datetimeValue_ = java.util.Collections.unmodifiableList(datetimeValue_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PValues_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PValues_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PValues.class, org.apache.doris.proto.Types.PValues.Builder.class);
    }

    private int bitField0_;
    public static final int TYPE_FIELD_NUMBER = 1;
    private org.apache.doris.proto.Types.PGenericType type_;
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     * @return The type.
     */
    public org.apache.doris.proto.Types.PGenericType getType() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }
    /**
     * <code>required .doris.PGenericType type = 1;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
      return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
    }

    public static final int HAS_NULL_FIELD_NUMBER = 2;
    private boolean hasNull_;
    /**
     * <code>optional bool has_null = 2 [default = false];</code>
     * @return Whether the hasNull field is set.
     */
    public boolean hasHasNull() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional bool has_null = 2 [default = false];</code>
     * @return The hasNull.
     */
    public boolean getHasNull() {
      return hasNull_;
    }

    public static final int NULL_MAP_FIELD_NUMBER = 3;
    private com.google.protobuf.Internal.BooleanList nullMap_;
    /**
     * <code>repeated bool null_map = 3;</code>
     * @return A list containing the nullMap.
     */
    public java.util.List<java.lang.Boolean>
        getNullMapList() {
      return nullMap_;
    }
    /**
     * <code>repeated bool null_map = 3;</code>
     * @return The count of nullMap.
     */
    public int getNullMapCount() {
      return nullMap_.size();
    }
    /**
     * <code>repeated bool null_map = 3;</code>
     * @param index The index of the element to return.
     * @return The nullMap at the given index.
     */
    public boolean getNullMap(int index) {
      return nullMap_.getBoolean(index);
    }

    public static final int DOUBLE_VALUE_FIELD_NUMBER = 4;
    private com.google.protobuf.Internal.DoubleList doubleValue_;
    /**
     * <code>repeated double double_value = 4;</code>
     * @return A list containing the doubleValue.
     */
    public java.util.List<java.lang.Double>
        getDoubleValueList() {
      return doubleValue_;
    }
    /**
     * <code>repeated double double_value = 4;</code>
     * @return The count of doubleValue.
     */
    public int getDoubleValueCount() {
      return doubleValue_.size();
    }
    /**
     * <code>repeated double double_value = 4;</code>
     * @param index The index of the element to return.
     * @return The doubleValue at the given index.
     */
    public double getDoubleValue(int index) {
      return doubleValue_.getDouble(index);
    }

    public static final int FLOAT_VALUE_FIELD_NUMBER = 5;
    private com.google.protobuf.Internal.FloatList floatValue_;
    /**
     * <code>repeated float float_value = 5;</code>
     * @return A list containing the floatValue.
     */
    public java.util.List<java.lang.Float>
        getFloatValueList() {
      return floatValue_;
    }
    /**
     * <code>repeated float float_value = 5;</code>
     * @return The count of floatValue.
     */
    public int getFloatValueCount() {
      return floatValue_.size();
    }
    /**
     * <code>repeated float float_value = 5;</code>
     * @param index The index of the element to return.
     * @return The floatValue at the given index.
     */
    public float getFloatValue(int index) {
      return floatValue_.getFloat(index);
    }

    public static final int INT32_VALUE_FIELD_NUMBER = 6;
    private com.google.protobuf.Internal.IntList int32Value_;
    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @return A list containing the int32Value.
     */
    public java.util.List<java.lang.Integer>
        getInt32ValueList() {
      return int32Value_;
    }
    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @return The count of int32Value.
     */
    public int getInt32ValueCount() {
      return int32Value_.size();
    }
    /**
     * <code>repeated int32 int32_value = 6;</code>
     * @param index The index of the element to return.
     * @return The int32Value at the given index.
     */
    public int getInt32Value(int index) {
      return int32Value_.getInt(index);
    }

    public static final int INT64_VALUE_FIELD_NUMBER = 7;
    private com.google.protobuf.Internal.LongList int64Value_;
    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @return A list containing the int64Value.
     */
    public java.util.List<java.lang.Long>
        getInt64ValueList() {
      return int64Value_;
    }
    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @return The count of int64Value.
     */
    public int getInt64ValueCount() {
      return int64Value_.size();
    }
    /**
     * <code>repeated int64 int64_value = 7;</code>
     * @param index The index of the element to return.
     * @return The int64Value at the given index.
     */
    public long getInt64Value(int index) {
      return int64Value_.getLong(index);
    }

    public static final int UINT32_VALUE_FIELD_NUMBER = 8;
    private com.google.protobuf.Internal.IntList uint32Value_;
    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @return A list containing the uint32Value.
     */
    public java.util.List<java.lang.Integer>
        getUint32ValueList() {
      return uint32Value_;
    }
    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @return The count of uint32Value.
     */
    public int getUint32ValueCount() {
      return uint32Value_.size();
    }
    /**
     * <code>repeated uint32 uint32_value = 8;</code>
     * @param index The index of the element to return.
     * @return The uint32Value at the given index.
     */
    public int getUint32Value(int index) {
      return uint32Value_.getInt(index);
    }

    public static final int UINT64_VALUE_FIELD_NUMBER = 9;
    private com.google.protobuf.Internal.LongList uint64Value_;
    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @return A list containing the uint64Value.
     */
    public java.util.List<java.lang.Long>
        getUint64ValueList() {
      return uint64Value_;
    }
    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @return The count of uint64Value.
     */
    public int getUint64ValueCount() {
      return uint64Value_.size();
    }
    /**
     * <code>repeated uint64 uint64_value = 9;</code>
     * @param index The index of the element to return.
     * @return The uint64Value at the given index.
     */
    public long getUint64Value(int index) {
      return uint64Value_.getLong(index);
    }

    public static final int BOOL_VALUE_FIELD_NUMBER = 10;
    private com.google.protobuf.Internal.BooleanList boolValue_;
    /**
     * <code>repeated bool bool_value = 10;</code>
     * @return A list containing the boolValue.
     */
    public java.util.List<java.lang.Boolean>
        getBoolValueList() {
      return boolValue_;
    }
    /**
     * <code>repeated bool bool_value = 10;</code>
     * @return The count of boolValue.
     */
    public int getBoolValueCount() {
      return boolValue_.size();
    }
    /**
     * <code>repeated bool bool_value = 10;</code>
     * @param index The index of the element to return.
     * @return The boolValue at the given index.
     */
    public boolean getBoolValue(int index) {
      return boolValue_.getBoolean(index);
    }

    public static final int STRING_VALUE_FIELD_NUMBER = 11;
    private com.google.protobuf.LazyStringList stringValue_;
    /**
     * <code>repeated string string_value = 11;</code>
     * @return A list containing the stringValue.
     */
    public com.google.protobuf.ProtocolStringList
        getStringValueList() {
      return stringValue_;
    }
    /**
     * <code>repeated string string_value = 11;</code>
     * @return The count of stringValue.
     */
    public int getStringValueCount() {
      return stringValue_.size();
    }
    /**
     * <code>repeated string string_value = 11;</code>
     * @param index The index of the element to return.
     * @return The stringValue at the given index.
     */
    public java.lang.String getStringValue(int index) {
      return stringValue_.get(index);
    }
    /**
     * <code>repeated string string_value = 11;</code>
     * @param index The index of the value to return.
     * @return The bytes of the stringValue at the given index.
     */
    public com.google.protobuf.ByteString
        getStringValueBytes(int index) {
      return stringValue_.getByteString(index);
    }

    public static final int BYTES_VALUE_FIELD_NUMBER = 12;
    private java.util.List<com.google.protobuf.ByteString> bytesValue_;
    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @return A list containing the bytesValue.
     */
    public java.util.List<com.google.protobuf.ByteString>
        getBytesValueList() {
      return bytesValue_;
    }
    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @return The count of bytesValue.
     */
    public int getBytesValueCount() {
      return bytesValue_.size();
    }
    /**
     * <code>repeated bytes bytes_value = 12;</code>
     * @param index The index of the element to return.
     * @return The bytesValue at the given index.
     */
    public com.google.protobuf.ByteString getBytesValue(int index) {
      return bytesValue_.get(index);
    }

    public static final int DATETIME_VALUE_FIELD_NUMBER = 13;
    private java.util.List<org.apache.doris.proto.Types.PDateTime> datetimeValue_;
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PDateTime> getDatetimeValueList() {
      return datetimeValue_;
    }
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PDateTimeOrBuilder> 
        getDatetimeValueOrBuilderList() {
      return datetimeValue_;
    }
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    public int getDatetimeValueCount() {
      return datetimeValue_.size();
    }
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    public org.apache.doris.proto.Types.PDateTime getDatetimeValue(int index) {
      return datetimeValue_.get(index);
    }
    /**
     * <code>repeated .doris.PDateTime datetime_value = 13;</code>
     */
    public org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder(
        int index) {
      return datetimeValue_.get(index);
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasType()) {
        memoizedIsInitialized = 0;
        return false;
      }
      if (!getType().isInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeBool(2, hasNull_);
      }
      for (int i = 0; i < nullMap_.size(); i++) {
        output.writeBool(3, nullMap_.getBoolean(i));
      }
      for (int i = 0; i < doubleValue_.size(); i++) {
        output.writeDouble(4, doubleValue_.getDouble(i));
      }
      for (int i = 0; i < floatValue_.size(); i++) {
        output.writeFloat(5, floatValue_.getFloat(i));
      }
      for (int i = 0; i < int32Value_.size(); i++) {
        output.writeInt32(6, int32Value_.getInt(i));
      }
      for (int i = 0; i < int64Value_.size(); i++) {
        output.writeInt64(7, int64Value_.getLong(i));
      }
      for (int i = 0; i < uint32Value_.size(); i++) {
        output.writeUInt32(8, uint32Value_.getInt(i));
      }
      for (int i = 0; i < uint64Value_.size(); i++) {
        output.writeUInt64(9, uint64Value_.getLong(i));
      }
      for (int i = 0; i < boolValue_.size(); i++) {
        output.writeBool(10, boolValue_.getBoolean(i));
      }
      for (int i = 0; i < stringValue_.size(); i++) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 11, stringValue_.getRaw(i));
      }
      for (int i = 0; i < bytesValue_.size(); i++) {
        output.writeBytes(12, bytesValue_.get(i));
      }
      for (int i = 0; i < datetimeValue_.size(); i++) {
        output.writeMessage(13, datetimeValue_.get(i));
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
          .computeMessageSize(1, getType());
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(2, hasNull_);
      }
      {
        int dataSize = 0;
        dataSize = 1 * getNullMapList().size();
        size += dataSize;
        size += 1 * getNullMapList().size();
      }
      {
        int dataSize = 0;
        dataSize = 8 * getDoubleValueList().size();
        size += dataSize;
        size += 1 * getDoubleValueList().size();
      }
      {
        int dataSize = 0;
        dataSize = 4 * getFloatValueList().size();
        size += dataSize;
        size += 1 * getFloatValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < int32Value_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeInt32SizeNoTag(int32Value_.getInt(i));
        }
        size += dataSize;
        size += 1 * getInt32ValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < int64Value_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeInt64SizeNoTag(int64Value_.getLong(i));
        }
        size += dataSize;
        size += 1 * getInt64ValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < uint32Value_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeUInt32SizeNoTag(uint32Value_.getInt(i));
        }
        size += dataSize;
        size += 1 * getUint32ValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < uint64Value_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeUInt64SizeNoTag(uint64Value_.getLong(i));
        }
        size += dataSize;
        size += 1 * getUint64ValueList().size();
      }
      {
        int dataSize = 0;
        dataSize = 1 * getBoolValueList().size();
        size += dataSize;
        size += 1 * getBoolValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < stringValue_.size(); i++) {
          dataSize += computeStringSizeNoTag(stringValue_.getRaw(i));
        }
        size += dataSize;
        size += 1 * getStringValueList().size();
      }
      {
        int dataSize = 0;
        for (int i = 0; i < bytesValue_.size(); i++) {
          dataSize += com.google.protobuf.CodedOutputStream
            .computeBytesSizeNoTag(bytesValue_.get(i));
        }
        size += dataSize;
        size += 1 * getBytesValueList().size();
      }
      for (int i = 0; i < datetimeValue_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(13, datetimeValue_.get(i));
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
      if (!(obj instanceof org.apache.doris.proto.Types.PValues)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PValues other = (org.apache.doris.proto.Types.PValues) obj;

      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (!getType()
            .equals(other.getType())) return false;
      }
      if (hasHasNull() != other.hasHasNull()) return false;
      if (hasHasNull()) {
        if (getHasNull()
            != other.getHasNull()) return false;
      }
      if (!getNullMapList()
          .equals(other.getNullMapList())) return false;
      if (!getDoubleValueList()
          .equals(other.getDoubleValueList())) return false;
      if (!getFloatValueList()
          .equals(other.getFloatValueList())) return false;
      if (!getInt32ValueList()
          .equals(other.getInt32ValueList())) return false;
      if (!getInt64ValueList()
          .equals(other.getInt64ValueList())) return false;
      if (!getUint32ValueList()
          .equals(other.getUint32ValueList())) return false;
      if (!getUint64ValueList()
          .equals(other.getUint64ValueList())) return false;
      if (!getBoolValueList()
          .equals(other.getBoolValueList())) return false;
      if (!getStringValueList()
          .equals(other.getStringValueList())) return false;
      if (!getBytesValueList()
          .equals(other.getBytesValueList())) return false;
      if (!getDatetimeValueList()
          .equals(other.getDatetimeValueList())) return false;
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
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + getType().hashCode();
      }
      if (hasHasNull()) {
        hash = (37 * hash) + HAS_NULL_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getHasNull());
      }
      if (getNullMapCount() > 0) {
        hash = (37 * hash) + NULL_MAP_FIELD_NUMBER;
        hash = (53 * hash) + getNullMapList().hashCode();
      }
      if (getDoubleValueCount() > 0) {
        hash = (37 * hash) + DOUBLE_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getDoubleValueList().hashCode();
      }
      if (getFloatValueCount() > 0) {
        hash = (37 * hash) + FLOAT_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getFloatValueList().hashCode();
      }
      if (getInt32ValueCount() > 0) {
        hash = (37 * hash) + INT32_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getInt32ValueList().hashCode();
      }
      if (getInt64ValueCount() > 0) {
        hash = (37 * hash) + INT64_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getInt64ValueList().hashCode();
      }
      if (getUint32ValueCount() > 0) {
        hash = (37 * hash) + UINT32_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getUint32ValueList().hashCode();
      }
      if (getUint64ValueCount() > 0) {
        hash = (37 * hash) + UINT64_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getUint64ValueList().hashCode();
      }
      if (getBoolValueCount() > 0) {
        hash = (37 * hash) + BOOL_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getBoolValueList().hashCode();
      }
      if (getStringValueCount() > 0) {
        hash = (37 * hash) + STRING_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getStringValueList().hashCode();
      }
      if (getBytesValueCount() > 0) {
        hash = (37 * hash) + BYTES_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getBytesValueList().hashCode();
      }
      if (getDatetimeValueCount() > 0) {
        hash = (37 * hash) + DATETIME_VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getDatetimeValueList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PValues parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValues parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValues parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PValues parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PValues prototype) {
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
     * Protobuf type {@code doris.PValues}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PValues)
        org.apache.doris.proto.Types.PValuesOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PValues_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PValues_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PValues.class, org.apache.doris.proto.Types.PValues.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PValues.newBuilder()
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
          getTypeFieldBuilder();
          getDatetimeValueFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        if (typeBuilder_ == null) {
          type_ = null;
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        hasNull_ = false;
        bitField0_ = (bitField0_ & ~0x00000002);
        nullMap_ = emptyBooleanList();
        bitField0_ = (bitField0_ & ~0x00000004);
        doubleValue_ = emptyDoubleList();
        bitField0_ = (bitField0_ & ~0x00000008);
        floatValue_ = emptyFloatList();
        bitField0_ = (bitField0_ & ~0x00000010);
        int32Value_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000020);
        int64Value_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000040);
        uint32Value_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000080);
        uint64Value_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000100);
        boolValue_ = emptyBooleanList();
        bitField0_ = (bitField0_ & ~0x00000200);
        stringValue_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000400);
        bytesValue_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000800);
        if (datetimeValueBuilder_ == null) {
          datetimeValue_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00001000);
        } else {
          datetimeValueBuilder_.clear();
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PValues_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValues getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PValues.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValues build() {
        org.apache.doris.proto.Types.PValues result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PValues buildPartial() {
        org.apache.doris.proto.Types.PValues result = new org.apache.doris.proto.Types.PValues(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          if (typeBuilder_ == null) {
            result.type_ = type_;
          } else {
            result.type_ = typeBuilder_.build();
          }
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.hasNull_ = hasNull_;
          to_bitField0_ |= 0x00000002;
        }
        if (((bitField0_ & 0x00000004) != 0)) {
          nullMap_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000004);
        }
        result.nullMap_ = nullMap_;
        if (((bitField0_ & 0x00000008) != 0)) {
          doubleValue_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000008);
        }
        result.doubleValue_ = doubleValue_;
        if (((bitField0_ & 0x00000010) != 0)) {
          floatValue_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000010);
        }
        result.floatValue_ = floatValue_;
        if (((bitField0_ & 0x00000020) != 0)) {
          int32Value_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000020);
        }
        result.int32Value_ = int32Value_;
        if (((bitField0_ & 0x00000040) != 0)) {
          int64Value_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000040);
        }
        result.int64Value_ = int64Value_;
        if (((bitField0_ & 0x00000080) != 0)) {
          uint32Value_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000080);
        }
        result.uint32Value_ = uint32Value_;
        if (((bitField0_ & 0x00000100) != 0)) {
          uint64Value_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000100);
        }
        result.uint64Value_ = uint64Value_;
        if (((bitField0_ & 0x00000200) != 0)) {
          boolValue_.makeImmutable();
          bitField0_ = (bitField0_ & ~0x00000200);
        }
        result.boolValue_ = boolValue_;
        if (((bitField0_ & 0x00000400) != 0)) {
          stringValue_ = stringValue_.getUnmodifiableView();
          bitField0_ = (bitField0_ & ~0x00000400);
        }
        result.stringValue_ = stringValue_;
        if (((bitField0_ & 0x00000800) != 0)) {
          bytesValue_ = java.util.Collections.unmodifiableList(bytesValue_);
          bitField0_ = (bitField0_ & ~0x00000800);
        }
        result.bytesValue_ = bytesValue_;
        if (datetimeValueBuilder_ == null) {
          if (((bitField0_ & 0x00001000) != 0)) {
            datetimeValue_ = java.util.Collections.unmodifiableList(datetimeValue_);
            bitField0_ = (bitField0_ & ~0x00001000);
          }
          result.datetimeValue_ = datetimeValue_;
        } else {
          result.datetimeValue_ = datetimeValueBuilder_.build();
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
        if (other instanceof org.apache.doris.proto.Types.PValues) {
          return mergeFrom((org.apache.doris.proto.Types.PValues)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PValues other) {
        if (other == org.apache.doris.proto.Types.PValues.getDefaultInstance()) return this;
        if (other.hasType()) {
          mergeType(other.getType());
        }
        if (other.hasHasNull()) {
          setHasNull(other.getHasNull());
        }
        if (!other.nullMap_.isEmpty()) {
          if (nullMap_.isEmpty()) {
            nullMap_ = other.nullMap_;
            bitField0_ = (bitField0_ & ~0x00000004);
          } else {
            ensureNullMapIsMutable();
            nullMap_.addAll(other.nullMap_);
          }
          onChanged();
        }
        if (!other.doubleValue_.isEmpty()) {
          if (doubleValue_.isEmpty()) {
            doubleValue_ = other.doubleValue_;
            bitField0_ = (bitField0_ & ~0x00000008);
          } else {
            ensureDoubleValueIsMutable();
            doubleValue_.addAll(other.doubleValue_);
          }
          onChanged();
        }
        if (!other.floatValue_.isEmpty()) {
          if (floatValue_.isEmpty()) {
            floatValue_ = other.floatValue_;
            bitField0_ = (bitField0_ & ~0x00000010);
          } else {
            ensureFloatValueIsMutable();
            floatValue_.addAll(other.floatValue_);
          }
          onChanged();
        }
        if (!other.int32Value_.isEmpty()) {
          if (int32Value_.isEmpty()) {
            int32Value_ = other.int32Value_;
            bitField0_ = (bitField0_ & ~0x00000020);
          } else {
            ensureInt32ValueIsMutable();
            int32Value_.addAll(other.int32Value_);
          }
          onChanged();
        }
        if (!other.int64Value_.isEmpty()) {
          if (int64Value_.isEmpty()) {
            int64Value_ = other.int64Value_;
            bitField0_ = (bitField0_ & ~0x00000040);
          } else {
            ensureInt64ValueIsMutable();
            int64Value_.addAll(other.int64Value_);
          }
          onChanged();
        }
        if (!other.uint32Value_.isEmpty()) {
          if (uint32Value_.isEmpty()) {
            uint32Value_ = other.uint32Value_;
            bitField0_ = (bitField0_ & ~0x00000080);
          } else {
            ensureUint32ValueIsMutable();
            uint32Value_.addAll(other.uint32Value_);
          }
          onChanged();
        }
        if (!other.uint64Value_.isEmpty()) {
          if (uint64Value_.isEmpty()) {
            uint64Value_ = other.uint64Value_;
            bitField0_ = (bitField0_ & ~0x00000100);
          } else {
            ensureUint64ValueIsMutable();
            uint64Value_.addAll(other.uint64Value_);
          }
          onChanged();
        }
        if (!other.boolValue_.isEmpty()) {
          if (boolValue_.isEmpty()) {
            boolValue_ = other.boolValue_;
            bitField0_ = (bitField0_ & ~0x00000200);
          } else {
            ensureBoolValueIsMutable();
            boolValue_.addAll(other.boolValue_);
          }
          onChanged();
        }
        if (!other.stringValue_.isEmpty()) {
          if (stringValue_.isEmpty()) {
            stringValue_ = other.stringValue_;
            bitField0_ = (bitField0_ & ~0x00000400);
          } else {
            ensureStringValueIsMutable();
            stringValue_.addAll(other.stringValue_);
          }
          onChanged();
        }
        if (!other.bytesValue_.isEmpty()) {
          if (bytesValue_.isEmpty()) {
            bytesValue_ = other.bytesValue_;
            bitField0_ = (bitField0_ & ~0x00000800);
          } else {
            ensureBytesValueIsMutable();
            bytesValue_.addAll(other.bytesValue_);
          }
          onChanged();
        }
        if (datetimeValueBuilder_ == null) {
          if (!other.datetimeValue_.isEmpty()) {
            if (datetimeValue_.isEmpty()) {
              datetimeValue_ = other.datetimeValue_;
              bitField0_ = (bitField0_ & ~0x00001000);
            } else {
              ensureDatetimeValueIsMutable();
              datetimeValue_.addAll(other.datetimeValue_);
            }
            onChanged();
          }
        } else {
          if (!other.datetimeValue_.isEmpty()) {
            if (datetimeValueBuilder_.isEmpty()) {
              datetimeValueBuilder_.dispose();
              datetimeValueBuilder_ = null;
              datetimeValue_ = other.datetimeValue_;
              bitField0_ = (bitField0_ & ~0x00001000);
              datetimeValueBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getDatetimeValueFieldBuilder() : null;
            } else {
              datetimeValueBuilder_.addAllMessages(other.datetimeValue_);
            }
          }
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasType()) {
          return false;
        }
        if (!getType().isInitialized()) {
          return false;
        }
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PValues parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PValues) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private org.apache.doris.proto.Types.PGenericType type_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> typeBuilder_;
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       * @return The type.
       */
      public org.apache.doris.proto.Types.PGenericType getType() {
        if (typeBuilder_ == null) {
          return type_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        } else {
          return typeBuilder_.getMessage();
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          type_ = value;
          onChanged();
        } else {
          typeBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder setType(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (typeBuilder_ == null) {
          type_ = builderForValue.build();
          onChanged();
        } else {
          typeBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder mergeType(org.apache.doris.proto.Types.PGenericType value) {
        if (typeBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
              type_ != null &&
              type_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            type_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(type_).mergeFrom(value).buildPartial();
          } else {
            type_ = value;
          }
          onChanged();
        } else {
          typeBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public Builder clearType() {
        if (typeBuilder_ == null) {
          type_ = null;
          onChanged();
        } else {
          typeBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getTypeBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getTypeFieldBuilder().getBuilder();
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getTypeOrBuilder() {
        if (typeBuilder_ != null) {
          return typeBuilder_.getMessageOrBuilder();
        } else {
          return type_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : type_;
        }
      }
      /**
       * <code>required .doris.PGenericType type = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getTypeFieldBuilder() {
        if (typeBuilder_ == null) {
          typeBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getType(),
                  getParentForChildren(),
                  isClean());
          type_ = null;
        }
        return typeBuilder_;
      }

      private boolean hasNull_ ;
      /**
       * <code>optional bool has_null = 2 [default = false];</code>
       * @return Whether the hasNull field is set.
       */
      public boolean hasHasNull() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional bool has_null = 2 [default = false];</code>
       * @return The hasNull.
       */
      public boolean getHasNull() {
        return hasNull_;
      }
      /**
       * <code>optional bool has_null = 2 [default = false];</code>
       * @param value The hasNull to set.
       * @return This builder for chaining.
       */
      public Builder setHasNull(boolean value) {
        bitField0_ |= 0x00000002;
        hasNull_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool has_null = 2 [default = false];</code>
       * @return This builder for chaining.
       */
      public Builder clearHasNull() {
        bitField0_ = (bitField0_ & ~0x00000002);
        hasNull_ = false;
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.BooleanList nullMap_ = emptyBooleanList();
      private void ensureNullMapIsMutable() {
        if (!((bitField0_ & 0x00000004) != 0)) {
          nullMap_ = mutableCopy(nullMap_);
          bitField0_ |= 0x00000004;
         }
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @return A list containing the nullMap.
       */
      public java.util.List<java.lang.Boolean>
          getNullMapList() {
        return ((bitField0_ & 0x00000004) != 0) ?
                 java.util.Collections.unmodifiableList(nullMap_) : nullMap_;
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @return The count of nullMap.
       */
      public int getNullMapCount() {
        return nullMap_.size();
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @param index The index of the element to return.
       * @return The nullMap at the given index.
       */
      public boolean getNullMap(int index) {
        return nullMap_.getBoolean(index);
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @param index The index to set the value at.
       * @param value The nullMap to set.
       * @return This builder for chaining.
       */
      public Builder setNullMap(
          int index, boolean value) {
        ensureNullMapIsMutable();
        nullMap_.setBoolean(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @param value The nullMap to add.
       * @return This builder for chaining.
       */
      public Builder addNullMap(boolean value) {
        ensureNullMapIsMutable();
        nullMap_.addBoolean(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @param values The nullMap to add.
       * @return This builder for chaining.
       */
      public Builder addAllNullMap(
          java.lang.Iterable<? extends java.lang.Boolean> values) {
        ensureNullMapIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, nullMap_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool null_map = 3;</code>
       * @return This builder for chaining.
       */
      public Builder clearNullMap() {
        nullMap_ = emptyBooleanList();
        bitField0_ = (bitField0_ & ~0x00000004);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.DoubleList doubleValue_ = emptyDoubleList();
      private void ensureDoubleValueIsMutable() {
        if (!((bitField0_ & 0x00000008) != 0)) {
          doubleValue_ = mutableCopy(doubleValue_);
          bitField0_ |= 0x00000008;
         }
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @return A list containing the doubleValue.
       */
      public java.util.List<java.lang.Double>
          getDoubleValueList() {
        return ((bitField0_ & 0x00000008) != 0) ?
                 java.util.Collections.unmodifiableList(doubleValue_) : doubleValue_;
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @return The count of doubleValue.
       */
      public int getDoubleValueCount() {
        return doubleValue_.size();
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @param index The index of the element to return.
       * @return The doubleValue at the given index.
       */
      public double getDoubleValue(int index) {
        return doubleValue_.getDouble(index);
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @param index The index to set the value at.
       * @param value The doubleValue to set.
       * @return This builder for chaining.
       */
      public Builder setDoubleValue(
          int index, double value) {
        ensureDoubleValueIsMutable();
        doubleValue_.setDouble(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @param value The doubleValue to add.
       * @return This builder for chaining.
       */
      public Builder addDoubleValue(double value) {
        ensureDoubleValueIsMutable();
        doubleValue_.addDouble(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @param values The doubleValue to add.
       * @return This builder for chaining.
       */
      public Builder addAllDoubleValue(
          java.lang.Iterable<? extends java.lang.Double> values) {
        ensureDoubleValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, doubleValue_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated double double_value = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearDoubleValue() {
        doubleValue_ = emptyDoubleList();
        bitField0_ = (bitField0_ & ~0x00000008);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.FloatList floatValue_ = emptyFloatList();
      private void ensureFloatValueIsMutable() {
        if (!((bitField0_ & 0x00000010) != 0)) {
          floatValue_ = mutableCopy(floatValue_);
          bitField0_ |= 0x00000010;
         }
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @return A list containing the floatValue.
       */
      public java.util.List<java.lang.Float>
          getFloatValueList() {
        return ((bitField0_ & 0x00000010) != 0) ?
                 java.util.Collections.unmodifiableList(floatValue_) : floatValue_;
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @return The count of floatValue.
       */
      public int getFloatValueCount() {
        return floatValue_.size();
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @param index The index of the element to return.
       * @return The floatValue at the given index.
       */
      public float getFloatValue(int index) {
        return floatValue_.getFloat(index);
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @param index The index to set the value at.
       * @param value The floatValue to set.
       * @return This builder for chaining.
       */
      public Builder setFloatValue(
          int index, float value) {
        ensureFloatValueIsMutable();
        floatValue_.setFloat(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @param value The floatValue to add.
       * @return This builder for chaining.
       */
      public Builder addFloatValue(float value) {
        ensureFloatValueIsMutable();
        floatValue_.addFloat(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @param values The floatValue to add.
       * @return This builder for chaining.
       */
      public Builder addAllFloatValue(
          java.lang.Iterable<? extends java.lang.Float> values) {
        ensureFloatValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, floatValue_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated float float_value = 5;</code>
       * @return This builder for chaining.
       */
      public Builder clearFloatValue() {
        floatValue_ = emptyFloatList();
        bitField0_ = (bitField0_ & ~0x00000010);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.IntList int32Value_ = emptyIntList();
      private void ensureInt32ValueIsMutable() {
        if (!((bitField0_ & 0x00000020) != 0)) {
          int32Value_ = mutableCopy(int32Value_);
          bitField0_ |= 0x00000020;
         }
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @return A list containing the int32Value.
       */
      public java.util.List<java.lang.Integer>
          getInt32ValueList() {
        return ((bitField0_ & 0x00000020) != 0) ?
                 java.util.Collections.unmodifiableList(int32Value_) : int32Value_;
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @return The count of int32Value.
       */
      public int getInt32ValueCount() {
        return int32Value_.size();
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @param index The index of the element to return.
       * @return The int32Value at the given index.
       */
      public int getInt32Value(int index) {
        return int32Value_.getInt(index);
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @param index The index to set the value at.
       * @param value The int32Value to set.
       * @return This builder for chaining.
       */
      public Builder setInt32Value(
          int index, int value) {
        ensureInt32ValueIsMutable();
        int32Value_.setInt(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @param value The int32Value to add.
       * @return This builder for chaining.
       */
      public Builder addInt32Value(int value) {
        ensureInt32ValueIsMutable();
        int32Value_.addInt(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @param values The int32Value to add.
       * @return This builder for chaining.
       */
      public Builder addAllInt32Value(
          java.lang.Iterable<? extends java.lang.Integer> values) {
        ensureInt32ValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, int32Value_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int32 int32_value = 6;</code>
       * @return This builder for chaining.
       */
      public Builder clearInt32Value() {
        int32Value_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000020);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.LongList int64Value_ = emptyLongList();
      private void ensureInt64ValueIsMutable() {
        if (!((bitField0_ & 0x00000040) != 0)) {
          int64Value_ = mutableCopy(int64Value_);
          bitField0_ |= 0x00000040;
         }
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @return A list containing the int64Value.
       */
      public java.util.List<java.lang.Long>
          getInt64ValueList() {
        return ((bitField0_ & 0x00000040) != 0) ?
                 java.util.Collections.unmodifiableList(int64Value_) : int64Value_;
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @return The count of int64Value.
       */
      public int getInt64ValueCount() {
        return int64Value_.size();
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @param index The index of the element to return.
       * @return The int64Value at the given index.
       */
      public long getInt64Value(int index) {
        return int64Value_.getLong(index);
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @param index The index to set the value at.
       * @param value The int64Value to set.
       * @return This builder for chaining.
       */
      public Builder setInt64Value(
          int index, long value) {
        ensureInt64ValueIsMutable();
        int64Value_.setLong(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @param value The int64Value to add.
       * @return This builder for chaining.
       */
      public Builder addInt64Value(long value) {
        ensureInt64ValueIsMutable();
        int64Value_.addLong(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @param values The int64Value to add.
       * @return This builder for chaining.
       */
      public Builder addAllInt64Value(
          java.lang.Iterable<? extends java.lang.Long> values) {
        ensureInt64ValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, int64Value_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated int64 int64_value = 7;</code>
       * @return This builder for chaining.
       */
      public Builder clearInt64Value() {
        int64Value_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000040);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.IntList uint32Value_ = emptyIntList();
      private void ensureUint32ValueIsMutable() {
        if (!((bitField0_ & 0x00000080) != 0)) {
          uint32Value_ = mutableCopy(uint32Value_);
          bitField0_ |= 0x00000080;
         }
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @return A list containing the uint32Value.
       */
      public java.util.List<java.lang.Integer>
          getUint32ValueList() {
        return ((bitField0_ & 0x00000080) != 0) ?
                 java.util.Collections.unmodifiableList(uint32Value_) : uint32Value_;
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @return The count of uint32Value.
       */
      public int getUint32ValueCount() {
        return uint32Value_.size();
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @param index The index of the element to return.
       * @return The uint32Value at the given index.
       */
      public int getUint32Value(int index) {
        return uint32Value_.getInt(index);
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @param index The index to set the value at.
       * @param value The uint32Value to set.
       * @return This builder for chaining.
       */
      public Builder setUint32Value(
          int index, int value) {
        ensureUint32ValueIsMutable();
        uint32Value_.setInt(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @param value The uint32Value to add.
       * @return This builder for chaining.
       */
      public Builder addUint32Value(int value) {
        ensureUint32ValueIsMutable();
        uint32Value_.addInt(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @param values The uint32Value to add.
       * @return This builder for chaining.
       */
      public Builder addAllUint32Value(
          java.lang.Iterable<? extends java.lang.Integer> values) {
        ensureUint32ValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, uint32Value_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint32 uint32_value = 8;</code>
       * @return This builder for chaining.
       */
      public Builder clearUint32Value() {
        uint32Value_ = emptyIntList();
        bitField0_ = (bitField0_ & ~0x00000080);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.LongList uint64Value_ = emptyLongList();
      private void ensureUint64ValueIsMutable() {
        if (!((bitField0_ & 0x00000100) != 0)) {
          uint64Value_ = mutableCopy(uint64Value_);
          bitField0_ |= 0x00000100;
         }
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @return A list containing the uint64Value.
       */
      public java.util.List<java.lang.Long>
          getUint64ValueList() {
        return ((bitField0_ & 0x00000100) != 0) ?
                 java.util.Collections.unmodifiableList(uint64Value_) : uint64Value_;
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @return The count of uint64Value.
       */
      public int getUint64ValueCount() {
        return uint64Value_.size();
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @param index The index of the element to return.
       * @return The uint64Value at the given index.
       */
      public long getUint64Value(int index) {
        return uint64Value_.getLong(index);
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @param index The index to set the value at.
       * @param value The uint64Value to set.
       * @return This builder for chaining.
       */
      public Builder setUint64Value(
          int index, long value) {
        ensureUint64ValueIsMutable();
        uint64Value_.setLong(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @param value The uint64Value to add.
       * @return This builder for chaining.
       */
      public Builder addUint64Value(long value) {
        ensureUint64ValueIsMutable();
        uint64Value_.addLong(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @param values The uint64Value to add.
       * @return This builder for chaining.
       */
      public Builder addAllUint64Value(
          java.lang.Iterable<? extends java.lang.Long> values) {
        ensureUint64ValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, uint64Value_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated uint64 uint64_value = 9;</code>
       * @return This builder for chaining.
       */
      public Builder clearUint64Value() {
        uint64Value_ = emptyLongList();
        bitField0_ = (bitField0_ & ~0x00000100);
        onChanged();
        return this;
      }

      private com.google.protobuf.Internal.BooleanList boolValue_ = emptyBooleanList();
      private void ensureBoolValueIsMutable() {
        if (!((bitField0_ & 0x00000200) != 0)) {
          boolValue_ = mutableCopy(boolValue_);
          bitField0_ |= 0x00000200;
         }
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @return A list containing the boolValue.
       */
      public java.util.List<java.lang.Boolean>
          getBoolValueList() {
        return ((bitField0_ & 0x00000200) != 0) ?
                 java.util.Collections.unmodifiableList(boolValue_) : boolValue_;
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @return The count of boolValue.
       */
      public int getBoolValueCount() {
        return boolValue_.size();
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @param index The index of the element to return.
       * @return The boolValue at the given index.
       */
      public boolean getBoolValue(int index) {
        return boolValue_.getBoolean(index);
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @param index The index to set the value at.
       * @param value The boolValue to set.
       * @return This builder for chaining.
       */
      public Builder setBoolValue(
          int index, boolean value) {
        ensureBoolValueIsMutable();
        boolValue_.setBoolean(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @param value The boolValue to add.
       * @return This builder for chaining.
       */
      public Builder addBoolValue(boolean value) {
        ensureBoolValueIsMutable();
        boolValue_.addBoolean(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @param values The boolValue to add.
       * @return This builder for chaining.
       */
      public Builder addAllBoolValue(
          java.lang.Iterable<? extends java.lang.Boolean> values) {
        ensureBoolValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, boolValue_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bool bool_value = 10;</code>
       * @return This builder for chaining.
       */
      public Builder clearBoolValue() {
        boolValue_ = emptyBooleanList();
        bitField0_ = (bitField0_ & ~0x00000200);
        onChanged();
        return this;
      }

      private com.google.protobuf.LazyStringList stringValue_ = com.google.protobuf.LazyStringArrayList.EMPTY;
      private void ensureStringValueIsMutable() {
        if (!((bitField0_ & 0x00000400) != 0)) {
          stringValue_ = new com.google.protobuf.LazyStringArrayList(stringValue_);
          bitField0_ |= 0x00000400;
         }
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @return A list containing the stringValue.
       */
      public com.google.protobuf.ProtocolStringList
          getStringValueList() {
        return stringValue_.getUnmodifiableView();
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @return The count of stringValue.
       */
      public int getStringValueCount() {
        return stringValue_.size();
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param index The index of the element to return.
       * @return The stringValue at the given index.
       */
      public java.lang.String getStringValue(int index) {
        return stringValue_.get(index);
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param index The index of the value to return.
       * @return The bytes of the stringValue at the given index.
       */
      public com.google.protobuf.ByteString
          getStringValueBytes(int index) {
        return stringValue_.getByteString(index);
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param index The index to set the value at.
       * @param value The stringValue to set.
       * @return This builder for chaining.
       */
      public Builder setStringValue(
          int index, java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStringValueIsMutable();
        stringValue_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param value The stringValue to add.
       * @return This builder for chaining.
       */
      public Builder addStringValue(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStringValueIsMutable();
        stringValue_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param values The stringValue to add.
       * @return This builder for chaining.
       */
      public Builder addAllStringValue(
          java.lang.Iterable<java.lang.String> values) {
        ensureStringValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, stringValue_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @return This builder for chaining.
       */
      public Builder clearStringValue() {
        stringValue_ = com.google.protobuf.LazyStringArrayList.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000400);
        onChanged();
        return this;
      }
      /**
       * <code>repeated string string_value = 11;</code>
       * @param value The bytes of the stringValue to add.
       * @return This builder for chaining.
       */
      public Builder addStringValueBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureStringValueIsMutable();
        stringValue_.add(value);
        onChanged();
        return this;
      }

      private java.util.List<com.google.protobuf.ByteString> bytesValue_ = java.util.Collections.emptyList();
      private void ensureBytesValueIsMutable() {
        if (!((bitField0_ & 0x00000800) != 0)) {
          bytesValue_ = new java.util.ArrayList<com.google.protobuf.ByteString>(bytesValue_);
          bitField0_ |= 0x00000800;
         }
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @return A list containing the bytesValue.
       */
      public java.util.List<com.google.protobuf.ByteString>
          getBytesValueList() {
        return ((bitField0_ & 0x00000800) != 0) ?
                 java.util.Collections.unmodifiableList(bytesValue_) : bytesValue_;
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @return The count of bytesValue.
       */
      public int getBytesValueCount() {
        return bytesValue_.size();
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @param index The index of the element to return.
       * @return The bytesValue at the given index.
       */
      public com.google.protobuf.ByteString getBytesValue(int index) {
        return bytesValue_.get(index);
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @param index The index to set the value at.
       * @param value The bytesValue to set.
       * @return This builder for chaining.
       */
      public Builder setBytesValue(
          int index, com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureBytesValueIsMutable();
        bytesValue_.set(index, value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @param value The bytesValue to add.
       * @return This builder for chaining.
       */
      public Builder addBytesValue(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  ensureBytesValueIsMutable();
        bytesValue_.add(value);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @param values The bytesValue to add.
       * @return This builder for chaining.
       */
      public Builder addAllBytesValue(
          java.lang.Iterable<? extends com.google.protobuf.ByteString> values) {
        ensureBytesValueIsMutable();
        com.google.protobuf.AbstractMessageLite.Builder.addAll(
            values, bytesValue_);
        onChanged();
        return this;
      }
      /**
       * <code>repeated bytes bytes_value = 12;</code>
       * @return This builder for chaining.
       */
      public Builder clearBytesValue() {
        bytesValue_ = java.util.Collections.emptyList();
        bitField0_ = (bitField0_ & ~0x00000800);
        onChanged();
        return this;
      }

      private java.util.List<org.apache.doris.proto.Types.PDateTime> datetimeValue_ =
        java.util.Collections.emptyList();
      private void ensureDatetimeValueIsMutable() {
        if (!((bitField0_ & 0x00001000) != 0)) {
          datetimeValue_ = new java.util.ArrayList<org.apache.doris.proto.Types.PDateTime>(datetimeValue_);
          bitField0_ |= 0x00001000;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder> datetimeValueBuilder_;

      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PDateTime> getDatetimeValueList() {
        if (datetimeValueBuilder_ == null) {
          return java.util.Collections.unmodifiableList(datetimeValue_);
        } else {
          return datetimeValueBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public int getDatetimeValueCount() {
        if (datetimeValueBuilder_ == null) {
          return datetimeValue_.size();
        } else {
          return datetimeValueBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public org.apache.doris.proto.Types.PDateTime getDatetimeValue(int index) {
        if (datetimeValueBuilder_ == null) {
          return datetimeValue_.get(index);
        } else {
          return datetimeValueBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder setDatetimeValue(
          int index, org.apache.doris.proto.Types.PDateTime value) {
        if (datetimeValueBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureDatetimeValueIsMutable();
          datetimeValue_.set(index, value);
          onChanged();
        } else {
          datetimeValueBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder setDatetimeValue(
          int index, org.apache.doris.proto.Types.PDateTime.Builder builderForValue) {
        if (datetimeValueBuilder_ == null) {
          ensureDatetimeValueIsMutable();
          datetimeValue_.set(index, builderForValue.build());
          onChanged();
        } else {
          datetimeValueBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder addDatetimeValue(org.apache.doris.proto.Types.PDateTime value) {
        if (datetimeValueBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureDatetimeValueIsMutable();
          datetimeValue_.add(value);
          onChanged();
        } else {
          datetimeValueBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder addDatetimeValue(
          int index, org.apache.doris.proto.Types.PDateTime value) {
        if (datetimeValueBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureDatetimeValueIsMutable();
          datetimeValue_.add(index, value);
          onChanged();
        } else {
          datetimeValueBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder addDatetimeValue(
          org.apache.doris.proto.Types.PDateTime.Builder builderForValue) {
        if (datetimeValueBuilder_ == null) {
          ensureDatetimeValueIsMutable();
          datetimeValue_.add(builderForValue.build());
          onChanged();
        } else {
          datetimeValueBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder addDatetimeValue(
          int index, org.apache.doris.proto.Types.PDateTime.Builder builderForValue) {
        if (datetimeValueBuilder_ == null) {
          ensureDatetimeValueIsMutable();
          datetimeValue_.add(index, builderForValue.build());
          onChanged();
        } else {
          datetimeValueBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder addAllDatetimeValue(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PDateTime> values) {
        if (datetimeValueBuilder_ == null) {
          ensureDatetimeValueIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, datetimeValue_);
          onChanged();
        } else {
          datetimeValueBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder clearDatetimeValue() {
        if (datetimeValueBuilder_ == null) {
          datetimeValue_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00001000);
          onChanged();
        } else {
          datetimeValueBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public Builder removeDatetimeValue(int index) {
        if (datetimeValueBuilder_ == null) {
          ensureDatetimeValueIsMutable();
          datetimeValue_.remove(index);
          onChanged();
        } else {
          datetimeValueBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public org.apache.doris.proto.Types.PDateTime.Builder getDatetimeValueBuilder(
          int index) {
        return getDatetimeValueFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public org.apache.doris.proto.Types.PDateTimeOrBuilder getDatetimeValueOrBuilder(
          int index) {
        if (datetimeValueBuilder_ == null) {
          return datetimeValue_.get(index);  } else {
          return datetimeValueBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PDateTimeOrBuilder> 
           getDatetimeValueOrBuilderList() {
        if (datetimeValueBuilder_ != null) {
          return datetimeValueBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(datetimeValue_);
        }
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public org.apache.doris.proto.Types.PDateTime.Builder addDatetimeValueBuilder() {
        return getDatetimeValueFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PDateTime.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public org.apache.doris.proto.Types.PDateTime.Builder addDatetimeValueBuilder(
          int index) {
        return getDatetimeValueFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PDateTime.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PDateTime datetime_value = 13;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PDateTime.Builder> 
           getDatetimeValueBuilderList() {
        return getDatetimeValueFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder> 
          getDatetimeValueFieldBuilder() {
        if (datetimeValueBuilder_ == null) {
          datetimeValueBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PDateTime, org.apache.doris.proto.Types.PDateTime.Builder, org.apache.doris.proto.Types.PDateTimeOrBuilder>(
                  datetimeValue_,
                  ((bitField0_ & 0x00001000) != 0),
                  getParentForChildren(),
                  isClean());
          datetimeValue_ = null;
        }
        return datetimeValueBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PValues)
    }

    // @@protoc_insertion_point(class_scope:doris.PValues)
    private static final org.apache.doris.proto.Types.PValues DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PValues();
    }

    public static org.apache.doris.proto.Types.PValues getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PValues>
        PARSER = new com.google.protobuf.AbstractParser<PValues>() {
      @java.lang.Override
      public PValues parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PValues(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PValues> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PValues> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PValues getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PFunctionOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PFunction)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>required string function_name = 1;</code>
     * @return Whether the functionName field is set.
     */
    boolean hasFunctionName();
    /**
     * <code>required string function_name = 1;</code>
     * @return The functionName.
     */
    java.lang.String getFunctionName();
    /**
     * <code>required string function_name = 1;</code>
     * @return The bytes for functionName.
     */
    com.google.protobuf.ByteString
        getFunctionNameBytes();

    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PGenericType> 
        getInputsList();
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    org.apache.doris.proto.Types.PGenericType getInputs(int index);
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    int getInputsCount();
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
        getInputsOrBuilderList();
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getInputsOrBuilder(
        int index);

    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     * @return Whether the output field is set.
     */
    boolean hasOutput();
    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     * @return The output.
     */
    org.apache.doris.proto.Types.PGenericType getOutput();
    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     */
    org.apache.doris.proto.Types.PGenericTypeOrBuilder getOutputOrBuilder();

    /**
     * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
     * @return Whether the type field is set.
     */
    boolean hasType();
    /**
     * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
     * @return The type.
     */
    org.apache.doris.proto.Types.PFunction.FunctionType getType();

    /**
     * <code>optional bool variadic = 5;</code>
     * @return Whether the variadic field is set.
     */
    boolean hasVariadic();
    /**
     * <code>optional bool variadic = 5;</code>
     * @return The variadic.
     */
    boolean getVariadic();

    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PFunction.Property> 
        getPropertiesList();
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    org.apache.doris.proto.Types.PFunction.Property getProperties(int index);
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    int getPropertiesCount();
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PFunction.PropertyOrBuilder> 
        getPropertiesOrBuilderList();
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    org.apache.doris.proto.Types.PFunction.PropertyOrBuilder getPropertiesOrBuilder(
        int index);
  }
  /**
   * <pre>
   * this mesage may not used for now
   * </pre>
   *
   * Protobuf type {@code doris.PFunction}
   */
  public  static final class PFunction extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PFunction)
      PFunctionOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PFunction.newBuilder() to construct.
    private PFunction(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PFunction() {
      functionName_ = "";
      inputs_ = java.util.Collections.emptyList();
      type_ = 0;
      properties_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PFunction();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PFunction(
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
                inputs_ = new java.util.ArrayList<org.apache.doris.proto.Types.PGenericType>();
                mutable_bitField0_ |= 0x00000002;
              }
              inputs_.add(
                  input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry));
              break;
            }
            case 26: {
              org.apache.doris.proto.Types.PGenericType.Builder subBuilder = null;
              if (((bitField0_ & 0x00000002) != 0)) {
                subBuilder = output_.toBuilder();
              }
              output_ = input.readMessage(org.apache.doris.proto.Types.PGenericType.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(output_);
                output_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000002;
              break;
            }
            case 32: {
              int rawValue = input.readEnum();
                @SuppressWarnings("deprecation")
              org.apache.doris.proto.Types.PFunction.FunctionType value = org.apache.doris.proto.Types.PFunction.FunctionType.valueOf(rawValue);
              if (value == null) {
                unknownFields.mergeVarintField(4, rawValue);
              } else {
                bitField0_ |= 0x00000004;
                type_ = rawValue;
              }
              break;
            }
            case 40: {
              bitField0_ |= 0x00000008;
              variadic_ = input.readBool();
              break;
            }
            case 50: {
              if (!((mutable_bitField0_ & 0x00000020) != 0)) {
                properties_ = new java.util.ArrayList<org.apache.doris.proto.Types.PFunction.Property>();
                mutable_bitField0_ |= 0x00000020;
              }
              properties_.add(
                  input.readMessage(org.apache.doris.proto.Types.PFunction.Property.PARSER, extensionRegistry));
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
          inputs_ = java.util.Collections.unmodifiableList(inputs_);
        }
        if (((mutable_bitField0_ & 0x00000020) != 0)) {
          properties_ = java.util.Collections.unmodifiableList(properties_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PFunction_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PFunction_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PFunction.class, org.apache.doris.proto.Types.PFunction.Builder.class);
    }

    /**
     * Protobuf enum {@code doris.PFunction.FunctionType}
     */
    public enum FunctionType
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>UDF = 0;</code>
       */
      UDF(0),
      /**
       * <pre>
       * not supported now
       * </pre>
       *
       * <code>UDAF = 1;</code>
       */
      UDAF(1),
      /**
       * <code>UDTF = 2;</code>
       */
      UDTF(2),
      ;

      /**
       * <code>UDF = 0;</code>
       */
      public static final int UDF_VALUE = 0;
      /**
       * <pre>
       * not supported now
       * </pre>
       *
       * <code>UDAF = 1;</code>
       */
      public static final int UDAF_VALUE = 1;
      /**
       * <code>UDTF = 2;</code>
       */
      public static final int UDTF_VALUE = 2;


      public final int getNumber() {
        return value;
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       * @deprecated Use {@link #forNumber(int)} instead.
       */
      @java.lang.Deprecated
      public static FunctionType valueOf(int value) {
        return forNumber(value);
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       */
      public static FunctionType forNumber(int value) {
        switch (value) {
          case 0: return UDF;
          case 1: return UDAF;
          case 2: return UDTF;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<FunctionType>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static final com.google.protobuf.Internal.EnumLiteMap<
          FunctionType> internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<FunctionType>() {
              public FunctionType findValueByNumber(int number) {
                return FunctionType.forNumber(number);
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
        return org.apache.doris.proto.Types.PFunction.getDescriptor().getEnumTypes().get(0);
      }

      private static final FunctionType[] VALUES = values();

      public static FunctionType valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int value;

      private FunctionType(int value) {
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:doris.PFunction.FunctionType)
    }

    public interface PropertyOrBuilder extends
        // @@protoc_insertion_point(interface_extends:doris.PFunction.Property)
        com.google.protobuf.MessageOrBuilder {

      /**
       * <code>required string key = 1;</code>
       * @return Whether the key field is set.
       */
      boolean hasKey();
      /**
       * <code>required string key = 1;</code>
       * @return The key.
       */
      java.lang.String getKey();
      /**
       * <code>required string key = 1;</code>
       * @return The bytes for key.
       */
      com.google.protobuf.ByteString
          getKeyBytes();

      /**
       * <code>required string val = 2;</code>
       * @return Whether the val field is set.
       */
      boolean hasVal();
      /**
       * <code>required string val = 2;</code>
       * @return The val.
       */
      java.lang.String getVal();
      /**
       * <code>required string val = 2;</code>
       * @return The bytes for val.
       */
      com.google.protobuf.ByteString
          getValBytes();
    }
    /**
     * Protobuf type {@code doris.PFunction.Property}
     */
    public  static final class Property extends
        com.google.protobuf.GeneratedMessageV3 implements
        // @@protoc_insertion_point(message_implements:doris.PFunction.Property)
        PropertyOrBuilder {
    private static final long serialVersionUID = 0L;
      // Use Property.newBuilder() to construct.
      private Property(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
        super(builder);
      }
      private Property() {
        key_ = "";
        val_ = "";
      }

      @java.lang.Override
      @SuppressWarnings({"unused"})
      protected java.lang.Object newInstance(
          UnusedPrivateParameter unused) {
        return new Property();
      }

      @java.lang.Override
      public final com.google.protobuf.UnknownFieldSet
      getUnknownFields() {
        return this.unknownFields;
      }
      private Property(
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
                key_ = bs;
                break;
              }
              case 18: {
                com.google.protobuf.ByteString bs = input.readBytes();
                bitField0_ |= 0x00000002;
                val_ = bs;
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
        return org.apache.doris.proto.Types.internal_static_doris_PFunction_Property_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunction_Property_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PFunction.Property.class, org.apache.doris.proto.Types.PFunction.Property.Builder.class);
      }

      private int bitField0_;
      public static final int KEY_FIELD_NUMBER = 1;
      private volatile java.lang.Object key_;
      /**
       * <code>required string key = 1;</code>
       * @return Whether the key field is set.
       */
      public boolean hasKey() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required string key = 1;</code>
       * @return The key.
       */
      public java.lang.String getKey() {
        java.lang.Object ref = key_;
        if (ref instanceof java.lang.String) {
          return (java.lang.String) ref;
        } else {
          com.google.protobuf.ByteString bs = 
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            key_ = s;
          }
          return s;
        }
      }
      /**
       * <code>required string key = 1;</code>
       * @return The bytes for key.
       */
      public com.google.protobuf.ByteString
          getKeyBytes() {
        java.lang.Object ref = key_;
        if (ref instanceof java.lang.String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          key_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }

      public static final int VAL_FIELD_NUMBER = 2;
      private volatile java.lang.Object val_;
      /**
       * <code>required string val = 2;</code>
       * @return Whether the val field is set.
       */
      public boolean hasVal() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>required string val = 2;</code>
       * @return The val.
       */
      public java.lang.String getVal() {
        java.lang.Object ref = val_;
        if (ref instanceof java.lang.String) {
          return (java.lang.String) ref;
        } else {
          com.google.protobuf.ByteString bs = 
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            val_ = s;
          }
          return s;
        }
      }
      /**
       * <code>required string val = 2;</code>
       * @return The bytes for val.
       */
      public com.google.protobuf.ByteString
          getValBytes() {
        java.lang.Object ref = val_;
        if (ref instanceof java.lang.String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          val_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }

      private byte memoizedIsInitialized = -1;
      @java.lang.Override
      public final boolean isInitialized() {
        byte isInitialized = memoizedIsInitialized;
        if (isInitialized == 1) return true;
        if (isInitialized == 0) return false;

        if (!hasKey()) {
          memoizedIsInitialized = 0;
          return false;
        }
        if (!hasVal()) {
          memoizedIsInitialized = 0;
          return false;
        }
        memoizedIsInitialized = 1;
        return true;
      }

      @java.lang.Override
      public void writeTo(com.google.protobuf.CodedOutputStream output)
                          throws java.io.IOException {
        if (((bitField0_ & 0x00000001) != 0)) {
          com.google.protobuf.GeneratedMessageV3.writeString(output, 1, key_);
        }
        if (((bitField0_ & 0x00000002) != 0)) {
          com.google.protobuf.GeneratedMessageV3.writeString(output, 2, val_);
        }
        unknownFields.writeTo(output);
      }

      @java.lang.Override
      public int getSerializedSize() {
        int size = memoizedSize;
        if (size != -1) return size;

        size = 0;
        if (((bitField0_ & 0x00000001) != 0)) {
          size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, key_);
        }
        if (((bitField0_ & 0x00000002) != 0)) {
          size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, val_);
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
        if (!(obj instanceof org.apache.doris.proto.Types.PFunction.Property)) {
          return super.equals(obj);
        }
        org.apache.doris.proto.Types.PFunction.Property other = (org.apache.doris.proto.Types.PFunction.Property) obj;

        if (hasKey() != other.hasKey()) return false;
        if (hasKey()) {
          if (!getKey()
              .equals(other.getKey())) return false;
        }
        if (hasVal() != other.hasVal()) return false;
        if (hasVal()) {
          if (!getVal()
              .equals(other.getVal())) return false;
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
        if (hasKey()) {
          hash = (37 * hash) + KEY_FIELD_NUMBER;
          hash = (53 * hash) + getKey().hashCode();
        }
        if (hasVal()) {
          hash = (37 * hash) + VAL_FIELD_NUMBER;
          hash = (53 * hash) + getVal().hashCode();
        }
        hash = (29 * hash) + unknownFields.hashCode();
        memoizedHashCode = hash;
        return hash;
      }

      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          java.nio.ByteBuffer data)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          java.nio.ByteBuffer data,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          com.google.protobuf.ByteString data)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          com.google.protobuf.ByteString data,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(byte[] data)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          byte[] data,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return PARSER.parseFrom(data, extensionRegistry);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(java.io.InputStream input)
          throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
            .parseWithIOException(PARSER, input);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          java.io.InputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
            .parseWithIOException(PARSER, input, extensionRegistry);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseDelimitedFrom(java.io.InputStream input)
          throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
            .parseDelimitedWithIOException(PARSER, input);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseDelimitedFrom(
          java.io.InputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
            .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
          com.google.protobuf.CodedInputStream input)
          throws java.io.IOException {
        return com.google.protobuf.GeneratedMessageV3
            .parseWithIOException(PARSER, input);
      }
      public static org.apache.doris.proto.Types.PFunction.Property parseFrom(
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
      public static Builder newBuilder(org.apache.doris.proto.Types.PFunction.Property prototype) {
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
       * Protobuf type {@code doris.PFunction.Property}
       */
      public static final class Builder extends
          com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
          // @@protoc_insertion_point(builder_implements:doris.PFunction.Property)
          org.apache.doris.proto.Types.PFunction.PropertyOrBuilder {
        public static final com.google.protobuf.Descriptors.Descriptor
            getDescriptor() {
          return org.apache.doris.proto.Types.internal_static_doris_PFunction_Property_descriptor;
        }

        @java.lang.Override
        protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
            internalGetFieldAccessorTable() {
          return org.apache.doris.proto.Types.internal_static_doris_PFunction_Property_fieldAccessorTable
              .ensureFieldAccessorsInitialized(
                  org.apache.doris.proto.Types.PFunction.Property.class, org.apache.doris.proto.Types.PFunction.Property.Builder.class);
        }

        // Construct using org.apache.doris.proto.Types.PFunction.Property.newBuilder()
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
          }
        }
        @java.lang.Override
        public Builder clear() {
          super.clear();
          key_ = "";
          bitField0_ = (bitField0_ & ~0x00000001);
          val_ = "";
          bitField0_ = (bitField0_ & ~0x00000002);
          return this;
        }

        @java.lang.Override
        public com.google.protobuf.Descriptors.Descriptor
            getDescriptorForType() {
          return org.apache.doris.proto.Types.internal_static_doris_PFunction_Property_descriptor;
        }

        @java.lang.Override
        public org.apache.doris.proto.Types.PFunction.Property getDefaultInstanceForType() {
          return org.apache.doris.proto.Types.PFunction.Property.getDefaultInstance();
        }

        @java.lang.Override
        public org.apache.doris.proto.Types.PFunction.Property build() {
          org.apache.doris.proto.Types.PFunction.Property result = buildPartial();
          if (!result.isInitialized()) {
            throw newUninitializedMessageException(result);
          }
          return result;
        }

        @java.lang.Override
        public org.apache.doris.proto.Types.PFunction.Property buildPartial() {
          org.apache.doris.proto.Types.PFunction.Property result = new org.apache.doris.proto.Types.PFunction.Property(this);
          int from_bitField0_ = bitField0_;
          int to_bitField0_ = 0;
          if (((from_bitField0_ & 0x00000001) != 0)) {
            to_bitField0_ |= 0x00000001;
          }
          result.key_ = key_;
          if (((from_bitField0_ & 0x00000002) != 0)) {
            to_bitField0_ |= 0x00000002;
          }
          result.val_ = val_;
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
          if (other instanceof org.apache.doris.proto.Types.PFunction.Property) {
            return mergeFrom((org.apache.doris.proto.Types.PFunction.Property)other);
          } else {
            super.mergeFrom(other);
            return this;
          }
        }

        public Builder mergeFrom(org.apache.doris.proto.Types.PFunction.Property other) {
          if (other == org.apache.doris.proto.Types.PFunction.Property.getDefaultInstance()) return this;
          if (other.hasKey()) {
            bitField0_ |= 0x00000001;
            key_ = other.key_;
            onChanged();
          }
          if (other.hasVal()) {
            bitField0_ |= 0x00000002;
            val_ = other.val_;
            onChanged();
          }
          this.mergeUnknownFields(other.unknownFields);
          onChanged();
          return this;
        }

        @java.lang.Override
        public final boolean isInitialized() {
          if (!hasKey()) {
            return false;
          }
          if (!hasVal()) {
            return false;
          }
          return true;
        }

        @java.lang.Override
        public Builder mergeFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
          org.apache.doris.proto.Types.PFunction.Property parsedMessage = null;
          try {
            parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
          } catch (com.google.protobuf.InvalidProtocolBufferException e) {
            parsedMessage = (org.apache.doris.proto.Types.PFunction.Property) e.getUnfinishedMessage();
            throw e.unwrapIOException();
          } finally {
            if (parsedMessage != null) {
              mergeFrom(parsedMessage);
            }
          }
          return this;
        }
        private int bitField0_;

        private java.lang.Object key_ = "";
        /**
         * <code>required string key = 1;</code>
         * @return Whether the key field is set.
         */
        public boolean hasKey() {
          return ((bitField0_ & 0x00000001) != 0);
        }
        /**
         * <code>required string key = 1;</code>
         * @return The key.
         */
        public java.lang.String getKey() {
          java.lang.Object ref = key_;
          if (!(ref instanceof java.lang.String)) {
            com.google.protobuf.ByteString bs =
                (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
              key_ = s;
            }
            return s;
          } else {
            return (java.lang.String) ref;
          }
        }
        /**
         * <code>required string key = 1;</code>
         * @return The bytes for key.
         */
        public com.google.protobuf.ByteString
            getKeyBytes() {
          java.lang.Object ref = key_;
          if (ref instanceof String) {
            com.google.protobuf.ByteString b = 
                com.google.protobuf.ByteString.copyFromUtf8(
                    (java.lang.String) ref);
            key_ = b;
            return b;
          } else {
            return (com.google.protobuf.ByteString) ref;
          }
        }
        /**
         * <code>required string key = 1;</code>
         * @param value The key to set.
         * @return This builder for chaining.
         */
        public Builder setKey(
            java.lang.String value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
          key_ = value;
          onChanged();
          return this;
        }
        /**
         * <code>required string key = 1;</code>
         * @return This builder for chaining.
         */
        public Builder clearKey() {
          bitField0_ = (bitField0_ & ~0x00000001);
          key_ = getDefaultInstance().getKey();
          onChanged();
          return this;
        }
        /**
         * <code>required string key = 1;</code>
         * @param value The bytes for key to set.
         * @return This builder for chaining.
         */
        public Builder setKeyBytes(
            com.google.protobuf.ByteString value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
          key_ = value;
          onChanged();
          return this;
        }

        private java.lang.Object val_ = "";
        /**
         * <code>required string val = 2;</code>
         * @return Whether the val field is set.
         */
        public boolean hasVal() {
          return ((bitField0_ & 0x00000002) != 0);
        }
        /**
         * <code>required string val = 2;</code>
         * @return The val.
         */
        public java.lang.String getVal() {
          java.lang.Object ref = val_;
          if (!(ref instanceof java.lang.String)) {
            com.google.protobuf.ByteString bs =
                (com.google.protobuf.ByteString) ref;
            java.lang.String s = bs.toStringUtf8();
            if (bs.isValidUtf8()) {
              val_ = s;
            }
            return s;
          } else {
            return (java.lang.String) ref;
          }
        }
        /**
         * <code>required string val = 2;</code>
         * @return The bytes for val.
         */
        public com.google.protobuf.ByteString
            getValBytes() {
          java.lang.Object ref = val_;
          if (ref instanceof String) {
            com.google.protobuf.ByteString b = 
                com.google.protobuf.ByteString.copyFromUtf8(
                    (java.lang.String) ref);
            val_ = b;
            return b;
          } else {
            return (com.google.protobuf.ByteString) ref;
          }
        }
        /**
         * <code>required string val = 2;</code>
         * @param value The val to set.
         * @return This builder for chaining.
         */
        public Builder setVal(
            java.lang.String value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
          val_ = value;
          onChanged();
          return this;
        }
        /**
         * <code>required string val = 2;</code>
         * @return This builder for chaining.
         */
        public Builder clearVal() {
          bitField0_ = (bitField0_ & ~0x00000002);
          val_ = getDefaultInstance().getVal();
          onChanged();
          return this;
        }
        /**
         * <code>required string val = 2;</code>
         * @param value The bytes for val to set.
         * @return This builder for chaining.
         */
        public Builder setValBytes(
            com.google.protobuf.ByteString value) {
          if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
          val_ = value;
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


        // @@protoc_insertion_point(builder_scope:doris.PFunction.Property)
      }

      // @@protoc_insertion_point(class_scope:doris.PFunction.Property)
      private static final org.apache.doris.proto.Types.PFunction.Property DEFAULT_INSTANCE;
      static {
        DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PFunction.Property();
      }

      public static org.apache.doris.proto.Types.PFunction.Property getDefaultInstance() {
        return DEFAULT_INSTANCE;
      }

      @java.lang.Deprecated public static final com.google.protobuf.Parser<Property>
          PARSER = new com.google.protobuf.AbstractParser<Property>() {
        @java.lang.Override
        public Property parsePartialFrom(
            com.google.protobuf.CodedInputStream input,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
          return new Property(input, extensionRegistry);
        }
      };

      public static com.google.protobuf.Parser<Property> parser() {
        return PARSER;
      }

      @java.lang.Override
      public com.google.protobuf.Parser<Property> getParserForType() {
        return PARSER;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunction.Property getDefaultInstanceForType() {
        return DEFAULT_INSTANCE;
      }

    }

    private int bitField0_;
    public static final int FUNCTION_NAME_FIELD_NUMBER = 1;
    private volatile java.lang.Object functionName_;
    /**
     * <code>required string function_name = 1;</code>
     * @return Whether the functionName field is set.
     */
    public boolean hasFunctionName() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>required string function_name = 1;</code>
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
     * <code>required string function_name = 1;</code>
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

    public static final int INPUTS_FIELD_NUMBER = 2;
    private java.util.List<org.apache.doris.proto.Types.PGenericType> inputs_;
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PGenericType> getInputsList() {
      return inputs_;
    }
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
        getInputsOrBuilderList() {
      return inputs_;
    }
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    public int getInputsCount() {
      return inputs_.size();
    }
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    public org.apache.doris.proto.Types.PGenericType getInputs(int index) {
      return inputs_.get(index);
    }
    /**
     * <code>repeated .doris.PGenericType inputs = 2;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getInputsOrBuilder(
        int index) {
      return inputs_.get(index);
    }

    public static final int OUTPUT_FIELD_NUMBER = 3;
    private org.apache.doris.proto.Types.PGenericType output_;
    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     * @return Whether the output field is set.
     */
    public boolean hasOutput() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     * @return The output.
     */
    public org.apache.doris.proto.Types.PGenericType getOutput() {
      return output_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : output_;
    }
    /**
     * <code>optional .doris.PGenericType output = 3;</code>
     */
    public org.apache.doris.proto.Types.PGenericTypeOrBuilder getOutputOrBuilder() {
      return output_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : output_;
    }

    public static final int TYPE_FIELD_NUMBER = 4;
    private int type_;
    /**
     * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
     * @return Whether the type field is set.
     */
    public boolean hasType() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
     * @return The type.
     */
    public org.apache.doris.proto.Types.PFunction.FunctionType getType() {
      @SuppressWarnings("deprecation")
      org.apache.doris.proto.Types.PFunction.FunctionType result = org.apache.doris.proto.Types.PFunction.FunctionType.valueOf(type_);
      return result == null ? org.apache.doris.proto.Types.PFunction.FunctionType.UDF : result;
    }

    public static final int VARIADIC_FIELD_NUMBER = 5;
    private boolean variadic_;
    /**
     * <code>optional bool variadic = 5;</code>
     * @return Whether the variadic field is set.
     */
    public boolean hasVariadic() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional bool variadic = 5;</code>
     * @return The variadic.
     */
    public boolean getVariadic() {
      return variadic_;
    }

    public static final int PROPERTIES_FIELD_NUMBER = 6;
    private java.util.List<org.apache.doris.proto.Types.PFunction.Property> properties_;
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PFunction.Property> getPropertiesList() {
      return properties_;
    }
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PFunction.PropertyOrBuilder> 
        getPropertiesOrBuilderList() {
      return properties_;
    }
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    public int getPropertiesCount() {
      return properties_.size();
    }
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    public org.apache.doris.proto.Types.PFunction.Property getProperties(int index) {
      return properties_.get(index);
    }
    /**
     * <code>repeated .doris.PFunction.Property properties = 6;</code>
     */
    public org.apache.doris.proto.Types.PFunction.PropertyOrBuilder getPropertiesOrBuilder(
        int index) {
      return properties_.get(index);
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!hasFunctionName()) {
        memoizedIsInitialized = 0;
        return false;
      }
      for (int i = 0; i < getInputsCount(); i++) {
        if (!getInputs(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasOutput()) {
        if (!getOutput().isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getPropertiesCount(); i++) {
        if (!getProperties(i).isInitialized()) {
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
      for (int i = 0; i < inputs_.size(); i++) {
        output.writeMessage(2, inputs_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        output.writeMessage(3, getOutput());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeEnum(4, type_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeBool(5, variadic_);
      }
      for (int i = 0; i < properties_.size(); i++) {
        output.writeMessage(6, properties_.get(i));
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
      for (int i = 0; i < inputs_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, inputs_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, getOutput());
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeEnumSize(4, type_);
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBoolSize(5, variadic_);
      }
      for (int i = 0; i < properties_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(6, properties_.get(i));
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
      if (!(obj instanceof org.apache.doris.proto.Types.PFunction)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PFunction other = (org.apache.doris.proto.Types.PFunction) obj;

      if (hasFunctionName() != other.hasFunctionName()) return false;
      if (hasFunctionName()) {
        if (!getFunctionName()
            .equals(other.getFunctionName())) return false;
      }
      if (!getInputsList()
          .equals(other.getInputsList())) return false;
      if (hasOutput() != other.hasOutput()) return false;
      if (hasOutput()) {
        if (!getOutput()
            .equals(other.getOutput())) return false;
      }
      if (hasType() != other.hasType()) return false;
      if (hasType()) {
        if (type_ != other.type_) return false;
      }
      if (hasVariadic() != other.hasVariadic()) return false;
      if (hasVariadic()) {
        if (getVariadic()
            != other.getVariadic()) return false;
      }
      if (!getPropertiesList()
          .equals(other.getPropertiesList())) return false;
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
      if (getInputsCount() > 0) {
        hash = (37 * hash) + INPUTS_FIELD_NUMBER;
        hash = (53 * hash) + getInputsList().hashCode();
      }
      if (hasOutput()) {
        hash = (37 * hash) + OUTPUT_FIELD_NUMBER;
        hash = (53 * hash) + getOutput().hashCode();
      }
      if (hasType()) {
        hash = (37 * hash) + TYPE_FIELD_NUMBER;
        hash = (53 * hash) + type_;
      }
      if (hasVariadic()) {
        hash = (37 * hash) + VARIADIC_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashBoolean(
            getVariadic());
      }
      if (getPropertiesCount() > 0) {
        hash = (37 * hash) + PROPERTIES_FIELD_NUMBER;
        hash = (53 * hash) + getPropertiesList().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PFunction parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunction parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunction parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunction parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PFunction prototype) {
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
     * <pre>
     * this mesage may not used for now
     * </pre>
     *
     * Protobuf type {@code doris.PFunction}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PFunction)
        org.apache.doris.proto.Types.PFunctionOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunction_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunction_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PFunction.class, org.apache.doris.proto.Types.PFunction.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PFunction.newBuilder()
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
          getInputsFieldBuilder();
          getOutputFieldBuilder();
          getPropertiesFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        functionName_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        if (inputsBuilder_ == null) {
          inputs_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          inputsBuilder_.clear();
        }
        if (outputBuilder_ == null) {
          output_ = null;
        } else {
          outputBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        type_ = 0;
        bitField0_ = (bitField0_ & ~0x00000008);
        variadic_ = false;
        bitField0_ = (bitField0_ & ~0x00000010);
        if (propertiesBuilder_ == null) {
          properties_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000020);
        } else {
          propertiesBuilder_.clear();
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunction_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunction getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PFunction.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunction build() {
        org.apache.doris.proto.Types.PFunction result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunction buildPartial() {
        org.apache.doris.proto.Types.PFunction result = new org.apache.doris.proto.Types.PFunction(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.functionName_ = functionName_;
        if (inputsBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)) {
            inputs_ = java.util.Collections.unmodifiableList(inputs_);
            bitField0_ = (bitField0_ & ~0x00000002);
          }
          result.inputs_ = inputs_;
        } else {
          result.inputs_ = inputsBuilder_.build();
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          if (outputBuilder_ == null) {
            result.output_ = output_;
          } else {
            result.output_ = outputBuilder_.build();
          }
          to_bitField0_ |= 0x00000002;
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          to_bitField0_ |= 0x00000004;
        }
        result.type_ = type_;
        if (((from_bitField0_ & 0x00000010) != 0)) {
          result.variadic_ = variadic_;
          to_bitField0_ |= 0x00000008;
        }
        if (propertiesBuilder_ == null) {
          if (((bitField0_ & 0x00000020) != 0)) {
            properties_ = java.util.Collections.unmodifiableList(properties_);
            bitField0_ = (bitField0_ & ~0x00000020);
          }
          result.properties_ = properties_;
        } else {
          result.properties_ = propertiesBuilder_.build();
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
        if (other instanceof org.apache.doris.proto.Types.PFunction) {
          return mergeFrom((org.apache.doris.proto.Types.PFunction)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PFunction other) {
        if (other == org.apache.doris.proto.Types.PFunction.getDefaultInstance()) return this;
        if (other.hasFunctionName()) {
          bitField0_ |= 0x00000001;
          functionName_ = other.functionName_;
          onChanged();
        }
        if (inputsBuilder_ == null) {
          if (!other.inputs_.isEmpty()) {
            if (inputs_.isEmpty()) {
              inputs_ = other.inputs_;
              bitField0_ = (bitField0_ & ~0x00000002);
            } else {
              ensureInputsIsMutable();
              inputs_.addAll(other.inputs_);
            }
            onChanged();
          }
        } else {
          if (!other.inputs_.isEmpty()) {
            if (inputsBuilder_.isEmpty()) {
              inputsBuilder_.dispose();
              inputsBuilder_ = null;
              inputs_ = other.inputs_;
              bitField0_ = (bitField0_ & ~0x00000002);
              inputsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getInputsFieldBuilder() : null;
            } else {
              inputsBuilder_.addAllMessages(other.inputs_);
            }
          }
        }
        if (other.hasOutput()) {
          mergeOutput(other.getOutput());
        }
        if (other.hasType()) {
          setType(other.getType());
        }
        if (other.hasVariadic()) {
          setVariadic(other.getVariadic());
        }
        if (propertiesBuilder_ == null) {
          if (!other.properties_.isEmpty()) {
            if (properties_.isEmpty()) {
              properties_ = other.properties_;
              bitField0_ = (bitField0_ & ~0x00000020);
            } else {
              ensurePropertiesIsMutable();
              properties_.addAll(other.properties_);
            }
            onChanged();
          }
        } else {
          if (!other.properties_.isEmpty()) {
            if (propertiesBuilder_.isEmpty()) {
              propertiesBuilder_.dispose();
              propertiesBuilder_ = null;
              properties_ = other.properties_;
              bitField0_ = (bitField0_ & ~0x00000020);
              propertiesBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getPropertiesFieldBuilder() : null;
            } else {
              propertiesBuilder_.addAllMessages(other.properties_);
            }
          }
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!hasFunctionName()) {
          return false;
        }
        for (int i = 0; i < getInputsCount(); i++) {
          if (!getInputs(i).isInitialized()) {
            return false;
          }
        }
        if (hasOutput()) {
          if (!getOutput().isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getPropertiesCount(); i++) {
          if (!getProperties(i).isInitialized()) {
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
        org.apache.doris.proto.Types.PFunction parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PFunction) e.getUnfinishedMessage();
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
       * <code>required string function_name = 1;</code>
       * @return Whether the functionName field is set.
       */
      public boolean hasFunctionName() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>required string function_name = 1;</code>
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
       * <code>required string function_name = 1;</code>
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
       * <code>required string function_name = 1;</code>
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
       * <code>required string function_name = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearFunctionName() {
        bitField0_ = (bitField0_ & ~0x00000001);
        functionName_ = getDefaultInstance().getFunctionName();
        onChanged();
        return this;
      }
      /**
       * <code>required string function_name = 1;</code>
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

      private java.util.List<org.apache.doris.proto.Types.PGenericType> inputs_ =
        java.util.Collections.emptyList();
      private void ensureInputsIsMutable() {
        if (!((bitField0_ & 0x00000002) != 0)) {
          inputs_ = new java.util.ArrayList<org.apache.doris.proto.Types.PGenericType>(inputs_);
          bitField0_ |= 0x00000002;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> inputsBuilder_;

      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PGenericType> getInputsList() {
        if (inputsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(inputs_);
        } else {
          return inputsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public int getInputsCount() {
        if (inputsBuilder_ == null) {
          return inputs_.size();
        } else {
          return inputsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericType getInputs(int index) {
        if (inputsBuilder_ == null) {
          return inputs_.get(index);
        } else {
          return inputsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder setInputs(
          int index, org.apache.doris.proto.Types.PGenericType value) {
        if (inputsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureInputsIsMutable();
          inputs_.set(index, value);
          onChanged();
        } else {
          inputsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder setInputs(
          int index, org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (inputsBuilder_ == null) {
          ensureInputsIsMutable();
          inputs_.set(index, builderForValue.build());
          onChanged();
        } else {
          inputsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder addInputs(org.apache.doris.proto.Types.PGenericType value) {
        if (inputsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureInputsIsMutable();
          inputs_.add(value);
          onChanged();
        } else {
          inputsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder addInputs(
          int index, org.apache.doris.proto.Types.PGenericType value) {
        if (inputsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureInputsIsMutable();
          inputs_.add(index, value);
          onChanged();
        } else {
          inputsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder addInputs(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (inputsBuilder_ == null) {
          ensureInputsIsMutable();
          inputs_.add(builderForValue.build());
          onChanged();
        } else {
          inputsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder addInputs(
          int index, org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (inputsBuilder_ == null) {
          ensureInputsIsMutable();
          inputs_.add(index, builderForValue.build());
          onChanged();
        } else {
          inputsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder addAllInputs(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PGenericType> values) {
        if (inputsBuilder_ == null) {
          ensureInputsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, inputs_);
          onChanged();
        } else {
          inputsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder clearInputs() {
        if (inputsBuilder_ == null) {
          inputs_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
          onChanged();
        } else {
          inputsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public Builder removeInputs(int index) {
        if (inputsBuilder_ == null) {
          ensureInputsIsMutable();
          inputs_.remove(index);
          onChanged();
        } else {
          inputsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getInputsBuilder(
          int index) {
        return getInputsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getInputsOrBuilder(
          int index) {
        if (inputsBuilder_ == null) {
          return inputs_.get(index);  } else {
          return inputsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
           getInputsOrBuilderList() {
        if (inputsBuilder_ != null) {
          return inputsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(inputs_);
        }
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder addInputsBuilder() {
        return getInputsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PGenericType.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder addInputsBuilder(
          int index) {
        return getInputsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PGenericType.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PGenericType inputs = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PGenericType.Builder> 
           getInputsBuilderList() {
        return getInputsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getInputsFieldBuilder() {
        if (inputsBuilder_ == null) {
          inputsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  inputs_,
                  ((bitField0_ & 0x00000002) != 0),
                  getParentForChildren(),
                  isClean());
          inputs_ = null;
        }
        return inputsBuilder_;
      }

      private org.apache.doris.proto.Types.PGenericType output_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> outputBuilder_;
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       * @return Whether the output field is set.
       */
      public boolean hasOutput() {
        return ((bitField0_ & 0x00000004) != 0);
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       * @return The output.
       */
      public org.apache.doris.proto.Types.PGenericType getOutput() {
        if (outputBuilder_ == null) {
          return output_ == null ? org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : output_;
        } else {
          return outputBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public Builder setOutput(org.apache.doris.proto.Types.PGenericType value) {
        if (outputBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          output_ = value;
          onChanged();
        } else {
          outputBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public Builder setOutput(
          org.apache.doris.proto.Types.PGenericType.Builder builderForValue) {
        if (outputBuilder_ == null) {
          output_ = builderForValue.build();
          onChanged();
        } else {
          outputBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public Builder mergeOutput(org.apache.doris.proto.Types.PGenericType value) {
        if (outputBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0) &&
              output_ != null &&
              output_ != org.apache.doris.proto.Types.PGenericType.getDefaultInstance()) {
            output_ =
              org.apache.doris.proto.Types.PGenericType.newBuilder(output_).mergeFrom(value).buildPartial();
          } else {
            output_ = value;
          }
          onChanged();
        } else {
          outputBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000004;
        return this;
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public Builder clearOutput() {
        if (outputBuilder_ == null) {
          output_ = null;
          onChanged();
        } else {
          outputBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000004);
        return this;
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public org.apache.doris.proto.Types.PGenericType.Builder getOutputBuilder() {
        bitField0_ |= 0x00000004;
        onChanged();
        return getOutputFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      public org.apache.doris.proto.Types.PGenericTypeOrBuilder getOutputOrBuilder() {
        if (outputBuilder_ != null) {
          return outputBuilder_.getMessageOrBuilder();
        } else {
          return output_ == null ?
              org.apache.doris.proto.Types.PGenericType.getDefaultInstance() : output_;
        }
      }
      /**
       * <code>optional .doris.PGenericType output = 3;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder> 
          getOutputFieldBuilder() {
        if (outputBuilder_ == null) {
          outputBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PGenericType, org.apache.doris.proto.Types.PGenericType.Builder, org.apache.doris.proto.Types.PGenericTypeOrBuilder>(
                  getOutput(),
                  getParentForChildren(),
                  isClean());
          output_ = null;
        }
        return outputBuilder_;
      }

      private int type_ = 0;
      /**
       * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
       * @return Whether the type field is set.
       */
      public boolean hasType() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
       * @return The type.
       */
      public org.apache.doris.proto.Types.PFunction.FunctionType getType() {
        @SuppressWarnings("deprecation")
        org.apache.doris.proto.Types.PFunction.FunctionType result = org.apache.doris.proto.Types.PFunction.FunctionType.valueOf(type_);
        return result == null ? org.apache.doris.proto.Types.PFunction.FunctionType.UDF : result;
      }
      /**
       * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
       * @param value The type to set.
       * @return This builder for chaining.
       */
      public Builder setType(org.apache.doris.proto.Types.PFunction.FunctionType value) {
        if (value == null) {
          throw new NullPointerException();
        }
        bitField0_ |= 0x00000008;
        type_ = value.getNumber();
        onChanged();
        return this;
      }
      /**
       * <code>optional .doris.PFunction.FunctionType type = 4 [default = UDF];</code>
       * @return This builder for chaining.
       */
      public Builder clearType() {
        bitField0_ = (bitField0_ & ~0x00000008);
        type_ = 0;
        onChanged();
        return this;
      }

      private boolean variadic_ ;
      /**
       * <code>optional bool variadic = 5;</code>
       * @return Whether the variadic field is set.
       */
      public boolean hasVariadic() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <code>optional bool variadic = 5;</code>
       * @return The variadic.
       */
      public boolean getVariadic() {
        return variadic_;
      }
      /**
       * <code>optional bool variadic = 5;</code>
       * @param value The variadic to set.
       * @return This builder for chaining.
       */
      public Builder setVariadic(boolean value) {
        bitField0_ |= 0x00000010;
        variadic_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bool variadic = 5;</code>
       * @return This builder for chaining.
       */
      public Builder clearVariadic() {
        bitField0_ = (bitField0_ & ~0x00000010);
        variadic_ = false;
        onChanged();
        return this;
      }

      private java.util.List<org.apache.doris.proto.Types.PFunction.Property> properties_ =
        java.util.Collections.emptyList();
      private void ensurePropertiesIsMutable() {
        if (!((bitField0_ & 0x00000020) != 0)) {
          properties_ = new java.util.ArrayList<org.apache.doris.proto.Types.PFunction.Property>(properties_);
          bitField0_ |= 0x00000020;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PFunction.Property, org.apache.doris.proto.Types.PFunction.Property.Builder, org.apache.doris.proto.Types.PFunction.PropertyOrBuilder> propertiesBuilder_;

      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PFunction.Property> getPropertiesList() {
        if (propertiesBuilder_ == null) {
          return java.util.Collections.unmodifiableList(properties_);
        } else {
          return propertiesBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public int getPropertiesCount() {
        if (propertiesBuilder_ == null) {
          return properties_.size();
        } else {
          return propertiesBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public org.apache.doris.proto.Types.PFunction.Property getProperties(int index) {
        if (propertiesBuilder_ == null) {
          return properties_.get(index);
        } else {
          return propertiesBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder setProperties(
          int index, org.apache.doris.proto.Types.PFunction.Property value) {
        if (propertiesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePropertiesIsMutable();
          properties_.set(index, value);
          onChanged();
        } else {
          propertiesBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder setProperties(
          int index, org.apache.doris.proto.Types.PFunction.Property.Builder builderForValue) {
        if (propertiesBuilder_ == null) {
          ensurePropertiesIsMutable();
          properties_.set(index, builderForValue.build());
          onChanged();
        } else {
          propertiesBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder addProperties(org.apache.doris.proto.Types.PFunction.Property value) {
        if (propertiesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePropertiesIsMutable();
          properties_.add(value);
          onChanged();
        } else {
          propertiesBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder addProperties(
          int index, org.apache.doris.proto.Types.PFunction.Property value) {
        if (propertiesBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensurePropertiesIsMutable();
          properties_.add(index, value);
          onChanged();
        } else {
          propertiesBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder addProperties(
          org.apache.doris.proto.Types.PFunction.Property.Builder builderForValue) {
        if (propertiesBuilder_ == null) {
          ensurePropertiesIsMutable();
          properties_.add(builderForValue.build());
          onChanged();
        } else {
          propertiesBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder addProperties(
          int index, org.apache.doris.proto.Types.PFunction.Property.Builder builderForValue) {
        if (propertiesBuilder_ == null) {
          ensurePropertiesIsMutable();
          properties_.add(index, builderForValue.build());
          onChanged();
        } else {
          propertiesBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder addAllProperties(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PFunction.Property> values) {
        if (propertiesBuilder_ == null) {
          ensurePropertiesIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, properties_);
          onChanged();
        } else {
          propertiesBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder clearProperties() {
        if (propertiesBuilder_ == null) {
          properties_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000020);
          onChanged();
        } else {
          propertiesBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public Builder removeProperties(int index) {
        if (propertiesBuilder_ == null) {
          ensurePropertiesIsMutable();
          properties_.remove(index);
          onChanged();
        } else {
          propertiesBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public org.apache.doris.proto.Types.PFunction.Property.Builder getPropertiesBuilder(
          int index) {
        return getPropertiesFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public org.apache.doris.proto.Types.PFunction.PropertyOrBuilder getPropertiesOrBuilder(
          int index) {
        if (propertiesBuilder_ == null) {
          return properties_.get(index);  } else {
          return propertiesBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PFunction.PropertyOrBuilder> 
           getPropertiesOrBuilderList() {
        if (propertiesBuilder_ != null) {
          return propertiesBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(properties_);
        }
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public org.apache.doris.proto.Types.PFunction.Property.Builder addPropertiesBuilder() {
        return getPropertiesFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PFunction.Property.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public org.apache.doris.proto.Types.PFunction.Property.Builder addPropertiesBuilder(
          int index) {
        return getPropertiesFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PFunction.Property.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PFunction.Property properties = 6;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PFunction.Property.Builder> 
           getPropertiesBuilderList() {
        return getPropertiesFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PFunction.Property, org.apache.doris.proto.Types.PFunction.Property.Builder, org.apache.doris.proto.Types.PFunction.PropertyOrBuilder> 
          getPropertiesFieldBuilder() {
        if (propertiesBuilder_ == null) {
          propertiesBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PFunction.Property, org.apache.doris.proto.Types.PFunction.Property.Builder, org.apache.doris.proto.Types.PFunction.PropertyOrBuilder>(
                  properties_,
                  ((bitField0_ & 0x00000020) != 0),
                  getParentForChildren(),
                  isClean());
          properties_ = null;
        }
        return propertiesBuilder_;
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


      // @@protoc_insertion_point(builder_scope:doris.PFunction)
    }

    // @@protoc_insertion_point(class_scope:doris.PFunction)
    private static final org.apache.doris.proto.Types.PFunction DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PFunction();
    }

    public static org.apache.doris.proto.Types.PFunction getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PFunction>
        PARSER = new com.google.protobuf.AbstractParser<PFunction>() {
      @java.lang.Override
      public PFunction parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PFunction(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PFunction> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PFunction> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PFunction getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PFunctionContextOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PFunctionContext)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return Whether the version field is set.
     */
    boolean hasVersion();
    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return The version.
     */
    java.lang.String getVersion();
    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return The bytes for version.
     */
    com.google.protobuf.ByteString
        getVersionBytes();

    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PValue> 
        getStagingInputValsList();
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    org.apache.doris.proto.Types.PValue getStagingInputVals(int index);
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    int getStagingInputValsCount();
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
        getStagingInputValsOrBuilderList();
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    org.apache.doris.proto.Types.PValueOrBuilder getStagingInputValsOrBuilder(
        int index);

    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    java.util.List<org.apache.doris.proto.Types.PValue> 
        getConstantArgsList();
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    org.apache.doris.proto.Types.PValue getConstantArgs(int index);
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    int getConstantArgsCount();
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
        getConstantArgsOrBuilderList();
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    org.apache.doris.proto.Types.PValueOrBuilder getConstantArgsOrBuilder(
        int index);

    /**
     * <code>optional string error_msg = 4;</code>
     * @return Whether the errorMsg field is set.
     */
    boolean hasErrorMsg();
    /**
     * <code>optional string error_msg = 4;</code>
     * @return The errorMsg.
     */
    java.lang.String getErrorMsg();
    /**
     * <code>optional string error_msg = 4;</code>
     * @return The bytes for errorMsg.
     */
    com.google.protobuf.ByteString
        getErrorMsgBytes();

    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     * @return Whether the queryId field is set.
     */
    boolean hasQueryId();
    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     * @return The queryId.
     */
    org.apache.doris.proto.Types.PUniqueId getQueryId();
    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     */
    org.apache.doris.proto.Types.PUniqueIdOrBuilder getQueryIdOrBuilder();

    /**
     * <code>optional bytes thread_local_fn_state = 6;</code>
     * @return Whether the threadLocalFnState field is set.
     */
    boolean hasThreadLocalFnState();
    /**
     * <code>optional bytes thread_local_fn_state = 6;</code>
     * @return The threadLocalFnState.
     */
    com.google.protobuf.ByteString getThreadLocalFnState();

    /**
     * <code>optional bytes fragment_local_fn_state = 7;</code>
     * @return Whether the fragmentLocalFnState field is set.
     */
    boolean hasFragmentLocalFnState();
    /**
     * <code>optional bytes fragment_local_fn_state = 7;</code>
     * @return The fragmentLocalFnState.
     */
    com.google.protobuf.ByteString getFragmentLocalFnState();

    /**
     * <code>optional string string_result = 8;</code>
     * @return Whether the stringResult field is set.
     */
    boolean hasStringResult();
    /**
     * <code>optional string string_result = 8;</code>
     * @return The stringResult.
     */
    java.lang.String getStringResult();
    /**
     * <code>optional string string_result = 8;</code>
     * @return The bytes for stringResult.
     */
    com.google.protobuf.ByteString
        getStringResultBytes();

    /**
     * <code>optional int64 num_updates = 9;</code>
     * @return Whether the numUpdates field is set.
     */
    boolean hasNumUpdates();
    /**
     * <code>optional int64 num_updates = 9;</code>
     * @return The numUpdates.
     */
    long getNumUpdates();

    /**
     * <code>optional int64 num_removes = 10;</code>
     * @return Whether the numRemoves field is set.
     */
    boolean hasNumRemoves();
    /**
     * <code>optional int64 num_removes = 10;</code>
     * @return The numRemoves.
     */
    long getNumRemoves();

    /**
     * <code>optional int64 num_warnings = 11;</code>
     * @return Whether the numWarnings field is set.
     */
    boolean hasNumWarnings();
    /**
     * <code>optional int64 num_warnings = 11;</code>
     * @return The numWarnings.
     */
    long getNumWarnings();
  }
  /**
   * Protobuf type {@code doris.PFunctionContext}
   */
  public  static final class PFunctionContext extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PFunctionContext)
      PFunctionContextOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PFunctionContext.newBuilder() to construct.
    private PFunctionContext(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PFunctionContext() {
      version_ = "V2_0";
      stagingInputVals_ = java.util.Collections.emptyList();
      constantArgs_ = java.util.Collections.emptyList();
      errorMsg_ = "";
      threadLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
      fragmentLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
      stringResult_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PFunctionContext();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PFunctionContext(
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
              version_ = bs;
              break;
            }
            case 18: {
              if (!((mutable_bitField0_ & 0x00000002) != 0)) {
                stagingInputVals_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValue>();
                mutable_bitField0_ |= 0x00000002;
              }
              stagingInputVals_.add(
                  input.readMessage(org.apache.doris.proto.Types.PValue.PARSER, extensionRegistry));
              break;
            }
            case 26: {
              if (!((mutable_bitField0_ & 0x00000004) != 0)) {
                constantArgs_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValue>();
                mutable_bitField0_ |= 0x00000004;
              }
              constantArgs_.add(
                  input.readMessage(org.apache.doris.proto.Types.PValue.PARSER, extensionRegistry));
              break;
            }
            case 34: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              errorMsg_ = bs;
              break;
            }
            case 42: {
              org.apache.doris.proto.Types.PUniqueId.Builder subBuilder = null;
              if (((bitField0_ & 0x00000004) != 0)) {
                subBuilder = queryId_.toBuilder();
              }
              queryId_ = input.readMessage(org.apache.doris.proto.Types.PUniqueId.PARSER, extensionRegistry);
              if (subBuilder != null) {
                subBuilder.mergeFrom(queryId_);
                queryId_ = subBuilder.buildPartial();
              }
              bitField0_ |= 0x00000004;
              break;
            }
            case 50: {
              bitField0_ |= 0x00000008;
              threadLocalFnState_ = input.readBytes();
              break;
            }
            case 58: {
              bitField0_ |= 0x00000010;
              fragmentLocalFnState_ = input.readBytes();
              break;
            }
            case 66: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000020;
              stringResult_ = bs;
              break;
            }
            case 72: {
              bitField0_ |= 0x00000040;
              numUpdates_ = input.readInt64();
              break;
            }
            case 80: {
              bitField0_ |= 0x00000080;
              numRemoves_ = input.readInt64();
              break;
            }
            case 88: {
              bitField0_ |= 0x00000100;
              numWarnings_ = input.readInt64();
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
          stagingInputVals_ = java.util.Collections.unmodifiableList(stagingInputVals_);
        }
        if (((mutable_bitField0_ & 0x00000004) != 0)) {
          constantArgs_ = java.util.Collections.unmodifiableList(constantArgs_);
        }
        this.unknownFields = unknownFields.build();
        makeExtensionsImmutable();
      }
    }
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return org.apache.doris.proto.Types.internal_static_doris_PFunctionContext_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PFunctionContext_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PFunctionContext.class, org.apache.doris.proto.Types.PFunctionContext.Builder.class);
    }

    private int bitField0_;
    public static final int VERSION_FIELD_NUMBER = 1;
    private volatile java.lang.Object version_;
    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return Whether the version field is set.
     */
    public boolean hasVersion() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return The version.
     */
    public java.lang.String getVersion() {
      java.lang.Object ref = version_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          version_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string version = 1 [default = "V2_0"];</code>
     * @return The bytes for version.
     */
    public com.google.protobuf.ByteString
        getVersionBytes() {
      java.lang.Object ref = version_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        version_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int STAGING_INPUT_VALS_FIELD_NUMBER = 2;
    private java.util.List<org.apache.doris.proto.Types.PValue> stagingInputVals_;
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PValue> getStagingInputValsList() {
      return stagingInputVals_;
    }
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
        getStagingInputValsOrBuilderList() {
      return stagingInputVals_;
    }
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    public int getStagingInputValsCount() {
      return stagingInputVals_.size();
    }
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    public org.apache.doris.proto.Types.PValue getStagingInputVals(int index) {
      return stagingInputVals_.get(index);
    }
    /**
     * <code>repeated .doris.PValue staging_input_vals = 2;</code>
     */
    public org.apache.doris.proto.Types.PValueOrBuilder getStagingInputValsOrBuilder(
        int index) {
      return stagingInputVals_.get(index);
    }

    public static final int CONSTANT_ARGS_FIELD_NUMBER = 3;
    private java.util.List<org.apache.doris.proto.Types.PValue> constantArgs_;
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    public java.util.List<org.apache.doris.proto.Types.PValue> getConstantArgsList() {
      return constantArgs_;
    }
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    public java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
        getConstantArgsOrBuilderList() {
      return constantArgs_;
    }
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    public int getConstantArgsCount() {
      return constantArgs_.size();
    }
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    public org.apache.doris.proto.Types.PValue getConstantArgs(int index) {
      return constantArgs_.get(index);
    }
    /**
     * <code>repeated .doris.PValue constant_args = 3;</code>
     */
    public org.apache.doris.proto.Types.PValueOrBuilder getConstantArgsOrBuilder(
        int index) {
      return constantArgs_.get(index);
    }

    public static final int ERROR_MSG_FIELD_NUMBER = 4;
    private volatile java.lang.Object errorMsg_;
    /**
     * <code>optional string error_msg = 4;</code>
     * @return Whether the errorMsg field is set.
     */
    public boolean hasErrorMsg() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string error_msg = 4;</code>
     * @return The errorMsg.
     */
    public java.lang.String getErrorMsg() {
      java.lang.Object ref = errorMsg_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          errorMsg_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string error_msg = 4;</code>
     * @return The bytes for errorMsg.
     */
    public com.google.protobuf.ByteString
        getErrorMsgBytes() {
      java.lang.Object ref = errorMsg_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        errorMsg_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int QUERY_ID_FIELD_NUMBER = 5;
    private org.apache.doris.proto.Types.PUniqueId queryId_;
    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     * @return Whether the queryId field is set.
     */
    public boolean hasQueryId() {
      return ((bitField0_ & 0x00000004) != 0);
    }
    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     * @return The queryId.
     */
    public org.apache.doris.proto.Types.PUniqueId getQueryId() {
      return queryId_ == null ? org.apache.doris.proto.Types.PUniqueId.getDefaultInstance() : queryId_;
    }
    /**
     * <code>optional .doris.PUniqueId query_id = 5;</code>
     */
    public org.apache.doris.proto.Types.PUniqueIdOrBuilder getQueryIdOrBuilder() {
      return queryId_ == null ? org.apache.doris.proto.Types.PUniqueId.getDefaultInstance() : queryId_;
    }

    public static final int THREAD_LOCAL_FN_STATE_FIELD_NUMBER = 6;
    private com.google.protobuf.ByteString threadLocalFnState_;
    /**
     * <code>optional bytes thread_local_fn_state = 6;</code>
     * @return Whether the threadLocalFnState field is set.
     */
    public boolean hasThreadLocalFnState() {
      return ((bitField0_ & 0x00000008) != 0);
    }
    /**
     * <code>optional bytes thread_local_fn_state = 6;</code>
     * @return The threadLocalFnState.
     */
    public com.google.protobuf.ByteString getThreadLocalFnState() {
      return threadLocalFnState_;
    }

    public static final int FRAGMENT_LOCAL_FN_STATE_FIELD_NUMBER = 7;
    private com.google.protobuf.ByteString fragmentLocalFnState_;
    /**
     * <code>optional bytes fragment_local_fn_state = 7;</code>
     * @return Whether the fragmentLocalFnState field is set.
     */
    public boolean hasFragmentLocalFnState() {
      return ((bitField0_ & 0x00000010) != 0);
    }
    /**
     * <code>optional bytes fragment_local_fn_state = 7;</code>
     * @return The fragmentLocalFnState.
     */
    public com.google.protobuf.ByteString getFragmentLocalFnState() {
      return fragmentLocalFnState_;
    }

    public static final int STRING_RESULT_FIELD_NUMBER = 8;
    private volatile java.lang.Object stringResult_;
    /**
     * <code>optional string string_result = 8;</code>
     * @return Whether the stringResult field is set.
     */
    public boolean hasStringResult() {
      return ((bitField0_ & 0x00000020) != 0);
    }
    /**
     * <code>optional string string_result = 8;</code>
     * @return The stringResult.
     */
    public java.lang.String getStringResult() {
      java.lang.Object ref = stringResult_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          stringResult_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string string_result = 8;</code>
     * @return The bytes for stringResult.
     */
    public com.google.protobuf.ByteString
        getStringResultBytes() {
      java.lang.Object ref = stringResult_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        stringResult_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int NUM_UPDATES_FIELD_NUMBER = 9;
    private long numUpdates_;
    /**
     * <code>optional int64 num_updates = 9;</code>
     * @return Whether the numUpdates field is set.
     */
    public boolean hasNumUpdates() {
      return ((bitField0_ & 0x00000040) != 0);
    }
    /**
     * <code>optional int64 num_updates = 9;</code>
     * @return The numUpdates.
     */
    public long getNumUpdates() {
      return numUpdates_;
    }

    public static final int NUM_REMOVES_FIELD_NUMBER = 10;
    private long numRemoves_;
    /**
     * <code>optional int64 num_removes = 10;</code>
     * @return Whether the numRemoves field is set.
     */
    public boolean hasNumRemoves() {
      return ((bitField0_ & 0x00000080) != 0);
    }
    /**
     * <code>optional int64 num_removes = 10;</code>
     * @return The numRemoves.
     */
    public long getNumRemoves() {
      return numRemoves_;
    }

    public static final int NUM_WARNINGS_FIELD_NUMBER = 11;
    private long numWarnings_;
    /**
     * <code>optional int64 num_warnings = 11;</code>
     * @return Whether the numWarnings field is set.
     */
    public boolean hasNumWarnings() {
      return ((bitField0_ & 0x00000100) != 0);
    }
    /**
     * <code>optional int64 num_warnings = 11;</code>
     * @return The numWarnings.
     */
    public long getNumWarnings() {
      return numWarnings_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      for (int i = 0; i < getStagingInputValsCount(); i++) {
        if (!getStagingInputVals(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      for (int i = 0; i < getConstantArgsCount(); i++) {
        if (!getConstantArgs(i).isInitialized()) {
          memoizedIsInitialized = 0;
          return false;
        }
      }
      if (hasQueryId()) {
        if (!getQueryId().isInitialized()) {
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
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, version_);
      }
      for (int i = 0; i < stagingInputVals_.size(); i++) {
        output.writeMessage(2, stagingInputVals_.get(i));
      }
      for (int i = 0; i < constantArgs_.size(); i++) {
        output.writeMessage(3, constantArgs_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 4, errorMsg_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        output.writeMessage(5, getQueryId());
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        output.writeBytes(6, threadLocalFnState_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        output.writeBytes(7, fragmentLocalFnState_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 8, stringResult_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        output.writeInt64(9, numUpdates_);
      }
      if (((bitField0_ & 0x00000080) != 0)) {
        output.writeInt64(10, numRemoves_);
      }
      if (((bitField0_ & 0x00000100) != 0)) {
        output.writeInt64(11, numWarnings_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, version_);
      }
      for (int i = 0; i < stagingInputVals_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(2, stagingInputVals_.get(i));
      }
      for (int i = 0; i < constantArgs_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(3, constantArgs_.get(i));
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(4, errorMsg_);
      }
      if (((bitField0_ & 0x00000004) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(5, getQueryId());
      }
      if (((bitField0_ & 0x00000008) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(6, threadLocalFnState_);
      }
      if (((bitField0_ & 0x00000010) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeBytesSize(7, fragmentLocalFnState_);
      }
      if (((bitField0_ & 0x00000020) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(8, stringResult_);
      }
      if (((bitField0_ & 0x00000040) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(9, numUpdates_);
      }
      if (((bitField0_ & 0x00000080) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(10, numRemoves_);
      }
      if (((bitField0_ & 0x00000100) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt64Size(11, numWarnings_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PFunctionContext)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PFunctionContext other = (org.apache.doris.proto.Types.PFunctionContext) obj;

      if (hasVersion() != other.hasVersion()) return false;
      if (hasVersion()) {
        if (!getVersion()
            .equals(other.getVersion())) return false;
      }
      if (!getStagingInputValsList()
          .equals(other.getStagingInputValsList())) return false;
      if (!getConstantArgsList()
          .equals(other.getConstantArgsList())) return false;
      if (hasErrorMsg() != other.hasErrorMsg()) return false;
      if (hasErrorMsg()) {
        if (!getErrorMsg()
            .equals(other.getErrorMsg())) return false;
      }
      if (hasQueryId() != other.hasQueryId()) return false;
      if (hasQueryId()) {
        if (!getQueryId()
            .equals(other.getQueryId())) return false;
      }
      if (hasThreadLocalFnState() != other.hasThreadLocalFnState()) return false;
      if (hasThreadLocalFnState()) {
        if (!getThreadLocalFnState()
            .equals(other.getThreadLocalFnState())) return false;
      }
      if (hasFragmentLocalFnState() != other.hasFragmentLocalFnState()) return false;
      if (hasFragmentLocalFnState()) {
        if (!getFragmentLocalFnState()
            .equals(other.getFragmentLocalFnState())) return false;
      }
      if (hasStringResult() != other.hasStringResult()) return false;
      if (hasStringResult()) {
        if (!getStringResult()
            .equals(other.getStringResult())) return false;
      }
      if (hasNumUpdates() != other.hasNumUpdates()) return false;
      if (hasNumUpdates()) {
        if (getNumUpdates()
            != other.getNumUpdates()) return false;
      }
      if (hasNumRemoves() != other.hasNumRemoves()) return false;
      if (hasNumRemoves()) {
        if (getNumRemoves()
            != other.getNumRemoves()) return false;
      }
      if (hasNumWarnings() != other.hasNumWarnings()) return false;
      if (hasNumWarnings()) {
        if (getNumWarnings()
            != other.getNumWarnings()) return false;
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
      if (hasVersion()) {
        hash = (37 * hash) + VERSION_FIELD_NUMBER;
        hash = (53 * hash) + getVersion().hashCode();
      }
      if (getStagingInputValsCount() > 0) {
        hash = (37 * hash) + STAGING_INPUT_VALS_FIELD_NUMBER;
        hash = (53 * hash) + getStagingInputValsList().hashCode();
      }
      if (getConstantArgsCount() > 0) {
        hash = (37 * hash) + CONSTANT_ARGS_FIELD_NUMBER;
        hash = (53 * hash) + getConstantArgsList().hashCode();
      }
      if (hasErrorMsg()) {
        hash = (37 * hash) + ERROR_MSG_FIELD_NUMBER;
        hash = (53 * hash) + getErrorMsg().hashCode();
      }
      if (hasQueryId()) {
        hash = (37 * hash) + QUERY_ID_FIELD_NUMBER;
        hash = (53 * hash) + getQueryId().hashCode();
      }
      if (hasThreadLocalFnState()) {
        hash = (37 * hash) + THREAD_LOCAL_FN_STATE_FIELD_NUMBER;
        hash = (53 * hash) + getThreadLocalFnState().hashCode();
      }
      if (hasFragmentLocalFnState()) {
        hash = (37 * hash) + FRAGMENT_LOCAL_FN_STATE_FIELD_NUMBER;
        hash = (53 * hash) + getFragmentLocalFnState().hashCode();
      }
      if (hasStringResult()) {
        hash = (37 * hash) + STRING_RESULT_FIELD_NUMBER;
        hash = (53 * hash) + getStringResult().hashCode();
      }
      if (hasNumUpdates()) {
        hash = (37 * hash) + NUM_UPDATES_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getNumUpdates());
      }
      if (hasNumRemoves()) {
        hash = (37 * hash) + NUM_REMOVES_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getNumRemoves());
      }
      if (hasNumWarnings()) {
        hash = (37 * hash) + NUM_WARNINGS_FIELD_NUMBER;
        hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
            getNumWarnings());
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PFunctionContext parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PFunctionContext prototype) {
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
     * Protobuf type {@code doris.PFunctionContext}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PFunctionContext)
        org.apache.doris.proto.Types.PFunctionContextOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunctionContext_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunctionContext_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PFunctionContext.class, org.apache.doris.proto.Types.PFunctionContext.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PFunctionContext.newBuilder()
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
          getStagingInputValsFieldBuilder();
          getConstantArgsFieldBuilder();
          getQueryIdFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        version_ = "V2_0";
        bitField0_ = (bitField0_ & ~0x00000001);
        if (stagingInputValsBuilder_ == null) {
          stagingInputVals_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
        } else {
          stagingInputValsBuilder_.clear();
        }
        if (constantArgsBuilder_ == null) {
          constantArgs_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
        } else {
          constantArgsBuilder_.clear();
        }
        errorMsg_ = "";
        bitField0_ = (bitField0_ & ~0x00000008);
        if (queryIdBuilder_ == null) {
          queryId_ = null;
        } else {
          queryIdBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000010);
        threadLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000020);
        fragmentLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
        bitField0_ = (bitField0_ & ~0x00000040);
        stringResult_ = "";
        bitField0_ = (bitField0_ & ~0x00000080);
        numUpdates_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000100);
        numRemoves_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000200);
        numWarnings_ = 0L;
        bitField0_ = (bitField0_ & ~0x00000400);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PFunctionContext_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunctionContext getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunctionContext build() {
        org.apache.doris.proto.Types.PFunctionContext result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PFunctionContext buildPartial() {
        org.apache.doris.proto.Types.PFunctionContext result = new org.apache.doris.proto.Types.PFunctionContext(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.version_ = version_;
        if (stagingInputValsBuilder_ == null) {
          if (((bitField0_ & 0x00000002) != 0)) {
            stagingInputVals_ = java.util.Collections.unmodifiableList(stagingInputVals_);
            bitField0_ = (bitField0_ & ~0x00000002);
          }
          result.stagingInputVals_ = stagingInputVals_;
        } else {
          result.stagingInputVals_ = stagingInputValsBuilder_.build();
        }
        if (constantArgsBuilder_ == null) {
          if (((bitField0_ & 0x00000004) != 0)) {
            constantArgs_ = java.util.Collections.unmodifiableList(constantArgs_);
            bitField0_ = (bitField0_ & ~0x00000004);
          }
          result.constantArgs_ = constantArgs_;
        } else {
          result.constantArgs_ = constantArgsBuilder_.build();
        }
        if (((from_bitField0_ & 0x00000008) != 0)) {
          to_bitField0_ |= 0x00000002;
        }
        result.errorMsg_ = errorMsg_;
        if (((from_bitField0_ & 0x00000010) != 0)) {
          if (queryIdBuilder_ == null) {
            result.queryId_ = queryId_;
          } else {
            result.queryId_ = queryIdBuilder_.build();
          }
          to_bitField0_ |= 0x00000004;
        }
        if (((from_bitField0_ & 0x00000020) != 0)) {
          to_bitField0_ |= 0x00000008;
        }
        result.threadLocalFnState_ = threadLocalFnState_;
        if (((from_bitField0_ & 0x00000040) != 0)) {
          to_bitField0_ |= 0x00000010;
        }
        result.fragmentLocalFnState_ = fragmentLocalFnState_;
        if (((from_bitField0_ & 0x00000080) != 0)) {
          to_bitField0_ |= 0x00000020;
        }
        result.stringResult_ = stringResult_;
        if (((from_bitField0_ & 0x00000100) != 0)) {
          result.numUpdates_ = numUpdates_;
          to_bitField0_ |= 0x00000040;
        }
        if (((from_bitField0_ & 0x00000200) != 0)) {
          result.numRemoves_ = numRemoves_;
          to_bitField0_ |= 0x00000080;
        }
        if (((from_bitField0_ & 0x00000400) != 0)) {
          result.numWarnings_ = numWarnings_;
          to_bitField0_ |= 0x00000100;
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
        if (other instanceof org.apache.doris.proto.Types.PFunctionContext) {
          return mergeFrom((org.apache.doris.proto.Types.PFunctionContext)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PFunctionContext other) {
        if (other == org.apache.doris.proto.Types.PFunctionContext.getDefaultInstance()) return this;
        if (other.hasVersion()) {
          bitField0_ |= 0x00000001;
          version_ = other.version_;
          onChanged();
        }
        if (stagingInputValsBuilder_ == null) {
          if (!other.stagingInputVals_.isEmpty()) {
            if (stagingInputVals_.isEmpty()) {
              stagingInputVals_ = other.stagingInputVals_;
              bitField0_ = (bitField0_ & ~0x00000002);
            } else {
              ensureStagingInputValsIsMutable();
              stagingInputVals_.addAll(other.stagingInputVals_);
            }
            onChanged();
          }
        } else {
          if (!other.stagingInputVals_.isEmpty()) {
            if (stagingInputValsBuilder_.isEmpty()) {
              stagingInputValsBuilder_.dispose();
              stagingInputValsBuilder_ = null;
              stagingInputVals_ = other.stagingInputVals_;
              bitField0_ = (bitField0_ & ~0x00000002);
              stagingInputValsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getStagingInputValsFieldBuilder() : null;
            } else {
              stagingInputValsBuilder_.addAllMessages(other.stagingInputVals_);
            }
          }
        }
        if (constantArgsBuilder_ == null) {
          if (!other.constantArgs_.isEmpty()) {
            if (constantArgs_.isEmpty()) {
              constantArgs_ = other.constantArgs_;
              bitField0_ = (bitField0_ & ~0x00000004);
            } else {
              ensureConstantArgsIsMutable();
              constantArgs_.addAll(other.constantArgs_);
            }
            onChanged();
          }
        } else {
          if (!other.constantArgs_.isEmpty()) {
            if (constantArgsBuilder_.isEmpty()) {
              constantArgsBuilder_.dispose();
              constantArgsBuilder_ = null;
              constantArgs_ = other.constantArgs_;
              bitField0_ = (bitField0_ & ~0x00000004);
              constantArgsBuilder_ = 
                com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders ?
                   getConstantArgsFieldBuilder() : null;
            } else {
              constantArgsBuilder_.addAllMessages(other.constantArgs_);
            }
          }
        }
        if (other.hasErrorMsg()) {
          bitField0_ |= 0x00000008;
          errorMsg_ = other.errorMsg_;
          onChanged();
        }
        if (other.hasQueryId()) {
          mergeQueryId(other.getQueryId());
        }
        if (other.hasThreadLocalFnState()) {
          setThreadLocalFnState(other.getThreadLocalFnState());
        }
        if (other.hasFragmentLocalFnState()) {
          setFragmentLocalFnState(other.getFragmentLocalFnState());
        }
        if (other.hasStringResult()) {
          bitField0_ |= 0x00000080;
          stringResult_ = other.stringResult_;
          onChanged();
        }
        if (other.hasNumUpdates()) {
          setNumUpdates(other.getNumUpdates());
        }
        if (other.hasNumRemoves()) {
          setNumRemoves(other.getNumRemoves());
        }
        if (other.hasNumWarnings()) {
          setNumWarnings(other.getNumWarnings());
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        for (int i = 0; i < getStagingInputValsCount(); i++) {
          if (!getStagingInputVals(i).isInitialized()) {
            return false;
          }
        }
        for (int i = 0; i < getConstantArgsCount(); i++) {
          if (!getConstantArgs(i).isInitialized()) {
            return false;
          }
        }
        if (hasQueryId()) {
          if (!getQueryId().isInitialized()) {
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
        org.apache.doris.proto.Types.PFunctionContext parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PFunctionContext) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object version_ = "V2_0";
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @return Whether the version field is set.
       */
      public boolean hasVersion() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @return The version.
       */
      public java.lang.String getVersion() {
        java.lang.Object ref = version_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            version_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @return The bytes for version.
       */
      public com.google.protobuf.ByteString
          getVersionBytes() {
        java.lang.Object ref = version_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          version_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @param value The version to set.
       * @return This builder for chaining.
       */
      public Builder setVersion(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        version_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @return This builder for chaining.
       */
      public Builder clearVersion() {
        bitField0_ = (bitField0_ & ~0x00000001);
        version_ = getDefaultInstance().getVersion();
        onChanged();
        return this;
      }
      /**
       * <code>optional string version = 1 [default = "V2_0"];</code>
       * @param value The bytes for version to set.
       * @return This builder for chaining.
       */
      public Builder setVersionBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        version_ = value;
        onChanged();
        return this;
      }

      private java.util.List<org.apache.doris.proto.Types.PValue> stagingInputVals_ =
        java.util.Collections.emptyList();
      private void ensureStagingInputValsIsMutable() {
        if (!((bitField0_ & 0x00000002) != 0)) {
          stagingInputVals_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValue>(stagingInputVals_);
          bitField0_ |= 0x00000002;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder> stagingInputValsBuilder_;

      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValue> getStagingInputValsList() {
        if (stagingInputValsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(stagingInputVals_);
        } else {
          return stagingInputValsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public int getStagingInputValsCount() {
        if (stagingInputValsBuilder_ == null) {
          return stagingInputVals_.size();
        } else {
          return stagingInputValsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public org.apache.doris.proto.Types.PValue getStagingInputVals(int index) {
        if (stagingInputValsBuilder_ == null) {
          return stagingInputVals_.get(index);
        } else {
          return stagingInputValsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder setStagingInputVals(
          int index, org.apache.doris.proto.Types.PValue value) {
        if (stagingInputValsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStagingInputValsIsMutable();
          stagingInputVals_.set(index, value);
          onChanged();
        } else {
          stagingInputValsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder setStagingInputVals(
          int index, org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (stagingInputValsBuilder_ == null) {
          ensureStagingInputValsIsMutable();
          stagingInputVals_.set(index, builderForValue.build());
          onChanged();
        } else {
          stagingInputValsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder addStagingInputVals(org.apache.doris.proto.Types.PValue value) {
        if (stagingInputValsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStagingInputValsIsMutable();
          stagingInputVals_.add(value);
          onChanged();
        } else {
          stagingInputValsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder addStagingInputVals(
          int index, org.apache.doris.proto.Types.PValue value) {
        if (stagingInputValsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureStagingInputValsIsMutable();
          stagingInputVals_.add(index, value);
          onChanged();
        } else {
          stagingInputValsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder addStagingInputVals(
          org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (stagingInputValsBuilder_ == null) {
          ensureStagingInputValsIsMutable();
          stagingInputVals_.add(builderForValue.build());
          onChanged();
        } else {
          stagingInputValsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder addStagingInputVals(
          int index, org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (stagingInputValsBuilder_ == null) {
          ensureStagingInputValsIsMutable();
          stagingInputVals_.add(index, builderForValue.build());
          onChanged();
        } else {
          stagingInputValsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder addAllStagingInputVals(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PValue> values) {
        if (stagingInputValsBuilder_ == null) {
          ensureStagingInputValsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, stagingInputVals_);
          onChanged();
        } else {
          stagingInputValsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder clearStagingInputVals() {
        if (stagingInputValsBuilder_ == null) {
          stagingInputVals_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000002);
          onChanged();
        } else {
          stagingInputValsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public Builder removeStagingInputVals(int index) {
        if (stagingInputValsBuilder_ == null) {
          ensureStagingInputValsIsMutable();
          stagingInputVals_.remove(index);
          onChanged();
        } else {
          stagingInputValsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder getStagingInputValsBuilder(
          int index) {
        return getStagingInputValsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public org.apache.doris.proto.Types.PValueOrBuilder getStagingInputValsOrBuilder(
          int index) {
        if (stagingInputValsBuilder_ == null) {
          return stagingInputVals_.get(index);  } else {
          return stagingInputValsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
           getStagingInputValsOrBuilderList() {
        if (stagingInputValsBuilder_ != null) {
          return stagingInputValsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(stagingInputVals_);
        }
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder addStagingInputValsBuilder() {
        return getStagingInputValsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PValue.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder addStagingInputValsBuilder(
          int index) {
        return getStagingInputValsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PValue.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValue staging_input_vals = 2;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValue.Builder> 
           getStagingInputValsBuilderList() {
        return getStagingInputValsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder> 
          getStagingInputValsFieldBuilder() {
        if (stagingInputValsBuilder_ == null) {
          stagingInputValsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder>(
                  stagingInputVals_,
                  ((bitField0_ & 0x00000002) != 0),
                  getParentForChildren(),
                  isClean());
          stagingInputVals_ = null;
        }
        return stagingInputValsBuilder_;
      }

      private java.util.List<org.apache.doris.proto.Types.PValue> constantArgs_ =
        java.util.Collections.emptyList();
      private void ensureConstantArgsIsMutable() {
        if (!((bitField0_ & 0x00000004) != 0)) {
          constantArgs_ = new java.util.ArrayList<org.apache.doris.proto.Types.PValue>(constantArgs_);
          bitField0_ |= 0x00000004;
         }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder> constantArgsBuilder_;

      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValue> getConstantArgsList() {
        if (constantArgsBuilder_ == null) {
          return java.util.Collections.unmodifiableList(constantArgs_);
        } else {
          return constantArgsBuilder_.getMessageList();
        }
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public int getConstantArgsCount() {
        if (constantArgsBuilder_ == null) {
          return constantArgs_.size();
        } else {
          return constantArgsBuilder_.getCount();
        }
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public org.apache.doris.proto.Types.PValue getConstantArgs(int index) {
        if (constantArgsBuilder_ == null) {
          return constantArgs_.get(index);
        } else {
          return constantArgsBuilder_.getMessage(index);
        }
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder setConstantArgs(
          int index, org.apache.doris.proto.Types.PValue value) {
        if (constantArgsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureConstantArgsIsMutable();
          constantArgs_.set(index, value);
          onChanged();
        } else {
          constantArgsBuilder_.setMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder setConstantArgs(
          int index, org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (constantArgsBuilder_ == null) {
          ensureConstantArgsIsMutable();
          constantArgs_.set(index, builderForValue.build());
          onChanged();
        } else {
          constantArgsBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder addConstantArgs(org.apache.doris.proto.Types.PValue value) {
        if (constantArgsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureConstantArgsIsMutable();
          constantArgs_.add(value);
          onChanged();
        } else {
          constantArgsBuilder_.addMessage(value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder addConstantArgs(
          int index, org.apache.doris.proto.Types.PValue value) {
        if (constantArgsBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureConstantArgsIsMutable();
          constantArgs_.add(index, value);
          onChanged();
        } else {
          constantArgsBuilder_.addMessage(index, value);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder addConstantArgs(
          org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (constantArgsBuilder_ == null) {
          ensureConstantArgsIsMutable();
          constantArgs_.add(builderForValue.build());
          onChanged();
        } else {
          constantArgsBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder addConstantArgs(
          int index, org.apache.doris.proto.Types.PValue.Builder builderForValue) {
        if (constantArgsBuilder_ == null) {
          ensureConstantArgsIsMutable();
          constantArgs_.add(index, builderForValue.build());
          onChanged();
        } else {
          constantArgsBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder addAllConstantArgs(
          java.lang.Iterable<? extends org.apache.doris.proto.Types.PValue> values) {
        if (constantArgsBuilder_ == null) {
          ensureConstantArgsIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(
              values, constantArgs_);
          onChanged();
        } else {
          constantArgsBuilder_.addAllMessages(values);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder clearConstantArgs() {
        if (constantArgsBuilder_ == null) {
          constantArgs_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000004);
          onChanged();
        } else {
          constantArgsBuilder_.clear();
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public Builder removeConstantArgs(int index) {
        if (constantArgsBuilder_ == null) {
          ensureConstantArgsIsMutable();
          constantArgs_.remove(index);
          onChanged();
        } else {
          constantArgsBuilder_.remove(index);
        }
        return this;
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder getConstantArgsBuilder(
          int index) {
        return getConstantArgsFieldBuilder().getBuilder(index);
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public org.apache.doris.proto.Types.PValueOrBuilder getConstantArgsOrBuilder(
          int index) {
        if (constantArgsBuilder_ == null) {
          return constantArgs_.get(index);  } else {
          return constantArgsBuilder_.getMessageOrBuilder(index);
        }
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public java.util.List<? extends org.apache.doris.proto.Types.PValueOrBuilder> 
           getConstantArgsOrBuilderList() {
        if (constantArgsBuilder_ != null) {
          return constantArgsBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(constantArgs_);
        }
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder addConstantArgsBuilder() {
        return getConstantArgsFieldBuilder().addBuilder(
            org.apache.doris.proto.Types.PValue.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public org.apache.doris.proto.Types.PValue.Builder addConstantArgsBuilder(
          int index) {
        return getConstantArgsFieldBuilder().addBuilder(
            index, org.apache.doris.proto.Types.PValue.getDefaultInstance());
      }
      /**
       * <code>repeated .doris.PValue constant_args = 3;</code>
       */
      public java.util.List<org.apache.doris.proto.Types.PValue.Builder> 
           getConstantArgsBuilderList() {
        return getConstantArgsFieldBuilder().getBuilderList();
      }
      private com.google.protobuf.RepeatedFieldBuilderV3<
          org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder> 
          getConstantArgsFieldBuilder() {
        if (constantArgsBuilder_ == null) {
          constantArgsBuilder_ = new com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.doris.proto.Types.PValue, org.apache.doris.proto.Types.PValue.Builder, org.apache.doris.proto.Types.PValueOrBuilder>(
                  constantArgs_,
                  ((bitField0_ & 0x00000004) != 0),
                  getParentForChildren(),
                  isClean());
          constantArgs_ = null;
        }
        return constantArgsBuilder_;
      }

      private java.lang.Object errorMsg_ = "";
      /**
       * <code>optional string error_msg = 4;</code>
       * @return Whether the errorMsg field is set.
       */
      public boolean hasErrorMsg() {
        return ((bitField0_ & 0x00000008) != 0);
      }
      /**
       * <code>optional string error_msg = 4;</code>
       * @return The errorMsg.
       */
      public java.lang.String getErrorMsg() {
        java.lang.Object ref = errorMsg_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            errorMsg_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string error_msg = 4;</code>
       * @return The bytes for errorMsg.
       */
      public com.google.protobuf.ByteString
          getErrorMsgBytes() {
        java.lang.Object ref = errorMsg_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          errorMsg_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string error_msg = 4;</code>
       * @param value The errorMsg to set.
       * @return This builder for chaining.
       */
      public Builder setErrorMsg(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        errorMsg_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string error_msg = 4;</code>
       * @return This builder for chaining.
       */
      public Builder clearErrorMsg() {
        bitField0_ = (bitField0_ & ~0x00000008);
        errorMsg_ = getDefaultInstance().getErrorMsg();
        onChanged();
        return this;
      }
      /**
       * <code>optional string error_msg = 4;</code>
       * @param value The bytes for errorMsg to set.
       * @return This builder for chaining.
       */
      public Builder setErrorMsgBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000008;
        errorMsg_ = value;
        onChanged();
        return this;
      }

      private org.apache.doris.proto.Types.PUniqueId queryId_;
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PUniqueId, org.apache.doris.proto.Types.PUniqueId.Builder, org.apache.doris.proto.Types.PUniqueIdOrBuilder> queryIdBuilder_;
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       * @return Whether the queryId field is set.
       */
      public boolean hasQueryId() {
        return ((bitField0_ & 0x00000010) != 0);
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       * @return The queryId.
       */
      public org.apache.doris.proto.Types.PUniqueId getQueryId() {
        if (queryIdBuilder_ == null) {
          return queryId_ == null ? org.apache.doris.proto.Types.PUniqueId.getDefaultInstance() : queryId_;
        } else {
          return queryIdBuilder_.getMessage();
        }
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public Builder setQueryId(org.apache.doris.proto.Types.PUniqueId value) {
        if (queryIdBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          queryId_ = value;
          onChanged();
        } else {
          queryIdBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public Builder setQueryId(
          org.apache.doris.proto.Types.PUniqueId.Builder builderForValue) {
        if (queryIdBuilder_ == null) {
          queryId_ = builderForValue.build();
          onChanged();
        } else {
          queryIdBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public Builder mergeQueryId(org.apache.doris.proto.Types.PUniqueId value) {
        if (queryIdBuilder_ == null) {
          if (((bitField0_ & 0x00000010) != 0) &&
              queryId_ != null &&
              queryId_ != org.apache.doris.proto.Types.PUniqueId.getDefaultInstance()) {
            queryId_ =
              org.apache.doris.proto.Types.PUniqueId.newBuilder(queryId_).mergeFrom(value).buildPartial();
          } else {
            queryId_ = value;
          }
          onChanged();
        } else {
          queryIdBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000010;
        return this;
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public Builder clearQueryId() {
        if (queryIdBuilder_ == null) {
          queryId_ = null;
          onChanged();
        } else {
          queryIdBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000010);
        return this;
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public org.apache.doris.proto.Types.PUniqueId.Builder getQueryIdBuilder() {
        bitField0_ |= 0x00000010;
        onChanged();
        return getQueryIdFieldBuilder().getBuilder();
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      public org.apache.doris.proto.Types.PUniqueIdOrBuilder getQueryIdOrBuilder() {
        if (queryIdBuilder_ != null) {
          return queryIdBuilder_.getMessageOrBuilder();
        } else {
          return queryId_ == null ?
              org.apache.doris.proto.Types.PUniqueId.getDefaultInstance() : queryId_;
        }
      }
      /**
       * <code>optional .doris.PUniqueId query_id = 5;</code>
       */
      private com.google.protobuf.SingleFieldBuilderV3<
          org.apache.doris.proto.Types.PUniqueId, org.apache.doris.proto.Types.PUniqueId.Builder, org.apache.doris.proto.Types.PUniqueIdOrBuilder> 
          getQueryIdFieldBuilder() {
        if (queryIdBuilder_ == null) {
          queryIdBuilder_ = new com.google.protobuf.SingleFieldBuilderV3<
              org.apache.doris.proto.Types.PUniqueId, org.apache.doris.proto.Types.PUniqueId.Builder, org.apache.doris.proto.Types.PUniqueIdOrBuilder>(
                  getQueryId(),
                  getParentForChildren(),
                  isClean());
          queryId_ = null;
        }
        return queryIdBuilder_;
      }

      private com.google.protobuf.ByteString threadLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes thread_local_fn_state = 6;</code>
       * @return Whether the threadLocalFnState field is set.
       */
      public boolean hasThreadLocalFnState() {
        return ((bitField0_ & 0x00000020) != 0);
      }
      /**
       * <code>optional bytes thread_local_fn_state = 6;</code>
       * @return The threadLocalFnState.
       */
      public com.google.protobuf.ByteString getThreadLocalFnState() {
        return threadLocalFnState_;
      }
      /**
       * <code>optional bytes thread_local_fn_state = 6;</code>
       * @param value The threadLocalFnState to set.
       * @return This builder for chaining.
       */
      public Builder setThreadLocalFnState(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000020;
        threadLocalFnState_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes thread_local_fn_state = 6;</code>
       * @return This builder for chaining.
       */
      public Builder clearThreadLocalFnState() {
        bitField0_ = (bitField0_ & ~0x00000020);
        threadLocalFnState_ = getDefaultInstance().getThreadLocalFnState();
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString fragmentLocalFnState_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>optional bytes fragment_local_fn_state = 7;</code>
       * @return Whether the fragmentLocalFnState field is set.
       */
      public boolean hasFragmentLocalFnState() {
        return ((bitField0_ & 0x00000040) != 0);
      }
      /**
       * <code>optional bytes fragment_local_fn_state = 7;</code>
       * @return The fragmentLocalFnState.
       */
      public com.google.protobuf.ByteString getFragmentLocalFnState() {
        return fragmentLocalFnState_;
      }
      /**
       * <code>optional bytes fragment_local_fn_state = 7;</code>
       * @param value The fragmentLocalFnState to set.
       * @return This builder for chaining.
       */
      public Builder setFragmentLocalFnState(com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000040;
        fragmentLocalFnState_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional bytes fragment_local_fn_state = 7;</code>
       * @return This builder for chaining.
       */
      public Builder clearFragmentLocalFnState() {
        bitField0_ = (bitField0_ & ~0x00000040);
        fragmentLocalFnState_ = getDefaultInstance().getFragmentLocalFnState();
        onChanged();
        return this;
      }

      private java.lang.Object stringResult_ = "";
      /**
       * <code>optional string string_result = 8;</code>
       * @return Whether the stringResult field is set.
       */
      public boolean hasStringResult() {
        return ((bitField0_ & 0x00000080) != 0);
      }
      /**
       * <code>optional string string_result = 8;</code>
       * @return The stringResult.
       */
      public java.lang.String getStringResult() {
        java.lang.Object ref = stringResult_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            stringResult_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string string_result = 8;</code>
       * @return The bytes for stringResult.
       */
      public com.google.protobuf.ByteString
          getStringResultBytes() {
        java.lang.Object ref = stringResult_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          stringResult_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string string_result = 8;</code>
       * @param value The stringResult to set.
       * @return This builder for chaining.
       */
      public Builder setStringResult(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000080;
        stringResult_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string string_result = 8;</code>
       * @return This builder for chaining.
       */
      public Builder clearStringResult() {
        bitField0_ = (bitField0_ & ~0x00000080);
        stringResult_ = getDefaultInstance().getStringResult();
        onChanged();
        return this;
      }
      /**
       * <code>optional string string_result = 8;</code>
       * @param value The bytes for stringResult to set.
       * @return This builder for chaining.
       */
      public Builder setStringResultBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000080;
        stringResult_ = value;
        onChanged();
        return this;
      }

      private long numUpdates_ ;
      /**
       * <code>optional int64 num_updates = 9;</code>
       * @return Whether the numUpdates field is set.
       */
      public boolean hasNumUpdates() {
        return ((bitField0_ & 0x00000100) != 0);
      }
      /**
       * <code>optional int64 num_updates = 9;</code>
       * @return The numUpdates.
       */
      public long getNumUpdates() {
        return numUpdates_;
      }
      /**
       * <code>optional int64 num_updates = 9;</code>
       * @param value The numUpdates to set.
       * @return This builder for chaining.
       */
      public Builder setNumUpdates(long value) {
        bitField0_ |= 0x00000100;
        numUpdates_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 num_updates = 9;</code>
       * @return This builder for chaining.
       */
      public Builder clearNumUpdates() {
        bitField0_ = (bitField0_ & ~0x00000100);
        numUpdates_ = 0L;
        onChanged();
        return this;
      }

      private long numRemoves_ ;
      /**
       * <code>optional int64 num_removes = 10;</code>
       * @return Whether the numRemoves field is set.
       */
      public boolean hasNumRemoves() {
        return ((bitField0_ & 0x00000200) != 0);
      }
      /**
       * <code>optional int64 num_removes = 10;</code>
       * @return The numRemoves.
       */
      public long getNumRemoves() {
        return numRemoves_;
      }
      /**
       * <code>optional int64 num_removes = 10;</code>
       * @param value The numRemoves to set.
       * @return This builder for chaining.
       */
      public Builder setNumRemoves(long value) {
        bitField0_ |= 0x00000200;
        numRemoves_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 num_removes = 10;</code>
       * @return This builder for chaining.
       */
      public Builder clearNumRemoves() {
        bitField0_ = (bitField0_ & ~0x00000200);
        numRemoves_ = 0L;
        onChanged();
        return this;
      }

      private long numWarnings_ ;
      /**
       * <code>optional int64 num_warnings = 11;</code>
       * @return Whether the numWarnings field is set.
       */
      public boolean hasNumWarnings() {
        return ((bitField0_ & 0x00000400) != 0);
      }
      /**
       * <code>optional int64 num_warnings = 11;</code>
       * @return The numWarnings.
       */
      public long getNumWarnings() {
        return numWarnings_;
      }
      /**
       * <code>optional int64 num_warnings = 11;</code>
       * @param value The numWarnings to set.
       * @return This builder for chaining.
       */
      public Builder setNumWarnings(long value) {
        bitField0_ |= 0x00000400;
        numWarnings_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional int64 num_warnings = 11;</code>
       * @return This builder for chaining.
       */
      public Builder clearNumWarnings() {
        bitField0_ = (bitField0_ & ~0x00000400);
        numWarnings_ = 0L;
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


      // @@protoc_insertion_point(builder_scope:doris.PFunctionContext)
    }

    // @@protoc_insertion_point(class_scope:doris.PFunctionContext)
    private static final org.apache.doris.proto.Types.PFunctionContext DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PFunctionContext();
    }

    public static org.apache.doris.proto.Types.PFunctionContext getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PFunctionContext>
        PARSER = new com.google.protobuf.AbstractParser<PFunctionContext>() {
      @java.lang.Override
      public PFunctionContext parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PFunctionContext(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PFunctionContext> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PFunctionContext> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PFunctionContext getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PHandShakeRequestOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PHandShakeRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>optional string hello = 1;</code>
     * @return Whether the hello field is set.
     */
    boolean hasHello();
    /**
     * <code>optional string hello = 1;</code>
     * @return The hello.
     */
    java.lang.String getHello();
    /**
     * <code>optional string hello = 1;</code>
     * @return The bytes for hello.
     */
    com.google.protobuf.ByteString
        getHelloBytes();
  }
  /**
   * Protobuf type {@code doris.PHandShakeRequest}
   */
  public  static final class PHandShakeRequest extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PHandShakeRequest)
      PHandShakeRequestOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PHandShakeRequest.newBuilder() to construct.
    private PHandShakeRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PHandShakeRequest() {
      hello_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PHandShakeRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PHandShakeRequest(
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
              hello_ = bs;
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
      return org.apache.doris.proto.Types.internal_static_doris_PHandShakeRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PHandShakeRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PHandShakeRequest.class, org.apache.doris.proto.Types.PHandShakeRequest.Builder.class);
    }

    private int bitField0_;
    public static final int HELLO_FIELD_NUMBER = 1;
    private volatile java.lang.Object hello_;
    /**
     * <code>optional string hello = 1;</code>
     * @return Whether the hello field is set.
     */
    public boolean hasHello() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional string hello = 1;</code>
     * @return The hello.
     */
    public java.lang.String getHello() {
      java.lang.Object ref = hello_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          hello_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string hello = 1;</code>
     * @return The bytes for hello.
     */
    public com.google.protobuf.ByteString
        getHelloBytes() {
      java.lang.Object ref = hello_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        hello_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, hello_);
      }
      unknownFields.writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, hello_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PHandShakeRequest)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PHandShakeRequest other = (org.apache.doris.proto.Types.PHandShakeRequest) obj;

      if (hasHello() != other.hasHello()) return false;
      if (hasHello()) {
        if (!getHello()
            .equals(other.getHello())) return false;
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
      if (hasHello()) {
        hash = (37 * hash) + HELLO_FIELD_NUMBER;
        hash = (53 * hash) + getHello().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeRequest parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PHandShakeRequest prototype) {
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
     * Protobuf type {@code doris.PHandShakeRequest}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PHandShakeRequest)
        org.apache.doris.proto.Types.PHandShakeRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PHandShakeRequest.class, org.apache.doris.proto.Types.PHandShakeRequest.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PHandShakeRequest.newBuilder()
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
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        hello_ = "";
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeRequest getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PHandShakeRequest.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeRequest build() {
        org.apache.doris.proto.Types.PHandShakeRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeRequest buildPartial() {
        org.apache.doris.proto.Types.PHandShakeRequest result = new org.apache.doris.proto.Types.PHandShakeRequest(this);
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          to_bitField0_ |= 0x00000001;
        }
        result.hello_ = hello_;
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
        if (other instanceof org.apache.doris.proto.Types.PHandShakeRequest) {
          return mergeFrom((org.apache.doris.proto.Types.PHandShakeRequest)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PHandShakeRequest other) {
        if (other == org.apache.doris.proto.Types.PHandShakeRequest.getDefaultInstance()) return this;
        if (other.hasHello()) {
          bitField0_ |= 0x00000001;
          hello_ = other.hello_;
          onChanged();
        }
        this.mergeUnknownFields(other.unknownFields);
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        org.apache.doris.proto.Types.PHandShakeRequest parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PHandShakeRequest) e.getUnfinishedMessage();
          throw e.unwrapIOException();
        } finally {
          if (parsedMessage != null) {
            mergeFrom(parsedMessage);
          }
        }
        return this;
      }
      private int bitField0_;

      private java.lang.Object hello_ = "";
      /**
       * <code>optional string hello = 1;</code>
       * @return Whether the hello field is set.
       */
      public boolean hasHello() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional string hello = 1;</code>
       * @return The hello.
       */
      public java.lang.String getHello() {
        java.lang.Object ref = hello_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            hello_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string hello = 1;</code>
       * @return The bytes for hello.
       */
      public com.google.protobuf.ByteString
          getHelloBytes() {
        java.lang.Object ref = hello_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          hello_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string hello = 1;</code>
       * @param value The hello to set.
       * @return This builder for chaining.
       */
      public Builder setHello(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        hello_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string hello = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearHello() {
        bitField0_ = (bitField0_ & ~0x00000001);
        hello_ = getDefaultInstance().getHello();
        onChanged();
        return this;
      }
      /**
       * <code>optional string hello = 1;</code>
       * @param value The bytes for hello to set.
       * @return This builder for chaining.
       */
      public Builder setHelloBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000001;
        hello_ = value;
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


      // @@protoc_insertion_point(builder_scope:doris.PHandShakeRequest)
    }

    // @@protoc_insertion_point(class_scope:doris.PHandShakeRequest)
    private static final org.apache.doris.proto.Types.PHandShakeRequest DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PHandShakeRequest();
    }

    public static org.apache.doris.proto.Types.PHandShakeRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PHandShakeRequest>
        PARSER = new com.google.protobuf.AbstractParser<PHandShakeRequest>() {
      @java.lang.Override
      public PHandShakeRequest parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PHandShakeRequest(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PHandShakeRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PHandShakeRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PHandShakeRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  public interface PHandShakeResponseOrBuilder extends
      // @@protoc_insertion_point(interface_extends:doris.PHandShakeResponse)
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

    /**
     * <code>optional string hello = 2;</code>
     * @return Whether the hello field is set.
     */
    boolean hasHello();
    /**
     * <code>optional string hello = 2;</code>
     * @return The hello.
     */
    java.lang.String getHello();
    /**
     * <code>optional string hello = 2;</code>
     * @return The bytes for hello.
     */
    com.google.protobuf.ByteString
        getHelloBytes();
  }
  /**
   * Protobuf type {@code doris.PHandShakeResponse}
   */
  public  static final class PHandShakeResponse extends
      com.google.protobuf.GeneratedMessageV3 implements
      // @@protoc_insertion_point(message_implements:doris.PHandShakeResponse)
      PHandShakeResponseOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use PHandShakeResponse.newBuilder() to construct.
    private PHandShakeResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }
    private PHandShakeResponse() {
      hello_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new PHandShakeResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet
    getUnknownFields() {
      return this.unknownFields;
    }
    private PHandShakeResponse(
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
            case 18: {
              com.google.protobuf.ByteString bs = input.readBytes();
              bitField0_ |= 0x00000002;
              hello_ = bs;
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
      return org.apache.doris.proto.Types.internal_static_doris_PHandShakeResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.doris.proto.Types.internal_static_doris_PHandShakeResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.doris.proto.Types.PHandShakeResponse.class, org.apache.doris.proto.Types.PHandShakeResponse.Builder.class);
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

    public static final int HELLO_FIELD_NUMBER = 2;
    private volatile java.lang.Object hello_;
    /**
     * <code>optional string hello = 2;</code>
     * @return Whether the hello field is set.
     */
    public boolean hasHello() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string hello = 2;</code>
     * @return The hello.
     */
    public java.lang.String getHello() {
      java.lang.Object ref = hello_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          hello_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string hello = 2;</code>
     * @return The bytes for hello.
     */
    public com.google.protobuf.ByteString
        getHelloBytes() {
      java.lang.Object ref = hello_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        hello_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
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
      if (((bitField0_ & 0x00000002) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, hello_);
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
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, hello_);
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
      if (!(obj instanceof org.apache.doris.proto.Types.PHandShakeResponse)) {
        return super.equals(obj);
      }
      org.apache.doris.proto.Types.PHandShakeResponse other = (org.apache.doris.proto.Types.PHandShakeResponse) obj;

      if (hasStatus() != other.hasStatus()) return false;
      if (hasStatus()) {
        if (!getStatus()
            .equals(other.getStatus())) return false;
      }
      if (hasHello() != other.hasHello()) return false;
      if (hasHello()) {
        if (!getHello()
            .equals(other.getHello())) return false;
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
      if (hasHello()) {
        hash = (37 * hash) + HELLO_FIELD_NUMBER;
        hash = (53 * hash) + getHello().hashCode();
      }
      hash = (29 * hash) + unknownFields.hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static org.apache.doris.proto.Types.PHandShakeResponse parseFrom(
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
    public static Builder newBuilder(org.apache.doris.proto.Types.PHandShakeResponse prototype) {
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
     * Protobuf type {@code doris.PHandShakeResponse}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:doris.PHandShakeResponse)
        org.apache.doris.proto.Types.PHandShakeResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.doris.proto.Types.PHandShakeResponse.class, org.apache.doris.proto.Types.PHandShakeResponse.Builder.class);
      }

      // Construct using org.apache.doris.proto.Types.PHandShakeResponse.newBuilder()
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
        hello_ = "";
        bitField0_ = (bitField0_ & ~0x00000002);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return org.apache.doris.proto.Types.internal_static_doris_PHandShakeResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeResponse getDefaultInstanceForType() {
        return org.apache.doris.proto.Types.PHandShakeResponse.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeResponse build() {
        org.apache.doris.proto.Types.PHandShakeResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.doris.proto.Types.PHandShakeResponse buildPartial() {
        org.apache.doris.proto.Types.PHandShakeResponse result = new org.apache.doris.proto.Types.PHandShakeResponse(this);
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
        if (((from_bitField0_ & 0x00000002) != 0)) {
          to_bitField0_ |= 0x00000002;
        }
        result.hello_ = hello_;
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
        if (other instanceof org.apache.doris.proto.Types.PHandShakeResponse) {
          return mergeFrom((org.apache.doris.proto.Types.PHandShakeResponse)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.doris.proto.Types.PHandShakeResponse other) {
        if (other == org.apache.doris.proto.Types.PHandShakeResponse.getDefaultInstance()) return this;
        if (other.hasStatus()) {
          mergeStatus(other.getStatus());
        }
        if (other.hasHello()) {
          bitField0_ |= 0x00000002;
          hello_ = other.hello_;
          onChanged();
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
        org.apache.doris.proto.Types.PHandShakeResponse parsedMessage = null;
        try {
          parsedMessage = PARSER.parsePartialFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          parsedMessage = (org.apache.doris.proto.Types.PHandShakeResponse) e.getUnfinishedMessage();
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

      private java.lang.Object hello_ = "";
      /**
       * <code>optional string hello = 2;</code>
       * @return Whether the hello field is set.
       */
      public boolean hasHello() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional string hello = 2;</code>
       * @return The hello.
       */
      public java.lang.String getHello() {
        java.lang.Object ref = hello_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            hello_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string hello = 2;</code>
       * @return The bytes for hello.
       */
      public com.google.protobuf.ByteString
          getHelloBytes() {
        java.lang.Object ref = hello_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          hello_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string hello = 2;</code>
       * @param value The hello to set.
       * @return This builder for chaining.
       */
      public Builder setHello(
          java.lang.String value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        hello_ = value;
        onChanged();
        return this;
      }
      /**
       * <code>optional string hello = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearHello() {
        bitField0_ = (bitField0_ & ~0x00000002);
        hello_ = getDefaultInstance().getHello();
        onChanged();
        return this;
      }
      /**
       * <code>optional string hello = 2;</code>
       * @param value The bytes for hello to set.
       * @return This builder for chaining.
       */
      public Builder setHelloBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) {
    throw new NullPointerException();
  }
  bitField0_ |= 0x00000002;
        hello_ = value;
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


      // @@protoc_insertion_point(builder_scope:doris.PHandShakeResponse)
    }

    // @@protoc_insertion_point(class_scope:doris.PHandShakeResponse)
    private static final org.apache.doris.proto.Types.PHandShakeResponse DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new org.apache.doris.proto.Types.PHandShakeResponse();
    }

    public static org.apache.doris.proto.Types.PHandShakeResponse getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<PHandShakeResponse>
        PARSER = new com.google.protobuf.AbstractParser<PHandShakeResponse>() {
      @java.lang.Override
      public PHandShakeResponse parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        return new PHandShakeResponse(input, extensionRegistry);
      }
    };

    public static com.google.protobuf.Parser<PHandShakeResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<PHandShakeResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.doris.proto.Types.PHandShakeResponse getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PStatus_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PStatus_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PScalarType_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PScalarType_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PStructField_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PStructField_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PTypeNode_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PTypeNode_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PTypeDesc_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PTypeDesc_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PUniqueId_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PUniqueId_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PGenericType_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PGenericType_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PList_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PList_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PMap_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PMap_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PField_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PField_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PStruct_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PStruct_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PDecimal_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PDecimal_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PDateTime_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PDateTime_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PValue_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PValue_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PValues_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PValues_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PFunction_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PFunction_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PFunction_Property_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PFunction_Property_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PFunctionContext_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PFunctionContext_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PHandShakeRequest_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PHandShakeRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_doris_PHandShakeResponse_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_doris_PHandShakeResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\013types.proto\022\005doris\"2\n\007PStatus\022\023\n\013statu" +
      "s_code\030\001 \002(\005\022\022\n\nerror_msgs\030\002 \003(\t\"J\n\013PSca" +
      "larType\022\014\n\004type\030\001 \002(\005\022\013\n\003len\030\002 \001(\005\022\021\n\tpr" +
      "ecision\030\003 \001(\005\022\r\n\005scale\030\004 \001(\005\"-\n\014PStructF" +
      "ield\022\014\n\004name\030\001 \002(\t\022\017\n\007comment\030\002 \001(\t\"n\n\tP" +
      "TypeNode\022\014\n\004type\030\001 \002(\005\022\'\n\013scalar_type\030\002 " +
      "\001(\0132\022.doris.PScalarType\022*\n\rstruct_fields" +
      "\030\003 \003(\0132\023.doris.PStructField\",\n\tPTypeDesc" +
      "\022\037\n\005types\030\001 \003(\0132\020.doris.PTypeNode\"#\n\tPUn" +
      "iqueId\022\n\n\002hi\030\001 \002(\003\022\n\n\002lo\030\002 \002(\003\"\251\004\n\014PGene" +
      "ricType\022&\n\002id\030\002 \002(\0162\032.doris.PGenericType" +
      ".TypeId\022\037\n\tlist_type\030\013 \001(\0132\014.doris.PList" +
      "\022\035\n\010map_type\030\014 \001(\0132\013.doris.PMap\022#\n\013struc" +
      "t_type\030\r \001(\0132\016.doris.PStruct\022%\n\014decimal_" +
      "type\030\016 \001(\0132\017.doris.PDecimal\"\344\002\n\006TypeId\022\t" +
      "\n\005UINT8\020\000\022\n\n\006UINT16\020\001\022\n\n\006UINT32\020\002\022\n\n\006UIN" +
      "T64\020\003\022\013\n\007UINT128\020\004\022\013\n\007UINT256\020\005\022\010\n\004INT8\020" +
      "\006\022\t\n\005INT16\020\007\022\t\n\005INT32\020\010\022\t\n\005INT64\020\t\022\n\n\006IN" +
      "T128\020\n\022\n\n\006INT256\020\013\022\t\n\005FLOAT\020\014\022\n\n\006DOUBLE\020" +
      "\r\022\013\n\007BOOLEAN\020\016\022\010\n\004DATE\020\017\022\014\n\010DATETIME\020\020\022\007" +
      "\n\003HLL\020\021\022\n\n\006BITMAP\020\022\022\010\n\004LIST\020\023\022\007\n\003MAP\020\024\022\n" +
      "\n\006STRUCT\020\025\022\n\n\006STRING\020\026\022\r\n\tDECIMAL32\020\027\022\r\n" +
      "\tDECIMAL64\020\030\022\016\n\nDECIMAL128\020\031\022\t\n\005BYTES\020\032\022" +
      "\013\n\007NOTHING\020\033\022\014\n\007UNKNOWN\020\347\007\"2\n\005PList\022)\n\014e" +
      "lement_type\030\001 \002(\0132\023.doris.PGenericType\"V" +
      "\n\004PMap\022%\n\010key_type\030\001 \002(\0132\023.doris.PGeneri" +
      "cType\022\'\n\nvalue_type\030\002 \002(\0132\023.doris.PGener" +
      "icType\"J\n\006PField\022!\n\004type\030\001 \002(\0132\023.doris.P" +
      "GenericType\022\014\n\004name\030\002 \001(\t\022\017\n\007comment\030\003 \001" +
      "(\t\"6\n\007PStruct\022\035\n\006fields\030\001 \003(\0132\r.doris.PF" +
      "ield\022\014\n\004name\030\002 \002(\t\",\n\010PDecimal\022\021\n\tprecis" +
      "ion\030\001 \002(\r\022\r\n\005scale\030\002 \002(\r\"x\n\tPDateTime\022\014\n" +
      "\004year\030\001 \001(\005\022\r\n\005month\030\002 \001(\005\022\013\n\003day\030\003 \001(\005\022" +
      "\014\n\004hour\030\004 \001(\005\022\016\n\006minute\030\005 \001(\005\022\016\n\006second\030" +
      "\006 \001(\005\022\023\n\013microsecond\030\007 \001(\005\"\255\002\n\006PValue\022!\n" +
      "\004type\030\001 \002(\0132\023.doris.PGenericType\022\026\n\007is_n" +
      "ull\030\002 \001(\010:\005false\022\024\n\014double_value\030\003 \001(\001\022\023" +
      "\n\013float_value\030\004 \001(\002\022\023\n\013int32_value\030\005 \001(\005" +
      "\022\023\n\013int64_value\030\006 \001(\003\022\024\n\014uint32_value\030\007 " +
      "\001(\r\022\024\n\014uint64_value\030\010 \001(\004\022\022\n\nbool_value\030" +
      "\t \001(\010\022\024\n\014string_value\030\n \001(\t\022\023\n\013bytes_val" +
      "ue\030\013 \001(\014\022(\n\016datetime_value\030\014 \001(\0132\020.doris" +
      ".PDateTime\"\301\002\n\007PValues\022!\n\004type\030\001 \002(\0132\023.d" +
      "oris.PGenericType\022\027\n\010has_null\030\002 \001(\010:\005fal" +
      "se\022\020\n\010null_map\030\003 \003(\010\022\024\n\014double_value\030\004 \003" +
      "(\001\022\023\n\013float_value\030\005 \003(\002\022\023\n\013int32_value\030\006" +
      " \003(\005\022\023\n\013int64_value\030\007 \003(\003\022\024\n\014uint32_valu" +
      "e\030\010 \003(\r\022\024\n\014uint64_value\030\t \003(\004\022\022\n\nbool_va" +
      "lue\030\n \003(\010\022\024\n\014string_value\030\013 \003(\t\022\023\n\013bytes" +
      "_value\030\014 \003(\014\022(\n\016datetime_value\030\r \003(\0132\020.d" +
      "oris.PDateTime\"\262\002\n\tPFunction\022\025\n\rfunction" +
      "_name\030\001 \002(\t\022#\n\006inputs\030\002 \003(\0132\023.doris.PGen" +
      "ericType\022#\n\006output\030\003 \001(\0132\023.doris.PGeneri" +
      "cType\0220\n\004type\030\004 \001(\0162\035.doris.PFunction.Fu" +
      "nctionType:\003UDF\022\020\n\010variadic\030\005 \001(\010\022-\n\npro" +
      "perties\030\006 \003(\0132\031.doris.PFunction.Property" +
      "\032$\n\010Property\022\013\n\003key\030\001 \002(\t\022\013\n\003val\030\002 \002(\t\"+" +
      "\n\014FunctionType\022\007\n\003UDF\020\000\022\010\n\004UDAF\020\001\022\010\n\004UDT" +
      "F\020\002\"\310\002\n\020PFunctionContext\022\025\n\007version\030\001 \001(" +
      "\t:\004V2_0\022)\n\022staging_input_vals\030\002 \003(\0132\r.do" +
      "ris.PValue\022$\n\rconstant_args\030\003 \003(\0132\r.dori" +
      "s.PValue\022\021\n\terror_msg\030\004 \001(\t\022\"\n\010query_id\030" +
      "\005 \001(\0132\020.doris.PUniqueId\022\035\n\025thread_local_" +
      "fn_state\030\006 \001(\014\022\037\n\027fragment_local_fn_stat" +
      "e\030\007 \001(\014\022\025\n\rstring_result\030\010 \001(\t\022\023\n\013num_up" +
      "dates\030\t \001(\003\022\023\n\013num_removes\030\n \001(\003\022\024\n\014num_" +
      "warnings\030\013 \001(\003\"\"\n\021PHandShakeRequest\022\r\n\005h" +
      "ello\030\001 \001(\t\"C\n\022PHandShakeResponse\022\036\n\006stat" +
      "us\030\001 \001(\0132\016.doris.PStatus\022\r\n\005hello\030\002 \001(\tB" +
      "\030\n\026org.apache.doris.proto"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_doris_PStatus_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_doris_PStatus_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PStatus_descriptor,
        new java.lang.String[] { "StatusCode", "ErrorMsgs", });
    internal_static_doris_PScalarType_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_doris_PScalarType_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PScalarType_descriptor,
        new java.lang.String[] { "Type", "Len", "Precision", "Scale", });
    internal_static_doris_PStructField_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_doris_PStructField_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PStructField_descriptor,
        new java.lang.String[] { "Name", "Comment", });
    internal_static_doris_PTypeNode_descriptor =
      getDescriptor().getMessageTypes().get(3);
    internal_static_doris_PTypeNode_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PTypeNode_descriptor,
        new java.lang.String[] { "Type", "ScalarType", "StructFields", });
    internal_static_doris_PTypeDesc_descriptor =
      getDescriptor().getMessageTypes().get(4);
    internal_static_doris_PTypeDesc_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PTypeDesc_descriptor,
        new java.lang.String[] { "Types", });
    internal_static_doris_PUniqueId_descriptor =
      getDescriptor().getMessageTypes().get(5);
    internal_static_doris_PUniqueId_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PUniqueId_descriptor,
        new java.lang.String[] { "Hi", "Lo", });
    internal_static_doris_PGenericType_descriptor =
      getDescriptor().getMessageTypes().get(6);
    internal_static_doris_PGenericType_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PGenericType_descriptor,
        new java.lang.String[] { "Id", "ListType", "MapType", "StructType", "DecimalType", });
    internal_static_doris_PList_descriptor =
      getDescriptor().getMessageTypes().get(7);
    internal_static_doris_PList_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PList_descriptor,
        new java.lang.String[] { "ElementType", });
    internal_static_doris_PMap_descriptor =
      getDescriptor().getMessageTypes().get(8);
    internal_static_doris_PMap_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PMap_descriptor,
        new java.lang.String[] { "KeyType", "ValueType", });
    internal_static_doris_PField_descriptor =
      getDescriptor().getMessageTypes().get(9);
    internal_static_doris_PField_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PField_descriptor,
        new java.lang.String[] { "Type", "Name", "Comment", });
    internal_static_doris_PStruct_descriptor =
      getDescriptor().getMessageTypes().get(10);
    internal_static_doris_PStruct_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PStruct_descriptor,
        new java.lang.String[] { "Fields", "Name", });
    internal_static_doris_PDecimal_descriptor =
      getDescriptor().getMessageTypes().get(11);
    internal_static_doris_PDecimal_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PDecimal_descriptor,
        new java.lang.String[] { "Precision", "Scale", });
    internal_static_doris_PDateTime_descriptor =
      getDescriptor().getMessageTypes().get(12);
    internal_static_doris_PDateTime_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PDateTime_descriptor,
        new java.lang.String[] { "Year", "Month", "Day", "Hour", "Minute", "Second", "Microsecond", });
    internal_static_doris_PValue_descriptor =
      getDescriptor().getMessageTypes().get(13);
    internal_static_doris_PValue_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PValue_descriptor,
        new java.lang.String[] { "Type", "IsNull", "DoubleValue", "FloatValue", "Int32Value", "Int64Value", "Uint32Value", "Uint64Value", "BoolValue", "StringValue", "BytesValue", "DatetimeValue", });
    internal_static_doris_PValues_descriptor =
      getDescriptor().getMessageTypes().get(14);
    internal_static_doris_PValues_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PValues_descriptor,
        new java.lang.String[] { "Type", "HasNull", "NullMap", "DoubleValue", "FloatValue", "Int32Value", "Int64Value", "Uint32Value", "Uint64Value", "BoolValue", "StringValue", "BytesValue", "DatetimeValue", });
    internal_static_doris_PFunction_descriptor =
      getDescriptor().getMessageTypes().get(15);
    internal_static_doris_PFunction_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PFunction_descriptor,
        new java.lang.String[] { "FunctionName", "Inputs", "Output", "Type", "Variadic", "Properties", });
    internal_static_doris_PFunction_Property_descriptor =
      internal_static_doris_PFunction_descriptor.getNestedTypes().get(0);
    internal_static_doris_PFunction_Property_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PFunction_Property_descriptor,
        new java.lang.String[] { "Key", "Val", });
    internal_static_doris_PFunctionContext_descriptor =
      getDescriptor().getMessageTypes().get(16);
    internal_static_doris_PFunctionContext_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PFunctionContext_descriptor,
        new java.lang.String[] { "Version", "StagingInputVals", "ConstantArgs", "ErrorMsg", "QueryId", "ThreadLocalFnState", "FragmentLocalFnState", "StringResult", "NumUpdates", "NumRemoves", "NumWarnings", });
    internal_static_doris_PHandShakeRequest_descriptor =
      getDescriptor().getMessageTypes().get(17);
    internal_static_doris_PHandShakeRequest_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PHandShakeRequest_descriptor,
        new java.lang.String[] { "Hello", });
    internal_static_doris_PHandShakeResponse_descriptor =
      getDescriptor().getMessageTypes().get(18);
    internal_static_doris_PHandShakeResponse_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_doris_PHandShakeResponse_descriptor,
        new java.lang.String[] { "Status", "Hello", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
