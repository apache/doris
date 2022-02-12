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

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.29.0)",
    comments = "Source: function_service.proto")
public final class PFunctionServiceGrpc {

  private PFunctionServiceGrpc() {}

  public static final String SERVICE_NAME = "doris.PFunctionService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PFunctionCallRequest,
      org.apache.doris.proto.FunctionService.PFunctionCallResponse> getFnCallMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "fn_call",
      requestType = org.apache.doris.proto.FunctionService.PFunctionCallRequest.class,
      responseType = org.apache.doris.proto.FunctionService.PFunctionCallResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PFunctionCallRequest,
      org.apache.doris.proto.FunctionService.PFunctionCallResponse> getFnCallMethod() {
    io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PFunctionCallRequest, org.apache.doris.proto.FunctionService.PFunctionCallResponse> getFnCallMethod;
    if ((getFnCallMethod = PFunctionServiceGrpc.getFnCallMethod) == null) {
      synchronized (PFunctionServiceGrpc.class) {
        if ((getFnCallMethod = PFunctionServiceGrpc.getFnCallMethod) == null) {
          PFunctionServiceGrpc.getFnCallMethod = getFnCallMethod =
              io.grpc.MethodDescriptor.<org.apache.doris.proto.FunctionService.PFunctionCallRequest, org.apache.doris.proto.FunctionService.PFunctionCallResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "fn_call"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.FunctionService.PFunctionCallRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.FunctionService.PFunctionCallResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PFunctionServiceMethodDescriptorSupplier("fn_call"))
              .build();
        }
      }
    }
    return getFnCallMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PCheckFunctionRequest,
      org.apache.doris.proto.FunctionService.PCheckFunctionResponse> getCheckFnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "check_fn",
      requestType = org.apache.doris.proto.FunctionService.PCheckFunctionRequest.class,
      responseType = org.apache.doris.proto.FunctionService.PCheckFunctionResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PCheckFunctionRequest,
      org.apache.doris.proto.FunctionService.PCheckFunctionResponse> getCheckFnMethod() {
    io.grpc.MethodDescriptor<org.apache.doris.proto.FunctionService.PCheckFunctionRequest, org.apache.doris.proto.FunctionService.PCheckFunctionResponse> getCheckFnMethod;
    if ((getCheckFnMethod = PFunctionServiceGrpc.getCheckFnMethod) == null) {
      synchronized (PFunctionServiceGrpc.class) {
        if ((getCheckFnMethod = PFunctionServiceGrpc.getCheckFnMethod) == null) {
          PFunctionServiceGrpc.getCheckFnMethod = getCheckFnMethod =
              io.grpc.MethodDescriptor.<org.apache.doris.proto.FunctionService.PCheckFunctionRequest, org.apache.doris.proto.FunctionService.PCheckFunctionResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "check_fn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.FunctionService.PCheckFunctionRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.FunctionService.PCheckFunctionResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PFunctionServiceMethodDescriptorSupplier("check_fn"))
              .build();
        }
      }
    }
    return getCheckFnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<org.apache.doris.proto.Types.PHandShakeRequest,
      org.apache.doris.proto.Types.PHandShakeResponse> getHandShakeMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "hand_shake",
      requestType = org.apache.doris.proto.Types.PHandShakeRequest.class,
      responseType = org.apache.doris.proto.Types.PHandShakeResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<org.apache.doris.proto.Types.PHandShakeRequest,
      org.apache.doris.proto.Types.PHandShakeResponse> getHandShakeMethod() {
    io.grpc.MethodDescriptor<org.apache.doris.proto.Types.PHandShakeRequest, org.apache.doris.proto.Types.PHandShakeResponse> getHandShakeMethod;
    if ((getHandShakeMethod = PFunctionServiceGrpc.getHandShakeMethod) == null) {
      synchronized (PFunctionServiceGrpc.class) {
        if ((getHandShakeMethod = PFunctionServiceGrpc.getHandShakeMethod) == null) {
          PFunctionServiceGrpc.getHandShakeMethod = getHandShakeMethod =
              io.grpc.MethodDescriptor.<org.apache.doris.proto.Types.PHandShakeRequest, org.apache.doris.proto.Types.PHandShakeResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "hand_shake"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.Types.PHandShakeRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  org.apache.doris.proto.Types.PHandShakeResponse.getDefaultInstance()))
              .setSchemaDescriptor(new PFunctionServiceMethodDescriptorSupplier("hand_shake"))
              .build();
        }
      }
    }
    return getHandShakeMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static PFunctionServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceStub>() {
        @java.lang.Override
        public PFunctionServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PFunctionServiceStub(channel, callOptions);
        }
      };
    return PFunctionServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static PFunctionServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceBlockingStub>() {
        @java.lang.Override
        public PFunctionServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PFunctionServiceBlockingStub(channel, callOptions);
        }
      };
    return PFunctionServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static PFunctionServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<PFunctionServiceFutureStub>() {
        @java.lang.Override
        public PFunctionServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new PFunctionServiceFutureStub(channel, callOptions);
        }
      };
    return PFunctionServiceFutureStub.newStub(factory, channel);
  }

  /**
   */
  public static abstract class PFunctionServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void fnCall(org.apache.doris.proto.FunctionService.PFunctionCallRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PFunctionCallResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getFnCallMethod(), responseObserver);
    }

    /**
     */
    public void checkFn(org.apache.doris.proto.FunctionService.PCheckFunctionRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PCheckFunctionResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getCheckFnMethod(), responseObserver);
    }

    /**
     */
    public void handShake(org.apache.doris.proto.Types.PHandShakeRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.Types.PHandShakeResponse> responseObserver) {
      asyncUnimplementedUnaryCall(getHandShakeMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getFnCallMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.apache.doris.proto.FunctionService.PFunctionCallRequest,
                org.apache.doris.proto.FunctionService.PFunctionCallResponse>(
                  this, METHODID_FN_CALL)))
          .addMethod(
            getCheckFnMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.apache.doris.proto.FunctionService.PCheckFunctionRequest,
                org.apache.doris.proto.FunctionService.PCheckFunctionResponse>(
                  this, METHODID_CHECK_FN)))
          .addMethod(
            getHandShakeMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                org.apache.doris.proto.Types.PHandShakeRequest,
                org.apache.doris.proto.Types.PHandShakeResponse>(
                  this, METHODID_HAND_SHAKE)))
          .build();
    }
  }

  /**
   */
  public static final class PFunctionServiceStub extends io.grpc.stub.AbstractAsyncStub<PFunctionServiceStub> {
    private PFunctionServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PFunctionServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PFunctionServiceStub(channel, callOptions);
    }

    /**
     */
    public void fnCall(org.apache.doris.proto.FunctionService.PFunctionCallRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PFunctionCallResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getFnCallMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void checkFn(org.apache.doris.proto.FunctionService.PCheckFunctionRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PCheckFunctionResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getCheckFnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void handShake(org.apache.doris.proto.Types.PHandShakeRequest request,
        io.grpc.stub.StreamObserver<org.apache.doris.proto.Types.PHandShakeResponse> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getHandShakeMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   */
  public static final class PFunctionServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<PFunctionServiceBlockingStub> {
    private PFunctionServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PFunctionServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PFunctionServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public org.apache.doris.proto.FunctionService.PFunctionCallResponse fnCall(org.apache.doris.proto.FunctionService.PFunctionCallRequest request) {
      return blockingUnaryCall(
          getChannel(), getFnCallMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.doris.proto.FunctionService.PCheckFunctionResponse checkFn(org.apache.doris.proto.FunctionService.PCheckFunctionRequest request) {
      return blockingUnaryCall(
          getChannel(), getCheckFnMethod(), getCallOptions(), request);
    }

    /**
     */
    public org.apache.doris.proto.Types.PHandShakeResponse handShake(org.apache.doris.proto.Types.PHandShakeRequest request) {
      return blockingUnaryCall(
          getChannel(), getHandShakeMethod(), getCallOptions(), request);
    }
  }

  /**
   */
  public static final class PFunctionServiceFutureStub extends io.grpc.stub.AbstractFutureStub<PFunctionServiceFutureStub> {
    private PFunctionServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected PFunctionServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new PFunctionServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.doris.proto.FunctionService.PFunctionCallResponse> fnCall(
        org.apache.doris.proto.FunctionService.PFunctionCallRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getFnCallMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.doris.proto.FunctionService.PCheckFunctionResponse> checkFn(
        org.apache.doris.proto.FunctionService.PCheckFunctionRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getCheckFnMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<org.apache.doris.proto.Types.PHandShakeResponse> handShake(
        org.apache.doris.proto.Types.PHandShakeRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getHandShakeMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_FN_CALL = 0;
  private static final int METHODID_CHECK_FN = 1;
  private static final int METHODID_HAND_SHAKE = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final PFunctionServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(PFunctionServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_FN_CALL:
          serviceImpl.fnCall((org.apache.doris.proto.FunctionService.PFunctionCallRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PFunctionCallResponse>) responseObserver);
          break;
        case METHODID_CHECK_FN:
          serviceImpl.checkFn((org.apache.doris.proto.FunctionService.PCheckFunctionRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.doris.proto.FunctionService.PCheckFunctionResponse>) responseObserver);
          break;
        case METHODID_HAND_SHAKE:
          serviceImpl.handShake((org.apache.doris.proto.Types.PHandShakeRequest) request,
              (io.grpc.stub.StreamObserver<org.apache.doris.proto.Types.PHandShakeResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class PFunctionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    PFunctionServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return org.apache.doris.proto.FunctionService.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("PFunctionService");
    }
  }

  private static final class PFunctionServiceFileDescriptorSupplier
      extends PFunctionServiceBaseDescriptorSupplier {
    PFunctionServiceFileDescriptorSupplier() {}
  }

  private static final class PFunctionServiceMethodDescriptorSupplier
      extends PFunctionServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    PFunctionServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (PFunctionServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new PFunctionServiceFileDescriptorSupplier())
              .addMethod(getFnCallMethod())
              .addMethod(getCheckFnMethod())
              .addMethod(getHandShakeMethod())
              .build();
        }
      }
    }
    return result;
  }
}
