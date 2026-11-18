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

package org.apache.doris.rpc;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.DNSCache;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http2.DefaultHttp2DataFrame;
import io.netty.handler.codec.http2.DefaultHttp2Headers;
import io.netty.handler.codec.http2.DefaultHttp2HeadersFrame;
import io.netty.handler.codec.http2.DefaultHttp2ResetFrame;
import io.netty.handler.codec.http2.Http2DataFrame;
import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Frame;
import io.netty.handler.codec.http2.Http2FrameCodecBuilder;
import io.netty.handler.codec.http2.Http2Headers;
import io.netty.handler.codec.http2.Http2HeadersFrame;
import io.netty.handler.codec.http2.Http2MultiplexHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class BackendServiceBatchClientTest {
    private ExecutorService executor;
    private int savedMaxMessageSize;
    private boolean savedDirectExecutor;
    private boolean savedRetryEnabled;
    private Map<String, Field> savedConfFields;

    @BeforeEach
    void setUp() throws Exception {
        savedMaxMessageSize = Config.grpc_max_message_size_bytes;
        savedDirectExecutor = Config.grpc_backend_client_use_direct_executor;
        savedRetryEnabled = Config.grpc_backend_client_enable_retry;
        savedConfFields = ConfigBase.confFields;
        Config.grpc_max_message_size_bytes = 1024 * 1024;
        Config.grpc_backend_client_use_direct_executor = false;
        Config.grpc_backend_client_enable_retry = false;
        Map<String, Field> confFields = savedConfFields == null ? new HashMap<>() : new HashMap<>(savedConfFields);
        confFields.put("grpc_backend_client_enable_retry", Config.class.getField("grpc_backend_client_enable_retry"));
        ConfigBase.confFields = confFields;
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        } finally {
            Config.grpc_max_message_size_bytes = savedMaxMessageSize;
            Config.grpc_backend_client_use_direct_executor = savedDirectExecutor;
            Config.grpc_backend_client_enable_retry = savedRetryEnabled;
            ConfigBase.confFields = savedConfFields;
        }
    }

    @Test
    void testBatchRpcOverRealChannelPreservesContextAndDeadline() throws Exception {
        InternalService.PTabletKeyLookupRequest lookup = InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(7)
                .addKeyTuples(InternalService.KeyTuple.newBuilder().addKeyColumnRep("key"))
                .setUuid(InternalService.UUID.newBuilder().setUuidHigh(11).setUuidLow(12))
                .setDescTbl(ByteString.copyFromUtf8("descriptors"))
                .setOutputExpr(ByteString.copyFromUtf8("output expressions"))
                .setIsBinaryRow(true)
                .setVersion(123)
                .setQueryOptions(ByteString.copyFromUtf8("query options"))
                .build();
        AtomicReference<Deadline> batchDeadline = new AtomicReference<>();
        AtomicReference<Deadline> unaryDeadline = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0).addService(new PBackendServiceGrpc.PBackendServiceImplBase() {
            @Override
            public void tabletFetchDataBatch(InternalService.PTabletKeyLookupBatchRequest request,
                    StreamObserver<InternalService.PTabletKeyLookupBatchResponse> observer) {
                batchDeadline.set(Context.current().getDeadline());
                Assertions.assertEquals(2, request.getItemsCount());
                for (InternalService.PTabletKeyLookupBatchItem item : request.getItemsList()) {
                    Assertions.assertEquals(lookup, item.getRequest());
                    Assertions.assertTrue(item.getRemainingTimeoutMs() > 0);
                }
                observer.onNext(InternalService.PTabletKeyLookupBatchResponse.newBuilder()
                        .setStatus(PStatus.newBuilder().setStatusCode(0))
                        .addResults(InternalService.PTabletKeyLookupResponse.newBuilder()
                                .setStatus(PStatus.newBuilder().setStatusCode(0))
                                .setNeedResendQueryContext(true))
                        .addResults(InternalService.PTabletKeyLookupResponse.newBuilder()
                                .setStatus(PStatus.newBuilder().setStatusCode(0))
                                .setRowBatch(ByteString.copyFrom(new byte[32]))).build());
                observer.onCompleted();
            }

            @Override
            public void tabletFetchData(InternalService.PTabletKeyLookupRequest request,
                    StreamObserver<InternalService.PTabletKeyLookupResponse> observer) {
                unaryDeadline.set(Context.current().getDeadline());
                Assertions.assertEquals(lookup, request);
                observer.onNext(InternalService.PTabletKeyLookupResponse.newBuilder()
                        .setStatus(PStatus.newBuilder().setStatusCode(0))
                        .setRowBatch(ByteString.copyFrom(new byte[32])).build());
                observer.onCompleted();
            }
        }).build().start();
        BackendServiceClient client = null;
        try {
            client = new BackendServiceClient(new TNetworkAddress("127.0.0.1", server.getPort()), "127.0.0.1", executor);
            InternalService.PTabletKeyLookupBatchItem item = InternalService.PTabletKeyLookupBatchItem.newBuilder()
                    .setRequest(lookup).setRemainingTimeoutMs(1000).build();
            InternalService.PTabletKeyLookupBatchResponse response = client.fetchTabletDataBatchAsync(
                    InternalService.PTabletKeyLookupBatchRequest.newBuilder().addItems(item).addItems(item).build(),
                    5000).get(5, TimeUnit.SECONDS);
            Assertions.assertEquals(2, response.getResultsCount());
            Assertions.assertTrue(response.getResults(0).getNeedResendQueryContext());
            Assertions.assertEquals(32, response.getResults(1).getRowBatch().size());
            Assertions.assertNotNull(batchDeadline.get());
            client.fetchTabletDataAsync(lookup).get(5, TimeUnit.SECONDS);
            Assertions.assertNull(unaryDeadline.get());
            Config.grpc_max_message_size_bytes = 48;
            BackendServiceClient smallClient = new BackendServiceClient(
                    new TNetworkAddress("127.0.0.1", server.getPort()), "127.0.0.1", executor);
            try {
                ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                        () -> smallClient.fetchTabletDataBatchAsync(InternalService.PTabletKeyLookupBatchRequest
                                .newBuilder().addItems(item).addItems(item).build(), 5000).get(5, TimeUnit.SECONDS));
                Assertions.assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED,
                        io.grpc.Status.fromThrowable(failure).getCode());
                Assertions.assertEquals(32, smallClient.fetchTabletDataAsync(lookup)
                        .get(5, TimeUnit.SECONDS).getRowBatch().size());
            } finally {
                smallClient.shutdown();
            }
        } finally {
            try {
                if (client != null) {
                    client.shutdown();
                }
            } finally {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void testProxyRebuildsCachedClientWhenRetryFlagChanges() throws Exception {
        InternalService.PTabletKeyLookupRequest lookup = newLookupRequest(101);
        BackendServiceProxy proxy = new BackendServiceProxy();
        try (RefusedOnceH2cServer server = new RefusedOnceH2cServer(newLookupResponse("retried"));
                MockedStatic<Env> mockedEnv = mockCurrentEnvWithDnsCache()) {
            TNetworkAddress address = new TNetworkAddress("127.0.0.1", server.getPort());
            try {
                setRetryEnabledViaMutableConfig(false);
                server.refuseNextAttempt();
                int attemptsBefore = server.getAttemptCount();
                ExecutionException noRetryFailure = Assertions.assertThrows(ExecutionException.class,
                        () -> proxy.fetchTabletDataAsync(address, lookup).get(5, TimeUnit.SECONDS));
                assertRefusedStreamFailure(noRetryFailure);
                Assertions.assertEquals(1, server.getAttemptCount() - attemptsBefore);

                setRetryEnabledViaMutableConfig(true);
                server.refuseNextAttempt();
                attemptsBefore = server.getAttemptCount();
                InternalService.PTabletKeyLookupResponse retried = proxy.fetchTabletDataAsync(address, lookup)
                        .get(5, TimeUnit.SECONDS);
                Assertions.assertEquals("retried", retried.getRowBatch().toStringUtf8());
                Assertions.assertEquals(2, server.getAttemptCount() - attemptsBefore);

                setRetryEnabledViaMutableConfig(false);
                server.refuseNextAttempt();
                attemptsBefore = server.getAttemptCount();
                ExecutionException disabledAgain = Assertions.assertThrows(ExecutionException.class,
                        () -> proxy.fetchTabletDataAsync(address, lookup).get(5, TimeUnit.SECONDS));
                assertRefusedStreamFailure(disabledAgain);
                Assertions.assertEquals(1, server.getAttemptCount() - attemptsBefore);

                server.assertNoAsyncFailure();
            } finally {
                proxy.removeProxy(address);
            }
        }
    }

    @Test
    void testInFlightRpcFinishesWhileProxySwitchesRetryClients() throws Exception {
        CountDownLatch firstCallStarted = new CountDownLatch(1);
        CountDownLatch releaseFirstCall = new CountDownLatch(1);
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0).addService(new PBackendServiceGrpc.PBackendServiceImplBase() {
            @Override
            public void tabletFetchData(InternalService.PTabletKeyLookupRequest request,
                    StreamObserver<InternalService.PTabletKeyLookupResponse> observer) {
                int call = requests.incrementAndGet();
                try {
                    if (call == 1) {
                        firstCallStarted.countDown();
                        if (!releaseFirstCall.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("timed out waiting to release the first RPC");
                        }
                    }
                    observer.onNext(newLookupResponse("call-" + call));
                    observer.onCompleted();
                } catch (Throwable t) {
                    serverFailure.compareAndSet(null, t);
                    observer.onError(t);
                }
            }
        }).build().start();
        BackendServiceProxy proxy = new BackendServiceProxy();
        TNetworkAddress address = new TNetworkAddress("127.0.0.1", server.getPort());
        try (MockedStatic<Env> mockedEnv = mockCurrentEnvWithDnsCache()) {
            setRetryEnabledViaMutableConfig(false);
            ListenableFuture<InternalService.PTabletKeyLookupResponse> firstFuture =
                    proxy.fetchTabletDataAsync(address, newLookupRequest(201));
            Assertions.assertTrue(firstCallStarted.await(5, TimeUnit.SECONDS));

            setRetryEnabledViaMutableConfig(true);
            InternalService.PTabletKeyLookupResponse second = proxy.fetchTabletDataAsync(address, newLookupRequest(202))
                    .get(5, TimeUnit.SECONDS);
            Assertions.assertEquals("call-2", second.getRowBatch().toStringUtf8());
            Assertions.assertFalse(firstFuture.isDone(),
                    "first in-flight RPC should still be pending after retry-enabled rebuild");

            setRetryEnabledViaMutableConfig(false);
            InternalService.PTabletKeyLookupResponse third = proxy.fetchTabletDataAsync(address, newLookupRequest(203))
                    .get(5, TimeUnit.SECONDS);
            Assertions.assertEquals("call-3", third.getRowBatch().toStringUtf8());
            Assertions.assertFalse(firstFuture.isDone(),
                    "first in-flight RPC should still be pending after retry-disabled rebuild");

            releaseFirstCall.countDown();
            InternalService.PTabletKeyLookupResponse first = firstFuture.get(5, TimeUnit.SECONDS);
            Assertions.assertEquals("call-1", first.getRowBatch().toStringUtf8());
            Assertions.assertEquals(3, requests.get());
            Assertions.assertNull(serverFailure.get(), "server handler failed");
        } finally {
            releaseFirstCall.countDown();
            try {
                proxy.removeProxy(address);
            } finally {
                server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    private void setRetryEnabledViaMutableConfig(boolean enabled) throws Exception {
        ConfigBase.setMutableConfig("grpc_backend_client_enable_retry", Boolean.toString(enabled));
        Assertions.assertEquals(enabled, Config.grpc_backend_client_enable_retry);
    }

    private MockedStatic<Env> mockCurrentEnvWithDnsCache() {
        Env env = Mockito.mock(Env.class);
        Mockito.when(env.getDnsCache()).thenReturn(new DNSCache());
        MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class);
        mockedEnv.when(Env::getCurrentEnv).thenReturn(env);
        return mockedEnv;
    }

    private void assertRefusedStreamFailure(ExecutionException failure) {
        Throwable cause = failure.getCause() == null ? failure : failure.getCause();
        Status status = Status.fromThrowable(cause);
        Assertions.assertEquals(Status.Code.UNAVAILABLE, status.getCode());
        String description = String.valueOf(status.getDescription()).toUpperCase(Locale.ROOT);
        Assertions.assertTrue(description.contains("REFUSED_STREAM"), description);
    }

    private static InternalService.PTabletKeyLookupRequest newLookupRequest(long tabletId) {
        return InternalService.PTabletKeyLookupRequest.newBuilder().setTabletId(tabletId).build();
    }

    private static InternalService.PTabletKeyLookupResponse newLookupResponse(String payload) {
        return InternalService.PTabletKeyLookupResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(0))
                .setRowBatch(ByteString.copyFromUtf8(payload))
                .build();
    }

    private static final class RefusedOnceH2cServer implements AutoCloseable {
        private static final String EXPECTED_PATH = "/" + PBackendServiceGrpc.SERVICE_NAME + "/tablet_fetch_data";

        private final NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        private final NioEventLoopGroup workerGroup = new NioEventLoopGroup(1);
        private final AtomicBoolean refuseNextAttempt = new AtomicBoolean();
        private final AtomicInteger attemptCount = new AtomicInteger();
        private final AtomicReference<Throwable> asyncFailure = new AtomicReference<>();
        private final InternalService.PTabletKeyLookupResponse successResponse;
        private final Channel serverChannel;

        RefusedOnceH2cServer(InternalService.PTabletKeyLookupResponse successResponse) throws InterruptedException {
            this.successResponse = successResponse;
            serverChannel = new ServerBootstrap()
                    .group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(Http2FrameCodecBuilder.forServer().build());
                            ch.pipeline().addLast(new Http2MultiplexHandler(new ChannelInitializer<Channel>() {
                                @Override
                                protected void initChannel(Channel streamChannel) {
                                    streamChannel.pipeline().addLast(new StreamHandler());
                                }
                            }));
                        }
                    })
                    .bind(0)
                    .sync()
                    .channel();
        }

        int getPort() {
            return ((InetSocketAddress) serverChannel.localAddress()).getPort();
        }

        int getAttemptCount() {
            return attemptCount.get();
        }

        void refuseNextAttempt() {
            refuseNextAttempt.set(true);
        }

        void assertNoAsyncFailure() {
            Assertions.assertNull(asyncFailure.get(), "raw h2 server failed");
        }

        @Override
        public void close() throws InterruptedException {
            try {
                serverChannel.close().sync();
            } finally {
                workerGroup.shutdownGracefully().sync();
                bossGroup.shutdownGracefully().sync();
            }
        }

        private ByteBuf encodeGrpcMessage(InternalService.PTabletKeyLookupResponse response) {
            byte[] bytes = response.toByteArray();
            ByteBuf payload = Unpooled.buffer(5 + bytes.length);
            payload.writeByte(0);
            payload.writeInt(bytes.length);
            payload.writeBytes(bytes);
            return payload;
        }

        private final class StreamHandler extends SimpleChannelInboundHandler<Http2Frame> {
            private boolean handled;

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, Http2Frame frame) {
                if (frame instanceof Http2HeadersFrame) {
                    Http2Headers headers = ((Http2HeadersFrame) frame).headers();
                    CharSequence path = headers.path();
                    if (path != null && !EXPECTED_PATH.contentEquals(path)) {
                        asyncFailure.compareAndSet(null,
                                new AssertionError("unexpected path: " + path));
                    }
                }

                boolean endStream = (frame instanceof Http2HeadersFrame && ((Http2HeadersFrame) frame).isEndStream())
                        || (frame instanceof Http2DataFrame && ((Http2DataFrame) frame).isEndStream());
                if (!endStream || handled) {
                    return;
                }
                handled = true;
                attemptCount.incrementAndGet();
                if (refuseNextAttempt.compareAndSet(true, false)) {
                    ctx.writeAndFlush(new DefaultHttp2ResetFrame(Http2Error.REFUSED_STREAM));
                    return;
                }

                Http2Headers responseHeaders = new DefaultHttp2Headers()
                        .status(HttpResponseStatus.OK.codeAsText())
                        .set(HttpHeaderNames.CONTENT_TYPE, "application/grpc+proto");
                Http2Headers responseTrailers = new DefaultHttp2Headers().set("grpc-status", "0");
                ctx.write(new DefaultHttp2HeadersFrame(responseHeaders, false));
                ctx.write(new DefaultHttp2DataFrame(encodeGrpcMessage(successResponse), false));
                ctx.writeAndFlush(new DefaultHttp2HeadersFrame(responseTrailers, true));
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                asyncFailure.compareAndSet(null, cause);
                ctx.close();
            }
        }
    }
}
