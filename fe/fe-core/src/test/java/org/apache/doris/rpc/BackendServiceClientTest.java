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

import org.apache.doris.common.Config;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.PBackendServiceGrpc;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for BackendServiceClient to verify that it uses
 * resolved IP addresses instead of hostnames for gRPC connections.
 */
public class BackendServiceClientTest {
    private ExecutorService executor;
    private int originalGrpcKeepAliveSeconds;
    private int originalGrpcMaxMessageSize;
    private long originalRemoteFragmentExecTimeout;

    @BeforeEach
    public void setUp() {
        // Create executor for tests
        executor = Executors.newCachedThreadPool();

        // Save original config values
        originalGrpcKeepAliveSeconds = Config.grpc_keep_alive_second;
        originalGrpcMaxMessageSize = Config.grpc_max_message_size_bytes;
        originalRemoteFragmentExecTimeout = Config.remote_fragment_exec_timeout_ms;

        // Set test config values to reasonable defaults
        Config.grpc_keep_alive_second = 60;
        Config.grpc_max_message_size_bytes = 1024 * 1024 * 100; // 100MB
        Config.remote_fragment_exec_timeout_ms = 5000;
    }

    @AfterEach
    public void tearDown() {
        // Restore original config
        Config.grpc_keep_alive_second = originalGrpcKeepAliveSeconds;
        Config.grpc_max_message_size_bytes = originalGrpcMaxMessageSize;
        Config.remote_fragment_exec_timeout_ms = originalRemoteFragmentExecTimeout;

        if (executor != null) {
            executor.shutdown();
        }
    }

    @Test
    public void testBatchRpcOverRealChannelPreservesContextAndDeadline() throws Exception {
        AtomicReference<Deadline> batchDeadline = new AtomicReference<>();
        AtomicReference<Deadline> unaryDeadline = new AtomicReference<>();
        Server server = ServerBuilder.forPort(0).addService(new PBackendServiceGrpc.PBackendServiceImplBase() {
            @Override
            public void tabletFetchDataBatch(InternalService.PTabletKeyLookupBatchRequest request,
                    StreamObserver<InternalService.PTabletKeyLookupBatchResponse> observer) {
                batchDeadline.set(Context.current().getDeadline());
                Assertions.assertEquals(2, request.getItemsCount());
                Assertions.assertEquals("Asia/Tokyo", request.getItems(0).getRequest().getTimeZone());
                Assertions.assertEquals(123, request.getItems(0).getRequest().getVersion());
                observer.onNext(InternalService.PTabletKeyLookupBatchResponse.newBuilder()
                        .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                        .addResults(InternalService.PTabletKeyLookupResponse.newBuilder()
                                .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                                .setNeedResendQueryContext(true))
                        .addResults(InternalService.PTabletKeyLookupResponse.newBuilder()
                                .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                                .setRowBatch(com.google.protobuf.ByteString.copyFrom(new byte[32]))).build());
                observer.onCompleted();
            }

            @Override
            public void tabletFetchData(InternalService.PTabletKeyLookupRequest request,
                    StreamObserver<InternalService.PTabletKeyLookupResponse> observer) {
                unaryDeadline.set(Context.current().getDeadline());
                observer.onNext(InternalService.PTabletKeyLookupResponse.newBuilder()
                        .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                        .setRowBatch(com.google.protobuf.ByteString.copyFrom(new byte[32])).build());
                observer.onCompleted();
            }
        }).build().start();
        BackendServiceClient client = new BackendServiceClient(
                new TNetworkAddress("127.0.0.1", server.getPort()), "127.0.0.1", executor);
        try {
            InternalService.PTabletKeyLookupRequest lookup = InternalService.PTabletKeyLookupRequest.newBuilder()
                    .setTabletId(7).setTimeZone("Asia/Tokyo").setVersion(123).build();
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
            client.fetchTabletDataAsync(lookup, 1000).get(5, TimeUnit.SECONDS);
            Assertions.assertNotNull(unaryDeadline.get());
            Config.grpc_max_message_size_bytes = 48;
            BackendServiceClient smallClient = new BackendServiceClient(
                    new TNetworkAddress("127.0.0.1", server.getPort()), "127.0.0.1", executor);
            try {
                java.util.concurrent.ExecutionException failure = Assertions.assertThrows(
                        java.util.concurrent.ExecutionException.class,
                        () -> smallClient.fetchTabletDataBatchAsync(InternalService.PTabletKeyLookupBatchRequest
                                .newBuilder().addItems(item).addItems(item).build(), 5000).get(5, TimeUnit.SECONDS));
                Assertions.assertEquals(io.grpc.Status.Code.RESOURCE_EXHAUSTED,
                        io.grpc.Status.fromThrowable(failure).getCode());
                Assertions.assertEquals(32, smallClient.fetchTabletDataAsync(lookup, 1000)
                        .get(5, TimeUnit.SECONDS).getRowBatch().size());
            } finally {
                smallClient.shutdown();
            }
        } finally {
            client.shutdown();
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    /**
     * Test that BackendServiceClient uses the resolved IP address
     * when creating the gRPC channel.
     */
    @Test
    public void testClientUsesResolvedIp() {
        String hostname = "backend.example.com";
        String resolvedIp = "10.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with resolved IP
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify client was created
        Assertions.assertNotNull(client);

        // Verify the address is stored
        TNetworkAddress storedAddress = Deencapsulation.getField(client, "address");
        Assertions.assertEquals(address, storedAddress);

        // Verify the channel was created (non-null)
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assertions.assertNotNull(channel);

        // Note: We cannot easily verify that the channel uses the IP instead of hostname
        // without inspecting the channel's internal state, which is implementation-dependent.
        // In a real scenario, you would use integration tests or network monitoring to verify.

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that when resolved IP is empty, the client falls back to using hostname.
     */
    @Test
    public void testClientFallsBackToHostnameWhenIpIsEmpty() {
        String hostname = "localhost"; // Use localhost to ensure it resolves
        String emptyIp = "";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with empty IP - should fallback to hostname
        BackendServiceClient client = new BackendServiceClient(address, emptyIp, executor);

        // Verify client was created
        Assertions.assertNotNull(client);

        // Verify channel was created
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assertions.assertNotNull(channel);

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that when resolved IP is null, the client falls back to using hostname.
     */
    @Test
    public void testClientFallsBackToHostnameWhenIpIsNull() {
        String hostname = "localhost"; // Use localhost to ensure it resolves
        String nullIp = null;
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client with null IP - should fallback to hostname
        BackendServiceClient client = new BackendServiceClient(address, nullIp, executor);

        // Verify client was created
        Assertions.assertNotNull(client);

        // Verify channel was created
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assertions.assertNotNull(channel);

        // Cleanup
        client.shutdown();
    }

    /**
     * Test that the client's isNormalState() method works correctly
     * after creation.
     */
    @Test
    public void testIsNormalState() {
        String hostname = "localhost";
        String resolvedIp = "127.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify client is in normal state initially
        // (IDLE or CONNECTING state is considered normal)
        Assertions.assertTrue(client.isNormalState(), "Client should be in normal state after creation");

        // Cleanup
        client.shutdown();

        // After shutdown, state should no longer be normal
        // Note: This might be racy, so we don't assert on it strictly
    }

    /**
     * Test that shutdown() properly closes the channel.
     */
    @Test
    public void testShutdown() throws InterruptedException {
        String hostname = "localhost";
        String resolvedIp = "127.0.0.1";
        int port = 9060;

        TNetworkAddress address = new TNetworkAddress(hostname, port);

        // Create client
        BackendServiceClient client = new BackendServiceClient(address, resolvedIp, executor);

        // Verify channel is not shutdown initially
        ManagedChannel channel = Deencapsulation.getField(client, "channel");
        Assertions.assertFalse(channel.isShutdown(), "Channel should not be shutdown initially");

        // Shutdown client
        client.shutdown();

        // Give it a moment to shutdown
        Thread.sleep(100);

        // Verify channel is shutdown or terminated
        Assertions.assertTrue(channel.isShutdown() || channel.isTerminated(), "Channel should be shutdown or terminated");
    }

    /**
     * Test that multiple clients can be created with different addresses.
     */
    @Test
    public void testMultipleClients() {
        TNetworkAddress address1 = new TNetworkAddress("localhost", 9060);
        TNetworkAddress address2 = new TNetworkAddress("localhost", 9061);

        BackendServiceClient client1 = new BackendServiceClient(address1, "127.0.0.1", executor);
        BackendServiceClient client2 = new BackendServiceClient(address2, "127.0.0.1", executor);

        Assertions.assertNotNull(client1);
        Assertions.assertNotNull(client2);
        Assertions.assertTrue(client1.isNormalState());
        Assertions.assertTrue(client2.isNormalState());

        // Cleanup
        client1.shutdown();
        client2.shutdown();
    }

    @Test
    public void testSyncTabletMeta() {
        TNetworkAddress address = new TNetworkAddress("localhost", 9060);
        BackendServiceClient client = new BackendServiceClient(address, "127.0.0.1", executor);

        PBackendServiceGrpc.PBackendServiceFutureStub stub =
                Mockito.mock(PBackendServiceGrpc.PBackendServiceFutureStub.class);
        Deencapsulation.setField(client, "stub", stub);

        InternalService.PSyncTabletMetaRequest request = InternalService.PSyncTabletMetaRequest.newBuilder()
                .addTabletIds(10001L)
                .build();
        ListenableFuture<InternalService.PSyncTabletMetaResponse> expectedFuture = Futures.immediateFuture(
                InternalService.PSyncTabletMetaResponse.newBuilder()
                        .setSyncedTablets(1)
                        .build());
        Mockito.when(stub.syncTabletMeta(request)).thenReturn(expectedFuture);

        ListenableFuture<InternalService.PSyncTabletMetaResponse> actualFuture = client.syncTabletMeta(request);

        Assertions.assertSame(expectedFuture, actualFuture);
        Mockito.verify(stub).syncTabletMeta(request);
        client.shutdown();
    }
}
