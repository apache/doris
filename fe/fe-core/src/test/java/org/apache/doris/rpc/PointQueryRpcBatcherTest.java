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
import org.apache.doris.common.ThreadPoolManager;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupBatchResponse;
import org.apache.doris.proto.InternalService.PTabletKeyLookupRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.thrift.TNetworkAddress;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class PointQueryRpcBatcherTest {
    private final TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8060);
    private final List<Thread> workers = new ArrayList<>();
    private int savedSize;
    private int savedWait;
    private int savedBytes;
    private FakeTransport transport;
    private PointQueryRpcBatcher batcher;

    @BeforeEach
    void setUp() {
        savedSize = Config.point_query_rpc_batch_max_size;
        savedWait = Config.point_query_rpc_batch_max_wait_us;
        savedBytes = Config.point_query_rpc_batch_max_request_bytes;
        Config.point_query_rpc_batch_max_size = 2;
        Config.point_query_rpc_batch_max_wait_us = 5_000_000;
        Config.point_query_rpc_batch_max_request_bytes = PointQueryRpcBatcher.MAX_BYTES;
        transport = new FakeTransport();
        batcher = new PointQueryRpcBatcher(ignored -> transport.client, MoreExecutors.directExecutor());
        keepUnaryInFlight();
    }

    @AfterEach
    void tearDown() throws Exception {
        try {
            for (Thread worker : workers) {
                worker.interrupt();
            }
            for (Thread worker : workers) {
                worker.join(5000);
                Assertions.assertFalse(worker.isAlive());
            }
        } finally {
            transport.pendingUnary.cancel(true);
            Config.point_query_rpc_batch_max_size = savedSize;
            Config.point_query_rpc_batch_max_wait_us = savedWait;
            Config.point_query_rpc_batch_max_request_bytes = savedBytes;
        }
    }

    @Test
    void solitaryQueriesDoNotWaitAndFailuresReleaseInFlightCount() {
        PointQueryRpcBatcher fresh = new PointQueryRpcBatcher(
                ignored -> transport.client, MoreExecutors.directExecutor());
        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
            Assertions.assertEquals(row(1), fresh.submit(address, request(1), 20_000).get());
            transport.failNextUnary = true;
            assertGrpc(Status.Code.UNAVAILABLE, fresh.submit(address, request(2), 20_000));
            Assertions.assertEquals(row(3), fresh.submit(address, request(3), 20_000).get());
            ListenableFuture<PTabletKeyLookupResponse> pending =
                    fresh.submit(address, request(Long.MIN_VALUE), 120_000);
            pending.cancel(true);
            Assertions.assertEquals(row(4), fresh.submit(address, request(4), 20_000).get());
        });
        Assertions.assertTrue(transport.batches.isEmpty());
    }

    @Test
    void fullBatchWakesOwnerAndMapsIndependentResults() throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        Assertions.assertEquals(1, transport.batches.size());
        Assertions.assertEquals(2, transport.batches.get(0).getItemsCount());
        transport.result.set(okBatch().addResults(row(1)).addResults(PTabletKeyLookupResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(4).addErrorMsgs("item failure"))).build());
        Assertions.assertEquals(row(1), results.get(0).get());
        Assertions.assertEquals(4, results.get(1).get().getStatus().getStatusCode());
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void fallbackSelectsSeparateClientsOutsideCompletionThread() throws Exception {
        List<Thread> resolutions = new ArrayList<>();
        List<Runnable> fallbacks = new ArrayList<>();
        FakeTransport firstFallback = new FakeTransport();
        FakeTransport secondFallback = new FakeTransport();
        List<BackendServiceClient> clients = java.util.Arrays.asList(
                transport.client, firstFallback.client, secondFallback.client);
        batcher = new PointQueryRpcBatcher(ignored -> {
            resolutions.add(Thread.currentThread());
            return clients.get(resolutions.size() - 1);
        }, fallbacks::add);
        keepUnaryInFlight();
        resolutions.clear();
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        Thread completion = new Thread(() -> transport.result.setException(
                Status.RESOURCE_EXHAUSTED.asRuntimeException()));
        completion.start();
        completion.join(5000);
        Assertions.assertFalse(completion.isAlive());
        Assertions.assertEquals(1, resolutions.size());
        Assertions.assertEquals(1, fallbacks.size());
        fallbacks.get(0).run();
        Assertions.assertEquals(row(1), results.get(0).get());
        Assertions.assertEquals(row(2), results.get(1).get());
        Assertions.assertEquals(java.util.Collections.singletonList(1L), firstFallback.unaries);
        Assertions.assertEquals(java.util.Collections.singletonList(2L), secondFallback.unaries);
        Assertions.assertTrue(resolutions.stream().allMatch(thread -> thread == Thread.currentThread()));
    }

    @Test
    void singletonAndDisabledLimitsUseUnary() throws Exception {
        Config.point_query_rpc_batch_max_wait_us = 50;
        Assertions.assertEquals(row(1), submit(1).get(5, TimeUnit.SECONDS));
        Config.point_query_rpc_batch_max_wait_us = 0;
        Assertions.assertEquals(row(2), submit(2).get());
        Config.point_query_rpc_batch_max_wait_us = 5_000_000;
        Config.point_query_rpc_batch_max_size = 1;
        Assertions.assertEquals(row(3), submit(3).get());
        Assertions.assertEquals(3, transport.unaries.size());
        Assertions.assertTrue(transport.batches.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(ints = {0, 50})
    void unarySubmissionFailureCompletesItem(int waitUs) throws Exception {
        Config.point_query_rpc_batch_max_wait_us = waitUs;
        IllegalStateException failure = new IllegalStateException("channel closed during submission");
        Mockito.when(transport.client.fetchTabletDataAsync(Mockito.any(PTabletKeyLookupRequest.class))).thenThrow(failure);
        ListenableFuture<PTabletKeyLookupResponse> result = submit(1);
        ExecutionException thrown = Assertions.assertThrows(ExecutionException.class,
                () -> result.get(5, TimeUnit.SECONDS));
        Assertions.assertSame(failure, thrown.getCause());
    }

    @Test
    void saturatedFallbackPoolCompletesEveryItem() throws Exception {
        ThreadPoolExecutor pool = ThreadPoolManager.newDaemonCacheThreadPoolThrowException(
                1, "point_query_batch_fallback_test", false);
        CountDownLatch occupied = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Future<?> blocker = null;
        try {
            blocker = pool.submit(() -> {
                occupied.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });
            Assertions.assertTrue(occupied.await(5, TimeUnit.SECONDS));
            batcher = new PointQueryRpcBatcher(ignored -> transport.client, pool);
            keepUnaryInFlight();
            CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
            ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
            transport.result.setException(Status.RESOURCE_EXHAUSTED.asRuntimeException());
            for (ListenableFuture<?> result : java.util.Arrays.asList(first.get(5, TimeUnit.SECONDS), second)) {
                Assertions.assertTrue(result.isDone(), "Rejected fallback must not leave a pending query");
                ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                        () -> result.get(5, TimeUnit.SECONDS));
                Assertions.assertInstanceOf(RejectedExecutionException.class, failure.getCause());
            }
            Assertions.assertTrue(transport.unaries.isEmpty());
        } finally {
            release.countDown();
            try {
                if (blocker != null) {
                    blocker.get(5, TimeUnit.SECONDS);
                }
            } finally {
                pool.shutdownNow();
                pool.awaitTermination(5, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void requestByteCapIncludesProtobufEnvelopeAndOversizeUsesUnary() throws Exception {
        PTabletKeyLookupRequest request = request(1).toBuilder().setDescTbl(ByteString.copyFrom(new byte[200])).build();
        Config.point_query_rpc_batch_max_request_bytes = request.getSerializedSize();
        Assertions.assertEquals(row(1), batcher.submit(address, request, 1000).get());
        Assertions.assertTrue(transport.batches.isEmpty());
        Assertions.assertEquals(1, transport.unaries.size());
    }

    @Test
    void byteBoundaryFlushesOldBatchWithoutLosingNewOwner() throws Exception {
        PTabletKeyLookupRequest request = request(1).toBuilder().setDescTbl(ByteString.copyFrom(new byte[200])).build();
        Config.point_query_rpc_batch_max_request_bytes = 300;
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request);
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> second = owner(request.toBuilder()
                .setTabletId(2).build());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        // Interrupting the second owner also flushes its lone request and preserves the interrupt.
        workers.get(1).interrupt();
        Assertions.assertEquals(row(2), second.get(5, TimeUnit.SECONDS).get());
        Assertions.assertTrue(transport.batches.isEmpty());
    }

    @Test
    void malformedResponseFailsEveryItem() throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        transport.result.set(okBatch().addResults(row(1)).build());
        assertGrpc(Status.Code.DATA_LOSS, results.get(0));
        assertGrpc(Status.Code.DATA_LOSS, results.get(1));
    }

    @Test
    void missingEnvelopeStatusFailsEveryItem() throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        transport.result.set(PTabletKeyLookupBatchResponse.newBuilder()
                .addResults(row(1)).addResults(row(2)).build());
        assertGrpc(Status.Code.DATA_LOSS, results.get(0));
        assertGrpc(Status.Code.DATA_LOSS, results.get(1));
    }

    @Test
    void outerAdmissionErrorIsReturnedToEveryItem() throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        transport.result.set(PTabletKeyLookupBatchResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(4).addErrorMsgs("queue full")).build());
        Assertions.assertEquals(4, results.get(0).get().getStatus().getStatusCode());
        Assertions.assertEquals(4, results.get(1).get().getStatus().getStatusCode());
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {"UNIMPLEMENTED", "RESOURCE_EXHAUSTED", "INTERNAL"})
    void recoverableBatchFailureFallsBackAndAllowsNextBatch(Status.Code code) throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        transport.result.setException(Status.fromCode(code).withDescription("FLOW_CONTROL_ERROR").asRuntimeException());
        Assertions.assertEquals(row(1), results.get(0).get());
        Assertions.assertEquals(row(2), results.get(1).get());
        Assertions.assertEquals(java.util.Arrays.asList(1L, 2L), transport.unaries);
        transport.result = SettableFuture.create();
        results = pair();
        transport.result.set(okBatch().addResults(row(1)).addResults(row(2)).build());
        Assertions.assertEquals(row(1), results.get(0).get());
        Assertions.assertEquals(row(2), results.get(1).get());
        Assertions.assertEquals(2, transport.batches.size());
    }

    @ParameterizedTest
    @EnumSource(value = Status.Code.class, names = {"UNAVAILABLE", "INTERNAL"})
    void ordinaryTransportFailureDoesNotReplay(Status.Code code) throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        transport.result.setException(Status.fromCode(code).asRuntimeException());
        assertGrpc(code, results.get(0));
        assertGrpc(code, results.get(1));
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void cancellingOneItemDoesNotCancelSiblingButAllCancelSharedRpc() throws Exception {
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        results.get(0).cancel(true);
        Assertions.assertFalse(transport.result.isCancelled());
        results.get(1).cancel(true);
        Assertions.assertTrue(transport.result.isCancelled());
    }

    @Test
    void cancellingSingletonCancelsUnaryRpc() throws Exception {
        Config.point_query_rpc_batch_max_size = 1;
        SettableFuture<PTabletKeyLookupResponse> rpc = SettableFuture.create();
        Mockito.when(transport.client.fetchTabletDataAsync(Mockito.any(PTabletKeyLookupRequest.class)))
                .thenReturn(rpc);
        ListenableFuture<PTabletKeyLookupResponse> result = submit(1);
        result.cancel(true);
        Assertions.assertTrue(rpc.isCancelled());
    }

    @Test
    void cancellationBeforeDispatchRemovesItem() throws Exception {
        Config.point_query_rpc_batch_max_size = 3;
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        second.cancel(true);
        ListenableFuture<PTabletKeyLookupResponse> third = submit(3);
        Assertions.assertEquals(2, transport.batches.get(0).getItemsCount());
        Assertions.assertEquals(3, transport.batches.get(0).getItems(1).getRequest().getTabletId());
        transport.result.set(okBatch().addResults(row(1)).addResults(row(3)).build());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(3), third.get());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void expiredBudgetIsNotRenewedByResponseOrFallback(boolean fallback) throws Exception {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
        batcher = new PointQueryRpcBatcher(ignored -> transport.client, clock::get, MoreExecutors.directExecutor());
        keepUnaryInFlight();
        assertGrpc(Status.Code.DEADLINE_EXCEEDED, batcher.submit(address, request(0), 0));
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        clock.addAndGet(TimeUnit.SECONDS.toNanos(30));
        if (fallback) {
            transport.result.setException(Status.RESOURCE_EXHAUSTED.asRuntimeException());
        } else {
            transport.result.set(okBatch().addResults(row(1)).addResults(row(2)).build());
        }
        for (ListenableFuture<?> result : results) {
            assertGrpc(Status.Code.DEADLINE_EXCEEDED, result);
        }
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void clientResolutionFailureCompletesEveryItem() throws Exception {
        AtomicInteger resolutions = new AtomicInteger();
        batcher = new PointQueryRpcBatcher(ignored -> {
            if (resolutions.getAndIncrement() == 0) {
                return transport.client;
            }
            throw new java.net.UnknownHostException("unresolvable backend");
        }, MoreExecutors.directExecutor());
        keepUnaryInFlight();
        List<ListenableFuture<PTabletKeyLookupResponse>> results = pair();
        for (ListenableFuture<?> result : results) {
            ExecutionException failure = Assertions.assertThrows(ExecutionException.class,
                    () -> result.get(5, TimeUnit.SECONDS));
            Assertions.assertInstanceOf(java.net.UnknownHostException.class, failure.getCause());
        }
    }

    @Test
    void concurrentProducersSendEveryRequestExactlyOnceWithinHardCap() throws Exception {
        Config.point_query_rpc_batch_max_size = 100;
        Config.point_query_rpc_batch_max_wait_us = 1000;
        transport.echo = true;
        CyclicBarrier start = new CyclicBarrier(32);
        List<CompletableFuture<Void>> done = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            final int id = i;
            CompletableFuture<Void> future = new CompletableFuture<>();
            done.add(future);
            Thread thread = new Thread(() -> {
                try {
                    start.await(10, TimeUnit.SECONDS);
                    for (int j = 0; j < 50; j++) {
                        long key = id * 50L + j;
                        Assertions.assertEquals(row(key), submit(key).get(10, TimeUnit.SECONDS));
                    }
                    future.complete(null);
                } catch (Throwable t) {
                    future.completeExceptionally(t);
                }
            });
            workers.add(thread);
            thread.start();
        }
        CompletableFuture.allOf(done.toArray(new CompletableFuture<?>[0])).get(30, TimeUnit.SECONDS);
        List<Long> ids = new ArrayList<>(transport.unaries);
        for (PTabletKeyLookupBatchRequest request : transport.batches) {
            Assertions.assertTrue(request.getItemsCount() <= 8);
            Assertions.assertTrue(request.getSerializedSize() <= PointQueryRpcBatcher.MAX_BYTES);
            request.getItemsList().forEach(item -> ids.add(item.getRequest().getTabletId()));
        }
        Assertions.assertEquals(1600, ids.size());
        Assertions.assertEquals(1600, ids.stream().distinct().count());
        Assertions.assertFalse(transport.batches.isEmpty());
    }

    private List<ListenableFuture<PTabletKeyLookupResponse>> pair() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        return java.util.Arrays.asList(first.get(5, TimeUnit.SECONDS), second);
    }

    private void keepUnaryInFlight() {
        // Coalescing tests model a busy BE; a solitary lookup intentionally skips the wait.
        Assertions.assertFalse(batcher.submit(address, request(Long.MIN_VALUE), 120_000).isDone());
    }

    private CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> owner(PTabletKeyLookupRequest request) {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> result = new CompletableFuture<>();
        Thread thread = new Thread(() -> {
            try {
                result.complete(batcher.submit(address, request, 20_000));
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        });
        workers.add(thread);
        thread.start();
        // Observe the actual producer park, rather than guessing when it has enqueued its request.
        Awaitility.await().pollInterval(Duration.ofMillis(1)).atMost(Duration.ofSeconds(5))
                .until(() -> thread.getState() == Thread.State.TIMED_WAITING);
        return result;
    }

    private ListenableFuture<PTabletKeyLookupResponse> submit(long id) {
        return batcher.submit(address, request(id), 20_000);
    }

    private static PTabletKeyLookupRequest request(long id) {
        return PTabletKeyLookupRequest.newBuilder().setTabletId(id).build();
    }

    private static PTabletKeyLookupResponse row(long id) {
        return PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0))
                .setRowBatch(ByteString.copyFromUtf8(Long.toString(id))).build();
    }

    private static PTabletKeyLookupBatchResponse.Builder okBatch() {
        return PTabletKeyLookupBatchResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0));
    }

    private static void assertGrpc(Status.Code code, ListenableFuture<?> result) {
        ExecutionException failure = Assertions.assertThrows(ExecutionException.class, () -> result.get(5, TimeUnit.SECONDS));
        Assertions.assertEquals(code, Status.fromThrowable(failure).getCode());
    }

    private static class FakeTransport {
        private final BackendServiceClient client = Mockito.mock(BackendServiceClient.class);
        private final List<PTabletKeyLookupBatchRequest> batches = java.util.Collections.synchronizedList(new ArrayList<>());
        private final List<Long> unaries = java.util.Collections.synchronizedList(new ArrayList<>());
        private SettableFuture<PTabletKeyLookupBatchResponse> result = SettableFuture.create();
        private final SettableFuture<PTabletKeyLookupResponse> pendingUnary = SettableFuture.create();
        private boolean echo;
        private boolean failNextUnary;

        private FakeTransport() {
            Mockito.when(client.fetchTabletDataAsync(Mockito.any(PTabletKeyLookupRequest.class)))
                    .thenAnswer(call -> unary(call.getArgument(0)));
            Mockito.when(client.fetchTabletDataBatchAsync(
                    Mockito.any(PTabletKeyLookupBatchRequest.class), Mockito.anyLong()))
                    .thenAnswer(call -> batch(call.getArgument(0), call.getArgument(1)));
        }

        private ListenableFuture<PTabletKeyLookupResponse> unary(PTabletKeyLookupRequest request) {
            if (request.getTabletId() == Long.MIN_VALUE) {
                return pendingUnary;
            }
            if (failNextUnary) {
                failNextUnary = false;
                throw Status.UNAVAILABLE.asRuntimeException();
            }
            unaries.add(request.getTabletId());
            return Futures.immediateFuture(row(request.getTabletId()));
        }

        private ListenableFuture<PTabletKeyLookupBatchResponse> batch(
                PTabletKeyLookupBatchRequest request, long timeoutMs) {
            Assertions.assertTrue(timeoutMs > 0);
            batches.add(request);
            if (echo) {
                PTabletKeyLookupBatchResponse.Builder response = okBatch();
                request.getItemsList().forEach(item -> response.addResults(row(item.getRequest().getTabletId())));
                return Futures.immediateFuture(response.build());
            }
            return result;
        }
    }
}
