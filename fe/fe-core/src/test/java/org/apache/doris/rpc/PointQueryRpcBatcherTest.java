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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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
        batcher = new PointQueryRpcBatcher(transport, MoreExecutors.directExecutor());
    }

    @AfterEach
    void tearDown() throws Exception {
        for (Thread worker : workers) {
            worker.interrupt();
            worker.join(5000);
            Assertions.assertFalse(worker.isAlive());
        }
        Config.point_query_rpc_batch_max_size = savedSize;
        Config.point_query_rpc_batch_max_wait_us = savedWait;
        Config.point_query_rpc_batch_max_request_bytes = savedBytes;
    }

    @Test
    void fullBatchWakesOwnerAndMapsIndependentResults() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        Assertions.assertEquals(1, transport.batches.size());
        Assertions.assertEquals(2, transport.batches.get(0).getItemsCount());
        transport.result.set(okBatch().addResults(row(1)).addResults(PTabletKeyLookupResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(4).addErrorMsgs("item failure"))).build());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(4, second.get().getStatus().getStatusCode());
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void onlyFallbackIoUsesExecutor() throws Exception {
        List<Runnable> tasks = new ArrayList<>();
        batcher = new PointQueryRpcBatcher(transport, tasks::add);
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.set(okBatch().addResults(row(1)).addResults(row(2)).build());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(2), second.get());
        Assertions.assertTrue(tasks.isEmpty());

        transport.result = SettableFuture.create();
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> third = owner(request(3));
        ListenableFuture<PTabletKeyLookupResponse> fourth = submit(4);
        transport.result.setException(Status.UNIMPLEMENTED.asRuntimeException());
        Assertions.assertEquals(1, tasks.size());
        Assertions.assertTrue(transport.unaries.isEmpty());
        Assertions.assertFalse(fourth.isDone());
        tasks.get(0).run();
        Assertions.assertEquals(row(3), third.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(4), fourth.get());
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
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.set(okBatch().addResults(row(1)).build());
        assertGrpc(Status.Code.DATA_LOSS, first.get(5, TimeUnit.SECONDS));
        assertGrpc(Status.Code.DATA_LOSS, second);
    }

    @Test
    void outerAdmissionErrorIsReturnedToEveryItem() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.set(PTabletKeyLookupBatchResponse.newBuilder()
                .setStatus(PStatus.newBuilder().setStatusCode(4).addErrorMsgs("queue full")).build());
        Assertions.assertEquals(4, first.get(5, TimeUnit.SECONDS).get().getStatus().getStatusCode());
        Assertions.assertEquals(4, second.get().getStatus().getStatusCode());
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void unknownMethodFallsBackAndCooldownExpires() throws Exception {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
        batcher = new PointQueryRpcBatcher(transport, clock::get, MoreExecutors.directExecutor());
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.setException(Status.UNIMPLEMENTED.asRuntimeException());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(2), second.get());
        Assertions.assertEquals(row(3), submit(3).get());
        Assertions.assertEquals(1, transport.batches.size());
        clock.addAndGet(PointQueryRpcBatcher.UNARY_BACKOFF_NS + 1);
        transport.result = SettableFuture.create();
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> fourth = owner(request(4));
        ListenableFuture<PTabletKeyLookupResponse> fifth = submit(5);
        transport.result.set(okBatch().addResults(row(4)).addResults(row(5)).build());
        Assertions.assertEquals(row(4), fourth.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(5), fifth.get());
        Assertions.assertEquals(2, transport.batches.size());
    }

    @Test
    void oversizedBatchResponseFallsBackToIndividualRpcLimits() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.setException(Status.RESOURCE_EXHAUSTED.asRuntimeException());
        Assertions.assertEquals(row(1), first.get(5, TimeUnit.SECONDS).get());
        Assertions.assertEquals(row(2), second.get());
        Assertions.assertEquals(row(3), submit(3).get());
        Assertions.assertEquals(1, transport.batches.size());
    }

    @Test
    void ordinaryTransportFailureDoesNotReplay() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        transport.result.setException(Status.UNAVAILABLE.asRuntimeException());
        assertGrpc(Status.Code.UNAVAILABLE, first.get(5, TimeUnit.SECONDS));
        assertGrpc(Status.Code.UNAVAILABLE, second);
        Assertions.assertTrue(transport.unaries.isEmpty());
    }

    @Test
    void cancellingOneItemDoesNotCancelSiblingButAllCancelSharedRpc() throws Exception {
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        first.get(5, TimeUnit.SECONDS).cancel(true);
        Assertions.assertFalse(transport.result.isCancelled());
        second.cancel(true);
        Assertions.assertTrue(transport.result.isCancelled());
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

    @Test
    void expiredItemsAreNotSentAndLateResponsesDoNotSucceed() throws Exception {
        AtomicLong clock = new AtomicLong(TimeUnit.SECONDS.toNanos(1));
        batcher = new PointQueryRpcBatcher(transport, clock::get, MoreExecutors.directExecutor());
        assertGrpc(Status.Code.DEADLINE_EXCEEDED, batcher.submit(address, request(0), 0));
        CompletableFuture<ListenableFuture<PTabletKeyLookupResponse>> first = owner(request(1));
        ListenableFuture<PTabletKeyLookupResponse> second = submit(2);
        clock.addAndGet(TimeUnit.SECONDS.toNanos(30));
        transport.result.set(okBatch().addResults(row(1)).addResults(row(2)).build());
        assertGrpc(Status.Code.DEADLINE_EXCEEDED, first.get(5, TimeUnit.SECONDS));
        assertGrpc(Status.Code.DEADLINE_EXCEEDED, second);
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

    private static class FakeTransport implements PointQueryRpcBatcher.Transport {
        private final List<PTabletKeyLookupBatchRequest> batches = java.util.Collections.synchronizedList(new ArrayList<>());
        private final List<Long> unaries = java.util.Collections.synchronizedList(new ArrayList<>());
        private SettableFuture<PTabletKeyLookupBatchResponse> result = SettableFuture.create();
        private boolean echo;

        @Override
        public ListenableFuture<PTabletKeyLookupResponse> unary(
                TNetworkAddress address, PTabletKeyLookupRequest request, long timeoutMs) {
            Assertions.assertTrue(timeoutMs > 0);
            unaries.add(request.getTabletId());
            return Futures.immediateFuture(row(request.getTabletId()));
        }

        @Override
        public ListenableFuture<PTabletKeyLookupBatchResponse> batch(
                TNetworkAddress address, PTabletKeyLookupBatchRequest request, long timeoutMs) {
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
