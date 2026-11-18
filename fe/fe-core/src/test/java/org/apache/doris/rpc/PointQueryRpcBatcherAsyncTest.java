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
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

class PointQueryRpcBatcherAsyncTest {
    private final TNetworkAddress address = new TNetworkAddress("127.0.0.1", 8060);
    private final BackendServiceClient client = Mockito.mock(BackendServiceClient.class);
    private final SettableFuture<PTabletKeyLookupResponse> firstRpc = SettableFuture.create();
    private final Map<Long, AtomicInteger> dispatched = new ConcurrentHashMap<>();
    private PointQueryRpcBatcher batcher;
    private boolean oldAsync;
    private int oldWait;
    private int oldSize;
    private int oldBytes;

    @BeforeEach
    void setUp() {
        oldAsync = Config.enable_point_query_rpc_async_flush;
        oldWait = Config.point_query_rpc_batch_max_wait_us;
        oldSize = Config.point_query_rpc_batch_max_size;
        oldBytes = Config.point_query_rpc_batch_max_request_bytes;
        Config.enable_point_query_rpc_async_flush = true;
        Config.point_query_rpc_batch_max_wait_us = 5_000_000;
        Config.point_query_rpc_batch_max_size = 4;
        Config.point_query_rpc_batch_max_request_bytes = PointQueryRpcBatcher.MAX_BYTES;
        Mockito.when(client.fetchTabletDataAsync(Mockito.any())).thenAnswer(call -> {
            PTabletKeyLookupRequest request = call.getArgument(0);
            dispatched.computeIfAbsent(request.getTabletId(), key -> new AtomicInteger()).incrementAndGet();
            return request.getTabletId() == 0 ? firstRpc : Futures.immediateFuture(response(request.getTabletId()));
        });
        Mockito.when(client.fetchTabletDataBatchAsync(Mockito.any(), Mockito.anyLong())).thenAnswer(call -> {
            org.apache.doris.proto.InternalService.PTabletKeyLookupBatchRequest request = call.getArgument(0);
            PTabletKeyLookupBatchResponse.Builder result = PTabletKeyLookupBatchResponse.newBuilder()
                    .setStatus(PStatus.newBuilder().setStatusCode(0));
            request.getItemsList().forEach(item -> {
                long id = item.getRequest().getTabletId();
                dispatched.computeIfAbsent(id, key -> new AtomicInteger()).incrementAndGet();
                result.addResults(response(id));
            });
            return Futures.immediateFuture(result.build());
        });
        batcher = new PointQueryRpcBatcher(ignored -> client, MoreExecutors.directExecutor());
        batcher.submit(address, request(0), 30000);
    }

    @AfterEach
    void tearDown() {
        batcher.close();
        Config.enable_point_query_rpc_async_flush = oldAsync;
        Config.point_query_rpc_batch_max_wait_us = oldWait;
        Config.point_query_rpc_batch_max_size = oldSize;
        Config.point_query_rpc_batch_max_request_bytes = oldBytes;
    }

    private static PTabletKeyLookupRequest request(long id) {
        return PTabletKeyLookupRequest.newBuilder().setTabletId(id).build();
    }

    private static PTabletKeyLookupResponse response(long id) {
        return PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0))
                .setRowBatch(ByteString.copyFromUtf8(Long.toString(id))).build();
    }

    @Test
    void submitReturnsWithoutOwnerParkAndShutdownCompletesPendingItem() {
        ListenableFuture<PTabletKeyLookupResponse> pending = Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(1), () -> batcher.submit(address, request(1), 30000));
        Assertions.assertFalse(pending.isDone());
        batcher.close();
        Assertions.assertTrue(pending.isDone());
        Assertions.assertTrue(firstRpc.isCancelled());
        Assertions.assertTrue(batcher.submit(address, request(2), 30000).isDone());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void flushUsesProducerOrDispatcherInsteadOfTimer(boolean timer) throws Exception {
        Config.point_query_rpc_batch_max_size = 2;
        Config.point_query_rpc_batch_max_wait_us = timer ? 1000 : 5_000_000;
        AtomicReference<Thread> submitting = new AtomicReference<>();
        try (PointQueryRpcBatcher checked = new PointQueryRpcBatcher(ignored -> {
            submitting.set(Thread.currentThread());
            return client;
        }, MoreExecutors.directExecutor())) {
            checked.submit(address, request(0), 30000);
            ListenableFuture<PTabletKeyLookupResponse> first = checked.submit(address, request(1), 30000);
            if (!timer) {
                checked.submit(address, request(2), 30000).get(5, TimeUnit.SECONDS);
            }
            Assertions.assertEquals(response(1), first.get(5, TimeUnit.SECONDS));
            if (timer) {
                Assertions.assertTrue(submitting.get().getName().startsWith("point-query-dispatch-"));
            } else {
                Assertions.assertSame(Thread.currentThread(), submitting.get());
            }
        }
    }

    @Test
    void fullBatchAndTimerRacesDispatchEveryItemExactlyOnce() throws Exception {
        Config.point_query_rpc_batch_max_wait_us = 1000;
        ExecutorService producers = Executors.newFixedThreadPool(8);
        try {
            List<Future<ListenableFuture<PTabletKeyLookupResponse>>> items = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                final int id = i;
                items.add(producers.submit(() -> batcher.submit(address, request(id), 30000)));
            }
            for (int i = 0; i < items.size(); i++) {
                Assertions.assertEquals(Integer.toString(i + 1), items.get(i).get(5, TimeUnit.SECONDS)
                        .get(5, TimeUnit.SECONDS).getRowBatch().toStringUtf8());
                Assertions.assertEquals(1, dispatched.get((long) i + 1).get());
            }
        } finally {
            producers.shutdownNow();
        }
    }

    @Test
    void byteOverflowDetachesOnlyExpectedBatch() throws Exception {
        byte[] payload = new byte[600 * 1024];
        PTabletKeyLookupRequest large = request(1).toBuilder().setDescTbl(ByteString.copyFrom(payload)).build();
        ListenableFuture<PTabletKeyLookupResponse> first = batcher.submit(address, large, 30000);
        ListenableFuture<PTabletKeyLookupResponse> next = batcher.submit(address,
                large.toBuilder().setTabletId(2).build(), 30000);
        Assertions.assertEquals("1", first.get(5, TimeUnit.SECONDS).getRowBatch().toStringUtf8());
        Assertions.assertFalse(next.isDone());
        batcher.close();
        Assertions.assertTrue(next.isDone());
        Assertions.assertEquals(1, dispatched.get(1L).get());
    }

    @Test
    void cancellingOnePendingItemDoesNotCancelOtherItems() throws Exception {
        Config.point_query_rpc_batch_max_size = 2;
        ListenableFuture<PTabletKeyLookupResponse> cancelled = batcher.submit(address, request(1), 30000);
        cancelled.cancel(true);
        ListenableFuture<PTabletKeyLookupResponse> live = batcher.submit(address, request(2), 30000);
        Assertions.assertEquals("2", live.get(5, TimeUnit.SECONDS).getRowBatch().toStringUtf8());
        Assertions.assertTrue(cancelled.isCancelled());
        Assertions.assertFalse(firstRpc.isCancelled());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void schedulerRejectionCompletesItemWithoutCallerRuns(boolean timer) throws Exception {
        Config.point_query_rpc_batch_max_size = 2;
        ListenableFuture<PTabletKeyLookupResponse> first = batcher.submit(address, request(1), 30000);
        batcher.submit(address, request(2), 30000).get(5, TimeUnit.SECONDS);
        first.get(5, TimeUnit.SECONDS);
        Object scheduler = Deencapsulation.getField(batcher, "flushScheduler");
        if (timer) {
            ScheduledThreadPoolExecutor[] timers = Deencapsulation.getField(scheduler, "timers");
            for (ScheduledThreadPoolExecutor executor : timers) {
                executor.shutdownNow();
            }
        } else {
            ThreadPoolExecutor dispatchers = Deencapsulation.getField(scheduler, "dispatchers");
            dispatchers.shutdown();
        }
        Config.point_query_rpc_batch_max_wait_us = 1000;
        ListenableFuture<PTabletKeyLookupResponse> rejected = batcher.submit(address, request(3), 30000);
        Assertions.assertThrows(java.util.concurrent.ExecutionException.class,
                () -> rejected.get(5, TimeUnit.SECONDS));
        Assertions.assertFalse(dispatched.containsKey(3L));
    }

    @Test
    void shutdownRacingTimerNeverLeavesAnUnfinishedFuture() throws Exception {
        Config.point_query_rpc_batch_max_wait_us = 1000;
        List<ListenableFuture<PTabletKeyLookupResponse>> items = new ArrayList<>();
        for (int i = 1; i <= 50; i++) {
            items.add(batcher.submit(address, request(i), 30000));
        }
        Thread shutdown = new Thread(batcher::close);
        shutdown.start();
        shutdown.join(5000);
        Assertions.assertFalse(shutdown.isAlive());
        Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> items.stream().allMatch(Future::isDone));
    }
}
