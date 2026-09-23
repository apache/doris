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

package org.apache.doris.qe;

import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.LocalTablet;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.HashDistributionPruner;
import org.apache.doris.planner.PartitionColumnFilter;
import org.apache.doris.proto.InternalService.PTabletKeyLookupRequest;
import org.apache.doris.proto.InternalService.PTabletKeyLookupResponse;
import org.apache.doris.proto.Types.PStatus;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TStatusCode;

import com.google.protobuf.ByteString;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

class BatchPointQueryExecutorTest {
    private static final long VERSION = 12;

    private BatchPointQueryExecutor executor(int maxBytes, BatchPointQueryExecutor.LookupTransport transport) {
        return new BatchPointQueryExecutor(null, maxBytes, new ConnectContext(), transport);
    }

    private BatchPointQueryExecutor.TabletRequest task(long tablet) {
        return new BatchPointQueryExecutor.TabletRequest(PTabletKeyLookupRequest.newBuilder()
                .setTabletId(tablet).setSnapshotVersion(VERSION).build(),
                Arrays.asList(new Backend(1, "localhost", 9050), new Backend(2, "localhost", 9051)));
    }

    private PTabletKeyLookupResponse response(int value) throws Exception {
        TResultBatch result = new TResultBatch().setIsCompressed(false).setPacketSeq(0)
                .setRows(Collections.singletonList(ByteBuffer.wrap(new byte[] {(byte) value})));
        return PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0))
                .setSnapshotVersion(VERSION)
                .setRowBatch(ByteString.copyFrom(new TSerializer().serialize(result))).build();
    }

    private long deadline() {
        return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    }

    @Test
    void testHashRoutingDeduplicatesAndPreservesEveryKey() {
        Column keyColumn = new Column("address", Type.VARCHAR);
        List<Long> tablets = LongStream.range(1000, 1128).boxed().collect(Collectors.toList());
        List<LiteralExpr> keys = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            keys.add(new StringLiteral(String.format("0x%040x", i)));
        }
        keys.add(keys.get(0));
        Map<Long, List<LiteralExpr>> groups = BatchPointQueryExecutor.groupKeysByTablet(keys, keyColumn, tablets);
        Assertions.assertTrue(groups.size() > 1);
        Assertions.assertEquals(100, groups.values().stream().mapToInt(List::size).sum());
        MaterializedIndex index = new MaterializedIndex();
        index.appendTablets(tablets.stream().map(LocalTablet::new).collect(Collectors.toList()));
        groups.forEach((tablet, group) -> group.forEach(key -> {
            PartitionColumnFilter filter = new PartitionColumnFilter();
            filter.setLowerBound(key, true);
            filter.setUpperBound(key, true);
            Assertions.assertEquals(Collections.singletonList(tablet),
                    new ArrayList<>(new HashDistributionPruner(null, index, Collections.singletonList(keyColumn),
                            Collections.singletonMap("address", filter), 128, true).prune()));
        }));
    }

    @Test
    void testBoundedFanoutAndResultCollection() throws Exception {
        AtomicInteger outstanding = new AtomicInteger();
        AtomicInteger peak = new AtomicInteger();
        PTabletKeyLookupResponse response = response(7);
        BatchPointQueryExecutor executor = executor(1024 * 1024, (backend, request) -> {
            peak.accumulateAndGet(outstanding.incrementAndGet(), Math::max);
            return new CompletableFuture<PTabletKeyLookupResponse>() {
                @Override
                public PTabletKeyLookupResponse get(long timeout, TimeUnit unit) {
                    outstanding.decrementAndGet();
                    return response;
                }
            };
        });
        List<BatchPointQueryExecutor.TabletRequest> tasks = LongStream.range(0, 70)
                .mapToObj(this::task).collect(Collectors.toList());
        RowBatch result = executor.executeRequests(tasks, deadline());
        Assertions.assertEquals(70, result.getBatch().getRowsSize());
        Assertions.assertEquals(BatchPointQueryExecutor.MAX_CONCURRENT_REQUESTS, peak.get());
        Assertions.assertTrue(result.isEos());
    }

    @Test
    void testRpcFailureRetriesReplicaWithoutDuplicatingResults() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        PTabletKeyLookupResponse response = response(8);
        BatchPointQueryExecutor executor = executor(1024, (backend, request) -> {
            CompletableFuture<PTabletKeyLookupResponse> future = new CompletableFuture<>();
            if (sent.incrementAndGet() == 1) {
                future.completeExceptionally(new RuntimeException("connection reset"));
            } else {
                future.complete(response);
            }
            return future;
        });
        Assertions.assertEquals(1, executor.executeRequests(Collections.singletonList(task(1)), deadline())
                .getBatch().getRowsSize());
        Assertions.assertEquals(2, sent.get());
    }

    @Test
    void testFailureCancelsPendingAndDoesNotReturnPartialRows() throws Exception {
        List<CompletableFuture<PTabletKeyLookupResponse>> futures = new ArrayList<>();
        BatchPointQueryExecutor executor = executor(1024, (backend, request) -> {
            CompletableFuture<PTabletKeyLookupResponse> future = new CompletableFuture<>();
            futures.add(future);
            if (request.getTabletId() == 1) {
                future.complete(PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder()
                        .setStatusCode(5).addErrorMsgs("injected failure")).build());
            }
            return future;
        });
        Assertions.assertThrows(UserException.class,
                () -> executor.executeRequests(Arrays.asList(task(1), task(2)), deadline()));
        Assertions.assertTrue(futures.get(1).isCancelled());
    }

    @Test
    void testBusyReplicaResponseRetriesWithoutDuplicatingRows() throws Exception {
        AtomicInteger sent = new AtomicInteger();
        PTabletKeyLookupResponse success = response(8);
        BatchPointQueryExecutor executor = executor(1024, (backend, request) -> CompletableFuture.completedFuture(
                sent.incrementAndGet() == 1 ? PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder()
                        .setStatusCode(TStatusCode.CANCELLED.getValue()).addErrorMsgs("work queue full")).build()
                        : success));
        Assertions.assertEquals(1, executor.executeRequests(Collections.singletonList(task(1)), deadline())
                .getBatch().getRowsSize());
        Assertions.assertEquals(2, sent.get());
    }

    @Test
    void testOldBackendCannotReturnUnversionedResult() {
        BatchPointQueryExecutor executor = executor(1024, (backend, request) -> CompletableFuture.completedFuture(
                PTabletKeyLookupResponse.newBuilder().setStatus(PStatus.newBuilder().setStatusCode(0))
                        .setEmptyBatch(true).build()));
        Assertions.assertThrows(UserException.class,
                () -> executor.executeRequests(Collections.singletonList(task(1)), deadline()));
    }

    @Test
    void testAggregateResponseBudget() throws Exception {
        PTabletKeyLookupResponse response = response(9);
        BatchPointQueryExecutor executor = executor(response.getRowBatch().size(),
                (backend, request) -> CompletableFuture.completedFuture(response));
        Assertions.assertThrows(UserException.class,
                () -> executor.executeRequests(Arrays.asList(task(1), task(2)), deadline()));
    }

    @Test
    void testCancellationCancelsOutstandingRpc() throws Exception {
        List<CompletableFuture<PTabletKeyLookupResponse>> futures = new ArrayList<>();
        BatchPointQueryExecutor[] executor = new BatchPointQueryExecutor[1];
        executor[0] = executor(1024, (backend, request) -> {
            CompletableFuture<PTabletKeyLookupResponse> future = new CompletableFuture<PTabletKeyLookupResponse>() {
                @Override
                public PTabletKeyLookupResponse get(long timeout, TimeUnit unit)
                        throws InterruptedException, ExecutionException, TimeoutException {
                    executor[0].cancel(Status.CANCELLED);
                    return super.get(timeout, unit);
                }
            };
            futures.add(future);
            return future;
        });
        Assertions.assertThrows(UserException.class,
                () -> executor[0].executeRequests(Arrays.asList(task(1), task(2)), deadline()));
        Assertions.assertTrue(futures.stream().allMatch(CompletableFuture::isCancelled));
    }

    @Test
    void testExpiredDeadlineDoesNotIssueRpc() {
        AtomicInteger sent = new AtomicInteger();
        BatchPointQueryExecutor executor = executor(1024, (backend, request) -> {
            sent.incrementAndGet();
            return new CompletableFuture<>();
        });
        Assertions.assertThrows(UserException.class,
                () -> executor.executeRequests(Collections.singletonList(task(1)), System.nanoTime() - 1));
        Assertions.assertEquals(0, sent.get());
    }
}
