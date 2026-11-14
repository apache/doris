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

import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.common.Status;
import org.apache.doris.common.UserException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanContext;
import org.apache.doris.proto.InternalService;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TResultBatch;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

public class PointQueryRpcExecutorTest {
    @Test
    public void testRowBatchDecodingMatchesThriftIncludingMetadata() throws Exception {
        byte[] large = new byte[256 * 1024];
        for (int i = 0; i < large.length; i++) {
            large[i] = (byte) i;
        }
        TResultBatch original = new TResultBatch(Arrays.asList(ByteBuffer.wrap(new byte[] {0, -1, 7}),
                ByteBuffer.wrap(new byte[0]), ByteBuffer.wrap(large)), false, 123456789L);
        original.setAttachedInfos(Collections.singletonMap("information", "中文\u0000value"));
        byte[] wire = new TSerializer().serialize(original);
        ByteString contiguous = ByteString.copyFrom(wire);
        ByteString segmented = ByteString.copyFrom(Arrays.asList(
                contiguous.substring(0, 4096), contiguous.substring(4096, 10000),
                contiguous.substring(10000)));
        for (ByteString bytes : Arrays.asList(contiguous, segmented)) {
            TResultBatch actual = PointQueryExecutor.deserializeRowBatch(bytes, wire.length);
            Assertions.assertEquals(original, actual);
            actual.getRows().get(0).get();
            Assertions.assertEquals(original, PointQueryExecutor.deserializeRowBatch(bytes, wire.length));
        }
    }

    @Test
    public void testRowBatchLimitsMalformedAndNextRequest() throws Exception {
        TResultBatch original = new TResultBatch(Collections.singletonList(ByteBuffer.wrap(new byte[] {1, 2})),
                true, Long.MAX_VALUE);
        byte[] wire = new TSerializer().serialize(original);
        ByteString bytes = ByteString.copyFrom(wire);
        Assertions.assertThrows(TException.class, () -> PointQueryExecutor.deserializeRowBatch(bytes, wire.length - 1));
        Assertions.assertThrows(TException.class,
                () -> PointQueryExecutor.deserializeRowBatch(bytes.substring(0, wire.length - 1), wire.length));
        Assertions.assertEquals(original, PointQueryExecutor.deserializeRowBatch(bytes, wire.length));
        TResultBatch empty = new TResultBatch(Collections.emptyList(), false, 0);
        Assertions.assertEquals(empty, PointQueryExecutor.deserializeRowBatch(
                ByteString.copyFrom(new TSerializer().serialize(empty)), 1024));
    }

    @Test
    public void testRouteUsesCurrentKeyOnEachExecution() throws Exception {
        Column key = new Column("product_id", PrimitiveType.BIGINT);
        CountingOlapScanNode scanNode = new CountingOlapScanNode();
        scanNode.addConjunct(equalityPredicate(key.getName()));
        PointQueryExecutor executor = new PointQueryExecutor(newShortCircuitQueryContext(scanNode), 1024);

        scanNode.getConjuncts().get(0).setChild(1, new IntLiteral(1));
        Assertions.assertTrue(executor.getNext().isEos());
        scanNode.getConjuncts().get(0).setChild(1, new IntLiteral(2));
        Assertions.assertTrue(executor.getNext().isEos());

        Assertions.assertEquals(Arrays.asList("1", "2"), scanNode.observedKeys);
        Assertions.assertEquals(2, scanNode.routeEvaluationTimes);
    }

    @Test
    public void testEmptyRouteClearsPreviousTabletAndBackends() throws Exception {
        CountingOlapScanNode scanNode = new CountingOlapScanNode();
        PointQueryExecutor executor = new PointQueryExecutor(newShortCircuitQueryContext(scanNode), 1024);
        Deencapsulation.setField(executor, "tabletID", 123L);
        Deencapsulation.setField(executor, "candidateBackends",
                new ArrayList<>(Collections.singletonList(new Backend(1, "127.0.0.1", 9060))));

        executor.selectPointQueryRoute();

        Assertions.assertEquals(0L, (long) Deencapsulation.getField(executor, "tabletID"));
        Assertions.assertNull(Deencapsulation.getField(executor, "candidateBackends"));
    }

    @Test
    public void testLocalFailuresDoNotBlacklistBackend() {
        Backend backend = new Backend(1, "127.0.0.1", 9060);
        try (MockedStatic<SimpleScheduler> scheduler = Mockito.mockStatic(SimpleScheduler.class)) {
            Throwable[] errors = {io.grpc.Status.DEADLINE_EXCEEDED.asRuntimeException(),
                    io.grpc.Status.CANCELLED.asRuntimeException(),
                    new RejectedExecutionException("fallback pool full")};
            TStatusCode[] codes = {TStatusCode.TIMEOUT, TStatusCode.CANCELLED, TStatusCode.INTERNAL_ERROR};
            for (int i = 0; i < errors.length; i++) {
                Status status = new Status();
                PointQueryExecutor.recordFailure(status, backend, new ExecutionException(errors[i]));
                Assertions.assertEquals(codes[i], status.getErrorCode());
            }
            scheduler.verifyNoInteractions();
        }
    }

    @Test
    public void testExpiredBudgetDoesNotSubmitAnotherRpc() throws Exception {
        PointQueryExecutor executor = Mockito.spy(new PointQueryExecutor(
                Mockito.mock(ShortCircuitQueryContext.class), 1024));
        lookup(executor, 0);
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class)) {
            Assertions.assertThrows(UserException.class, executor::getNext);
            proxies.verifyNoInteractions();
        }
    }

    @Test
    public void testWaitingTimeoutCancelsUnaryRpc() throws Exception {
        SettableFuture<InternalService.PTabletKeyLookupResponse> future = SettableFuture.create();
        PointQueryExecutor executor = Mockito.spy(new PointQueryExecutor(
                Mockito.mock(ShortCircuitQueryContext.class), 1024));
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class);
                MockedStatic<SimpleScheduler> scheduler = Mockito.mockStatic(SimpleScheduler.class)) {
            proxies.when(() -> BackendServiceProxy.fetchTabletDataAsync(
                    Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(future);
            lookup(executor, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200));
            Assertions.assertThrows(UserException.class, executor::getNext);
            Assertions.assertTrue(future.isCancelled());
            scheduler.verifyNoInteractions();
        }
    }

    @Test
    public void testContextResendSharesAttemptDeadlineAndFullResendFails() throws Exception {
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(new ConnectContext())) {
            context.call();
            context.connectContext.setCommand(org.apache.doris.mysql.MysqlCommand.COM_STMT_EXECUTE);
            PointQueryExecutor executor = Mockito.spy(new PointQueryExecutor(
                    newShortCircuitQueryContext(new CountingOlapScanNode()), 1024));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            PointQueryExecutor.Lookup lookup = lookup(executor, deadline);
            InternalService.PTabletKeyLookupResponse miss = InternalService.PTabletKeyLookupResponse.newBuilder()
                    .setStatus(org.apache.doris.proto.Types.PStatus.newBuilder().setStatusCode(0))
                    .setNeedResendQueryContext(true).build();
            lookup.accept(miss, null);
            Assertions.assertFalse(lookup.done());
            Assertions.assertEquals(deadline, lookup.deadlineNs());
            InternalService.PTabletKeyLookupRequest full = Deencapsulation.getField(lookup, "request");
            Assertions.assertTrue(full.hasDescTbl());
            lookup.accept(miss, null);
            Assertions.assertTrue(lookup.done());
            Assertions.assertThrows(UserException.class, lookup::result);
        }
    }

    @Test
    public void testNewReplicaGetsFreshBudget() throws Exception {
        try (AutoCloseConnectContext context = new AutoCloseConnectContext(new ConnectContext())) {
            context.call();
            PointQueryExecutor executor = Mockito.spy(new PointQueryExecutor(
                    newShortCircuitQueryContext(new CountingOlapScanNode()), 1024));
            executor.setTimeout(10000);
            PointQueryExecutor.Lookup lookup = lookup(executor, System.nanoTime() - 1);
            Deencapsulation.setField(executor, "candidateBackends", Arrays.asList(
                    new Backend(1, "127.0.0.1", 9060), new Backend(2, "127.0.0.2", 9060)));
            lookup.accept(null, new java.util.concurrent.TimeoutException());
            Assertions.assertFalse(lookup.done());
            Assertions.assertEquals(1, (int) Deencapsulation.getField(lookup, "attempt"));
            Assertions.assertTrue(lookup.deadlineNs() - System.nanoTime() > TimeUnit.SECONDS.toNanos(5));
        }
    }

    @Test
    public void testLookupCancelsActiveRpc() throws Exception {
        PointQueryExecutor executor = Mockito.spy(new PointQueryExecutor(
                Mockito.mock(ShortCircuitQueryContext.class), 1024));
        PointQueryExecutor.Lookup lookup = lookup(executor, System.nanoTime() + TimeUnit.SECONDS.toNanos(10));
        SettableFuture<InternalService.PTabletKeyLookupResponse> future = SettableFuture.create();
        try (MockedStatic<BackendServiceProxy> proxies = Mockito.mockStatic(BackendServiceProxy.class)) {
            proxies.when(() -> BackendServiceProxy.fetchTabletDataAsync(
                    Mockito.any(), Mockito.any(), Mockito.anyLong())).thenReturn(future);
            Assertions.assertSame(future, lookup.submit());
            executor.cancel();
            Assertions.assertTrue(future.isCancelled());
        }
    }

    @Test
    public void testQueryIdCancellationKeepsTheNextExecutorRunning() throws Exception {
        ConnectContext context = new ConnectContext();
        StmtExecutor old = new StmtExecutor(context, "select 1");
        StmtExecutor next = new StmtExecutor(context, "select 2");
        TUniqueId oldId = new TUniqueId(10, 20);
        Deencapsulation.setField(old, "executionQueryId", oldId);
        PointQueryExecutor point = new PointQueryExecutor(Mockito.mock(ShortCircuitQueryContext.class), 1024);
        SettableFuture<InternalService.PTabletKeyLookupResponse> active = SettableFuture.create();
        Deencapsulation.setField(point, "activeFuture", active);
        old.registerQueryCancellationCallback(point::cancel);
        Status reason = new Status(TStatusCode.CANCELLED, "kill by id");
        old.cancelQueryId(DebugUtil.printId(new TUniqueId(30, 40)), reason);
        Assertions.assertFalse(active.isCancelled());
        old.cancelQueryId(DebugUtil.printId(oldId), reason);
        Assertions.assertTrue(active.isCancelled());
        Assertions.assertNull(next.getPendingCancelReason());
        Assertions.assertSame(next, context.getExecutor());
    }

    private static PointQueryExecutor.Lookup lookup(PointQueryExecutor executor, long deadline) throws Exception {
        Backend backend = new Backend(1, "127.0.0.1", 9060);
        Deencapsulation.setField(executor, "candidateBackends", Collections.singletonList(backend));
        PointQueryExecutor.Lookup lookup = executor.new Lookup();
        Deencapsulation.setField(lookup, "backend", backend);
        Deencapsulation.setField(lookup, "deadlineNs", deadline);
        Deencapsulation.setField(lookup, "request", InternalService.PTabletKeyLookupRequest.newBuilder()
                .setTabletId(1).build());
        Mockito.doReturn(lookup).when(executor).prepareLookup();
        return lookup;
    }

    private static ShortCircuitQueryContext newShortCircuitQueryContext(OlapScanNode scanNode) throws Exception {
        ShortCircuitQueryContext context = Mockito.mock(ShortCircuitQueryContext.class, Mockito.CALLS_REAL_METHODS);
        Deencapsulation.setField(context, "serializedDescTable", ByteString.EMPTY);
        Deencapsulation.setField(context, "serializedOutputExpr", ByteString.EMPTY);
        Deencapsulation.setField(context, "serializedQueryOptions", ByteString.EMPTY);
        Deencapsulation.setField(context, "cacheID", UUID.randomUUID());
        Deencapsulation.setField(context, "tbl", scanNode.getOlapTable());
        Deencapsulation.setField(context, "tableName", "test_table");
        Deencapsulation.setField(context, "scanNode", scanNode);
        return context;
    }

    private static BinaryPredicate equalityPredicate(String columnName) {
        return new BinaryPredicate(BinaryPredicate.Operator.EQ,
                new SlotRef(null, columnName), new IntLiteral(0));
    }

    private static class CountingOlapScanNode extends OlapScanNode {
        private final ArrayList<Long> scanTabletIds = new ArrayList<>();
        private final ArrayList<String> observedKeys = new ArrayList<>();
        private int routeEvaluationTimes;

        CountingOlapScanNode() {
            super(new PlanNodeId(0), descriptor(), "TEST OLAP SCAN", ScanContext.EMPTY);
        }

        private static TupleDescriptor descriptor() {
            OlapTable table = Mockito.mock(OlapTable.class);
            Mockito.when(table.getName()).thenReturn("test_table");
            Mockito.when(table.getBaseSchemaKeyColumns()).thenReturn(Collections.emptyList());
            TupleDescriptor descriptor = new DescriptorTable().createTupleDescriptor();
            descriptor.setTable(table);
            return descriptor;
        }

        @Override
        public List<TScanRangeLocations> lazyEvaluateRangeLocations(List<Column> keyColumns) {
            routeEvaluationTimes++;
            if (!getConjuncts().isEmpty()) {
                observedKeys.add(getConjuncts().get(0).getChild(1).getStringValue());
            }
            return Collections.emptyList();
        }

        @Override
        public ArrayList<Long> getScanTabletIds() {
            return scanTabletIds;
        }
    }
}
