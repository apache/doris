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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.UserException;
import org.apache.doris.planner.OlapScanNode;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;
import org.apache.doris.tso.TSOTimestamp;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class WaitForChangeVisibleTest extends TestWithFeService {

    private static final long DB_ID = 100L;
    private static final long TABLE_ID = 200L;

    // -------- helpers --------

    private void invokeWait(ConnectContext ctx, List<ScanNode> scanNodes, TQueryGlobals globals)
            throws Throwable {
        Method m = Coordinator.class.getDeclaredMethod("waitForTimeBasedReadTransactionsVisible",
                ConnectContext.class, List.class, TQueryGlobals.class);
        m.setAccessible(true);
        try {
            m.invoke(null, ctx, scanNodes, globals);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
    }

    private ConnectContext mockContext(boolean eventual, long timeoutMs) {
        SessionVariable sv = Mockito.mock(SessionVariable.class);
        Mockito.when(sv.isEnableEventualConsistentChange()).thenReturn(eventual);
        Mockito.when(sv.getChangeVisibleTimeoutMs()).thenReturn(timeoutMs);
        ConnectContext ctx = Mockito.mock(ConnectContext.class);
        Mockito.when(ctx.getSessionVariable()).thenReturn(sv);
        return ctx;
    }

    private OlapScanNode mockChangeScan(long endTs) {
        Database db = Mockito.mock(Database.class);
        Mockito.when(db.getId()).thenReturn(DB_ID);
        OlapTable table = Mockito.mock(OlapTable.class);
        Mockito.when(table.getId()).thenReturn(TABLE_ID);
        Mockito.when(table.getDatabase()).thenReturn(db);

        OlapScanNode scan = Mockito.mock(OlapScanNode.class);
        Mockito.when(scan.hasChangeScan()).thenReturn(true);
        Mockito.when(scan.hasChangeEndTimestamp()).thenReturn(true);
        Mockito.when(scan.getChangeEndTimestamp()).thenReturn(endTs);
        Mockito.when(scan.getOlapTable()).thenReturn(table);
        return scan;
    }

    private TransactionState mockCommittedTxn(long commitPhysicalMs) {
        TransactionState txn = Mockito.mock(TransactionState.class);
        Mockito.when(txn.getTransactionStatus()).thenReturn(TransactionStatus.COMMITTED);
        Mockito.when(txn.getTableIdList()).thenReturn(Lists.newArrayList(TABLE_ID));
        Mockito.when(txn.getTransactionId()).thenReturn(1L);
        Mockito.when(txn.getCommitTSO()).thenReturn(TSOTimestamp.composeTimestamp(commitPhysicalMs, 0));
        return txn;
    }

    private TQueryGlobals globals(long timestampMs) {
        TQueryGlobals g = new TQueryGlobals();
        g.setTimestampMs(timestampMs);
        return g;
    }

    // -------- early-return branches (no txn mgr needed) --------

    @Test
    public void testNullContextReturns() throws Throwable {
        invokeWait(null, Collections.emptyList(), globals(System.currentTimeMillis()));
    }

    @Test
    public void testEventualConsistentReturns() throws Throwable {
        ConnectContext ctx = mockContext(true, 1000);
        invokeWait(ctx, Lists.newArrayList(mockChangeScan(System.currentTimeMillis())),
                globals(System.currentTimeMillis()));
    }

    @Test
    public void testNoChangeScanReturns() throws Throwable {
        ConnectContext ctx = mockContext(false, 1000);
        ScanNode plain = Mockito.mock(ScanNode.class); // not OlapScanNode
        invokeWait(ctx, Lists.newArrayList(plain), globals(System.currentTimeMillis()));
    }

    // -------- branches that touch the global transaction mgr --------

    @Test
    public void testTxnBecomesVisible() throws Throwable {
        long now = System.currentTimeMillis();
        ConnectContext ctx = mockContext(false, 5000);

        TransactionState txn = mockCommittedTxn(now - 1000); // commit <= endTs
        AtomicReference<TransactionStatus> status = new AtomicReference<>(TransactionStatus.COMMITTED);
        Mockito.when(txn.getTransactionStatus()).thenAnswer(i -> status.get());
        Mockito.doAnswer(i -> {
            status.set(TransactionStatus.VISIBLE);
            return null;
        }).when(txn).waitTransactionVisible(Mockito.anyLong());

        GlobalTransactionMgrIface mgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(mgr.getCommittedTransactions(DB_ID)).thenReturn(Lists.newArrayList(txn));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(mgr);
            invokeWait(ctx, Lists.newArrayList(mockChangeScan(now)), globals(now));
        }
        Mockito.verify(txn).waitTransactionVisible(Mockito.anyLong());
    }

    @Test
    public void testTxnCommittedAfterEndTsSkipped() throws Throwable {
        long now = System.currentTimeMillis();
        ConnectContext ctx = mockContext(false, 5000);

        TransactionState txn = mockCommittedTxn(now + 60_000); // commit > endTs -> skip
        GlobalTransactionMgrIface mgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(mgr.getCommittedTransactions(DB_ID)).thenReturn(Lists.newArrayList(txn));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(mgr);
            invokeWait(ctx, Lists.newArrayList(mockChangeScan(now)), globals(now));
        }
        Mockito.verify(txn, Mockito.never()).waitTransactionVisible(Mockito.anyLong());
    }

    @Test
    public void testTimeoutBeforeWaiting() throws Throwable {
        long now = System.currentTimeMillis();
        ConnectContext ctx = mockContext(false, 0); // deadline == now -> remaining <= 0

        TransactionState txn = mockCommittedTxn(now - 1000);
        GlobalTransactionMgrIface mgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(mgr.getCommittedTransactions(DB_ID)).thenReturn(Lists.newArrayList(txn));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(mgr);
            UserException e = Assertions.assertThrows(UserException.class,
                    () -> invokeWait(ctx, Lists.newArrayList(mockChangeScan(now)), globals(now)));
            Assertions.assertTrue(e.getMessage().contains("timeout waiting committed transactions"));
        }
    }

    @Test
    public void testTimeoutWhileWaitingStillCommitted() throws Throwable {
        long now = System.currentTimeMillis();
        ConnectContext ctx = mockContext(false, 100); // small timeout

        TransactionState txn = mockCommittedTxn(now - 1000);
        Mockito.when(txn.getTransactionStatus()).thenReturn(TransactionStatus.COMMITTED); // never visible
        Mockito.doAnswer(i -> {
            long ms = i.getArgument(0);
            Thread.sleep(Math.max(ms, 1)); // push past deadline in one wait
            return null;
        }).when(txn).waitTransactionVisible(Mockito.anyLong());

        GlobalTransactionMgrIface mgr = Mockito.mock(GlobalTransactionMgrIface.class);
        Mockito.when(mgr.getCommittedTransactions(DB_ID)).thenReturn(Lists.newArrayList(txn));

        try (MockedStatic<Env> mockedEnv = Mockito.mockStatic(Env.class, Mockito.CALLS_REAL_METHODS)) {
            mockedEnv.when(Env::getCurrentGlobalTransactionMgr).thenReturn(mgr);
            UserException e = Assertions.assertThrows(UserException.class,
                    () -> invokeWait(ctx, Lists.newArrayList(mockChangeScan(now)), globals(now)));
            Assertions.assertTrue(e.getMessage().contains("timeout waiting transaction become visible"));
        }
    }
}
