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

package org.apache.doris.tso;

import org.apache.doris.cloud.proto.Cloud.CheckTxnConflictResponse;
import org.apache.doris.cloud.proto.Cloud.TxnInfoPB;
import org.apache.doris.cloud.proto.Cloud.TxnStatusPB;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.protobuf.ByteString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TSOTransactionTrackerTest {
    private final ReentrantLock lock = new ReentrantLock();
    private final TSOTransactionTracker tracker = new TSOTransactionTracker(lock);
    private final GlobalTransactionMgrIface txnMgr = Mockito.mock(GlobalTransactionMgrIface.class);

    private void reset(long delayMs) {
        lock.lock();
        try {
            tracker.reset(0, delayMs);
        } finally {
            lock.unlock();
        }
    }

    private void register(long dbId, long txnId, long tso) {
        lock.lock();
        try {
            tracker.register(Pair.of(dbId, txnId), tso, 0, Collections.singleton(100L));
        } finally {
            lock.unlock();
        }
    }

    private long candidate(long currentTso, long durableTso) {
        lock.lock();
        try {
            return tracker.candidateCommittedTso(currentTso, durableTso);
        } finally {
            lock.unlock();
        }
    }

    static CheckTxnConflictResponse recoveryBatch(ByteString nextKey, TxnInfoPB... transactions) {
        return CheckTxnConflictResponse.newBuilder().setStrictRecoveryCheckApplied(true)
                .setRecoveryBatchApplied(true).setNextRecoveryKey(nextKey)
                .addAllConflictTxns(Arrays.asList(transactions)).build();
    }

    private static TxnInfoPB recoveryTxn(long dbId, long txnId, long tso, long... tables) {
        TxnInfoPB.Builder info = TxnInfoPB.newBuilder().setDbId(dbId).setTxnId(txnId)
                .setStatus(TxnStatusPB.TXN_STATUS_PREPARED);
        for (long table : tables) {
            info.addTableIds(table);
        }
        if (tso > 0) {
            info.setCommitTso(tso).setStatus(TxnStatusPB.TXN_STATUS_COMMITTED);
        }
        return info.build();
    }

    private void finishRecovery() throws Exception {
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.doReturn(recoveryBatch(ByteString.EMPTY)).when(txnMgr)
                .getTsoRecoveryTransactions(1000L, ByteString.EMPTY);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
    }

    private TSOTransactionTracker.WaitResult awaitTable(long dbId, long tableId, long endTso) throws Exception {
        lock.lock();
        try {
            return tracker.awaitTransactions(Collections.singletonMap(dbId, Collections.singletonList(tableId)),
                    endTso, 0);
        } finally {
            lock.unlock();
        }
    }

    @Test
    public void testReadWaitFiltersDatabaseTableAndExclusivePhysicalEnd() throws Exception {
        reset(0);
        finishRecovery();
        register(1, 10, TSOTimestamp.composeTimestamp(100, 1));
        long end = TSOTimestamp.composePhysicalTimestamp(101);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(2, 100, end));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 200, end));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED,
                awaitTable(1, 100, TSOTimestamp.composePhysicalTimestamp(100)));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 100, end));
        // Retrying later must not hide the original in-window request or lose any involved table.
        lock.lock();
        try {
            tracker.register(Pair.of(1L, 10L), TSOTimestamp.composeTimestamp(200, 1), 0, Set.of(100L, 200L));
        } finally {
            lock.unlock();
        }
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 100, end));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 200, end));
        tracker.transactionFinished(1, 10);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 200, end));
    }

    @Test
    public void testEmptyRegistrationSetCannotBypassRecovery() throws Exception {
        reset(2000);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.RECOVERING, awaitTable(1, 100, 200));
    }

    @Test
    public void testLoadedRecoveryWaitsOnlyRelatedTablesAndKeepsPrefixFrozen() throws Exception {
        reset(2000);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(ByteString.EMPTY,
                        recoveryTxn(1, 10, 100, 100), recoveryTxn(1, 20, 0, 200)));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(2));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 300, 200));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(2, 100, 200));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 100, 90));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 100, 200));
        // No persisted TSO is not proof that an old in-flight commit lies outside this window.
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 200, 90));
        Assertions.assertEquals(80, candidate(250, 80));
        tracker.transactionFinished(1, 10);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 100, 200));
        Assertions.assertEquals(80, candidate(250, 80));
        tracker.transactionFinished(1, 20);
        Assertions.assertTrue(tracker.isRecoveryReady());
        Assertions.assertEquals(250, candidate(250, 80));
    }

    @Test
    public void testFailedBatchResumesWithoutOpeningAnIncompleteRecovery() throws Exception {
        reset(0);
        ByteString nextKey = ByteString.copyFromUtf8("next batch");
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(nextKey, recoveryTxn(1, 10, 100, 100)));
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, nextKey))
                .thenThrow(new UserException("batch RPC failed"))
                .thenReturn(recoveryBatch(ByteString.EMPTY, recoveryTxn(2, 20, 0, 200)));
        Assertions.assertThrows(UserException.class,
                () -> tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3)));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.RECOVERING, awaitTable(1, 300, 200));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 300, 200));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 100, 200));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(2, 200, 200));
        Assertions.assertEquals(80, candidate(250, 80));
        Mockito.verify(txnMgr).getTransactionIdWatermark();
        Mockito.verify(txnMgr).getTsoRecoveryTransactions(1000L, ByteString.EMPTY);
    }

    @Test
    public void testRecoveryBatchesMergeConcurrentRegistrationsAndFinishNotifications() throws Exception {
        reset(0);
        ByteString nextKey = ByteString.copyFromUtf8("next batch");
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY)).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isHeldByCurrentThread());
            register(1, 10, 200); // Same old transaction is retried through the new master.
            return recoveryBatch(nextKey, recoveryTxn(1, 10, 0, 200));
        });
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, nextKey)).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isHeldByCurrentThread());
            tracker.transactionFinished(1, 10);
            register(2, 2000, 150); // New transactions must survive importing the old list.
            return recoveryBatch(ByteString.EMPTY, recoveryTxn(1, 20, 0, 300));
        });
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 100, 300));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, awaitTable(1, 200, 300));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(2, 100, 300));
        Assertions.assertEquals(80, candidate(300, 80));
        tracker.transactionFinished(1, 20);
        Assertions.assertEquals(149, candidate(300, 80));
    }

    @Test
    public void testRecoveredUnknownTsoRetainsTablesAcrossLocalRetry() throws Exception {
        reset(0);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(ByteString.EMPTY, recoveryTxn(1, 10, 0, 200)));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        register(1, 10, 300);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 100, 200));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(1, 200, 200));
        Assertions.assertEquals(80, candidate(400, 80));
        tracker.transactionFinished(1, 10);
        Assertions.assertEquals(400, candidate(400, 80));
    }

    @Test
    public void testRecoveredTransactionsAreReconciledInBoundedRotatingBatches() throws Exception {
        reset(0);
        TxnInfoPB[] transactions = new TxnInfoPB[150];
        for (int i = 0; i < transactions.length; i++) {
            transactions[i] = recoveryTxn(1, i + 1, 0, 100);
        }
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(ByteString.EMPTY, transactions));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
        Mockito.verify(txnMgr, Mockito.atMost(64)).getTransactionState(Mockito.anyLong(), Mockito.anyLong());
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(5));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(6));
        Mockito.verify(txnMgr).getTransactionState(1, 150);
        Assertions.assertEquals(80, candidate(200, 80)); // Missing state must retain recovery entries.
        TransactionState state = Mockito.mock(TransactionState.class);
        Mockito.when(state.getTransactionStatus()).thenReturn(TransactionStatus.COMMITTED);
        Mockito.when(txnMgr.getTransactionState(Mockito.anyLong(), Mockito.anyLong())).thenReturn(state);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(7));
        Assertions.assertEquals(80, candidate(200, 80));
        Mockito.when(state.getTransactionStatus()).thenReturn(TransactionStatus.ABORTED);
        for (int i = 0; i < 3; i++) {
            tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(8 + i));
        }
        Assertions.assertTrue(tracker.isRecoveryReady());
        Assertions.assertEquals(200, candidate(200, 80));
    }

    @Test
    public void testRecoveredTransactionCompletionWakesOnlyItsWaiters() throws Exception {
        reset(0);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(ByteString.EMPTY,
                        recoveryTxn(1, 10, 0, 100), recoveryTxn(2, 20, 0, 100)));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TSOTransactionTracker.WaitResult> waiting = submitReadWait(executor, started);
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
            TransactionState aborted = Mockito.mock(TransactionState.class);
            Mockito.when(aborted.getTransactionStatus()).thenReturn(TransactionStatus.ABORTED);
            Mockito.when(txnMgr.getTransactionState(1, 10)).thenReturn(aborted);
            tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
            Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, awaitTable(2, 100, 200));
            Assertions.assertEquals(80, candidate(300, 80));
        } finally {
            executor.shutdownNow();
        }
    }

    private Future<TSOTransactionTracker.WaitResult> submitReadWait(ExecutorService executor, CountDownLatch started) {
        return executor.submit(() -> {
            lock.lock();
            try {
                started.countDown();
                return tracker.awaitTransactions(Map.of(1L, Collections.singletonList(100L)),
                        200, TimeUnit.SECONDS.toNanos(30));
            } finally {
                lock.unlock();
            }
        });
    }

    @Test
    public void testReconciliationWakesReadWithoutAdvancingTheGlobalPrefix() throws Exception {
        reset(0);
        finishRecovery();
        register(1, 10, 100);
        register(2, 20, 90); // An unrelated database continues to hold the global prefix.
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TSOTransactionTracker.WaitResult> waiting = submitReadWait(executor, started);
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
            TransactionState aborted = Mockito.mock(TransactionState.class);
            Mockito.when(aborted.getTransactionStatus()).thenReturn(TransactionStatus.ABORTED);
            Mockito.when(txnMgr.getTransactionState(1, 10)).thenReturn(aborted);
            tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
            Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(89, candidate(300, 80));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testWaitReleasesAllocatorLockAndDoesNotFollowLaterWrites() throws Exception {
        reset(0);
        finishRecovery();
        register(1, 10, 100);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TSOTransactionTracker.WaitResult> waiting = submitReadWait(executor, started);
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
            // Acquiring the lock proves the waiter released it; allocation can continue while it waits.
            register(1, 20, 300);
            tracker.transactionFinished(1, 10);
            Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(30, TimeUnit.SECONDS));
            Assertions.assertEquals(1, tracker.getPendingCount());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testResetInvalidatesAnInFlightReadWait() throws Exception {
        reset(0);
        finishRecovery();
        register(1, 10, 100);
        CountDownLatch started = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<TSOTransactionTracker.WaitResult> waiting = submitReadWait(executor, started);
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
            reset(2000);
            Assertions.assertEquals(TSOTransactionTracker.WaitResult.RECOVERING, waiting.get(30, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void testOutOfOrderVisibilityAndRetryRetainEarliestTso() throws Exception {
        reset(2000);
        finishRecovery();
        register(1, 10, 100);
        register(1, 20, 120);
        tracker.transactionFinished(1, 20);
        Assertions.assertEquals(99, candidate(150, 80));
        register(1, 10, 200); // A retry must not hide a delayed request carrying TSO 100.
        Assertions.assertEquals(99, candidate(250, 99));
        tracker.transactionFinished(1, 10);
        tracker.transactionFinished(1, 10); // Duplicate terminal notification is harmless.
        Assertions.assertEquals(250, candidate(250, 99));
    }

    @Test
    public void testRecoveryCapturesFixedWatermarkAfterDelayAndPreservesNewPending() throws Exception {
        reset(2000);
        register(2, 2001, 200);
        tracker.checkTransactions(txnMgr, TimeUnit.MILLISECONDS.toNanos(1999));
        Mockito.verify(txnMgr, Mockito.never()).getTransactionIdWatermark();
        Assertions.assertEquals(80, candidate(250, 80));
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L, 2000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenReturn(recoveryBatch(ByteString.EMPTY, recoveryTxn(1, 10, 0, 100)));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(2));
        Assertions.assertEquals(80, candidate(250, 80));
        TransactionState aborted = Mockito.mock(TransactionState.class);
        Mockito.when(aborted.getTransactionStatus()).thenReturn(TransactionStatus.ABORTED);
        Mockito.when(txnMgr.getTransactionState(1, 10)).thenReturn(aborted);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        Assertions.assertEquals(199, candidate(250, 80));
        Mockito.verify(txnMgr).getTransactionIdWatermark();
        Mockito.verify(txnMgr).getTsoRecoveryTransactions(1000L, ByteString.EMPTY);
    }

    @Test
    public void testOnlyRealTerminalStatesReleasePending() throws Exception {
        reset(2000);
        finishRecovery();
        for (TransactionStatus status : TransactionStatus.values()) {
            register(1, 10, 100);
            TransactionState state = Mockito.mock(TransactionState.class);
            Mockito.when(state.getTransactionStatus()).thenReturn(status);
            Mockito.when(txnMgr.getTransactionState(1, 10)).thenReturn(state);
            tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
            boolean terminal = status == TransactionStatus.VISIBLE || status == TransactionStatus.ABORTED;
            Assertions.assertEquals(terminal ? 150 : 99, candidate(150, 80), status.toString());
        }
    }

    @Test
    public void testMissingTransactionAndFailedRecoveryNeverAdvance() throws Exception {
        reset(0);
        register(1, 10, 100);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY))
                .thenThrow(new UserException("old MS has no strict check capability"));
        Assertions.assertThrows(UserException.class,
                () -> tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3)));
        Assertions.assertEquals(80, candidate(150, 80));
        Assertions.assertEquals(1, tracker.getPendingCount());
        finishRecovery();
        Assertions.assertEquals(99, candidate(150, 80));
    }

    @Test
    public void testRpcDoesNotHoldAllocatorLockAndOldResultCannotRemoveNewRegistration() throws Exception {
        reset(2000);
        finishRecovery();
        register(1, 10, 100);
        TransactionState visible = Mockito.mock(TransactionState.class);
        Mockito.when(visible.getTransactionStatus()).thenReturn(TransactionStatus.VISIBLE);
        Mockito.when(txnMgr.getTransactionState(1, 10)).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isHeldByCurrentThread());
            reset(2000); // Simulate reinitialization while an old reconciliation request is in flight.
            register(1, 10, 200);
            return visible;
        });
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
        Assertions.assertEquals(1, tracker.getPendingCount());
        Assertions.assertEquals(200, tracker.getOldestPendingTso());
        Assertions.assertFalse(tracker.isRecoveryReady());
    }

    @Test
    public void testOldRecoveryResultCannotOpenNewRecovery() throws Exception {
        reset(0);
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.when(txnMgr.getTsoRecoveryTransactions(1000L, ByteString.EMPTY)).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isHeldByCurrentThread());
            reset(2000);
            return recoveryBatch(ByteString.EMPTY);
        });
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        Assertions.assertFalse(tracker.isRecoveryReady());
        Assertions.assertEquals(80, candidate(150, 80));
    }

    @Test
    public void testReconciliationBatchIsBoundedAndRotatesPastOldest() throws Exception {
        reset(0);
        finishRecovery();
        for (int i = 1; i <= 150; i++) {
            register(1, i, 1000 + i);
        }
        Mockito.clearInvocations(txnMgr);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(4));
        Mockito.verify(txnMgr, Mockito.atMost(64)).getTransactionState(Mockito.anyLong(), Mockito.anyLong());
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(5));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(6));
        Mockito.verify(txnMgr, Mockito.times(3)).getTransactionState(1, 1);
        Mockito.verify(txnMgr).getTransactionState(1, 150);
        Assertions.assertEquals(150, tracker.getPendingCount());
    }
}
