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

import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.transaction.GlobalTransactionMgrIface;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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
            tracker.register(Pair.of(dbId, txnId), tso, 0);
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

    private void finishRecovery() throws Exception {
        Mockito.when(txnMgr.getTransactionIdWatermark()).thenReturn(1000L);
        Mockito.doReturn(true).when(txnMgr).isPreviousTransactionsFinishedForTsoRecovery(1000L);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
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
        Mockito.when(txnMgr.isPreviousTransactionsFinishedForTsoRecovery(1000L)).thenReturn(false, true);
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(2));
        Assertions.assertEquals(80, candidate(250, 80));
        tracker.checkTransactions(txnMgr, TimeUnit.SECONDS.toNanos(3));
        Assertions.assertEquals(199, candidate(250, 80));
        Mockito.verify(txnMgr).getTransactionIdWatermark();
        Mockito.verify(txnMgr, Mockito.times(2)).isPreviousTransactionsFinishedForTsoRecovery(1000L);
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
        Mockito.when(txnMgr.isPreviousTransactionsFinishedForTsoRecovery(1000L))
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
        Mockito.when(txnMgr.isPreviousTransactionsFinishedForTsoRecovery(1000L)).thenAnswer(invocation -> {
            Assertions.assertFalse(lock.isHeldByCurrentThread());
            reset(2000);
            return true;
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
