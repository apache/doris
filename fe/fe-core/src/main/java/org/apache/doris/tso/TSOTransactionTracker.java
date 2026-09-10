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

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** In-memory commit registrations. Uses the allocator's lock so no allocated TSO can be missed. */
final class TSOTransactionTracker {
    private static final Logger LOG = LogManager.getLogger(TSOTransactionTracker.class);
    private static final int CHECK_BATCH_SIZE = 64;
    private static final long CHECK_AGE_NANOS = TimeUnit.SECONDS.toNanos(1);
    private final ReentrantLock lock;
    private final Condition transactionsChanged;
    private final Map<Pair<Long, Long>, PendingTransaction> pendingByTxn = new HashMap<>();
    private final TreeMap<Long, PendingTransaction> pendingByTso = new TreeMap<>();
    private long generation;
    private long recoveryDeadlineNanos;
    private long recoveryWatermark;
    private boolean recoveryReady;
    private long pollCursor;

    enum WaitResult {
        FINISHED, TIMED_OUT, RECOVERING
    }

    private static final class PendingTransaction {
        private final Pair<Long, Long> identity;
        private final long tso;
        private final long registeredAtNanos;
        private final Set<Long> tableIds;

        private PendingTransaction(Pair<Long, Long> identity, long tso, long nowNanos, Set<Long> tableIds) {
            this.identity = identity;
            this.tso = tso;
            this.registeredAtNanos = nowNanos;
            this.tableIds = new HashSet<>(tableIds);
        }
    }

    TSOTransactionTracker(ReentrantLock lock) {
        this.lock = lock;
        this.transactionsChanged = lock.newCondition();
    }

    void reset(long nowNanos, long recoveryDelayMs) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        generation++;
        pendingByTxn.clear();
        pendingByTso.clear();
        recoveryDeadlineNanos = nowNanos + TimeUnit.MILLISECONDS.toNanos(recoveryDelayMs);
        recoveryWatermark = 0;
        recoveryReady = false;
        pollCursor = 0;
        transactionsChanged.signalAll();
    }

    void register(Pair<Long, Long> identity, long tso, long nowNanos, Set<Long> tableIds) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        Preconditions.checkArgument(!tableIds.isEmpty(), "commit registration requires table IDs");
        PendingTransaction existing = pendingByTxn.get(identity);
        if (existing != null) {
            // A timed-out request can still commit using the earlier TSO.
            existing.tableIds.addAll(tableIds);
            return;
        }
        PendingTransaction pending = new PendingTransaction(identity, tso, nowNanos, tableIds);
        pendingByTxn.put(identity, pending);
        Preconditions.checkState(pendingByTso.put(tso, pending) == null);
    }

    /** Called with the allocator lock after validating endTso against its current clock. */
    WaitResult awaitTransactions(Map<Long, List<Long>> dbToTableIds, long endTso, long remainingNanos)
            throws InterruptedException {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        if (!recoveryReady) {
            return WaitResult.RECOVERING;
        }
        long waitStartNanos = System.nanoTime();
        long waitGeneration = generation;
        List<PendingTransaction> remaining = new ArrayList<>();
        for (PendingTransaction pending : pendingByTso.headMap(endTso, true).values()) {
            List<Long> tables = dbToTableIds.get(pending.identity.first);
            if (tables != null && !Collections.disjoint(tables, pending.tableIds)) {
                remaining.add(pending);
            }
        }
        // Allocation/registration and this snapshot share the lock. Later allocations are outside
        // the validated window; only this fixed set can affect the read. awaitNanos releases the lock.
        while (true) {
            if (generation != waitGeneration) {
                return WaitResult.RECOVERING;
            }
            remaining.removeIf(pending -> pendingByTxn.get(pending.identity) != pending);
            if (remaining.isEmpty()) {
                return WaitResult.FINISHED;
            }
            long nanosLeft = remainingNanos - (System.nanoTime() - waitStartNanos);
            if (nanosLeft <= 0) {
                return WaitResult.TIMED_OUT;
            }
            transactionsChanged.awaitNanos(nanosLeft);
        }
    }

    long candidateCommittedTso(long currentTso, long durableCommittedTso) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        if (!recoveryReady) {
            return durableCommittedTso;
        }
        long candidate = pendingByTso.isEmpty() ? currentTso
                : Math.min(currentTso, pendingByTso.firstKey() - 1);
        Preconditions.checkState(candidate >= durableCommittedTso, "committed TSO must not regress");
        return candidate;
    }

    void transactionFinished(long dbId, long txnId) {
        lock.lock();
        try {
            PendingTransaction pending = pendingByTxn.remove(Pair.of(dbId, txnId));
            if (pending != null) {
                pendingByTso.remove(pending.tso);
                transactionsChanged.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /** Runs on a separate daemon. Neither recovery nor transaction RPCs hold the allocator lock. */
    void checkTransactions(GlobalTransactionMgrIface txnMgr, long nowNanos) throws UserException {
        long checkGeneration;
        long watermark;
        boolean checkRecovery;
        List<PendingTransaction> batch = new ArrayList<>();
        lock.lock();
        try {
            checkGeneration = generation;
            watermark = recoveryWatermark;
            checkRecovery = !recoveryReady && nowNanos - recoveryDeadlineNanos >= 0;
            if (!pendingByTso.isEmpty()) {
                // Always check the transaction blocking the prefix, then rotate through the rest.
                PendingTransaction oldest = pendingByTso.firstEntry().getValue();
                if (nowNanos - oldest.registeredAtNanos >= CHECK_AGE_NANOS) {
                    batch.add(oldest);
                }
                for (int i = 0; i < Math.min(CHECK_BATCH_SIZE - 1, pendingByTso.size()); i++) {
                    Map.Entry<Long, PendingTransaction> next = pendingByTso.higherEntry(pollCursor);
                    if (next == null) {
                        next = pendingByTso.firstEntry();
                    }
                    pollCursor = next.getKey();
                    PendingTransaction pending = next.getValue();
                    if (pending != oldest && nowNanos - pending.registeredAtNanos >= CHECK_AGE_NANOS) {
                        batch.add(pending);
                    }
                }
            }
        } finally {
            lock.unlock();
        }

        // Reconcile registrations even while the recovery scan is failing or waiting on old transactions.
        for (PendingTransaction pending : batch) {
            TransactionState state = txnMgr.getTransactionState(pending.identity.first, pending.identity.second);
            // null includes RPC errors and NOT_FOUND; neither proves that a transaction is finished.
            if (state == null || (state.getTransactionStatus() != TransactionStatus.VISIBLE
                    && state.getTransactionStatus() != TransactionStatus.ABORTED)) {
                continue;
            }
            lock.lock();
            try {
                if (generation == checkGeneration && pendingByTxn.get(pending.identity) == pending) {
                    pendingByTxn.remove(pending.identity);
                    pendingByTso.remove(pending.tso);
                    transactionsChanged.signalAll();
                }
            } finally {
                lock.unlock();
            }
        }

        if (checkRecovery) {
            if (watermark == 0) {
                watermark = txnMgr.getTransactionIdWatermark();
                Preconditions.checkState(watermark > 0, "invalid recovery transaction watermark");
                lock.lock();
                try {
                    if (generation != checkGeneration) {
                        return;
                    }
                    recoveryWatermark = watermark;
                } finally {
                    lock.unlock();
                }
            }
            boolean finished = txnMgr.isPreviousTransactionsFinishedForTsoRecovery(watermark);
            lock.lock();
            try {
                if (generation == checkGeneration && finished) {
                    recoveryReady = true;
                    LOG.info("TSO recovery completed, transaction watermark={}", watermark);
                }
            } finally {
                lock.unlock();
            }
        }
    }

    long getPendingCount() {
        lock.lock();
        try {
            return pendingByTxn.size();
        } finally {
            lock.unlock();
        }
    }

    long getOldestPendingTso() {
        lock.lock();
        try {
            return pendingByTso.isEmpty() ? 0 : pendingByTso.firstKey();
        } finally {
            lock.unlock();
        }
    }

    long getOldestPendingTxnId() {
        lock.lock();
        try {
            return pendingByTso.isEmpty() ? 0 : pendingByTso.firstEntry().getValue().identity.second;
        } finally {
            lock.unlock();
        }
    }

    long getOldestPendingAgeMs() {
        lock.lock();
        try {
            return pendingByTso.isEmpty() ? 0 : TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - pendingByTso.firstEntry().getValue().registeredAtNanos);
        } finally {
            lock.unlock();
        }
    }

    long getRecoveryWatermark() {
        lock.lock();
        try {
            return recoveryWatermark;
        } finally {
            lock.unlock();
        }
    }

    boolean isRecoveryReady() {
        lock.lock();
        try {
            return recoveryReady;
        } finally {
            lock.unlock();
        }
    }
}
