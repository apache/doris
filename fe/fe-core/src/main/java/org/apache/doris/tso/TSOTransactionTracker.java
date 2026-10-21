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

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
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
    private final ReentrantLock lock;
    private final Condition transactionsChanged;
    private final Map<Pair<Long, Long>, PendingTransaction> pendingByTxn = new HashMap<>();
    private final TreeMap<Long, PendingTransaction> pendingByTso = new TreeMap<>();
    private long generation;

    enum WaitResult {
        FINISHED, TIMED_OUT, RESET
    }

    private static final class PendingTransaction {
        private final Pair<Long, Long> identity;
        private final long tso;
        private final long registeredAtNanos;
        private final Set<Long> tableIds;

        private PendingTransaction(Pair<Long, Long> identity, long tso, long nowNanos, Collection<Long> tableIds) {
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

    void reset() {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        generation++;
        pendingByTxn.clear();
        pendingByTso.clear();
        transactionsChanged.signalAll();
    }

    void register(Pair<Long, Long> identity, long tso, long nowNanos, Set<Long> tableIds) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        Preconditions.checkArgument(!tableIds.isEmpty(), "commit registration requires table IDs");
        PendingTransaction existing = pendingByTxn.get(identity);
        if (existing != null) {
            existing.tableIds.addAll(tableIds);
            return;
        }
        PendingTransaction pending = new PendingTransaction(identity, tso, nowNanos, tableIds);
        pendingByTxn.put(identity, pending);
        Preconditions.checkState(pendingByTso.put(tso, pending) == null);
    }

    void replaceFenced(Pair<Long, Long> identity, long rejectedTso, long fenceTso, long newTso,
            long nowNanos, Set<Long> tableIds) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        Preconditions.checkArgument(!tableIds.isEmpty(), "commit registration requires table IDs");
        Preconditions.checkArgument(rejectedTso <= fenceTso, "rejected TSO must be fenced");
        Preconditions.checkArgument(newTso > fenceTso, "replacement TSO must be above the fence");
        PendingTransaction existing = pendingByTxn.get(identity);
        Set<Long> mergedTableIds = new HashSet<>();
        if (existing != null) {
            Preconditions.checkState(existing.tso <= fenceTso, "registered transaction TSO must be fenced");
            Preconditions.checkState(pendingByTso.remove(existing.tso) == existing);
            mergedTableIds.addAll(existing.tableIds);
        }
        mergedTableIds.addAll(tableIds);
        PendingTransaction replacement = new PendingTransaction(identity, newTso, nowNanos, mergedTableIds);
        pendingByTxn.put(identity, replacement);
        Preconditions.checkState(pendingByTso.put(newTso, replacement) == null);
        transactionsChanged.signalAll();
    }

    /** Called with the allocator lock after validating endTso against its current clock. */
    WaitResult awaitTransactions(Map<Long, List<Long>> dbToTableIds, long endTso, long remainingNanos)
            throws InterruptedException {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        long waitStartNanos = System.nanoTime();
        long waitGeneration = generation;
        List<PendingTransaction> remaining = new ArrayList<>();
        for (PendingTransaction pending : pendingByTso.headMap(endTso, true).values()) {
            List<Long> tables = dbToTableIds.get(pending.identity.first);
            if (tables != null && !Collections.disjoint(tables, pending.tableIds)) {
                remaining.add(pending);
            }
        }
        while (true) {
            if (generation != waitGeneration) {
                return WaitResult.RESET;
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
        if (currentTso < durableCommittedTso) {
            return durableCommittedTso;
        }
        long candidate = pendingByTso.isEmpty() ? currentTso
                : Math.min(currentTso, pendingByTso.firstKey() - 1);
        Preconditions.checkState(candidate >= durableCommittedTso, "committed TSO must not regress");
        return candidate;
    }

    void markTxnFinished(long dbId, long txnId) {
        lock.lock();
        try {
            remove(Pair.of(dbId, txnId));
        } finally {
            lock.unlock();
        }
    }

    void abandonCommitTso(long dbId, long txnId, long tso) {
        lock.lock();
        try {
            Pair<Long, Long> identity = Pair.of(dbId, txnId);
            PendingTransaction pending = pendingByTxn.get(identity);
            if (pending != null && pending.tso == tso) {
                remove(identity);
            }
        } finally {
            lock.unlock();
        }
    }

    private void remove(Pair<Long, Long> identity) {
        Preconditions.checkState(lock.isHeldByCurrentThread());
        PendingTransaction pending = pendingByTxn.remove(identity);
        if (pending != null) {
            Preconditions.checkState(pendingByTso.remove(pending.tso) == pending);
            transactionsChanged.signalAll();
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
}
