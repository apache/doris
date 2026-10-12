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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public class TSOTransactionTrackerTest {
    private ReentrantLock lock;
    private TSOTransactionTracker tracker;
    private ExecutorService executor;

    @BeforeEach
    public void setUp() {
        lock = new ReentrantLock();
        tracker = new TSOTransactionTracker(lock);
        executor = Executors.newSingleThreadExecutor();
        lock.lock();
        try {
            tracker.reset();
        } finally {
            lock.unlock();
        }
    }

    @AfterEach
    public void tearDown() {
        executor.shutdownNow();
    }

    private void register(long dbId, long txnId, long tso, long... tableIds) {
        Set<Long> tables = new java.util.HashSet<>();
        for (long tableId : tableIds) {
            tables.add(tableId);
        }
        lock.lock();
        try {
            tracker.register(Pair.of(dbId, txnId), tso, System.nanoTime(), tables);
        } finally {
            lock.unlock();
        }
    }

    private TSOTransactionTracker.WaitResult await(long dbId, long tableId, long endTso, long timeoutMs)
            throws InterruptedException {
        lock.lock();
        try {
            return tracker.awaitTransactions(Map.of(dbId, List.of(tableId)), endTso,
                    TimeUnit.MILLISECONDS.toNanos(timeoutMs));
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

    private Future<TSOTransactionTracker.WaitResult> awaitAsync(long dbId, long tableId, long endTso,
            long timeoutMs, CountDownLatch started) {
        return executor.submit(() -> {
            lock.lock();
            try {
                started.countDown();
                return tracker.awaitTransactions(Map.of(dbId, List.of(tableId)), endTso,
                        TimeUnit.MILLISECONDS.toNanos(timeoutMs));
            } finally {
                lock.unlock();
            }
        });
    }

    @Test
    public void testWaitFiltersDatabaseTableAndEndTso() throws Exception {
        register(1, 10, 100, 1000);
        register(1, 20, 120, 2000);
        register(2, 30, 130, 1000);

        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, await(1, 2000, 110, 0));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, await(2, 1000, 120, 0));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, await(1, 1000, 100, 0));
        Assertions.assertEquals(99, candidate(150, 90));

        tracker.markTxnFinished(1, 10);
        Assertions.assertEquals(119, candidate(150, 90));
        tracker.markTxnFinished(1, 20);
        tracker.markTxnFinished(2, 30);
        Assertions.assertEquals(150, candidate(150, 90));
    }

    @Test
    public void testWaitUsesFixedRegistrationSnapshot() throws Exception {
        register(1, 10, 100, 1000);
        Future<TSOTransactionTracker.WaitResult> waiting = executor.submit(() -> await(1, 1000, 150, 5000));
        Thread.sleep(100);
        register(1, 20, 120, 1000);
        tracker.markTxnFinished(1, 10);

        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(5, TimeUnit.SECONDS));
        Assertions.assertEquals(119, candidate(150, 90));
    }

    @Test
    public void testResetInvalidatesWait() throws Exception {
        register(1, 10, 100, 1000);
        Future<TSOTransactionTracker.WaitResult> waiting = executor.submit(() -> await(1, 1000, 150, 5000));
        Thread.sleep(100);
        lock.lock();
        try {
            tracker.reset();
        } finally {
            lock.unlock();
        }

        Assertions.assertEquals(TSOTransactionTracker.WaitResult.RESET, waiting.get(5, TimeUnit.SECONDS));
    }

    @Test
    public void testFencedReplacementAndAttemptScopedAbandon() {
        register(1, 10, 100, 1000);
        lock.lock();
        try {
            tracker.replaceFenced(Pair.of(1L, 10L), 100, 110, 120,
                    System.nanoTime(), Collections.singleton(2000L));
        } finally {
            lock.unlock();
        }

        Assertions.assertEquals(119, candidate(150, 90));
        tracker.abandonCommitTso(1, 10, 100);
        Assertions.assertEquals(1, tracker.getPendingCount());
        tracker.abandonCommitTso(1, 10, 120);
        Assertions.assertEquals(0, tracker.getPendingCount());
    }

    @Test
    public void testRepeatedRegistrationRetainsEarliestTso() throws Exception {
        register(1, 10, 100, 1000);
        register(1, 10, 120, 2000);

        Assertions.assertEquals(1, tracker.getPendingCount());
        Assertions.assertEquals(100, tracker.getOldestPendingTso());
        Assertions.assertEquals(10, tracker.getOldestPendingTxnId());
        Assertions.assertTrue(tracker.getOldestPendingAgeMs() >= 0);
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, await(1, 2000, 100, 0));
        tracker.abandonCommitTso(1, 10, 120);
        Assertions.assertEquals(1, tracker.getPendingCount());
        tracker.markTxnFinished(1, 10);
        Assertions.assertEquals(0, tracker.getPendingCount());
    }

    @Test
    public void testFencedReplacementWakesOldAttemptWaiterAndMergesTables() throws Exception {
        register(1, 10, 100, 1000);
        CountDownLatch started = new CountDownLatch(1);
        Future<TSOTransactionTracker.WaitResult> waiting = awaitAsync(1, 1000, 110, 5000, started);
        Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));

        lock.lock();
        try {
            tracker.replaceFenced(Pair.of(1L, 10L), 100, 110, 120,
                    System.nanoTime(), Collections.singleton(2000L));
        } finally {
            lock.unlock();
        }

        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(5, TimeUnit.SECONDS));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, await(1, 1000, 120, 0));
        Assertions.assertEquals(TSOTransactionTracker.WaitResult.TIMED_OUT, await(1, 2000, 120, 0));
        Assertions.assertEquals(120, tracker.getOldestPendingTso());
    }

    @Test
    public void testAbandonWakesWaiterOnlyForMatchingAttempt() throws Exception {
        register(1, 10, 100, 1000);
        CountDownLatch started = new CountDownLatch(1);
        Future<TSOTransactionTracker.WaitResult> waiting = awaitAsync(1, 1000, 100, 5000, started);
        Assertions.assertTrue(started.await(5, TimeUnit.SECONDS));

        tracker.abandonCommitTso(1, 10, 99);
        Assertions.assertEquals(1, tracker.getPendingCount());
        tracker.abandonCommitTso(1, 10, 100);

        Assertions.assertEquals(TSOTransactionTracker.WaitResult.FINISHED, waiting.get(5, TimeUnit.SECONDS));
        Assertions.assertEquals(0, tracker.getPendingCount());
        Assertions.assertEquals(0, tracker.getOldestPendingTso());
        Assertions.assertEquals(0, tracker.getOldestPendingTxnId());
        Assertions.assertEquals(0, tracker.getOldestPendingAgeMs());
    }

    @Test
    public void testFencedReplacementWithoutExistingRegistration() {
        lock.lock();
        try {
            tracker.replaceFenced(Pair.of(1L, 10L), 100, 110, 120,
                    System.nanoTime(), Set.of(1000L));
        } finally {
            lock.unlock();
        }

        Assertions.assertEquals(1, tracker.getPendingCount());
        Assertions.assertEquals(120, tracker.getOldestPendingTso());
        Assertions.assertEquals(10, tracker.getOldestPendingTxnId());
        Assertions.assertEquals(119, candidate(150, 90));
        Assertions.assertEquals(150, candidate(100, 150));
    }

    @Test
    public void testRegistrationPreconditions() {
        Assertions.assertThrows(IllegalStateException.class, tracker::reset);
        Assertions.assertThrows(IllegalStateException.class,
                () -> tracker.register(Pair.of(1L, 10L), 100, System.nanoTime(), Set.of(1000L)));
        Assertions.assertThrows(IllegalStateException.class,
                () -> tracker.replaceFenced(Pair.of(1L, 10L), 100, 110, 120,
                        System.nanoTime(), Set.of(1000L)));
        Assertions.assertThrows(IllegalStateException.class,
                () -> tracker.awaitTransactions(Map.of(1L, List.of(1000L)), 100, 0));
        Assertions.assertThrows(IllegalStateException.class, () -> tracker.candidateCommittedTso(100, 90));

        lock.lock();
        try {
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> tracker.register(Pair.of(1L, 10L), 100, System.nanoTime(), Collections.emptySet()));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> tracker.replaceFenced(Pair.of(1L, 10L), 111, 110, 120,
                            System.nanoTime(), Set.of(1000L)));
            Assertions.assertThrows(IllegalArgumentException.class,
                    () -> tracker.replaceFenced(Pair.of(1L, 10L), 100, 110, 110,
                            System.nanoTime(), Set.of(1000L)));
        } finally {
            lock.unlock();
        }
    }
}
