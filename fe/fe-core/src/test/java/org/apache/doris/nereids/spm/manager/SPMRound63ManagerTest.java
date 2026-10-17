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

package org.apache.doris.nereids.spm.manager;

import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Round-63 review fixes of the matching-path cache miss and its surroundings:
 *
 * - the miss read is RECONCILED before it may match or be cached: one deterministic
 *   winner per id (a failed old-row cleanup leaves both physical versions behind) and the
 *   active mutation fences (a committed-but-unreadable flip must not be served from its
 *   pre-flip row);
 * - the publication is validated ATOMICALLY against the hash's mutation epoch, which a
 *   status flip and a refresh update now ADVANCE as well - a slow miss that read ENABLED
 *   while a DISABLE published DISABLED used to replace the completed state, and an empty
 *   miss that raced a CREATE used to erase the concurrently published hash;
 * - the miss read is bounded by the remaining rewrite budget (an exhausted budget
 *   degrades the attempt without any read, and the timeout threaded to the store shrinks
 *   with it);
 * - the pre-snapshot settlement closes HELD mutation windows whose committed row became
 *   readable BEFORE the quiet gate, so refresh / SHOW / promotion reload no longer
 *   bounce until the 120 s marker expiry;
 * - the INSERT failure types are explicit: an ERROR that may have committed travels as
 *   AmbiguousInsertException, a reported SUCCESS whose row is unreadable as
 *   UnconfirmedVisibleException.
 */
public class SPMRound63ManagerTest {

    private final BaselineTestDoubles.TestBaselineStore store =
            new BaselineTestDoubles.TestBaselineStore();
    private final BaselineTestDoubles.TestClusterGateway gateway =
            new BaselineTestDoubles.TestClusterGateway();
    private BaselineManager manager;

    private BaselineManager newManager() {
        BaselineManager created = new BaselineManager(store, gateway, System::currentTimeMillis);
        BaselineManagerTestAccess.clear(created);
        return created;
    }

    private static BaselinePlan row(long id, String digest, long hash, BaselineStatus status,
            long updateTime) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(hash);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(status);
        plan.setCreateTime(updateTime);
        plan.setUpdateTime(updateTime);
        return plan;
    }

    private static BaselinePlan row(long id, String digest, long hash, BaselineStatus status) {
        return row(id, digest, hash, status, 1_000L * (id + 1));
    }

    /** Runs the body with a capacity-1 cache (the miss path needs an eviction). */
    private void withCapacityOne(Runnable body) {
        int previousCapacity = VariableMgr.getDefaultSessionVariable().getSpmBaselineCacheSize();
        VariableMgr.getDefaultSessionVariable().setSpmBaselineCacheSize(1);
        try {
            body.run();
        } finally {
            VariableMgr.getDefaultSessionVariable().setSpmBaselineCacheSize(previousCapacity);
        }
    }

    // ==================== #5: empty miss racing a CREATE ====================

    /**
     * A MISS whose read returns EMPTY removes the hash from the index - but it must not
     * discard a hash a concurrent CREATE published in the meantime. The copy-on-write
     * removal used to assign its old copy minus the miss's hash, so {A} minus A assigned
     * AFTER a CREATE's {A, B} made B invisible to hasBaselines / candidate lookup until
     * the next refresh. The removal now runs under the same lock as the publishers.
     */
    @Test
    public void testEmptyMissDoesNotEraseAConcurrentCreateHash() throws Exception {
        store.durableOverride = true;
        manager = newManager();
        manager.applyRefreshedBaselines(Map.of(1L, row(1L, "digest_a", 0xA1L,
                BaselineStatus.ENABLED)));

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch readerRelease = new CountDownLatch(1);
        store.byHashReader = hash -> {
            if (hash == 0xA1L) {
                readerEntered.countDown();
                try {
                    readerRelease.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return List.of(); // the hash's rows are gone from the table
            }
            if (hash == 0xB2L) {
                return List.of(row(3L, "digest_b", 0xB2L, BaselineStatus.ENABLED, 9_000L));
            }
            return List.of();
        };

        AtomicReference<List<BaselinePlan>> missResult = new AtomicReference<>();
        Thread miss = new Thread(() ->
                missResult.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        miss.start();
        Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS),
                "the miss read must be in flight");

        // the concurrent CREATE (a successful publisher of hash B)
        manager.createBaseline(row(3L, "digest_b", 0xB2L, BaselineStatus.ENABLED, 9_000L));

        readerRelease.countDown();
        miss.join(TimeUnit.SECONDS.toMillis(5));
        Assertions.assertTrue(missResult.get().isEmpty());

        Assertions.assertTrue(manager.hasBaselines(),
                "the empty miss must not erase the concurrently published hash");
        Assertions.assertEquals(1,
                manager.findCandidateBaselines("digest_b", 0xB2L).size(),
                "the CREATE's hash stays visible after the empty miss removed its own");
    }

    // ==================== #1: winner + fence reconciliation on the miss ====================

    /**
     * After a DISABLE whose old-row cleanup failed, BOTH physical versions stay in the
     * table (DUPLICATE KEY): once the hash is evicted, the miss used to return them
     * unfiltered and the active-status filter picked the STALE ENABLED row. The miss now
     * applies the snapshot's per-id winner (pickDurableWinner) before matching.
     */
    @Test
    public void testMissReconcilesPerIdWinnerAfterEviction() {
        store.durableOverride = true;
        manager = newManager();
        withCapacityOne(() -> {
            BaselinePlan enabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
            BaselinePlan other = row(3L, "digest_c", 0xC3L, BaselineStatus.ENABLED, 5_000L);
            manager.applyRefreshedBaselines(Map.of(1L, enabled, 3L, other));
            store.byHashReader = hash -> hash == 0xA1L ? List.of(enabled) : List.of(other);
            Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
            Assertions.assertEquals(1, manager.findCandidateBaselines("digest_c", 0xC3L).size());
            Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                    "hash A must be evicted for the miss path to run");

            // the DISABLE inserted the new-status row while the old ENABLED cleanup failed:
            // both rows are physically present, the DISABLED one is the durable winner
            BaselinePlan staleEnabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
            BaselinePlan freshDisabled = row(1L, "digest_a", 0xA1L, BaselineStatus.DISABLED, 2_000L);
            store.byHashReader = hash -> List.of(staleEnabled, freshDisabled);

            Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                    "the miss must serve the durable WINNER (DISABLED), never the stale"
                            + " ENABLED row a failed cleanup left behind");
            Assertions.assertEquals(1, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                    "the reconciled winner set is what the miss caches");
        });
    }

    /**
     * A committed-but-unreadable DISABLE keeps a confirmed mutation fence: the miss read
     * still returns the pre-flip ENABLED row, and serving it would replay a baseline whose
     * durable winner is already DISABLED. The active fence masks the id on the miss path
     * as well, and the fully masked read is neither cached nor kept as a live hash.
     */
    @Test
    public void testMissMasksAnIdWithAnActiveStatusFence() {
        store.durableOverride = true;
        manager = newManager();
        withCapacityOne(() -> {
            BaselinePlan enabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
            BaselinePlan other = row(3L, "digest_c", 0xC3L, BaselineStatus.ENABLED, 5_000L);
            manager.applyRefreshedBaselines(Map.of(1L, enabled, 3L, other));
            store.byHashReader = hash -> hash == 0xA1L ? List.of(enabled) : List.of(other);
            Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
            Assertions.assertEquals(1, manager.findCandidateBaselines("digest_c", 0xC3L).size());
            Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L));

            BaselineManagerTestAccess.recordPendingMutationFence(manager, 1L,
                    BaselineStatus.DISABLED, 2_000L);
            AtomicInteger reads = new AtomicInteger();
            BaselinePlan stillEnabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
            store.byHashReader = hash -> {
                reads.incrementAndGet();
                return List.of(stillEnabled);
            };

            Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                    "the committed flip's fence must mask the pre-flip row on the miss");
            Assertions.assertEquals(1, reads.get(), "the read DID happen (and was discarded)");
            Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                    "a fully masked read is never cached");
            Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());
            Assertions.assertEquals(1, reads.get(),
                    "the fully masked outcome retires the stale hash from the index: the"
                            + " next attempt short-circuits without another read");

            BaselineManagerTestAccess.removePendingMutationFence(manager, 1L);
            manager.applyRefreshedBaselines(Map.of(
                    1L, row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 3_000L)));
            store.byHashReader = hash -> List.of(
                    row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 3_000L));
            Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size(),
                    "without the fence the same read matches again");
        });
    }

    // ==================== #2: epoch advance on status flip / refresh ====================

    /**
     * A slow miss reads ENABLED while a REFRESH replaces the hash's rows (a DISABLE
     * committed): the refresh used to PRESERVE an existing hash's sequence, so the
     * equality check still passed and the stale rows were published / returned. The
     * refresh now advances the epochs of every updated hash, so the miss discards its
     * stale rows for this attempt too.
     */
    @Test
    public void testLatchMissDiscardsStaleRowsAgainstRefreshUpdate() throws Exception {
        store.durableOverride = true;
        manager = newManager();
        withCapacityOne(() -> {
            try {
                BaselinePlan enabled = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
                BaselinePlan other = row(3L, "digest_c", 0xC3L, BaselineStatus.ENABLED, 5_000L);
                manager.applyRefreshedBaselines(Map.of(1L, enabled, 3L, other));
                store.byHashReader = hash -> hash == 0xA1L ? List.of(enabled) : List.of(other);
                Assertions.assertEquals(1,
                        manager.findCandidateBaselines("digest_a", 0xA1L).size());
                Assertions.assertEquals(1,
                        manager.findCandidateBaselines("digest_c", 0xC3L).size());
                Assertions.assertEquals(0,
                        BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                        "hash A must be evicted for the miss path to run");

                CountDownLatch readerEntered = new CountDownLatch(1);
                CountDownLatch readerRelease = new CountDownLatch(1);
                AtomicInteger reads = new AtomicInteger();
                AtomicReference<List<BaselinePlan>> served =
                        new AtomicReference<>(List.of(enabled));
                store.byHashReader = hash -> {
                    reads.incrementAndGet();
                    readerEntered.countDown();
                    try {
                        readerRelease.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return served.get();
                };

                AtomicReference<List<BaselinePlan>> result = new AtomicReference<>();
                Thread miss = new Thread(() ->
                        result.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
                miss.start();
                Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS));

                // the refresh replaced the hash's rows while the miss was in flight (hash C's
                // cached entry changes too, which is what advances EVERY updated hash's
                // epoch)
                manager.applyRefreshedBaselines(Map.of(
                        1L, row(1L, "digest_a", 0xA1L, BaselineStatus.DISABLED, 2_000L),
                        3L, row(3L, "digest_c", 0xC3L, BaselineStatus.DISABLED, 9_000L)));
                readerRelease.countDown();
                miss.join(TimeUnit.SECONDS.toMillis(5));

                Assertions.assertTrue(result.get().isEmpty(),
                        "the stale ENABLED rows read before the refresh must be discarded");
                Assertions.assertEquals(0,
                        BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                        "a discarded read publishes nothing");

                // nothing was cached: the next attempt reads again and serves the refreshed
                // state (the DISABLED winner is not matchable)
                served.set(List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.DISABLED, 2_000L)));
                Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());
                Assertions.assertEquals(2, reads.get(), "the fresh read was needed");
                Assertions.assertEquals(1,
                        BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                        "the refreshed winner set is cached by the fresh read");
                // and the cached copy serves the next match without another read
                Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());
                Assertions.assertEquals(2, reads.get());
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        });
    }

    /**
     * The status-flip half of the same race: the CONFIRMED flip's publish must advance the
     * hash's epoch too, or the in-flight miss replaces the completed DISABLED state with
     * its ENABLED rows. The test then proves the fence keeps masking (and the fully masked
     * read retires the index entry) until the fence is gone.
     */
    @Test
    public void testLatchMissDiscardsStaleRowsAgainstStatusFlip() throws Exception {
        store.durableOverride = true;
        manager = newManager();
        BaselinePlan published = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        manager.applyRefreshedBaselines(Map.of(1L, published));

        CountDownLatch readerEntered = new CountDownLatch(1);
        CountDownLatch readerRelease = new CountDownLatch(1);
        AtomicInteger reads = new AtomicInteger();
        BaselinePlan preFlipRow = row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            readerEntered.countDown();
            try {
                readerRelease.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return List.of(preFlipRow);
        };

        AtomicReference<List<BaselinePlan>> result = new AtomicReference<>();
        Thread miss = new Thread(() ->
                result.set(manager.findCandidateBaselines("digest_a", 0xA1L)));
        miss.start();
        Assertions.assertTrue(readerEntered.await(5, TimeUnit.SECONDS));

        // the completed DISABLE: publishStatus advances the epoch and the confirmed fence
        // masks the pre-flip row until a snapshot shows the new one
        BaselineManagerTestAccess.publishStatus(manager, published, BaselineStatus.DISABLED,
                2_000L);
        BaselineManagerTestAccess.recordPendingMutationFence(manager, 1L,
                BaselineStatus.DISABLED, 2_000L);
        readerRelease.countDown();
        miss.join(TimeUnit.SECONDS.toMillis(5));

        Assertions.assertTrue(result.get().isEmpty(),
                "the completed flip must not be overwritten by the in-flight miss");
        Assertions.assertEquals(0, BaselineManagerTestAccess.cachedHashCountFor(manager, 0xA1L),
                "the discarded read publishes nothing");

        // the fence still masks: the next read is discarded as well (and retires the hash)
        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty());
        Assertions.assertEquals(2, reads.get());
        Assertions.assertFalse(manager.hasBaselines(),
                "a fully masked read must stop short-circuiting the hash index");

        // once the outcome is resolved the baseline matches again
        BaselineManagerTestAccess.removePendingMutationFence(manager, 1L);
        manager.applyRefreshedBaselines(Map.of(
                1L, row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 3_000L)));
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 3_000L));
        };
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L).size());
        Assertions.assertEquals(3, reads.get());
    }

    // ==================== #8: the miss is bounded by the rewrite budget ====================

    /**
     * An EXHAUSTED rewrite budget degrades the attempt without the synchronous read: the
     * miss used to issue the internal query with its fixed ten-second timeout even after
     * spm_rewrite_timeout_ms (one second by default) was consumed.
     */
    @Test
    public void testExhaustedBudgetSkipsTheMissRead() {
        store.durableOverride = true;
        manager = newManager();
        manager.applyRefreshedBaselines(Map.of(1L, row(1L, "digest_a", 0xA1L,
                BaselineStatus.ENABLED)));
        AtomicInteger reads = new AtomicInteger();
        store.byHashReader = hash -> {
            reads.incrementAndGet();
            return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED));
        };

        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L,
                System.currentTimeMillis() - 5L).isEmpty());
        Assertions.assertEquals(0, reads.get(),
                "an exhausted budget must not issue the synchronous by-hash read");

        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L,
                System.currentTimeMillis() + 5_000L).size(),
                "a fresh budget reads normally");
        Assertions.assertEquals(1, reads.get());
    }

    /**
     * The remaining budget is THREADED to the store as the statement timeout (rounded up
     * to seconds and capped by the fixed internal timeout), instead of the constant ten
     * seconds.
     */
    @Test
    public void testMissReadTimeoutIsBoundedByRemainingBudget() {
        store.durableOverride = true;
        manager = newManager();
        manager.applyRefreshedBaselines(Map.of(1L, row(1L, "digest_a", 0xA1L,
                BaselineStatus.ENABLED)));
        AtomicInteger observedTimeout = new AtomicInteger(-1);
        store.byHashTimedReader = (hash, timeoutSeconds) -> {
            observedTimeout.set(timeoutSeconds);
            return List.of(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED));
        };

        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_a", 0xA1L,
                System.currentTimeMillis() + 1_500L).size());
        Assertions.assertTrue(observedTimeout.get() >= 1 && observedTimeout.get() <= 10,
                "the timeout must be the (rounded) remaining budget, capped by the fixed"
                        + " internal timeout, got: " + observedTimeout.get());
    }

    // ==================== #4: settle held windows before the quiet gate ====================

    /**
     * A successful DISABLE whose row appears just after its visibility probes exhausted
     * leaves the SHARED pending marker visible: the snapshot's quiet gate rejects it, and
     * the post-snapshot settlement could never run because that gate failed first - so
     * refresh / SHOW / promotion reload bounced until the marker aged out. The
     * pre-snapshot settlement now probes the held ids and closes the window whose
     * committed row became readable; a still-unreadable probe keeps it held.
     */
    @Test
    public void testPreSnapshotSettlementClosesReadableHeldWindows() {
        store.durableOverride = true;
        manager = newManager();
        BaselinePlan disabled = row(1L, "digest_a", 0xA1L, BaselineStatus.DISABLED, 2_000L);
        AtomicReference<BaselinePlan> durableRow = new AtomicReference<>(disabled);
        store.idAllocatorStore = new BaselineTestDoubles.IdAllocatorStore() {
            @Override
            public long watermark() {
                return 1L;
            }

            @Override
            public void insert(BaselinePlan plan) {
            }

            @Override
            public List<BaselinePlan> readById(long id) {
                BaselinePlan current = durableRow.get();
                return current != null && current.getId() == id ? List.of(current) : List.of();
            }

            @Override
            public void deleteByIdentity(BaselinePlan plan) {
            }
        };
        store.mutationClockStore = new RecordingClockStore();

        Assertions.assertTrue(BaselineManagerTestAccess.holdUnresolvedMutationWindow(
                manager, 777L, disabled));
        Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow());

        // the committed row IS readable: settlement closes the window
        BaselineManagerTestAccess.settleHeldMutationWindowsBeforeSnapshot(manager);
        Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "the readable outcome must close its held window before the quiet gate");

        // an unreadable outcome (still the pre-flip row) keeps the window held
        durableRow.set(row(1L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L));
        Assertions.assertTrue(BaselineManagerTestAccess.holdUnresolvedMutationWindow(
                manager, 778L, disabled));
        BaselineManagerTestAccess.settleHeldMutationWindowsBeforeSnapshot(manager);
        Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "an unreadable flip keeps its window held (fail closed)");
    }

    /**
     * The snapshot reader runs the settlement BEFORE each attempt's quiet gate: with a
     * fence that stays pending until the settlement ran TWICE, the read still converges
     * (exactly two settlement runs prove the callback precedes each attempt's fence read).
     */
    @Test
    public void testSnapshotReaderSettlesBeforeTheQuietGate() {
        AtomicInteger settlements = new AtomicInteger();
        Runnable settle = settlements::incrementAndGet;
        BaselineManager.SnapshotFenceReader fenceReader = () ->
                new BaselineManager.SnapshotFence(1L, 1L, 1L,
                        settlements.get() >= 2 ? 0L : 7L);
        BaselineManager.SnapshotPageReader reader =
                (pageStart, offset) -> new ArrayList<ResultRow>();

        Map<Long, List<BaselinePlan>> snapshot = BaselineManagerTestAccess
                .readStableRawSnapshotWithSettle(reader, fenceReader, settle);
        Assertions.assertNotNull(snapshot);
        Assertions.assertEquals(2, settlements.get(),
                "the reader must settle BEFORE reading the fence of each attempt");
    }

    // ==================== the explicit INSERT failure types ====================

    /**
     * An ERROR that may have been raised AFTER a commit travels as AmbiguousInsertException
     * (the create path remembers the identity so its retry defers / adopts).
     */
    @Test
    public void testAmbiguousCreateTravelsAsAmbiguousInsertException() {
        store.durableOverride = true;
        manager = newManager();
        store.idAllocatorStore = new BaselineTestDoubles.IdAllocatorStore() {
            @Override
            public long watermark() {
                return 0L;
            }

            @Override
            public void insert(BaselinePlan plan) {
                throw new RuntimeException("insert timed out");
            }

            @Override
            public List<BaselinePlan> readById(long id) {
                return List.of(); // no read proves the row
            }

            @Override
            public void deleteByIdentity(BaselinePlan plan) {
            }
        };

        RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                () -> manager.createBaseline(row(1L, "digest_a", 0xA1L,
                        BaselineStatus.ENABLED, 5_000L)));
        Assertions.assertEquals("AmbiguousInsertException", failure.getClass().getSimpleName(),
                "an error that may have committed is AMBIGUOUS, never a proven failure: "
                        + failure);
        Assertions.assertEquals(1, BaselineManagerTestAccess.pendingCreateCount(manager),
                "the attempted identity stays remembered for the retry");
    }

    /**
     * The type hierarchy the callers branch on: AmbiguousStatusInsertException IS an
     * AmbiguousInsertException, while the reported-success-unreadable outcome is its own
     * UnconfirmedVisibleException.
     */
    @Test
    public void testInsertFailureTypeHierarchyIsExplicit() throws Exception {
        Class<?> ambiguous = Class.forName(
                "org.apache.doris.nereids.spm.manager.BaselineManager$AmbiguousInsertException");
        Class<?> ambiguousStatus = Class.forName(
                "org.apache.doris.nereids.spm.manager.BaselineManager$AmbiguousStatusInsertException");
        Class<?> unconfirmedVisible = Class.forName(
                "org.apache.doris.nereids.spm.manager.BaselineManager$UnconfirmedVisibleException");
        Assertions.assertTrue(ambiguous.isAssignableFrom(ambiguousStatus),
                "the conditional status INSERT's ambiguous outcome IS an ambiguous INSERT");
        Assertions.assertFalse(unconfirmedVisible.isAssignableFrom(ambiguous),
                "a confirmed-but-unreadable write must not be treated as ambiguous");
    }

    /** A scripted mutation clock: the slot reads quiet, the window bookkeeping is recorded. */
    private static final class RecordingClockStore
            implements BaselineTestDoubles.MutationClockStore {
        @Override
        public long[] slotState() {
            return new long[] {0L, 0L};
        }

        @Override
        public long[] clock() {
            return new long[] {0L, 0L, 0L, 0L};
        }

        @Override
        public void append(long lastId, long tick, long pending) {
        }

        @Override
        public void prune(long lastId, long tick) {
        }

        @Override
        public void closeWindow(long windowTick) {
        }

        @Override
        public long countVisiblePending(long tick) {
            return 1L;
        }
    }
}
