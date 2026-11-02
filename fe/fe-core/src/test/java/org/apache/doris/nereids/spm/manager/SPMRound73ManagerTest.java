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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-73 review fixes on the manager.
 *
 * <ul>
 *   <li>#2 a DROP deletes its own row and retires the same-key twins AFTERWARDS, so a twin
 *       delete that throws leaves the dropped id ABSENT: the retry never reaches the twin
 *       retirement again and reported success with that twin still matchable. The identity is
 *       now retained and the absent-row retry RESUMES the cleanup.</li>
 *   <li>#4 every identity group of a snapshot filter issued its own distributed SELECT, so
 *       2,000 live baselines meant 2,000 sequential queries on every periodic refresh on every
 *       FE (all of them under the refresh's writer lock). The compact states are read in
 *       bounded batches now, with the single-identity history read kept as the fallback.</li>
 *   <li>#5 the no-op INSERT and its visibility probes can consume the shared window's lifetime
 *       before the version prune starts: an aged window is IGNORED by every other FE, so the
 *       DELETE ran unfenced. The prune now checks its lease and gets a RENEWED window.</li>
 *   <li>#6 a 16-bit random process salt could give two windows one identity in the same
 *       millisecond, and either writer's exact-row close would then delete the other's still
 *       open marker. The salt is wider and the open VERIFIES that the marker it appended is the
 *       only one carrying its tick.</li>
 * </ul>
 */
public class SPMRound73ManagerTest {

    /** A durable store modelling rows, tombstones, the mutation clock and identity states. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        /** Every OPEN-window append, in order (the ticks that carry pending != 0). */
        final List<Long> pendingTicks = new CopyOnWriteArrayList<>();
        /** Every exact-row window close (see PRUNE_HWM_OPEN_SQL). */
        final List<Long> closedWindows = new CopyOnWriteArrayList<>();
        /** The newest pending tick that was open when each prune ran. */
        final List<Long> pruneWindowTicks = new CopyOnWriteArrayList<>();
        /** One id whose identity DELETE fails (the partial-cleanup schedule). */
        long failDeletesForId = -1L;
        /** How far the clock jumps while the no-op intent's row is written (see #5). */
        long clockJumpOnInsertMillis;
        /** The compact identity state the batched read answers for every slot. */
        SeqReservation batchedState;
        /** Per-slot override: the state that names the row's OWN incarnation (round-89). */
        final Map<Long, SeqReservation> batchedStatesBySlot = new ConcurrentHashMap<>();
        /** The state the single (history-reconciling) identity read answers. */
        SeqReservation singleState;
        /** How many rows the first pending-window probe reports (2 = a colliding identity). */
        long firstWindowProbeCount = 1L;
        int batchedReads;
        int singleReads;
        final AtomicLong clockJumpMillis = new AtomicLong();

        @Override
        public boolean isDurable() {
            return true;
        }

        @Override
        public boolean hashReadsAvailable() {
            return true;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            List<BaselinePlan> found = new ArrayList<>();
            for (BaselinePlan row : rows.values()) {
                if (row.getBindSqlHash() == hash) {
                    found.add(row);
                }
            }
            return found;
        }

        @Override
        public Map<Long, BaselinePlan> readSnapshot() {
            return new ConcurrentHashMap<>(rows);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
            if (clockJumpOnInsertMillis != 0) {
                clockJumpMillis.addAndGet(clockJumpOnInsertMillis);
            }
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (plan.getId() == failDeletesForId) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            rows.remove(plan.getId());
        }

        @Override
        public boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            tombstones.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
            return true;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            Set<String> found = new HashSet<>();
            for (String tombstone : tombstones) {
                int cut = tombstone.indexOf('|');
                if (ids.contains(Long.parseLong(tombstone.substring(0, cut)))) {
                    found.add(tombstone);
                }
            }
            return found;
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids, int timeoutSeconds) {
            return readDroppedIdentities(ids);
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            singleReads++;
            return singleState;
        }

        @Override
        public Map<Long, SeqReservation> readLatestReservations(Collection<Long> compactIds,
                int timeoutSeconds) {
            batchedReads++;
            if (batchedState == null) {
                return Map.of();
            }
            Map<Long, SeqReservation> states = new HashMap<>();
            for (Long compactId : compactIds) {
                SeqReservation answer = batchedStatesBySlot.get(compactId);
                states.put(compactId, answer == null ? batchedState : answer);
            }
            return states;
        }

        // ==================== mutation clock ====================

        @Override
        public long[] slotState() {
            long maxTick = 0;
            for (Long tick : pendingTicks) {
                maxTick = Math.max(maxTick, tick);
            }
            return new long[] {0L, maxTick};
        }

        @Override
        public void appendHwmRecord(long lastId, long tick, long pending) {
            if (pending != 0) {
                pendingTicks.add(pending);
            }
        }

        @Override
        public void closeMutationWindow(long windowTick) {
            closedWindows.add(windowTick);
        }

        @Override
        public long countVisiblePending(long tick) {
            if (pendingTicks.isEmpty()) {
                return 0L;
            }
            if (tick == pendingTicks.get(0) && pendingTicks.size() == 1) {
                return firstWindowProbeCount;
            }
            return pendingTicks.contains(tick) ? 1L : 0L;
        }

        @Override
        public void pruneSupersededVersions(BaselinePlan identity, long keepFromMicros) {
            pruneWindowTicks.add(pendingTicks.isEmpty() ? 0L
                    : pendingTicks.get(pendingTicks.size() - 1));
        }

        @Override
        public long countVersionsBefore(BaselinePlan identity, long keepFromMicros) {
            return 0L;
        }

        @Override
        public long readHwmRecord() {
            // the DROP key cleanup bounds its retirement by the id high-water mark
            return tableWatermark();
        }

        @Override
        public long tableWatermark() {
            long max = 0;
            for (BaselinePlan row : rows.values()) {
                max = Math.max(max, row.getId());
            }
            return max;
        }
    }

    private static BaselinePlan row(long id, String digest, BaselineStatus status) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1 where k = ?");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(0xA1L);
        plan.setPlanSql("SELECT k FROM t1 WHERE k = _spm_const_var(1)");
        plan.setSource(BaselineSource.USER);
        plan.setStatus(status);
        plan.setCreateTime(1_000L);
        plan.setUpdateTime(1_000L);
        return plan;
    }

    private static BaselineManager manager(TestStore store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(),
                () -> 1_700_000_000_000L + store.clockJumpMillis.get());
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    // ==================== #2: an absent-row DROP retry resumes the twin cleanup ====================

    /**
     * Same-key handoff rows 40/41/42, `DROP BASELINE PLAN 41`: the drop deletes 41 and retires
     * 40, then 42's DELETE throws. The retry finds 41 ABSENT durably and used to only settle the
     * tombstone obligation - so it reported success while 42 stayed durable and kept matching
     * the dropped key (its higher allocation is the newest non-dropped compact reservation, which
     * the identity filter does not hide).
     */
    @Test
    public void testAbsentRowDropRetryResumesTwinCleanup() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(40L, row(40L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));
        store.failDeletesForId = 42L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertTrue(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "the failed attempt retains the identity whose twins still have to go");
        Assertions.assertNotNull(store.rows.get(42L),
                "precondition: the failing twin is still durable");

        // the retry: 41 is gone durably, so it must RESUME the cleanup instead of only
        // settling the markers
        store.failDeletesForId = -1L;
        Assertions.assertFalse(manager.dropBaseline(41L), "the row of 41 is already gone");
        Assertions.assertNull(store.rows.get(42L),
                "the retry must delete the twin the failed attempt left behind");
        Assertions.assertFalse(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "a completed cleanup releases the retention");
        Assertions.assertTrue(store.tombstones.contains(
                        42L + "|" + "digest_a" + "|" + planSqlHash(row(42L, "digest_a", null))),
                "the resumed twin's marker is appended: " + store.tombstones);
    }

    private static long planSqlHash(BaselinePlan plan) {
        try {
            java.lang.reflect.Method method = BaselineManager.class
                    .getDeclaredMethod("identityPlanSqlHash", BaselinePlan.class);
            method.setAccessible(true);
            return (Long) method.invoke(null, plan);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The COMPACT identity slot id of one row (see BaselineManager#compactSeqId). */
    private static long compactSeqId(BaselinePlan plan) {
        try {
            java.lang.reflect.Method method = BaselineManager.class
                    .getDeclaredMethod("compactSeqId", String.class, long.class);
            method.setAccessible(true);
            return (Long) method.invoke(null, plan.getBindSqlDigest(), planSqlHash(plan));
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    // ==================== #4: batched identity-state reads ====================

    /**
     * 250 DISTINCT identities in one snapshot: the batched read answers every healthy identity,
     * so no single-identity query runs at all (the pre-fix form issued one per identity).
     */
    @Test
    public void testSnapshotFilterReadsIdentityStatesInBatches() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.batchedState = new BaselinePersistence.SeqReservation(7L, 1_000L, false, false);
        store.singleState = store.batchedState;

        Map<Long, BaselinePlan> snapshot = new LinkedHashMap<>();
        for (long id = 1; id <= 250; id++) {
            BaselinePlan plan = row(id, "digest_" + id, BaselineStatus.ENABLED);
            snapshot.put(id, plan);
            // the real compact answer names the row's OWN incarnation: such a slot needs no
            // history reconciliation (round-89), so the batched read still answers it
            store.batchedStatesBySlot.put(compactSeqId(plan),
                    new BaselinePersistence.SeqReservation(id, 1_000L, false, false));
        }
        int surviving = BaselineManagerTestAccess.filterSnapshot(manager, snapshot);

        Assertions.assertEquals(250, surviving, "healthy identities keep their rows");
        Assertions.assertEquals(0, store.singleReads,
                "a healthy identity must not cost its own distributed read");
        Assertions.assertEquals(3, store.batchedReads,
                "250 identities are read in 3 bounded batches of 100");
    }

    /**
     * A compact DROP tombstone can PREDATE a newer reservation of the same identity, so a slot
     * the batch reports as dropped still takes the single-identity read - which reconciles the
     * append-only history - and the NEWER healthy reservation wins.
     */
    @Test
    public void testDroppedCompactStateFallsBackToTheHistory() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.batchedState = new BaselinePersistence.SeqReservation(7L, 1_000L, false, true);
        store.singleState = new BaselinePersistence.SeqReservation(7L, 2_000L, false, false);

        Map<Long, BaselinePlan> snapshot = new LinkedHashMap<>();
        snapshot.put(5L, row(5L, "digest_a", BaselineStatus.ENABLED));
        int surviving = BaselineManagerTestAccess.filterSnapshot(manager, snapshot);

        Assertions.assertEquals(1, surviving,
                "the history's newer reservation resolves the identity: the row stays");
        Assertions.assertEquals(1, store.singleReads,
                "the compact tombstone still triggers the history read");
    }

    // ==================== #5: the prune gets a renewed window lease ====================

    /**
     * The no-op INSERT's confirmation probes can consume the window's successor-facing age
     * bound. A window past that bound is ignored by every other FE, so the pruning DELETE ran
     * with nothing left to reject a paginated reader's stale page. The prune must verify its
     * lease and - since the intent is already durable and readable - run under a RENEWED window.
     */
    @Test
    public void testAgedWindowIsRenewedBeforeTheVersionPrune() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan enabled = row(5L, "digest_a", BaselineStatus.ENABLED);
        store.rows.put(5L, enabled);
        manager.applyRefreshedBaselines(Map.of(5L, enabled));
        // the intent write burns the whole window budget (the reviewer's slow confirmation)
        store.clockJumpOnInsertMillis = 200_000L;

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED),
                "the no-op ALTER still succeeds");

        Assertions.assertEquals(2, store.pendingTicks.size(),
                "the aged window is replaced by a fresh one before the prune: "
                        + store.pendingTicks);
        Assertions.assertEquals(1, store.pruneWindowTicks.size(), "the prune still runs");
        Assertions.assertEquals(store.pendingTicks.get(1),
                store.pruneWindowTicks.get(0).longValue(),
                "the DELETE runs under the RENEWED window, not the aged one");
        Assertions.assertTrue(store.closedWindows.contains(store.pendingTicks.get(1)),
                "the renewed window is closed when the statement finishes");
    }

    // ==================== #6: a colliding window identity is abandoned ====================

    /**
     * Two FEs can draw the SAME salt in the SAME millisecond. Their windows then share one
     * identity, and either writer's exact-row close deletes the other's still-open marker. The
     * open now verifies that the marker it appended is the only one carrying its tick: a
     * colliding tick is ABANDONED (closing it would delete the other FE's marker) and the window
     * is re-opened with a fresh one.
     */
    @Test
    public void testCollidingWindowIdentityIsAbandonedAndRetried() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.firstWindowProbeCount = 2L;

        long tick = BaselineManagerTestAccess.beginRowMutation(manager);
        try {
            Assertions.assertEquals(2, store.pendingTicks.size(),
                    "the colliding tick is abandoned and a fresh one is opened: "
                            + store.pendingTicks);
            Assertions.assertEquals(store.pendingTicks.get(1).longValue(), tick,
                    "the returned window is the fresh one");
            Assertions.assertFalse(store.closedWindows.contains(store.pendingTicks.get(0)),
                    "the collided row is NOT closed: its tick also names another FE's window");
        } finally {
            BaselineManagerTestAccess.endRowMutation(manager, tick);
        }
        Assertions.assertTrue(store.closedWindows.contains(tick),
                "the accepted window is closed normally");
    }

    /** A window identity that is unique cluster-wide opens on the FIRST attempt. */
    @Test
    public void testUniqueWindowIdentityOpensImmediately() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);

        long tick = BaselineManagerTestAccess.beginRowMutation(manager);
        try {
            Assertions.assertEquals(1, store.pendingTicks.size(),
                    "no retry is needed when the identity is unique: " + store.pendingTicks);
            Assertions.assertEquals(store.pendingTicks.get(0).longValue(), tick);
        } finally {
            BaselineManagerTestAccess.endRowMutation(manager, tick);
        }
    }
}
