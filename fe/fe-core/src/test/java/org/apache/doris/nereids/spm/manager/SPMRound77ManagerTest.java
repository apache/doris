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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Round-77 review fixes on the manager.
 *
 * <ul>
 *   <li>#7 the resumed key cleanup's bound is an epoch MILLISECOND (the stored update time is
 *       millis): a seconds bound was always smaller, so every twin - including the ones the failed
 *       attempt still owed - was skipped;</li>
 *   <li>#8 a status statement's stored second may only lead wall time by a BOUNDED amount, because
 *       every shared fence is wall-clock bounded: an unbounded lead let an already committed no-op
 *       intent outlive the fence and reverse a successor's successful ALTER;</li>
 *   <li>#10 the close of a mutation window verifies that the window identity is NOT shared before
 *       deleting it: two FEs with equal salts in the same millisecond hold one identity, and the
 *       exact-tick delete then removed the other writer's still-open marker as well.</li>
 * </ul>
 */
public class SPMRound77ManagerTest {

    /** A durable store modelling rows, tombstones and the mutation clock. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        final List<Long> pendingTicks = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<Long> closedWindows = new java.util.concurrent.CopyOnWriteArrayList<>();
        /** How many rows the window probe reports (2 = the identity is shared). */
        long windowHolders = 1L;
        long failDeletesForId = -1L;
        long newestStoredSecond;
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
        public long newestStoredUpdateSecond(long id) {
            return newestStoredSecond;
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
            if (!pendingTicks.contains(tick)) {
                return 0L;
            }
            return windowHolders;
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

    // ==================== #7: the resumed key cleanup's bound ========================

    /**
     * The failed attempt leaves the twin durable (its DELETE threw) and the retry resumes the
     * cleanup. The bound is the instant the DROP removed its own rows, in the SAME unit the stored
     * update time uses - a seconds bound compared against stored MILLIS skipped every twin, so the
     * retry reported success with the leftover still durable and matchable.
     */
    @Test
    public void testResumedKeyCleanupRetiresTheTwinTheFailedAttemptLeft() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));
        store.failDeletesForId = 42L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertTrue(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "precondition: the failed attempt kept the cleanup");
        Assertions.assertNotNull(store.rows.get(42L), "precondition: the twin is still durable");

        store.failDeletesForId = -1L;
        Assertions.assertFalse(manager.dropBaseline(41L), "the row of 41 is already gone");
        Assertions.assertNull(store.rows.get(42L),
                "the resumed cleanup must retire the twin the failed attempt left behind");
    }

    /**
     * A baseline CREATED after the DROP is still kept: only rows that existed when the DROP
     * removed its identity are twins of that DROP.
     */
    @Test
    public void testResumedKeyCleanupKeepsABaselineCreatedAfterwards() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));
        store.failDeletesForId = 42L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertTrue(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "precondition: the failed attempt kept the cleanup");
        store.failDeletesForId = -1L;
        // the twin was re-created AFTER the retention instant (a fresh incarnation)
        store.rows.get(42L).setUpdateTime(System.currentTimeMillis() + 5_000L);

        Assertions.assertFalse(manager.dropBaseline(41L));
        Assertions.assertNotNull(store.rows.get(42L),
                "a baseline created after the DROP must survive its retried key cleanup");
    }

    // ==================== #8: the stored second may only lead wall time by a bound ====

    /**
     * Every shared fence is wall-clock bounded, so a stored second that leads wall time by MORE
     * than the shortest bound breaks "the fence expired, therefore the write is lost": the already
     * committed intent would outlive the fence and reverse a successor's successful ALTER. A
     * statement whose stored second would exceed the bound fails retryably instead.
     */
    @Test
    public void testUnboundedStoredSecondLeadIsRefused() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));

        // the id's newest stored second is far ahead of wall time (a burst of flips)
        store.newestStoredSecond = System.currentTimeMillis() / 1000L + 3_600L;

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.updateStatus(41L, BaselineStatus.ENABLED),
                "a no-op statement whose stored second would lead wall time beyond every"
                        + " fence must fail retryably");
        Assertions.assertTrue(error.getMessage().contains("lead"),
                "the failure names the unbounded lead: " + error.getMessage());
    }

    /** A lead inside the bound keeps working (the ordinary flip path). */
    @Test
    public void testBoundedStoredSecondLeadIsAccepted() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));

        // one second ahead: well inside the bound
        store.newestStoredSecond = System.currentTimeMillis() / 1000L + 1L;

        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.ENABLED),
                "a lead inside the fence bound is the ordinary bump");
    }

    // ==================== #10: closing a window verifies its ownership ================

    /**
     * Two FEs with equal salts in the same millisecond hold ONE tick, and the exact-tick delete
     * removes BOTH markers - including the other writer's still-open one. An open-time probe
     * cannot see it (that row may still be unreadable), so the CLOSE verifies the identity is not
     * shared and leaves the rows to their age bound instead.
     */
    @Test
    public void testSharedWindowIdentityIsNotClosed() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        // the open-time probe reports a single row, the close-time probe sees two
        long tick = BaselineManagerTestAccess.beginRowMutation(manager);
        store.windowHolders = 2L;
        BaselineManagerTestAccess.endRowMutation(manager, tick);

        Assertions.assertFalse(store.closedWindows.contains(tick),
                "a SHARED window identity must not be deleted: it also names another FE's"
                        + " still-open marker");
    }

    /** An unshared window is closed exactly like before. */
    @Test
    public void testUnsharedWindowIdentityIsClosed() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        long tick = BaselineManagerTestAccess.beginRowMutation(manager);
        BaselineManagerTestAccess.endRowMutation(manager, tick);

        Assertions.assertTrue(store.closedWindows.contains(tick),
                "an unshared window is closed by its own completion");
    }
}
