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
 * Round-76 review fixes on the manager.
 *
 * <ul>
 *   <li>#2 a DROP that deleted its own row but failed only to make its tombstones READABLE kept
 *       the key-cleanup retention alive. The user then re-CREATEd the same SQL (a NEW id), and
 *       the retried {@code DROP ... IF EXISTS <oldId>} resolved its cache miss by RESUMING that
 *       cleanup - whose {@code (digest, planSql, fingerprint)} match also matches the fresh row,
 *       so the retry deleted the baseline the user had just created;</li>
 *   <li>#5 the identity DELETE only checked leadership BEFORE it opened its mutation window,
 *       while the status DELETE re-checks leadership AND the window age immediately before the
 *       statement. A writer paused across a handoff therefore dispatched the identity DELETE
 *       after the successor had already updated the row;</li>
 *   <li>#7 a DROP's success fence was an EXPIRING one although the DELETE had PROVEN the
 *       transaction committed: after the bound the next refresh re-published the
 *       still-unreadable pre-DROP row. The DROP now records the same CONFIRMED absence fence an
 *       ALTER records for its proven flip.</li>
 * </ul>
 */
public class SPMRound76ManagerTest {

    /** A durable store modelling rows, tombstones and a mutation clock. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        final List<Long> pendingTicks = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<Long> closedWindows = new java.util.concurrent.CopyOnWriteArrayList<>();
        /** When set, every tombstone append fails (the unreadable-marker schedule). */
        boolean failTombstones;
        /** When set, the DELETE of this id throws (the ambiguous-commit schedule). */
        long failDeletesForId = -1L;
        /** Runs while the mutation window of a DELETE is being opened. */
        Runnable windowOpenHook;
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
            if (failTombstones) {
                return false;
            }
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
                if (windowOpenHook != null) {
                    windowOpenHook.run();
                }
            }
        }

        @Override
        public void closeMutationWindow(long windowTick) {
            closedWindows.add(windowTick);
        }

        @Override
        public long countVisiblePending(long tick) {
            return pendingTicks.contains(tick) ? 1L : 0L;
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

    private static BaselineManager manager(TestStore store,
            BaselineTestDoubles.TestClusterGateway gateway) {
        BaselineManager manager = new BaselineManager(store, gateway,
                () -> 1_700_000_000_000L + store.clockJumpMillis.get());
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    // ==================== #2: the retry must not delete a re-CREATEd baseline ============

    /**
     * The reviewer's schedule: DROP 41 deletes its row but its tombstone confirmation fails, so
     * the DROP reports a retryable error with the key-cleanup retention still recorded. The user
     * then CREATEd the same SQL again (id 42). The retried DROP - which finds 41 ABSENT - used to
     * resume the retained cleanup, and that retirement matched 42 on
     * {@code (digest, planSql, fingerprint)}: the fresh baseline was deleted.
     */
    @Test
    public void testDropRetryKeepsTheBaselineRecreatedAfterIt() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        // the tombstone append cannot be made readable: the DROP fails retryably
        store.failTombstones = true;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertNull(store.rows.get(41L), "precondition: the drop's DELETE committed");
        Assertions.assertTrue(BaselineManagerTestAccess.owesDropTombstoneConfirmation(41L),
                "precondition: the failed attempt owes its marker confirmation");
        Assertions.assertFalse(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "a FINISHED key cleanup must not stay retained across a failed"
                        + " confirmation: nothing is left to resume");

        // the user re-creates the SAME SQL: a new id, the same (digest, planSql, fingerprint)
        store.failTombstones = false;
        BaselinePlan recreated = row(42L, "digest_a", BaselineStatus.ENABLED);
        store.rows.put(42L, recreated);

        Assertions.assertFalse(manager.dropBaseline(41L), "the row of 41 is already gone");
        Assertions.assertNotNull(store.rows.get(42L),
                "the retried DROP of 41 must NOT retire the baseline the user re-created");
        Assertions.assertFalse(store.tombstones.contains(
                        42L + "|" + "digest_a" + "|" + planSqlHash(42L, "digest_a")),
                "the re-created baseline carries no DROP tombstone: " + store.tombstones);
    }

    // ==================== #5: the identity DELETE re-checks at dispatch =================

    /**
     * A writer that is PAUSED between `assertLeaderForWrite` and the DELETE must not dispatch
     * after the handoff: the status DELETE already re-validates leadership and the window age
     * immediately before its statement, and the identity DELETE now does the same. The paused
     * caller is simulated by dropping leadership inside the store's own round-trip.
     */
    @Test
    public void testIdentityDeleteIsRefusedWhenLeadershipIsLostAtDispatch() {
        TestStore store = new TestStore();
        BaselineTestDoubles.TestClusterGateway gateway =
                new BaselineTestDoubles.TestClusterGateway();
        java.util.concurrent.atomic.AtomicBoolean leader =
                new java.util.concurrent.atomic.AtomicBoolean(true);
        gateway.leaderProbe = leader::get;
        BaselineManager manager = manager(store, gateway);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        // The handoff lands WHILE the mutation window is being opened - i.e. AFTER the
        // leadership check that precedes it and BEFORE the DELETE is dispatched. Only the
        // dispatch-time re-check (which the status DELETE already runs) can see it.
        store.windowOpenHook = () -> leader.set(false);

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L),
                "a writer demoted between the fence and the statement must not dispatch the"
                        + " identity DELETE");
        Assertions.assertNotNull(store.rows.get(41L),
                "the identity DELETE never landed, so the successor's row is untouched");
        Assertions.assertFalse(store.closedWindows.isEmpty(),
                "the opened window is released even though the statement was refused");
    }

    // ==================== #7: the DROP's absence fence is CONFIRMED =====================

    /**
     * The identity DELETE returned OK, so the transaction committed and the absence is
     * IRREVERSIBLE - only its publication can still lag a local read. The fence must therefore
     * be CONFIRMED (exempt from the age bound), exactly like an ALTER's proven flip: an expiring
     * fence was dropped after the bound and the next refresh republished the pre-DROP row.
     */
    @Test
    public void testSuccessfulDropRecordsAConfirmedAbsenceFence() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));

        Assertions.assertTrue(manager.dropBaseline(41L), "the DROP reports success");
        Assertions.assertNull(store.rows.get(41L), "the row is deleted durably");

        Assertions.assertTrue(BaselineManagerTestAccess.hasPendingMutationFence(manager, 41L),
                "the dropped id stays masked until a snapshot shows it gone");
        Assertions.assertTrue(
                BaselineManagerTestAccess.isPendingMutationFenceConfirmed(manager, 41L),
                "a PROVEN delete is fenced with the CONFIRMED form: an expiring fence would"
                        + " re-publish the pre-DROP row once the bound elapsed");
    }

    /**
     * The cache-miss DROP path (a promotion-window snapshot) records the same CONFIRMED fence:
     * every identity DELETE on that path also returned OK.
     */
    @Test
    public void testCacheMissDropAlsoRecordsAConfirmedAbsenceFence() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));

        Assertions.assertTrue(manager.dropBaseline(41L),
                "the DROP reconciles the durable row although the cache is empty");
        Assertions.assertTrue(
                BaselineManagerTestAccess.isPendingMutationFenceConfirmed(manager, 41L),
                "both DROP paths fence a proven absence the same way");
    }

    /** The AMBIGUOUS path keeps an EXPIRING fence: that delete is not proven. */
    @Test
    public void testUnprovenDeleteKeepsAnExpiringFence() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.failDeletesForId = 41L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertTrue(BaselineManagerTestAccess.hasPendingMutationFence(manager, 41L),
                "the unresolved delete still masks the id");
        Assertions.assertFalse(
                BaselineManagerTestAccess.isPendingMutationFenceConfirmed(manager, 41L),
                "an UNPROVEN delete must keep the expiring fence: after the bound the write is"
                        + " presumed lost and the persisted state wins again");
    }

    private static long planSqlHash(long id, String digest) {
        try {
            BaselinePlan probe = row(id, digest, null);
            java.lang.reflect.Method method = BaselineManager.class
                    .getDeclaredMethod("identityPlanSqlHash", BaselinePlan.class);
            method.setAccessible(true);
            return (Long) method.invoke(null, probe);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }
}
