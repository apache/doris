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

/**
 * Round-80 review fixes on the manager.
 *
 * <ul>
 *   <li>#3 the REAL flip's stored instant is derived from a keyed read of the id's newest
 *       row, and that read cannot see a foreign write that COMMITTED but is still
 *       unpublished (the old leader's no-op DISABLE across a handoff). The flip then writes
 *       an instant that does not beat the hidden row, and the next refresh reverts the
 *       successful ALTER. The read is now bracketed by a QUIET shared mutation clock, so the
 *       statement fails retryably while any window is open instead of silently ordering
 *       itself below a write nothing here ever saw.</li>
 *   <li>#6 the load path applied the snapshot to the row cache twice; the second call could
 *       only re-diff against the signatures the first had just written;</li>
 *   <li>#10/#11 the DROP's KEY CLEANUP retention reads the id watermark, and that read can
 *       fail AFTER the identity DELETE was already CONFIRMED. It used to run before the
 *       cache eviction and before the durable tombstone obligation, so such a failure left
 *       the dropped ENABLED baseline matchable here (#10) and lost the tombstone a delayed
 *       status INSERT of a demoted master needs to be defeated after a cache loss / handoff
 *       (#11). Both obligations are now settled first, in the cache-miss path's order.</li>
 * </ul>
 */
public class SPMRound80ManagerTest {

    /** A durable store modelling rows, tombstones and the SHARED mutation clock. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        final List<Long> pendingTicks = new java.util.concurrent.CopyOnWriteArrayList<>();
        long newestStoredMicros = 1_000L;
        long idWatermark;
        /** When set, the compact id watermark read fails (a transient internal-table error). */
        boolean watermarkUnreadable;
        /**
         * The tick of a FOREIGN FE's still-open mutation window (see SELECT_CLOCK_SQL): a
         * handoff leaves the old leader's window behind until its row becomes readable, and
         * this FE has no local fence for it.
         */
        long foreignPendingTick;

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            return true;
        }

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
        public long newestStoredUpdateMicros(long id) {
            return newestStoredMicros;
        }

        @Override
        public long readHwmRecord() {
            if (watermarkUnreadable) {
                throw new RuntimeException("internal read failed (hwm)");
            }
            return idWatermark;
        }

        @Override
        public long readSequenceTail(long floor) {
            return 0L;
        }

        // ==================== shared mutation clock ====================

        /**
         * the SHARED clock: MAX(tick), COUNT(*), SUM(tick) and the OPEN-window marker. The
         * window of THIS FE is closed by the ALTER itself, so only a FOREIGN one (see
         * foreignPendingTick) can make a probe non-quiet.
         */
        @Override
        public long[] clock() {
            long maxTick = 0;
            for (Long tick : pendingTicks) {
                maxTick = Math.max(maxTick, tick);
            }
            return new long[] {0L, 0L, 0L, foreignPendingTick == 0 ? 0L : foreignPendingTick};
        }

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
        // PARSEABLE text: a refreshed row is deepened (its transient trees are rebuilt) before
        // it enters the cache, and unparseable text would silently leave the cache empty.
        plan.setBindSql("select k from t1");
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
                () -> 1_700_000_000_000L);
        BaselineManagerTestAccess.clear(manager);
        return manager;
    }

    /**
     * CACHED path: the KEY CLEANUP's watermark read fails (round-83 #6 turned the retention into
     * the FIRST step of the DROP, so the failure now happens with the row still PRESENT).
     *
     * Nothing may be half-done: the identity is still durable and still matchable, no tombstone /
     * fence / cleanup obligation was recorded, and the retry repeats the WHOLE DROP. Before the
     * reorder the delete could land first, and a failure at this point left no cleanup obligation
     * at all (an absent-row retry then reported success while a newer same-key twin stayed
     * matchable - see testDropRetiresTheSurvivingTwinAfterTheWatermarkFailure).
     */
    @Test
    public void testCachedDropFailsBeforeDeletingWhenTheWatermarkReadFails() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        // install the row as a COMPLETE cache entry: that is exactly the state a warm match
        // leaves behind, and it is what puts the DROP below on the CACHED branch
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        BaselineManagerTestAccess.installCachedRow(manager,
                row(41L, "digest_a", BaselineStatus.ENABLED));
        Assertions.assertNotNull(BaselineManagerTestAccess.rawCacheGet(manager, 41L),
                "the warmed row puts the DROP on the cached path");

        assertUnreadableWatermarkLeavesTheDropUntouched(store, manager, 41L);
        Assertions.assertNotNull(BaselineManagerTestAccess.rawCacheGet(manager, 41L),
                "the cached row must stay: nothing was deleted");
    }

    /**
     * CACHE-MISS path (a promotion reload / stale snapshot): the same watermark failure must
     * leave the durable rows untouched here as well.
     */
    @Test
    public void testDurableDropFailsBeforeDeletingWhenTheWatermarkReadFails() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan plan = row(41L, "digest_a", BaselineStatus.ENABLED);
        store.rows.put(41L, plan);
        manager.applyRefreshedBaselines(Map.of(41L, plan));
        Assertions.assertNull(BaselineManagerTestAccess.rawCacheGet(manager, 41L),
                "a durable refresh leaves the row durable-only (the cache-miss DROP path)");

        assertUnreadableWatermarkLeavesTheDropUntouched(store, manager, 41L);
    }

    /**
     * The scenario the retention ORDER exists for (review round-83 #6): a DROP whose KEY CLEANUP
     * cannot be prepared must not leave a state an `IF EXISTS` retry reads as "already dropped".
     * The id has a NEWER same-key twin; the failed attempt keeps both rows, and the retry - now
     * with a readable watermark - deletes the id AND retires the twin, so nothing of the key stays
     * eligible for a cold match.
     */
    @Test
    public void testDropRetiresTheSurvivingTwinAfterTheWatermarkFailure() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));
        // the allocation high-water mark covers the twin, so the retirement may reclaim it
        store.idWatermark = 42L;
        BaselineManagerTestAccess.installCachedRow(manager,
                row(41L, "digest_a", BaselineStatus.ENABLED));

        assertUnreadableWatermarkLeavesTheDropUntouched(store, manager, 41L);
        Assertions.assertTrue(store.rows.containsKey(42L),
                "the twin is untouched by the failed attempt");

        store.watermarkUnreadable = false;
        Assertions.assertTrue(manager.dropBaseline(41L),
                "the retry repeats the WHOLE DROP once the watermark is readable");
        Assertions.assertFalse(store.rows.containsKey(41L), "the id is deleted");
        Assertions.assertFalse(store.rows.containsKey(42L),
                "the same-key twin is retired with the same DROP - an absent-row retry must never"
                        + " report success while it is still matchable: " + store.rows);
        Assertions.assertFalse(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "the finished cleanup releases its retention");
    }

    /**
     * Drops one id whose key cleanup cannot be prepared (an unreadable id watermark) and asserts
     * the DROP did NOTHING: the row is still durable and matchable, and no tombstone, fence or
     * cleanup obligation was recorded for it.
     *
     * @param store   the simulated durable store
     * @param manager the manager under test
     * @param id      the id to drop
     */
    private static void assertUnreadableWatermarkLeavesTheDropUntouched(TestStore store,
            BaselineManager manager, long id) {
        store.watermarkUnreadable = true;

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.dropBaseline(id),
                "an unbounded key cleanup keeps the DROP retryable");
        Assertions.assertTrue(failure.getMessage().contains("watermark"),
                "the failure names the unreadable bound: " + failure.getMessage());
        Assertions.assertTrue(store.rows.containsKey(id),
                "the retention is the FIRST step: the identity delete never ran");
        Assertions.assertNotNull(manager.getBaseline(id),
                "the baseline is still durable and matchable");
        Assertions.assertTrue(store.tombstones.isEmpty(),
                "no tombstone may be appended for a DROP that did nothing: " + store.tombstones);
        Assertions.assertFalse(BaselineManagerTestAccess.owesDropTombstoneConfirmation(id),
                "no readable-tombstone obligation either");
        Assertions.assertFalse(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                "the id is not fenced: it was never dropped");
        Assertions.assertFalse(BaselineManagerTestAccess.retainsDropKeyCleanup(id),
                "a cleanup that could not be bounded is not retained");
    }

    /**
     * A foreign window is OPEN (the old leader's no-op DISABLE, unresolved across a handoff),
     * so the keyed read of the id's newest stored instant may be missing that hidden row: the
     * REAL flip REPAIR below must fail retryably instead of writing an ENABLED row whose instant
     * does not beat the hidden DISABLED one - the durable pick would then revert the successful
     * ALTER on the next refresh.
     */
    @Test
    public void testRealFlipRepairIsRefusedWhileAForeignWindowIsOpen() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        // the cache knows ENABLED (the state this FE promoted with; the load path publishes the
        // persisted SCALARS without rebuilding their trees) ...
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        BaselineManagerTestAccess.prepareLoad(manager);
        manager.loadFromInternalTable();
        Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(41L).getStatus(),
                "precondition: the cache holds the ENABLED row");
        // ... and the old leader's later DISABLE is in the table but no read shows it yet
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.DISABLED));
        store.foreignPendingTick = 900_001L;

        // the REPAIR's instant read is the seam that must fail closed: it is what orders the
        // flip's row, and an open foreign window means a row of this id may be hidden from it
        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManagerTestAccess.newestStoredInstantAroundQuietClock(manager, 41L),
                "a foreign window must stop the REPAIR from computing its stored instant");
        Assertions.assertTrue(error.getMessage().contains("still in flight"),
                "the failure names the open window: " + error.getMessage());
    }

    /** Once the foreign window closed, the very same flip is ordered against a complete read. */
    @Test
    public void testRealFlipRepairProceedsOnceTheForeignWindowClosed() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        BaselineManagerTestAccess.prepareLoad(manager);
        manager.loadFromInternalTable();
        Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(41L).getStatus(),
                "precondition: the cache holds the ENABLED row");
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.DISABLED));

        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.ENABLED),
                "a quiet clock proves the keyed read saw every durable row of the id");
        Assertions.assertEquals(BaselineStatus.ENABLED, store.rows.get(41L).getStatus());
    }

    /**
     * The load path applies the snapshot to the row cache ONCE (see publishLoadedSnapshot): the
     * former second call re-diffed against the signatures the first had just written, so it
     * could only report "nothing changed". The published state must be complete either way.
     */
    @Test
    public void testLoadPublishesEveryRowOnce() {
        TestStore store = new TestStore();
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        BaselinePlan second = row(42L, "digest_b", BaselineStatus.DISABLED);
        second.setBindSqlHash(0xB2L);
        store.rows.put(42L, second);
        store.idWatermark = 42L;
        BaselineManager manager = manager(store);

        manager.loadFromInternalTable();

        Assertions.assertEquals(2, manager.getAllBaselines().size(),
                "every persisted row is published: " + manager.getAllBaselines());
        Set<Long> loadedIds = new java.util.TreeSet<>();
        for (BaselinePlan loaded : manager.getAllBaselines()) {
            loadedIds.add(loaded.getId());
        }
        Assertions.assertEquals(Set.of(41L, 42L), loadedIds,
                "the load publishes each row exactly once: " + loadedIds);
    }
}
