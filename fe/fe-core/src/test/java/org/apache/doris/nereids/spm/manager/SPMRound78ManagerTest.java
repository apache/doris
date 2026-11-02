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
 * Round-78 review fixes on the manager.
 *
 * <ul>
 *   <li>#2 a REAL status flip must respect the stored-second lead bound too: an unbounded bump let
 *       a visible row lead wall time past every shared fence, so after a handoff a no-op ENABLE
 *       could tie with the still-unreadable DISABLE and lose the tie-break;</li>
 *   <li>#6 the resumed DROP key cleanup is bounded by the ID high-water mark, not by a timestamp:
 *       a future-dated preexisting twin was skipped and a same-second CREATE was deleted;</li>
 *   <li>the refresh drives its epoch advance from ONE changed-hash set derived from the durable
 *       content signature, which also covers hashes with no cached entry.</li>
 * </ul>
 */
public class SPMRound78ManagerTest {

    /** A durable store modelling rows, tombstones and the mutation clock. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final Set<String> tombstones = ConcurrentHashMap.newKeySet();
        final List<Long> pendingTicks = new java.util.concurrent.CopyOnWriteArrayList<>();
        final List<Long> closedWindows = new java.util.concurrent.CopyOnWriteArrayList<>();
        long failDeletesForId = -1L;
        long newestStoredMicros;
        long idWatermark;
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
        public long newestStoredUpdateMicros(long id) {
            return newestStoredMicros;
        }

        @Override
        public long readHwmRecord() {
            return idWatermark;
        }

        @Override
        public long readSequenceTail(long floor) {
            return 0L;
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

    // ==================== #2: the real flip respects the lead bound ==================

    /**
     * The REAL flip used to advance the stored second by itself, bypassing the lead bound. A burst
     * of alternating ALTERs could then leave a visible row whose stored second was far ahead of
     * wall time, while every fence covering its publication is wall-clock bounded - the exact
     * schedule in which a later no-op ENABLE ties with the unreadable DISABLE and the DISABLED
     * tie-break reverses it.
     */
    @Test
    public void testRealFlipRefusesAnUnboundedStoredInstantLead() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));
        // the id's newest stored instant is far ahead of wall time
        store.newestStoredMicros = System.currentTimeMillis() * 1000L + 3_600_000_000L;

        IllegalStateException error = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.updateStatus(41L, BaselineStatus.DISABLED),
                "a REAL flip must fail retryably instead of writing a row no fence can cover");
        Assertions.assertTrue(error.getMessage().contains("lead"),
                "the failure names the unbounded lead: " + error.getMessage());
    }

    /** A lead inside the bound keeps the ordinary flip working. */
    @Test
    public void testRealFlipAcceptsABoundedStoredInstantLead() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));
        store.newestStoredMicros = System.currentTimeMillis() * 1000L + 1_000_000L;

        Assertions.assertTrue(manager.updateStatus(41L, BaselineStatus.DISABLED),
                "a one-second lead is the ordinary bump and must still succeed");
    }

    // ==================== #6: the DROP cleanup is bounded by the id watermark ========

    /**
     * A PREEXISTING twin whose stored update time is in the FUTURE (the flip pushes it there) is
     * still a twin of the DROP and must be retired: a timestamp bound skipped it and left it
     * durable and matchable.
     */
    @Test
    public void testResumedCleanupRetiresAFutureDatedPreexistingTwin() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.idWatermark = 42L;
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));
        // the twin's stored second lies in the future (a burst of flips pushed it there)
        store.rows.get(42L).setUpdateTime(System.currentTimeMillis() + 3_600_000L);
        store.failDeletesForId = 42L;

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        Assertions.assertTrue(BaselineManagerTestAccess.retainsDropKeyCleanup(41L),
                "precondition: the failed attempt kept the cleanup");

        store.failDeletesForId = -1L;
        Assertions.assertFalse(manager.dropBaseline(41L), "the row of 41 is already gone");
        Assertions.assertNull(store.rows.get(42L),
                "a twin that existed BEFORE the DROP must be retired whatever its stored second");
    }

    /**
     * A CREATE allocated AFTER the DROP is kept even when its stored second equals the retention
     * instant: allocation order, not a second-precision timestamp, defines "existed then".
     */
    @Test
    public void testResumedCleanupKeepsABaselineAllocatedAfterTheDrop() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        store.idWatermark = 41L;
        store.rows.put(41L, row(41L, "digest_a", BaselineStatus.ENABLED));
        store.failDeletesForId = 42L;
        store.rows.put(42L, row(42L, "digest_a", BaselineStatus.ENABLED));

        Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(41L));
        store.failDeletesForId = -1L;
        // DATETIME keeps second precision, so a CREATE landing after the DROP can carry the SAME
        // stored second as the retention instant - only the allocation order tells them apart
        Assertions.assertFalse(manager.dropBaseline(41L));
        Assertions.assertNotNull(store.rows.get(42L),
                "the allocation order decides: an id above the watermark is a NEW baseline");
    }

    // ==================== the unified changed-hash set ===============================

    /**
     * A refresh that only changes a COLD hash (no cached entry) must still advance that hash's
     * epoch: the changed set comes from the durable content signature, not from the cached row
     * count.
     */
    @Test
    public void testColdHashChangeAdvancesItsEpoch() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan enabled = row(41L, "digest_a", BaselineStatus.ENABLED);
        store.rows.put(41L, enabled);
        manager.applyRefreshedBaselines(Map.of(41L, enabled));
        long before = BaselineManagerTestAccess.knownHashEpoch(manager, enabled.getBindSqlHash());

        // the hash has NO cached entry (a durable refresh only reconciles EXISTING entries), so
        // the durable content signature is the only witness of the change
        BaselinePlan disabled = row(41L, "digest_a", BaselineStatus.DISABLED);
        store.rows.put(41L, disabled);
        manager.applyRefreshedBaselines(Map.of(41L, disabled));

        Assertions.assertTrue(BaselineManagerTestAccess.knownHashEpoch(manager, enabled.getBindSqlHash()) > before,
                "a changed hash must advance its epoch whether or not it was cached");
    }

    /** An unchanged refresh leaves the epoch alone (no blanket bump any more). */
    @Test
    public void testUnchangedRefreshKeepsTheEpoch() {
        TestStore store = new TestStore();
        BaselineManager manager = manager(store);
        BaselinePlan enabled = row(41L, "digest_a", BaselineStatus.ENABLED);
        store.rows.put(41L, enabled);
        manager.applyRefreshedBaselines(Map.of(41L, enabled));
        long before = BaselineManagerTestAccess.knownHashEpoch(manager, enabled.getBindSqlHash());

        manager.applyRefreshedBaselines(Map.of(41L, store.rows.get(41L)));

        Assertions.assertEquals(before, BaselineManagerTestAccess.knownHashEpoch(manager, enabled.getBindSqlHash()),
                "an unchanged snapshot must not kill the epochs of unchanged hashes");
    }
}
