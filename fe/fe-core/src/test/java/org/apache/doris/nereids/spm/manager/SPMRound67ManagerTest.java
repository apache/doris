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
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Round-67 review fixes on the manager:
 *
 * - #2 the DROP's durable confirmation must include the SAME-KEY TWINS it retired: their
 *   DELETE runs (the wrong-overload fix), but their own resurrection fence was only best
 *   effort - when the twin's marker append failed while the removed row's markers were
 *   readable, the DROP reported success and a demoted master's already-dispatched status
 *   INSERT of the twin could publish, reviving the dropped baseline on another FE / after
 *   a restart. The retired identities now ride in the DROP's confirmation AND in its
 *   retained retry obligation;
 * - #4 a refresh must reuse the cached SQL trees for a SCALAR-ONLY change: a status-only
 *   update used to replace the cached object with a reparsed one (deepenPersistedRow under
 *   the cache monitor), so a follower refreshing many disabled baselines rebuilt their SQL
 *   serially under the very monitor every warm query needs (getComplete).
 */
public class SPMRound67ManagerTest {

    /** A durable table whose only by-hash read is the TIMED overload (like production). */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** One id whose identity DELETE fails (the best-effort leader repair case). */
        long failDeletesForId = -1L;
        /** One id whose DROP tombstone append fails (best effort on the wire). */
        long failMarkerForId = -1L;

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
            if (id == failMarkerForId) {
                return false; // the append did not land
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
        public long tableWatermark() {
            long max = 0;
            for (BaselinePlan row : rows.values()) {
                max = Math.max(max, row.getId());
            }
            return max;
        }
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
        plan.setSchemaFingerprint("t1|100|k:int|NN");
        plan.setCreateTime(updateTime);
        plan.setUpdateTime(updateTime);
        return plan;
    }

    private static BaselineManager newManager(BaselinePersistence store) {
        BaselineManager created = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(created);
        return created;
    }

    // ==================== #2: the twins' tombstones are part of the DROP obligation ====================

    /**
     * The same-key twins at 40/41 with a failed best-effort repair. The twin's DROP marker
     * append FAILS while the removed row 41's markers are readable: the DROP must NOT
     * report success (the twin's resurrection fence is unproven) and must retain the
     * twin identity in its obligation; the retry - which finds row 41 already gone - then
     * re-appends and CONFIRMS the twin's marker before settling.
     */
    @Test
    public void testTwinTombstoneParticipatesInTheDropConfirmation() {
        TestStore store = new TestStore();
        BaselineManager manager = newManager(store);
        BaselinePlan twin = row(40L, "digest_t", 0x71L, BaselineStatus.ENABLED, 1_000L);
        BaselinePlan winner = row(41L, "digest_t", 0x71L, BaselineStatus.ENABLED, 2_000L);
        store.rows.put(40L, twin);
        store.rows.put(41L, winner);
        store.failDeletesForId = 40L; // the leader repair of the refresh fails
        manager.applyRefreshedBaselines(Map.of(40L, twin, 41L, winner));
        store.failDeletesForId = -1L;
        Assertions.assertTrue(store.rows.containsKey(40L),
                "precondition: the best-effort repair could not retire the twin");

        // the twin's own tombstone append does not land
        store.failMarkerForId = 40L;
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.dropBaseline(41L));
        Assertions.assertTrue(failure.getMessage().contains("cannot confirm the drop tombstones"),
                "the DROP must fail retryably on the unconfirmed twin marker: "
                        + failure.getMessage());
        Assertions.assertFalse(store.rows.containsKey(40L),
                "the twin's DELETE itself did run (only its tombstone is unproven)");
        Assertions.assertTrue(
                BaselineManagerTestAccess.owesDropTombstoneConfirmation(41L),
                "the obligation is retained for the retry");

        // the retry: the row is gone, and the RETAINED obligation re-appends and confirms
        // the twin's marker before settling
        store.failMarkerForId = -1L;
        Assertions.assertFalse(manager.dropBaseline(41L),
                "the retry finds the row already deleted");
        Assertions.assertTrue(
                store.tombstones.stream().anyMatch(t -> t.startsWith("40|digest_t|")),
                "the twin's resurrection fence must be appended by the retry obligation: "
                        + store.tombstones);
        Assertions.assertFalse(
                BaselineManagerTestAccess.owesDropTombstoneConfirmation(41L),
                "the settled obligation is released");
    }

    // ==================== #4: a scalar-only refresh reuses the cached trees ====================

    /**
     * A status-only refresh: the persisted row changes ONLY in its status (and timestamps),
     * so the cached object's parsed trees stay valid. The merge must adopt the scalars into
     * the cached object; replacing it with a reparsed one put the deepen work under the
     * cache monitor that every warm query needs.
     */
    @Test
    public void testStatusOnlyRefreshKeepsTheCachedObject() {
        TestStore store = new TestStore();
        BaselineManager manager = newManager(store);
        BaselinePlan enabled = row(5L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        store.rows.put(5L, enabled);
        manager.applyRefreshedBaselines(Map.of(5L, enabled));
        manager.findCandidateBaselines("digest_a", 0xA1L);
        BaselinePlan cached = BaselineManagerTestAccess.rawCacheGet(manager, 5L);
        Assertions.assertNotNull(cached, "precondition: the row is cached");

        // another FE disabled it: same SQL / identity, only the scalars moved
        BaselinePlan disabled = row(5L, "digest_a", 0xA1L, BaselineStatus.DISABLED, 2_000L);
        store.rows.put(5L, disabled);
        manager.applyRefreshedBaselines(Map.of(5L, disabled));

        BaselinePlan after = BaselineManagerTestAccess.rawCacheGet(manager, 5L);
        Assertions.assertSame(cached, after,
                "a status-only refresh must ADOPT the persisted scalars into the cached"
                        + " object instead of replacing it with a reparsed one under the"
                        + " cache monitor");
        Assertions.assertEquals(BaselineStatus.DISABLED, after.getStatus());
        Assertions.assertEquals(2_000L, after.getUpdateTime());
        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                "the refreshed DISABLED row must stop matching");
    }
}
