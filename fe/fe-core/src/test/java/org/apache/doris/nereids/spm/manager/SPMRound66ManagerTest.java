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
 * Round-66 review fixes on the manager:
 *
 * - #1 a status flip must publish to the row the CACHE holds NOW: the object the flip
 *   started on can be evicted while its detached status row is written, and mutating only
 *   that object left the reloaded cache entry ENABLED - warm matching returns a cached row
 *   directly (the mutation fence is consulted on a cache MISS), so queries kept replaying
 *   a successfully DISABLED baseline;
 * - #5 the same-key twin cleanup must use the read the LIVE store implements: production
 *   TablePersistence overrides only {@code readRowsByHash(long, int)}, while the plain
 *   overload defaults to an empty list on the interface, so the cleanup saw no twins on
 *   production and a handoff loser stayed durable after its winner was dropped.
 */
public class SPMRound66ManagerTest {

    /**
     * A durable table whose PLAIN by-hash read is left at the interface default, exactly
     * like the live adapter: only the TIMED overload answers. A by-hash read can be
     * scripted to hand out a FRESH object per call (modeling an eviction + reload).
     */
    private static final class TimedOnlyStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** One id whose identity DELETE fails (the best-effort leader repair case). */
        long failDeletesForId = -1L;
        /** When set, the timed read returns FRESH copies of the stored row (a reload). */
        boolean freshCopiesPerRead;

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

        /** ONLY the TIMED overload, like the production TablePersistence. */
        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            List<BaselinePlan> found = new ArrayList<>();
            for (BaselinePlan row : rows.values()) {
                if (row.getBindSqlHash() != hash) {
                    continue;
                }
                if (freshCopiesPerRead) {
                    BaselinePlan copy = new BaselinePlan();
                    copy.setId(row.getId());
                    copy.setBindSql(row.getBindSql());
                    copy.setBindSqlDigest(row.getBindSqlDigest());
                    copy.setBindSqlHash(row.getBindSqlHash());
                    copy.setPlanSql(row.getPlanSql());
                    copy.setSource(row.getSource());
                    copy.setStatus(row.getStatus());
                    copy.setSchemaFingerprint(row.getSchemaFingerprint());
                    copy.setCreateTime(row.getCreateTime());
                    copy.setUpdateTime(row.getUpdateTime());
                    found.add(copy);
                } else {
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

    // ==================== #1: publish to the row the cache holds now ====================

    /**
     * The flip starts on an object that is later EVICTED, and the same id is reloaded
     * (from the still-ENABLED durable row) into a DIFFERENT cache object before the flip
     * publishes. The publication must update the CURRENT cached row: mutating only the
     * stale object left warm matching to serve the replayed, successfully-disabled
     * baseline.
     */
    @Test
    public void testStatusPublishUpdatesTheCurrentCachedRow() {
        TimedOnlyStore store = new TimedOnlyStore();
        BaselineManager manager = newManager(store);
        BaselinePlan flipOrigin = row(5L, "digest_a", 0xA1L, BaselineStatus.ENABLED, 1_000L);
        store.rows.put(5L, flipOrigin);
        manager.applyRefreshedBaselines(Map.of(5L, flipOrigin));

        // an eviction + reload puts a DIFFERENT object of the same id into the cache
        BaselineManagerTestAccess.evictRowFromCacheOnly(manager, 5L);
        store.freshCopiesPerRead = true;
        manager.findCandidateBaselines("digest_a", 0xA1L);
        BaselinePlan current = BaselineManagerTestAccess.rawCacheGet(manager, 5L);
        Assertions.assertNotNull(current,
                "precondition: the reloaded object is cached");
        Assertions.assertNotSame(flipOrigin, current,
                "precondition: the cache holds a DIFFERENT object than the flip's origin");
        Assertions.assertEquals(BaselineStatus.ENABLED, current.getStatus());

        // the flip completes after the reload
        BaselineManagerTestAccess.publishStatus(manager, flipOrigin, BaselineStatus.DISABLED,
                2_000L);

        BaselinePlan published = BaselineManagerTestAccess.rawCacheGet(manager, 5L);
        Assertions.assertNotNull(published);
        Assertions.assertEquals(BaselineStatus.DISABLED, published.getStatus(),
                "the CURRENT cached row must receive the confirmed flip: warm matching"
                        + " returns it directly and never consults the mutation fence");
        Assertions.assertEquals(2_000L, published.getUpdateTime());
        Assertions.assertTrue(manager.findCandidateBaselines("digest_a", 0xA1L).isEmpty(),
                "the disabled baseline must stop matching");
    }

    // ==================== #5: the twin cleanup uses the reader the live store implements ====================

    /**
     * The same-key twins at 40 and 41: the leader repair of the refresh is best-effort and
     * fails, so both rows stay durable and matching exposes the winner 41. Dropping 41 must
     * also retire 40 - with the PLAIN by-hash read left unimplemented (exactly like
     * production), only a caller that uses the TIMED overload can see the loser.
     */
    @Test
    public void testTwinRetirementUsesTheTimedByHashRead() {
        TimedOnlyStore store = new TimedOnlyStore();
        BaselineManager manager = newManager(store);
        BaselinePlan twin = row(40L, "digest_t", 0x71L, BaselineStatus.ENABLED, 1_000L);
        BaselinePlan winner = row(41L, "digest_t", 0x71L, BaselineStatus.ENABLED, 2_000L);
        store.rows.put(40L, twin);
        store.rows.put(41L, winner);
        // the leader repair cannot retire the loser in this window
        store.failDeletesForId = 40L;
        manager.applyRefreshedBaselines(Map.of(40L, twin, 41L, winner));
        store.failDeletesForId = -1L;
        Assertions.assertTrue(store.rows.containsKey(40L),
                "precondition: the best-effort repair could not retire the twin");

        Assertions.assertTrue(manager.dropBaseline(41L));
        Assertions.assertFalse(store.rows.containsKey(40L),
                "the DROP must read the durable rows through the TIMED overload (the plain"
                        + " one defaults to an empty list) and retire the superseded twin");
        Assertions.assertTrue(
                store.tombstones.stream().anyMatch(t -> t.startsWith("40|digest_t|")),
                "the twin's identity gets its own tombstone");
    }
}
