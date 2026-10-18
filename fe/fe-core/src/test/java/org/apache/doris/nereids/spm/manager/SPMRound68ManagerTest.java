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
 * Round-68 review fixes on the manager:
 *
 * - #2 a DROP that retires a SAME-KEY TWIN durably must evict that twin from the LOCAL
 *      cache as well, with its publication epoch. A warm bucket can hold only the twin
 *      while the dropped id is not cached at all; completeRowsForHash serves a warm bucket
 *      BEFORE any tombstone / fence reconciliation, so the retired twin kept being
 *      replayed until the next refresh or eviction;
 * - #5 the conditional status INSERT carries the DURABLE-ORDER term: no newer row of the
 *      id may exist. A status statement that stalled past its shared mutation window can
 *      no longer land after a successor's successful no-op and reverse it (the successor
 *      also records its own intent, see persistNoOpStatusIntent).
 */
public class SPMRound68ManagerTest {

    /** A durable table whose only by-hash read is the TIMED overload (like production). */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<String> tombstones = new CopyOnWriteArrayList<>();
        /** One id whose identity DELETE fails (the best-effort leader repair case). */
        long failDeletesForId = -1L;
        /** Rows appended by the unconditional insert path (see persistNoOpStatusIntent). */
        final List<BaselinePlan> intentRows = new CopyOnWriteArrayList<>();

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
            intentRows.add(plan);
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

    // ==================== #2: the retired twin leaves the warm bucket ====================

    /**
     * The warm cache holds ONLY the twin (id 40): the row the user drops (id 41) was never
     * cached. The cache-miss DROP retires the twin durably and confirms both identities -
     * before the fix nothing removed the cached 40, and warm matching (which serves the
     * complete bucket without any tombstone reconciliation) kept replaying a baseline the
     * DROP had reported removed.
     */
    @Test
    public void testDroppedTwinLeavesTheWarmBucket() {
        TestStore store = new TestStore();
        BaselineManager manager = newManager(store);
        BaselinePlan twin = row(40L, "digest_t", 0x71L, BaselineStatus.ENABLED, 1_000L);

        // the twin is the only row this FE has loaded / cached for the key
        store.rows.put(40L, twin);
        manager.applyRefreshedBaselines(Map.of(40L, twin));
        manager.findCandidateBaselines("digest_t", 0x71L);
        Assertions.assertEquals(1, manager.findCandidateBaselines("digest_t", 0x71L).size(),
                "precondition: the key matches through the warm bucket");

        // the winner appears later (handoff twin) and is NOT cached here
        BaselinePlan winner = row(41L, "digest_t", 0x71L, BaselineStatus.ENABLED, 2_000L);
        store.rows.put(41L, winner);

        Assertions.assertTrue(manager.dropBaseline(41L),
                "the cache-miss DROP must retire the durable rows and its twin");

        Assertions.assertFalse(store.rows.containsKey(40L),
                "precondition: the twin's durable row was retired by the DROP");
        Assertions.assertNull(BaselineManagerTestAccess.rawCacheGet(manager, 40L),
                "the retired twin must LEAVE the cache: a warm bucket bypasses every"
                        + " tombstone / fence reconciliation");
        Assertions.assertTrue(manager.findCandidateBaselines("digest_t", 0x71L).isEmpty(),
                "queries must not keep replaying a baseline the DROP removed");
    }

    // ==================== #5: the flip is conditional on the durable ORDER ====================

    /**
     * Without the durable-order term a stalled status statement finds its previous-status
     * precondition intact (the old row is still physically there in the DUPLICATE-key table)
     * and lands with a stored second derived from the row it saw - reversing a successor's
     * successful no-op ENABLE. The term requires that no newer row of the id exists.
     */
    @Test
    public void testConditionalStatusInsertCarriesTheDurableOrderTerm() {
        String unbound = BaselineManager.conditionalStatusInsertSql(0L);
        Assertions.assertFalse(unbound.contains("NOT EXISTS"),
                "an unknown stored second cannot express the ordering term: " + unbound);
        Assertions.assertTrue(unbound.contains("`status` = '${previousStatus}'"),
                "the previous-status predicate must stay: " + unbound);

        String bounded = BaselineManager.conditionalStatusInsertSql(1_700_000_000L);
        Assertions.assertTrue(bounded.contains("NOT EXISTS"),
                "the flip must require that no NEWER row of the id exists: " + bounded);
        Assertions.assertTrue(bounded.contains("UNIX_TIMESTAMP(`update_time`)"),
                "the comparison is on the stored second (the table's ordering key): " + bounded);
        Assertions.assertTrue(bounded.contains("> ${previousUpdateSecond}"),
                "the statement is bound to the state it flips from: " + bounded);
        Assertions.assertTrue(bounded.trim().endsWith("LIMIT 1"),
                "the ordering term must precede the LIMIT: " + bounded);
    }

    /**
     * The unbounded form is what the default (non-durable) stores see: a substituted store
     * that only models the previous-status conditional keeps its historic semantics.
     */
    @Test
    public void testUnboundedConditionalInsertIsUnchanged() {
        Assertions.assertEquals(
                BaselineManager.conditionalStatusInsertSql(0L),
                BaselineManager.conditionalStatusInsertSql(-1L),
                "a non-positive stored second is 'unknown' and adds no ordering term");
    }
}
