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
 * Round-69 review fixes on the no-op status statement:
 *
 * - #2 the ordering INTENT of a successful no-op must be durably confirmed. It used to be
 *      best effort: if the intent INSERT failed without committing, the ALTER still reported
 *      success - and a stalled opposite-status statement, whose shared window had long
 *      expired, could then land and reverse the completed ALTER. The failure now surfaces as
 *      a retryable error;
 * - #3 a successful no-op appends a version and nothing removed the earlier ones, so one
 *      baseline accumulated unbounded durable rows (and every cold match re-parsed all of
 *      them). The intent is now followed by a prune of the versions it supersedes, keeping
 *      everything strictly newer.
 */
public class SPMRound69ManagerTest {

    /** Durable store recording the intent insert and the prune request. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<BaselinePlan> inserted = new CopyOnWriteArrayList<>();
        /** (id, keepFromMicros) of every prune request. */
        final List<long[]> pruned = new CopyOnWriteArrayList<>();
        /** When true the intent INSERT fails without committing. */
        boolean failInserts;

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
            if (failInserts) {
                throw new RuntimeException("internal statement failed before commit");
            }
            inserted.add(plan);
            rows.put(plan.getId(), plan);
        }

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            return true;
        }

        @Override
        public void pruneSupersededVersions(BaselinePlan identity, long keepFromMicros) {
            pruned.add(new long[] {identity.getId(), keepFromMicros});
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            return new HashSet<>();
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

    private static BaselinePlan row(long id, BaselineStatus status, long updateTime) {
        BaselinePlan plan = new BaselinePlan();
        plan.setId(id);
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest("digest_a");
        plan.setBindSqlHash(0xA1L);
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

    /** Same-status ALTER on a cached, durably matching row: the no-op intent path. */
    private static BaselineManager cachedEnabledManager(TestStore store) {
        BaselineManager manager = newManager(store);
        BaselinePlan enabled = row(5L, BaselineStatus.ENABLED, 1_000L);
        store.rows.put(5L, enabled);
        manager.applyRefreshedBaselines(Map.of(5L, enabled));
        return manager;
    }

    // ==================== #2: a failed intent fails the ALTER retryably ====================

    /**
     * The intent INSERT fails before commit. Reporting success would leave no ordering
     * evidence at all: a DISABLE dispatched earlier (window expired, possibly across a
     * handoff) could land afterwards and reverse this ENABLE. The ALTER must fail
     * retryably instead.
     */
    @Test
    public void testFailedIntentFailsTheNoOpAlterRetryably() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.failInserts = true;

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertTrue(failure.getMessage().contains("retry the"),
                "the failure must be retryable: " + failure.getMessage());
        Assertions.assertTrue(store.inserted.isEmpty(),
                "precondition: nothing was written");
        Assertions.assertTrue(store.pruned.isEmpty(),
                "a failed intent must not prune versions either");
        Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(5L).getStatus(),
                "the cache state is untouched by the failed no-op");
    }

    // ==================== #3: the confirmed intent prunes superseded versions ====================

    /**
     * A successful no-op appends its ordering-intent row and then reclaims the versions it
     * supersedes: one baseline used to accumulate one durable row per ALTER forever, and
     * every cold match re-parsed all of them before collapsing by id.
     */
    @Test
    public void testConfirmedIntentPrunesSupersededVersions() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));

        Assertions.assertEquals(1, store.inserted.size(),
                "the successful no-op records ONE ordering-intent row");
        BaselinePlan intent = store.inserted.get(0);
        Assertions.assertEquals(BaselineStatus.ENABLED, intent.getStatus());
        Assertions.assertTrue(intent.getUpdateTime() > 1_000L,
                "the intent carries a strictly later stored instant: " + intent.getUpdateTime());

        Assertions.assertEquals(1, store.pruned.size(),
                "the superseded versions are reclaimed right after the intent is confirmed");
        long[] prune = store.pruned.get(0);
        Assertions.assertEquals(5L, prune[0]);
        Assertions.assertEquals(intent.getUpdateTime(), prune[1],
                "the prune keeps everything from the intent's own stored instant on, so the"
                        + " intent row and any NEWER concurrent version survive");
    }

    /** A second no-op keeps exactly one prune per statement (bounded growth). */
    @Test
    public void testRepeatedNoOpsStayBounded() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));

        Assertions.assertEquals(2, store.inserted.size());
        Assertions.assertEquals(2, store.pruned.size(),
                "every confirmed intent reclaims its predecessors");
        long first = store.pruned.get(0)[1];
        long second = store.pruned.get(1)[1];
        Assertions.assertTrue(second >= first,
                "the retained second never moves backwards: " + first + " -> " + second);
    }
}
