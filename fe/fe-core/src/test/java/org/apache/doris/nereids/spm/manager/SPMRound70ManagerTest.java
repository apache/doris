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
 * Round-70 review fixes on the no-op status statement:
 *
 * - #1 the version PRUNE is a DELETE that changes the row set a PAGINATED snapshot reader
 *      walks (`id >= lastId LIMIT n OFFSET m`), so it must run inside the SHARED mutation
 *      window: a follower that ended page 1 on a pruned row continues past the row it still
 *      holds, and its quiet clock / empty end probe would accept the stale page;
 * - #2 a committed-but-UNREADABLE intent must keep its shared window open (the INSERT used to
 *      close it and leave only this FE's local fence): a successor that sees the previous
 *      same-status row with a quiet clock could otherwise qualify an opposite-status write
 *      against it, and the unreadable intent (a later stored second) would win and reverse
 *      the successful ALTER;
 * - #7 an unreadable NEWEST stored second must fail the ALTER retryably: rapid ALTERs move
 *      the stored second beyond wall time, so the wall-clock fallback could leave an older
 *      delayed opposite-status row eligible.
 */
public class SPMRound70ManagerTest {

    /** Durable store modelling the shared high-water-mark window and the prune request. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<BaselinePlan> inserted = new CopyOnWriteArrayList<>();
        /** (id, keepFromSecond) of every prune request. */
        final List<long[]> pruned = new CopyOnWriteArrayList<>();
        /** Whether the prune ran while a shared window was OPEN. */
        boolean pruneRanInsideWindow;
        /** Append-only high-water-mark log: {tick, pending}. */
        final List<long[]> hwm = new CopyOnWriteArrayList<>();
        /** When true the intent INSERT reports success but its row is never READABLE. */
        boolean intentUnreadable;
        /** When true reading the newest stored second of the id fails. */
        boolean newestSecondUnreadable;

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
            inserted.add(plan);
            if (!intentUnreadable) {
                rows.put(plan.getId(), plan);
            }
        }

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            return !intentUnreadable;
        }

        @Override
        public long newestStoredUpdateSecond(long id) {
            if (newestSecondUnreadable) {
                throw new RuntimeException("internal read failed");
            }
            return 0;
        }

        @Override
        public long[] slotState() {
            long maxTick = 0;
            for (long[] row : hwm) {
                maxTick = Math.max(maxTick, row[0]);
            }
            return new long[] {0L, maxTick};
        }

        @Override
        public void appendHwmRecord(long lastId, long tick, long pending) {
            hwm.add(new long[] {tick, pending});
        }

        @Override
        public void pruneSupersededVersions(BaselinePlan identity, long keepFromSecond) {
            pruneRanInsideWindow = windowOpen();
            pruned.add(new long[] {identity.getId(), keepFromSecond});
        }

        /** The window is OPEN while the last high-water-mark row is still pending. */
        boolean windowOpen() {
            return !hwm.isEmpty() && hwm.get(hwm.size() - 1)[1] != 0;
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

    private static BaselineManager cachedEnabledManager(TestStore store) {
        BaselineManager manager = new BaselineManager(store,
                new BaselineTestDoubles.TestClusterGateway(), System::currentTimeMillis);
        BaselineManagerTestAccess.clear(manager);
        BaselinePlan enabled = row(5L, BaselineStatus.ENABLED, 1_000L);
        store.rows.put(5L, enabled);
        manager.applyRefreshedBaselines(Map.of(5L, enabled));
        return manager;
    }

    // ==================== #1: the prune runs inside the shared window ====================

    /**
     * The prune is a DELETE that changes the row set a paginated reader walks, so it must be
     * bracketed by the shared mutation protocol - exactly like the flip's own writes.
     */
    @Test
    public void testPruneRunsInsideTheSharedMutationWindow() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));

        Assertions.assertEquals(1, store.pruned.size(), "the intent is confirmed and pruned");
        Assertions.assertTrue(store.pruneRanInsideWindow,
                "the DELETE must run while the shared mutation window is OPEN, or a"
                        + " paginated snapshot can step past the row it removed");
        Assertions.assertFalse(store.windowOpen(),
                "the window is closed once the intent and the prune completed");
    }

    // ==================== #2: an unreadable intent keeps the window open ====================

    /**
     * The INSERT reported success (so its row is durable) but no read sees it yet. The ALTER
     * stays successful, but the SHARED window must stay unresolved: only a local fence
     * would let a successor on another FE qualify an opposite-status write against the
     * previous same-status row.
     */
    @Test
    public void testUnreadableIntentKeepsTheSharedWindowOpen() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.intentUnreadable = true;

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED),
                "the committed-but-lagging intent still lets the statement succeed");
        Assertions.assertEquals(1, store.inserted.size());
        Assertions.assertTrue(store.pruned.isEmpty(),
                "an unreadable intent is not pruned (no read can order it yet)");
        Assertions.assertTrue(store.windowOpen(),
                "the shared window must stay OPEN until a snapshot shows the intent row");
    }

    // ==================== #7: an unreadable newest second fails closed ====================

    /**
     * Rapid ALTERs advance the stored second beyond wall time, so a wall-clock fallback could
     * silently produce an intent OLDER than a delayed opposite-status row. An unreadable
     * newest second must fail the ALTER retryably instead.
     */
    @Test
    public void testUnreadableNewestSecondFailsTheAlterRetryably() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.newestSecondUnreadable = true;

        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertTrue(failure.getMessage().contains("retry the ALTER"),
                "the failure must be retryable: " + failure.getMessage());
        Assertions.assertTrue(store.inserted.isEmpty(),
                "nothing may be written without the ordering input");
        Assertions.assertTrue(store.hwm.isEmpty(),
                "no mutation window is opened for a statement that never ran");
        Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(5L).getStatus());
    }
}
