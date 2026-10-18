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
 * Round-71 review fixes on the no-op status statement and its mutation window:
 *
 * <ul>
 *   <li>#9 the version PRUNE is a DELETE: SQL OK only proves it was DISPATCHED. A paginated
 *       snapshot reader still holding a pruned row steps past the intent row behind it, so
 *       the window must stay open until the reclaim is provably visible;</li>
 *   <li>#10a a held window is NOT satisfied by a row that merely carries the same STATUS: a
 *       successful no-op intent has the same status as the row it supersedes, so the older
 *       row closed the window while the newer intent was still unreadable;</li>
 *   <li>#10b an AMBIGUOUS intent insert (an error that may have been raised after a commit)
 *       keeps the shared window open instead of leaving only a local fence.</li>
 * </ul>
 */
public class SPMRound71ManagerTest {

    /** Durable store modelling the shared high-water-mark window and the prune probe. */
    private static final class TestStore implements BaselinePersistence {
        final Map<Long, BaselinePlan> rows = new ConcurrentHashMap<>();
        final List<BaselinePlan> inserted = new CopyOnWriteArrayList<>();
        /** (id, keepFromSecond) of every prune request. */
        final List<long[]> pruned = new CopyOnWriteArrayList<>();
        /** Append-only high-water-mark log: {tick, pending}. */
        final List<long[]> hwm = new CopyOnWriteArrayList<>();
        /** When true the intent INSERT reports success but its row is never READABLE. */
        boolean intentUnreadable;
        /** When true every INSERT reports an error that may hide a commit. */
        boolean ambiguousInsert;
        /** How many reclaimed versions the prune probe still reads (0 = provably gone). */
        long readableReclaimedVersions;

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
            if (ambiguousInsert) {
                throw new RuntimeException("insert reported an error (statement timed out)");
            }
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
            pruned.add(new long[] {identity.getId(), keepFromSecond});
        }

        @Override
        public long countVersionsBefore(BaselinePlan identity, long keepFromSecond) {
            return readableReclaimedVersions;
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

        /** The window is OPEN while the last high-water-mark row is still pending. */
        boolean windowOpen() {
            return !hwm.isEmpty() && hwm.get(hwm.size() - 1)[1] != 0;
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

    // ==================== #9: an unprovable prune keeps the window open ====================

    /**
     * The DELETE returned OK, but the reclaimed rows are still readable: a paginated reader
     * positioned on one of them steps past the intent row behind it, its quiet clock and
     * empty end probe accept the stale page, and a warm cache is overwritten with
     * pre-intent rows.
     */
    @Test
    public void testUnprovablePruneKeepsTheSharedWindowOpen() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.readableReclaimedVersions = 3L;

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertEquals(1, store.pruned.size(), "the prune still runs");
        Assertions.assertTrue(store.windowOpen(),
                "an unproven reclaim must keep the shared window OPEN");
        Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "the window is tracked so a later read can prove the reclaim");
    }

    /** A prune whose reclaim IS visible closes the window as before (no false hold). */
    @Test
    public void testVisiblePruneClosesTheWindow() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertEquals(1, store.pruned.size());
        Assertions.assertFalse(store.windowOpen(),
                "a provably reclaimed prune closes the window");
        Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow());
    }

    // ==================== #10a: the same STATUS is not the outcome ====================

    /**
     * A successful no-op intent carries the SAME status as the row it supersedes. When the
     * snapshot still shows the OLDER same-status row the window used to close, and the
     * successor it fenced could then qualify an opposite-status write against a row the
     * unreadable intent (a LATER stored second) wins over.
     */
    @Test
    public void testOlderSameStatusRowDoesNotSatisfyTheHeldWindow() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.intentUnreadable = true;

        Assertions.assertTrue(manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "the unreadable intent holds the window");

        // the snapshot still shows the OLDER same-status row (the pre-intent version)
        manager.applyRefreshedBaselines(Map.of(5L, row(5L, BaselineStatus.ENABLED, 1_000L)));
        Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "a same-status row with an OLDER stored second must NOT satisfy the window");

        // once the intent row itself becomes readable the window settles
        BaselinePlan intent = store.inserted.get(0);
        rowsPublish(store, intent);
        manager.applyRefreshedBaselines(Map.of(5L, intent));
        Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                "the intent row satisfies the window and it closes");
    }

    // ==================== #10b: an ambiguous insert keeps the window ====================

    /**
     * An INSERT that reported an error may have been raised AFTER a commit, and no read can
     * prove it. The statement fails retryably, but the shared window must stay unresolved -
     * a local fence alone lets another FE qualify an opposite-status write that the
     * possibly-committed row then reverses.
     */
    @Test
    public void testAmbiguousInsertKeepsTheSharedWindowOpen() {
        TestStore store = new TestStore();
        BaselineManager manager = cachedEnabledManager(store);
        store.ambiguousInsert = true;

        Assertions.assertThrows(RuntimeException.class,
                () -> manager.updateStatus(5L, BaselineStatus.ENABLED));
        Assertions.assertTrue(store.windowOpen(),
                "an ambiguous intent insert must keep the shared window OPEN");
        Assertions.assertEquals(BaselineStatus.ENABLED,
                manager.getBaseline(5L).getStatus(),
                "the cached status is untouched by the failed statement");
    }

    private static void rowsPublish(TestStore store, BaselinePlan intent) {
        store.intentUnreadable = false;
        store.rows.put(intent.getId(), intent);
    }
}
