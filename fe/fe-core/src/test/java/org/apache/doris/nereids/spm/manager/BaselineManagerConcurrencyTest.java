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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Tenth review round: master-handoff safety of the global baseline store.
 *
 * - Id allocation is collision-safe: an already-running forwarded CREATE / capture cycle
 *   dispatched by an OLD master is fenced by the CURRENT leadership, and when the probe
 *   still meets a competing row on the same id (DUPLICATE KEY(id) table) both sides
 *   deterministically keep the SAME winner - the loser re-allocates from a fresh
 *   watermark instead of leaving two unrelated rows under one id (the latch-driven
 *   handoff scenario is simulated through the allocator seam).
 * - The promotion reload is fenced against concurrent writers AND against loads whose
 *   snapshot read started before the invalidation: a DROP that commits while the maps
 *   are empty must never be resurrected by a stale snapshot (the DROP finds no map entry
 *   and skips its version bump, so only a generation check can reject it).
 */
public class BaselineManagerConcurrencyTest {

    private static BaselinePlan baseline(String digest, String planSql) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(7);
        plan.setPlanSql(planSql);
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        return plan;
    }

    /** In-memory simulation of the shared durable store (two masters, lock-free). */
    private static final class SimulatedStore implements BaselineManager.IdAllocatorStoreForTest {
        private final Map<Long, List<BaselinePlan>> rows = new ConcurrentHashMap<>();
        /** When set, the FIRST by-id probe blocks on it until released. */
        private CountDownLatch probeEntered;
        private CountDownLatch probeRelease;
        /** When set, every identity delete blocks on it until released. */
        private CountDownLatch deleteEntered;
        private CountDownLatch deleteRelease;
        /**
         * The "other master" row to inject into the FIRST by-id probe, under the id the
         * probe actually asks for (the id generator state depends on the test order).
         */
        private Supplier<BaselinePlan> foreignSupplier;
        private boolean injectOnFirstProbe;
        private long injectedId = -1;

        @Override
        public long watermark() {
            long max = 0;
            for (List<BaselinePlan> idRows : rows.values()) {
                for (BaselinePlan row : idRows) {
                    max = Math.max(max, row.getId());
                }
            }
            return max;
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.compute(plan.getId(), (id, current) -> {
                List<BaselinePlan> updated = current == null
                        ? new ArrayList<>() : new ArrayList<>(current);
                updated.add(plan);
                return updated;
            });
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (probeEntered != null) {
                probeEntered.countDown();
                try {
                    probeRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (injectOnFirstProbe && foreignSupplier != null) {
                // the other master's INSERT lands between this master's INSERT and probe
                BaselinePlan foreign = foreignSupplier.get();
                foreign.setId(id);
                injectedId = id;
                injectOnFirstProbe = false;
                foreignSupplier = null;
                insert(foreign);
            }
            return new ArrayList<>(rows.getOrDefault(id, List.of()));
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (deleteEntered != null) {
                deleteEntered.countDown();
                try {
                    deleteRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            rows.computeIfPresent(plan.getId(), (id, current) -> {
                List<BaselinePlan> updated = new ArrayList<>(current);
                updated.removeIf(row -> row.getBindSqlDigest().equals(plan.getBindSqlDigest())
                        && row.getPlanSql().equals(plan.getPlanSql()));
                return updated.isEmpty() ? null : updated;
            });
        }

        private List<BaselinePlan> rowsOf(long id) {
            return new ArrayList<>(rows.getOrDefault(id, List.of()));
        }
    }

    // ==================== #1: deterministic id-collision resolution ====================

    @Test
    public void testSmallerIdentityKeepsTheContestedId() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        store.injectOnFirstProbe = true;
        store.foreignSupplier = () -> baseline("zz", "select 2"); // larger identity loses
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            BaselinePlan mine = baseline("aa", "select 1");  // smaller identity wins

            long id = manager.createBaseline(mine);
            Assertions.assertEquals(store.injectedId, id,
                    "the smaller (digest, planSql) identity keeps the contested id");
            List<BaselinePlan> durable = store.rowsOf(id);
            Assertions.assertEquals(1, durable.size(),
                    "exactly one row must survive the resolved collision: " + durable);
            Assertions.assertEquals("aa", durable.get(0).getBindSqlDigest(),
                    "the competing row must be repaired away");
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "the store must publish exactly the surviving baseline");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    @Test
    public void testLosingCreateReallocatesAboveTheWatermark() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        store.injectOnFirstProbe = true;
        store.foreignSupplier = () -> baseline("aa", "select 1"); // smaller identity wins
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long id = manager.createBaseline(baseline("zz", "select 2"));
            long contested = store.injectedId;
            Assertions.assertTrue(contested > 0, "the probe must have injected the competing row");
            Assertions.assertTrue(id > contested,
                    "the losing create must re-allocate from a fresh watermark, got " + id);
            List<BaselinePlan> contestedRows = store.rowsOf(contested);
            Assertions.assertEquals(1, contestedRows.size(),
                    "the other master's row must stay untouched under the contested id");
            Assertions.assertEquals("aa", contestedRows.get(0).getBindSqlDigest());
            Assertions.assertEquals(1, store.rowsOf(id).size(),
                    "the re-allocated row must be durable under its new id");
            Assertions.assertEquals("zz", store.rowsOf(id).get(0).getBindSqlDigest());
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "the store must publish exactly the surviving baseline");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Latch-driven handoff: both masters read the same watermark and allocate N+1; the
     * second INSERT lands while the first create is INSIDE its collision probe. The two
     * sides must converge on the same winner deterministically - here the OTHER master
     * (smaller digest) wins, so this create re-allocates instead of leaving two rows
     * under the contested id.
     */
    @Test
    public void testHandoffLatchKeepsExactlyOneRowPerId() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        store.probeEntered = new CountDownLatch(1);
        store.probeRelease = new CountDownLatch(1);
        store.injectOnFirstProbe = true;
        store.foreignSupplier = () -> baseline("aa", "select 1");
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long[] createdId = new long[1];
            Thread create = new Thread(() -> createdId[0] = manager.createBaseline(
                    baseline("zz", "select 2")));
            create.start();
            Assertions.assertTrue(store.probeEntered.await(5, TimeUnit.SECONDS),
                    "the create must reach the collision probe");

            // the other master's row lands INSIDE the blocked probe (same MAX(id) start)
            store.probeRelease.countDown();
            create.join(10_000);
            Assertions.assertFalse(create.isAlive(), "the fenced create must finish");

            long contested = store.injectedId;
            Assertions.assertTrue(contested > 0, "the probe must have injected the competing row");
            Assertions.assertEquals(1, store.rowsOf(contested).size(),
                    "the contested id must end up with exactly one row: "
                            + store.rowsOf(contested));
            Assertions.assertEquals("aa", store.rowsOf(contested).get(0).getBindSqlDigest(),
                    "both masters must agree on the smaller identity as the winner");
            Assertions.assertTrue(createdId[0] > contested,
                    "the losing create must move to a fresh id instead of double-inserting, got "
                            + createdId[0]);
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== #3a: stale load snapshots are rejected ====================

    @Test
    public void testLoadSnapshotFromBeforeInvalidationIsDiscarded() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        manager.prepareLoadForTest();
        CountDownLatch hookEntered = new CountDownLatch(1);
        CountDownLatch hookRelease = new CountDownLatch(1);
        try {
            BaselineManager.snapshotReadStartedHookForTest = () -> {
                hookEntered.countDown();
                try {
                    hookRelease.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            };
            // the snapshot still contains a row the concurrent DROP is deleting
            BaselineManager.snapshotReaderForTest = () -> Map.of(1L,
                    withId(baseline("d", "select 1"), 1));

            Thread loader = new Thread(manager::loadFromInternalTable);
            loader.start();
            Assertions.assertTrue(hookEntered.await(5, TimeUnit.SECONDS),
                    "the load must reach the snapshot-read hook");
            // the promotion / DROP window: invalidate while the load is inside the read
            manager.invalidatePublishedStoreForTest();
            hookRelease.countDown();
            loader.join(10_000);

            Assertions.assertTrue(manager.getAllBaselines().isEmpty(),
                    "the stale snapshot must not be republished (deleted-row resurrection)");

            // the load must stay RETRYABLE: without the hook the next load publishes
            BaselineManager.snapshotReadStartedHookForTest = null;
            BaselineManager.snapshotReaderForTest = () -> Map.of(1L,
                    withId(baseline("d", "select 1"), 1));
            manager.loadFromInternalTable();
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "a discarded load must not mark the store loaded; the retry must run");
        } finally {
            BaselineManager.snapshotReadStartedHookForTest = null;
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== #3b: invalidation is serialized with the writers ====================

    @Test
    public void testInvalidationWaitsForAnInFlightWriter() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long id = manager.createBaseline(baseline("aa", "select 1"));
            store.deleteEntered = new CountDownLatch(1);
            store.deleteRelease = new CountDownLatch(1);
            AtomicBoolean dropReturned = new AtomicBoolean(false);
            AtomicBoolean invalidateSawDropReturned = new AtomicBoolean(false);

            Thread drop = new Thread(() -> {
                manager.dropBaseline(id);
                dropReturned.set(true);
            });
            drop.start();
            Assertions.assertTrue(store.deleteEntered.await(5, TimeUnit.SECONDS),
                    "the DROP must be inside its durable delete (holding writerLock)");

            Thread invalidate = new Thread(() -> {
                manager.invalidatePublishedStoreForTest();
                invalidateSawDropReturned.set(dropReturned.get());
            });
            invalidate.start();
            // give the invalidation a chance to (incorrectly) bypass the writer
            invalidate.join(300);
            Assertions.assertTrue(invalidate.isAlive(),
                    "the invalidation must WAIT for the in-flight writer instead of clearing"
                            + " the maps under it (a DROP that then finds no map entry skips its"
                            + " version bump)");

            store.deleteRelease.countDown();
            drop.join(10_000);
            invalidate.join(10_000);
            Assertions.assertTrue(invalidateSawDropReturned.get(),
                    "the invalidation must observe the committed DROP");
            Assertions.assertTrue(manager.getAllBaselines().isEmpty(),
                    "the dropped baseline must not be resurrected by the invalidation");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    private static BaselinePlan withId(BaselinePlan plan, long id) {
        plan.setId(id);
        // the snapshot rows are already parsed (transient trees would be rebuilt by the
        // real reader); the generation-discard test only needs the id set
        return plan;
    }
}
