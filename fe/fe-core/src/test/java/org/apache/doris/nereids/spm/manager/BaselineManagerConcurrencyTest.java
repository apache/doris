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
import org.apache.doris.nereids.spm.SPMUtils;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
        /** When set, every INSERT fails BEFORE committing (an ambiguous write). */
        private boolean failInsert;
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
        /**
         * The id high-water mark of the append-only SEQUENCE table (round-32 #2): the ids
         * the create path has RESERVED, which survive a DROP of the row that held them -
         * exactly the durable state the baselines table's own MAX(id) loses.
         */
        private long reservedHighWater;
        /** identity-carrying reservations, keyed by digest + planSql hash (round-39 #4). */
        private final Map<String, long[]> keyedReservations = new ConcurrentHashMap<>();
        /**
         * When set, every by-id read AFTER an identity delete fails (round-35 #1): the
         * lingering-row cleanup of a DROP cannot be completed.
         */
        private boolean failReadAfterDelete;
        private boolean deleted;

        @Override
        public long seqWatermark() {
            return reservedHighWater;
        }

        @Override
        public void reserveId(long id) {
            reservedHighWater = Math.max(reservedHighWater, id);
        }

        /**
         * The identity-carrying reservation (round-39 #4): the plain reservation only
         * extends the watermark - the DURABLE fence is driven by the explicit UNCONFIRMED
         * marker below ({@link #notePendingSeqState}). A reservation exists for every
         * create (successful ones included) and must never fence.
         */
        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMs) {
            reserveId(id);
        }

        @Override
        public void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
            keyedReservations.put(bindSqlDigest + '\u0001' + planSqlHash,
                    new long[] {id, atMillis});
        }

        @Override
        public BaselineManager.SeqReservation pendingSeqReservation(String bindSqlDigest,
                long planSqlHash) {
            long[] entry = keyedReservations.get(bindSqlDigest + '\u0001' + planSqlHash);
            return entry == null ? null : new BaselineManager.SeqReservation(entry[0], entry[1]);
        }

        /** Round-40 #7: the marker DELETE of a resolved ambiguous write. */
        @Override
        public void retirePendingSeqState(String bindSqlDigest, long planSqlHash, long markerId) {
            long[] entry = keyedReservations.get(bindSqlDigest + '\u0001' + planSqlHash);
            if (entry != null && entry[0] == markerId) {
                keyedReservations.remove(bindSqlDigest + '\u0001' + planSqlHash);
            }
        }

        /** The table-wide newest stored update_time (round-39 #9). */
        @Override
        public long newestStoredUpdateSecond() {
            long max = 0;
            for (List<BaselinePlan> idRows : rows.values()) {
                for (BaselinePlan row : idRows) {
                    max = Math.max(max, row.getUpdateTime() / 1000L);
                }
            }
            return max;
        }

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
            if (failInsert) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            rows.compute(plan.getId(), (id, current) -> {
                List<BaselinePlan> updated = current == null
                        ? new ArrayList<>() : new ArrayList<>(current);
                updated.add(plan);
                return updated;
            });
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (failReadAfterDelete && deleted) {
                throw new RuntimeException("internal table read timed out");
            }
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
            deleted = true;
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

        private void replaceRows(long id, List<BaselinePlan> replacement) {
            if (replacement.isEmpty()) {
                rows.remove(id);
            } else {
                rows.put(id, new ArrayList<>(replacement));
            }
        }
    }

    // ==================== #1: deterministic id-collision resolution ====================

    /**
     * round-32 #2: a DROP must not make the id reusable. The baselines table's own MAX(id)
     * loses the highest id with its row, so a FE that never saw that row (a follower
     * promoting after the drop) would hand the id to a DIFFERENT baseline - and a delayed
     * {@code DROP BASELINE PLAN IF EXISTS N} retry for the old row would delete the new
     * one. The append-only id sequence outlives the row.
     */
    @Test
    public void testDroppedHighestIdIsNeverHandedOutAgain() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long first = manager.createBaseline(baseline("aa", "select 1"));
            long highest = manager.createBaseline(baseline("bb", "select 2"));
            Assertions.assertTrue(highest > first, "ids are allocated upward");
            Assertions.assertTrue(manager.dropBaseline(highest), "the highest row is dropped");
            Assertions.assertEquals(first, store.watermark(),
                    "precondition: the baselines table's MAX(id) falls back after the drop");

            // the take-over that never saw `highest`: its next CREATE must not reuse the id
            long next = manager.createBaseline(baseline("cc", "select 3"));
            Assertions.assertTrue(next > highest,
                    "the dropped id must stay reserved, got " + next + " after " + highest);
            Assertions.assertEquals(1, store.rowsOf(next).size(),
                    "the new baseline exists under its own id");
            Assertions.assertEquals("cc", store.rowsOf(next).get(0).getBindSqlDigest());

            // the delayed id-based retry of the OLD drop cannot reach the new baseline
            Assertions.assertFalse(manager.dropBaseline(highest),
                    "the old id resolves to nothing");
            Assertions.assertEquals(1, store.rowsOf(next).size(),
                    "the new baseline must survive the delayed retry: " + store.rowsOf(next));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

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

    // ==================== confirmed post-forward DDL refresh (round-13) ====================

    /**
     * loaded == true and the post-DDL read fails transiently: the old best-effort refresh
     * swallowed the error and kept the (possibly PRE-DDL) rows - a successful DROP /
     * disable kept replaying locally. The confirmed path must fence the stale rows out
     * and surface a retryable failure instead of pretending success.
     */
    @Test
    public void testForwardedDdlConfirmationFailsClosedOnReadError() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, baseline("d1", "p1"));
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertTrue(manager.hasBaselines(), "fixture row must be loaded");

            BaselineManager.snapshotReaderForTest = () -> {
                throw new RuntimeException("tablet unavailable");
            };
            Assertions.assertThrows(IllegalStateException.class,
                    manager::refreshAfterForwardedDdl,
                    "an unconfirmable post-DDL read must surface as a retryable failure");
            Assertions.assertFalse(manager.hasBaselines(),
                    "the possibly pre-DDL rows must be fenced out instead of replaying a"
                            + " dropped / disabled baseline");
        } finally {
            manager.clearForTest();
        }
    }

    /**
     * loaded == false and an OLDER load (started before the DDL) holds the load slot: the
     * old initial-load branch returned without fencing it, so its pre-DDL snapshot could
     * publish afterwards (a CREATE stayed invisible). The confirmed path must fence that
     * snapshot (store generation) and publish a fresh POST-DDL read.
     */
    @Test
    public void testForwardedDdlFencesInFlightPreDdlLoad() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            CountDownLatch readStarted = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            AtomicInteger reads = new AtomicInteger();
            BaselineManager.snapshotReaderForTest = () -> {
                if (reads.incrementAndGet() == 1) {
                    readStarted.countDown();
                    try {
                        releaseRead.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return Map.of(1L, baseline("d-pre", "p-pre")); // PRE-DDL snapshot
                }
                return Map.of(); // POST-DDL: the row is gone
            };
            manager.prepareLoadForTest();
            Thread loader = new Thread(manager::loadFromInternalTable, "spm-pre-ddl-load");
            loader.start();
            Assertions.assertTrue(readStarted.await(5, TimeUnit.SECONDS),
                    "the in-flight load must reach its snapshot read");

            AtomicReference<Throwable> confirmationFailure = new AtomicReference<>();
            Thread confirmer = new Thread(() -> {
                try {
                    manager.refreshAfterForwardedDdl();
                } catch (Throwable t) {
                    confirmationFailure.set(t);
                }
            }, "spm-ddl-confirm");
            confirmer.start();
            // give the confirmer time to fence the generation and start waiting for the
            // load slot the loader still holds
            Thread.sleep(200);
            releaseRead.countDown();
            loader.join(10_000);
            confirmer.join(10_000);

            Assertions.assertFalse(loader.isAlive(), "the pre-DDL load must finish");
            Assertions.assertFalse(confirmer.isAlive(), "the confirmed refresh must finish");
            Assertions.assertNull(confirmationFailure.get(),
                    "the confirmed refresh must succeed: " + confirmationFailure.get());
            Assertions.assertEquals(2, reads.get(),
                    "the fence must discard the PRE-DDL snapshot and read again");
            Assertions.assertFalse(manager.hasBaselines(),
                    "the post-DDL (empty) snapshot must be the published state");
        } finally {
            manager.clearForTest();
        }
    }

    // ==================== status publishes only after durable success (round-14) ====================

    /**
     * The live object must NOT flip before the durable row exists: matching readers do
     * not take the writer lock, so an early setStatus would let a concurrent query
     * replay a baseline that remains DISABLED durably when the INSERT later fails -
     * while ALTER still reports failure.
     */
    @Test
    public void testStatusIsNotPublishedBeforeDurableSuccess() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        CountDownLatch insertEntered = new CountDownLatch(1);
        CountDownLatch insertRelease = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try {
            BaselineManager.statusProtocolStoreForTest =
                    new BaselineManager.StatusProtocolStoreForTest() {
                        @Override
                        public void insert(BaselinePlan plan) {
                            // ONLY the status-update INSERT blocks: the CREATE-time
                            // DISABLED insert must complete, otherwise the test would
                            // block itself before the update thread ever starts
                            if (plan.getStatus() != BaselineStatus.ENABLED) {
                                return;
                            }
                            insertEntered.countDown();
                            try {
                                insertRelease.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            throw new RuntimeException("INSERT failed");
                        }

                        @Override
                        public void deleteByIdAndStatus(long id, BaselineStatus status) {
                            // the repair delete after the failure: no-op
                        }

                        @Override
                        public int countByIdAndStatus(long id, BaselineStatus status) {
                            // the OLD row is still durable: the reconcile must NOT keep
                            // the new status
                            return status == BaselineStatus.DISABLED ? 1 : 0;
                        }
                    };
            BaselinePlan disabled = baseline("d-status", "p-status");
            disabled.setStatus(BaselineStatus.DISABLED);
            long id = manager.createBaseline(disabled);

            Thread updater = new Thread(() -> {
                try {
                    manager.updateStatus(id, BaselineStatus.ENABLED);
                    failure.set(null);
                } catch (RuntimeException e) {
                    failure.set(e);
                }
            });
            updater.start();
            Assertions.assertTrue(insertEntered.await(5, TimeUnit.SECONDS),
                    "the durable INSERT must be in flight");

            Assertions.assertEquals(BaselineStatus.DISABLED,
                    manager.getBaseline(id).getStatus(),
                    "the new status must not be published before the durable row exists");

            insertRelease.countDown();
            updater.join(10_000);
            Assertions.assertNotNull(failure.get(),
                    "the failed INSERT must surface (ALTER reports failure)");
            Assertions.assertEquals(BaselineStatus.DISABLED,
                    manager.getBaseline(id).getStatus(),
                    "a failed durable change must leave the baseline DISABLED in memory"
                            + " too (it would keep replaying otherwise)");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== promotion window / stale fingerprint (round 16) ====================

    /** A protocol store recording every write it receives. */
    private static final class RecordingProtocolStore
            implements BaselineManager.StatusProtocolStoreForTest {
        private final List<String> operations = new ArrayList<>();
        private final java.util.function.LongFunction<Integer> enabledCounts;
        private final java.util.function.LongFunction<Integer> disabledCounts;

        RecordingProtocolStore(int enabledRows, int disabledRows) {
            this.enabledCounts = id -> enabledRows;
            this.disabledCounts = id -> disabledRows;
        }

        @Override
        public void insert(BaselinePlan plan) {
            operations.add("insert:" + plan.getStatus());
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            operations.add("delete:" + status);
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            return status == BaselineStatus.ENABLED
                    ? enabledCounts.apply(id) : disabledCounts.apply(id);
        }
    }

    /**
     * Env.transferToMaster sets isReady BEFORE forceReloadFromInternalTable finishes, so
     * a freshly promoted follower can still serve its pre-promotion snapshot: an
     * "ALTER ... ENABLE" that the cache believes is a no-op may actually have to flip a
     * durably DISABLED row. The early return must confirm against the durable table
     * instead of reporting success without changing the durable status.
     */
    @Test
    public void testNoOpAlterRepairsStaleDurableStatus() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        RecordingProtocolStore store = new RecordingProtocolStore(0, 1);
        try {
            BaselineManager.statusProtocolStoreForTest = store;
            BaselinePlan stale = baseline("fp-status", "select 1");
            stale.setSchemaFingerprint("fp-A");
            stale.setStatus(BaselineStatus.ENABLED); // pre-promotion snapshot
            long id = manager.createBaseline(stale);

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED),
                    "the ALTER must repair the stale status, not silently succeed");
            Assertions.assertEquals(BaselineStatus.ENABLED,
                    manager.getBaseline(id).getStatus());
            Assertions.assertTrue(store.operations.contains("insert:ENABLED"),
                    "the durable ENABLED row must be written: " + store.operations);
            Assertions.assertTrue(store.operations.contains("delete:DISABLED"),
                    "the durably stale DISABLED row must be deleted: " + store.operations);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The other promotion-window direction: the cache holds a row the previous master
     * already DROPPED durably. An ALTER must not report success for it - it is removed
     * from the cache and reported as missing.
     */
    @Test
    public void testNoOpAlterDropsRowGoneDurably() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        RecordingProtocolStore store = new RecordingProtocolStore(0, 0);
        try {
            BaselineManager.statusProtocolStoreForTest = store;
            BaselinePlan stale = baseline("fp-gone", "select 2");
            stale.setSchemaFingerprint("fp-A");
            stale.setStatus(BaselineStatus.ENABLED);
            long id = manager.createBaseline(stale);
            store.operations.clear(); // the CREATE itself writes the initial row

            Assertions.assertFalse(manager.updateStatus(id, BaselineStatus.ENABLED),
                    "a row that no longer exists durably must not report success");
            Assertions.assertNull(manager.getBaseline(id),
                    "the stale cache entry must be dropped");
            Assertions.assertTrue(store.operations.isEmpty(),
                    "no durable write may follow: " + store.operations);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * CREATE dedup must consider the SCHEMA FINGERPRINT: after ALTER TABLE t ADD COLUMN
     * extra the stored fingerprint is stale (SPMPlanner skips the old baseline), so a
     * repeated CREATE produces the same digest / planSql under a NEW fingerprint and must
     * allocate a usable row instead of returning the unusable id. The same durable-truth
     * check applies to a cached row the previous master dropped.
     */
    @Test
    public void testCreateDedupDistinguishesFingerprintAndDurablePresence() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            BaselinePlan first = baseline("fp-create", "select 3");
            first.setSchemaFingerprint("fp-old");
            long idOld = manager.createBaseline(first);

            BaselinePlan altered = baseline("fp-create", "select 3");
            altered.setSchemaFingerprint("fp-new");
            long idNew = manager.createBaseline(altered);
            Assertions.assertNotEquals(idOld, idNew,
                    "a stale-fingerprint row is NOT a duplicate: the re-CREATE must"
                            + " produce a usable baseline");
            Assertions.assertNull(manager.getBaseline(idOld),
                    "the stale row is retired from the cache");

            BaselinePlan same = baseline("fp-create", "select 3");
            same.setSchemaFingerprint("fp-new");
            Assertions.assertEquals(idNew, manager.createBaseline(same),
                    "an exact duplicate (same fingerprint) still dedups");

            // a cached duplicate that is gone durably (promotion window) must be
            // replaced, not returned
            manager.clearForTest();
            BaselineManager.statusProtocolStoreForTest = new RecordingProtocolStore(0, 0);
            BaselinePlan cached = baseline("fp-dropped", "select 4");
            cached.setSchemaFingerprint("fp-x");
            long idCached = manager.createBaseline(cached);
            BaselinePlan again = baseline("fp-dropped", "select 4");
            again.setSchemaFingerprint("fp-x");
            long idRecreated = manager.createBaseline(again);
            Assertions.assertNotEquals(idCached, idRecreated,
                    "a durably dropped cached row must not be returned as the duplicate");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-24: handoff fencing of the status flip ====================

    /**
     * Status store simulating both round-24 races: a handoff landing between the flip's
     * two writes (the delayed DELETE) and a DROP completing while the ALTER was stalled
     * (the conditional INSERT finds no previous row).
     */
    private static class HandoffStatusStore
            implements BaselineManager.StatusProtocolStoreForTest {
        final List<String> operations = new ArrayList<>();
        final Map<BaselineStatus, Integer> rows = new java.util.EnumMap<>(BaselineStatus.class);
        boolean demoteOnInsert;
        boolean previousRowVanished;
        private final AtomicBoolean leader;

        HandoffStatusStore(AtomicBoolean leader) {
            this.leader = leader;
        }

        @Override
        public void insert(BaselinePlan plan) {
            operations.add("insert:" + plan.getStatus());
            rows.merge(plan.getStatus(), 1, Integer::sum);
            if (demoteOnInsert) {
                leader.set(false); // the handoff lands before the flip's DELETE
            }
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            if (previousRowVanished) {
                return false; // the conditional INSERT matched no row
            }
            insert(plan);
            return true;
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            operations.add("delete:" + status);
            rows.computeIfPresent(status, (k, v) -> v - 1);
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            return rows.getOrDefault(status, 0);
        }
    }

    /**
     * round-24: the DELETE half of a status flip is fenced like the identity delete. An
     * old master can run ALTER N ENABLED->DISABLED, insert the DISABLED row and be
     * demoted before its DELETE(ENABLED) executes; the new master then completes ALTER N
     * ENABLE (INSERT ENABLED + DELETE DISABLED) and the delayed DELETE would remove the
     * new master's ONLY durable row - both ALTERs reported success and the baseline was
     * durably gone. The demotion is injected right after the flip's INSERT, which is
     * exactly where the handoff lands.
     */
    @Test
    public void testStatusDeleteIsFencedAfterADemotion() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        AtomicBoolean leader = new AtomicBoolean(true);
        HandoffStatusStore store = new HandoffStatusStore(leader);
        try {
            BaselineManager.statusProtocolStoreForTest = store;
            BaselineManager.leaderProbeForTest = leader::get;
            long id = manager.createBaseline(baseline("fp-fence", "select 5"));
            store.operations.clear();
            store.demoteOnInsert = true;

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("no longer the master"),
                    failure.getMessage());
            Assertions.assertTrue(
                    store.operations.stream().noneMatch(op -> op.startsWith("delete:")),
                    "the delayed DELETE must never reach the table: " + store.operations);
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the fenced ALTER still published the CONFIRMED new-status row: the old"
                            + " status must not stay replayable (round-34 #2)");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-24: the INSERT half of a status flip is CONDITIONAL on the previous-status
     * row. A DROP that completed while the ALTER was stalled (the old master passed its
     * leadership check, then lost the master before its INSERT landed) must not be
     * resurrected: the conditional statement writes nothing and the ALTER fails retryably
     * instead of publishing an ACTIVE baseline the DROP had already reported removed.
     */
    @Test
    public void testStatusFlipIsRefusedWhenThePreviousRowWasDropped() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        AtomicBoolean leader = new AtomicBoolean(true);
        HandoffStatusStore store = new HandoffStatusStore(leader);
        try {
            BaselineManager.statusProtocolStoreForTest = store;
            BaselineManager.leaderProbeForTest = leader::get;
            BaselinePlan disabled = baseline("fp-dropped-mid-alter", "select 6");
            disabled.setStatus(BaselineStatus.DISABLED);
            long id = manager.createBaseline(disabled);
            store.operations.clear();
            store.previousRowVanished = true; // the new master's DROP completed

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED));
            Assertions.assertTrue(failure.getMessage().contains("row was gone"),
                    failure.getMessage());
            Assertions.assertTrue(
                    store.operations.stream().noneMatch(op -> op.startsWith("insert:")),
                    "the flip must not resurrect a dropped baseline: " + store.operations);
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "a refused flip must not be published");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The benign twin of the conflict above: the previous row is gone because ANOTHER
     * master already completed the SAME flip (DISABLED -> ENABLED). The reconcile sees
     * the requested status durably and reports success without a second durable write - a
     * conflict must not turn into a spurious failure (and the compensation must never
     * delete the other master's row).
     */
    @Test
    public void testStatusFlipConflictWithAFlippedRowIsReconciled() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        AtomicBoolean leader = new AtomicBoolean(true);
        HandoffStatusStore store = new HandoffStatusStore(leader) {
            @Override
            public boolean insertIfPreviousPresent(BaselinePlan plan,
                    BaselineStatus previousStatus) {
                // the other master's flip completes between our probe and our INSERT
                rows.computeIfPresent(previousStatus, (k, v) -> v - 1);
                rows.merge(BaselineStatus.ENABLED, 1, Integer::sum);
                return false;
            }
        };
        try {
            BaselineManager.statusProtocolStoreForTest = store;
            BaselineManager.leaderProbeForTest = leader::get;
            BaselinePlan disabled = baseline("fp-concurrent-flip", "select 7");
            disabled.setStatus(BaselineStatus.DISABLED);
            long id = manager.createBaseline(disabled);

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED),
                    "the requested status IS durable: the ALTER reports success");
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus());
            Assertions.assertEquals(1,
                    store.countByIdAndStatus(id, BaselineStatus.ENABLED),
                    "the other master's row must not be compensated away");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-25: forwarded-DDL visibility and collision fencing ====================

    /**
     * round-25 #1: the post-forward refresh must SYNCHRONIZE with the master BEFORE it
     * reads the snapshot. A forwarded GLOBAL DDL (FORWARD_NO_SYNC) carries no journal wait
     * of its own, so a follower's still-visible OLD version would be read and published as
     * the "confirmed" post-DDL state - after a DROP the removed baseline would keep being
     * replayed locally although the command reported success. The reader below returns
     * the PRE-DDL snapshot until the sync ran, so a refresh that read first would publish
     * the dropped row.
     */
    @Test
    public void testForwardedDdlRefreshSynchronizesBeforeReading() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        List<String> order = new ArrayList<>();
        try {
            manager.createBaseline(baseline("fp-forwarded", "select 8"));
            Map<Long, BaselinePlan> preDdl = Map.copyOf(
                    manager.getAllBaselines().stream().collect(
                            java.util.stream.Collectors.toMap(BaselinePlan::getId, p -> p)));
            BaselineManager.forwardedDdlSyncForTest = () -> order.add("sync");
            BaselineManager.snapshotReaderForTest = () -> {
                order.add("read");
                // the DDL (a DROP of that baseline) is only visible AFTER the journal sync
                return order.contains("sync") ? Map.of() : preDdl;
            };

            manager.refreshAfterForwardedDdl();

            Assertions.assertEquals(List.of("sync", "read"), order,
                    "the master's completed DDL must be synchronized to BEFORE the snapshot"
                            + " read: " + order);
            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "the confirmed refresh must publish the post-DDL state, not the"
                            + " pre-DDL snapshot");
        } finally {
            BaselineManager.forwardedDdlSyncForTest = null;
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Round-28 #3: the post-forward journal synchronization is part of the FAIL-CLOSED
     * path. A forwarded GLOBAL DROP / DISABLE can commit on the master before
     * afterForwardToMaster reaches the refresh, so a sync timeout must not leave the
     * published cache as it is: loaded, baselines and the hash index would keep the old
     * ENABLED row and later default-consistency local queries would keep replaying a
     * baseline the master already dropped. The publishing store must be invalidated and
     * the caller must see a retryable failure.
     */
    @Test
    public void testForwardedDdlRefreshInvalidatesCacheWhenTheSyncFails() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d1", "p1"), 7L));
            manager.loadFromInternalTable();
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "precondition: the follower replays the pre-DDL row");

            BaselineManager.forwardedDdlSyncForTest = () -> {
                throw new RuntimeException("journal sync timed out");
            };
            IllegalStateException failure = Assertions.assertThrows(
                    IllegalStateException.class, manager::refreshAfterForwardedDdl,
                    "an unconfirmable post-DDL refresh must surface as a retryable error");
            Assertions.assertTrue(failure.getMessage().contains("journal sync timed out")
                            || failure.getMessage().contains("invalidated"),
                    failure.getMessage());

            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "the published rows must be fenced out: the master completed a DDL"
                            + " this FE could not confirm");
            Assertions.assertFalse(manager.hasBaselines(),
                    "the query path must not match the unconfirmed row either");

            // the store recovers on the next successful refresh (fail closed, not broken)
            BaselineManager.forwardedDdlSyncForTest = () -> { };
            BaselineManager.snapshotReaderForTest = () -> Map.of(
                    7L, withId(baseline("d1", "p1"), 7L),
                    8L, withId(baseline("d2", "p2"), 8L));
            manager.refreshAfterForwardedDdl();
            Assertions.assertEquals(2, manager.getAllBaselines().size(),
                    "the recovered refresh publishes the current durable rows");
        } finally {
            BaselineManager.forwardedDdlSyncForTest = null;
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Round-28 #6: the whole-table snapshot read is PAGINATED, because one SELECT * over a
     * table without a retention cap had to return every row inside the fixed per-query
     * timeout - once the snapshot outgrew it the read failed as a whole and never
     * converged. The loop walks the id space with an INCLUSIVE lower bound, so an id
     * carried by two rows (an interrupted status flip) is always read as a whole and the
     * winner resolution sees both rows; a short page ends the snapshot.
     */
    @Test
    public void testSnapshotPaginationReadsEveryPageAndKeepsIdGroupsWhole() {
        Map<Long, List<BaselinePlan>> table = new java.util.LinkedHashMap<>();
        table.put(1L, List.of(withId(baseline("d1", "select k from t1"), 1L)));
        table.put(2L, List.of(withId(baseline("d2", "select k from t1"), 2L)));
        table.put(3L, List.of(withId(baseline("d3", "select k from t1"), 3L),
                withId(baseline("d3", "select k from t1"), 3L)));
        table.put(4L, List.of(withId(baseline("d4", "select k from t1"), 4L)));
        List<String> requests = new ArrayList<>();
        Map<Long, BaselinePlan> snapshot;
        try {
            snapshot = BaselineManager.collectSnapshotPages((pageStart, offset) -> {
                requests.add((pageStart == null ? "null" : pageStart) + "@" + offset);
                return tableReader(table, 2).readPage(pageStart, offset);
            }, 2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(List.of(1L, 2L, 3L, 4L),
                snapshot.keySet().stream().sorted().collect(java.util.stream.Collectors.toList()),
                "every id must be read exactly once: " + snapshot.keySet());
        Assertions.assertEquals(List.of("null@0", "2@1", "3@2"), requests,
                "the loop continues from the LAST ROW READ (inclusive id bound plus the rows"
                        + " of that id group already consumed), so every row is read exactly"
                        + " once: " + requests);
    }

    /**
     * A faithful page reader over an in-memory table (see
     * {@link BaselineManager#collectSnapshotPages}): rows with {@code id >= pageStart}
     * (every row for the first page), ordered by id, skipping {@code offset} rows and
     * returning at most {@code pageSize} of them.
     */
    private static BaselineManager.SnapshotPageReader tableReader(
            Map<Long, List<BaselinePlan>> table, int pageSize) {
        return (pageStart, offset) -> table.entrySet().stream()
                .filter(entry -> pageStart == null || entry.getKey() >= pageStart)
                .flatMap(entry -> entry.getValue().stream())
                .skip(offset)
                .limit(pageSize)
                .map(BaselineManagerConcurrencyTest::rowOf)
                .collect(java.util.stream.Collectors.toList());
    }

    /**
     * round-33 #4: an id group can hold MORE rows than one snapshot page. The ALTER
     * protocol deliberately keeps the old-status row when its delete fails, so repeated
     * opposite-status failures grow the group by one row per flip. The walk must read the
     * WHOLE group (row offset within the inclusive bound): jumping past it after the first
     * page omitted the rows behind it - possibly the NEWEST durable status - and the
     * unchanged before/after fence then let the refresh publish the OLD one.
     */
    @Test
    public void testSnapshotPaginationReadsEveryRowOfAnOversizedIdGroup() {
        Map<Long, List<BaselinePlan>> table = new java.util.LinkedHashMap<>();
        table.put(7L, List.of(
                flipRow(7L, BaselineStatus.ENABLED, 100_000L),
                flipRow(7L, BaselineStatus.DISABLED, 150_000L),
                flipRow(7L, BaselineStatus.ENABLED, 200_000L),
                flipRow(7L, BaselineStatus.DISABLED, 250_000L),
                flipRow(7L, BaselineStatus.ENABLED, 300_000L)));
        table.put(8L, List.of(withId(baseline("d8", "select k from t8"), 8L)));
        List<String> requests = new ArrayList<>();
        Map<Long, BaselinePlan> snapshot;
        try {
            snapshot = BaselineManager.collectSnapshotPages((pageStart, offset) -> {
                requests.add((pageStart == null ? "null" : pageStart) + "@" + offset);
                return tableReader(table, 2).readPage(pageStart, offset);
            }, 2);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(BaselineStatus.ENABLED, snapshot.get(7L).getStatus(),
                "the NEWEST row of the oversized group must win (the first page alone would"
                        + " have kept DISABLED): " + requests);
        Assertions.assertEquals(300_000L, snapshot.get(7L).getUpdateTime());
        Assertions.assertTrue(snapshot.containsKey(8L),
                "the walk must continue past the group: " + snapshot.keySet());
        Assertions.assertTrue(requests.contains("7@2"),
                "the group is read through the row offset instead of being cut after its"
                        + " first page: " + requests);
        Assertions.assertEquals(List.of("null@0", "7@2", "7@4", "8@1"), requests,
                "every row of the oversized group is read exactly once: " + requests);
    }

    /**
     * round-38 #3: an OFFSET continuation that lands inside an id group is only sound when
     * the page order is a TOTAL order over that group's rows. With {@code ORDER BY `id`}
     * alone the engine may return a repeated id's rows in ANY order in EVERY execution
     * (SQL promises nothing for equal sort keys): page one ends on the OLD row of the
     * group, the continuation's OFFSET then re-reads that row and skips the NEWER one -
     * and the duplicate/skip pair leaves the row count equal to the fence's COUNT(*), so
     * the completeness proof cannot see it and refresh publishes the OLD status (a failed
     * DISABLE silently re-enables until the next stable read).
     */
    @Test
    public void testSnapshotPaginationSurvivesTieReorderingBetweenPages() throws Exception {
        // the reviewer's shape: 1,999 lower-id rows plus an id whose group STRADDLES the
        // 2,000-row page boundary (an ALTER DISABLE whose compensating delete failed left
        // the older ENABLED row behind: (ENABLED, older) + (DISABLED, newer))
        Map<Long, List<BaselinePlan>> table = new java.util.LinkedHashMap<>();
        for (long id = 1; id <= 1999; id++) {
            table.put(id, List.of(withId(baseline("d" + id, "select k from t" + id), id)));
        }
        table.put(2000L, List.of(
                flipRow(2000L, BaselineStatus.ENABLED, 100_000L),
                flipRow(2000L, BaselineStatus.DISABLED, 150_000L)));

        String pageSql = BaselineManager.snapshotPageSql(2000L, 1);
        Assertions.assertTrue(pageSql.contains("ORDER BY `id`, `update_time`, `status`"),
                "the page order must be TOTAL over the rows of one id, not merely by id:"
                        + " an id-only order lets a valid execution re-order the tie and the"
                        + " OFFSET skip a row behind it: " + pageSql);

        Map<Long, BaselinePlan> snapshot = BaselineManager.readStableSnapshot(
                tieReorderingReader(table, 2000),
                () -> new BaselineManager.SnapshotFence(2000L, 2001L, ""));
        Assertions.assertEquals(BaselineStatus.DISABLED, snapshot.get(2000L).getStatus(),
                "the NEWER row of the straddling group must win even though the engine's"
                        + " tie order differs between the two pages (the duplicate/skip pair"
                        + " keeps the row count unchanged, so the fence cannot see it)");
        Assertions.assertEquals(2000, snapshot.size(),
                "every distinct id must be present exactly once: " + snapshot.keySet());
    }

    /**
     * A page reader emulating an engine that HONORS the ORDER BY the manager requested but
     * is FREE to order rows that remain tied under it differently in every execution (SQL
     * promises nothing for equal sort keys): on the second and later calls it reverses
     * every run of rows that tie under the DECLARED order. Under {@code ORDER BY `id`} that
     * reproduces the reviewer's reordering (the two rows of one id tie); under the total
     * order the rows no longer tie, so the same engine cannot re-order them and the
     * continuation reads the group completely.
     */
    private static BaselineManager.SnapshotPageReader tieReorderingReader(
            Map<Long, List<BaselinePlan>> table, int pageSize) {
        AtomicInteger calls = new AtomicInteger();
        return (pageStart, offset) -> {
            List<String> keys = orderByColumnsOf(BaselineManager.snapshotPageSql(pageStart, offset));
            List<ResultRow> rows = table.entrySet().stream()
                    .filter(entry -> pageStart == null || entry.getKey() >= pageStart)
                    .flatMap(entry -> entry.getValue().stream())
                    .map(BaselineManagerConcurrencyTest::rowOf)
                    .collect(java.util.stream.Collectors.toList());
            rows.sort(comparatorOf(keys));
            if (calls.incrementAndGet() >= 2) {
                reverseTiedRuns(rows, keys);
            }
            return rows.stream().skip(offset).limit(pageSize)
                    .collect(java.util.stream.Collectors.toList());
        };
    }

    /** The order columns of one page SQL (the manager's own request). */
    private static List<String> orderByColumnsOf(String sql) {
        int at = sql.indexOf("ORDER BY ");
        Assertions.assertTrue(at > 0, "the page SQL must declare its order: " + sql);
        String clause = sql.substring(at + "ORDER BY ".length());
        int limit = clause.indexOf(" LIMIT ");
        Assertions.assertTrue(limit > 0, "the order clause must precede LIMIT: " + sql);
        List<String> keys = new ArrayList<>();
        for (String key : clause.substring(0, limit).split(",")) {
            keys.add(key.trim().replace("`", ""));
        }
        return keys;
    }

    /** Compares two snapshot rows by the ORDER BY columns the manager declared. */
    private static java.util.Comparator<ResultRow> comparatorOf(List<String> keys) {
        return (left, right) -> {
            for (String key : keys) {
                int column = snapshotColumnIndex(key);
                String l = left.getWithDefault(column, "");
                String r = right.getWithDefault(column, "");
                int compared = "id".equals(key)
                        ? Long.compare(Long.parseLong(l.trim()), Long.parseLong(r.trim()))
                        : l.compareTo(r);
                if (compared != 0) {
                    return compared;
                }
            }
            return 0;
        };
    }

    /** The position of one tie-break column in the snapshot row (SNAPSHOT_COLUMNS order). */
    private static int snapshotColumnIndex(String name) {
        switch (name) {
            case "id":
                return 0;
            case "status":
                return 9;
            case "update_time":
                return 11;
            default:
                throw new AssertionError("unexpected snapshot order column: " + name);
        }
    }

    /**
     * Reverses every maximal run of rows that remain EQUAL under the declared order - the
     * engine may emit such rows in any order in any execution (see
     * {@link #tieReorderingReader}).
     */
    private static void reverseTiedRuns(List<ResultRow> rows, List<String> keys) {
        java.util.Comparator<ResultRow> comparator = comparatorOf(keys);
        int start = 0;
        for (int i = 1; i <= rows.size(); i++) {
            if (i == rows.size() || comparator.compare(rows.get(i - 1), rows.get(i)) != 0) {
                if (i - start > 1) {
                    java.util.Collections.reverse(rows.subList(start, i));
                }
                start = i;
            }
        }
    }

    /**
     * round-33 #4 (fail-closed side): the fence alone cannot see a TRUNCATED page - a
     * partial result looks exactly like a short, completed page. The loop reads every row
     * exactly once, so the rows it read must equal the fence's row count; when they do not
     * (e.g. an internal-query row limit cancelled a page and dropped its whole result),
     * the snapshot must NOT be published.
     */
    @Test
    public void testSnapshotReadFailsClosedWhenPagesAreTruncated() {
        int[] fenceReads = {0};
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManager.readStableSnapshot(
                        // the first page comes back EMPTY although the fence counts rows
                        (pageStart, offset) -> List.of(),
                        () -> {
                            fenceReads[0]++;
                            // a fence that does NOT move: only the completeness check can
                            // reject this read
                            return new BaselineManager.SnapshotFence(2, 2, "t");
                        }));
        Assertions.assertTrue(failure.getMessage().contains("truncated read"),
                "the failure must name the incomplete read: " + failure.getMessage());
        Assertions.assertEquals(6, fenceReads[0],
                "one fence pair per attempt, bounded to the retry budget");
    }

    /** One row of an interrupted status flip (id, status, updateTime). */
    private static BaselinePlan flipRow(long id, BaselineStatus status, long updateTime) {
        BaselinePlan row = baseline("d7", "select k from t7");
        row.setId(id);
        row.setStatus(status);
        row.setUpdateTime(updateTime);
        return row;
    }

    /**
     * round-33 #1: a status flip whose INSERT reported SQL OK with the transaction
     * COMMITTED - only the PUBLICATION lags past every probe - IS durable, and the
     * old-status row is still readable because the DELETE half never ran. Keeping the OLD
     * cached status (which the old-row reconciliation below does, since it cannot confirm
     * the flip) let queries keep replaying a durably DISABLED baseline until the next
     * refresh, while the committed DISABLED row won the durable pair. The cache must be
     * reconciled with the COMMITTED write (fail closed against the stale cache), and the
     * publication that follows must not flip it back.
     */
    @Test
    public void testUnconfirmedStatusInsertPublishesTheCommittedFlip() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-flip", "p-flip"));
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus());

            // the INSERT(DISABLED) commits, but no read sees it within the probe budget
            BaselineManager.durableVisibilityProbeForTest = (rowId, status) -> false;
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the committed flip is the landed ALTER");
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the cache must reconcile with the COMMITTED DISABLED row instead of"
                            + " keeping the old ENABLED status replayable");
            Assertions.assertEquals(2, store.rowsOf(id).size(),
                    "both rows stay durable (the old-row delete never ran): " + store.rowsOf(id));

            // the committed row publishes: the durable winner must be the new status
            BaselineManager.durableVisibilityProbeForTest = null;
            List<BaselinePlan> published = store.rowsOf(id);
            BaselinePlan winner = published.get(0);
            for (int i = 1; i < published.size(); i++) {
                winner = BaselineManager.pickDurableWinner(winner, published.get(i));
            }
            Assertions.assertEquals(BaselineStatus.DISABLED, winner.getStatus(),
                    "the later updateTime of the committed DISABLED row wins the pair");
            manager.applyRefreshedBaselines(Map.of(id, winner));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the publication must not flip the reconciled cache back");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-33 #2: a CREATE whose INSERT committed but stayed unreadable is remembered as
     * a pending create. When the referenced table changes (ALTER TABLE t ADD COLUMN x)
     * before the client retries, the retry carries the NEW schema fingerprint. Matching
     * the pending write by digest + planSql alone adopted the OLD-fingerprint row and
     * reported success for a baseline that matching / replay reject as stale. The retry
     * must retire the stale row and create the new incarnation instead.
     */
    @Test
    public void testPendingCreateWithAChangedFingerprintIsReplacedNotAdopted() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> false;
            BaselinePlan first = baseline("d-fp", "p-fp");
            first.setSchemaFingerprint("F1");
            RuntimeException failed = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(first));
            Assertions.assertTrue(failed.getMessage().contains("not readable"),
                    failed.getMessage());
            long pendingId = store.rows.keySet().iterator().next();
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "the committed identity must be remembered");

            // the schema changed while the write awaited publication; on the retry the row
            // is readable again - but it describes the OLD schema
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> true;
            BaselinePlan retried = baseline("d-fp", "p-fp");
            retried.setSchemaFingerprint("F2");
            long newId = manager.createBaseline(retried);

            Assertions.assertNotEquals(pendingId, newId,
                    "the stale incarnation must not be adopted for a changed fingerprint");
            Assertions.assertEquals("F2", manager.getBaseline(newId).getSchemaFingerprint(),
                    "the replacement row carries the CURRENT fingerprint");
            Assertions.assertTrue(store.rowsOf(pendingId).isEmpty(),
                    "the stale committed row must be retired: " + store.rowsOf(pendingId));
            Assertions.assertEquals(0, manager.pendingCreateCountForTest());
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Round-31 #3: the paginated snapshot must describe ONE state of the table. The loop
     * issues one SELECT per page and the internal table offers no read view, so a DDL
     * committing between two pages would be merged into a state that never existed: the
     * review example reads ENABLED low-id A on page 1, the master drops A and creates
     * high-id B, page 2 reads B - the published map kept BOTH, so SHOW reported the
     * completed DROP and matching replayed A until the next refresh. The fence (MAX(id) /
     * COUNT(*) / MAX(update_time)) is read before AND after the loop; when it moved, the
     * whole read restarts.
     */
    @Test
    public void testSnapshotReadRetriesWhenDdlLandsDuringThePageLoop() {
        Map<Long, List<BaselinePlan>> table = new java.util.LinkedHashMap<>();
        table.put(1L, List.of(withId(baseline("dA", "select k from ta"), 1L)));
        boolean[] ddlDone = {false};
        List<String> fenceCalls = new ArrayList<>();
        Map<Long, BaselinePlan> snapshot;
        try {
            snapshot = BaselineManager.readStableSnapshot((pageStart, offset) -> {
                List<ResultRow> rows = table.entrySet().stream()
                        .filter(entry -> pageStart == null || entry.getKey() >= pageStart)
                        .flatMap(entry -> entry.getValue().stream())
                        .skip(offset)
                        .map(BaselineManagerConcurrencyTest::rowOf)
                        .collect(java.util.stream.Collectors.toList());
                if (!rows.isEmpty() && !ddlDone[0]) {
                    // the master DROPs A and CREATEs B while this page is being read
                    ddlDone[0] = true;
                    table.clear();
                    table.put(9L, List.of(withId(baseline("dB", "select k from tb"), 9L)));
                }
                return rows;
            }, () -> {
                fenceCalls.add("fence");
                return new BaselineManager.SnapshotFence(
                        table.keySet().stream().mapToLong(Long::longValue).max().orElse(0L),
                        table.values().stream().mapToLong(List::size).sum(),
                        table.keySet().toString());
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        Assertions.assertEquals(List.of(9L),
                snapshot.keySet().stream().sorted().collect(java.util.stream.Collectors.toList()),
                "the retried read publishes ONLY the state after the DDL: " + snapshot.keySet());
        Assertions.assertFalse(snapshot.containsKey(1L),
                "the dropped baseline A must not survive in the published snapshot");
        Assertions.assertEquals(4, fenceCalls.size(),
                "one fence pair per attempt: the mixed first read is discarded, the second"
                        + " one is published");
    }

    /**
     * Round-31 #3 (fail-closed side): a table that never stays stable while its snapshot is
     * read must NOT be published - a mixed state would let a dropped / re-created baseline
     * keep replaying until the next refresh. The read gives up after its bounded retries
     * with a RETRYABLE error (every caller re-reads on its next cycle).
     */
    @Test
    public void testSnapshotReadFailsClosedWhenTheTableNeverStaysStable() {
        int[] fenceReads = {0};
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManager.readStableSnapshot((pageStart, offset) -> List.of(),
                        () -> new BaselineManager.SnapshotFence(++fenceReads[0], 1, "t")));
        Assertions.assertTrue(failure.getMessage().contains("kept changing"),
                "the failure must say the table moved and the operation is retryable:"
                        + " " + failure.getMessage());
        Assertions.assertEquals(6, fenceReads[0],
                "one fence pair per attempt, bounded to the retry budget");
    }

    /** One internal-table row built from a BaselinePlan (column order mirrors fromRow). */
    private static ResultRow rowOf(BaselinePlan plan) {
        return new ResultRow(List.of(
                Long.toString(plan.getId()), plan.getBindSql(), plan.getBindSqlDigest(),
                Long.toString(plan.getBindSqlHash()), plan.getPlanSql(), "NaN",
                Double.toString(plan.getCost()), Long.toString(plan.getQueryTimeMs()),
                plan.getSource().toString(), plan.getStatus().toString(),
                BaselineManager.toTs(plan.getCreateTime()),
                BaselineManager.toTs(plan.getUpdateTime()),
                "0", "0", "false", ""));
    }

    /**
     * round-25 #2: when the id-collision repair loses leadership the CREATE must FAIL. The
     * competing row is a DIFFERENT baseline another master already returned under the same
     * id, so publishing / returning the id with both rows alive would let a reload pick the
     * other incarnation - this caller's baseline would silently not exist.
     */
    @Test
    public void testCreateFailsWhenTheCollisionRepairLosesLeadership() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        AtomicBoolean leader = new AtomicBoolean(true);
        DemotingIdentityStore store = new DemotingIdentityStore(leader);
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.leaderProbeForTest = leader::get;
            store.foreign = baseline("zz-foreign", "select 9"); // ours wins ("aa" < "zz")
            BaselinePlan own = baseline("aa-own", "select 10");

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(own));
            Assertions.assertTrue(failure.getMessage().contains("no longer the master"),
                    failure.getMessage());
            Assertions.assertTrue(store.insertedId > 0, "the create must have reached the"
                    + " collision probe");
            Assertions.assertNull(manager.getBaseline(store.insertedId),
                    "a create whose repair failed must not be published");
            Assertions.assertEquals(1, store.countDigest(store.insertedId, "zz-foreign"),
                    "the competing row must be left untouched");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Identity store whose INSERT drops the mastership (the handoff lands between the
     * create's INSERT and its collision repair) and which injects a competing row into the
     * FIRST by-id probe.
     */
    private static class DemotingIdentityStore
            implements BaselineManager.IdAllocatorStoreForTest {
        final Map<Long, List<BaselinePlan>> rows = new ConcurrentHashMap<>();
        BaselinePlan foreign;
        long insertedId = -1;
        private final AtomicBoolean leader;
        private boolean injected;

        DemotingIdentityStore(AtomicBoolean leader) {
            this.leader = leader;
        }

        @Override
        public long watermark() {
            return 0;
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.computeIfAbsent(plan.getId(), id -> new ArrayList<>()).add(plan);
            insertedId = plan.getId();
            leader.set(false); // the handoff lands right after OUR insert
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (foreign != null && !injected) {
                injected = true;
                foreign.setId(id);
                rows.computeIfAbsent(id, k -> new ArrayList<>()).add(foreign);
            }
            return new ArrayList<>(rows.getOrDefault(id, List.of()));
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            rows.computeIfPresent(plan.getId(), (id, current) -> {
                List<BaselinePlan> updated = new ArrayList<>(current);
                updated.removeIf(row -> Objects.equals(row.getBindSqlDigest(), plan.getBindSqlDigest())
                        && Objects.equals(row.getPlanSql(), plan.getPlanSql()));
                return updated.isEmpty() ? null : updated;
            });
        }

        int countDigest(long id, String digest) {
            return (int) rows.getOrDefault(id, List.of()).stream()
                    .filter(row -> Objects.equals(row.getBindSqlDigest(), digest)).count();
        }
    }

    // ==================== SHOW uses the confirmed read (round-15) ====================

    /**
     * SHOW BASELINE PLANS used the ASYNCHRONOUS getAllBaselines(): right after startup / a
     * promotion (empty map, load not finished) it listed ZERO rows although durable
     * baselines existed, and a failed read never converged. The command now requires a
     * confirmed read (confirmGlobalRowsForShow, the same path round-27 exercises for
     * GLOBAL DDL completed on another FE); the query-matching read stays nonblocking and
     * empty.
     */
    @Test
    public void testConfirmedLoadIsRequiredForShow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            // startup / promotion state: the store is NOT loaded and the table gate is on
            manager.prepareLoadForTest();
            manager.setPersistToTableForTest(true);
            BaselineManager.forwardedDdlSyncForTest = () -> { };
            BaselineManager.snapshotReaderForTest = () -> {
                throw new RuntimeException("internal table not ready");
            };
            Assertions.assertThrows(IllegalStateException.class,
                    manager::confirmGlobalRowsForShow,
                    "SHOW must surface a retryable failure instead of listing ZERO rows"
                            + " from an unreadable store");
            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "the query-matching read stays nonblocking and simply empty");

            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d1", "p1"), 7L));
            manager.confirmGlobalRowsForShow();
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "the confirmed read publishes the durable rows for SHOW");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.forwardedDdlSyncForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== authoritative SHOW rows (round-27) ====================

    /**
     * Round-27: on a NON-master FE whose cache is already loaded, ensureLoadedConfirmed()
     * returned immediately and getAllBaselines() copied the OLD map - so a GLOBAL DDL the
     * master completed after this FE's load stayed invisible (a completed DROP stayed
     * listed) until the next refresh daemon cycle. The SHOW path now re-reads the durable
     * rows: the master sync runs BEFORE that read (observable through the seam) and the
     * fresh snapshot replaces the cache, so SHOW - and only SHOW - reflects the committed
     * state.
     */
    @Test
    public void testShowRefreshesDurableRowsDespiteLoadedCache() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        List<String> order = new ArrayList<>();
        try {
            // this FE loaded the table BEFORE the master's DDL: one row, and it is LOADED
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d1", "p1"), 7L));
            manager.loadFromInternalTable();
            Assertions.assertFalse(manager.getAllBaselines().isEmpty(), "precondition: loaded");

            // the master then CREATEd id 8 and DROPped 7; this FE has not refreshed yet
            BaselineManager.forwardedDdlSyncForTest = () -> order.add("sync");
            BaselineManager.snapshotReaderForTest = () -> {
                order.add("read");
                return Map.of(8L, withId(baseline("d2", "p2"), 8L));
            };
            manager.confirmGlobalRowsForShow();
            Assertions.assertEquals(List.of("sync", "read"), order,
                    "the master sync must precede the authoritative snapshot read");
            List<BaselinePlan> rows = manager.getAllBaselines();
            Assertions.assertEquals(1, rows.size(),
                    "SHOW must list the committed state, not this FE's old cache: " + rows);
            Assertions.assertEquals(8L, rows.get(0).getId(),
                    "the row created on the master is visible, the dropped one is gone");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.forwardedDdlSyncForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Round-27: when the authoritative read fails, SHOW must fail retryably instead of
     * printing the old cache as if it were confirmed. The published cache is NOT
     * invalidated (no committed write is known to have happened, unlike the
     * forwarded-DDL path), so query matching keeps its state and the next SHOW retries.
     */
    @Test
    public void testShowFailsRetryableWhenTheRowsCannotBeRead() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d1", "p1"), 7L));
            manager.loadFromInternalTable();
            BaselineManager.forwardedDdlSyncForTest = () -> { };

            BaselineManager.snapshotReaderForTest = () -> {
                throw new RuntimeException("internal table not ready");
            };
            Assertions.assertThrows(IllegalStateException.class,
                    manager::confirmGlobalRowsForShow,
                    "an unconfirmable read must surface as a retryable SHOW failure");
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "the cache stays usable for query matching (not invalidated)");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.forwardedDdlSyncForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== unresolved deletes / temporary markers (round-18) ====================
    // ==================== unresolved deletes / temporary markers (round-18) ====================

    /** Scripted identity store: the durable rows plus injectable read / delete failures. */
    private static final class IdentityStoreSimulator
            implements BaselineManager.IdAllocatorStoreForTest {
        private final Map<Long, BaselinePlan> rows = new java.util.concurrent.ConcurrentHashMap<>();
        private boolean failDelete;
        private boolean failRead;

        @Override
        public long watermark() {
            return 0;
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (failRead) {
                throw new RuntimeException("tablet unavailable");
            }
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (failDelete) {
                throw new RuntimeException("internal statement timed out after 10s");
            }
            rows.remove(plan.getId());
        }
    }

    /**
     * A failed DELETE whose reconciliation READ also fails must NOT be treated as proof
     * that the row is gone: dropBaseline would remove the cached entry and report success
     * although the durable row still existed, and the next refresh / restart resurrected
     * the dropped baseline.
     */
    @Test
    public void testUnconfirmableDeleteKeepsTheDropFailed() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-drop", "p-drop"));
            BaselinePlan created = manager.getBaseline(id);
            store.rows.put(id, created);
            BaselineManager.idAllocatorStoreForTest = store;

            store.failDelete = true;
            store.failRead = true;
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.dropBaseline(id),
                    "an unconfirmable delete must surface as a failure");
            Assertions.assertTrue(failure.getMessage().contains("could not be confirmed"),
                    failure.getMessage());
            Assertions.assertNotNull(manager.getBaseline(id),
                    "the unconfirmed drop must keep the cached row");

            // the row is READABLE and still present: still a failure
            store.failRead = false;
            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(id));
            Assertions.assertNotNull(manager.getBaseline(id));

            // only a readable ABSENT row proves the delete landed
            store.rows.clear();
            Assertions.assertTrue(manager.dropBaseline(id));
            Assertions.assertNull(manager.getBaseline(id));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The temporary-table marker inside ordinary TEXT (a predicate literal like
     * {@code s = '_#TEMP#_'} or a comment) is NOT a temporary-relation reference: the old
     * raw substring test rejected the persisted bind SQL on every refresh, so the
     * baseline silently vanished from every FE although its row stayed durable.
     */
    @Test
    public void testTemporaryMarkerInLiteralIsNotATemporaryRelation() throws Exception {
        BaselinePlan parsed = BaselineManager.parsePersistedRowForTest(row(
                "SELECT k FROM t1 WHERE s = '_#TEMP#_'",
                "SELECT k FROM t1 WHERE s = 'x'"));
        Assertions.assertEquals("SELECT k FROM t1 WHERE s = '_#TEMP#_'",
                parsed.getBindSql());
        Assertions.assertNotNull(parsed.getParameterizedBindPlan(),
                "the row must be fully usable after the load");

        // a legacy row that genuinely references the creator's temporary table by NAME
        // still fails closed
        Assertions.assertThrows(RuntimeException.class,
                () -> BaselineManager.parsePersistedRowForTest(row(
                        "SELECT k FROM `111_#TEMP#_t`",
                        "SELECT k FROM `111_#TEMP#_t`")));
    }

    /** One internal-table row (column order mirrors fromRow). */
    private static ResultRow row(String bindSql, String planSql) {
        return new ResultRow(List.of(
                "77", bindSql, "d-temp", "7", planSql, "NaN", "0", "0",
                "USER", "ENABLED", "2026-01-01 00:00:00", "2026-01-01 00:00:00",
                "0", "0", "false", ""));
    }

    // ==================== promotion-window reconciliation (round-19) ====================

    /**
     * A promoted follower can have loaded=true with a snapshot that MISSES a row created
     * on the prior master. The map miss must not be reported as "the baseline does not
     * exist": DROP BASELINE PLAN errored while DROP IF EXISTS reported success although
     * the durable row stayed (and resurrected on the next refresh). The miss is
     * reconciled against the durable table and the row is deleted by identity.
     */
    @Test
    public void testDropCacheMissReconcilesAgainstTheDurableTable() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            manager.setPersistToTableForTest(true);
            BaselineManager.snapshotReaderForTest = () -> Map.of();
            BaselineManager.idAllocatorStoreForTest = store;
            store.rows.put(21L, withId(baseline("d-drop2", "p-drop2"), 21L));

            Assertions.assertTrue(manager.dropBaseline(21L),
                    "DROP must reconcile the cache miss against the durable table");
            Assertions.assertTrue(store.rows.isEmpty(),
                    "the durable row must be deleted, not silently declared absent");

            store.rows.put(22L, withId(baseline("d-drop3", "p-drop3"), 22L));
            store.failRead = true;
            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(22L),
                    "an unconfirmable durable state must surface instead of a false absence");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A load snapshot read BEFORE a local mutation (CREATE which already passed its own
     * load check) must be DISCARDED: publishing it cleared the freshly inserted cache
     * entry (the durable row was intact but invisible on this FE until the next refresh).
     */
    @Test
    public void testLoadSnapshotPredatingALocalMutationIsDiscarded() throws Exception {
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
            // the snapshot is read BEFORE the local mutation
            BaselineManager.snapshotReaderForTest = () -> Map.of();
            Thread loader = new Thread(manager::loadFromInternalTable);
            loader.start();
            Assertions.assertTrue(hookEntered.await(5, TimeUnit.SECONDS),
                    "the load must reach the snapshot-read hook");

            BaselinePlan created = withId(baseline("d-local", "p-local"), 7L);
            manager.applyRefreshedBaselines(Map.of(7L, created));
            Assertions.assertNotNull(manager.getBaseline(7L),
                    "fixture: the local mutation published its row");

            hookRelease.countDown();
            loader.join(10_000);
            Assertions.assertNotNull(manager.getBaseline(7L),
                    "the snapshot predates the local mutation: publishing it must not erase"
                            + " the locally published row");

            // the retry reads a snapshot that CONTAINS the row and publishes it
            BaselineManager.snapshotReadStartedHookForTest = null;
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, created);
            manager.loadFromInternalTable();
            Assertions.assertNotNull(manager.getBaseline(7L));
        } finally {
            BaselineManager.snapshotReadStartedHookForTest = null;
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    /** Scripted status store: rows keyed by status with injectable delete failures. */
    private static final class StatusProtocolSimulator
            implements BaselineManager.StatusProtocolStoreForTest {
        private final Map<BaselineStatus, Integer> rows = new ConcurrentHashMap<>();
        private boolean failOldDeleteAfterCommit;
        private boolean failOldDeleteWithoutCommit;
        /**
         * When set together with {@link #failOldDeleteWithoutCommit}, the reconciliation
         * reads that follow the failed delete are UNCONFIRMABLE as well (every count
         * throws): the outcome is then genuinely unknown.
         */
        private boolean failReconcileReadAfterFailedDelete;
        /** Set by the failed delete: no read can tell what happened. */
        private boolean unconfirmableReads;
        /**
         * The conditional INSERT reports SQL OK but wrote NOTHING (its SELECT matched no
         * previous-status row): the previous-status row is NOT removed, so a store that
         * only counted "is the previous row still there" cannot see the zero write - the
         * affected-row count can.
         */
        private boolean failInsertWithoutWriting;

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            if (failInsertWithoutWriting) {
                return false; // matched no row -> wrote nothing (affected rows = 0)
            }
            insert(plan);
            return true;
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.merge(plan.getStatus(), 1, Integer::sum);
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            if (failOldDeleteAfterCommit && status == BaselineStatus.ENABLED) {
                rows.computeIfPresent(status, (k, v) -> Math.max(0, v - 1));
                throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
            }
            if (failOldDeleteWithoutCommit && status == BaselineStatus.ENABLED) {
                if (failReconcileReadAfterFailedDelete) {
                    unconfirmableReads = true;
                }
                throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
            }
            rows.computeIfPresent(status, (k, v) -> Math.max(0, v - 1));
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            if (unconfirmableReads) {
                throw new RuntimeException("internal table read timed out");
            }
            return rows.getOrDefault(status, 0);
        }
    }

    /**
     * DELETE(old) can COMMIT but report KV_TXN_MAYBE_COMMITTED: the catch must reconcile
     * instead of blindly deleting the new row, otherwise no durable version survives
     * (the next refresh / restart silently drops the baseline).
     */
    @Test
    public void testCommittedStatusDeleteDespiteAnErrorKeepsTheNewRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        StatusProtocolSimulator store = new StatusProtocolSimulator();
        try {
            long id = manager.createBaseline(baseline("d-st", "p-st"));
            BaselineManager.statusProtocolStoreForTest = store;
            store.rows.put(BaselineStatus.ENABLED, 1);
            store.failOldDeleteAfterCommit = true;

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "a committed delete that reported an error must be reconciled, not"
                            + " compensated away");
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
            Assertions.assertEquals(0, store.rows.getOrDefault(BaselineStatus.ENABLED, 0));
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.DISABLED, 0),
                    "exactly the new version must survive: " + store.rows);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The uncommitted counterpart: DELETE(old) failed AND did not commit. Reporting the
     * failure is correct, but the freshly inserted row must be KEPT: an old-row delete
     * that DID commit behind the reconciliation read (publication lag) would otherwise be
     * compensated into a state where neither status survives once that delete publishes.
     * Both rows stay, the load path resolves them deterministically (pickDurableWinner),
     * and the next refresh / ALTER retry reconciles the cache with the winner.
     */
    @Test
    public void testFailedStatusDeleteKeepsTheNewStatusAndReportsSuccess() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        StatusProtocolSimulator store = new StatusProtocolSimulator();
        try {
            long id = manager.createBaseline(baseline("d-st2", "p-st2"));
            BaselineManager.statusProtocolStoreForTest = store;
            store.rows.put(BaselineStatus.ENABLED, 1);
            store.failOldDeleteWithoutCommit = true;

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the CONFIRMED new-status row is the durable winner: the ALTER reports"
                            + " success");
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the cache must follow the durable winner - keeping the OLD status let"
                            + " queries replay a baseline the durable table already disabled");
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.ENABLED, 0),
                    "the old version must survive: " + store.rows);
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.DISABLED, 0),
                    "the new row is KEPT for an unknown outcome - a compensating delete of"
                            + " it would leave NOTHING behind when the old-row delete actually"
                            + " committed and only its publication lagged: " + store.rows);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-34 #1: SQL OK does NOT prove the conditional INSERT wrote a row. Across a
     * handoff the old leader can precheck ENABLED, pause, and then run its
     * {@code INSERT ... SELECT ... WHERE status = 'ENABLED'} AFTER the new master disabled
     * the baseline: the statement matches nothing, and if the new master re-ENABLEs before
     * the old leader checks, the previous row is present AGAIN - the old "previous row is
     * gone" conflict check stays silent while every new-status probe fails, so treating the
     * result as a committed flip published a status the durable table never had. The
     * zero-write signal (affected rows / the seam's insert result) must win: report the
     * conflict and publish NOTHING.
     */
    @Test
    public void testZeroRowConditionalInsertIsNeverPublishedAsAFlip() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        StatusProtocolSimulator store = new StatusProtocolSimulator();
        try {
            long id = manager.createBaseline(baseline("d-zerowrite", "p-zerowrite"));
            BaselineManager.statusProtocolStoreForTest = store;
            // durable state: ENABLED (the old leader prechecked exactly this)
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the conditional INSERT wrote NOTHING, yet the previous-status row is present
            // again (the concurrent flip landed and was flipped back)
            store.failInsertWithoutWriting = true;

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("row was gone"),
                    failure.getMessage());
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus(),
                    "an unobserved status must never be published");
            Assertions.assertEquals(0, store.rows.getOrDefault(BaselineStatus.DISABLED, 0),
                    "the failed flip must leave the durable state alone: " + store.rows);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-34 #5: the internal DATETIME stores only SECONDS. A DISABLE followed by an
     * ENABLE inside one second left EQUAL durable timestamps, and {@code pickDurableWinner}
     * prefers DISABLED on a tie - the refresh / restart silently reversed the later ENABLE
     * while this FE served it. A flip therefore advances its row past the newest existing
     * stored SECOND.
     */
    @Test
    public void testStatusFlipIsDurablyLaterThanTheRowItSupersedes() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselinePlan disabled = baseline("d-tie", "p-tie");
            disabled.setStatus(BaselineStatus.DISABLED);
            long id = manager.createBaseline(disabled);
            // the existing row was written inside the SAME second as the flip below
            long now = System.currentTimeMillis();
            BaselinePlan existing = store.rowsOf(id).get(0);
            existing.setUpdateTime(now);

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED));
            BaselinePlan enabled = store.rowsOf(id).stream()
                    .filter(row -> row.getStatus() == BaselineStatus.ENABLED)
                    .findFirst().orElseThrow();
            Assertions.assertTrue(
                    BaselineManager.toTs(enabled.getUpdateTime())
                            .compareTo(BaselineManager.toTs(now)) > 0,
                    "the flip must be stored LATER than the row it supersedes, even inside"
                            + " one second: " + enabled.getUpdateTime() + " vs " + now);
            BaselinePlan winner = store.rowsOf(id).get(0);
            for (BaselinePlan row : store.rowsOf(id)) {
                winner = BaselineManager.pickDurableWinner(winner, row);
            }
            Assertions.assertEquals(BaselineStatus.ENABLED, winner.getStatus(),
                    "the later ENABLE must win the durable pair instead of being reversed"
                            + " by the DISABLED tie-break");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The affected-row signal of the conditional status INSERT (see
     * {@code BaselineManager#insertWroteRows}): 0 means the statement matched no
     * previous-status row and wrote NOTHING, anything else (including "unknown") counts as
     * written and is then subject to the visibility confirmation.
     */
    @Test
    public void testConditionalInsertAffectedRowsDistinguishAZeroWrite() {
        Assertions.assertFalse(BaselineManager.insertWroteRows(0L),
                "a conditional INSERT that matched no row reported SQL OK with 0 affected"
                        + " rows: nothing was written");
        Assertions.assertTrue(BaselineManager.insertWroteRows(1L));
        Assertions.assertTrue(BaselineManager.insertWroteRows(5L));
        Assertions.assertTrue(BaselineManager.insertWroteRows(-1L),
                "an unknown count is NOT proof of a zero write");
    }

    /**
     * round-30 #2 / round-34 #2: an UNCONFIRMABLE old-row delete can no longer lose the
     * only other durable version - the confirmed INSERT alone decides the winner, so both
     * rows stay and the cache follows the new status.
     */
    @Test
    public void testFailedReconciliationReadKeepsBothStatusRows() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        StatusProtocolSimulator store = new StatusProtocolSimulator();
        try {
            long id = manager.createBaseline(baseline("d-st3", "p-st3"));
            BaselineManager.statusProtocolStoreForTest = store;
            store.rows.put(BaselineStatus.ENABLED, 1);
            store.failOldDeleteWithoutCommit = true;
            store.failReconcileReadAfterFailedDelete = true;

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the confirmed INSERT alone decides the outcome: the failing old-row"
                            + " delete cannot turn it back into a failure");
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the durable winner decides, not the (unreadable) old row");
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.ENABLED, 0),
                    "the old version must survive: " + store.rows);
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.DISABLED, 0),
                    "an unconfirmable outcome must never compensate the new row away: "
                            + store.rows);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-30 #4: a status ROUND TRIP (ENABLED at T0 -> DISABLE at T1 -> ENABLE at T2)
     * leaves every compared field at its T0 value, so the refresh discarded the fresh T2
     * row and kept reporting the stale T0 object forever (SHOW included). The persisted
     * update_time takes part in the comparison at the table's SECOND precision.
     */
    @Test
    public void testRefreshDetectsAStatusRoundTripByItsUpdateTime() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            long t0 = 1_700_000_000_000L;
            BaselinePlan cached = baseline("d-flip", "p-flip");
            cached.setId(11);
            cached.setCreateTime(t0);
            cached.setUpdateTime(t0);
            manager.applyRefreshedBaselines(java.util.Map.of(11L, cached));
            Assertions.assertEquals(t0, manager.getBaseline(11).getUpdateTime(),
                    "precondition: the follower cached the T0 row");

            // the master completed DISABLE (T1) and ENABLE (T2): same status, same SQL,
            // only the persisted update_time moved
            BaselinePlan roundTrip = baseline("d-flip", "p-flip");
            roundTrip.setId(11);
            roundTrip.setCreateTime(t0);
            roundTrip.setUpdateTime(t0 + 2_000L);
            manager.applyRefreshedBaselines(java.util.Map.of(11L, roundTrip));

            Assertions.assertEquals(t0 + 2_000L, manager.getBaseline(11).getUpdateTime(),
                    "a status round trip must NOT be discarded as 'unchanged' - the cached"
                            + " object would report its stale update_time forever");
        } finally {
            manager.clearForTest();
        }
    }

    /**
     * round-30 #7: an INSERT can report SQL OK (committed) while no read sees the row. The
     * CREATE fails retryably - but the id IS consumed. A client retry that allocated a
     * second id would write the same baseline twice: both rows publish under different
     * ids, and dropping the id the client was told about leaves the other one ACTIVE. The
     * retry must ADOPT the remembered identity instead (deferring until it is readable).
     */
    @Test
    public void testRetryOfAnUnconfirmedCreateDoesNotAllocateASecondId() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            // the INSERT commits, but no read sees the row within the probe budget
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> false;

            RuntimeException first = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(baseline("d-pend", "p-pend")));
            Assertions.assertTrue(first.getMessage().contains("not readable"),
                    first.getMessage());
            Assertions.assertEquals(1, store.rows.size(),
                    "the committed insert consumed exactly one id: " + store.rows);
            long consumedId = store.rows.keySet().iterator().next();
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "the unconfirmed identity must be remembered");

            // the retry DEFERS instead of writing a second row
            RuntimeException deferred = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(baseline("d-pend", "p-pend")));
            Assertions.assertTrue(deferred.getMessage().contains("awaiting publication"),
                    "the retry must defer until the committed write resolves: "
                            + deferred.getMessage());
            Assertions.assertEquals(1, store.rows.size(),
                    "no second id may be allocated for the same baseline: " + store.rows);

            // once the committed row is readable the deferral resolves and the durable-key
            // dedup ADOPTS that very row (the same id, no second row)
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> true;
            long adoptedId = manager.createBaseline(baseline("d-pend", "p-pend"));
            Assertions.assertEquals(consumedId, adoptedId,
                    "the retry must adopt the committed id, not allocate a new one");
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "a readable row resolves the remembered identity");
            Assertions.assertEquals(1, store.rows.size(),
                    "still no second row: " + store.rows);
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-22: write visibility is confirmed ====================

    /**
     * An internal INSERT can report OK with the transaction merely COMMITTED: the create
     * must not publish / return the id until the row is READABLE, and a short publication
     * lag is absorbed by the bounded read-back retries (the visibility seam simulates the
     * invisible window).
     */
    @Test
    public void testCreateWaitsForTheInsertToBecomeReadable() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            AtomicInteger probes = new AtomicInteger();
            // the first two read-backs still see the pre-publication state
            BaselineManager.durableVisibilityProbeForTest =
                    (id, status) -> probes.incrementAndGet() > 2;

            long id = manager.createBaseline(baseline("d-vis", "p-vis"));

            Assertions.assertTrue(probes.get() >= 3,
                    "the create must keep probing until the row is readable: " + probes.get());
            Assertions.assertNotNull(manager.getBaseline(id),
                    "the confirmed row is published under the returned id");
            Assertions.assertEquals(1, store.rowsOf(id).size());
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A row that NEVER becomes readable fails the create RETRYABLY and publishes nothing:
     * an id handed out for an invisible row could be re-allocated by a new master reading
     * the old MAX(id), and the winner rule would later discard one of the two rows.
     */
    @Test
    public void testUnreadableInsertFailsWithoutPublishingTheId() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> false;

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(baseline("d-hidden", "p-hidden")));
            Assertions.assertTrue(failure.getMessage().contains("not readable"),
                    failure.getMessage());
            Assertions.assertTrue(manager.getAllBaselines().isEmpty(),
                    "an unconfirmed write must not be published into the cache");
            Assertions.assertTrue(store.watermark() > 0,
                    "the committed row itself stays in the store (a retry adopts it)");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The DROP half: a reported-successful identity delete whose PUBLICATION lags past every
     * probe is still a committed delete - the DROP must fail CLOSED (remove the cache entry
     * and report success). Failing it instead kept an ACTIVE baseline in the master's cache
     * that ordinary queries kept replaying although the DROP had already landed.
     */
    @Test
    public void testDropFailsClosedWhenTheDeletePublicationLags() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-drop", "p-drop"));

            BaselineManager.durableVisibilityProbeForTest = (rowId, status) -> true;
            Assertions.assertTrue(manager.dropBaseline(id),
                    "a committed delete that is merely not visible yet IS the durable outcome");
            Assertions.assertNull(manager.getBaseline(id),
                    "the dropped baseline must not stay replayable in the cache");

            BaselineManager.durableVisibilityProbeForTest = (rowId, status) -> false;
            Assertions.assertFalse(manager.dropBaseline(id),
                    "a second drop of the same id reports the baseline as absent");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-23: durable identity reconciliation ====================

    /**
     * A REUSED id must not let the CREATE duplicate fast path hand back an id whose
     * durable row is a different baseline: the confirmation compares the full identity
     * (key + status + fingerprint), not just id + status.
     */
    @Test
    public void testCreateDoesNotAdoptAReusedId() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long originalId = manager.createBaseline(baseline("d-reuse", "p-reuse"));

            // the id is REUSED by another baseline (the original row was dropped)
            BaselinePlan reused = baseline("d-other", "p-other");
            reused.setId(originalId);
            reused.setStatus(BaselineStatus.ENABLED);
            store.replaceRows(originalId, List.of(reused));

            // the cached duplicate still holds the ORIGINAL identity
            long id = manager.createBaseline(baseline("d-reuse", "p-reuse"));
            Assertions.assertNotEquals(originalId, id,
                    "the reused id must not be returned for a different durable incarnation");
            Assertions.assertTrue(store.rowsOf(originalId).stream()
                            .anyMatch(row -> "d-other".equals(row.getBindSqlDigest())),
                    "the other incarnation stays untouched: " + store.rowsOf(originalId));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * An opposite-status ALTER over a STALE cache must not recreate a baseline the
     * previous master already dropped: the durable probe runs BEFORE the flip.
     */
    @Test
    public void testStaleCacheAlterDoesNotResurrectADroppedBaseline() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-resurrect", "p-resurrect"));
            store.replaceRows(id, List.of()); // the previous master dropped it durably

            Assertions.assertFalse(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "an ALTER for a durably dropped baseline must not report success");
            Assertions.assertNull(manager.getBaseline(id),
                    "the stale cache entry must be retired");
            Assertions.assertTrue(store.rowsOf(id).isEmpty(),
                    "no row may be INSERTed back: " + store.rowsOf(id));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A DROP over a promotion-window snapshot whose id now names a DIFFERENT baseline
     * must clear the id durably (the DROP is keyed by the user-facing id).
     */
    @Test
    public void testDropWipesALingeringRowOfAReusedId() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-wipe", "p-wipe"));

            BaselinePlan reused = baseline("d-new", "p-new");
            reused.setId(id);
            reused.setStatus(BaselineStatus.ENABLED);
            store.replaceRows(id, List.of(reused));

            Assertions.assertTrue(manager.dropBaseline(id));
            Assertions.assertTrue(store.rowsOf(id).isEmpty(),
                    "the lingering row of the reused id must be removed: " + store.rowsOf(id));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A same-id refresh must REPLACE a cached object whose persisted incarnation changed
     * in a replay-relevant field (schema fingerprint): keeping the stale object made
     * every later refresh repeat the choice and replay reject the newly valid baseline.
     */
    @Test
    public void testRefreshReplacesAChangedIncarnation() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            long id = manager.createBaseline(baseline("d-inc", "p-inc"));
            Assertions.assertNull(manager.getBaseline(id).getSchemaFingerprint(),
                    "precondition: the created row carries no fingerprint");

            // unchanged fields must KEEP the object (object identity for hot readers)
            BaselinePlan before = manager.getBaseline(id);
            BaselinePlan same = baseline("d-inc", "p-inc");
            same.setId(id);
            same.setStatus(BaselineStatus.ENABLED);
            manager.applyRefreshedBaselines(Map.of(id, same));
            Assertions.assertSame(before, manager.getBaseline(id),
                    "an unchanged row keeps the live object");

            // a NEW fingerprint is a different incarnation
            BaselinePlan changed = baseline("d-inc", "p-inc");
            changed.setId(id);
            changed.setStatus(BaselineStatus.ENABLED);
            changed.setSchemaFingerprint("new-fingerprint");
            manager.applyRefreshedBaselines(Map.of(id, changed));
            Assertions.assertEquals("new-fingerprint",
                    manager.getBaseline(id).getSchemaFingerprint(),
                    "the refresh must replace the object when the fingerprint changed");
        } finally {
            manager.clearForTest();
        }
    }

    // ==================== round-20: ALTER cache-miss / winner reconciliation ====================

    /**
     * A promoted follower can serve a snapshot that PREDATES a CREATE the previous
     * master already committed (isReady is set before forceReloadFromInternalTable
     * finishes). ALTER must reconcile the cache miss against the durable table - adopt
     * the row and flip its status - instead of reporting "does not exist".
     */
    @Test
    public void testAlterCacheMissAdoptsTheDurableRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselinePlan durable = baseline("d-adopt", "p-adopt");
            durable.setId(4242L);
            durable.setStatus(BaselineStatus.ENABLED);
            durable.setUpdateTime(1000L);
            store.insert(durable);
            Assertions.assertNull(manager.getBaseline(4242L), "precondition: cache miss");

            Assertions.assertTrue(manager.updateStatus(4242L, BaselineStatus.DISABLED),
                    "the ALTER must not report a present durable row as missing");
            BaselinePlan cached = manager.getBaseline(4242L);
            Assertions.assertNotNull(cached, "the durable row must be adopted into the cache");
            Assertions.assertEquals(BaselineStatus.DISABLED, cached.getStatus());
            Assertions.assertTrue(store.rowsOf(4242L).stream()
                            .anyMatch(row -> row.getStatus() == BaselineStatus.DISABLED),
                    "the requested status must reach the durable table: " + store.rowsOf(4242L));

            // control: a PROVEN absence still reports missing
            Assertions.assertFalse(manager.updateStatus(9999L, BaselineStatus.ENABLED),
                    "an id with no durable row must still be reported as missing");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The no-op confirmation must check the durable table: an unconfirmable state (the
     * read fails) must fail RETRYABLY - reporting success left a durably DISABLED /
     * DROPPED row while the cache served the opposite state until the next refresh.
     */
    @Test
    public void testNoOpAlterWithUnreadableDurableStateFails() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            long id = manager.createBaseline(baseline("d-unknown", "p-unknown"));
            BaselineManager.statusProtocolStoreForTest =
                    new BaselineManager.StatusProtocolStoreForTest() {
                        @Override
                        public void insert(BaselinePlan plan) {
                        }

                        @Override
                        public void deleteByIdAndStatus(long id, BaselineStatus status) {
                        }

                        @Override
                        public int countByIdAndStatus(long id, BaselineStatus status) {
                            throw new RuntimeException("tablet unavailable");
                        }
                    };
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED),
                    "an unreadable durable state must fail the ALTER retryably");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A failed status flip can leave BOTH rows behind; the reload resolves them with
     * pickDurableWinner (later updateTime wins). The no-op confirmation must apply the
     * SAME rule: a bare count returned MATCHES while the NEWER row carried the opposite
     * status, so the next ALTER back reported success without changing the effective
     * state. With row content available the probe now repairs towards the winner.
     */
    @Test
    public void testNoOpAlterReconcilesTheDurableWinner() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-winner", "p-winner"));
            BaselinePlan live = manager.getBaseline(id);
            Assertions.assertNotNull(live);
            live.setUpdateTime(500L); // the cached ENABLED row is the OLD version now
            BaselinePlan newerDisabled = baseline("d-winner", "p-winner");
            newerDisabled.setId(id);
            newerDisabled.setStatus(BaselineStatus.DISABLED);
            newerDisabled.setUpdateTime(2000L);
            store.insert(newerDisabled);

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED),
                    "the ALTER must reconcile towards the effective durable winner");
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus());
            List<BaselinePlan> rows = store.rowsOf(id);
            BaselinePlan winner = rows.get(0);
            for (int i = 1; i < rows.size(); i++) {
                winner = BaselineManager.pickDurableWinner(winner, rows.get(i));
            }
            Assertions.assertEquals(BaselineStatus.ENABLED, winner.getStatus(),
                    "the effective durable winner must be the requested status: " + rows);
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The count-only seam cannot order two durable rows (no update times): the probe
     * must fail closed instead of picking the cached status' row - the old count
     * returned MATCHES as soon as the OLDER row carried the expected status.
     */
    @Test
    public void testNoOpAlterRefusesAnUndecidableDurableWinner() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            long id = manager.createBaseline(baseline("d-tie", "p-tie"));
            BaselineManager.statusProtocolStoreForTest = new RecordingProtocolStore(1, 1);
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED),
                    "two durable versions without update times must fail closed");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * An ALTER whose persistInsert(durablePlan) reports an error BEFORE committing must
     * not be "confirmed" by the still-present OLD-status row: the reconciliation probe
     * checks the STATUS it wrote, so it sees ABSENT and the write fails visibly - the
     * old-version delete never runs. The old unconstrained probe matched the old row,
     * updateStatus then deleted it and NO durable version remained (the next refresh /
     * restart lost the baseline).
     */
    @Test
    public void testAmbiguousInsertIsNotConfirmedByTheOldStatusRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-ins", "p-ins"));
            store.failInsert = true;

            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED),
                    "an INSERT that never committed must not be confirmed by the OLD row");
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus(),
                    "the failed ALTER must not flip the live object");
            Assertions.assertEquals(1, store.rowsOf(id).size(),
                    "the old version must survive: " + store.rowsOf(id));
            Assertions.assertEquals(BaselineStatus.ENABLED, store.rowsOf(id).get(0).getStatus(),
                    "no durable version may be deleted: " + store.rowsOf(id));
        } finally {
            store.failInsert = false;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-26 #4: the load slot is claimed before spawning ====================

    /**
     * Concurrent SPM queries used to check {@code loaded} / {@code loadInProgress} and
     * then each start an {@code spm-baseline-async-load} thread; only the CAS winner
     * INSIDE the thread performed the read, every other thread exited immediately - a
     * query burst (worst while the internal table is unreadable) created a throwaway
     * thread per caller. The slot is now claimed atomically at SCHEDULING time: while
     * one load is in flight every other scheduler returns without spawning.
     */
    @Test
    public void testBackgroundLoadIsScheduledOncePerSlot() throws Exception {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            manager.prepareLoadForTest();
            AtomicInteger spawns = new AtomicInteger();
            BaselineManager.asyncLoadSpawnCountForTest = spawns;
            CountDownLatch readStarted = new CountDownLatch(1);
            CountDownLatch releaseRead = new CountDownLatch(1);
            BaselineManager.snapshotReaderForTest = () -> {
                readStarted.countDown();
                try {
                    releaseRead.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return Map.of();
            };

            List<Thread> schedulers = new ArrayList<>();
            for (int i = 0; i < 40; i++) {
                Thread scheduler = new Thread(manager::scheduleAsyncLoadForTest,
                        "spm-schedule-" + i);
                schedulers.add(scheduler);
                scheduler.start();
            }
            Assertions.assertTrue(readStarted.await(10, TimeUnit.SECONDS),
                    "the winning scheduler must start the load");
            for (Thread scheduler : schedulers) {
                scheduler.join(10_000);
            }
            Assertions.assertEquals(1, spawns.get(),
                    "one in-flight load must absorb the whole burst: " + spawns.get());
            releaseRead.countDown();
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.asyncLoadSpawnCountForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-26 #5: the create-path dedup is INDEXED ====================

    /**
     * Every GLOBAL CREATE filtered (bind_sql_digest, plan_sql) in SQL, but the table is
     * keyed / distributed only by id: the predicate scans every bucket and row while the
     * table grows without a cap (auto capture), so the fixed read timeout eventually
     * slowed down / failed CREATE. When the table's MAX(id) is not above the largest id
     * this store has seen, the store already holds every durable row: the by-key answer
     * comes from the in-memory index and NO durable read runs. The UT store cannot serve
     * SQL, so a CREATE that still reached the scan would fail here instead of succeeding.
     */
    @Test
    public void testCreateDedupIsServedFromTheStoreIndex() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            // the stale-fingerprint row a previous incarnation left behind
            BaselinePlan stale = baseline("fp-stale", "select 1");
            stale.setId(41L);
            stale.setSchemaFingerprint("fp-old");
            BaselineManager.snapshotReaderForTest = () -> Map.of(41L, stale);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertTrue(manager.hasBaselines(), "the fixture row must load");

            manager.setPersistToTableForTest(true);
            SimulatedStore store = new SimulatedStore();
            store.insert(stale); // MAX(id) == the largest id the store has seen
            BaselineManager.idAllocatorStoreForTest = store;

            // the SAME key under a NEW fingerprint: the in-memory duplicate check treats
            // it as stale, and the durable-key lookup must be answered by the index
            BaselinePlan fresh = baseline("fp-stale", "select 1");
            fresh.setSchemaFingerprint("fp-new");
            long id = manager.createBaseline(fresh);
            Assertions.assertTrue(id > 0);
            Assertions.assertNotEquals(41L, id, "a changed fingerprint gets a NEW row");
            Assertions.assertEquals(1, store.rowsOf(id).size(),
                    "the new row must be durable: " + store.rowsOf(id));
            Assertions.assertTrue(store.rowsOf(41L).isEmpty(),
                    "the stale row is retired through the by-id identity delete");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /** The scanned read is required exactly when the table carries an id the store never saw. */
    @Test
    public void testDurableScanDecisionFollowsTheTableWatermark() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            BaselinePlan row = baseline("fp-w", "select 2");
            row.setId(7L);
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, row);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();

            Assertions.assertFalse(manager.mustScanDurableForKey(7L),
                    "MAX(id) == the largest seen id: the store holds every durable row");
            Assertions.assertFalse(manager.mustScanDurableForKey(6L));
            Assertions.assertTrue(manager.mustScanDurableForKey(8L),
                    "a higher MAX(id) proves the table has a row the store never saw");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-35: drop eviction + transition evidence ================

    /**
     * round-35 #1: persistDeleteByIdentity CONFIRMS the requested row is gone, then
     * wipeDurableRowsById reads the table AGAIN to clean up a lingering OTHER
     * incarnation. When that cleanup read fails, the DROP reported the failure but the
     * cached ENABLED entry stayed in the map - ordinary queries kept matching and
     * REPLAYING a baseline whose durable row no longer existed until a later refresh.
     * The confirmed identity delete must evict the cache entry no matter what the
     * cleanup reports, while the DROP itself still fails retryably (the retry takes the
     * cache-miss path and finds the row already gone).
     */
    @Test
    public void testConfirmedIdentityDeleteEvictsTheCacheWhenTheCleanupReadFails() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            long id = manager.createBaseline(baseline("d-cleanup", "p-cleanup"));
            Assertions.assertNotNull(manager.getBaseline(id), "precondition: cached");
            store.failReadAfterDelete = true;

            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(id),
                    "the lingering-row cleanup failure still reports the DROP as failed");
            Assertions.assertNull(manager.getBaseline(id),
                    "the confirmed identity delete must stop the cache entry from matching /"
                            + " replaying a row that is already gone");
            Assertions.assertTrue(store.rowsOf(id).isEmpty(),
                    "the requested row IS durably gone: " + store.rowsOf(id));

            store.failReadAfterDelete = false;
            Assertions.assertFalse(manager.dropBaseline(id),
                    "the retry observes the row already gone (the confirmed delete landed)");
            Assertions.assertNull(manager.getBaseline(id));
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-35 #2: a previously failed old-row DELETE can leave a STALE row of the OLD
     * status beside the winner (round-30 keeps both rows when the delete outcome is
     * unknown). The status-only evidence then accepted that stale row as the publication
     * of an ENABLE whose conditional INSERT ABORTED: the probe found the leftover ENABLED
     * row of the failed DISABLE and {@code confirmInsertVisible} agreed, so a failed
     * DISABLED delete published ENABLED + success while a durable reload still picks the
     * DISABLED winner. The attempted STORED SECOND is the discriminator: a flip stores
     * its row strictly later than every row it met.
     */
    @Test
    public void testAStaleSameStatusRowDoesNotProveAnAbortedStatusInsert() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        BaselineManager.idAllocatorStoreForTest = store;
        try {
            BaselinePlan stale = baseline("d-shadow", "p-shadow");
            stale.setId(9L);
            stale.setUpdateTime(System.currentTimeMillis() - 5_000L);
            store.insert(stale); // the leftover ENABLED row of the failed DISABLE

            BaselinePlan attempted = baseline("d-shadow", "p-shadow");
            attempted.setId(9L);
            attempted.setUpdateTime(System.currentTimeMillis());
            Assertions.assertFalse(BaselineManager.observedInsertRowIsOurs(attempted),
                    "the stale ENABLED row at another stored second is NOT the row THIS"
                            + " ENABLE would have written");

            BaselinePlan written = baseline("d-shadow", "p-shadow");
            written.setId(9L);
            written.setUpdateTime(attempted.getUpdateTime());
            store.insert(written);
            Assertions.assertTrue(BaselineManager.observedInsertRowIsOurs(attempted),
                    "the row at the ATTEMPTED stored second proves the INSERT landed");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The visibility confirmation of a JUST-WRITTEN row asks for the ATTEMPTED stored
     * second (round-35 #2): a simulator that models the stale-row case implements the
     * three-argument form and must receive the attempted row's update time.
     */
    @Test
    public void testVisibilityConfirmationCarriesTheAttemptedStoredSecond() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        BaselineManager.idAllocatorStoreForTest = store;
        List<Long> probedSeconds = new ArrayList<>();
        BaselineManager.durableVisibilityProbeForTest =
                new BaselineManager.DurableVisibilityProbeForTest() {
                    @Override
                    public boolean isReadable(long id, BaselineStatus status) {
                        throw new AssertionError("the write confirmation must carry the"
                                + " attempted stored second");
                    }

                    @Override
                    public boolean isReadable(long id, BaselineStatus status, long updateTime) {
                        probedSeconds.add(updateTime);
                        return true;
                    }
                };
        try {
            long id = manager.createBaseline(baseline("d-second", "p-second"));
            Assertions.assertEquals(1, probedSeconds.size(),
                    "the create's insert must be confirmed exactly once");
            Assertions.assertEquals(manager.getBaseline(id).getUpdateTime() / 1000L,
                    probedSeconds.get(0) / 1000L,
                    "the confirmation must ask for the ATTEMPTED row's stored second");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== round-39: unresolved creates, identity, epochs ====================

    /**
     * round-39 #11: an INSERT error may be raised AFTER a commit (a statement timeout),
     * so it must surface as an UNCONFIRMED write: the create remembers the attempted
     * identity, the retry DEFERS (or adopts) instead of allocating a SECOND id, and no
     * baseline is published while the outcome is unresolved.
     */
    @Test
    public void testAmbiguousInsertErrorKeepsTheCreateIdentityPending() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-amb", "p-amb")),
                    "an error whose commit is uncertain must fail the create retryably");
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "the attempted identity must be remembered: the durable-key read cannot"
                            + " see an unpublished row");
            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "nothing may be published while the outcome is unresolved");

            // the retry DEFERS instead of consuming a second id
            IllegalStateException deferred = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-amb", "p-amb")));
            Assertions.assertTrue(deferred.getMessage().contains("awaiting publication"),
                    deferred.getMessage());

            // the committed row becomes READABLE: the retry adopts its id
            long reservedId = store.reservedHighWater;
            Assertions.assertTrue(reservedId > 0, "the id was reserved");
            BaselinePlan committed = baseline("d-amb", "p-amb");
            committed.setId(reservedId);
            store.replaceRows(reservedId, List.of(committed));
            long adopted = manager.createBaseline(baseline("d-amb", "p-amb"));
            Assertions.assertEquals(reservedId, adopted,
                    "the adopted id must be the reserved one - a second id left two"
                            + " ENABLED rows once the first published");
            Assertions.assertEquals(1, store.rowsOf(reservedId).size(),
                    "no second row may be inserted for the same baseline");
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "the adoption retires the pending record");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #4: the promotion reload must NOT clear the pending-create registry - the
     * reload cannot see an unpublished row, so the identity it describes is still the only
     * guard against a second id.
     */
    @Test
    public void testPromotionReloadKeepsUnconfirmedCreateIdentities() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-keep", "p-keep")));
            Assertions.assertEquals(1, manager.pendingCreateCountForTest());

            // the FE is demoted / re-promoted: the store is invalidated and reloaded
            manager.invalidatePublishedStoreForTest();
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "clearing the registry here let a re-promoted FE load a snapshot before"
                            + " the row published, see no key duplicate and assign a SECOND id");

            IllegalStateException deferred = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-keep", "p-keep")));
            Assertions.assertTrue(deferred.getMessage().contains("awaiting publication"),
                    deferred.getMessage());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #4 (cross-FE): the retry after a TRUE leader transfer runs on an FE whose
     * in-memory registry is empty. The identity-carrying id RESERVATION in the shared
     * sequence table is the durable fence: while its row is still unreadable the retry
     * defers instead of allocating a second id.
     */
    @Test
    public void testDurableReservationFencesACrossFeRetry() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-x", "p-x")));
            long reserved = store.reservedHighWater;
            Assertions.assertTrue(reserved > 0, "the id reservation must carry the identity");

            // a NEW leader: its in-memory registry is empty, the shared table is not
            manager.clearForTest();
            BaselineManager.idAllocatorStoreForTest = store;
            IllegalStateException deferred = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-x", "p-x")));
            Assertions.assertTrue(deferred.getMessage().contains("awaiting publication"),
                    "the durable reservation must defer the retry: " + deferred.getMessage());
            Assertions.assertEquals(reserved, store.reservedHighWater,
                    "the retry must NOT consume a second id");

            // the committed row publishes: the retry adopts the reserved id
            BaselinePlan committed = baseline("d-x", "p-x");
            committed.setId(reserved);
            store.replaceRows(reserved, List.of(committed));
            long adopted = manager.createBaseline(baseline("d-x", "p-x"));
            Assertions.assertEquals(reserved, adopted,
                    "the durable reservation's id must survive as the single row");
            Assertions.assertEquals(1, store.rowsOf(reserved).size());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #12: when the pending-create registry is FULL a NEW create fails admission
     * BEFORE it writes; an unresolved identity is never EVICTED. Evicting the oldest entry
     * let a retry of that key see its reserved sequence id but neither its row nor a
     * pending record - it allocated a new id and both rows later published ENABLED.
     */
    @Test
    public void testFullPendingRegistryFailsAdmissionInsteadOfEvicting() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            for (int i = 0; i < 64; i++) {
                final int key = i;
                Assertions.assertThrows(IllegalStateException.class,
                        () -> manager.createBaseline(baseline("d-" + key, "p-" + key)));
            }
            Assertions.assertEquals(64, manager.pendingCreateCountForTest(),
                    "every committed-but-unreadable identity stays recorded");
            long reservedBefore = store.reservedHighWater;

            // the 65th key fails ADMISSION: nothing more is written
            IllegalStateException full = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-65", "p-65")));
            Assertions.assertTrue(full.getMessage().contains("registry is full"),
                    full.getMessage());
            Assertions.assertEquals(reservedBefore, store.reservedHighWater,
                    "a refused admission must not even reserve an id");
            Assertions.assertEquals(64, manager.pendingCreateCountForTest(),
                    "the first key's identity is still recorded - no eviction");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-40 #5: a FULL registry of committed-but-unpublished creates must not keep
     * rejecting the retry of one of ITS OWN keys. The retry now resolves the remembered
     * write before the capacity check: the row had long become readable, so the record is
     * adopted (and retired) instead of throwing "registry is full".
     */
    @Test
    public void testFullRegistryRetryAdoptsItsPublishedRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            List<Long> reservedIds = fillPendingRegistry(manager, store, "d-f5-");
            Assertions.assertEquals(64, manager.pendingCreateCountForTest());
            publishPendingRows(store, "d-f5-", reservedIds);
            store.failInsert = false;

            // every row is readable now: the retry of the FIRST key adopts its reserved
            // id - before the fix it threw "registry is full" BEFORE resolvePendingCreate
            long adopted = manager.createBaseline(baseline("d-f5-0", "p-d-f5-0"));
            Assertions.assertEquals(reservedIds.get(0).longValue(), adopted,
                    "the retry must adopt the publication of its own key");
            Assertions.assertEquals(63, manager.pendingCreateCountForTest(),
                    "the adopted record is retired");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-40 #5: a create of a DIFFERENT key while the registry is full RECONCILES the
     * records first - their rows became readable (or their fence expired) and they stop
     * fencing - instead of refusing admission forever. Nothing is evicted while still
     * unresolved (round-39 #12 keeps that property).
     */
    @Test
    public void testFullRegistryReconcilesPublishedCreatesForANewKey() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            List<Long> reservedIds = fillPendingRegistry(manager, store, "d-f5b-");
            publishPendingRows(store, "d-f5b-", reservedIds);
            store.failInsert = false;

            long fresh = manager.createBaseline(baseline("d-f5b-new", "p-d-f5b-new"));
            Assertions.assertTrue(fresh > reservedIds.get(63),
                    "the new create proceeds on a reconciled registry: " + fresh);
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "every readable record was retired by the reconciliation");
            Assertions.assertNull(
                    store.pendingSeqReservation("d-f5b-0", SPMUtils.hashOf("p-d-f5b-0")),
                    "the durable markers of the reconciled records are retired too");
            Assertions.assertEquals(1, store.rowsOf(fresh).size());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-40 #7: adopting the committed row of an ambiguous create must RETIRE its
     * durable marker. Left behind, the marker made a DROP + immediate re-CREATE of the
     * same bind/plan defer for the whole marker fence: the probe found the old marker and
     * the (dropped) row's absence and reported "still awaiting publication".
     */
    @Test
    public void testDurableMarkerIsRetiredAfterTheAdoption() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-ret", "p-ret")));
            long reserved = store.reservedHighWater;
            Assertions.assertNotNull(
                    store.pendingSeqReservation("d-ret", SPMUtils.hashOf("p-ret")),
                    "the ambiguous create appended the durable marker");

            // the committed row publishes and the retry adopts it - the marker is retired
            BaselinePlan committed = baseline("d-ret", "p-ret");
            committed.setId(reserved);
            store.replaceRows(reserved, List.of(committed));
            store.failInsert = false;
            Assertions.assertEquals(reserved,
                    manager.createBaseline(baseline("d-ret", "p-ret")));
            Assertions.assertNull(
                    store.pendingSeqReservation("d-ret", SPMUtils.hashOf("p-ret")),
                    "a resolved marker must stop fencing");

            // DROP the adopted baseline and immediately re-CREATE the same bind/plan
            Assertions.assertTrue(manager.dropBaseline(reserved));
            long recreated = manager.createBaseline(baseline("d-ret", "p-ret"));
            Assertions.assertTrue(recreated > reserved,
                    "the re-create must not be blocked by the stale marker: " + recreated);
            Assertions.assertEquals(1, store.rowsOf(recreated).size());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-40 #12: the durable adoption must check the reserved row's SCHEMA
     * FINGERPRINT. L1 committed under F1 and missed its probes; L2 takes over, an ALTER
     * TABLE changes the schema to F2, and the retry on L2 finds the now-readable F1 row.
     * Adopting it would report success for a baseline every replay rejects as stale - the
     * row is retired and a fresh one is created under F2 (exactly like the in-memory
     * registry path).
     */
    @Test
    public void testDurableAdoptionRejectsAStaleFingerprint() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            store.failInsert = true;
            BaselinePlan original = baseline("d-fp", "p-fp");
            original.setSchemaFingerprint("F1");
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(original));
            long reserved = store.reservedHighWater;

            // a NEW leader (empty registry): the committed F1 row is readable, but the
            // current schema is F2
            manager.clearForTest();
            BaselineManager.idAllocatorStoreForTest = store;
            BaselinePlan committed = baseline("d-fp", "p-fp");
            committed.setId(reserved);
            committed.setSchemaFingerprint("F1");
            store.replaceRows(reserved, List.of(committed));
            store.failInsert = false;

            BaselinePlan retry = baseline("d-fp", "p-fp");
            retry.setSchemaFingerprint("F2");
            long id = manager.createBaseline(retry);
            Assertions.assertTrue(id > reserved,
                    "the stale F1 row must not be adopted: " + id);
            Assertions.assertTrue(store.rowsOf(reserved).isEmpty(),
                    "the stale committed row is retired: " + store.rowsOf(reserved));
            Assertions.assertEquals(1, store.rowsOf(id).size());
            Assertions.assertEquals("F2", store.rowsOf(id).get(0).getSchemaFingerprint(),
                    "the fresh row carries the CURRENT fingerprint");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Fills the pending-create registry with {@code 64} KEY-distinct ambiguous creates
     * (every INSERT throws before committing) and returns the id each attempt reserved.
     */
    private static List<Long> fillPendingRegistry(BaselineManager manager,
            SimulatedStore store, String prefix) {
        store.failInsert = true;
        List<Long> reservedIds = new ArrayList<>();
        for (int i = 0; i < 64; i++) {
            final int key = i;
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline(prefix + key, "p-" + prefix + key)));
            reservedIds.add(store.reservedHighWater);
        }
        return reservedIds;
    }

    /** Makes every committed-but-invisible row of {@link #fillPendingRegistry} readable. */
    private static void publishPendingRows(SimulatedStore store, String prefix,
            List<Long> reservedIds) {
        for (int i = 0; i < reservedIds.size(); i++) {
            BaselinePlan committed = baseline(prefix + i, "p-" + prefix + i);
            committed.setId(reservedIds.get(i));
            store.replaceRows(reservedIds.get(i), List.of(committed));
        }
    }

    /**
     * round-39 #7: the leadership is re-checked immediately before the id RESERVATION and
     * immediately before the ROW write. A demoted FE could pass the loop-top check and
     * pause; the new master then created the same key under N+1, and the old FE's
     * forwarded INSERT left TWO enabled baselines for it.
     */
    @Test
    public void testDemotionBeforeTheRowWriteStopsTheInsert() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            AtomicInteger checks = new AtomicInteger();
            // the loop-top and the reservation re-check pass; the WRITE-time re-check
            // fails (the handoff lands exactly in the pause the reviewer described)
            BaselineManager.leaderProbeForTest = () -> checks.incrementAndGet() <= 2;
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("d-w", "p-w")));
            Assertions.assertTrue(failure.getMessage().contains("no longer the master"),
                    failure.getMessage());
            Assertions.assertTrue(store.reservedHighWater > 0,
                    "the reservation happened before the demotion was noticed");
            Assertions.assertEquals(0, store.rowsOf(store.reservedHighWater).size(),
                    "the demoted FE must not write the baseline row - its INSERT would even"
                            + " FORWARD to the new master");
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "no pending record for a write that never started");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #9: after rapid flips gave ANOTHER baseline a FUTURE stored update_time,
     * flipping this one used to keep update_time = now - dwarfed by the future row - so
     * neither MAX(id), COUNT(*) nor MAX(update_time) (the paginated snapshot fence)
     * changed and a refresh could merge two states while the fence accepted the mix. The
     * flip must advance past the TABLE-wide newest stored second, so every flip moves
     * MAX(update_time).
     */
    @Test
    public void testStatusFlipAdvancesPastTheTableWideNewestStoredSecond() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-bump", "p-bump"));
            long future = System.currentTimeMillis() + 60_000L;
            BaselinePlan other = baseline("d-future", "p-future");
            other.setId(999L);
            other.setUpdateTime(future);
            store.replaceRows(999L, List.of(other));

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED));
            long flippedUpdateTime = store.rowsOf(id).stream()
                    .filter(row -> row.getStatus() == BaselineStatus.DISABLED)
                    .findFirst().orElseThrow().getUpdateTime();
            Assertions.assertTrue(flippedUpdateTime / 1000L > future / 1000L,
                    "the new-status row must exceed the TABLE-wide newest stored second ("
                            + flippedUpdateTime + " vs " + future + ")");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #5: the conditional status INSERT matches the CACHED row's IDENTITY as
     * well. With the cache holding B/id N while the durable winner is A/id N (a delayed
     * old-leader INSERT), matching only (id, previousStatus) wrote B's cached SQL with a
     * later timestamp - and the identity-scoped old-row DELETE cannot remove A, so a
     * reload replaced the durable baseline with stale B while ALTER reported success.
     */
    @Test
    public void testStaleCachedIncarnationIsNeverFlipped() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStatusStore store = new IdentityStatusStore(
                withId(baseline("d-A", "p-A"), 7L));
        try {
            // the cache holds B/id 7 (loaded before the old leader's delayed INSERT landed)
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, withId(baseline("d-B", "p-B"), 7L));
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertEquals("p-B", manager.getBaseline(7L).getPlanSql(),
                    "precondition: the cache is stale");
            BaselineManager.snapshotReaderForTest = null;

            // the ALTER of the stale incarnation: the identity-scoped condition matches
            // NO durable row -> retryable conflict, nothing written
            BaselineManager.statusProtocolStoreForTest = store;
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(7L, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("row was gone"),
                    failure.getMessage());
            Assertions.assertTrue(store.rowsOf(7L).stream().noneMatch(
                            row -> "p-B".equals(row.getPlanSql())
                                    && row.getStatus() == BaselineStatus.DISABLED),
                    "B's cached SQL must never be written with a later timestamp: "
                            + store.rowsOf(7L));
            Assertions.assertTrue(store.rowsOf(7L).stream().anyMatch(
                            row -> "p-A".equals(row.getPlanSql())),
                    "the durable winner A must be untouched");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * round-39 #14: a forwarded GLOBAL DDL's outcome is CONFIRMED before the snapshot may
     * publish. A GLOBAL DISABLE can return success while its DISABLED row is committed but
     * unreadable; republishing the old ENABLED snapshot kept replaying a baseline the
     * master already disabled on that connection. The bounded re-reads converge when the
     * publication lands, and an outcome that never appears fails CLOSED.
     */
    @Test
    public void testForwardedDdlOutcomeIsConfirmedBeforePublishing() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            manager.setPersistToTableForTest(true);
            AtomicInteger reads = new AtomicInteger();
            manager.prepareLoadForTest();
            BaselineManager.snapshotReaderForTest = () -> {
                if (reads.incrementAndGet() <= 2) {
                    return Map.of(7L, withId(baseline("d-fwd", "p-fwd"), 7L)); // pre-DDL
                }
                BaselinePlan disabled = withId(baseline("d-fwd", "p-fwd"), 7L);
                disabled.setStatus(BaselineStatus.DISABLED);
                return Map.of(7L, disabled);
            };
            manager.refreshAfterForwardedDdl(null,
                    BaselineManager.ForwardedDdlExpectation.status(7L, BaselineStatus.DISABLED));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(7L).getStatus(),
                    "the confirmed post-DDL outcome must be the published state");
            Assertions.assertTrue(reads.get() >= 3,
                    "the pre-DDL snapshot must be re-read until the flip is visible: "
                            + reads.get());

            // the outcome NEVER becomes visible: fail CLOSED, never republish the old row
            manager.clearForTest();
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d-fwd2", "p-fwd2"), 7L));
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.refreshAfterForwardedDdl(null,
                            BaselineManager.ForwardedDdlExpectation.status(7L,
                                    BaselineStatus.DISABLED)));
            Assertions.assertTrue(failure.getMessage().contains("cannot confirm"),
                    failure.getMessage());
            Assertions.assertFalse(manager.hasBaselines(),
                    "the unconfirmable refresh must invalidate the published cache");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Identity-keyed status store (round-39 #5): the conditional INSERT mirrors the
     * durable statement's (id, previousStatus, digest, planSql) condition, so a flip can
     * only land on the SAME incarnation it was computed from.
     */
    private static final class IdentityStatusStore implements BaselineManager.StatusProtocolStoreForTest {
        private final Map<Long, List<BaselinePlan>> rows = new ConcurrentHashMap<>();

        IdentityStatusStore(BaselinePlan... initial) {
            for (BaselinePlan row : initial) {
                rows.computeIfAbsent(row.getId(), k -> new ArrayList<>()).add(row);
            }
        }

        private List<BaselinePlan> rowsOf(long id) {
            return new ArrayList<>(rows.getOrDefault(id, List.of()));
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.computeIfAbsent(plan.getId(), k -> new ArrayList<>()).add(plan);
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            for (BaselinePlan row : rows.getOrDefault(plan.getId(), List.of())) {
                if (row.getStatus() == previousStatus
                        && java.util.Objects.equals(row.getBindSqlDigest(),
                                plan.getBindSqlDigest())
                        && java.util.Objects.equals(row.getPlanSql(), plan.getPlanSql())) {
                    insert(plan);
                    return true;
                }
            }
            return false; // nothing written: no identity-matching previous-status row
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            rows.computeIfPresent(id, (k, current) -> {
                List<BaselinePlan> updated = new ArrayList<>(current);
                updated.removeIf(row -> row.getStatus() == status);
                return updated.isEmpty() ? null : updated;
            });
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            int count = 0;
            for (BaselinePlan row : rows.getOrDefault(id, List.of())) {
                if (row.getStatus() == status) {
                    count++;
                }
            }
            return count;
        }

        @Override
        public long newestStoredUpdateSecond() {
            long max = 0;
            for (List<BaselinePlan> idRows : rows.values()) {
                for (BaselinePlan row : idRows) {
                    max = Math.max(max, row.getUpdateTime() / 1000L);
                }
            }
            return max;
        }
    }
}
