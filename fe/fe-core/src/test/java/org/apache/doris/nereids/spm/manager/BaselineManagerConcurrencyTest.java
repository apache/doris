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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Master-handoff safety of the global baseline store.
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

    /** One row's schema fingerprint with NULL and "" unified, exactly like IFNULL in SQL. */
    private static String normalizedFingerprint(BaselinePlan plan) {
        return plan.getSchemaFingerprint() == null ? "" : plan.getSchemaFingerprint();
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
         * The id high-water mark of the append-only SEQUENCE table: the ids
         * the create path has RESERVED, which survive a DROP of the row that held them -
         * exactly the durable state the baselines table's own MAX(id) loses.
         */
        private long reservedHighWater;
        /**
         * The DROP TOMBSTONES this FE appended, as id|digest|planSqlHash - the readable
         * shape production's scoped tombstone read returns (see appendDroppedMarker).
         */
        private final List<String> droppedMarkerRows =
                new java.util.concurrent.CopyOnWriteArrayList<>();
        /** When set, every tombstone append FAILS BEFORE landing (the DROP confirmation). */
        private boolean failMarkerAppend;
        /** Every mutation-clock advance requested by a mutation helper. */
        private int clockBumps;
        /** Invoked after a successful row INSERT (the handoff hook of the create tests). */
        private Runnable onInsert;
        /**
         * The LATEST identity record of each key, in the shape production reads it
         * ((last_id, reserve_time, unconfirmed, dropped)): the plain pre-INSERT
         * reservation every create appends ( - while its row is unreadable
         * and the record young it FENCES, exactly like the explicit marker), replaced by
         * the UNCONFIRMED marker of an ambiguous write or by a DROP TOMBSTONE.
         */
        private final Map<String, long[]> keyedReservations = new ConcurrentHashMap<>();

        /**
         * When set, every by-id read AFTER an identity delete fails: the
         * lingering-row cleanup of a DROP cannot be completed.
         */
        private boolean failReadAfterDelete;
        private boolean deleted;
        /** When set, every identity delete removes the row and THEN reports an error
         *  (a COMMITTED delete whose statement timed out - ). */
        private boolean failDeleteAfterCommit;
        /** When set, every identity delete reports an error WITHOUT removing the row
         *  (an ambiguous delete whose commit is unknown - ). */
        private boolean failDeleteKeepingRow;
        /**
         * When set, the id reservation just written is NOT readable yet (comments 4/6):
         * the sequence watermark keeps the PRE-reservation value, exactly what a
         * successor or a cross-FE retry reads while the publication lags.
         */
        private boolean holdReservationReadback;
        /** The watermark observed BEFORE the newest reservation (see holdReservationReadback). */
        private long laggedWatermark;

        private static String seqKey(String bindSqlDigest, long planSqlHash) {
            return bindSqlDigest + '\u0001' + planSqlHash;
        }

        @Override
        public long seqWatermark() {
            return holdReservationReadback ? laggedWatermark : reservedHighWater;
        }

        @Override
        public void reserveId(long id) {
            laggedWatermark = reservedHighWater;
            reservedHighWater = Math.max(reservedHighWater, id);
        }

        /**
         * The identity-carrying reservation (it RECORDS the
         * identity - production reads the latest record of the key, so a plain
         * reservation whose row stays unreadable fences a cross-FE retry for the durable
         * bound even when the explicit marker write failed / lagged).
         */
        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMs) {
            reserveId(id);
            keyedReservations.compute(seqKey(bindSqlDigest, planSqlHash), (key, current) ->
                    current != null && current[1] > reserveTimeMs
                            ? current : new long[] {id, reserveTimeMs, 0, 0});
        }

        @Override
        public void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
            keyedReservations.put(seqKey(bindSqlDigest, planSqlHash),
                    new long[] {id, atMillis, 1, 0});
        }

        /** The DROP TOMBSTONE of one identity ( / ). */
        @Override
        public void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            if (failMarkerAppend) {
                throw new RuntimeException("spm_baselines_seq append timed out");
            }
            keyedReservations.put(seqKey(bindSqlDigest, planSqlHash),
                    new long[] {id, atMillis, 0, 1});
            droppedMarkerRows.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
        }

        @Override
        public List<String> droppedMarkers() {
            return new ArrayList<>(droppedMarkerRows);
        }

        @Override
        public void bumpMutationClock() {
            clockBumps++;
        }

        @Override
        public BaselineManager.SeqReservation pendingSeqReservation(String bindSqlDigest,
                long planSqlHash) {
            long[] entry = keyedReservations.get(seqKey(bindSqlDigest, planSqlHash));
            return entry == null ? null : new BaselineManager.SeqReservation(
                    entry[0], entry[1], entry[2] == 1, entry[3] == 1);
        }

        /** The marker DELETE of a resolved ambiguous write. */
        @Override
        public void retirePendingSeqState(String bindSqlDigest, long planSqlHash, long markerId) {
            long[] entry = keyedReservations.get(seqKey(bindSqlDigest, planSqlHash));
            if (entry != null && entry[0] == markerId) {
                // the UNCONFIRMED row is DELETEd; the plain pre-INSERT reservation row of
                // the same attempt stays behind - it keeps the id and the
                // watermark, just without the marker semantics
                keyedReservations.put(seqKey(bindSqlDigest, planSqlHash),
                        new long[] {entry[0], entry[1], 0, 0});
            }
        }

        /**
         * Ages EVERY identity record (the fence of a plain reservation bounds a
         * RECENT unresolved write - a create that happened long ago must not defer a
         * legitimate re-create of its key).
         */
        void ageReservations(long millis) {
            for (String key : keyedReservations.keySet()) {
                keyedReservations.compute(key, (k, current) ->
                        new long[] {current[0], current[1] - millis, current[2], current[3]});
            }
        }

        /** The table-wide newest stored update_time. */
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
            if (onInsert != null) {
                onInsert.run();
            }
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
            if (failDeleteKeepingRow) {
                // an ambiguous delete: it REPORTED an error and the row is (still?) there -
                // it may just as well have committed with the publication lagging
                throw new RuntimeException("identity delete reported an error"
                        + " (statement timed out)");
            }
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
                // mirror DELETE_BY_IDENTITY_SQL: the SCHEMA FINGERPRINT completes the
                // identity, so one incarnation's delete can never remove the row of
                // ANOTHER incarnation sharing the id / digest / planSql
                updated.removeIf(row -> row.getBindSqlDigest().equals(plan.getBindSqlDigest())
                        && row.getPlanSql().equals(plan.getPlanSql())
                        && normalizedFingerprint(row).equals(normalizedFingerprint(plan)));
                return updated.isEmpty() ? null : updated;
            });
            if (failDeleteAfterCommit) {
                throw new RuntimeException("identity delete reported after commit"
                        + " (statement timed out)");
            }
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
     * A DROP must not make the id reusable. The baselines table's own MAX(id)
     * loses the highest id with its row, so a FE that never saw that row (a follower
     * promoting after the drop) would hand the id to a DIFFERENT baseline - and a delayed
     * DROP BASELINE PLAN IF EXISTS N retry for the old row would delete the new
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

    // ==================== confirmed post-forward DDL refresh ====================

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

    // ==================== status publishes only after durable success ====================

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
            // A read cannot PROVE the INSERT wrote nothing (publication of a
            // committed row lags), so the obsolete entry must stop serving matching and
            // the id stays fenced until the table shows the outcome - keeping the old
            // ENABLED state live let this FE replay a baseline whose committed DISABLED
            // row may publish at any moment
            Assertions.assertNull(manager.getBaseline(id),
                    "the obsolete entry must not stay matchable while the flip is unproven");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the unproven flip must be fenced");
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }

    // ==================== promotion window / stale fingerprint ====================

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

    // ====================: handoff fencing of the status flip ====================

    /**
     * Status store simulating both races: a handoff landing between the flip's
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
     * The DELETE half of a status flip is fenced like the identity delete. An
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
     * The INSERT half of a status flip is CONDITIONAL on the previous-status
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

    // ====================: forwarded-DDL visibility and collision fencing ====================

    /**
     * The post-forward refresh must SYNCHRONIZE with the master BEFORE it
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
     * The post-forward journal synchronization is part of the FAIL-CLOSED
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
     * The whole-table snapshot read is PAGINATED, because one SELECT * over a
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
     * BaselineManager#collectSnapshotPages): rows with id >= pageStart
     * (every row for the first page), ordered by id, skipping offset rows and
     * returning at most pageSize of them.
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
     * An id group can hold MORE rows than one snapshot page. The ALTER
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
     * An OFFSET continuation that lands inside an id group is only sound when
     * the page order is a TOTAL order over that group's rows. With ORDER BY `id`
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
                // a STABLE mutation clock: this test only exercises the pagination order
                () -> new BaselineManager.SnapshotFence(7L, 2001L, 14007L));
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
     * every run of rows that tie under the DECLARED order. Under ORDER BY `id` that
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
     * tieReorderingReader).
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
     * (fail-closed side): the fence alone cannot see a TRUNCATED page - a partial result
     * looks exactly like a short, completed page. Every empty / short page is therefore
     * followed by an END PROBE (a re-read of the same cursor): a probe that returns rows
     * the swallowed read hid proves the truncation, the snapshot must NOT be published,
     * and the read must fail closed after its bounded retries.
     */
    @Test
    public void testSnapshotReadFailsClosedWhenPagesAreTruncated() {
        int[] fenceReads = {0};
        AtomicInteger calls = new AtomicInteger();
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManager.readStableSnapshot(
                        // the page read swallows its rows (e.g. a cancelled internal query)
                        // and only the END PROBE of the same cursor reveals them - on
                        // EVERY attempt, so no read may ever be published
                        (pageStart, offset) -> calls.getAndIncrement() % 2 == 0
                                ? List.of()
                                : List.of(rowOf(withId(baseline("d9", "select k from t9"),
                                        5L))),
                        () -> {
                            fenceReads[0]++;
                            // a fence that does NOT move: only the end probe can reject
                            return new BaselineManager.SnapshotFence(2L, 2L, 2L);
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
     * A status flip whose INSERT reported SQL OK with the transaction
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
     * A CREATE whose INSERT committed but stayed unreadable is remembered as
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
     * Round-53 #2: the pending-create identity is the schema INCARNATION, not just the
     * (digest, planSql) key. FE A leaves an ambiguous F1 CREATE at id N whose row stays
     * unreadable; after `ALTER TABLE t ADD COLUMN x` the schema is F2 and the promoted FE
     * B reserves the SAME id N (A's reservation was invisible to its watermark read) and
     * creates the same SQL successfully under F2. On A's re-promotion the retry is
     * analyzed under F2 - the CURRENT schema.
     *
     * Matching A's F1 entry by the key alone made that retry (a) DEFER for the whole
     * fence bound ("a previously COMMITTED write of the same baseline is still awaiting
     * publication") although the F1 write is a different incarnation, and (b) risk
     * adopting B's F2 row as A's own. The retry must pass the stale entry by, retire it
     * (its own identity), leave B's successful row untouched, and create under F2.
     */
    @Test
    public void testPendingCreateFenceDoesNotDeferAnotherSchemaIncarnation() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            // FE A: an ambiguous CREATE under F1; the INSERT never commits, the outcome is
            // unknown and the row stays unreadable
            store.failInsert = true;
            BaselinePlan abandoned = baseline("d-inc", "p-inc");
            abandoned.setSchemaFingerprint("F1");
            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(abandoned));
            long contestedId = store.reservedHighWater;
            Assertions.assertTrue(contestedId > 0);
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "the ambiguous F1 write must be remembered");

            // the promoted FE B creates the same SQL under F2 - on the very same id
            store.failInsert = false;
            BaselinePlan successfulF2 = baseline("d-inc", "p-inc");
            successfulF2.setId(contestedId);
            successfulF2.setSchemaFingerprint("F2");
            store.insert(successfulF2);

            // A's re-promotion retries the CREATE; the statement is re-analyzed under the
            // CURRENT schema F2 and must NOT be deferred by the abandoned F1 entry
            BaselinePlan retry = baseline("d-inc", "p-inc");
            retry.setSchemaFingerprint("F2");
            long id = manager.createBaseline(retry);
            Assertions.assertTrue(id > contestedId,
                    "the retry must allocate above the consumed id: " + id);
            List<BaselinePlan> atContested = store.rowsOf(contestedId);
            Assertions.assertEquals(1, atContested.size(),
                    "B's successful row must survive A's abandoned write: " + atContested);
            Assertions.assertEquals("F2", atContested.get(0).getSchemaFingerprint());
            Assertions.assertNull(manager.getBaseline(contestedId),
                    "B's F2 row must never be adopted as A's F1 incarnation");
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "the stale F1 entry no longer fences");
            Assertions.assertEquals("F2", manager.getBaseline(id).getSchemaFingerprint());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Round-53 #2, readable variant: A's abandoned F1 row DID commit and becomes readable
     * while B's successful F2 row sits under the same id. Retiring the stale incarnation
     * must delete ONLY the F1 row - the identity-scoped DELETE carries the fingerprint -
     * so B's already REPORTED-successful baseline survives the retry's cleanup.
     */
    @Test
    public void testStaleIncarnationCleanupKeepsTheSuccessfulOne() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            // A: the INSERT commits but its publication never becomes visible for this FE
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> false;
            BaselinePlan committedF1 = baseline("d-inc2", "p-inc2");
            committedF1.setSchemaFingerprint("F1");
            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(committedF1));
            long contestedId = store.reservedHighWater;
            Assertions.assertEquals(1, store.rowsOf(contestedId).size(),
                    "the ambiguous write IS durable");
            Assertions.assertEquals(1, manager.pendingCreateCountForTest());

            // the row becomes readable AND the promoted FE B created the same SQL under F2
            // on the same id (a handoff collision), so both incarnations share the id
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselinePlan successfulF2 = baseline("d-inc2", "p-inc2");
            successfulF2.setId(contestedId);
            successfulF2.setSchemaFingerprint("F2");
            store.insert(successfulF2);

            BaselinePlan retry = baseline("d-inc2", "p-inc2");
            retry.setSchemaFingerprint("F2");
            long id = manager.createBaseline(retry);
            Assertions.assertTrue(id > contestedId, "a fresh id is allocated: " + id);
            List<BaselinePlan> survivor = store.rowsOf(contestedId);
            Assertions.assertEquals(1, survivor.size(),
                    "only the stale F1 incarnation may be retired: " + survivor);
            Assertions.assertEquals("F2", survivor.get(0).getSchemaFingerprint(),
                    "the CURRENT incarnation's row must survive");
            Assertions.assertEquals(0, manager.pendingCreateCountForTest());
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The paginated snapshot must describe ONE state of the table. The loop
     * issues one SELECT per page and the internal table offers no read view, so a DDL
     * committing between two pages would be merged into a state that never existed: a
     * concrete example reads ENABLED low-id A on page 1, the master drops A and creates
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
        long[] tick = {0};
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
                    tick[0]++; // the mutation clock moves with both writes
                    table.clear();
                    table.put(9L, List.of(withId(baseline("dB", "select k from tb"), 9L)));
                }
                return rows;
            }, () -> {
                fenceCalls.add("fence");
                return new BaselineManager.SnapshotFence(tick[0], tick[0], tick[0]);
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
     * (fail-closed side): a table that never stays stable while its snapshot is
     * read must NOT be published - a mixed state would let a dropped / re-created baseline
     * keep replaying until the next refresh. The read gives up after its bounded retries
     * with a RETRYABLE error (every caller re-reads on its next cycle).
     */
    @Test
    public void testSnapshotReadFailsClosedWhenTheTableNeverStaysStable() {
        int[] fenceReads = {0};
        IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                () -> BaselineManager.readStableSnapshot((pageStart, offset) -> List.of(),
                        () -> new BaselineManager.SnapshotFence(++fenceReads[0], 1L,
                                fenceReads[0])));
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
                // the TIMESTAMPTZ columns are read as epoch millis (see storedMillis)
                Long.toString(plan.getCreateTime()),
                Long.toString(plan.getUpdateTime()),
                "0", "0", "false", ""));
    }

    /**
     * When the id-collision repair loses leadership the CREATE must FAIL. The
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
     * The create's LAST leadership check must follow its row write: an internal INSERT of
     * a demoted FE FORWARDS to the new master and can commit right after the check before
     * the write, so the row exists durably while this FE no longer owns the write epoch -
     * and the promoted master's concurrent create for the SAME key (its MAX(id) read
     * predates this reservation) allocated another id, leaving two same-key rows that each
     * by-id probe sees alone (the reviewer's handoff twin). Publishing / returning the id
     * must therefore fail retryably once the write is observed on a drained leader.
     */
    @Test
    public void testCreateIsRefusedWhenTheRowWriteWinsTheDemotionRace() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        AtomicBoolean rowWritten = new AtomicBoolean();
        try {
            store.onInsert = () -> rowWritten.set(true);
            BaselineManager.idAllocatorStoreForTest = store;
            // every check BEFORE the row write passes; the store flips the mastership
            // while the INSERT is in flight
            BaselineManager.leaderProbeForTest = () -> !rowWritten.get();

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(baseline("aa-late-demotion", "select 11")));
            Assertions.assertTrue(failure.getMessage().contains("no longer the master"),
                    failure.getMessage());
            Assertions.assertTrue(store.watermark() > 0,
                    "the forwarded INSERT landed durably");
            Assertions.assertTrue(manager.getAllBaselines().isEmpty(),
                    "a drained leader must not publish the row it can no longer own: "
                            + manager.getAllBaselines().size() + " published");
        } finally {
            BaselineManager.leaderProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The same-key twins of a handoff window must converge on ONE baseline: the
     * deterministic owner is the LATER allocation (the larger id - the allocation order
     * every writer agrees on), the loser leaves the published map on every FE, and a
     * master-side publish also REPAIRS the loser away durably - otherwise a DROP of the
     * owner (or any reload) let the twin resurface and silently resurrect a "dropped"
     * baseline.
     */
    @Test
    public void testSameKeyTwinsCollapseToTheLaterAllocationAndAreRepaired() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselinePlan loser = withId(baseline("d-twin", "select twin"), 40L);
            BaselinePlan owner = withId(baseline("d-twin", "select twin"), 41L);
            store.insert(loser);
            store.insert(owner);

            manager.applyRefreshedBaselines(new java.util.HashMap<>(
                    Map.of(40L, loser, 41L, owner)));

            Assertions.assertNotNull(manager.getBaseline(41L),
                    "the later allocation owns the key");
            Assertions.assertNull(manager.getBaseline(40L),
                    "the twin must never be publishable: two enabled rows of one key"
                            + " (each by-id probe sees only its own id)");
            Assertions.assertTrue(store.rowsOf(40L).isEmpty(),
                    "the master-side publish repairs the loser durably: " + store.rowsOf(40L));
            Assertions.assertEquals(1, store.rowsOf(41L).size(),
                    "the owner's row is untouched");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Every durable mutation helper must bump the bounded mutation clock BEFORE its
     * statement: the stable-snapshot fence compares the (MAX(tick), COUNT(*), SUM(tick))
     * tuple around its paginated read, so a mutation that committed while the tick was
     * missed is invisible to the fence and a mixed snapshot could be certified stable
     * (the reviewer's C1). One bump per CREATE, status flip and DROP is the contract.
     */
    @Test
    public void testEveryMutationHelperAdvancesTheMutationClock() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-clock", "select clock"));
            int afterCreate = store.clockBumps;
            Assertions.assertTrue(afterCreate > 0, "the CREATE advanced the clock");

            manager.updateStatus(id, BaselineStatus.DISABLED);
            int afterFlip = store.clockBumps;
            Assertions.assertTrue(afterFlip > afterCreate,
                    "the status flip advanced the clock: " + afterCreate + " -> " + afterFlip);

            Assertions.assertTrue(manager.dropBaseline(id), "the DROP must succeed");
            Assertions.assertTrue(store.clockBumps > afterFlip,
                    "the DROP advanced the clock: " + afterFlip + " -> " + store.clockBumps);
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A DROP may only report success once its tombstones are READABLE (the reviewer's C4:
     * the best-effort appends previously only warned and the DROP succeeded, so a delayed
     * write of the removed row - or the unreadable loser of a handoff collision - revived
     * the "dropped" baseline on every FE whose filter lived in process memory), and the
     * process-local pending marker must NEVER fake that readability (C5): the scoped read
     * masks, the durable read stays empty, and with the append failing the DROP is
     * retryable while the row itself is already gone.
     */
    @Test
    public void testDropFailsWhenTheTombstonesCannotBeConfirmedDurably() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-marker", "select marker"));
            store.failMarkerAppend = true;

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.dropBaseline(id));
            Assertions.assertTrue(failure.getMessage().contains("retry the DROP"),
                    failure.getMessage());
            Assertions.assertTrue(store.readById(id).isEmpty(),
                    "the delete itself landed: the marker is the only unresolved half");
            Assertions.assertNull(manager.getBaseline(id),
                    "the fenced DROP must stop replaying the removed row");
            // C5: the failed append lives in THIS process only - and must not be
            // mistakable for a durable tombstone
            Assertions.assertTrue(
                    BaselineManager.droppedIdentitiesForTest(Set.of(id), true).isEmpty(),
                    "the DURABLE read must not see the failed append");
            Assertions.assertFalse(
                    BaselineManager.droppedIdentitiesForTest(Set.of(id), false).isEmpty(),
                    "the scoped read still masks the id locally (fail closed)");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The status protocol must be scoped by the FULL identity: matching (id, status)
     * alone let a REUSED id's newer incarnation (same id, different digest / planSql /
     * fingerprint) be read, flipped or counted as if it were the row this FE wrote (the
     * reviewer's C9). All three statements therefore bind the schema fingerprint exactly
     * like the identity delete does.
     */
    @Test
    public void testStatusProtocolSqlIsScopedByTheFullIdentity() throws Exception {
        for (String name : List.of("DELETE_BY_ID_AND_STATUS_SQL",
                "INSERT_IF_PREVIOUS_STATUS_SQL", "COUNT_BY_ID_AND_STATUS_SQL")) {
            java.lang.reflect.Field field = BaselineManager.class.getDeclaredField(name);
            field.setAccessible(true);
            String sql = (String) field.get(null);
            Assertions.assertTrue(
                    sql.contains("IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'"),
                    name + " must match the schema fingerprint: " + sql);
            if (!name.startsWith("COUNT")) {
                Assertions.assertTrue(sql.contains("`bind_sql_digest` = '${bindSqlDigest}'"),
                        name + " must match the identity: " + sql);
                Assertions.assertTrue(sql.contains("`plan_sql` = '${planSql}'"),
                        name + " must match the identity: " + sql);
            }
        }
    }

    /**
     * Identity store whose mastership drops while the by-id collision PROBE runs (the
     * handoff lands between the create's INSERT and its collision repair) and which
     * injects a competing row into the FIRST post-insert by-id probe.
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
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            if (insertedId > 0 && foreign != null && !injected) {
                injected = true;
                foreign.setId(id);
                rows.computeIfAbsent(id, k -> new ArrayList<>()).add(foreign);
                // the handoff lands as the collision probe / repair is about to run:
                // nothing of the competing row may be touched from here on
                leader.set(false);
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

    // ==================== SHOW uses the confirmed read ====================

    /**
     * A DROP TOMBSTONE must filter a durable row a delayed status write
     * revived. The status flip INSERTs the new row BEFORE deleting the old one, so a
     * demoted master's in-flight INSERT can commit AFTER the DROP removed the row - its
     * conditional precondition ran against the pre-DROP snapshot and Doris cannot
     * re-check it at commit. The append-only tombstone survives that commit; a load that
     * sees a matching (id, digest, planSqlHash) row must treat it as deleted AND repair
     * it away.
     */
    @Test
    public void testDropTombstoneFiltersAndRepairsTheRevivedRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselinePlan revived = withId(baseline("d-revive", "p-revive"), 7L);
            // a row WITHOUT a tombstone loads normally (the filter must not over-match)
            store.rows.put(7L, revived);
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, revived);
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "no tombstone exists: the row is published");

            // the completed DROP appends the identity tombstone; a delayed status write
            // then revives the row in the store. clearForTest() drops every test seam
            // (persistence included), so they are re-installed for the second load.
            store.appendTombstone(revived);
            manager.clearForTest();
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, revived);
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();

            Assertions.assertTrue(manager.getAllBaselines().isEmpty(),
                    "a row whose identity carries a DROP tombstone must not enter the"
                            + " matchable cache: " + manager.getAllBaselines());
            Assertions.assertTrue(store.rows.isEmpty(),
                    "the load must repair the revived row away: " + store.rows.keySet());
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * (comment #2): the per-id winner is picked among the rows that SURVIVED the
     * tombstone filter. A demoted leader's abandoned CREATE is condemned with an
     * identity-only tombstone and can publish late under the SAME id as another
     * leader's successful CREATE; when the condemned row wins the timestamp / digest
     * tie, filtering the already-COLLAPSED map removed the whole id - the valid row
     * stayed hidden from refresh and SHOW until a leader repaired the condemned row
     * and another refresh ran.
     */
    @Test
    public void testTombstonedTwinDoesNotHideTheValidRowOfTheSameId() {
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselinePlan valid = withId(baseline("d-valid", "select k from t1"), 7L);
            BaselinePlan condemned = withId(baseline("d-condemned", "select k from t1"), 7L);
            // the condemned row would WIN the tie (later update time) if it took part
            condemned.setUpdateTime(valid.getUpdateTime() + 60_000L);
            store.appendTombstone(condemned);
            BaselineManager.idAllocatorStoreForTest = store;

            Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = BaselineManager.collectFilteredSnapshot((pageStart, offset) ->
                        offset == 0 ? List.of(rowOf(condemned), rowOf(valid)) : List.of());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            Assertions.assertEquals(1, snapshot.size(),
                    "the valid row must survive the condemned twin: " + snapshot.keySet());
            Assertions.assertEquals("d-valid", snapshot.get(7L).getBindSqlDigest(),
                    "the condemned row must not remove the whole id: " + snapshot);
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
        }
    }

    /**
     * Right after a completed DROP, a fresh FE filters the dropped row from
     * its snapshot with the readable tombstone while a by-KEY lookup (the cached
     * duplicate / the durable-key read) still sees the pre-delete row during the delete's
     * publication delay. Adopting it reported CREATE success for the dropped baseline -
     * and the row vanished once the DELETE became readable. Every adoption path must
     * detect the tombstone, repair the remnant away and allocate a FRESH identity.
     */
    @Test
    public void testCreateDoesNotAdoptATombstonedByKeyRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.snapshotReaderForTest = () -> Map.of(41L,
                    withId(baseline("d-key", "p-key"), 41L));
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertEquals(1, manager.getAllBaselines().size(), "precondition: loaded");

            // the DROP completed (its tombstone is readable) and the row's own DELETE lags
            // behind: the cache AND the by-key read still see the incarnation
            BaselinePlan remnant = withId(baseline("d-key", "p-key"), 41L);
            store.rows.put(41L, remnant);
            store.appendTombstone(remnant);

            long fresh = manager.createBaseline(baseline("d-key", "p-key"));
            Assertions.assertNotEquals(41L, fresh,
                    "a tombstoned by-key row must not be adopted as the duplicate");
            Assertions.assertEquals(1, store.rows.size(),
                    "exactly the fresh row stays durable: " + store.rows.keySet());
            Assertions.assertTrue(store.rows.containsKey(fresh),
                    "the fresh identity is durable: " + store.rows.keySet());
            Assertions.assertNull(manager.getBaseline(41L),
                    "the dropped incarnation must not stay matchable");
            Assertions.assertNotNull(manager.getBaseline(fresh));
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * (pending-create half): a committed-but-unreadable CREATE is ADOPTED by
     * id when its row finally publishes. If the identity was DROPPED in the meantime (the
     * delayed row and the tombstone are both readable, only the DELETE lags), adopting it
     * reported success for the dropped baseline. The retry must allocate a FRESH identity
     * and retire the remnant.
     */
    @Test
    public void testPendingCreateDoesNotAdoptATombstonedRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.durableVisibilityProbeForTest = (id, status) -> false;
            BaselinePlan first = baseline("d-pending", "p-pending");
            RuntimeException failed = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.createBaseline(first));
            Assertions.assertTrue(failed.getMessage().contains("not readable"),
                    failed.getMessage());
            long pendingId = store.rows.keySet().iterator().next();
            Assertions.assertEquals(1, manager.pendingCreateCountForTest(),
                    "the committed identity must be remembered");

            // the row publishes - but its DROP completed in the meantime (only the DELETE
            // lags behind)
            BaselinePlan remnant = store.rows.get(pendingId);
            store.appendTombstone(remnant);
            BaselineManager.durableVisibilityProbeForTest = null;

            long fresh = manager.createBaseline(baseline("d-pending", "p-pending"));
            Assertions.assertNotEquals(pendingId, fresh,
                    "a tombstoned pending-create row must not be adopted");
            Assertions.assertFalse(store.rows.containsKey(pendingId),
                    "the dropped incarnation must be repaired away: " + store.rows.keySet());
            Assertions.assertEquals(1, store.rows.size(),
                    "exactly the fresh row stays durable: " + store.rows.keySet());
            Assertions.assertNotNull(manager.getBaseline(fresh));
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "the resolved fence must not linger");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * (ALTER half): the cache-miss reconciliation of an ALTER must not adopt a
     * row whose DROP completed - the row only lags its own delete, and reporting the ALTER
     * against it would modify a baseline on its way out.
     */
    @Test
    public void testAlterDoesNotAdoptATombstonedCacheMissRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        IdentityStoreSimulator store = new IdentityStoreSimulator();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            BaselineManager.snapshotReaderForTest = () -> Map.of();
            manager.setPersistToTableForTest(true);
            manager.prepareLoadForTest();
            manager.loadFromInternalTable();
            Assertions.assertEquals(0, manager.getAllBaselines().size(), "precondition: empty");

            BaselinePlan remnant = withId(baseline("d-alter", "p-alter"), 51L);
            store.rows.put(51L, remnant);
            store.appendTombstone(remnant);

            Assertions.assertFalse(manager.updateStatus(51L, BaselineStatus.DISABLED),
                    "a tombstoned durable row must read as absent, not be adopted");
            Assertions.assertTrue(store.rows.isEmpty(),
                    "the revived remnant must be repaired away: " + store.rows.keySet());
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A forwarded CREATE's outcome fence cannot name the master's id (the
     * follower cannot know which id the master allocated), so the expectation matches ANY
     * id that carries the created bind+plan TEXT - and a snapshot without it does not
     * satisfy the fence.
     */
    @Test
    public void testForwardedCreateExpectationMatchesTheCreatedText() {
        BaselineManager.ForwardedDdlExpectation created =
                BaselineManager.ForwardedDdlExpectation.created(
                        "select k from t1", "p-created");
        BaselinePlan row = withId(baseline("d-created", "p-created"), 42L);
        Assertions.assertTrue(created.isSatisfiedBy(Map.of(42L, row)),
                "any id carrying the created bind+plan text satisfies the expectation");
        Assertions.assertFalse(created.isSatisfiedBy(Map.of()),
                "no row at all does not satisfy it");
        BaselinePlan other = withId(baseline("d-other", "p-other"), 43L);
        Assertions.assertFalse(created.isSatisfiedBy(Map.of(43L, other)),
                "a different identity does not satisfy it");
        Assertions.assertTrue(created.describe().contains("CREATE"),
                "the failure message must name the create: " + created.describe());
    }

    /**
     * SHOW BASELINE PLANS used the ASYNCHRONOUS getAllBaselines(): right after startup / a
     * promotion (empty map, load not finished) it listed ZERO rows although durable
     * baselines existed, and a failed read never converged. The command now requires a
     * confirmed read (confirmGlobalRowsForShow, the same path exercises for
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

    // ==================== authoritative SHOW rows ====================

    /**
     * On a NON-master FE whose cache is already loaded, ensureLoadedConfirmed
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
     * When the authoritative read fails, SHOW must fail retryably instead of
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

    // ==================== unresolved deletes / temporary markers ====================
    // ==================== unresolved deletes / temporary markers ====================

    /** Scripted identity store: the durable rows plus injectable read / delete failures. */
    private static final class IdentityStoreSimulator
            implements BaselineManager.IdAllocatorStoreForTest {
        private final Map<Long, BaselinePlan> rows = new java.util.concurrent.ConcurrentHashMap<>();
        /** The DROP TOMBSTONES this FE appended, as id|digest|planSqlHash. */
        private final List<String> dropped = new java.util.concurrent.CopyOnWriteArrayList<>();
        private boolean failDelete;
        private boolean failRead;
        /** When set, every tombstone append fails before landing (the DROP confirmation). */
        private boolean failMarkerAppend;

        /** Appends the tombstone a completed DROP of row would have written. */
        void appendTombstone(BaselinePlan row) {
            dropped.add(row.getId() + "|" + row.getBindSqlDigest() + "|"
                    + org.apache.doris.nereids.spm.SPMUtils.hashOf(row.getPlanSql()));
        }

        @Override
        public List<String> droppedMarkers() {
            return new ArrayList<>(dropped);
        }

        @Override
        public void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            if (failMarkerAppend) {
                throw new RuntimeException("spm_baselines_seq append timed out");
            }
            dropped.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
        }

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
     * A failed DELETE whose reconciliation READ also fails must NOT be reported as a
     * success, and the possibly-deleted entry must leave the matchable cache
     * #10): the committed delete may merely lag its publication, and keeping the entry
     * let ordinary queries replay a baseline the user dropped until a refresh. The fence
     * holds the id out of every applied snapshot until a durable readback resolves it.
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
            Assertions.assertNull(manager.getBaseline(id),
                    "the possibly-deleted row must stop matching immediately");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the ambiguous delete stays fenced until the table shows the outcome");

            // the row is READABLE and still present: still a failure, still fenced
            store.failRead = false;
            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(id));
            Assertions.assertNull(manager.getBaseline(id));
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id));

            // only a readable ABSENT row proves the delete landed: the empty snapshot
            // resolves the fence (the cache miss of a retry then reports the absence)
            store.rows.clear();
            manager.applyRefreshedBaselines(Map.of());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id));
            Assertions.assertNull(manager.getBaseline(id));
            Assertions.assertFalse(manager.dropBaseline(id),
                    "the retry sees the delete already landed: no durable row is left");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * The temporary-table marker inside ordinary TEXT (a predicate literal like
     * s = '_#TEMP#_' or a comment) is NOT a temporary-relation reference: the old
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
                "USER", "ENABLED", "1767225600000", "1767225600000",
                "0", "0", "false", ""));
    }

    // ==================== promotion-window reconciliation ====================

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
         * When set together with failOldDeleteWithoutCommit, the reconciliation
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
     * SQL OK does NOT prove the conditional INSERT wrote a row. Across a
     * handoff the old leader can precheck ENABLED, pause, and then run its
     * INSERT ... SELECT ... WHERE status = 'ENABLED' AFTER the new master disabled
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
     * The internal DATETIME stores only SECONDS. A DISABLE followed by an
     * ENABLE inside one second left EQUAL durable timestamps, and pickDurableWinner
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
     * BaselineManager#insertWroteRows): 0 means the statement matched no
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
     * /: an UNCONFIRMABLE old-row delete can no longer lose the
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
     * A status ROUND TRIP (ENABLED at T0 -> DISABLE at T1 -> ENABLE at T2)
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
     * An INSERT can report SQL OK (committed) while no read sees the row. The
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

    // ====================: write visibility is confirmed ====================

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

    // ====================: pending mutation fences ====================

    /**
     * The conditional status INSERT may commit while its row stays unreadable
     * (the statement reported an error and the old row is still the only readable one).
     * Reconciling against that readable row left the OLD status replayable until a later
     * refresh; now the entry is fenced out - matching falls back - until the durable table
     * shows the flip.
     */
    @Test
    public void testAmbiguousStatusInsertHidesTheEntryUntilTheFlipIsVisible() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-amb", "p-amb"));
            BaselinePlan old = store.rowsOf(id).get(0);

            store.failInsert = true; // the transition INSERT reports an error (may have committed)
            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "an unproven flip must be fenced");
            Assertions.assertNull(manager.getBaseline(id),
                    "the obsolete ENABLED entry must not stay replayable");

            // a daemon snapshot still showing only the OLD row must not republish it
            manager.applyRefreshedBaselines(Map.of(id, old));
            Assertions.assertNull(manager.getBaseline(id),
                    "a stale snapshot must not resurrect the pre-flip row");

            // the committed DISABLED row publishes: the fence resolves and the flip shows
            BaselinePlan committed = old.copyPersistedScalars();
            committed.setStatus(BaselineStatus.DISABLED);
            committed.setUpdateTime(old.getUpdateTime() + 1000L);
            manager.applyRefreshedBaselines(Map.of(id, committed));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the visible outcome is applied");
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
                    "a visible outcome resolves the fence");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A committed DISABLE whose new row is temporarily unreadable must
     * survive a daemon snapshot that still carries the old ENABLED row.
     */
    @Test
    public void testCompletedDisableSurvivesAStaleDaemonSnapshot() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-dis", "p-dis"));
            BaselinePlan old = store.rowsOf(id).get(0);

            // the INSERT(DISABLED) committed, but no read sees it within the probe budget
            BaselineManager.durableVisibilityProbeForTest = (rowId, status) -> false;
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the unreadable committed flip must be fenced");

            BaselineManager.durableVisibilityProbeForTest = null;
            manager.applyRefreshedBaselines(Map.of(id, old));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus(),
                    "the stale ENABLED row must not revert the completed flip");

            // the flip becomes visible: the fence resolves
            BaselinePlan winner = store.rowsOf(id).get(0);
            for (BaselinePlan row : store.rowsOf(id)) {
                winner = BaselineManager.pickDurableWinner(winner, row);
            }
            manager.applyRefreshedBaselines(Map.of(id, winner));
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id));
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * An identity DELETE whose outcome is UNKNOWN (it reported an error
     * while the row is still readable - the delete may have committed with its
     * publication lagging) must not leave the dropped baseline replayable: the entry
     * leaves the cache immediately, and the fence keeps a later stale snapshot from
     * resurrecting it until a durable readback resolves the outcome.
     */
    @Test
    public void testAmbiguousIdentityDeleteFencesTheCache() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-ambdel", "p-ambdel"));
            BaselinePlan old = store.rowsOf(id).get(0);

            store.failDeleteKeepingRow = true; // the DELETE reported an error; the row stayed
            Assertions.assertThrows(RuntimeException.class, () -> manager.dropBaseline(id),
                    "an unprovable delete must surface as a retryable failure");
            Assertions.assertEquals(1, store.rowsOf(id).size(),
                    "the simulator kept the durable row for the resolution");
            Assertions.assertNull(manager.getBaseline(id),
                    "the possibly-deleted row must stop matching immediately");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the ambiguous delete is fenced until the table shows the outcome");

            manager.applyRefreshedBaselines(Map.of(id, old));
            Assertions.assertNull(manager.getBaseline(id),
                    "a stale snapshot still carrying the row must not republish it");

            manager.applyRefreshedBaselines(Map.of());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
                    "the confirmed absence resolves the fence");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * committed variant: the DELETE removed the row and THEN reported an
     * error (a statement timeout after commit). The reported error still surfaces on a
     * retry-able path, but the drop itself is treated as landed (the row is gone) and the
     * fence keeps a stale snapshot from bringing it back.
     */
    @Test
    public void testCommittedDeleteReportingAnErrorStillDropsTheBaseline() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-cmt", "p-cmt"));
            BaselinePlan old = store.rowsOf(id).get(0);

            store.failDeleteAfterCommit = true; // the DELETE committed, then errored
            Assertions.assertTrue(manager.dropBaseline(id),
                    "the committed delete is the durable outcome");
            Assertions.assertTrue(store.rowsOf(id).isEmpty(), "the row is really gone");
            Assertions.assertNull(manager.getBaseline(id));
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the delete's publication may lag: the id stays fenced");

            manager.applyRefreshedBaselines(Map.of(id, old));
            Assertions.assertNull(manager.getBaseline(id),
                    "a stale snapshot still carrying the row must not republish it");

            manager.applyRefreshedBaselines(Map.of());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
                    "the confirmed absence resolves the fence");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A completed DROP whose delete publication lags every probe must not be
     * re-added by a later daemon snapshot that still contains the row.
     */
    @Test
    public void testCompletedDropSurvivesAStaleDaemonSnapshot() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long id = manager.createBaseline(baseline("d-lag", "p-lag"));
            BaselinePlan old = store.rowsOf(id).get(0);

            BaselineManager.durableVisibilityProbeForTest = (rowId, status) -> true;
            Assertions.assertTrue(manager.dropBaseline(id),
                    "a committed delete that is merely not visible yet IS the durable outcome");
            Assertions.assertNull(manager.getBaseline(id));
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the completed DROP is fenced until the table shows it gone");

            manager.applyRefreshedBaselines(Map.of(id, old));
            Assertions.assertNull(manager.getBaseline(id),
                    "the daemon must not resurrect a dropped row from a stale snapshot");

            manager.applyRefreshedBaselines(Map.of());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
                    "the visible absence resolves the fence");
        } finally {
            BaselineManager.durableVisibilityProbeForTest = null;
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * A forwarded GLOBAL DDL whose expected outcome never becomes visible is
     * RETAINED as a fence - a later read on THIS FE (SHOW's confirmed read, the daemon)
     * must keep masking the contradicting old row, not just the forwarded statement.
     */
    @Test
    public void testForwardedDdlExpectationKeepsFencingLaterReads() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            BaselinePlan stale = baseline("d-fwd", "p-fwd");
            stale.setId(7L);
            stale.setStatus(BaselineStatus.ENABLED);
            // the follower's local read keeps returning the pre-DDL row
            BaselineManager.snapshotReaderForTest = () -> Map.of(7L, stale);
            BaselineManager.forwardedDdlSyncForTest = () -> { };
            manager.prepareLoadForTest();
            Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.refreshAfterForwardedDdl(null,
                            BaselineManager.ForwardedDdlExpectation.absent(7L)),
                    "an expected DROP whose delete never becomes visible fails closed");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(7L),
                    "the expectation must be retained for later reads");

            manager.applyRefreshedBaselines(Map.of(7L, stale));
            Assertions.assertNull(manager.getBaseline(7L),
                    "the retained fence must keep the dropped row masked");

            manager.applyRefreshedBaselines(Map.of());
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(7L),
                    "the visible absence resolves the fence");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            BaselineManager.forwardedDdlSyncForTest = null;
            manager.clearForTest();
        }
    }

    // ====================: durable identity reconciliation ====================

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
            // the reuse happened long AFTER the original create: the fence
            // bounds a RECENT unresolved write, so the legit re-create must not defer
            store.ageReservations(6 * 60 * 1000L);

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

    // ====================: ALTER cache-miss / winner reconciliation ====================

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
            // The outcome is unproven (a committed row may merely lag its
            // publication), so the ENABLED entry stops serving matching and the id is
            // fenced until the table shows the outcome - the old row's presence must
            // never be read as confirmation of the requested status
            Assertions.assertNull(manager.getBaseline(id),
                    "the failed ALTER must not leave an unproven status matchable");
            Assertions.assertTrue(manager.hasPendingMutationFenceForTest(id),
                    "the failed ALTER must fence the id");
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

    // ====================: the load slot is claimed before spawning ====================

    /**
     * Concurrent SPM queries used to check loaded / loadInProgress and
     * then each start an spm-baseline-async-load thread; only the CAS winner
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

    // ====================: the create-path dedup is INDEXED ====================

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

    // ====================: drop eviction + transition evidence ================

    /**
     * PersistDeleteByIdentity CONFIRMS the requested row is gone, then
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
     * A previously failed old-row DELETE can leave a STALE row of the OLD
     * status beside the winner ( keeps both rows when the delete outcome is
     * unknown). The status-only evidence then accepted that stale row as the publication
     * of an ENABLE whose conditional INSERT ABORTED: the probe found the leftover ENABLED
     * row of the failed DISABLE and confirmInsertVisible agreed, so a failed
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
     * second: a simulator that models the stale-row case implements the
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

    // ====================: unresolved creates, identity, epochs ====================

    /**
     * An INSERT error may be raised AFTER a commit (a statement timeout),
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
     * The promotion reload must NOT clear the pending-create registry - the
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
     * (cross-FE): the retry after a TRUE leader transfer runs on an FE whose
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
     * When the pending-create registry is FULL a NEW create fails admission
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
     * A FULL registry of committed-but-unpublished creates must not keep
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
     * A create of a DIFFERENT key while the registry is full RECONCILES the
     * records first - their rows became readable (or their fence expired) and they stop
     * fencing - instead of refusing admission forever. Nothing is evicted while still
     * unresolved ( keeps that property).
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
            BaselineManager.SeqReservation reconciled = store.pendingSeqReservation(
                    "d-f5b-0", SPMUtils.hashOf("p-d-f5b-0"));
            Assertions.assertNotNull(reconciled,
                    "the plain reservation row of the reconciled attempt stays (round-44 #5)");
            Assertions.assertFalse(reconciled.unconfirmed,
                    "the UNCONFIRMED marker is retired; only the plain reservation remains");
            Assertions.assertEquals(1, store.rowsOf(fresh).size());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Adopting the committed row of an ambiguous create must RETIRE its
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
            BaselineManager.SeqReservation marker = store.pendingSeqReservation(
                    "d-ret", SPMUtils.hashOf("p-ret"));
            Assertions.assertNotNull(marker, "the ambiguous create appended the durable marker");
            Assertions.assertTrue(marker.unconfirmed, "the record IS the unconfirmed marker");

            // the committed row publishes and the retry adopts it - the marker is retired
            BaselinePlan committed = baseline("d-ret", "p-ret");
            committed.setId(reserved);
            store.replaceRows(reserved, List.of(committed));
            store.failInsert = false;
            Assertions.assertEquals(reserved,
                    manager.createBaseline(baseline("d-ret", "p-ret")));
            BaselineManager.SeqReservation resolved = store.pendingSeqReservation(
                    "d-ret", SPMUtils.hashOf("p-ret"));
            Assertions.assertNotNull(resolved,
                    "the plain pre-INSERT reservation row survives the marker retirement"
                            + " (round-44 #5): it keeps the id / watermark");
            Assertions.assertFalse(resolved.unconfirmed,
                    "a resolved marker must stop fencing (unconfirmed = false)");

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
     * The durable adoption must check the reserved row's SCHEMA
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
            // Round-51 #15: the reservation identity now FOLDS the schema fingerprint, so the
            // F2 attempt cannot see the F1 attempt's sequence reservation at all - the row is
            // no longer retired through the scripted identity store (production retires it on
            // the next create's SQL by-key repair, which this store cannot express). What must
            // hold here is the SAFETY half: the stale row never enters the matchable cache and
            // never competes with the fresh incarnation.
            Assertions.assertNull(manager.getBaseline(reserved),
                    "the stale committed row must stay unmatchable: " + store.rowsOf(reserved));
            Assertions.assertTrue(store.rowsOf(reserved).stream()
                            .allMatch(row -> "F1".equals(row.getSchemaFingerprint())),
                    "only the stale F1 incarnation may linger at the old id: " + store.rowsOf(reserved));
            Assertions.assertEquals(1, store.rowsOf(id).size());
            Assertions.assertEquals("F2", store.rowsOf(id).get(0).getSchemaFingerprint(),
                    "the fresh row carries the CURRENT fingerprint");
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Fills the pending-create registry with 64 KEY-distinct ambiguous creates
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

    /** Makes every committed-but-invisible row of fillPendingRegistry readable. */
    private static void publishPendingRows(SimulatedStore store, String prefix,
            List<Long> reservedIds) {
        for (int i = 0; i < reservedIds.size(); i++) {
            BaselinePlan committed = baseline(prefix + i, "p-" + prefix + i);
            committed.setId(reservedIds.get(i));
            store.replaceRows(reservedIds.get(i), List.of(committed));
        }
    }

    /**
     * The leadership is re-checked immediately before the id RESERVATION and
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
     * After rapid flips gave ANOTHER baseline a FUTURE stored update_time,
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
     * The conditional status INSERT matches the CACHED row's IDENTITY as
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
     * A forwarded GLOBAL DDL's outcome is CONFIRMED before the snapshot may
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

    // ==================== comment 1: the mutation window is observable ====================

    /**
     * The tick alone cannot reject a snapshot that straddled a mutation: it is written
     * BEFORE the mutation's own statement (see beginRowMutation), so a DROP's DELETE can
     * land between two pages while BOTH fence reads see the SAME already-bumped tick - the
     * published map then mixes the two states. A concrete shape: page one ends ON the
     * dropped id (2000), the continuation's `id >= 2000 OFFSET 1` skips the successor
     * (2001) that slid into the removed slot, and the end probe accepts the short read - so
     * the dropped baseline kept replaying AND the successor went missing. The window
     * (pending) is held OPEN across attempt one's BOTH fence reads here: only the pending
     * gate can reject that mixed read.
     */
    @Test
    public void testSnapshotReadRetriesWhileAMutationWindowIsOpen() throws Exception {
        Map<Long, List<BaselinePlan>> table = new java.util.LinkedHashMap<>();
        for (long id = 1; id <= 1999; id++) {
            table.put(id, List.of(withId(baseline("dw" + id, "select k from tw" + id), id)));
        }
        table.put(2000L, List.of(withId(baseline("dw2000", "select k from tw2000"), 2000L)));
        table.put(2001L, List.of(withId(baseline("dw2001", "select k from tw2001"), 2001L)));

        long[] tick = {6L}; // the DROP's begin tick (beginRowMutation)
        long[] pending = {6L}; // ... whose window stays open across both reads of attempt 1
        AtomicInteger pageCalls = new AtomicInteger();
        CountDownLatch pageOneRead = new CountDownLatch(1);
        CountDownLatch deleteLanded = new CountDownLatch(1);
        Thread drop = new Thread(() -> {
            try {
                pageOneRead.await();
                table.remove(2000L); // the DELETE half of the open window
                deleteLanded.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        drop.start();
        AtomicInteger fenceReads = new AtomicInteger();
        List<String> fenceLog = new ArrayList<>();
        Map<Long, BaselinePlan> snapshot;
        try {
            snapshot = BaselineManager.readStableSnapshot((pageStart, offset) -> {
                List<ResultRow> rows = table.entrySet().stream()
                        .filter(entry -> pageStart == null || entry.getKey() >= pageStart)
                        .flatMap(entry -> entry.getValue().stream())
                        .skip(offset)
                        .map(BaselineManagerConcurrencyTest::rowOf)
                        .collect(java.util.stream.Collectors.toList());
                if (pageCalls.getAndIncrement() == 0) {
                    // page one IS the full 2,000-row page ending on id 2000: the DELETE
                    // commits while the page is in flight (its view still holds id 2000)
                    pageOneRead.countDown();
                    try {
                        deleteLanded.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                return rows;
            }, () -> {
                int read = fenceReads.incrementAndGet();
                fenceLog.add(read + ":" + tick[0] + "|" + pending[0]);
                BaselineManager.SnapshotFence fence = new BaselineManager.SnapshotFence(
                        tick[0], 2001L, 12006L, pending[0]);
                if (read == 2) {
                    // endRowMutation: the window closes with a NEWER tick - AFTER attempt
                    // one's second read already observed the open one
                    pending[0] = 0L;
                    tick[0] = 7L;
                }
                return fence;
            });
        } finally {
            drop.join();
        }
        Assertions.assertEquals(4, fenceReads.get(),
                "one fence pair per attempt: the window-crossing read is discarded and the"
                        + " stable post-drop read is published");
        Assertions.assertEquals(List.of("1:6|6", "2:6|6"), fenceLog.subList(0, 2),
                "attempt one saw the SAME tuple on both reads - only the open window"
                        + " (pending != 0) can reject its mixed view");
        Assertions.assertEquals(2000, snapshot.size(),
                "the state after the drop must be published: " + snapshot.keySet().size());
        Assertions.assertFalse(snapshot.containsKey(2000L),
                "the DROPped baseline must not survive in the published snapshot");
        Assertions.assertTrue(snapshot.containsKey(2001L),
                "the successor must not be lost to the OFFSET continuation");
    }

    /**
     * The default 3-arg fence is CLOSED (no open window) - scripted / legacy callers keep
     * working - and an open window is never quiet, whatever the tick tuple says.
     */
    @Test
    public void testSnapshotFenceQuietContract() {
        Assertions.assertTrue(new BaselineManager.SnapshotFence(1L, 1L, 1L).quiet(),
                "the legacy 3-arg fence carries no window");
        Assertions.assertTrue(new BaselineManager.SnapshotFence(1L, 1L, 1L, 0L).quiet());
        Assertions.assertFalse(new BaselineManager.SnapshotFence(1L, 1L, 1L, 4L).quiet(),
                "an open window is not quiet even with an unchanged tick tuple");
        Assertions.assertTrue(new BaselineManager.SnapshotFence(6L, 1L, 6L, 6L)
                        .matches(new BaselineManager.SnapshotFence(6L, 1L, 6L, 0L)),
                "matches() stays the tick tuple - the window is a SEPARATE gate");
    }

    // ==================== comments 4/6: the reservation must be READABLE ====================

    /**
     * An id reservation's SQL OK proves the row was ACCEPTED, not that a successor or a
     * retry can READ it: with the compact read still answering the OLD state (a lagging
     * publication, or a compact tombstone that has not been superseded yet) the next
     * CREATE - or a retry on another FE - reads a LOWER watermark and a resolved identity,
     * hands out a SECOND id for the same key, and two ENABLED rows can publish (comments 4
     * and 6). The create must CONFIRM the reservation through the exact reads a successor
     * uses and fail retryably BEFORE the baseline row write.
     */
    @Test
    public void testCreateFailsWhenTheReservationStaysUnreadable() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        SimulatedStore store = new SimulatedStore();
        try {
            BaselineManager.idAllocatorStoreForTest = store;
            long firstId = manager.createBaseline(baseline("rb-a", "rb-a-1"));
            Assertions.assertTrue(firstId > 0);

            store.holdReservationReadback = true; // the reservation write lags
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("rb-b", "rb-b-1")));
            Assertions.assertTrue(
                    failure.getMessage().contains("cannot confirm the id reservation"),
                    failure.getMessage());
            long reservedId = store.reservedHighWater;
            Assertions.assertTrue(reservedId > firstId,
                    "the id was allocated and reserved before the confirmation");
            Assertions.assertTrue(store.rowsOf(reservedId).isEmpty(),
                    "the unconfirmable create must NOT write its row: "
                            + store.rowsOf(reservedId));
            Assertions.assertEquals(1, store.rows.size(),
                    "only the first baseline's row exists: " + store.rows.keySet());
            Assertions.assertEquals(0, manager.pendingCreateCountForTest(),
                    "no pending record for a create that never wrote");

            // the successor's reads recover - but the recent plain reservation IS the durable
            // evidence of a possibly-committed write, so the retry DEFERS first (fail
            // closed) instead of allocating a second id next to it
            store.holdReservationReadback = false;
            IllegalStateException deferred = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.createBaseline(baseline("rb-b", "rb-b-1")));
            Assertions.assertTrue(
                    deferred.getMessage().contains("still awaiting publication"),
                    deferred.getMessage());

            // after the fence the abandoned identity is CONDEMNED (a durable tombstone) and
            // a FRESH id publishes - the reserved-but-unreadable id is never handed out again
            store.ageReservations(6 * 60 * 1000L);
            long retriedId = manager.createBaseline(baseline("rb-b", "rb-b-1"));
            Assertions.assertTrue(retriedId > reservedId,
                    "the reserved-but-unreadable id must not be reused: " + retriedId
                            + " vs " + reservedId);
            Assertions.assertEquals(1, store.rowsOf(retriedId).stream()
                            .filter(row -> row.getStatus() == BaselineStatus.ENABLED).count(),
                    "exactly one ENABLED row for the retried key: " + store.rowsOf(retriedId));
            Assertions.assertTrue(store.rowsOf(reservedId).isEmpty(),
                    "the abandoned reservation stays a gap, never a row");
            Assertions.assertEquals(BaselineStatus.ENABLED,
                    manager.getBaseline(retriedId).getStatus());
        } finally {
            BaselineManager.idAllocatorStoreForTest = null;
            manager.clearForTest();
        }
    }

    /**
     * Identity-keyed status store: the conditional INSERT mirrors the
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
