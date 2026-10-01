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
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    // ==================== SHOW uses the confirmed read (round-15) ====================

    /**
     * SHOW BASELINE PLANS used the ASYNCHRONOUS getAllBaselines(): right after startup / a
     * promotion (empty map, load not finished) it listed ZERO rows although durable
     * baselines existed, and a failed read never converged. The command now requires
     * ensureLoadedConfirmed(); the query-matching read stays nonblocking and empty.
     */
    @Test
    public void testConfirmedLoadIsRequiredForShow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        try {
            // startup / promotion state: the store is NOT loaded and the table gate is on
            manager.prepareLoadForTest();
            manager.setPersistToTableForTest(true);
            BaselineManager.snapshotReaderForTest = () -> {
                throw new RuntimeException("internal table not ready");
            };
            Assertions.assertThrows(IllegalStateException.class,
                    manager::ensureLoadedConfirmed,
                    "SHOW must surface a retryable failure instead of listing ZERO rows"
                            + " from an unreadable store");
            Assertions.assertEquals(0, manager.getAllBaselines().size(),
                    "the query-matching read stays nonblocking and simply empty");

            BaselineManager.snapshotReaderForTest =
                    () -> Map.of(7L, withId(baseline("d1", "p1"), 7L));
            manager.ensureLoadedConfirmed();
            Assertions.assertEquals(1, manager.getAllBaselines().size(),
                    "the confirmed read publishes the durable rows for SHOW");
        } finally {
            BaselineManager.snapshotReaderForTest = null;
            manager.clearForTest();
        }
    }
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
                throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
            }
            rows.computeIfPresent(status, (k, v) -> Math.max(0, v - 1));
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
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
     * The uncommitted counterpart: DELETE(old) failed AND did not commit. The rollback
     * may remove the freshly inserted row, but the OLD version must survive and the ALTER
     * must report failure.
     */
    @Test
    public void testUncommittedStatusDeleteKeepsTheOldRow() {
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        StatusProtocolSimulator store = new StatusProtocolSimulator();
        try {
            long id = manager.createBaseline(baseline("d-st2", "p-st2"));
            BaselineManager.statusProtocolStoreForTest = store;
            store.rows.put(BaselineStatus.ENABLED, 1);
            store.failOldDeleteWithoutCommit = true;

            Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus(),
                    "the failed ALTER must not flip the live object");
            Assertions.assertEquals(1, store.rows.getOrDefault(BaselineStatus.ENABLED, 0),
                    "the old version must survive: " + store.rows);
            Assertions.assertEquals(0, store.rows.getOrDefault(BaselineStatus.DISABLED, 0),
                    "the rollback removes the new row: " + store.rows);
        } finally {
            BaselineManager.statusProtocolStoreForTest = null;
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
}
