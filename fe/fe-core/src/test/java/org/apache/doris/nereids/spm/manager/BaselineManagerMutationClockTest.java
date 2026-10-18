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
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Round-54 #3 / #5: the mutation clock is the SHARED fence of every baseline-table
 * mutation, and its OPEN-WINDOW marker must be exactly as durable as the statement it
 * fences.
 *
 * - #5: the two high-water-mark history sweeps must never remove a row whose window is
 *   still OPEN. Another master appending at the same watermark with a newer tick used to
 *   prune it, after which every fence read reported a QUIET clock while the first
 *   master's DELETE was still in flight - a paginated refresh could then step over the
 *   successor of a row group that DELETE removed between two pages. Only the window's own
 *   completion may close it.
 * - #3: the marker must be READABLE before the row statement is dispatched (otherwise a
 *   successor's fence sees a quiet clock while the statement runs), and the MATCHES branch
 *   of a status ALTER must consult that shared marker instead of only its local fences -
 *   a matching durable read is not final while another master's flip is in flight.
 *
 * The simulator below mirrors the production slot statements (SELECT_SLOT_STATE_SQL /
 * SELECT_CLOCK_SQL / INSERT_HWM_SQL / PRUNE_HWM_SQL / PRUNE_HWM_SAME_WATERMARK_SQL /
 * PRUNE_HWM_OPEN_SQL / SELECT_PENDING_TICK_SQL); the test asserts the production
 * predicates against it, so the mirror cannot silently drift.
 */
public class BaselineManagerMutationClockTest {

    /** How long a slot row still counts as an OPEN window (see MUTATION_WINDOW_MAX_AGE_SECONDS). */
    private static final long OPEN_WINDOW_MAX_AGE_MILLIS = 120_000L;

    private final BaselineTestDoubles.TestBaselineStore doubles =
            new BaselineTestDoubles.TestBaselineStore();
    private final BaselineTestDoubles.TestClusterGateway gateway =
            new BaselineTestDoubles.TestClusterGateway();
    private final java.util.concurrent.atomic.AtomicLong windowClock =
            new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis());
    private BaselineManager manager;

    private BaselineManager newManager() {
        return new BaselineManager(doubles, gateway, windowClock::get);
    }

    private static BaselinePlan baseline(String digest, String planSql) {
        BaselinePlan plan = new BaselinePlan();
        plan.setBindSql("select k from t1");
        plan.setBindSqlDigest(digest);
        plan.setBindSqlHash(7);
        plan.setPlanSql(planSql);
        plan.setSource(BaselineSource.USER);
        plan.setStatus(BaselineStatus.ENABLED);
        plan.setSchemaFingerprint("fp");
        return plan;
    }

    /** The simulated slot store: one row per append ({lastId, tick, pending, writtenAt}). */
    private static final class MutationClockSimulator
            implements BaselineTestDoubles.MutationClockStore {
        private final List<long[]> rows = new ArrayList<>();
        /** Visibility probes that still report the open window as NOT readable. */
        private int invisibleProbes;
        /** Runs INSIDE every visibility probe (e.g. to age the window mid-probe). */
        private Runnable visibilityProbeHook;

        void seedOpenWindow(long lastId, long tick) {
            rows.add(new long[] {lastId, tick, tick, System.currentTimeMillis()});
        }

        void seedClosedRow(long lastId, long tick) {
            rows.add(new long[] {lastId, tick, 0L, System.currentTimeMillis()});
        }

        void makeWindowsInvisible(int probes) {
            this.invisibleProbes = probes;
        }

        boolean hasOpenWindow(long tick) {
            return rows.stream().anyMatch(row -> row[1] == tick && row[2] == tick);
        }

        boolean hasRow(long lastId, long tick) {
            return rows.stream().anyMatch(row -> row[0] == lastId && row[1] == tick);
        }

        @Override
        public long[] slotState() {
            long maxId = 0;
            long maxTick = 0;
            for (long[] row : rows) {
                maxId = Math.max(maxId, row[0]);
                maxTick = Math.max(maxTick, row[1]);
            }
            return new long[] {maxId, maxTick};
        }

        @Override
        public long[] clock() {
            long maxTick = 0;
            long count = 0;
            long sum = 0;
            long maxPending = 0;
            long oldestCounted = System.currentTimeMillis() - OPEN_WINDOW_MAX_AGE_MILLIS;
            for (long[] row : rows) {
                maxTick = Math.max(maxTick, row[1]);
                count++;
                sum += row[1];
                if (row[3] >= oldestCounted && row[2] > maxPending) {
                    maxPending = row[2];
                }
            }
            return new long[] {maxTick, count, sum, maxPending};
        }

        @Override
        public void append(long lastId, long tick, long pending) {
            rows.add(new long[] {lastId, tick, pending, System.currentTimeMillis()});
        }

        @Override
        public void prune(long lastId, long tick) {
            // PRUNE_HWM_SQL ('last_id' < lastId OR same watermark with an older tick) with
            // its OPEN-WINDOW guard: a pending row is never a victim
            rows.removeIf(row -> (row[0] < lastId || (row[0] == lastId && row[1] < tick))
                    && row[2] == 0);
        }

        @Override
        public void closeWindow(long windowTick) {
            // PRUNE_HWM_OPEN_SQL: the window's OWN row, named by its tick
            rows.removeIf(row -> row[1] == windowTick && row[2] == windowTick);
        }

        @Override
        public long countVisiblePending(long tick) {
            if (visibilityProbeHook != null) {
                visibilityProbeHook.run();
            }
            if (invisibleProbes > 0) {
                invisibleProbes--;
                return 0;
            }
            return rows.stream().filter(row -> row[1] == tick && row[2] == tick).count();
        }
    }

    /** A status-protocol store holding one row per status, recording every write. */
    private static final class StatusStoreSimulator
            implements BaselineTestDoubles.StatusProtocolStore {
        private final Map<BaselineStatus, Integer> rows =
                new EnumMap<>(BaselineStatus.class);
        private final List<String> operations = new ArrayList<>();

        /** Stored VERSION of each status ROW, in epoch MILLIS (see the versioned delete). */
        private final Map<BaselineStatus, List<Long>> versions =
                new EnumMap<>(BaselineStatus.class);

        /** When set, the conditional INSERT reports this error (an ambiguous outcome). */
        private RuntimeException insertFailure;

        /** Runs right AFTER a written conditional INSERT (e.g. to model a later flip). */
        private Runnable insertHook;

        /** Seeds one durable status row with its stored version. */
        void seed(BaselineStatus status, long versionMillis) {
            rows.merge(status, 1, Integer::sum);
            versions.computeIfAbsent(status, key -> new ArrayList<>()).add(versionMillis);
        }

        /** The stored versions of one status, oldest first. */
        List<Long> storedVersions(BaselineStatus status) {
            return versions.getOrDefault(status, List.of());
        }

        @Override
        public void insert(BaselinePlan plan) {
            operations.add("insert:" + plan.getStatus());
            rows.merge(plan.getStatus(), 1, Integer::sum);
            versions.computeIfAbsent(plan.getStatus(), key -> new ArrayList<>())
                    .add(plan.getUpdateTime() > 0 ? plan.getUpdateTime() : 1L);
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            if (insertFailure != null) {
                throw insertFailure;
            }
            boolean written = BaselineTestDoubles.StatusProtocolStore.super
                    .insertIfPreviousPresent(plan, previousStatus);
            if (written && insertHook != null) {
                insertHook.run();
            }
            return written;
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            operations.add("delete:" + status);
            List<Long> stored = versions.computeIfAbsent(status, key -> new ArrayList<>());
            if (!stored.isEmpty()) {
                stored.remove(stored.size() - 1);
            }
            rows.computeIfPresent(status, (key, count) -> count > 1 ? count - 1 : null);
        }

        /**
         * The VERSION-bound cleanup DELETE (see deleteByIdAndStatusSql): it removes exactly the
         * row whose stored version the flip completes - a row a LATER flip BACK to that status
         * wrote carries another (newer) version and must survive.
         */
        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status, long oldUpdateTimeMs) {
            operations.add("delete:" + status + "@" + oldUpdateTimeMs);
            if (oldUpdateTimeMs <= 0) {
                deleteByIdAndStatus(id, status);
                return;
            }
            List<Long> stored = versions.get(status);
            if (stored == null || !stored.remove(oldUpdateTimeMs)) {
                return; // the predicate matches no row: nothing is deleted
            }
            rows.computeIfPresent(status, (key, count) -> count > 1 ? count - 1 : null);
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            return rows.getOrDefault(status, 0);
        }

        @Override
        public long newestStoredUpdateSecond() {
            return versions.values().stream().flatMap(List::stream)
                    .mapToLong(Long::longValue).max().orElse(0L) / 1000L;
        }
    }

    /**
     * Round-54 #5: another master's append must not prune THIS master's still-open window,
     * and the paginated refresh must not publish a mixed snapshot while that window is
     * readable. The old same-watermark sweep removed every row with a lower tick - the open
     * marker included - so both fence reads reported a quiet clock although the first
     * master's DELETE was still in flight.
     */
    @Test
    public void testOpenMutationWindowSurvivesAForeignMastersPrune() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        try {
            doubles.mutationClockStore = clock;
            // master A opened window 1000 at the slot's watermark 100 (its row statement is
            // still in flight); a superseded CLOSED history row is there as well
            long openTick = 1_000L;
            clock.seedOpenWindow(100L, openTick);
            clock.seedClosedRow(100L, 900L);

            // the production predicates keep an OPEN row: PRUNE_HWM_SQL and
            // PRUNE_HWM_SAME_WATERMARK_SQL both filter `pending` = 0
            String[] prunes = BaselineManagerTestAccess.pruneHwmSql();
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(prunes[i].contains("`pending` = 0"),
                        "the history sweep must never prune an OPEN window: " + prunes[i]);
                Assertions.assertFalse(prunes[i].contains(" OR "),
                        "the delete fallback rejects an OR predicate outright (the whole"
                                + " prune would silently fail): " + prunes[i]);
            }

            // master B (the new leader) appends its completion at the SAME watermark with a
            // newer tick and runs both history sweeps
            BaselineManagerTestAccess.writeHwmRecord(manager, 100L, 2_000L, 0L);

            Assertions.assertTrue(clock.hasOpenWindow(openTick),
                    "the still-open mutation marker must survive a foreign master's prune");
            Assertions.assertFalse(clock.hasRow(100L, 900L),
                    "a superseded CLOSED row is still pruned");
            Assertions.assertFalse(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the shared clock must keep reporting the open window");

            // the paginated refresh reads page one, the in-flight DELETE lands (the model:
            // the marker stays), and the fence must refuse to publish the mixed snapshot -
            // even though the tick tuple itself did not move between the two fence reads
            Assertions.assertThrows(IllegalStateException.class,
                    () -> BaselineManagerTestAccess.readStableSnapshot((pageStart, offset) -> List.of(),
                            BaselineManagerTestAccess.fenceReader(manager)),
                    "a snapshot overlapping an open mutation window must never be published");

            // only the window's OWN completion closes it - and then the read is stable again
            clock.closeWindow(openTick);
            Assertions.assertTrue(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the closed window no longer poisons the fence");
            Assertions.assertTrue(
                    BaselineManagerTestAccess.readStableSnapshot((pageStart, offset) -> List.of(),
                            BaselineManagerTestAccess.fenceReader(manager)).isEmpty(),
                    "a quiet clock lets the empty snapshot through");
        } finally {
            doubles.mutationClockStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-54 #3 (writer side): a mutation whose window could not be made READABLE must
     * fail BEFORE its row statement runs - and must not leave the window open, fence the id
     * or touch the cache. An unreadable marker means every other FE reads a quiet clock
     * while this statement is in flight, which is exactly what let a successor accept a
     * matching read that the statement later reversed.
     */
    @Test
    public void testUnsharedMutationWindowBlocksTheRowStatement() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-unshared", "p-unshared"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-unshared", "p-unshared");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);

            // the open-window marker never becomes readable
            clock.makeWindowsInvisible(100);
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("not READABLE"),
                    "the failure must name the unshared window: " + failure.getMessage());
            Assertions.assertTrue(store.operations.isEmpty(),
                    "no row statement may run while its window is not shared: "
                            + store.operations);
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus(),
                    "the cache must not be published for a statement that never ran");
            Assertions.assertFalse(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                    "a statement that never ran must not fence the id");
            Assertions.assertEquals(0, clock.rows.stream().filter(
                            row -> row[2] != 0).count(),
                    "the unusable window must be closed again instead of poisoning the"
                            + " fence for its whole age bound");

            // once the marker IS readable the very same flip goes through
            clock.makeWindowsInvisible(0);
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "a shared window lets the flip run");
            Assertions.assertTrue(store.operations.contains("insert:DISABLED"),
                    "the row statement ran once the window was shared: " + store.operations);
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-54 #3 (reader side): the MATCHES branch of a status ALTER must not accept a
     * matching durable read while ANOTHER master's row statement is in flight. Master A's
     * DISABLE can be dispatched after a handoff and commit with a newer update_time than
     * the ENABLED row the new master just read; the pending marker A wrote before its
     * statement is the only shared evidence, so the ALTER must fail retryably instead of
     * reporting a no-op success it cannot keep.
     */
    @Test
    public void testMatchingStatusReadIsRefusedWhileAForeignWindowIsOpen() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-foreign", "p-foreign"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            // the durable row still carries the requested status: without the shared clock
            // this ALTER reports a no-op success
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-foreign", "p-foreign");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);
            long foreignWindow = 5_000L;
            clock.seedOpenWindow(100L, foreignWindow);

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED));
            Assertions.assertTrue(failure.getMessage().contains("still in flight"),
                    "the refusal must name the in-flight mutation: " + failure.getMessage());
            Assertions.assertTrue(store.operations.isEmpty(),
                    "the refused no-op must not write anything: " + store.operations);
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus());
            // the foreign statement completed: the matching read IS final now
            clock.closeWindow(foreignWindow);
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.ENABLED),
                    "a quiet clock lets the already-durable status be confirmed");
            Assertions.assertEquals(List.of("insert:ENABLED"), store.operations,
                    "a successful no-op still records its INTENT durably - the same status with a"
                            + " strictly later stored second - so a stalled earlier flip cannot win"
                            + " the durable pick and reverse it: " + store.operations);
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-55 #7: a window whose pre-write budget (the marker INSERT, its prunes and the
     * visibility probes) consumed its whole successor-facing bound must NOT dispatch its row
     * statement. A successor reading the clock right after would see QUIET while that
     * statement is still in flight and accept a matching durable row as final - the
     * statement then commits over the reported state. The mutation fails RETRYABLY with the
     * window CLOSED, exactly like an unreadable window.
     */
    @Test
    public void testWindowAgedPastTheBoundIsRefusedBeforeDispatch() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-aged", "p-aged"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-aged", "p-aged");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);

            // the first visibility probe answers only after the window clock jumped past
            // the whole bound: the guard must refuse the dispatch
            windowClock.set(1_700_000_000_000L);
            java.util.concurrent.atomic.AtomicBoolean jumped =
                    new java.util.concurrent.atomic.AtomicBoolean();
            clock.visibilityProbeHook = () -> {
                if (jumped.compareAndSet(false, true)) {
                    windowClock.addAndGet(200_000L);
                }
            };

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("window bound"),
                    "the refusal must name the expired window: " + failure.getMessage());
            Assertions.assertTrue(store.operations.isEmpty(),
                    "the statement must not be dispatched once its window has aged out: "
                            + store.operations);
            Assertions.assertEquals(BaselineStatus.ENABLED, manager.getBaseline(id).getStatus(),
                    "the cache must not be published for a statement that never ran");
            Assertions.assertFalse(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                    "a statement that never ran must not fence the id");
            Assertions.assertEquals(0, clock.rows.stream().filter(row -> row[2] != 0).count(),
                    "the expired window must be CLOSED again, so no successor keeps retrying"
                            + " until the bound elapses");
            Assertions.assertTrue(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the successor-facing clock is quiet again after the refusal");

            // a window that ages WITHIN the bound still dispatches its statement
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "a window inside its bound lets the flip run");
            Assertions.assertTrue(store.operations.contains("insert:DISABLED"),
                    "the row statement ran: " + store.operations);
            Assertions.assertEquals(BaselineStatus.DISABLED, manager.getBaseline(id).getStatus());
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            windowClock.set(System.currentTimeMillis());
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-56 #1: a mutation window's tick carries this FE's identity. Two FEs can open a
     * window within the SAME millisecond, and a clock-only tick made their markers
     * indistinguishable - a visibility probe could confirm the OTHER FE's already-readable
     * marker as its own (reporting a shared window while its own marker was unreadable), and
     * either FE's completion DELETE removed BOTH rows, so SELECT_CLOCK_SQL reported a quiet
     * clock while the other FE's DISABLE / DELETE was still in flight.
     */
    @Test
    public void testSameInstantWindowsOfDifferentFesStayDistinct() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-same-instant", "p-same-instant"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-same-instant", "p-same-instant");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);

            // FE A's still-open window, opened in millisecond MS (its own tick is a
            // different one: the collision is about the MILLISECOND, not the encoding)
            long ms = 1_700_000_000_000L;
            long tickA = ms;
            clock.seedOpenWindow(100L, tickA);
            Assertions.assertFalse(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "precondition: A's window poisons the fence");

            // FE B runs a complete mutation in the SAME millisecond under its own salt
            windowClock.set(ms);
            BaselineManagerTestAccess.setMutationTickSalt(0x2222L);
            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "B's own window is readable and independent of A's");
            Assertions.assertTrue(clock.hasOpenWindow(tickA),
                    "B's completion DELETE may only remove its OWN marker: with a clock-only"
                            + " tick both markers share the same value and either completion"
                            + " removes BOTH, leaving the clock quiet while the other FE's"
                            + " write is still in flight");

            // and A's readable marker must not make B's window look SHARED: with B's own
            // marker unreadable the mutation has to refuse (a clock-only tick matched A's
            // row here and reported the window shared although it was not)
            clock.makeWindowsInvisible(100);
            BaselineManagerTestAccess.setMutationTickSalt(0x3333L);
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED));
            Assertions.assertTrue(failure.getMessage().contains("not READABLE"),
                    "the refusal must name the unshared window: " + failure.getMessage());
            Assertions.assertTrue(clock.hasOpenWindow(tickA),
                    "the refusal must leave A's still-open window untouched");
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            windowClock.set(System.currentTimeMillis());
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-56 #2: the DROP tombstones are KEYED by the baseline id (a dedicated negative
     * id space), so the scoped read seeks exactly the requested identities instead of
     * re-scanning the whole append-only history once per id chunk - with a large CREATE /
     * DROP history that scan exhausted the fixed query timeout and GLOBAL SHOW / management
     * failed with it, because both the refresh and the confirmed reads go through the read.
     */
    @Test
    public void testDropTombstonesAreKeyedByBaselineId() {
        // the id space is disjoint from the constant id = 1 rows and injective in the id
        Assertions.assertTrue(BaselineManagerTestAccess.tombstoneSeqId(1L) < 0L,
                "tombstones live in the negative id space");
        Assertions.assertNotEquals(1L, BaselineManagerTestAccess.tombstoneSeqId(1L),
                "never the constant history id");
        Assertions.assertEquals(BaselineManagerTestAccess.tombstoneSeqId(7L) + 4L,
                BaselineManagerTestAccess.tombstoneSeqId(3L), "the mapping is injective");

        String insert = BaselineManagerTestAccess.droppedMarkerInsertSql();
        Assertions.assertTrue(insert.contains("(${tombstoneId}"),
                "the tombstone append must bind the KEYED id: " + insert);
        Assertions.assertFalse(insert.contains("VALUES (1,"),
                "appending under the constant id = 1 made the read unscannable: " + insert);
        String select = BaselineManagerTestAccess.droppedMarkerReadSql();
        Assertions.assertTrue(select.contains("`id` IN (${tombstoneIds})"),
                "the scoped read must SEEK the keyed ids: " + select);
        Assertions.assertTrue(select.contains("`last_id` IN (${ids})")
                        && select.contains("`dropped` = 1"),
                "and still pin the requested identities / the tombstone flag: " + select);
    }

    /**
     * Round-56 #4: the metadata sync before a forwarded CREATE's expectation must NOT be
     * swallowed. The caller computes its schema fingerprint immediately afterwards, so a
     * failed sync captures the FOLLOWER's pre-ALTER schema; the later refresh sync cannot
     * update that expectation any more - it reads the master's valid post-ALTER row and
     * rejects it against the stale fingerprint forever, invalidating the cache and
     * reporting the successful CREATE as failed.
     */
    @Test
    public void testForwardedDdlCallbackFailsWhenThePreSyncFails() {
        manager = newManager();
        gateway.forwardedDdlSync = () -> {
            throw new RuntimeException("journal sync unavailable");
        };
        try {
            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.syncWithMasterForDdlCallback(null));
            Assertions.assertTrue(failure.getMessage().contains("journal sync unavailable"),
                    "the sync failure must PROPAGATE so no expectation is captured from"
                            + " possibly stale metadata: " + failure.getMessage());

            // a successful sync stays silent
            gateway.forwardedDdlSync = () -> { };
            Assertions.assertDoesNotThrow(() -> manager.syncWithMasterForDdlCallback(null));
        } finally {
            gateway.forwardedDdlSync = null;
        }
    }

    /**
     * Round-57 C1: a committed-but-unreadable status flip must keep its SHARED mutation
     * window open. The conditional INSERT proves the DISABLED row was written while
     * confirmInsertVisible exhausts its probes, and the normal finally then closed the
     * cluster clock anyway: a promoted successor read the old ENABLED row plus a QUIET
     * clock, accepted ALTER ... ENABLE as a no-op, and the DISABLED row publishing later
     * reversed that successful ALTER. The window stays open until a snapshot read proves
     * the committed row (or the window ages past its bound).
     */
    @Test
    public void testUnreadableCommittedFlipKeepsTheSharedWindowOpen() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-held-window", "p-held-window"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-held-window", "p-held-window");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);
            // the committed DISABLED row never becomes visible to the confirmation probes
            doubles.visibilityProbe = (probeId, status) -> false;

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the PROVEN (affected-row) flip is reported successful");
            Assertions.assertEquals(BaselineStatus.DISABLED,
                    manager.getBaseline(id).getStatus(), "the committed flip is published");
            Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "the shared window must stay OPEN while the committed row is unreadable");
            Assertions.assertFalse(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the shared clock must keep reporting the mutation in flight, so a"
                            + " successor cannot conclude a MATCHING old-status read");

            // once a snapshot shows the committed status, the held window is closed and the
            // shared clock is quiet again
            BaselinePlan disabled = baseline("d-held-window", "p-held-window");
            disabled.setId(id);
            disabled.setStatus(BaselineStatus.DISABLED);
            Map<Long, BaselinePlan> persisted = new java.util.LinkedHashMap<>();
            persisted.put(id, disabled);
            manager.applyRefreshedBaselines(persisted);
            Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "a snapshot that proves the committed row closes the held window");
            Assertions.assertTrue(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the closed window no longer poisons the shared clock");
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            doubles.visibilityProbe = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * An id-allocator store whose tombstone appends only become READABLE after
     * publishMarkers(): the durable append succeeded, its publication lags - the
     * committed-but-invisible state the drop confirmation must not mistake for a settled
     * outcome.
     */
    private static final class LaggingTombstoneStore
            implements BaselineTestDoubles.IdAllocatorStore {
        private final Map<Long, BaselinePlan> rows = new java.util.LinkedHashMap<>();
        private final List<String> pendingMarkers = new ArrayList<>();
        private final List<String> publishedMarkers = new ArrayList<>();

        void publishMarkers() {
            publishedMarkers.addAll(pendingMarkers);
            pendingMarkers.clear();
        }

        @Override
        public long watermark() {
            return rows.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            BaselinePlan row = rows.get(plan.getId());
            if (row != null && row.getBindSqlDigest().equals(plan.getBindSqlDigest())) {
                rows.remove(plan.getId());
            }
        }

        @Override
        public void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            pendingMarkers.add(id + "|" + bindSqlDigest + "|" + planSqlHash);
        }

        @Override
        public List<String> droppedMarkers() {
            return publishedMarkers;
        }
    }

    /**
     * Round-57 C2: a DROP whose DELETE was confirmed but whose tombstones are not readable
     * must RETAIN the obligation. The retry reaches the absent-row branch (the row is
     * already deleted) and used to report "nothing to drop" without re-appending the
     * markers - a demoted master's delayed status INSERT could then revive the baseline.
     * The retry must settle (re-append + confirm) the owed markers before it may succeed.
     */
    @Test
    public void testDropRetrySettlesTheRetainedTombstoneObligation() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        LaggingTombstoneStore store = new LaggingTombstoneStore();
        try {
            doubles.idAllocatorStore = store;
            long id = manager.createBaseline(baseline("d-owed", "p-owed"));

            // first attempt: the DELETE is confirmed, the tombstone appends are not readable
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.dropBaseline(id));
            Assertions.assertTrue(failure.getMessage().contains("retry the DROP"),
                    "the first attempt must surface the unreadable tombstones: "
                            + failure.getMessage());
            Assertions.assertFalse(store.rows.containsKey(id), "the row is already deleted");
            Assertions.assertTrue(BaselineManagerTestAccess.owesDropTombstoneConfirmation(id),
                    "the failed confirmation must be retained as an obligation");

            // the retry finds the row ABSENT: it must settle the obligation, not report
            // "nothing to drop" while the markers are still unreadable
            IllegalStateException retry = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.dropBaseline(id));
            Assertions.assertTrue(retry.getMessage().contains("retained drop tombstones"),
                    "the absent-row retry must re-confirm the owed markers: "
                            + retry.getMessage());

            // once the markers publish, the retry converges and releases the obligation
            store.publishMarkers();
            Assertions.assertFalse(manager.dropBaseline(id),
                    "the absent-row retry succeeds once the tombstones are readable");
            Assertions.assertFalse(BaselineManagerTestAccess.owesDropTombstoneConfirmation(id),
                    "the settled obligation is released");
        } finally {
            doubles.idAllocatorStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-57 C7: a forwarded GLOBAL DDL's fence must be CONFIRMED (never expiring).
     * afterForwardToMaster runs only after the MASTER reported the DDL successful, so the
     * outcome IS durable - only its publication on this FE lags. The expiring pending
     * fence was dropped after PENDING_MUTATION_FENCE_MILLIS and the next refresh
     * republished the pre-DDL row: a DISABLE / DROP the master completed was replayed.
     */
    @Test
    public void testForwardedDdlFenceIsConfirmedAndCannotExpire() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        try {
            long id = manager.createBaseline(baseline("d-fwd", "p-fwd"));
            // the follower's confirmed refresh: the local snapshot never shows the DELETED
            // outcome (a long publication lag)
            doubles.snapshotReader = java.util.LinkedHashMap::new;
            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.refreshAfterForwardedDdl(null,
                            BaselineManager.ForwardedDdlExpectation.status(id,
                                    BaselineStatus.DISABLED)));
            Assertions.assertTrue(failure.getMessage().contains("has not reached its expected"),
                    "the unconfirmable outcome must fail closed: " + failure.getMessage());
            Assertions.assertTrue(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                    "the follower must retain the master's successful outcome as a fence");
            Assertions.assertTrue(BaselineManagerTestAccess.isPendingMutationFenceConfirmed(manager, id),
                    "the fence of a DDL the MASTER completed must NOT expire after five"
                            + " minutes: the next refresh would republish the pre-DDL row");
        } finally {
            doubles.snapshotReader = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-58 (comment 2): an AMBIGUOUS status INSERT error must keep the SHARED window
     * open. The conditional INSERT can report an error AFTER its commit (a statement
     * timeout) while observedInsertRowIsOurs cannot read the row yet; that ordinary
     * RuntimeException used to bypass the unresolved-write handler of
     * persistTransitionInsert, whose finally then closed the cluster clock - leaving only
     * this FE's local fence. After a handoff a successor read the old ENABLED row plus a
     * QUIET clock and accepted ALTER ... ENABLE as a no-op, and the DISABLED row publishing
     * later reversed that successful ALTER.
     */
    @Test
    public void testAmbiguousStatusInsertErrorKeepsTheSharedWindowOpen() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-amb-insert", "p-amb-insert"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-amb-insert", "p-amb-insert");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);
            // the conditional INSERT reports a TIMEOUT; no read can prove whether it committed
            store.insertFailure = new RuntimeException("statement timeout");

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("may still have COMMITTED"),
                    "the failure must name the ambiguous outcome: " + failure.getMessage());
            Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "the shared window must stay OPEN while the row may still publish");
            Assertions.assertFalse(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the shared clock must keep reporting the mutation in flight, so a"
                            + " successor cannot accept a MATCHING old-status read as final");
            Assertions.assertTrue(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                    "the local fence complements the shared window");

            // once a snapshot shows the requested row, the held window closes and the shared
            // clock is quiet again
            BaselinePlan disabled = baseline("d-amb-insert", "p-amb-insert");
            disabled.setId(id);
            disabled.setStatus(BaselineStatus.DISABLED);
            Map<Long, BaselinePlan> persisted = new java.util.LinkedHashMap<>();
            persisted.put(id, disabled);
            manager.applyRefreshedBaselines(persisted);
            Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "a snapshot proving the requested row closes the held window");
            Assertions.assertTrue(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the closed window no longer poisons the shared clock");
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-58 (comment 3): an AMBIGUOUS identity DELETE (the DROP half) must keep a shared
     * window open too. If the DELETE times out after dispatch while probeDurableRow still
     * SEES the row, the ordinary failure used to close the cluster window and dropBaseline
     * recorded only this FE's LOCAL absence fence and no tombstone: a successor read the old
     * ENABLED row, reported ALTER ... ENABLE as a successful no-op, and lost that row when
     * the delayed DELETE published. The cluster-visible window closes only once a read
     * proves the row gone (or it ages past its bound), and the matching ALTER is refused
     * while it is open.
     */
    @Test
    public void testAmbiguousDropDeleteKeepsTheSharedAbsenceWindowOpen() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        AmbiguousDeleteStore store = new AmbiguousDeleteStore();
        try {
            doubles.mutationClockStore = clock;
            doubles.idAllocatorStore = store;
            long id = manager.createBaseline(baseline("d-amb-drop", "p-amb-drop"));

            RuntimeException failure = Assertions.assertThrows(RuntimeException.class,
                    () -> manager.dropBaseline(id));
            Assertions.assertTrue(failure.getMessage().contains("SPM persist (delete) failed"),
                    "the ambiguous DELETE fails retryably: " + failure.getMessage());
            Assertions.assertTrue(store.rows.containsKey(id),
                    "the probe still SEES the row: its absence is unproven");
            Assertions.assertTrue(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "the shared window must stay OPEN until the DELETE outcome is settled");
            Assertions.assertFalse(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "a successor must not conclude on the old ENABLED row while the delayed"
                            + " DELETE may still erase it");
            Assertions.assertTrue(BaselineManagerTestAccess.hasPendingMutationFence(manager, id),
                    "the DROP's local absence fence complements the shared window");

            // the PROMOTED successor asks to ENABLE the row it reads: the matching no-op must
            // be refused while the DROP's outcome is open
            IllegalStateException refusal = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.ENABLED));
            Assertions.assertTrue(refusal.getMessage().contains("still unresolved")
                            || refusal.getMessage().contains("still in flight"),
                    "the matching ALTER must fail retryably: " + refusal.getMessage());

            // once a snapshot shows the row gone, the absence proves the DELETE landed
            manager.applyRefreshedBaselines(new java.util.LinkedHashMap<>());
            Assertions.assertFalse(BaselineManagerTestAccess.holdsUnresolvedMutationWindow(),
                    "the proven absence closes the held window");
            Assertions.assertTrue(BaselineManagerTestAccess.snapshotFence(manager).quiet(),
                    "the closed window no longer poisons the shared clock");
        } finally {
            doubles.mutationClockStore = null;
            doubles.idAllocatorStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-58 (comment 4a): a caller that pauses after its window opened must not dispatch
     * the row statement once the window aged past MUTATION_WINDOW_MAX_AGE_SECONDS - every
     * successor's fence read ignores the marker from then on, so the delayed cleanup DELETE
     * would land on a state written after the window closed. The ALTER stays successful (its
     * INSERT half is confirmed), but the cleanup is REFUSED before dispatch.
     */
    @Test
    public void testAgedOutWindowRefusesTheDelayedStatusCleanup() throws Exception {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-aged-cleanup", "p-aged-cleanup"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-aged-cleanup", "p-aged-cleanup");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);

            windowClock.set(1_700_000_000_000L);
            // the pause lands between the window's open and its dispatch: the THIRD leader
            // probe is the dispatch-time re-check of the cleanup DELETE
            java.util.concurrent.atomic.AtomicInteger probes =
                    new java.util.concurrent.atomic.AtomicInteger();
            gateway.leaderProbe = () -> {
                if (probes.incrementAndGet() == 3) {
                    windowClock.addAndGet(200_000L);
                }
                return true;
            };

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the confirmed INSERT half keeps the ALTER successful");
            Assertions.assertTrue(store.operations.contains("insert:DISABLED"),
                    "the INSERT half ran: " + store.operations);
            Assertions.assertEquals(0, store.operations.stream()
                            .filter(op -> op.startsWith("delete:")).count(),
                    "the delayed cleanup must NOT be dispatched into an aged window: "
                            + store.operations);
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            windowClock.set(System.currentTimeMillis());
            gateway.leaderProbe = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-58 (comment 4b): the SAME pause with a leadership handoff - the demoted FE must
     * refuse the cleanup at dispatch instead of deleting a row the new master wrote after
     * the handoff.
     */
    @Test
    public void testDemotionBeforeDispatchRefusesTheDelayedStatusCleanup() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-demoted-cleanup", "p-demoted-cleanup"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-demoted-cleanup", "p-demoted-cleanup");
            durableRow.setId(id);
            doubles.seedDurableRow(durableRow);

            java.util.concurrent.atomic.AtomicInteger probes =
                    new java.util.concurrent.atomic.AtomicInteger();
            gateway.leaderProbe = () -> probes.incrementAndGet() != 3;

            IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class,
                    () -> manager.updateStatus(id, BaselineStatus.DISABLED));
            Assertions.assertTrue(failure.getMessage().contains("no longer the master"),
                    "the dispatch-time re-check must report the handoff: "
                            + failure.getMessage());
            Assertions.assertTrue(store.operations.contains("insert:DISABLED"),
                    "the INSERT half ran: " + store.operations);
            Assertions.assertEquals(0, store.operations.stream()
                            .filter(op -> op.startsWith("delete:")).count(),
                    "a demoted FE must not dispatch the cleanup: " + store.operations);
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            gateway.leaderProbe = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * Round-58 (comment 4c): the cleanup DELETE is bound to the OLD row's stored VERSION. A
     * demoted master's delayed DELETE used to match (id, old status, identity) alone and
     * erased the row a LATER flip BACK to that status had written - both ALTERs reported
     * success and the baseline disappeared. The version bind leaves the newer row alone.
     */
    @Test
    public void testStatusCleanupDeleteIsBoundToTheOldRowVersion() {
        manager = newManager();
        BaselineManagerTestAccess.clear(manager);
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-versioned", "p-versioned"));
            doubles.statusProtocolStore = store;
            doubles.mutationClockStore = clock;
            long oldVersion = 1_700_000_000_000L;
            store.seed(BaselineStatus.ENABLED, oldVersion);
            // the durable table of this scenario holds the row the create wrote
            BaselinePlan durableRow = baseline("d-versioned", "p-versioned");
            durableRow.setId(id);
            durableRow.setUpdateTime(oldVersion);
            doubles.seedDurableRow(durableRow);
            // the flip BACK to ENABLED lands between this flip's INSERT and its cleanup: a
            // LATER version of the row the cleanup would erase
            long laterVersion = oldVersion + 60_000L;
            store.insertHook = () -> {
                store.seed(BaselineStatus.ENABLED, laterVersion);
                BaselinePlan laterRow = baseline("d-versioned", "p-versioned");
                laterRow.setId(id);
                laterRow.setUpdateTime(laterVersion);
                doubles.seedDurableRow(laterRow);
            };

            Assertions.assertTrue(manager.updateStatus(id, BaselineStatus.DISABLED),
                    "the flip's INSERT half is confirmed and the ALTER succeeds");
            Assertions.assertTrue(store.operations.contains("delete:ENABLED@" + oldVersion),
                    "the cleanup must be bound to the OLD row's version: " + store.operations);
            Assertions.assertEquals(1, store.countByIdAndStatus(id, BaselineStatus.ENABLED),
                    "the row a LATER flip wrote carries another version and must survive:"
                            + " deleting it made both ALTERs successful and the baseline"
                            + " disappear");
            Assertions.assertEquals(List.of(laterVersion),
                    store.storedVersions(BaselineStatus.ENABLED),
                    "exactly the LATER version survives");
        } finally {
            doubles.mutationClockStore = null;
            doubles.statusProtocolStore = null;
            BaselineManagerTestAccess.clear(manager);
        }
    }

    /**
     * An id-allocator store whose identity DELETE reports an AMBIGUOUS error (a statement
     * timeout after dispatch): the row may or may not still exist, and this simulator keeps it
     * READABLE - the state the DROP must NOT treat as a proven absence.
     */
    private static final class AmbiguousDeleteStore
            implements BaselineTestDoubles.IdAllocatorStore {
        private final Map<Long, BaselinePlan> rows = new java.util.LinkedHashMap<>();

        @Override
        public long watermark() {
            return rows.keySet().stream().mapToLong(Long::longValue).max().orElse(0L);
        }

        @Override
        public void insert(BaselinePlan plan) {
            rows.put(plan.getId(), plan);
        }

        @Override
        public List<BaselinePlan> readById(long id) {
            BaselinePlan row = rows.get(id);
            return row == null ? List.of() : List.of(row);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            throw new RuntimeException("KV_TXN_MAYBE_COMMITTED");
        }
    }
}
