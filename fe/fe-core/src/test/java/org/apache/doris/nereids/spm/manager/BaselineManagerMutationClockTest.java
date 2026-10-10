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
    private static final long OPEN_WINDOW_MAX_AGE_MILLIS = 60_000L;

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
            implements BaselineManager.MutationClockStoreForTest {
        private final List<long[]> rows = new ArrayList<>();
        /** Visibility probes that still report the open window as NOT readable. */
        private int invisibleProbes;

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
            if (invisibleProbes > 0) {
                invisibleProbes--;
                return 0;
            }
            return rows.stream().filter(row -> row[1] == tick && row[2] == tick).count();
        }
    }

    /** A status-protocol store holding one row per status, recording every write. */
    private static final class StatusStoreSimulator
            implements BaselineManager.StatusProtocolStoreForTest {
        private final Map<BaselineStatus, Integer> rows =
                new EnumMap<>(BaselineStatus.class);
        private final List<String> operations = new ArrayList<>();

        @Override
        public void insert(BaselinePlan plan) {
            operations.add("insert:" + plan.getStatus());
            rows.merge(plan.getStatus(), 1, Integer::sum);
        }

        @Override
        public void deleteByIdAndStatus(long id, BaselineStatus status) {
            operations.add("delete:" + status);
            rows.computeIfPresent(status, (key, count) -> count > 1 ? count - 1 : null);
        }

        @Override
        public int countByIdAndStatus(long id, BaselineStatus status) {
            return rows.getOrDefault(status, 0);
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
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        MutationClockSimulator clock = new MutationClockSimulator();
        try {
            BaselineManager.mutationClockStoreForTest = clock;
            // master A opened window 1000 at the slot's watermark 100 (its row statement is
            // still in flight); a superseded CLOSED history row is there as well
            long openTick = 1_000L;
            clock.seedOpenWindow(100L, openTick);
            clock.seedClosedRow(100L, 900L);

            // the production predicates keep an OPEN row: PRUNE_HWM_SQL and
            // PRUNE_HWM_SAME_WATERMARK_SQL both filter `pending` = 0
            String[] prunes = BaselineManager.pruneHwmSqlForTest();
            for (int i = 0; i < 2; i++) {
                Assertions.assertTrue(prunes[i].contains("`pending` = 0"),
                        "the history sweep must never prune an OPEN window: " + prunes[i]);
                Assertions.assertFalse(prunes[i].contains(" OR "),
                        "the delete fallback rejects an OR predicate outright (the whole"
                                + " prune would silently fail): " + prunes[i]);
            }

            // master B (the new leader) appends its completion at the SAME watermark with a
            // newer tick and runs both history sweeps
            BaselineManager.writeHwmRecordForTest(100L, 2_000L, 0L);

            Assertions.assertTrue(clock.hasOpenWindow(openTick),
                    "the still-open mutation marker must survive a foreign master's prune");
            Assertions.assertFalse(clock.hasRow(100L, 900L),
                    "a superseded CLOSED row is still pruned");
            Assertions.assertFalse(BaselineManager.snapshotFenceForTest().quiet(),
                    "the shared clock must keep reporting the open window");

            // the paginated refresh reads page one, the in-flight DELETE lands (the model:
            // the marker stays), and the fence must refuse to publish the mixed snapshot -
            // even though the tick tuple itself did not move between the two fence reads
            Assertions.assertThrows(IllegalStateException.class,
                    () -> BaselineManager.readStableSnapshot((pageStart, offset) -> List.of(),
                            BaselineManager::snapshotFenceForTest),
                    "a snapshot overlapping an open mutation window must never be published");

            // only the window's OWN completion closes it - and then the read is stable again
            clock.closeWindow(openTick);
            Assertions.assertTrue(BaselineManager.snapshotFenceForTest().quiet(),
                    "the closed window no longer poisons the fence");
            Assertions.assertTrue(
                    BaselineManager.readStableSnapshot((pageStart, offset) -> List.of(),
                            BaselineManager::snapshotFenceForTest).isEmpty(),
                    "a quiet clock lets the empty snapshot through");
        } finally {
            BaselineManager.mutationClockStoreForTest = null;
            manager.clearForTest();
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
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-unshared", "p-unshared"));
            BaselineManager.statusProtocolStoreForTest = store;
            BaselineManager.mutationClockStoreForTest = clock;
            store.rows.put(BaselineStatus.ENABLED, 1);

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
            Assertions.assertFalse(manager.hasPendingMutationFenceForTest(id),
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
            BaselineManager.mutationClockStoreForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
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
        BaselineManager manager = BaselineManager.getInstance();
        manager.clearForTest();
        MutationClockSimulator clock = new MutationClockSimulator();
        StatusStoreSimulator store = new StatusStoreSimulator();
        try {
            long id = manager.createBaseline(baseline("d-foreign", "p-foreign"));
            BaselineManager.statusProtocolStoreForTest = store;
            BaselineManager.mutationClockStoreForTest = clock;
            // the durable row still carries the requested status: without the shared clock
            // this ALTER reports a no-op success
            store.rows.put(BaselineStatus.ENABLED, 1);
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
            Assertions.assertTrue(store.operations.isEmpty(),
                    "confirming a matching read stays a no-op: " + store.operations);
        } finally {
            BaselineManager.mutationClockStoreForTest = null;
            BaselineManager.statusProtocolStoreForTest = null;
            manager.clearForTest();
        }
    }
}
