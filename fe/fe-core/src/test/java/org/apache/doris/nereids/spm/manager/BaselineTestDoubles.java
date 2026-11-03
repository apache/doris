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
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.qe.ConnectContext;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongFunction;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Test doubles for the BaselineManager durable layer. The manager's durable I/O is
 * injected through {@link BaselinePersistence} / {@link BaselineClusterGateway}, so a
 * unit test binds an isolated manager ({@code new BaselineManager(store, gateway,
 * clock)}) to a scripted store instead of installing process-global statics.
 *
 * Each hook is the counterpart of one historic simulator seam: the dispatch methods below
 * reproduce exactly the behavior the old seams produced, including their defaults (a store
 * whose scenario models no aspect falls back to the synchronous no-op answers).
 */
public final class BaselineTestDoubles {

    private BaselineTestDoubles() {
    }

    /**
     * Routes the status-protocol durable I/O (INSERT / DELETE by status / the
     * reconciliation count read) to a simulator, so a unit test can inject faults such as
     * "the old-row delete committed but reported KV_TXN_MAYBE_COMMITTED".
     */
    public interface StatusProtocolStore {
        void insert(BaselinePlan plan);

        void deleteByIdAndStatus(long id, BaselineStatus status);

        /**
         * The version-bound status-cleanup DELETE: the row is removed only when its stored
         * update_time is the version the flip completes. The default delegates to the
         * two-argument form, so simulators that model no versions keep their semantics.
         */
        default void deleteByIdAndStatus(long id, BaselineStatus status, long oldUpdateTimeMicros) {
            deleteByIdAndStatus(id, status);
        }

        int countByIdAndStatus(long id, BaselineStatus status);

        /**
         * The CONDITIONAL insert half of a status flip: the new-status row must NOT be
         * written once the previous-status row is gone. The default keeps the
         * unconditional simulators working.
         *
         * @return whether the new-status row was written
         */
        default boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            insert(plan);
            return true;
        }

        /**
         * The newest stored update_time of the WHOLE simulated table, in epoch MICROSECONDS
         * (0 = none): the updateStatus bump reads this.
         */
        default long newestStoredUpdateMicros() {
            return 0;
        }
    }

    /**
     * The create-time id allocator / collision protocol: the watermark read, the INSERT,
     * the by-id collision probe, the identity delete and its ambiguous-commit
     * reconciliation read.
     */
    public interface IdAllocatorStore {
        long watermark();

        default long seqWatermark() {
            return 0;
        }

        default void reserveId(long id) {
        }

        /** The identity-carrying reservation (see the manager's allocation protocol). */
        default void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMicros) {
            reserveId(id);
        }

        /** Records the DURABLE pending marker of an ambiguous create (unconfirmed = 1). */
        default void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
        }

        /** The latest identity record of one baseline in the simulated sequence table. */
        default BaselinePersistence.SeqReservation readLatestReservation(String bindSqlDigest,
                long planSqlHash) {
            return null;
        }

        /** Retires the durable UNCONFIRMED marker of a RESOLVED ambiguous write. */
        default void retirePendingSeqState(String bindSqlDigest, long planSqlHash,
                long markerId) {
        }

        /** Records a DROP TOMBSTONE: the identity of a baseline the DROP removed. */
        default void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
        }

        /** The recorded DROP TOMBSTONES as id|bindSqlDigest|planSqlHash keys. */
        default List<String> droppedMarkers() {
            return List.of();
        }

        /** The newest stored update_time of the WHOLE simulated table, in epoch MICROSECONDS. */
        default long newestStoredUpdateMicros() {
            return 0;
        }

        /**
         * The newest stored update_time OF ONE ID, in epoch MICROSECONDS (0 = none): a status
         * flip is ordered against ITS OWN id (the production reader is id-keyed), so a store
         * that only knows the table-wide value answers with it as a conservative superset.
         *
         * @param id the baseline id
         * @return the newest stored instant of that id
         */
        default long newestStoredUpdateMicros(long id) {
            return newestStoredUpdateMicros();
        }

        /** The simulated mutation-clock bump (counted by tests asserting clock advances). */
        default void bumpMutationClock() {
        }

        void insert(BaselinePlan plan);

        List<BaselinePlan> readById(long id);

        void deleteByIdentity(BaselinePlan plan);
    }

    /**
     * Routes the COMPACT HIGH-WATER-MARK slot - the id watermark, the bounded mutation
     * clock and its OPEN-WINDOW marker - to a simulator, so a unit test can drive the REAL
     * clock protocol without an internal table.
     */
    public interface MutationClockStore {
        /** SELECT_SLOT_STATE_SQL: the slot's (MAX(watermark), MAX(tick)). */
        long[] slotState();

        /** SELECT_CLOCK_SQL: (MAX(tick), COUNT(*), SUM(tick), MAX(pending)). */
        long[] clock();

        /** INSERT_HWM_SQL: appends one slot row. */
        void append(long lastId, long tick, long pending);

        /** PRUNE_HWM_SQL then PRUNE_HWM_SAME_WATERMARK_SQL, in that order. */
        void prune(long lastId, long tick);

        /** PRUNE_HWM_OPEN_SQL: removes the OPEN row of the window being closed. */
        void closeWindow(long windowTick);

        /** SELECT_PENDING_TICK_SQL: how many rows carry (tick, pending) = (tick, tick). */
        long countVisiblePending(long tick);
    }

    /**
     * The read-back visibility confirmation of a reported-successful write: one call is
     * ONE probe attempt, true = readable. A test decrements an invisible window here to
     * simulate the COMMITTED-but-not-yet-published state the real store exposes.
     */
    public interface DurableVisibilityProbe {
        boolean isReadable(long id, BaselineStatus status);

        /**
         * The confirmation of a row JUST WRITTEN additionally checks the ATTEMPTED STORED
         * SECOND. The default delegates to the two-argument form.
         */
        default boolean isReadable(long id, BaselineStatus status, long updateTime) {
            return isReadable(id, status);
        }
    }

    /** The scripted durable store bound to one isolated manager through its constructor. */
    public static final class TestBaselineStore implements BaselinePersistence {

        public StatusProtocolStore statusProtocolStore;
        public IdAllocatorStore idAllocatorStore;
        public MutationClockStore mutationClockStore;
        public LongSupplier hwmRecordRead;
        public LongSupplier seqTailRead;
        public LongFunction<List<BaselinePlan>> byHashReader;
        /**
         * The TIMED by-hash reader (round-63): observes the statement timeout the
         * matching-path cache miss threads through (the remaining rewrite budget).
         */
        public java.util.function.BiFunction<Long, Integer, List<BaselinePlan>>
                byHashTimedReader;
        public Supplier<Map<Long, BaselinePlan>> snapshotReader;
        public DurableVisibilityProbe visibilityProbe;
        /** Explicit durable override; null = durable exactly when a hook is installed. */
        public Boolean durableOverride;
        /**
         * Whether this store MODELS the reservation reads (the round-72 capability gate):
         * opt-in per scenario, because the confirmation proof only runs for a store whose
         * sequence watermark / identity record are real readbacks.
         */
        public boolean modelReservationReads;

        /**
         * The rows a status-protocol simulator WROTE (the insert halves of creates and
         * flips), keyed by id then status: the by-id reads are answered from here, so a
         * store that models status rows only still serves the manager's reconciliation
         * probes - exactly the old status-store probe branch did.
         */
        private final Map<Long, Map<BaselineStatus, BaselinePlan>> statusRows =
                new java.util.concurrent.ConcurrentHashMap<>();

        /** Seeds one durable row of a status-only store (the row its counts stand for). */
        public void seedDurableRow(BaselinePlan plan) {
            recordStatusRow(plan);
        }

        private void recordStatusRow(BaselinePlan plan) {
            statusRows.computeIfAbsent(plan.getId(), k -> new java.util.concurrent.ConcurrentHashMap<>())
                    .put(plan.getStatus(), plan.copyPersistedScalars());
        }

        private void forgetStatusRow(long id, BaselineStatus status) {
            Map<BaselineStatus, BaselinePlan> rows = statusRows.get(id);
            if (rows != null) {
                rows.remove(status);
            }
        }

        /** Mirrors the old seams: the store is the durable layer wherever one is scripted. */
        private boolean anyHookInstalled() {
            return statusProtocolStore != null || idAllocatorStore != null
                    || mutationClockStore != null || hwmRecordRead != null
                    || seqTailRead != null || byHashReader != null
                    || snapshotReader != null || visibilityProbe != null;
        }

        @Override
        public boolean isDurable() {
            return durableOverride != null ? durableOverride : anyHookInstalled();
        }

        @Override
        public boolean reservationReadsAvailable() {
            return modelReservationReads;
        }

        @Override
        public boolean hashReadsAvailable() {
            if (!isDurable()) {
                return false;
            }
            // a write-protocol simulator leaves the manager's in-memory store in charge
            // (the historic durable-hash-read gate kept those bindings out)
            return statusProtocolStore == null && idAllocatorStore == null
                    && snapshotReader == null;
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            if (idAllocatorStore != null) {
                return new ArrayList<>(idAllocatorStore.readById(id));
            }
            if (statusProtocolStore != null) {
                List<BaselinePlan> rows = new ArrayList<>();
                Map<BaselineStatus, BaselinePlan> written = statusRows.get(id);
                if (written != null) {
                    for (Map.Entry<BaselineStatus, BaselinePlan> entry : written.entrySet()) {
                        if (statusProtocolStore.countByIdAndStatus(id, entry.getKey()) > 0) {
                            rows.add(entry.getValue());
                        }
                    }
                }
                return rows;
            }
            return List.of();
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash) {
            return byHashReader == null ? List.of() : byHashReader.apply(hash);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            if (byHashTimedReader != null) {
                return byHashTimedReader.apply(hash, timeoutSeconds);
            }
            return readRowsByHash(hash);
        }

        @Override
        public Map<Long, BaselinePlan> readSnapshot() {
            return snapshotReader == null ? Map.of() : snapshotReader.get();
        }

        @Override
        public DurablePresence probeRow(long id, String bindSqlDigest, String planSql,
                BaselineStatus status, String schemaFingerprint, Long requiredUpdateTimeMicros) {
            if (idAllocatorStore == null) {
                // mirrors the real-read outcome of a store that models no by-id rows
                return DurablePresence.UNKNOWN;
            }
            try {
                for (BaselinePlan row : idAllocatorStore.readById(id)) {
                    if (rowMatches(row, id, bindSqlDigest, planSql, status, schemaFingerprint,
                            requiredUpdateTimeMicros)) {
                        return DurablePresence.PRESENT;
                    }
                }
                return DurablePresence.ABSENT;
            } catch (RuntimeException e) {
                return DurablePresence.UNKNOWN;
            }
        }

        @Override
        public int countByIdAndStatus(long id, String schemaFingerprint, BaselineStatus status) {
            return statusProtocolStore == null ? 0
                    : statusProtocolStore.countByIdAndStatus(id, status);
        }

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            if (visibilityProbe != null) {
                return visibilityProbe.isReadable(plan.getId(), plan.getStatus(),
                        plan.getUpdateTime());
            }
            if (idAllocatorStore != null) {
                // the identity-scoped readable check (the stored instant is NOT part of it:
                // the attempted-row proof of an AMBIGUOUS write asks for it separately,
                // see observedInsertRowIsOurs)
                return probeRow(plan.getId(), plan.getBindSqlDigest(), plan.getPlanSql(),
                        plan.getStatus(), plan.getSchemaFingerprint(), null)
                        == DurablePresence.PRESENT;
            }
            return true; // the simulated store is synchronous / skips the confirmation
        }

        @Override
        public boolean isRowGone(BaselinePlan plan) {
            return visibilityProbe == null
                    || !visibilityProbe.isReadable(plan.getId(), null);
        }

        @Override
        public boolean isStatusRowGone(long id, String schemaFingerprint, BaselineStatus status) {
            return visibilityProbe == null || !visibilityProbe.isReadable(id, status);
        }

        @Override
        public long newestStoredUpdateMicros(long id) {
            if (statusProtocolStore != null) {
                return statusProtocolStore.newestStoredUpdateMicros();
            }
            // ID-scoped, like the production reader (the table-wide form predates the bounded
            // mutation clock and made an unrelated future-dated row appear as this id's version)
            return idAllocatorStore == null ? 0 : idAllocatorStore.newestStoredUpdateMicros(id);
        }

        @Override
        public void insert(BaselinePlan plan) {
            if (idAllocatorStore != null) {
                idAllocatorStore.insert(plan);
            } else if (statusProtocolStore != null) {
                statusProtocolStore.insert(plan);
                recordStatusRow(plan);
            }
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            if (statusProtocolStore != null) {
                boolean written = statusProtocolStore.insertIfPreviousPresent(plan, previousStatus);
                if (written) {
                    recordStatusRow(plan);
                }
                return written;
            }
            insert(plan);
            return true;
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            if (idAllocatorStore != null) {
                idAllocatorStore.deleteByIdentity(plan);
            }
        }

        @Override
        public void deleteByIdAndStatus(BaselinePlan plan, BaselineStatus status,
                long oldUpdateTimeMicros) {
            if (statusProtocolStore != null) {
                statusProtocolStore.deleteByIdAndStatus(plan.getId(), status, oldUpdateTimeMicros);
                forgetStatusRow(plan.getId(), status);
            }
        }

        @Override
        public long tableWatermark() {
            return idAllocatorStore == null ? 0 : idAllocatorStore.watermark();
        }

        @Override
        public long sequenceWatermark() {
            if (idAllocatorStore != null) {
                return idAllocatorStore.seqWatermark();
            }
            if (hwmRecordRead != null) {
                long hwm = hwmRecordRead.getAsLong();
                if (hwm <= 0) {
                    return 0;
                }
                return Math.max(hwm, seqTailRead == null ? 0 : seqTailRead.getAsLong());
            }
            return 0;
        }

        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash, long reserveTimeMicros) {
            if (idAllocatorStore != null) {
                idAllocatorStore.reserveId(id, bindSqlDigest, planSqlHash, reserveTimeMicros);
            }
        }

        @Override
        public void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
            if (idAllocatorStore != null) {
                idAllocatorStore.notePendingSeqState(bindSqlDigest, planSqlHash, id, atMillis);
            }
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            return idAllocatorStore == null ? null
                    : idAllocatorStore.readLatestReservation(bindSqlDigest, planSqlHash);
        }

        @Override
        public void retirePendingSeqState(String bindSqlDigest, long planSqlHash, long markerId) {
            if (idAllocatorStore != null) {
                idAllocatorStore.retirePendingSeqState(bindSqlDigest, planSqlHash, markerId);
            }
        }

        @Override
        public boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            if (idAllocatorStore != null) {
                idAllocatorStore.appendDroppedMarker(id, bindSqlDigest, planSqlHash, atMillis);
            }
            return true;
        }

        @Override
        public Set<String> readDroppedIdentities(java.util.Collection<Long> ids) {
            return idAllocatorStore == null ? Set.of()
                    : new HashSet<>(idAllocatorStore.droppedMarkers());
        }

        @Override
        public long readHwmRecord() {
            return hwmRecordRead == null ? 0 : hwmRecordRead.getAsLong();
        }

        @Override
        public long readSequenceTail(long floor) {
            return seqTailRead == null ? 0 : seqTailRead.getAsLong();
        }

        @Override
        public long readLegacySequenceWatermark() {
            return 0;
        }

        @Override
        public long[] slotState() {
            return mutationClockStore == null ? new long[] {0L, 0L}
                    : mutationClockStore.slotState();
        }

        @Override
        public long[] clock() {
            return mutationClockStore == null ? new long[] {0L, 0L, 0L, 0L}
                    : mutationClockStore.clock();
        }

        @Override
        public void appendHwmRecord(long lastId, long tick, long pending) {
            if (mutationClockStore != null) {
                mutationClockStore.append(lastId, tick, pending);
            } else if (idAllocatorStore != null) {
                // the historic id-allocator simulators carried the clock bump themselves
                idAllocatorStore.bumpMutationClock();
            }
        }

        @Override
        public void pruneHwmRecords(long lastId, long tick) {
            if (mutationClockStore != null) {
                mutationClockStore.prune(lastId, tick);
            }
        }

        @Override
        public void closeMutationWindow(long windowTick) {
            if (mutationClockStore != null) {
                mutationClockStore.closeWindow(windowTick);
            }
        }

        @Override
        public long countVisiblePending(long tick) {
            return mutationClockStore == null ? 1L
                    : (mutationClockStore.countVisiblePending(tick) > 0 ? 1L : 0L);
        }

        private static boolean rowMatches(BaselinePlan row, long id, String bindSqlDigest,
                String planSql, BaselineStatus status, String schemaFingerprint,
                Long requiredUpdateTimeMicros) {
            return row.getId() == id
                    && Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                    && Objects.equals(row.getPlanSql(), planSql)
                    && (status == null || row.getStatus() == status)
                    && (schemaFingerprint == null
                            || Objects.equals(row.getSchemaFingerprint(), schemaFingerprint))
                    && (requiredUpdateTimeMicros == null
                            || row.getUpdateTime() == requiredUpdateTimeMicros);
        }
    }

    /** The scripted cluster gateway: the leadership probe and the journal-sync hook. */
    public static final class TestClusterGateway implements BaselineClusterGateway {

        public BooleanSupplier leaderProbe;
        public Runnable forwardedDdlSync;

        @Override
        public boolean isLeader() {
            return leaderProbe == null || leaderProbe.getAsBoolean();
        }

        @Override
        public void syncJournalWithMaster(ConnectContext ctx) {
            if (forwardedDdlSync != null) {
                forwardedDdlSync.run();
            }
        }
    }
}
