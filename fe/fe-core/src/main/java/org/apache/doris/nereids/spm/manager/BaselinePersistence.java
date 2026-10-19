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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The durable layer of {@link BaselineManager}: every internal-table statement the
 * manager issues, expressed as semantic operations.
 *
 * The production implementation
 * ({@code BaselineManager.TablePersistence}) executes the SQL against
 * {@code __internal_schema.spm_baselines} and its companion tables; unit tests may bind
 * an isolated manager to a substituted implementation (a Mockito mock or a test-local
 * fixture) through {@link BaselineManager#BaselineManager(BaselinePersistence,
 * BaselineClusterGateway, java.util.function.LongSupplier)}.
 *
 * Defaults are chosen so that a SIMULATED store (a test double whose scenario does not
 * model a particular aspect) degrades to the historically "synchronous store" behavior:
 * reads answer empty, writes succeed silently, and visibility / mutation-clock probes
 * answer "settled" - the manager's exact protocols still run unchanged when an
 * implementation overrides the corresponding methods.
 */
public interface BaselinePersistence {

    /**
     * One unit of the sequence table's identity records
     * (see the manager's allocation / tombstone protocol): the id, the instant it was
     * reserved and what the record REPRESENTS - an ambiguous-create marker
     * ({@code unconfirmed}), a tombstone ({@code dropped}) or the plain pre-INSERT
     * reservation of every create.
     */
    final class SeqReservation {
        final long id;
        final long reserveTimeMs;
        final boolean unconfirmed;
        final boolean dropped;

        public SeqReservation(long id, long reserveTimeMs) {
            this(id, reserveTimeMs, true, false);
        }

        public SeqReservation(long id, long reserveTimeMs, boolean unconfirmed,
                boolean dropped) {
            this.id = id;
            this.reserveTimeMs = reserveTimeMs;
            this.unconfirmed = unconfirmed;
            this.dropped = dropped;
        }

        public long id() {
            return id;
        }

        public long reserveTimeMs() {
            return reserveTimeMs;
        }

        public boolean unconfirmed() {
            return unconfirmed;
        }

        public boolean dropped() {
            return dropped;
        }
    }

    /** Outcome of one durable identity probe (see {@link #probeRow}). */
    enum DurablePresence {
        PRESENT, ABSENT, UNKNOWN
    }

    /**
     * Whether a durable backing store is behind this manager: durable reads (by-hash /
     * snapshot / eviction) are available, write routing is active and the mutation
     * window protocol runs. The live implementation answers the internal-schema
     * configuration (false in unit tests whose FE runs without the internal schema db);
     * substituted stores decide for their scenario.
     */
    default boolean isDurable() {
        return false;
    }

    /**
     * Whether the store serves the stable WHOLE-STORE reads of the matching path (the
     * by-hash cache fill and the SHOW snapshot): true for the live internal table; a
     * substituted store answers false unless it also models those reads, which leaves the
     * in-memory store of the manager in charge exactly like the historic simulator
     * bindings did.
     */
    default boolean hashReadsAvailable() {
        return isDurable();
    }

    /**
     * Whether the store MODELS the id-reservation readback: the allocation watermark plus
     * the identity's latest record (see {@link #readReservationView}).
     *
     * This is a STATIC CAPABILITY of the store implementation, never a dynamic value. It
     * used to be inferred from a readback VALUE ({@code sequenceWatermark() <= 0}), which
     * is wrong in the exact case the proof exists for: a fresh cluster's LAGGING readback
     * answers 0 while the reservation the CREATE must prove is simply not published yet,
     * so the proof was skipped and a successor handed the same id out twice - two ENABLED
     * rows then published under one id. Never turn this back into a value probe.
     *
     * The create's ownership proof (see BaselineManager#confirmReservationReadable) is
     * skipped for a store that answers false: the scripted collision / status stores
     * stand in for the WHOLE protocol instead of modelling its reads.
     */
    default boolean reservationReadsAvailable() {
        return false;
    }

    // ==================== row reads ====================

    /**
     * Every durable row carrying one id (the collision probe / reconciliation /
     * adoption read). {@code rebuildTrees} selects the FULL decode (transient
     * parameterized trees rebuilt, like a load) over the plain scalar decode.
     * Rows are returned RAW: the caller applies the drop-tombstone filter where the
     * original protocol did.
     */
    default List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
        return List.of();
    }

    /**
     * The COMPLETE row set of one bindSqlHash (the matching-path cache miss), already
     * tombstone-filtered - every baseline of that bind_sql whatever its status.
     */
    default List<BaselinePlan> readRowsByHash(long hash) {
        return List.of();
    }

    /**
     * {@link #readRowsByHash(long)} with an explicit statement timeout: the matching-path
     * cache miss bounds its WHOLE read by the caller's remaining rewrite budget
     * (spm_rewrite_timeout_ms), so a slow / unavailable baseline tablet degrades the
     * single rewrite attempt instead of stalling the query past its budget. A store that
     * does not model timeouts falls back to the plain read.
     *
     * @param timeoutSeconds the statement timeout in seconds (>= 1)
     */
    default List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
        return readRowsByHash(hash);
    }

    /**
     * One stable whole-table snapshot: FINAL rows (drop tombstones filtered, same-key
     * twins collapsed to the deterministic winner, per-id duplicates resolved), keyed by
     * id. The load / refresh / SHOW paths publish it as-is.
     */
    default Map<Long, BaselinePlan> readSnapshot() {
        return Map.of();
    }

    /**
     * Whether the durable table holds a row with this (id, key) AND - when
     * {@code status} is given - the expected status, the schema fingerprint (when given)
     * and the REQUIRED stored second (when given; the attempted second of a fresh write
     * is strictly later than every row it met, see the manager's status bump). A read
     * that cannot answer is UNKNOWN - never ABSENT.
     */
    default DurablePresence probeRow(long id, String bindSqlDigest, String planSql,
            BaselineStatus status, String schemaFingerprint, Long requiredUpdateTimeMs) {
        return DurablePresence.UNKNOWN;
    }

    /**
     * Number of durable rows carrying (id, status) AND the identity's schema
     * fingerprint (the status reconciliation / confirmation count).
     */
    default int countByIdAndStatus(long id, String schemaFingerprint, BaselineStatus status) {
        return 0;
    }

    /**
     * Whether the row just written is READABLE (the insert-visibility confirmation): the
     * identity, the attempted status and the ATTEMPTED stored second all match. A
     * simulated store is synchronous by default (answer true); the check is polled by
     * the caller, so an implementation modeling a publication window may answer false
     * for a bounded number of probes.
     */
    default boolean isRowReadable(BaselinePlan plan) {
        return true;
    }

    /**
     * Whether the identity's durable rows are ABSENT (the delete confirmation, see
     * {@link #isRowReadable} for the probing convention).
     */
    default boolean isRowGone(BaselinePlan plan) {
        return true;
    }

    /**
     * Whether no durable row carries (id, status, fingerprint) any more (the
     * old-status-row-gone confirmation of a status flip).
     */
    default boolean isStatusRowGone(long id, String schemaFingerprint, BaselineStatus status) {
        return true;
    }

    /**
     * The newest stored update_time of THIS id's rows, in epoch SECONDS (0 = none): the
     * status flip advances its new row strictly past it, so the flipped row is the
     * durable winner at the table's second precision.
     */
    default long newestStoredUpdateSecond(long id) {
        return 0;
    }

    // ==================== row writes ====================

    /** Inserts one baseline row (the create path, after its id was reserved). */
    default void insert(BaselinePlan plan) {
    }

    /**
     * The INSERT half of a status flip, CONDITIONAL on the previous-status row: returns
     * false when the conditional statement matched no row (a concurrent DROP / flip won)
     * and wrote nothing. The default is unconditional (a substituted store that models
     * no conditional insert keeps its historic behavior).
     */
    default boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
        insert(plan);
        return true;
    }

    /**
     * As insertIfPreviousPresent, with the DURABLE ORDER attached: the statement must also
     * observe that no NEWER row of the id exists, so a stalled earlier flip cannot land after
     * a successor already moved the intent (see
     * BaselineManager#conditionalStatusInsertSql).
     *
     * @param plan                 the new-status row
     * @param previousStatus       the status the flip starts from
     * @param previousUpdateSecond the stored second the flip is based on (0 = unknown)
     * @return whether a row was written
     */
    default boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus,
            long previousUpdateSecond) {
        return insertIfPreviousPresent(plan, previousStatus);
    }

    /** Identity-scoped delete (id + digest + planSql + fingerprint). */
    default void deleteByIdentity(BaselinePlan plan) {
    }

    /**
     * Removes the old-status row of a flip, bound to the stored VERSION of the row the
     * flip completes (the plan's identity scopes the predicate; see the manager's
     * deleteByIdAndStatusSql): only that row is removed; 0 = not bound.
     */
    default void deleteByIdAndStatus(BaselinePlan plan, BaselineStatus status,
            long oldUpdateTimeMs) {
    }

    /**
     * Reclaims the SUPERSEDED versions of one baseline identity: every row of the id whose
     * stored second is strictly EARLIER than keepFromSecond (see
     * BaselineManager#persistNoOpStatusIntent).
     *
     * A successful no-op ALTER appends an ordering-intent row and nothing ever removed the
     * earlier ones, so one baseline accumulated unbounded durable rows - and every cold match
     * re-parsed ALL of them before collapsing by id. The predicate preserves the intent row
     * itself and every NEWER concurrent version.
     *
     * @param identity       the baseline identity scoping the delete (id, digest, plan text,
     *                       fingerprint)
     * @param keepFromSecond the stored second to keep from (0 = nothing to prune)
     */
    default void pruneSupersededVersions(BaselinePlan identity, long keepFromSecond) {
    }

    /**
     * How many versions of one baseline identity are still READABLE below a stored second -
     * the visibility probe of a prune (see BaselineManager#confirmPruneVisible). SQL OK only
     * proves the DELETE was dispatched, and a paginated snapshot reader could still step
     * over a row it removes.
     *
     * @param identity       the identity scoping the read
     * @param keepFromSecond the stored second the prune keeps from
     * @return the count of older readable versions (0 = none / a store that models no
     *         versions, -1 = the store cannot answer)
     */
    default long countVersionsBefore(BaselinePlan identity, long keepFromSecond) {
        return 0;
    }

    // ==================== id allocation / identity records ====================

    /** The baselines table's own MAX(id) (see the manager's "Id source"). */
    default long tableWatermark() {
        return 0;
    }

    /**
     * The id ALLOCATION watermark: the compact high-water-mark record reconciled with the
     * sequence reservation tail (and the legacy history on a cluster that predates the
     * compact record).
     */
    default long sequenceWatermark() {
        return 0;
    }

    /**
     * Durably reserves an allocated id BEFORE its baseline row is written: records the
     * high-water mark, appends the identity-carrying reservation and flips the compact
     * identity slot (strict - a failed reservation fails the CREATE).
     */
    default void reserveId(long id, String bindSqlDigest, long planSqlHash, long reserveTimeMs) {
    }

    /**
     * Records the durable marker of an AMBIGUOUS create (unconfirmed = 1): a retry on
     * another FE defers on it instead of allocating a second id. Best effort.
     */
    default void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
            long atMillis) {
    }

    /**
     * The identity's LATEST reservation record (plain reservation / unconfirmed marker /
     * tombstone), or null when the store holds none (see {@link SeqReservation}).
     *
     * The answer is deliberately IDENTITY-scoped, not write-scoped: it tells a caller what
     * a SUCCESSOR would conclude about this identity right now, which is exactly what the
     * create's ownership proof asks (see {@link #readReservationView}), and what the
     * defer / adopt / condemn decision of a retried CREATE needs. It used to be named
     * pendingSeqReservation, which read as "only for a pending create" although the proof
     * uses it too.
     *
     * @param bindSqlDigest the identity's canonical bind digest
     * @param planSqlHash   the identity's plan SQL hash
     * @return the latest record, or null
     */
    default SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
        return null;
    }

    /**
     * The BATCH form of {@link #readLatestReservation}: the COMPACT identity record of every
     * requested slot in ONE read.
     *
     * A snapshot filter needs the state of EVERY identity in the snapshot, and the single form
     * issues one distributed SELECT per identity - with 2,000 live baselines that is 2,000
     * sequential queries on every periodic refresh on every FE, all of them while the refresh
     * holds the writer lock (so a GLOBAL CREATE / ALTER / DROP waits behind the whole loop).
     * The batch answers the healthy identities; the caller still takes the single read for a
     * slot this map does NOT answer, because that path replays the append-only HISTORY - a
     * compact tombstone can PREDATE a newer reservation of the same identity.
     *
     * The default performs no read (an empty map = "not answered"): a substituted / scripted
     * store keeps using the single-identity form, which is also what the history fallback
     * needs.
     *
     * @param compactIds the compact slot ids (see the manager's compactSeqId)
     * @param timeoutSeconds the statement timeout
     * @return compact slot id to its compact record (a slot without one is absent)
     */
    default Map<Long, SeqReservation> readLatestReservations(Collection<Long> compactIds,
            int timeoutSeconds) {
        return Map.of();
    }

    /**
     * One reservation VIEW of an identity: the allocation watermark plus the identity's
     * latest record - the two answers a successor / retry reads, taken as one value.
     *
     * The pair is NOT atomic, and does not need to be: the view only ever confirms what was
     * ALREADY written, and every caller re-reads it in a bounded loop (see
     * BaselineManager#confirmReservationReadable), so a view that misses either half just
     * makes the loop retry. Bundling it keeps the caller from composing the two reads by
     * hand, which made the confirmation look like an accident of the read interface.
     */
    final class ReservationView {
        private final long watermark;
        private final SeqReservation latest;

        public ReservationView(long watermark, SeqReservation latest) {
            this.watermark = watermark;
            this.latest = latest;
        }

        /** The allocation watermark a successor's CREATE reads (ids up to it are handed out). */
        public long watermark() {
            return watermark;
        }

        /** The identity's latest record, or null (see {@link #readLatestReservation}). */
        public SeqReservation latest() {
            return latest;
        }

        /**
         * Whether the reservation of exactly this id is VISIBLE: the watermark reaches it
         * and the identity's latest record IS that plain reservation (not an older id, not
         * an unconfirmed marker of it, not a tombstone).
         *
         * @param id the id this CREATE reserved
         * @return whether a successor would see the reservation
         */
        public boolean showsReservation(long id) {
            return watermark >= id && latest != null && latest.id == id && !latest.dropped;
        }
    }

    /**
     * The reservation view one identity exposes RIGHT NOW (see {@link ReservationView}),
     * read through the SAME two reads a successor / a retry on another FE performs: the
     * allocation watermark ({@code sequenceWatermark}) and the identity lookup
     * ({@code readLatestReservation}).
     *
     * That equivalence is the WHOLE point: the create's ownership proof must assert the
     * visibility a successor will have, so it may not use a "more reliable" read of its
     * own - a private, cleaner read would prove something the successor does not see. The
     * live store therefore OVERRIDES this only to batch / share the two reads, never to
     * change their answers.
     *
     * Note the watermark half is not necessarily a pure read: the live implementation may
     * best-effort SEED the compact high-water-mark record while answering (see the
     * manager's readCompactIdWatermark). That is intended - the successor's own read would
     * do the same - but it means this method must not be called from a pure-read context
     * that cannot tolerate an internal-table write.
     *
     * @param bindSqlDigest the identity's canonical bind digest
     * @param planSqlHash   the identity's plan SQL hash
     * @return the view a successor's two reads would assemble
     */
    default ReservationView readReservationView(String bindSqlDigest, long planSqlHash) {
        return new ReservationView(sequenceWatermark(),
                readLatestReservation(bindSqlDigest, planSqlHash));
    }

    /** Retires the unconfirmed marker(s) of one resolved ambiguous write. Best effort. */
    default void retirePendingSeqState(String bindSqlDigest, long planSqlHash, long markerId) {
    }

    /**
     * Appends one DROP TOMBSTONE (identity or id-scoped; an empty digest / 0 hash marks
     * the id-scoped form). Returns false when the append did not land - the caller then
     * keeps the identity masked in memory and retries.
     */
    default boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
            long atMillis) {
        return true;
    }

    /**
     * The DURABLE drop tombstones of the given ids as {@code id|digest|planSqlHash} /
     * {@code id||0} keys. A read failure propagates (loads fail closed).
     */
    default Set<String> readDroppedIdentities(Collection<Long> ids) {
        return Set.of();
    }

    /**
     * {@link #readDroppedIdentities(Collection)} with an explicit statement timeout: the
     * matching-path cache miss bounds its whole read - the rows AND the tombstone
     * filter - by the remaining rewrite budget (see readRowsByHash(long, int)).
     *
     * @param timeoutSeconds the statement timeout in seconds (>= 1)
     */
    default Set<String> readDroppedIdentities(Collection<Long> ids, int timeoutSeconds) {
        return readDroppedIdentities(ids);
    }

    // ==================== compact id high-water mark ====================

    /** The compact HWM slot's newest {@code last_id} (0 = no record). */
    default long readHwmRecord() {
        return 0;
    }

    /** The highest sequence reservation visible beyond {@code floor} (bounded read). */
    default long readSequenceTail(long floor) {
        return 0;
    }

    /** The legacy append-only history watermark (the pre-compact-record fallback). */
    default long readLegacySequenceWatermark() {
        return 0;
    }

    // ==================== mutation clock ====================

    /** The slot's [MAX(last_id), MAX(tick)]. */
    default long[] slotState() {
        return new long[] {0L, 0L};
    }

    /** The clock token: (MAX(tick), COUNT(*), SUM(tick), MAX(open window tick)). */
    default long[] clock() {
        return new long[] {0L, 0L, 0L, 0L};
    }

    /** Appends one high-water-mark / clock row. */
    default void appendHwmRecord(long lastId, long tick, long pending) {
    }

    /** Best-effort prune of the HWM rows the just-written one supersedes. */
    default void pruneHwmRecords(long lastId, long tick) {
    }

    /** Closes the OPEN window of one completed mutation (exact-row delete). */
    default void closeMutationWindow(long windowTick) {
    }

    /**
     * Whether the OPEN window just appended is READABLE by other readers (the
     * bounded read-back that makes the statement's dispatch conditional on the window
     * being shared). Substituted stores are synchronous: the default answers 1.
     */
    default long countVisiblePending(long tick) {
        return 1L;
    }
}
