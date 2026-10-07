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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.InternalSchema;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineScope;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.BaselineStatus;
import org.apache.doris.nereids.spm.SPMPlanTreeSupport;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.SPMUtils;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * BaselineManager - baseline storage, cache and index management (M3).
 *
 * Corresponds to design doc section 6.6. Manages the CRUD of baselines and maintains
 * two query structures:
 *
 * - hashIndex: bindSqlHash (Long) -> baseline id list (List of Long). Level 1
 *   coarse filtering with O(1) lookup.
 * - baselines: id -> BaselinePlan in-memory storage (Phase 1 MVP). Phase 2 persists it
 *   to the __internal_schema.spm_baselines internal table (see design doc 6.14).
 *
 * Candidate baseline lookup (the first two of the three-level filter):
 *
 * 1. Level 1: hashIndex.get(queryHash) -> candidate id list
 * 2. Level 2: exact digest.equals(baseline.bindSqlDigest) matching
 * 3. Sort by priority (see the comparator below)
 *
 * Id source (the GLOBAL id invariant): GLOBAL writes have a single writer (user DDL is
 * forwarded to the master, auto capture runs on the Leader), so "read the persistence
 * watermark, then allocate" is sufficient to keep the local generator from ever handing
 * out an id that is already used in the shared table: before EVERY allocation
 * createBaseline reads MAX(id) from the internal table (one light aggregation) and
 * advances idGenerator past it; when that read fails the CREATE fails visibly with a
 * retryable error and no id is allocated. This closes the failover lag hole (a new
 * master may start with a generator behind the table) and the load-failure hole (a
 * broken startup load leaves the generator at 1 while the table is full); it stays
 * correct even when a row was inserted out of contract (e.g. a manual table write). The
 * startup load / periodic refresh keep advancing the generator as a second safety net.
 * Nothing that does not allocate (ALTER / DROP / matching) reads the watermark, and
 * creates only come from the DDL / capture cycle, so the read is off the query hot path.
 * GLOBAL ids start at 1 and stay in [1, 2^62); SESSION ids live in [2^62, 2^63) (see
 * BaselineScope.ofId), so the scope of an id is always exact.
 *
 * Concurrency: the in-memory store (baselines / hashIndex / stateVersion / load state) is
 * guarded by one read-write lock. Lookups take the read lock and never serialize on each
 * other; create / drop / status / load / refresh take the write lock; internal-table I/O
 * runs outside the lock wherever correctness allows (see the individual methods).
 */
public class BaselineManager {

    // ==================== test seams (never set in production) ====================

    /**
     * Test seam: routes the status-protocol durable I/O (INSERT / DELETE by status / the
     * reconciliation count read) to a simulator instead of the internal table, so a unit
     * test can inject faults such as "the old-row delete committed but reported
     * KV_TXN_MAYBE_COMMITTED". Null in production.
     */
    @VisibleForTesting
    interface StatusProtocolStoreForTest {
        void insert(BaselinePlan plan);

        void deleteByIdAndStatus(long id, BaselineStatus status);

        int countByIdAndStatus(long id, BaselineStatus status);

        /**
         * The CONDITIONAL insert half of a status flip: mirrors the durable
         * INSERT ... SELECT ... WHERE id / status statement - the new-status row
         * must NOT be written once the previous-status row is gone (the caller then
         * refuses the flip, which is how the DROP-while-ALTER-stalled conflict surfaces).
         * The default keeps the unconditional simulators working: their scenarios always
         * keep the previous row.
         *
         * @return whether the new-status row was written
         */
        default boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            insert(plan);
            return true;
        }

        /**
         * The newest stored update_time of the WHOLE simulated table, in epoch SECONDS
         * (0 = none): the updateStatus bump reads this, so a simulator that
         * keeps future-bumped update_times must expose them for the bump to apply - the
         * default keeps simulators that never store future times working.
         *
         * @return the newest stored update_time in seconds, 0 when unavailable
         */
        default long newestStoredUpdateSecond() {
            return 0;
        }
    }

    /**
     * Test seam for the create-time id allocator / collision protocol: routes the
     * watermark read, the INSERT, the by-id collision probe, the identity delete AND its
     * ambiguous-commit reconciliation read to a simulator, so a unit test can inject a
     * COMPETING master's row between the INSERT and the probe (the latch-driven handoff
     * scenario) or an unconfirmable delete. Null in production.
     */
    @VisibleForTesting
    interface IdAllocatorStoreForTest {
        long watermark();

        default long seqWatermark() {
            return 0;
        }

        default void reserveId(long id) {
        }

        /**
         * The identity-carrying reservation: routes to the simulator's own
         * storage of pendingSeqReservation. The default keeps simulators that
         * model no keyed reservations working. The record fences a retry of the same key
         * while its id stays unreadable and it is younger than the durable fence
         *
         * @param id           the reserved id
         * @param bindSqlDigest the baseline's bind digest
         * @param planSqlHash   the hash of the baseline's plan SQL
         * @param reserveTimeMs the reservation instant (epoch millis)
         */
        default void reserveId(long id, String bindSqlDigest, long planSqlHash,
                long reserveTimeMs) {
            reserveId(id);
        }

        /**
         * Records the DURABLE pending marker of an ambiguous create: the
         * same identity-carrying append with unconfirmed = 1. The default keeps
         * simulators without keyed reservations working.
         *
         * @param bindSqlDigest the baseline's bind digest
         * @param planSqlHash   the hash of the baseline's plan SQL
         * @param id            the id the ambiguous write consumed
         * @param atMillis      the marker instant (epoch millis)
         */
        default void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
        }

        /**
         * The latest identity record of one baseline in the simulated sequence table, or
         * null: the durable unconfirmed-create fence of
         * resolveDurablePendingCreate reads it. Either the explicit UNCONFIRMED
         * marker of an ambiguous write, or - the plain reservation row
         * appended before every baseline write (the marker wins when both exist). The
         * default keeps simulators without keyed records working (no record = no fence).
         *
         * @param bindSqlDigest the baseline's bind digest
         * @param planSqlHash   the hash of the baseline's plan SQL
         * @return the record, or null when none exists
         */
        default SeqReservation pendingSeqReservation(String bindSqlDigest, long planSqlHash) {
            return null;
        }

        /**
         * Retires the durable UNCONFIRMED marker of a RESOLVED ambiguous write
         * the simulated equivalent of the marker DELETE. The default
         * keeps simulators without keyed markers working.
         *
         * @param bindSqlDigest the baseline's bind digest
         * @param planSqlHash   the hash of the baseline's plan SQL
         * @param markerId      the id the marker was appended for
         */
        default void retirePendingSeqState(String bindSqlDigest, long planSqlHash,
                long markerId) {
        }

        /**
         * Records a DROP TOMBSTONE: the append-only identity of a baseline
         * the DROP removed. The default keeps simulators without tombstones working (the
         * load filter then finds no marker).
         *
         * @param id            the dropped baseline id
         * @param bindSqlDigest the baseline's bind digest
         * @param planSqlHash   the hash of the baseline's plan SQL
         * @param atMillis      the marker instant (epoch millis)
         */
        default void appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
        }

        /**
         * The recorded DROP TOMBSTONES as id|bindSqlDigest|planSqlHash keys; the
         * load filter ignores rows matching one. The default keeps simulators working.
         *
         * @return the recorded tombstone keys (empty when none)
         */
        default List<String> droppedMarkers() {
            return List.of();
        }

        /**
         * The newest stored update_time of the WHOLE simulated table, in epoch SECONDS
         * (0 = none): see StatusProtocolStoreForTest#newestStoredUpdateSecond()
         *
         * @return the newest stored update_time in seconds, 0 when unavailable
         */
        default long newestStoredUpdateSecond() {
            return 0;
        }

        void insert(BaselinePlan plan);

        List<BaselinePlan> readById(long id);

        void deleteByIdentity(BaselinePlan plan);
    }

    @VisibleForTesting
    public static volatile StatusProtocolStoreForTest statusProtocolStoreForTest;

    @VisibleForTesting
    public static volatile IdAllocatorStoreForTest idAllocatorStoreForTest;

    /**
     * Test seam for the compact id high-water-mark RECORD (the value of SELECT_HWM_SQL),
     * null = the internal table. A scripted value also stands in for the legacy history
     * read: the seam answers the whole watermark decision (see readCompactIdWatermark).
     */
    @VisibleForTesting
    public static volatile java.util.function.LongSupplier hwmRecordReadForTest;

    /**
     * Test seam for the scoped sequence-tail read (the value SELECT_SEQ_TAIL_SQL
     * returns), null = the internal table. Only consulted with hwmRecordReadForTest
     * installed - the seam pair models the two stores the watermark read consults.
     */
    @VisibleForTesting
    public static volatile java.util.function.LongSupplier seqTailReadForTest;

    /**
     * Test seam replacing the live leadership probe of assertLeaderForWrite
     * (null in production). The store simulators bypass the live fence by design, so
     * without this seam a unit test cannot interleave a master handoff with an in-flight
     * write (the insert / delete halves of a status flip).
     */
    @VisibleForTesting
    public static volatile java.util.function.BooleanSupplier leaderProbeForTest;

    /**
     * Test seam for the read-back visibility confirmation of a reported-successful write
     * (see confirmInsertVisible): one call is ONE probe attempt, true = the row
     * (insert) or its status row is READABLE, false = not yet visible. A test
     * decrements an invisible window here to simulate the COMMITTED-but-not-yet-published
     * state the real store exposes. Null in production.
     */
    @VisibleForTesting
    interface DurableVisibilityProbeForTest {
        boolean isReadable(long id, BaselineStatus status);

        /**
         * The confirmation of a row JUST WRITTEN additionally checks the ATTEMPTED
         * STORED SECOND: the requested status ALONE is weak evidence (a previously
         * failed old-row delete can leave a STALE row of that very status behind, see
         * observedInsertRowIsOurs). The default delegates to the two-argument
         * form so a simulator that models only the visibility window of one row keeps
         * its semantics.
         *
         * @param id         the baseline id
         * @param status     the status the write attempted
         * @param updateTime the attempted row's update time (stored seconds)
         * @return whether THAT row is readable
         */
        default boolean isReadable(long id, BaselineStatus status, long updateTime) {
            return isReadable(id, status);
        }
    }

    @VisibleForTesting
    public static volatile DurableVisibilityProbeForTest durableVisibilityProbeForTest;

    /**
     * Test seam replacing the snapshot READ of the load path (loadFromInternalTable /
     * the promotion reload): lets a unit test return a controlled snapshot and, together
     * with snapshotReadStartedHookForTest, invalidate the store WHILE a load is
     * still inside its read - the stale snapshot must then be discarded instead of
     * republished. Null in production.
     */
    @VisibleForTesting
    public static volatile Supplier<Map<Long, BaselinePlan>> snapshotReaderForTest;

    /**
     * Test seam counting the background load threads that were actually STARTED by
     * scheduleAsyncLoad (one per load-slot claim). A query burst must coalesce
     * onto the in-flight load instead of starting one thread per caller, which this
     * counter makes observable. Null in production.
     */
    @VisibleForTesting
    public static volatile java.util.concurrent.atomic.AtomicInteger asyncLoadSpawnCountForTest;

    /**
     * Test seam replacing the journal synchronization of
     * refreshAfterForwardedDdl and confirmGlobalRowsForShow (null in
     * production): the real sync asks the master for its max journal id and waits
     * locally, which a unit test cannot do. A test whose snapshot reader returns a
     * PRE-DDL snapshot until this seam ran proves the sync happens BEFORE the snapshot
     * read.
     */
    @VisibleForTesting
    public static volatile Runnable forwardedDdlSyncForTest;

    /**
     * Test seam invoked by a load right after it captured its generation and BEFORE the
     * snapshot read: a test blocks here, invalidates the store (the promotion window)
     * and lets the load continue - the now-stale snapshot must be discarded.
     */
    @VisibleForTesting
    public static volatile Runnable snapshotReadStartedHookForTest;

    private static final Logger LOG = LogManager.getLogger(BaselineManager.class);

    /** Singleton. */
    private static final BaselineManager INSTANCE = new BaselineManager();

    // ==================== internal-table persistence (__internal_schema.spm_baselines) ==========

    /** Fully qualified internal table (FeConstants.INTERNAL_DB_NAME == "__internal_schema"). */
    private static final String SPM_BASELINES_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_TBL_NAME;

    /** The append-only id reservation table (see InternalSchema#SPM_BASELINES_SEQ_TBL_NAME). */
    private static final String SPM_BASELINES_SEQ_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_SEQ_TBL_NAME;

    /** The compact id high-water-mark table (see InternalSchema#SPM_BASELINES_HWM_TBL_NAME). */
    private static final String SPM_BASELINES_HWM_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_HWM_TBL_NAME;

    /** Column order follows InternalSchema.SPM_BASELINES_SCHEMA (unpaged selection). */
    private static final String SNAPSHOT_COLUMNS =
            "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest` FROM ";

    /**
     * First page of a whole-table snapshot: ordered by id so the pagination can continue
     * with SELECT_PAGE_SQL from the last row read. The order must be a TOTAL
     * order over the rows of ONE id - (update_time, status) break the id ties
     * and the CONTENT columns break the remaining ties: two
     * masters can leave two DIFFERENT rows of one id with the same stored second and the
     * same status (a delayed INSERT committing after a handoff collision), and with
     * ORDER BY `id` alone the engine may return such rows in ANY order in EVERY
     * execution, so an OFFSET continuation landing inside the group could re-read one row
     * and skip another while the row COUNT - the completeness proof - stays unchanged.
     * Rows equal in EVERY column remain interchangeable (they resolve to the same
     * pickDurableWinner outcome).
     */
    private static final String SELECT_ALL_ORDERED_SQL =
            SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE + " ORDER BY `id`, `update_time`, `status`,"
                    + " `bind_sql_digest`, `plan_sql`, `bind_sql`";

    /**
     * One continuation page of a whole-table snapshot: every row with id >=
     * ${lastId}, ordered by id and SKIPPING the first ${offset} rows of that
     * range. The offset is what keeps an id group larger than one page readable: a
     * repeated opposite-status ALTER failure leaves one more row under the id every time,
     * so the group can outgrow SNAPSHOT_PAGE_SIZE rows - a jump past it would
     * omit the rows behind the first page, possibly the newest durable status. The order
     * must remain a TOTAL order within one id (see SELECT_ALL_ORDERED_SQL) or a
     * tie order that differs between the two queries can make the offset skip a row of
     * the interrupted group (content tie-breakers added).
     */
    private static final String SELECT_PAGE_SQL = SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE
            + " WHERE `id` >= ${lastId} ORDER BY `id`, `update_time`, `status`,"
            + " `bind_sql_digest`, `plan_sql`, `bind_sql`"
            + " LIMIT ${pageSize} OFFSET ${offset}";

    /**
     * Rows per snapshot page (see readPersistedSnapshot). Bounds what ONE
     * internal query has to return, so a growing table can no longer make the whole
     * snapshot read fail against a fixed timeout.
     */
    private static final int SNAPSHOT_PAGE_SIZE = 2000;

    /**
     * Fence re-reads a paginated snapshot read is allowed before it fails closed (see
     * readStableSnapshot): one DDL overlapping the loop then converges on the
     * retry, while a table that never stays stable must not be published as a snapshot.
     */
    private static final int SNAPSHOT_STABILITY_ATTEMPTS = 3;

    /** The persistence-layer id watermark (see the class javadoc "Id source"): read
     *  before every id allocation. MAX over an aggregate is a light single-row query. */
    private static final String SELECT_MAX_ID_SQL = "SELECT MAX(`id`) FROM " + SPM_BASELINES_TABLE;

    /**
     * The compact id high-water mark: a tiny append-only table whose rows
     * carry the newest allocated id. The append-only HISTORY table
     * (InternalSchema#SPM_BASELINES_SEQ_TBL_NAME) grows by one row per create
     * forever, so its MAX(last_id) - the only unbounded read on the create path -
     * was replaced: every allocation also records itself here (pruning the superseded
     * rows), and a pre-upgrade cluster only pays the legacy full read ONCE (see
     * readPersistedWatermark).
     */
    private static final String SELECT_HWM_SQL = "SELECT MAX(`last_id`) FROM "
            + SPM_BASELINES_HWM_TABLE + " WHERE `id` = 1";

    /** Append one high-water-mark row (see SELECT_HWM_SQL). */
    private static final String INSERT_HWM_SQL = "INSERT INTO " + SPM_BASELINES_HWM_TABLE
            + " (`id`, `last_id`, `update_time`) VALUES (1, ${lastId}, NOW())";

    /**
     * Best-effort prune of the compact high-water-mark rows (see SELECT_HWM_SQL):
     * removes the rows the just-written one supersedes, so the surviving MAX read
     * stays a scan of a handful of rows. Correctness never depends on it - the read takes
     * the MAX - so failures are swallowed.
     */
    private static final String PRUNE_HWM_SQL = "DELETE FROM " + SPM_BASELINES_HWM_TABLE
            + " WHERE `last_id` < ${lastId}";

    /**
     * The scoped CONFIRMATION of SELECT_HWM_SQL (see readCompactIdWatermark): the
     * highest reservation VISIBLE beyond the recorded mark. The sequence table's key
     * starts with (id, last_id), so the read scans only the rows past the mark - a
     * healthy cluster has none, and a stale record never hides a reservation whose
     * HWM write was lost or is still unreadable.
     */
    private static final String SELECT_SEQ_TAIL_SQL = "SELECT MAX(`last_id`) FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = 1 AND `last_id` > ${floor}";

    /**
     * The id high-water mark that OUTLIVES the rows (see
     * InternalSchema#SPM_BASELINES_SEQ_TBL_NAME): MAX(last_id) over the append-only
     * reservation rows. Kept as the ONE-TIME legacy fallback of
     * readPersistedWatermark (a cluster created before the compact high-water
     * mark table exists); the per-create path reads the bounded
     * SELECT_HWM_SQL instead. The baselines table's own MAX(id) falls back to a
     * lower value as soon as its highest row is DROPped, and an id reused for a DIFFERENT
     * baseline would let a delayed DROP BASELINE PLAN IF EXISTS N retry delete the
     * new baseline.
     */
    private static final String SELECT_SEQ_ID_SQL = "SELECT MAX(`last_id`) FROM "
            + SPM_BASELINES_SEQ_TABLE;

    /** Appends one reservation row (the id just allocated). Append-only: MAX never falls. */
    private static final String INSERT_SEQ_ID_SQL = "INSERT INTO " + SPM_BASELINES_SEQ_TABLE
            + " (`id`, `last_id`, `bind_sql_digest`, `plan_sql_hash`, `reserve_time`,"
            + " `unconfirmed`, `dropped`)"
            + " VALUES (1, ${lastId}, '${bindSqlDigest}', ${planSqlHash}, '${reserveTime}',"
            + " ${unconfirmed}, 0)";

    /**
     * Appends a DROP TOMBSTONE: the identity of a baseline this FE just
     * removed, with dropped = 1. A demoted master's in-flight status INSERT can
     * commit AFTER the DROP deleted the row - its conditional precondition ran against
     * the pre-DROP snapshot, and Doris cannot re-check it at durable commit - and the
     * revived row would make the dropped baseline ACTIVE again on every loader. The
     * tombstone is APPEND-ONLY and survives that commit: a load that sees a baseline row
     * matching a tombstone's (id, bind_sql_digest, plan_sql_hash) treats it as deleted
     * (and repairs it away). Ids are never reused (the sequence watermark), so a matching
     * tombstone always describes this very incarnation.
     */
    private static final String INSERT_SEQ_DROPPED_SQL = "INSERT INTO " + SPM_BASELINES_SEQ_TABLE
            + " (`id`, `last_id`, `bind_sql_digest`, `plan_sql_hash`, `reserve_time`,"
            + " `unconfirmed`, `dropped`)"
            + " VALUES (1, ${lastId}, '${bindSqlDigest}', ${planSqlHash}, '${reserveTime}',"
            + " 0, 1)";

    /**
     * Reads the DROP TOMBSTONES of the GIVEN ids (see INSERT_SEQ_DROPPED_SQL,
     * the append-only sequence table retains every dropped = 1 row
     * forever, so an unrestricted read built a HashSet of the FULL historical drop set on
     * every load / refresh - with a small active set and heavy CREATE / DROP churn the
     * read grew without bound and, once it timed out, follower caches stopped
     * incorporating later GLOBAL changes. The read is scoped to the ids the caller is
     * actually filtering (the snapshot / point-read ids), chunked to keep each statement
     * bounded.
     */
    private static final String SELECT_SEQ_DROPPED_SQL = "SELECT `last_id`, `bind_sql_digest`,"
            + " `plan_sql_hash` FROM " + SPM_BASELINES_SEQ_TABLE + " WHERE `dropped` = 1"
            + " AND `last_id` IN (${ids})";

    /** How many ids one scoped tombstone read carries (see SELECT_SEQ_DROPPED_SQL). */
    private static final int DROPPED_MARKER_ID_CHUNK = 256;

    /**
     * The LATEST identity-carrying row of one baseline: the durable half of the
     * unresolved-create fence (see
     * resolveDurablePendingCreate). BOTH kinds of rows fence while their write's
     * outcome is unresolved:
     *   unconfirmed = 1: the marker of a create whose INSERT outcome was
     *       AMBIGUOUS;
     *   a PLAIN reservation (unconfirmed = 0): the row every create appends
     *       BEFORE its INSERT. Its separate ambiguous marker can fail / lag (that write
     *       is best effort), and a cross-FE retry that sees neither the baseline row nor
     *       the marker then allocated a SECOND id whose committed row later published a
     *       duplicate; the pre-INSERT reservation is the one record that always exists,
     *       so it fences too - but only while its row is not readable and its age is
     *       inside the fence bound.
     *   dropped = 1: a tombstone (a completed DROP or a condemned abandoned
     *       write) RESOLVED the identity - no fence, the key may be created again.
     * The NEWEST IDENTITY wins (highest last_id): ids name the successive
     * incarnations of one key, and reserve_time has only SECOND precision - a DROP of
     * id N followed by a re-create as N+1 within the same stored second used to sort
     * N's dropped = 1 tombstone BEFORE N+1's reservation, so resolveDurablePendingCreate
     * read the key as resolved, skipped the pending fence of N+1's committed-but-
     * unreadable INSERT, and allocated N+2 (both ENABLED rows could then publish).
     * Within one identity the latest state wins: a tombstone (appended after the
     * reservation) means resolved; the same-second order is tombstone, then marker,
     * then the plain row.
     */
    private static final String SELECT_PENDING_SEQ_SQL = "SELECT `last_id`, `reserve_time`,"
            + " `unconfirmed`, `dropped` FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql_hash` = ${planSqlHash}"
            + " ORDER BY `last_id` DESC, `reserve_time` DESC, `dropped` DESC,"
            + " `unconfirmed` DESC LIMIT 1";

    /**
     * The base of the synthetic `id` every COMPACT identity row carries (see
     * INSERT_COMPACT_SEQ_SQL). The sequence table's `id` column is its DUPLICATE key and
     * every historical row carries the constant 1: the identity read filtered on
     * bind_sql_digest / plan_sql_hash alone, which no sort order can bound - the table is
     * append-only and grows with every create / drop forever, so one identity read grew
     * into a full scan of the whole history. Each state change ALSO appends a compact
     * copy under this stable per-identity id, so the read seeks the key prefix and sees
     * only that identity's few surviving rows (the base keeps them clear of the
     * historical id = 1 rows and of the compact HWM rows of the OTHER tables).
     */
    private static final long COMPACT_SEQ_ID_BASE = 2L;

    /** Appends the compact copy of one identity state (see COMPACT_SEQ_ID_BASE). */
    private static final String INSERT_COMPACT_SEQ_SQL = "INSERT INTO " + SPM_BASELINES_SEQ_TABLE
            + " (`id`, `last_id`, `bind_sql_digest`, `plan_sql_hash`, `reserve_time`,"
            + " `unconfirmed`, `dropped`)"
            + " VALUES (${compactId}, ${lastId}, '${bindSqlDigest}', ${planSqlHash},"
            + " '${reserveTime}', ${unconfirmed}, ${dropped})";

    /**
     * Best-effort prune of the compact rows the just-written one supersedes
     * (see COMPACT_SEQ_ID_BASE): without it one identity's slot grows with every state
     * change. Correctness never depends on it - the read takes the MAX within the slot -
     * so failures are swallowed like PRUNE_HWM_SQL's. A compact id is a 64-bit HASH, so
     * two identities may share one slot: their rows carry their own digest / hash
     * predicates and the read falls back to the history when the slot's newest row is
     * another identity's.
     */
    private static final String PRUNE_COMPACT_SEQ_SQL = "DELETE FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = ${compactId} AND `last_id` < ${lastId}";

    /**
     * The BOUNDED identity read (see COMPACT_SEQ_ID_BASE): the same answer as
     * SELECT_PENDING_SEQ_SQL, sought through the `id` key prefix, with the identity
     * predicates kept because a compact id is a hash. SELECT_PENDING_SEQ_SQL remains the
     * fallback for a slot without a readable row.
     */
    private static final String SELECT_COMPACT_SEQ_SQL = "SELECT `last_id`, `reserve_time`,"
            + " `unconfirmed`, `dropped` FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = ${compactId}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql_hash` = ${planSqlHash}"
            + " ORDER BY `last_id` DESC, `reserve_time` DESC, `dropped` DESC,"
            + " `unconfirmed` DESC LIMIT 1";

    /**
     * Retires the UNCONFIRMED marker(s) of ONE resolved ambiguous write:
     * flipped by retireSeqPendingMarker. The DELETE touches only
     * unconfirmed = 1 rows of that last_id - the plain reservation row appended
     * before every create (and every other id ever reserved) stays, so MAX(last_id) and
     * with it the id watermark never fall.
     */
    private static final String DELETE_PENDING_SEQ_MARKER_SQL = "DELETE FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql_hash` = ${planSqlHash} AND `unconfirmed` = 1"
            + " AND `last_id` = ${lastId}";

    /**
     * The consistency fence of the paginated snapshot read (see
     * readStableSnapshot): the id high-water mark, the row count and the newest
     * update_time of the WHOLE table, read before AND after the page loop. An internal
     * paginated snapshot issues one SELECT per page and the internal table has no
     * long-lived read view, so a CREATE / ALTER / DROP committing between two pages would
     * otherwise be merged into a state no single point in time ever had (the reviewer's
     * example: a follower reads ENABLED low-id A on page 1, the master drops A and creates
     * high-id B before page 2 - the published cache then contains BOTH, SHOW reports the
     * completed DROP and matching replays A until the next refresh). MAX(id) catches every
     * CREATE, COUNT(*) catches a pure DROP, MAX(update_time) catches a status flip (SECOND
     * precision: a flip within the same second as the previous write remains a residual
     * window, closed by the refresh daemon).
     */
    private static final String SELECT_SNAPSHOT_FENCE_SQL = "SELECT MAX(`id`), COUNT(*),"
            + " MAX(`update_time`) FROM " + SPM_BASELINES_TABLE;

    /**
     * Reads every durable row carrying ONE id - the collision probe of a create (see
     * createBaseline): the table is DUPLICATE KEY(id), so an out-of-band writer
     * or a second master that started from the same watermark can have inserted a
     * DIFFERENT baseline under the id this create just allocated. Snapshot loading would
     * later collapse the two rows nondeterministically (pickDurableWinner), and dropping
     * the visible row could expose the other; the collision is therefore detected and
     * resolved at create time.
     */
    private static final String SELECT_BY_ID_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest` FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id}";

    /**
     * Durable-key lookup used by the create-time dedup: the in-memory index can be stale
     * (a follower that loaded=true before becoming master missed rows written afterwards),
     * so the authoritative duplicate check reads the (bind_sql_digest, plan_sql) key back
     * from the table before a new row is inserted.
     */
    private static final String SELECT_BY_KEY_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest` FROM " + SPM_BASELINES_TABLE
            + " WHERE `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'";

    private static final String INSERT_SQL = "INSERT INTO " + SPM_BASELINES_TABLE
            + " VALUES (${id}, '${bindSql}', '${bindSqlDigest}', ${bindSqlHash},"
            + " '${planSql}', '${queryId}', ${cost}, ${queryTimeMs}, '${source}', '${status}',"
            + " '${createTime}', '${updateTime}', ${sqlMode}, ${planSqlMode}, ${planFrozen},"
            + " '${schemaFingerprint}', '${planSqlDigest}')";

    /**
     * Deletes one row by id AND content key (bind_sql_digest + plan_sql): a delete
     * triggered by a stale id must never remove a row that happens to carry the same id
     * but different content (e.g. after the table was edited out of band, or the id was
     * rewound by an FE started with stale metadata).
     */
    private static final String DELETE_BY_IDENTITY_SQL = "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql` = '${planSql}'";

    /** Removes one baseline row by id + its previous status (status UPDATE support). */
    private static final String DELETE_BY_ID_AND_STATUS_SQL = "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `status` = '${status}'"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'";

    /**
     * INSERT half of a STATUS FLIP, CONDITIONAL on the previous-status row: the SELECT
     * yields one row only while a durable row still carries (id, previousStatus), so a
     * baseline that a concurrent DROP removed while the ALTER was in flight (or stalled
     * before its INSERT) is never resurrected - the ALTER then fails retryably instead
     * of publishing an ACTIVE status the completed DROP had already reported removed.
     *
     * The condition matches the CACHED row's IDENTITY as well: a leader
     * handoff can leave this FE caching B/id N while a delayed old-leader INSERT made
     * A/id N the durable winner. Matching (id, previousStatus) alone then wrote B's
     * cached SQL with a later timestamp - and the following identity-scoped DELETE
     * cannot remove A, so a reload replaced the durable baseline with stale B while
     * ALTER reported success. Requiring (id, previousStatus, bind_sql_digest, plan_sql)
     * makes the statement write NOTHING against a foreign incarnation; the caller then
     * reconciles its cache with the readable durable winner and reports the conflict as
     * retryable (or re-reads it as the completed flip).
     */
    private static final String INSERT_IF_PREVIOUS_STATUS_SQL = "INSERT INTO "
            + SPM_BASELINES_TABLE + " SELECT ${id}, '${bindSql}', '${bindSqlDigest}',"
            + " ${bindSqlHash}, '${planSql}', '${queryId}', ${cost}, ${queryTimeMs},"
            + " '${source}', '${status}', '${createTime}', '${updateTime}', ${sqlMode},"
            + " ${planSqlMode}, ${planFrozen}, '${schemaFingerprint}', '${planSqlDigest}' FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id} AND `status` = '${previousStatus}'"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " LIMIT 1";

    /** Reconciliation read of the ambiguous status-update path: how many durable rows
     *  currently carry (id, status). */
    private static final String COUNT_BY_ID_AND_STATUS_SQL = "SELECT COUNT(*) FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id} AND `status` = '${status}'";

    /**
     * The newest stored update_time of the WHOLE table: the status-flip bump advances
     * the new row past this value (see updateStatus), which is what makes every flip move
     * the SELECT_SNAPSHOT_FENCE_SQL fence.
     */
    private static final String SELECT_MAX_UPDATE_TIME_SQL = "SELECT MAX(`update_time`) FROM "
            + SPM_BASELINES_TABLE;

    /**
     * DATETIME column format (internal table create_time / update_time). The columns are
     * zone-free DATETIME, so they are written and read in UTC: the stored value denotes
     * the SAME instant on every FE, in every host zone and across DST changes (see
     * toTs).
     */
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Message of the leadership fence (see assertLeaderForWrite). */
    private static final String NO_LONGER_MASTER = "SPM baseline write refused: this FE is no"
            + " longer the master (retry on the new leader)";

    /**
     * Statement timeout (seconds) of the SPM internal-table reads. The temporary context
     * StatisticsUtil builds otherwise inherits the analyze timeout (12h by default): an
     * unavailable tablet / BE would stall the caller (master readiness, background load)
     * far beyond the advertised SPM budget.
     */
    private static final int INTERNAL_QUERY_TIMEOUT_SECONDS = 10;
    /**
     * Statement timeout (seconds) of the SPM internal-table WRITES (INSERT / DELETE).
     * createBaseline runs synchronously inside the single capture cycle under writerLock,
     * so a stalled write with the default analyze timeout (43,200 s) would delay every
     * later capture and every global baseline DDL for hours. A timeout is reconciled
     * against the durable table (the statement may have committed before the error was
     * reported).
     */
    private static final int BASELINE_WRITE_TIMEOUT_SECONDS = 10;

    /**
     * Bounded read-back confirmation of a reported-successful durable write (see
     * confirmInsertVisible): the attempts times the retry delay must cover a
     * normal publication lag; a longer invisible window fails the write retryably
     * instead of publishing an id no read can ever confirm.
     */
    private static final int BASELINE_VISIBILITY_ATTEMPTS = 5;

    /** Delay between two visibility probes of a reported-successful write (ms). */
    private static final long BASELINE_VISIBILITY_RETRY_MILLIS = 200L;

    /**
     * Bounded confirmation attempts of a forwarded GLOBAL DDL's durable outcome (see
     * ForwardedDdlExpectation): a short publication lag must converge, an
     * outcome that stays invisible fails CLOSED (never republish the pre-DDL row).
     */
    private static final int FORWARDED_DDL_CONFIRM_ATTEMPTS = BASELINE_VISIBILITY_ATTEMPTS;

    /** How long a management caller waits for an in-flight background load. */
    private static final long MANAGEMENT_LOAD_WAIT_MILLIS = 5_000L;

    /**
     * Bounded id-collision retries of one create (see createBaseline): reaching the cap
     * means at least MAX_ID_COLLISION_RETRIES competing writes landed on every id this
     * FE allocated - a retryable condition, never a silent success.
     */
    private static final int MAX_ID_COLLISION_RETRIES = 8;

    /**
     * Admission bound of pendingCreates: an unconfirmed create is a rare (and
     * manually retried) event, so a small registry is enough. When the bound is reached a
     * NEW create fails retryably BEFORE it writes - an unresolved identity
     * is never dropped, because evicting one re-exposes exactly that write to a duplicate
     * id on a retry.
     */
    private static final int MAX_PENDING_CREATES = 64;

    /**
     * How long the DURABLE unconfirmed-create fence holds a retry of the same baseline
     * (resolveDurablePendingCreate): the fence must survive a leader handoff and
     * the retry's own latency, while a row that never becomes readable must not block a
     * legitimate re-create of the key for long (the committed-row publication lag is
     * normally seconds; the previous tests of this class resolve within milliseconds).
     * Deliberately SHORTER than PENDING_CREATE_FENCE_MILLIS: the in-memory
     * registry knows the exact age of the SAME FE's attempt, the durable record only a
     * likely-dead write's instant. It bounds the fence of BOTH durable identity records
     * - the explicit unconfirmed marker and the plain reservation row appended before
     * every baseline write; when it elapses with the row still unreadable,
     * the identity is condemned with a tombstone before a fresh id is allocated
     */
    private static final long DURABLE_PENDING_CREATE_FENCE_MILLIS = 5 * 60 * 1000L;

    /**
     * How long an unconfirmed create fences a retry of the same baseline (see
     * pendingCreates): the committed row is normally readable within the
     * visibility-confirmation budget, and a write that never becomes readable after this
     * bound is treated as LOST - fencing longer would refuse every retry of that key
     * forever. Same bound as the audit loader's Publish-Timeout fence.
     */
    private static final long PENDING_CREATE_FENCE_MILLIS = 30 * 60 * 1000L;

    /**
     * How long a COMPLETED local mutation (or a forwarded GLOBAL DDL known to have
     * committed on the master) fences the persisted snapshots that contradict it
     * the durable write may be committed while its publication
     * still lags every local read, so a daemon snapshot / SHOW read that still returns
     * the pre-mutation row must not republish it - matching would keep serving a baseline
     * the user just dropped or disabled (and a stale snapshot must not hide a
     * just-enabled one either). The fence is bounded like the create-side fence: a write
     * that never becomes visible after this bound is treated as LOST and the persisted
     * state wins again.
     */
    private static final long PENDING_MUTATION_FENCE_MILLIS = 5 * 60 * 1000L;

    /**
     * One drop tombstone this FE could not append durably yet (see
     * writeDroppedMarker): the identity of the removed row, kept so the scoped tombstone
     * read keeps hiding it and the append is retried.
     */
    private static final class PendingDropMarker {
        final String bindSqlDigest;
        final long planSqlHash;

        PendingDropMarker(String bindSqlDigest, long planSqlHash) {
            this.bindSqlDigest = bindSqlDigest;
            this.planSqlHash = planSqlHash;
        }
    }

    /**
     * Drop tombstones whose durable append has not succeeded yet, id -> identity (see
     * writeDroppedMarker). Process-local by nature: another FE never saw the failed write,
     * but it also lost this tombstone's delayed-commit protection entirely, which is the
     * pre-existing exposure the retry shrinks.
     */
    private static final Map<Long, PendingDropMarker> pendingDroppedMarkers =
            new ConcurrentHashMap<>();

    // ==================== priority ordering ====================

    /**
     * Candidate baseline priority ordering:
     *
     * 1. both have queryMs (>= 0) -> the one with shorter time wins
     * 2. neither has queryMs (-1) -> the one with lower cost wins
     * 3. only one has queryMs -> the one without time wins (manual baselines win over
     *    auto-captured ones)
     *
     * queryMs = -1 means no actual execution time (manually created); >= 0 means known
     * execution stats (0 is a valid measured sub-millisecond time). The comparison is a
     * total order: equal known times compare equal and a known time never compares -1 in
     * both directions.
     */
    private static final Comparator<BaselinePlan> comparator = (o1, o2) -> {
        // -1 is "unknown" (manual create); 0 is a VALID measured time, so "known" is
        // >= 0. compare(0, 0) must be 0 and 0 vs -1 must be ordered consistently, or
        // stream sorting can misorder candidates / throw a comparator-contract error.
        long time1 = o1.getQueryTimeMs();
        long time2 = o2.getQueryTimeMs();
        boolean known1 = time1 >= 0;
        boolean known2 = time2 >= 0;
        if (known1 && known2) {
            return Long.compare(time1, time2);
        } else if (!known1 && !known2) {
            return Double.compare(o1.getCost(), o2.getCost());
        } else {
            // the one with time is ordered last (the one without time wins)
            return known1 ? 1 : -1;
        }
    };

    /** Auto-increment id counter. */
    private final AtomicLong idGenerator = new AtomicLong(1);

    /** Whether the persisted baselines have been loaded from the internal table (or the
     *  internal schema db is disabled). Set by loadFromInternalTable() and
     *  clearForTest(). */
    private volatile boolean loaded = false;

    /**
     * Coalesces the internal-table loads: at most ONE read may be in flight. The query
     * path (ensureLoaded) never blocks on the table - an unavailable tablet / BE used to
     * stall every rewrite attempt for the temporary context's full analyze timeout - it
     * just requests a background load; a failed read keeps loaded=false and is
     * retried by the next access / refresh cycle.
     */
    private final AtomicBoolean loadInProgress = new AtomicBoolean(false);

    /** Notified when an in-flight load finishes (management callers wait on it). */
    private final Object loadMonitor = new Object();

    /** Whether CRUD writes to the internal table (disabled by clearForTest for tests). */
    private volatile boolean persistToTable = true;

    /**
     * Guards the in-memory store: baselines / hashIndex /
     * stateVersion and the loaded state machine. Only accesses to those
     * structures are critical sections - internal-table I/O and the CPU work derived
     * from a snapshot (digest filter, priority sort) run OUTSIDE the lock. Rewrite
     * lookups (hasBaselines + findCandidateBaselines on every query) take the read lock
     * and therefore never serialize on each other; create / drop / status / load /
     * refresh take the write lock for their in-memory validation / publication only.
     */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /**
     * Serializes WRITERS (create / drop / status) against each other for their
     * internal-table read-modify-write sequences. Deliberately separate from stateLock:
     * every SPM query takes stateLock.readLock() in hasBaselines / findCandidateBaselines
     * BEFORE its rewrite timeout can help, so ONE slow persistence statement (each
     * auto-capture candidate issues one) would stall planning for every such query on the
     * FE. The state lock only protects in-memory validation and publication - see the
     * two-phase structure of createBaseline / dropBaseline /
     * updateStatus. Readers never touch this lock.
     */
    private final Object writerLock = new Object();

    /**
     * Monotonic version of the in-memory state, bumped by every local mutation (create /
     * drop / status change / load) and by every refresh that changed something. The periodic
     * refresh compares it to skip a table snapshot whose read overlapped a local mutation
     * (see refreshFromInternalTable). Guarded by stateLock.
     */
    private long stateVersion = 0;

    /**
     * The largest durable id this store has EVER seen - the load snapshot's MAX(id) and
     * every locally published row. The table allocates ids upward only, so a table whose
     * MAX(id) is not above this value has no row this store does not know: the
     * create-path key dedup can then be answered from the in-memory key index instead of
     * scanning the whole table (see readPersistedRowsForCreate).
     */
    private volatile long maxPersistedIdSeen = 0;

    /**
     * Creates whose INSERT reported SUCCESS but whose row was not READABLE yet (see
     * createBaseline): the id is consumed but invisible, so neither MAX(id) nor
     * the durable-key read can see it. A RETRY of the same CREATE must not allocate a
     * SECOND id for the same baseline - both rows would later publish under different ids
     * and dropping the id the client was told about would leave the other ACTIVE. The
     * retry ADOPTS the remembered row once it becomes readable and is DEFERRED (bounded by
     * PENDING_CREATE_FENCE_MILLIS) until then. Guarded by writerLock (every
     * access runs inside it). The registry is never SHRUNK by eviction:
     * createBaseline's admission fence refuses a new write while it is full, so an
     * unresolved identity always stays recorded until it becomes readable or its fence
     * expires. The durable reservation rows of SPM_BASELINES_SEQ_TABLE carry the
     * same identity for the cross-FE case (the retry runs on ANOTHER FE whose in-memory
     * registry is empty - see resolveDurablePendingCreate).
     */
    private final List<PendingCreate> pendingCreates = new ArrayList<>();

    /** One committed-but-unpublished create (see pendingCreates). */
    private static final class PendingCreate {
        final BaselinePlan plan;
        final long since;

        PendingCreate(BaselinePlan plan, long since) {
            this.plan = plan;
            this.since = since;
        }
    }

    /**
     * Completed (or attempted) mutations whose durable outcome is not visible to this FE's
     * own reads yet: id -> the state the durable table must reach (see
     * PendingMutationFence). While a fence is active, a persisted snapshot that
     * CONTRADICTS it is masked - its id is skipped by the refresh diff and the local
     * post-mutation state stays - so matching never republishes a baseline the user just
     * dropped or disabled, and a stale row never overwrites a committed flip. Recorded by
     * a completed DROP / status flip and their ambiguous-write failures, and by a
     * forwarded GLOBAL DDL whose expected outcome did not become
     * visible on this FE within its budget. Removed when a snapshot
     * satisfies it or after PENDING_MUTATION_FENCE_MILLIS (write presumed lost).
     * Never cleared by invalidatePublishedStore: the fences describe writes THIS
     * FE committed / observed, exactly like pendingCreates.
     */
    private final Map<Long, PendingMutationFence> pendingMutationFences =
            new ConcurrentHashMap<>();

    /**
     * One pending durable outcome of a local mutation (see pendingMutationFences).
     *
     * An ABSENCE fence may travel with the removed row's identity. The
     * DELETE that produced it is unconfirmed (it may have failed BEFORE commit), so NO
     * deletion marker may be written yet - a marker for a still-live row would make every
     * later load hide that row and re-issue the delete, silently completing a DROP that
     * reported failure. The identity is kept here instead: the marker is appended only
     * once a readable snapshot PROVES the row is gone (see
     * resolvePendingMutationFences).
     */
    private static final class PendingMutationFence {
        private final BaselineStatus expectedStatus; // null = the row must be absent
        private final long sinceMillis;
        private final long attemptedUpdateTimeMillis; // 0 = unknown (delete / forwarded DDL)
        private final BaselinePlan droppedIdentity; // non-null = defer the tombstone
        /**
         * Whether the fence describes a write this FE PROVED committed (the conditional
         * INSERT reported its row, or the committed-write probe matched): such a fence
         * must never be dropped by the age heuristic - its durable row can stay
         * unreadable past every bound (a long publication lag), and unmasking the id
         * then republishes the pre-mutation snapshot while the committed write is still
         * the durable winner. Only an outcome the snapshot shows removes it. An UNPROVEN
         * fence (ambiguous write / failed forwarded DDL) still expires: after the bound
         * the write is presumed lost and the persisted state wins again.
         */
        private final boolean confirmed;

        PendingMutationFence(BaselineStatus expectedStatus, long sinceMillis,
                long attemptedUpdateTimeMillis) {
            this(expectedStatus, sinceMillis, attemptedUpdateTimeMillis, null, false);
        }

        PendingMutationFence(BaselineStatus expectedStatus, long sinceMillis,
                long attemptedUpdateTimeMillis, BaselinePlan droppedIdentity) {
            this(expectedStatus, sinceMillis, attemptedUpdateTimeMillis, droppedIdentity, false);
        }

        PendingMutationFence(BaselineStatus expectedStatus, long sinceMillis,
                long attemptedUpdateTimeMillis, BaselinePlan droppedIdentity,
                boolean confirmed) {
            this.expectedStatus = expectedStatus;
            this.sinceMillis = sinceMillis;
            this.attemptedUpdateTimeMillis = attemptedUpdateTimeMillis;
            this.droppedIdentity = droppedIdentity;
            this.confirmed = confirmed;
        }

        /**
         * Whether the snapshot row shows the expected durable outcome: absent for an
         * absent fence, the expected status, or a strictly LATER write than the attempt
         * (any later row supersedes this fence).
         */
        boolean isSatisfiedBy(BaselinePlan row) {
            if (expectedStatus == null) {
                return row == null;
            }
            if (row == null) {
                return false;
            }
            if (row.getStatus() == expectedStatus) {
                return true;
            }
            return attemptedUpdateTimeMillis > 0
                    && row.getUpdateTime() >= attemptedUpdateTimeMillis;
        }
    }

    /**
     * One identity-carrying id reservation (see
     * IdAllocatorStoreForTest#pendingSeqReservation): the id, the instant its
     * creation reserved it and what the row REPRESENTS - an ambiguous-create marker
     * (unconfirmed), a tombstone (dropped) or the plain pre-INSERT
     * reservation of every create. The two-argument constructor keeps the
     * marker semantics for simulators that model only the ambiguous marker.
     */
    @VisibleForTesting
    static final class SeqReservation {
        final long id;
        final long reserveTimeMs;
        final boolean unconfirmed;
        final boolean dropped;

        SeqReservation(long id, long reserveTimeMs) {
            this(id, reserveTimeMs, true, false);
        }

        SeqReservation(long id, long reserveTimeMs, boolean unconfirmed, boolean dropped) {
            this.id = id;
            this.reserveTimeMs = reserveTimeMs;
            this.unconfirmed = unconfirmed;
            this.dropped = dropped;
        }
    }

    /** id -> BaselinePlan (Phase 1 in-memory storage). */
    private final Map<Long, BaselinePlan> baselines = new HashMap<>();

    /** bindSqlHash -> baseline id list (Level 1 coarse filter index). */
    private final Map<Long, List<Long>> hashIndex = new HashMap<>();

    /**
     * Bumped by every invalidation of the published store (the promotion reload). A load
     * that STARTED before an invalidation must never republish its snapshot: that
     * snapshot may still contain a row the concurrent DROP deleted - the DROP then finds
     * no map entry (the maps were cleared) and skips its stateVersion bump, so the
     * version guard alone cannot reject the stale snapshot. Bumped under writerLock (see
     * invalidatePublishedStore), read without a lock.
     */
    private final AtomicLong storeGeneration = new AtomicLong();

    private BaselineManager() {
    }

    public static BaselineManager getInstance() {
        return INSTANCE;
    }

    /**
     * Candidate baseline ordering shared with the SESSION-scope store: it applies the
     * exact same priority rules as this manager (see the comparator field).
     * Public so tests can verify the total-order contract directly.
     *
     * @param o1 first baseline
     * @param o2 second baseline
     * @return the comparison result
     */
    public static int compareCandidates(BaselinePlan o1, BaselinePlan o2) {
        return comparator.compare(o1, o2);
    }

    // ==================== CRUD ====================

    /**
     * Creates a baseline.
     *
     * Duplicate detection: identical (bindSqlHash, bindSqlDigest) with the exact same
     * planSql is skipped; a different planSql is allowed to coexist (one query can have
     * multiple plan baselines).
     *
     * @param plan the baseline (id is assigned here if unset)
     * @return the id of the created baseline (or the existing id when duplicated)
     */
    public long createBaseline(BaselinePlan plan) {
        ensureLoadedOrThrow();
        // Two-phase create. I/O (watermark read, durable-key dedup, repair deletes, INSERT)
        // runs under writerLock but NEVER under stateLock: the state lock only protects the
        // in-memory duplicate validation (phase 1) and the publication (phase 2). Holding
        // the state lock across the I/O would stall every SPM query's rewrite lookup.
        synchronized (writerLock) {
            // Resolve the remembered write of THIS key BEFORE the capacity check
            // a full registry used to reject even a retry of one of its
            // OWN keys before resolvePendingCreate could adopt the row that had become
            // readable and retire the record - and nothing else ever reaps the registry,
            // so the FE rejected every GLOBAL CREATE until restart / leadership change.
            PendingResolution resolved = resolvePendingCreate(plan);
            if (resolved.adoptedId != null) {
                return resolved.adoptedId;
            }
            // Admission fence BEFORE any write: when the registry of
            // committed-but-unpublished creates is full, this CREATE must fail before it
            // can write, not after. Evicting the OLDEST pending record used to let a
            // retry of that first key see its reserved sequence id but neither its row
            // nor a pending entry - it then allocated a new id and both rows later
            // published ENABLED. Refusing admission keeps every unresolved identity
            // recorded until it becomes readable (or its fence expires). A full registry
            // is RECONCILED first: records whose row became readable (or
            // whose fence expired) stop fencing, so only genuinely unresolved identities
            // can refuse the write.
            if (pendingCreates.size() >= MAX_PENDING_CREATES) {
                reconcilePendingCreates();
            }
            if (pendingCreates.size() >= MAX_PENDING_CREATES) {
                throw new IllegalStateException("SPM cannot create baseline: "
                        + pendingCreates.size() + " previously committed writes of other"
                        + " baselines are still awaiting publication (the pending-create"
                        + " registry is full); retry the statement later");
            }
            // Id watermark first (see the class javadoc "Id source"): the generator must be
            // advanced past the persistence layer BEFORE an id is handed out. A create whose
            // watermark read fails fails visibly and allocates nothing, instead of silently
            // colliding with a row written by a newer master.
            final long watermark = readPersistedWatermark();
            // Durable half of the same fence: the in-memory registry above
            // is per-FE, so a retry that runs on the new master after a handoff (or after
            // a restart) finds no record here although the original write COMMITTED - the
            // unconfirmed marker of the sequence table carries its identity and defers the
            // retry until the row is readable (then adopts the reserved id). A key the
            // in-memory registry already resolved must skip it: the marker cannot tell
            // "still publishing" from "already retired by this FE".
            if (!resolved.handled) {
                Long durableAdoptedId = resolveDurablePendingCreate(plan);
                if (durableAdoptedId != null) {
                    return durableAdoptedId;
                }
            }
            // Phase 1: duplicate validation against the in-memory index (read lock). A
            // duplicate must agree on the SCHEMA FINGERPRINT as well: after
            // ALTER TABLE t ADD COLUMN extra the stored fingerprint goes stale and
            // SPMPlanner skips the old baseline even for a still-matching bind, while a
            // repeated CREATE produces the same digest / planSql with a NEW fingerprint -
            // returning the old id would leave the user unable to recreate a usable
            // baseline without dropping it first. A stale row is retired once the
            // replacement row is durable.
            BaselinePlan duplicate = null;
            List<BaselinePlan> staleCacheRows = new ArrayList<>();
            stateLock.readLock().lock();
            try {
                if (plan.getBindSqlHash() != 0) {
                    for (BaselinePlan existing : findByHash(plan.getBindSqlHash())) {
                        if (!existing.getBindSqlDigest().equals(plan.getBindSqlDigest())
                                || !existing.getPlanSql().equals(plan.getPlanSql())) {
                            continue;
                        }
                        if (Objects.equals(existing.getSchemaFingerprint(),
                                plan.getSchemaFingerprint())) {
                            duplicate = existing;
                        } else {
                            staleCacheRows.add(existing);
                        }
                    }
                }
            } finally {
                stateLock.readLock().unlock();
            }
            if (duplicate != null) {
                // The duplicate is only real while it still EXISTS durably: right after a
                // promotion (Env.transferToMaster sets isReady BEFORE the baseline reload
                // finishes) this cache can hold a row the previous master already
                // dropped. Returning its id would report success for a baseline that is
                // not there.
                if (isTombstonedIdentity(duplicate)) {
                    // The identity carries a DROP TOMBSTONE - the user's DROP
                    // completed and the row only lags behind its own delete. Returning the
                    // cached id would report CREATE success for a baseline that disappears
                    // the moment the DELETE becomes readable.
                    LOG.warn("SPM baseline create: the cached duplicate {} carries a DROP"
                            + " tombstone (its row only lags the delete); creating a fresh"
                            + " row", duplicate.getId());
                    staleCacheRows.add(duplicate);
                } else if (!persistenceEnabled() && statusProtocolStoreForTest == null
                        && idAllocatorStoreForTest == null) {
                    return duplicate.getId();
                } else if (idAllocatorStoreForTest == null && statusProtocolStoreForTest != null) {
                    // count-only seam: it cannot compare the durable identity
                    if (durableRowCount(duplicate.getId(), duplicate.getStatus()) > 0) {
                        return duplicate.getId(); // exact duplicate -> skip
                    }
                } else if (probeDurableRow(duplicate.getId(), duplicate.getBindSqlDigest(),
                        duplicate.getPlanSql(), duplicate.getStatus(),
                        duplicate.getSchemaFingerprint()) == DurablePresence.PRESENT) {
                    // the durable row must be the SAME incarnation (id + key + status +
                    // fingerprint): a REUSED id carrying another baseline satisfied the
                    // old id+status probe, and returning it handed the user an id whose
                    // row is a different baseline
                    return duplicate.getId(); // exact duplicate -> skip
                } else {
                    LOG.warn("SPM baseline create: the cached duplicate {} is gone durably"
                            + " (promotion window); creating a fresh row", duplicate.getId());
                    staleCacheRows.add(duplicate);
                }
            }
            // Durable-key check: the in-memory index can be stale (e.g. a follower that
            // loaded=true before becoming master missed rows written after its last
            // refresh). Without this check the INSERT below would REPLACE a durable
            // baseline - changing its id and, on an INSERT failure, losing the old row.
            // A durable duplicate returns its id and is adopted into memory instead;
            // extra same-key rows (partial-state survivors) are repaired away idempotently.
            // writerLock keeps another writer's INSERT/DELETE pair out of this window.
            if (persistenceEnabled() && plan.getBindSqlDigest() != null) {
                // The KEY read is a point read too - a row revived after its
                // own DROP must not be adopted (and must be repaired away) here either
                List<BaselinePlan> durable = filterTombstonedDurableRows(
                        readPersistedRowsForCreate(plan, watermark));
                if (!durable.isEmpty()) {
                    List<BaselinePlan> sameFingerprint = new ArrayList<>();
                    List<BaselinePlan> staleRows = new ArrayList<>();
                    for (BaselinePlan row : durable) {
                        if (Objects.equals(row.getSchemaFingerprint(),
                                plan.getSchemaFingerprint())) {
                            sameFingerprint.add(row);
                        } else {
                            staleRows.add(row);
                        }
                    }
                    // Rows under an OLD fingerprint are unreachable for matching
                    // (schemaFingerprintBindSideContained / verifyReplayMetadata reject
                    // them) and would only compete with the row this CREATE is about to
                    // write: retire them (best effort) in favour of the usable version.
                    for (BaselinePlan row : staleRows) {
                        try {
                            persistDeleteByIdentity(row);
                            LOG.info("SPM retired the stale baseline row {} (schema fingerprint"
                                    + " changed)", row.getId());
                        } catch (RuntimeException e) {
                            LOG.warn("SPM failed to retire the stale baseline row (id={}): {}",
                                    row.getId(), e.getMessage());
                        }
                    }
                    if (!sameFingerprint.isEmpty()) {
                        BaselinePlan winner = sameFingerprint.get(0);
                        for (int i = 1; i < sameFingerprint.size(); i++) {
                            winner = pickDurableWinner(winner, sameFingerprint.get(i));
                        }
                        for (BaselinePlan row : sameFingerprint) {
                            if (row.getId() != winner.getId()) {
                                try {
                                    persistDeleteByIdentity(row);
                                } catch (RuntimeException e) {
                                    // best-effort repair: the key state is deterministic either
                                    // way (the winner above), the leftover row is warned below
                                    LOG.warn("SPM failed to repair a duplicate baseline row (id={}): {}",
                                            row.getId(), e.getMessage());
                                }
                            }
                        }
                        retireStaleInMemory(staleCacheRows);
                        publishBaseline(winner);
                        LOG.info("SPM baseline create deduplicated against the durable key: id={}",
                                winner.getId());
                        return winner.getId();
                    }
                    // only stale rows existed: the fall-through below retires their
                    // in-memory copies and allocates a NEW row (same digest / planSql,
                    // new fingerprint)
                }
            }
            // no usable duplicate survived (a stale-fingerprint row is retired instead of
            // being returned): drop the stale in-memory copies and allocate a fresh row
            retireStaleInMemory(staleCacheRows);
            // every baseline owned by the global manager is GLOBAL-scope (same value the
            // rows loaded from the internal table and the auto capturer get); the id stays
            // in the GLOBAL range [1, 2^62), so BaselineScope.ofId(id) is exact
            plan.setScope(BaselineScope.GLOBAL);
            if (watermark >= idGenerator.get()) {
                // watermark + 1 is an id the table has never seen; the generator is
                // monotonic and the watermark was read at the top of this create, so
                // concurrent local creates can only jump the generator further up, never
                // below a watermark any create has observed
                idGenerator.set(watermark + 1);
            }
            for (int attempt = 0; attempt < MAX_ID_COLLISION_RETRIES; attempt++) {
                // Fence the write with the CURRENT leadership (see assertLeaderForWrite):
                // an in-flight forwarded CREATE / capture dispatched by an OLD master can
                // reach this point after the handoff.
                assertLeaderForWrite();
                long id = idGenerator.getAndIncrement();
                plan.setId(id);
                long now = System.currentTimeMillis();
                plan.setCreateTime(now);
                plan.setUpdateTime(now);
                // reserve the id durably BEFORE the row: an id that was handed out must
                // never be handed out again after a DROP, or a delayed DROP-by-id retry
                // removes a DIFFERENT baseline (see reserveAllocatedId / the class javadoc
                // "Id source"). A crash between the two leaves a harmless GAP.
                //
                // Re-check the leadership at the RESERVATION too: this FE
                // can pass the check above and pause here, the new master then reads the
                // same MAX(id) and reserves id N for a DIFFERENT key - the reservation
                // (the id/key ownership record) must never be written by a demoted FE,
                // whose statement would even FORWARD to the new master.
                assertLeaderForWrite();
                reserveAllocatedId(plan);
                // persist first so a persist failure leaves the in-memory state untouched
                // and fails the DDL visibly; no same-key row can exist here (the
                // durable-key check above returned any), so the INSERT cannot overwrite an
                // existing baseline.
                //
                // Re-check the leadership immediately before the ROW write as well: the
                // pause between the reservation and this INSERT let a promoted FE create
                // the same key under N+1, and this FE's internal INSERT (executed on the
                // new master after forwarding) then left TWO enabled baselines for the
                // key - each by-id collision probe sees only its own id.
                assertLeaderForWrite();
                try {
                    persistInsert(plan);
                } catch (UnconfirmedInsertException unconfirmed) {
                    rememberPendingCreate(plan);
                    // persist the SAME identity durably: a retry on ANOTHER
                    // FE has no in-memory registry to consult
                    markSeqPendingUnconfirmed(plan);
                    throw unconfirmed;
                }
                if (idAllocatorStoreForTest == null && !persistenceEnabled()) {
                    // Phase 2: publish (the only state-lock section of a create).
                    publishBaseline(plan);
                    return id;
                }
                // Cluster-wide collision probe: MAX(id) only orders ids, it does not
                // RESERVE them - a second master (or an out-of-band writer) that read the
                // same watermark can have inserted a DIFFERENT baseline under this id,
                // and spm_baselines is DUPLICATE KEY(id), so both rows would survive and
                // snapshot loading would later collapse them nondeterministically.
                BaselinePlan foreign = readForeignRowWithSameId(id, plan);
                if (foreign == null) {
                    publishBaseline(plan);
                    return id;
                }
                if (winsIdCollision(plan, foreign)) {
                    // deterministic winner keeps the id; repair the competing row away so
                    // every restart / refresh converges on this baseline. The repair is
                    // NOT best-effort here: the competing row is a DIFFERENT baseline some
                    // other caller was already promised under this id (the new master can
                    // complete its own CREATE at the same MAX(id)+1 while this demoted
                    // create resumes), so returning the id with both rows alive would let
                    // a reload pick the OTHER incarnation - this caller's baseline would
                    // silently not exist. The repair is leadership-fenced, so a demotion
                    // mid-create fails the CREATE retryably instead.
                    persistDeleteByIdentity(foreign);
                    LOG.warn("SPM baseline create kept id {} on collision (digest {});"
                                    + " removed the competing row (digest {})",
                            id, plan.getBindSqlDigest(), foreign.getBindSqlDigest());
                    publishBaseline(plan);
                    return id;
                }
                // This create lost the deterministic tie-break: take its own row back and
                // allocate a fresh id above the (re-read) watermark; the other master's
                // row stays untouched.
                LOG.warn("SPM baseline create yielded a contested id {} (competing digest {});"
                        + " re-allocating", id, foreign.getBindSqlDigest());
                persistDeleteByIdentity(plan);
                long freshWatermark = readPersistedWatermark();
                if (freshWatermark >= idGenerator.get()) {
                    idGenerator.set(freshWatermark + 1);
                }
            }
            throw new IllegalStateException("SPM baseline create failed: an id collision could"
                    + " not be resolved after " + MAX_ID_COLLISION_RETRIES
                    + " attempts (retry the CREATE)");
        }
    }

    /**
     * Returns a durable row carrying the given id with a DIFFERENT identity than the row
     * this create just inserted, or null when the id is exclusively owned. A read failure
     * is rethrown as retryable: the retried create adopts its OWN already-inserted row
     * through the durable-key dedup above, so the retry is idempotent and never
     * duplicates.
     *
     * @param id  the just-inserted id
     * @param own the row this create inserted
     * @return the competing row, or null
     */
    private static BaselinePlan readForeignRowWithSameId(long id, BaselinePlan own) {
        if (idAllocatorStoreForTest != null) {
            return pickForeignRow(idAllocatorStoreForTest.readById(id), own);
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        try {
            List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_BY_ID_SQL, params,
                    INTERNAL_QUERY_TIMEOUT_SECONDS);
            List<BaselinePlan> parsed = new ArrayList<>();
            for (ResultRow row : rows) {
                try {
                    parsed.add(parsePersistedRow(row));
                } catch (Throwable t) {
                    LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
                }
            }
            return pickForeignRow(parsed, own);
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM baseline id collision probe failed (retry the CREATE): " + e.getMessage(), e);
        }
    }

    /** Deterministic pick among the rows sharing one id that are NOT this create's own. */
    private static BaselinePlan pickForeignRow(List<BaselinePlan> rows, BaselinePlan own) {
        BaselinePlan foreign = null;
        for (BaselinePlan row : rows) {
            if (sameIdentity(row, own)) {
                continue;
            }
            foreign = foreign == null ? row : pickDurableWinner(foreign, row);
        }
        return foreign;
    }

    /** Whether two rows describe the same baseline (the durable dedup key). */
    private static boolean sameIdentity(BaselinePlan a, BaselinePlan b) {
        return Objects.equals(a.getBindSqlDigest(), b.getBindSqlDigest())
                && Objects.equals(a.getPlanSql(), b.getPlanSql());
    }

    /**
     * For tests: the number of creates whose committed row is still awaiting publication
     * (see pendingCreates).
     *
     * @return the size of the pending-create registry
     */
    @VisibleForTesting
    public int pendingCreateCountForTest() {
        synchronized (writerLock) {
            return pendingCreates.size();
        }
    }

    /**
     * The outcome of resolvePendingCreate: the adopted id (when a remembered
     * write became readable) and whether the IN-MEMORY registry accounted for this key at
     * all (adopted, retired or expired here). A HANDLED key skips the durable pending
     * fence: the durable marker can only say "some write of this key was
     * ambiguous once" - when this FE already retired/resolved that write in memory, the
     * marker must not re-defer the very retry that resolved it.
     */
    private static final class PendingResolution {
        static final PendingResolution NONE = new PendingResolution(null, false);
        static final PendingResolution HANDLED = new PendingResolution(null, true);
        final Long adoptedId;
        final boolean handled;

        PendingResolution(Long adoptedId, boolean handled) {
            this.adoptedId = adoptedId;
            this.handled = handled;
        }
    }

    /**
     * Applies pendingCreates to a CREATE of the same baseline (see the call site
     * in createBaseline): a remembered write that has become READABLE is ADOPTED
     * (its id is published and returned) while a still-invisible write DEFERS the create
     * with a retryable error instead of consuming a second id.
     *
     * The adoption reads the row back by id instead of relying on the durable-key dedup:
     * a failed create never published into this FE's in-memory key index, so the indexed
     * dedup would not see the committed row and would allocate a second id for the same
     * baseline.
     *
     * @param plan the CREATE's baseline
     * @return the resolution (see PendingResolution)
     */
    private PendingResolution resolvePendingCreate(BaselinePlan plan) {
        if (pendingCreates.isEmpty()) {
            return PendingResolution.NONE;
        }
        boolean handled = false;
        Iterator<PendingCreate> iterator = pendingCreates.iterator();
        while (iterator.hasNext()) {
            PendingCreate pending = iterator.next();
            if (!sameIdentity(pending.plan, plan)) {
                continue;
            }
            handled = true;
            if (!durableRowReadable(pending.plan)) {
                if (System.currentTimeMillis() - pending.since <= PENDING_CREATE_FENCE_MILLIS) {
                    throw new IllegalStateException("SPM cannot create baseline "
                            + pending.plan.getId()
                            + ": a previously COMMITTED write of the same baseline is still"
                            + " awaiting publication (its id is consumed); retry the statement");
                }
                // The fence expired: a write that never became readable after this bound
                // is treated as LOST (the same convention as the audit loader's
                // Publish-Timeout fence), but the abandoned identity is
                // CONDEMNED first: if the write was merely invisible and its row publishes
                // later, every load filters it (and repairs it away) instead of publishing
                // a second enabled baseline next to the fresh one. The reserved id stays
                // consumed (the sequence watermark), the fresh id is allocated above it.
                iterator.remove();
                noteDroppedSeqState(pending.plan);
                LOG.warn("SPM pending create of baseline {}: still not readable after {} ms;"
                                + " condemning the identity (any late publication is repaired"
                                + " away) and allocating a fresh id",
                        pending.plan.getId(), PENDING_CREATE_FENCE_MILLIS);
                continue;
            }
            iterator.remove();
            if (!Objects.equals(pending.plan.getSchemaFingerprint(),
                    plan.getSchemaFingerprint())) {
                // The schema changed while the committed write awaited publication (e.g.
                // ALTER TABLE t ADD COLUMN x turned the fingerprint F1 into F2): the
                // pending row is STALE - matching / replay reject it - so adopting it
                // would report success for a baseline that can never be used. Retire it
                // (it is unreachable for matching either way) and fall through: the
                // normal create path allocates a fresh row under the CURRENT fingerprint.
                try {
                    persistDeleteByIdentity(pending.plan);
                } catch (RuntimeException e) {
                    // best effort: the durable-key check of the create below retires the
                    // row as soon as a read sees it
                    LOG.warn("SPM failed to retire the stale pending-create row (id={}): {}",
                            pending.plan.getId(), e.getMessage());
                }
                // the marker described THAT write; with the row retired it must not fence
                // a later retry either
                retireSeqPendingMarker(pending.plan, pending.plan.getId());
                LOG.warn("SPM pending create of baseline {}: its schema fingerprint changed"
                                + " ({} -> {}); replacing the stale committed row",
                        pending.plan.getId(), pending.plan.getSchemaFingerprint(),
                        plan.getSchemaFingerprint());
                continue;
            }
            Long adopted = adoptReadablePendingRow(pending.plan.getId(), plan);
            if (adopted != null) {
                // the resolution RETIRES the durable marker: without this,
                // the marker outlived the adoption and a DROP + immediate re-CREATE of
                // the same bind/plan deferred for the whole marker fence (the probe saw
                // the old marker and the now-absent row)
                retireSeqPendingMarker(pending.plan, pending.plan.getId());
                return new PendingResolution(adopted, true);
            }
            LOG.warn("SPM pending create of baseline {}: the id no longer carries this"
                    + " baseline; allocating a fresh id", pending.plan.getId());
        }
        return handled ? PendingResolution.HANDLED : PendingResolution.NONE;
    }

    /**
     * Adopts the READABLE durable row of one pending create: the row carrying the pending
     * id with THIS baseline's identity is published and its id returned; null when the id
     * no longer carries the identity (the caller falls through / allocates a fresh id).
     * Shared by the in-memory registry (resolvePendingCreate) and the durable
     * reservation fence (resolveDurablePendingCreate).
     *
     * @param pendingId the id the pending write consumed
     * @param plan      the CREATE's baseline
     * @return the adopted id, or null when the id carries no matching row
     */
    private Long adoptReadablePendingRow(long pendingId, BaselinePlan plan) {
        BaselinePlan winner = readableIdentityRow(pendingId, plan);
        if (winner == null) {
            return null;
        }
        publishBaseline(winner);
        LOG.info("SPM pending create of baseline {} adopted from the durable table",
                winner.getId());
        return winner.getId();
    }

    /** The durable winner among the rows of one id carrying THIS baseline's identity. */
    private static BaselinePlan readableIdentityRow(long pendingId, BaselinePlan plan) {
        BaselinePlan winner = null;
        // A row whose identity was DROPPED is not adoptable even when it is
        // readable right now - the drop's own DELETE may simply not be visible yet (see
        // filterTombstonedDurableRows). Returning null makes BOTH adoption paths (the
        // in-memory registry and the durable reservation fence) allocate a fresh row
        // instead of resurrecting the dropped incarnation.
        for (BaselinePlan row : filterTombstonedDurableRows(
                readPersistedParsedById(pendingId))) {
            if (!sameIdentity(row, plan)) {
                continue; // the id carries a DIFFERENT baseline: never adopt it
            }
            winner = winner == null ? row : pickDurableWinner(winner, row);
        }
        return winner;
    }

    /**
     * Retires the records that no longer fence when the admission bound is hit
     * with 64 committed-but-invisible writes the registry refused EVERY
     * new create - including a retry of one of those very keys - although the rows had
     * long become readable, and nothing else ever reaps it. A record whose durable row is
     * READABLE now (a retry of that key finds it through the durable-key dedup, so no id
     * can be lost) or whose fence expired (the write is treated as LOST, exactly like
     * resolvePendingCreate's expiry) is dropped; still-unresolved identities
     * stay recorded.
     */
    private void reconcilePendingCreates() {
        Iterator<PendingCreate> iterator = pendingCreates.iterator();
        while (iterator.hasNext()) {
            PendingCreate pending = iterator.next();
            if (durableRowReadable(pending.plan)) {
                iterator.remove();
                // the record stops fencing; its durable marker must also be retired or a
                // later DROP + re-CREATE of the key would defer on the stale marker

                retireSeqPendingMarker(pending.plan, pending.plan.getId());
                LOG.info("SPM pending create registry: baseline {} became readable; its"
                        + " record no longer fences", pending.plan.getId());
                continue;
            }
            if (System.currentTimeMillis() - pending.since > PENDING_CREATE_FENCE_MILLIS) {
                iterator.remove();
                // Condemn before a fresh id can be allocated - a later
                // publication of this identity must not surface as a second enabled row.
                noteDroppedSeqState(pending.plan);
                LOG.warn("SPM pending create of baseline {}: still not readable after {} ms;"
                                + " condemning the identity (any late publication is repaired"
                                + " away) and retiring the record",
                        pending.plan.getId(), PENDING_CREATE_FENCE_MILLIS);
            }
        }
    }

    /**
     * Remembers a create whose INSERT reported success but is not readable yet (see
     * confirmInsertVisible): the next CREATE of the same baseline must not
     * allocate a second id for it.
     *
     * @param plan the row that was written
     */
    private void rememberPendingCreate(BaselinePlan plan) {
        for (PendingCreate pending : pendingCreates) {
            if (sameIdentity(pending.plan, plan)) {
                return; // already remembered by an earlier attempt
            }
        }
        // NEVER evict an unresolved identity: the admission fence of
        // createBaseline keeps the registry below MAX_PENDING_CREATES before any write, so
        // this only runs past the bound when concurrent creates grew it - the new record
        // is still retained, because evicting an OLDER identity is exactly what could
        // duplicate a baseline on a retry.
        pendingCreates.add(new PendingCreate(plan, System.currentTimeMillis()));
        LOG.warn("SPM baseline create of id {} is committed but not readable yet; a retry will"
                + " adopt it instead of allocating a second id", plan.getId());
    }

    /**
     * Whether the durable row of a pending create is READABLE right now. The probe is
     * IDENTITY-scoped without a status constraint: the row carries the status the create
     * wrote, and an unreadable / unconfirmable answer must defer (never adopt).
     *
     * @param p the pending row
     * @return true when a read sees the row
     */
    private static boolean durableRowReadable(BaselinePlan p) {
        if (durableVisibilityProbeForTest != null) {
            return durableVisibilityProbeForTest.isReadable(p.getId(), p.getStatus());
        }
        if (!persistenceEnabled() && idAllocatorStoreForTest == null
                && statusProtocolStoreForTest == null) {
            return true;
        }
        return probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql())
                == DurablePresence.PRESENT;
    }

    /**
     * Deterministic tie-break of an id collision: every observer sees the same two
     * identities, so ordering by (digest, planSql) makes both masters yield to the SAME
     * winner (the lexicographically smaller identity) - exactly one of the two rows
     * survives, no matter which create runs the probe.
     */
    private static boolean winsIdCollision(BaselinePlan own, BaselinePlan foreign) {
        int digestCompare = String.valueOf(own.getBindSqlDigest())
                .compareTo(String.valueOf(foreign.getBindSqlDigest()));
        if (digestCompare != 0) {
            return digestCompare < 0;
        }
        return String.valueOf(own.getPlanSql())
                .compareTo(String.valueOf(foreign.getPlanSql())) < 0;
    }

    /**
     * Fences a durable write with the CURRENT leadership. An in-flight forwarded CREATE
     * or auto-capture cycle dispatched while this FE was master can reach the write AFTER
     * a handoff moved the master elsewhere (the capture daemon checks isMaster() only
     * once per cycle), and the new master allocates ids from the same MAX(id) with its
     * own process-local generator. Refusing the write from a non-master keeps the
     * allocator single-writer at the moment of the write; the collision probe above
     * covers the unavoidable tail where the demotion lands mid-write.
     */
    private static void assertLeaderForWrite() {
        if (leaderProbeForTest != null) {
            // test seam: the store simulators deliberately bypass the live probe, so this
            // is the only way a unit test can interleave a handoff with an in-flight write
            if (!leaderProbeForTest.getAsBoolean()) {
                throw new IllegalStateException(NO_LONGER_MASTER);
            }
            return;
        }
        if (!persistenceEnabled() || FeConstants.runningUnitTest
                || idAllocatorStoreForTest != null || statusProtocolStoreForTest != null) {
            // simulator stores stand in for the shared table in unit tests; the leader
            // fence guards LIVE writes only
            return;
        }
        if (Env.getCurrentEnv() != null && !Env.getCurrentEnv().isMaster()) {
            throw new IllegalStateException(NO_LONGER_MASTER);
        }
    }

    /**
     * Phase 2 of a writer: publishes a baseline into the in-memory store. No I/O - only
     * the hash index / map / version are touched, under the write lock. Replaces a row a
     * concurrent refresh may have loaded for the same id (its index entry is dropped
     * first, so no id is indexed twice).
     */
    private void publishBaseline(BaselinePlan plan) {
        stateLock.writeLock().lock();
        try {
            maxPersistedIdSeen = Math.max(maxPersistedIdSeen, plan.getId());
            BaselinePlan replaced = baselines.put(plan.getId(), plan);
            if (replaced != null) {
                removeFromHashIndex(replaced);
            }
            addToHashIndex(plan);
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Drops a baseline.
     *
     * @param id the baseline id
     * @return whether the drop succeeded
     */
    public boolean dropBaseline(long id) {
        ensureLoadedOrThrow();
        // Two-phase drop: the DELETE I/O runs under writerLock but NOT under stateLock;
        // the state lock only removes the in-memory entry afterwards.
        synchronized (writerLock) {
            BaselinePlan removed;
            stateLock.readLock().lock();
            try {
                removed = baselines.get(id);
            } finally {
                stateLock.readLock().unlock();
            }
            if (removed == null) {
                // The cache can be STALE inside the promotion reload window (Env.transferToMaster
                // sets isReady BEFORE forceReloadFromInternalTable finishes), and a reload can
                // also have cleared the map between ensureLoadedOrThrow and writerLock: a row
                // created on the PRIOR master may be missing here although it is durable. Confirm
                // the durable table before declaring the baseline absent - a present row is
                // deleted by identity (exactly the DROP the user asked for), and an unconfirmable
                // read surfaces as a retryable failure instead of a false "DROP succeeded" while
                // the durable row stays (DROP BASELINE PLAN reported an error, DROP IF EXISTS a
                // bogus success).
                return dropDurableRowByIdIfAbsentFromCache(id);
            }
            // persist first so a failure keeps both the in-memory state and the table row;
            // the delete is keyed by id + content, so a stale id can never remove an
            // unrelated row that reused the id
            assertLeaderForWrite();
            try {
                persistDeleteByIdentity(removed);
            } catch (RuntimeException e) {
                if (!NO_LONGER_MASTER.equals(e.getMessage())) {
                    // The DELETE may have COMMITTED while its publication lags every
                    // immediate probe: the row the user just asked to
                    // delete must stop matching NOW, and the fence keeps a later daemon
                    // snapshot / SHOW read from resurrecting it until the durable table
                    // shows the outcome (or the fence expires). The failure still
                    // propagates - the client may retry.
                    //
                    // NO deletion marker here. The DELETE may just as well
                    // have failed BEFORE commit (the row is live), and a tombstone would
                    // make every later load treat that live row as deleted - hiding it
                    // and re-issuing the delete, silently completing a DROP that reported
                    // failure. The identity stays on the fence and its tombstone is
                    // appended only once a snapshot proves the row gone (see
                    // resolvePendingMutationFences).
                    removeCachedBaseline(id);
                    recordPendingAbsenceFence(removed);
                }
                throw e;
            }
            // The cached object can predate a promotion reload AND the durable row under
            // this id may be a DIFFERENT incarnation (an id reused after the old row was
            // dropped): DROP is keyed by the user-facing id, so no row of this id may
            // survive - identity-deleting only the stale object would report success
            // while the real row stays and keeps matching later reads.
            try {
                wipeDurableRowsById(id, removed);
            } catch (RuntimeException e) {
                // The IDENTITY delete is CONFIRMED (persistDeleteByIdentity only returns
                // once the row is gone): this cached entry's durable row no longer exists,
                // so it must stop being matchable / replayable on this FE even though the
                // cleanup of a LINGERING other incarnation could not be read. Keeping the
                // entry let ordinary queries keep replaying a baseline whose row the DROP
                // had already deleted; the failure still propagates (retryable), and the
                // retry takes the cache-miss path which deletes the lingering rows.
                removeCachedBaseline(id);
                // The identity delete was CONFIRMED before this cleanup read failed, so
                // the removed row's tombstone may be appended NOW - withholding it (as
                // the absence fence does for an UNPROVEN delete) would lose the
                // delayed-commit protection the confirmation already earned. The ID is
                // condemned as well: the lingering incarnation could not be READ, and the
                // user dropped the id - any row of it that publishes later must not revive
                // the baseline (see wipeDurableRowsById).
                noteDroppedSeqState(removed);
                noteDroppedIdState(id);
                recordPendingMutationFence(id, null, 0);
                throw e;
            }
            removeCachedBaseline(id);
            // A DELAYED status INSERT of a demoted master can commit AFTER this delete
            // and revive the row: the append-only tombstone makes every
            // later load treat that incarnation as deleted, whatever the commit order.
            noteDroppedSeqState(removed);
            // The ID itself is condemned too: a handoff collision can leave a DIFFERENT
            // incarnation of this id (written by another master) unreadable at this
            // point, and it must not revive the dropped id when it publishes later - its
            // distinct digest would not match the identity tombstone above.
            noteDroppedIdState(id);
            // Even a CONFIRMED identity delete can stay unreadable to a later local read
            // (its publication lags the confirmation): the fence keeps the refresh / a
            // SHOW reload from re-adding the dropped row until the table shows it gone

            recordPendingMutationFence(id, null, 0);
            return true;
        }
    }

    /**
     * Removes one id from the in-memory store (baseline map + hash index) under the
     * write lock. Shared by dropBaseline's happy path and its lingering-row
     * cleanup failure path: once the identity delete is confirmed, the cached row must
     * stop being matchable / replayable no matter what the cleanup of a DIFFERENT
     * incarnation reported.
     */
    private void removeCachedBaseline(long id) {
        stateLock.writeLock().lock();
        try {
            BaselinePlan gone = baselines.remove(id);
            if (gone != null) {
                removeFromHashIndex(gone);
                stateVersion++;
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * See dropBaseline: reconciles a cache miss against the durable table. The
     * rows found (a reload window can leave several) are removed by IDENTITY, so a stale
     * id can never delete an unrelated row.
     */
    private boolean dropDurableRowByIdIfAbsentFromCache(long id) {
        List<BaselinePlan> durable = readPersistedById(id);
        if (durable.isEmpty()) {
            return false;
        }
        try {
            for (BaselinePlan row : durable) {
                persistDeleteByIdentity(row);
            }
        } catch (RuntimeException e) {
            if (!NO_LONGER_MASTER.equals(e.getMessage())) {
                // same fence as the cached path: the delete may have
                // committed while its publication lags, and a delayed status INSERT may
                // still revive the row.: the tombstone is NOT written here -
                // the delete may equally have failed BEFORE commit, and a marker would
                // then hide the still-live row and re-issue the delete on every load
                // (silently completing a DROP that reported failure). The identities
                // ride on the fence and are tombstoned once a snapshot proves absence.
                for (BaselinePlan row : durable) {
                    recordPendingAbsenceFence(row);
                }
            }
            throw e;
        }
        for (BaselinePlan row : durable) {
            noteDroppedSeqState(row);
        }
        noteDroppedIdState(id);
        recordPendingMutationFence(id, null, 0);
        LOG.info("SPM dropped baseline {} from the durable table while the local cache did"
                + " not have it (promotion reload / stale snapshot window)", id);
        return true;
    }

    /**
     * Removes any durable row of the given id that is NOT the identity just deleted (see
     * dropBaseline): a promotion-window snapshot can carry an old object whose
     * id now names a DIFFERENT baseline (the id was dropped and reused), and the DROP is
     * keyed by the user-facing id - identity-deleting only the stale object would report
     * success while the real row stays.
     */
    private static void wipeDurableRowsById(long id, BaselinePlan alreadyDeleted) {
        if (idAllocatorStoreForTest == null && !persistenceEnabled()) {
            return;
        }
        List<BaselinePlan> rows = readPersistedById(id);
        for (BaselinePlan row : rows) {
            if (sameIdentity(row, alreadyDeleted)) {
                continue; // the row this DROP already deleted
            }
            LOG.warn("SPM drop of baseline {} removed a lingering row of a different"
                    + " incarnation (digest {})", id, row.getBindSqlDigest());
            persistDeleteByIdentity(row);
            // the wiped incarnation gets its own tombstone: a still-in-flight write of
            // THAT identity must not revive it either
            noteDroppedSeqState(row);
        }
    }

    /**
     * ALTER counterpart of dropDurableRowByIdIfAbsentFromCache: reconciles a
     * GLOBAL cache miss against the durable table. Env.transferToMaster sets isReady
     * BEFORE forceReloadFromInternalTable finishes, so a freshly promoted follower can
     * miss a row the previous master created durably - returning false ("does not
     * exist") for a present row. The rows found are parsed exactly like a load
     * (transient trees rebuilt), the deterministic winner is ADOPTED into the cache so
     * the ALTER runs its normal two-phase flip, and only a PROVEN absence returns null.
     * A read / parse failure propagates as a retryable error.
     */
    private BaselinePlan adoptDurableRowIfAbsentFromCache(long id) {
        if (!persistenceEnabled() && idAllocatorStoreForTest == null
                && statusProtocolStoreForTest == null) {
            return null;
        }
        // A row revived after its DROP is NOT a present baseline - the
        // drop's delete may simply not be readable yet (see filterTombstonedDurableRows)
        List<BaselinePlan> durable = filterTombstonedDurableRows(readPersistedParsedById(id));
        if (durable.isEmpty()) {
            return null;
        }
        BaselinePlan winner = durable.get(0);
        for (int i = 1; i < durable.size(); i++) {
            winner = pickDurableWinner(winner, durable.get(i));
        }
        stateLock.writeLock().lock();
        try {
            BaselinePlan raced = baselines.get(id);
            if (raced != null) {
                return raced;
            }
            baselines.put(winner.getId(), winner);
            addToHashIndex(winner);
            stateVersion++;
            LOG.info("SPM adopted durable baseline {} into the cache after a promotion"
                    + " cache miss", id);
            return winner;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Reads every durable row carrying the given id (DROP reconcile / identity delete).
     * A read FAILURE propagates: “no row” may only be reported from a successful read.
     *
     * @param id the baseline id
     * @return the durable rows (empty when none)
     * @throws RuntimeException when the durable state cannot be read
     */
    private static List<BaselinePlan> readPersistedById(long id) {
        return readPersistedRowsById(id, false);
    }

    /**
     * Reads the durable rows of one id with their transient trees REBUILT (like a load):
     * the ALTER adoption path needs fully usable rows. Unparsable rows are skipped with a
     * warning; when every row of a NON-EMPTY result is unparsable the read fails
     * retryably - the FE would never load such a row, so silently reporting “not found”
     * would be wrong.
     *
     * @param id the baseline id
     * @return the parsed durable rows (empty when none)
     * @throws RuntimeException when the durable state cannot be read / parsed
     */
    private static List<BaselinePlan> readPersistedParsedById(long id) {
        return readPersistedRowsById(id, true);
    }

    /**
     * Shared reader of readPersistedById / readPersistedParsedById:
     * seam-aware and fail-closed on read errors.
     *
     * @param id          the baseline id
     * @param rebuildTrees whether each row is parsed like a load (parsePersistedRow)
     *                     instead of decoded as plain scalars (fromRow)
     * @return the rows (possibly empty)
     */
    private static List<BaselinePlan> readPersistedRowsById(long id, boolean rebuildTrees) {
        if (idAllocatorStoreForTest != null) {
            try {
                return new ArrayList<>(idAllocatorStoreForTest.readById(id));
            } catch (RuntimeException e) {
                throw new RuntimeException("SPM durable read by id failed: " + e.getMessage(), e);
            }
        }
        if (!persistenceEnabled()) {
            if (statusProtocolStoreForTest != null) {
                int rows = durableRowCount(id, BaselineStatus.ENABLED)
                        + durableRowCount(id, BaselineStatus.DISABLED);
                if (rows > 0) {
                    // the status-protocol seam cannot hand out the row CONTENT an identity
                    // delete needs: fail closed instead of reporting a false absence
                    throw new RuntimeException("SPM cannot read the durable row of baseline "
                            + id + " from the test store");
                }
            }
            return List.of();
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        boolean anyRow = false;
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_BY_ID_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            List<BaselinePlan> result = new ArrayList<>();
            if (rows != null) {
                for (ResultRow row : rows) {
                    anyRow = true;
                    if (!rebuildTrees) {
                        result.add(fromRow(row));
                        continue;
                    }
                    try {
                        result.add(parsePersistedRow(row));
                    } catch (Throwable t) {
                        LOG.warn("SPM skip the unparsable durable baseline {} (adoption): {}",
                                id, t.getMessage());
                    }
                }
            }
            if (!result.isEmpty() || !anyRow) {
                return result;
            }
        } catch (Exception e) {
            throw new RuntimeException("SPM durable read by id failed: " + e.getMessage(), e);
        }
        throw new RuntimeException("SPM durable baseline " + id
                + " exists but cannot be parsed; retry the ALTER");
    }

    /**
     * Updates the status of a baseline (ENABLE / DISABLE).
     *
     * @param id     the baseline id
     * @param status the target status
     * @return whether the update succeeded
     */
    public boolean updateStatus(long id, BaselineStatus status) {
        ensureLoadedOrThrow();
        // Two-phase status change: the INSERT(new) + DELETE(old) pair is internal-table I/O
        // and runs under writerLock, never under stateLock. Matching tolerates a status
        // flip racing with a lookup exactly like an ALTER landing right after the lookup
        // (see findCandidateBaselines); on failure the in-memory flip is reverted.
        synchronized (writerLock) {
            BaselinePlan plan;
            stateLock.readLock().lock();
            try {
                plan = baselines.get(id);
            } finally {
                stateLock.readLock().unlock();
            }
            if (plan == null) {
                // ALTER-specific cache-miss reconciliation (the DROP path does the same
                // for its identity delete): a promoted follower whose snapshot predates
                // the previous master's CREATE misses the row here and must not report
                // "does not exist" for a durable baseline.
                plan = adoptDurableRowIfAbsentFromCache(id);
                if (plan == null) {
                    return false;
                }
            }
            BaselineStatus previousStatus = plan.getStatus();
            boolean durableReconcile = persistenceEnabled()
                    || statusProtocolStoreForTest != null || idAllocatorStoreForTest != null;
            DurableStatus probe = null;
            if (durableReconcile) {
                // Reconcile against the durable incarnation BEFORE touching anything: a
                // promoted FE can serve a snapshot that predates the previous master's
                // DROP, and an opposite-status ALTER over that stale cache would INSERT
                // the requested status back - resurrecting a baseline the previous
                // master dropped. The probe also decides the REAL previous status (the
                // cache may have missed an earlier flip) and hands out the winning row,
                // whose update time bounds the new row's (see the second bump below).
                probe = probeDurableStatus(id, status);
                if (probe.winner != null && !sameIdentity(plan, probe.winner)) {
                    // A handoff can publish two DIFFERENT CREATE rows under one id (B
                    // cached its row after an empty collision probe, then A's delayed row
                    // appeared and won the same-second digest tie): completing the ALTER
                    // against B would report success for an identity the durable table no
                    // longer favors, and a differing-status flip would even write a newer
                    // B row and switch the durable winner. Adopt the winner that IS
                    // durable and fail retryably.
                    LOG.warn("SPM status update of baseline {}: the durable winner of this id"
                            + " is a DIFFERENT baseline (a handoff collision); adopting it and"
                            + " failing the ALTER retryably", id);
                    removeCachedBaseline(id);
                    publishBaseline(probe.winner);
                    throw new IllegalStateException("SPM baseline " + id + " is durably held"
                            + " by a different baseline (a handoff collision); the durable row"
                            + " was adopted - retry the ALTER");
                }
                if (probe.outcome == DurableStatusProbe.UNKNOWN) {
                    // The durable state cannot be CONFIRMED: reporting a no-op success
                    // could leave a durably DISABLED / DROPPED row while this FE serves
                    // its stale snapshot, and the next refresh restores the opposite
                    // state. Fail retryably until the requested status is provable.
                    throw new IllegalStateException("SPM cannot confirm the durable status of"
                            + " baseline " + id + "; retry the ALTER");
                }
                if (probe.outcome == DurableStatusProbe.ABSENT) {
                    // the row is gone durably (the previous master dropped it): never
                    // report a successful ALTER for a baseline that does not exist
                    LOG.warn("SPM status update of baseline {}: the row is gone durably;"
                            + " dropping it from the cache", id);
                    retireStaleInMemory(List.of(plan));
                    return false;
                }
                if (probe.outcome == DurableStatusProbe.MATCHES) {
                    // The requested status may also be an artifact of a fence: an earlier
                    // write of this id is still unresolved (committed-but-unreadable flip,
                    // or an unproven delete), and its row may publish after this read -
                    // accepting the match would then be reverted. Only a fence-free id may
                    // be repaired from a MATCHING read.
                    requireNoContradictingStatusFence(id, status);
                    // the durable row already carries the requested status (the cache
                    // missed an earlier flip): repair the live object and report success
                    // without rewriting the row
                    if (previousStatus != status) {
                        publishStatus(plan, status, System.currentTimeMillis());
                    }
                    return true;
                }
                // DIFFERS: the durable row carries the OTHER status. Treat that as the
                // real previous status so the INSERT(new) / DELETE(old) pair lands on the
                // durable state and the live object flips to the requested status.
                if (previousStatus == status) {
                    LOG.warn("SPM status update of baseline {}: the cache says {} but the"
                            + " durable row is {}; repairing", id, previousStatus,
                            otherStatus(status));
                }
                previousStatus = otherStatus(status);
            } else if (previousStatus == status) {
                // nothing durable to reconcile (unit tests / disabled persistence) and
                // the status is already the requested one
                return true;
            }
            // The internal table is a DUPLICATE-key table on which UPDATE is not supported, so a
            // status change is persisted as INSERT (new status) + DELETE (old status). The INSERT
            // runs FIRST so the durable new row exists before any delete: a failure can never
            // leave the in-memory state "old" while the only table row was already removed (the
            // delete-then-insert gap, where the next refresh / restart silently dropped the
            // baseline). Deleting by the PREVIOUS status can never touch the freshly inserted
            // row (the statuses differ).
            long newUpdateTime = System.currentTimeMillis();
            // DATETIME stores only SECONDS: two committed flips inside one second keep
            // EQUAL durable timestamps, and pickDurableWinner prefers DISABLED on such a
            // tie - a later ENABLE would then be reversed by the next refresh / restart
            // while this FE temporarily served ENABLED. Advancing the new row past the
            // newest EXISTING stored second keeps the durable order ("the later intent
            // wins") exact at the stored precision, so the row this statement writes is
            // always the durable winner once it is written.
            //
            // The bump reads the newest stored second of the WHOLE TABLE, not just the
            // rows of this id: after rapid flips gave another baseline B a
            // future stored update_time, flipping A and C kept update_time = now -
            // dwarfed by B - so NEITHER MAX(id), COUNT(*) nor MAX(update_time) (the
            // paginated snapshot fence) changed, and a refresh could merge pages of two
            // different states while a matching fence accepted the mix (refresh kept
            // replaying A after its successful DISABLE). Bumping past the table-wide
            // maximum makes EVERY flip move MAX(update_time), so the fence always
            // observes it.
            long newestSecond = readNewestStoredUpdateSecond();
            if (newUpdateTime / 1000L <= newestSecond) {
                newUpdateTime = (newestSecond + 1) * 1000L;
            }
            // Persist a DETACHED snapshot carrying the new status: the live object keeps
            // the old status until the durable write (or the confirmed reconciliation)
            // succeeded - matching readers do not take the writer lock, so publishing
            // early would let a concurrent query replay a baseline whose durable row is
            // still DISABLED when the write later fails, while ALTER still reports
            // failure.
            BaselinePlan durablePlan = plan.copyPersistedScalars();
            durablePlan.setStatus(status);
            durablePlan.setUpdateTime(newUpdateTime);
            boolean insertWritten;
            try {
                assertLeaderForWrite();
                insertWritten = persistTransitionInsert(durablePlan, previousStatus);
            } catch (UnconfirmedInsertException e) {
                // The conditional INSERT WROTE the new-status row (the affected-row count /
                // the committed-write probe is the evidence) - only its PUBLICATION lags
                // past every probe. The row is durable, and its stored second is strictly
                // later than every row the write met (see the bump above), so it IS the
                // durable winner (pickDurableWinner). Keeping the OLD cached status here
                // let ordinary queries keep replaying a baseline whose durable winner is
                // already the requested status. Publish the proven flip.
                publishStatus(plan, status, newUpdateTime);
                LOG.warn("SPM status update of baseline {} reported success but its row is"
                        + " not READABLE yet; the committed flip is the durable winner -"
                        + " publishing it", id);
                // the row may stay unreadable for a while: fence the id so a stale
                // snapshot cannot revert the committed flip
                recordConfirmedMutationFence(id, status, newUpdateTime);
                return true;
            } catch (RuntimeException e) {
                // The INSERT outcome cannot be PROVEN (it may have written nothing, or may
                // have committed without any read seeing it): never publish an unobserved
                // status. Reconcile the cache with a winner that IS readable, then report
                // the failure.
                reconcileCacheToDurableWinner(id, plan);
                // A successful read CANNOT prove the write landed nothing (publication of
                // a committed row lags every immediate probe), so the caller's failed flip
                // keeps the id FENCED and the obsolete entry out of matching until the
                // durable table shows the outcome: the committed DISABLED
                // row may publish at any moment, and the reconciled OLD-status entry would
                // keep being replayed until a refresh.
                recordPendingMutationFence(id, status, newUpdateTime);
                if (plan.getStatus() != status) {
                    removeCachedBaseline(id);
                }
                throw e;
            }
            if (!insertWritten) {
                // The conditional statement matched no previous-status row, so it reported
                // SQL OK while writing NOTHING (a concurrent DROP or status flip won the
                // race - the reviewer's handoff example). The requested status may still
                // be durable now: ANOTHER writer can have completed the very SAME flip
                // between our probe and our INSERT, in which case the ALTER is a no-op
                // success. Re-resolve before reporting the conflict.
                if (probeDurableStatus(id, status).outcome == DurableStatusProbe.MATCHES) {
                    // same fence guard as the reconciliation above: a MATCHING read while
                    // an unresolved write of this id may still publish is not the final
                    // state (the fenced write can land later and win)
                    requireNoContradictingStatusFence(id, status);
                    if (plan.getStatus() != status) {
                        publishStatus(plan, status, newUpdateTime);
                    }
                    LOG.warn("SPM status update of baseline {}: the requested status is already"
                            + " durable (a concurrent flip won the race); reporting success", id);
                    return true;
                }
                reconcileCacheToDurableWinner(id, plan);
                throw statusConflict(id, previousStatus);
            }
            try {
                persistDeleteByIdAndStatus(durablePlan, previousStatus);
            } catch (RuntimeException e) {
                // The INSERT half is CONFIRMED (its row is READABLE) and carries a strictly
                // later stored second than every row it met, so the new-status row IS the
                // durable winner whether the old-row delete committed, failed, or merely
                // lags its publication. The cache MUST follow that winner: leaving the OLD
                // status let this FE keep replaying a baseline the durable table has
                // already flipped until the next refresh.
                publishStatus(plan, status, newUpdateTime);
                // the old row may linger readable for a while: fence the id so a snapshot
                // still dominated by it cannot revert the confirmed winner
                recordConfirmedMutationFence(id, status, newUpdateTime);
                if (NO_LONGER_MASTER.equals(e.getMessage())) {
                    // a fenced write is reported to the client (retrying converges: the
                    // retry's probe sees the requested status durably), but the cache still
                    // follows the CONFIRMED durable winner
                    LOG.warn("SPM status update of baseline {}: the old-row delete was fenced"
                            + " ({}); the cache follows the confirmed new-status row", id,
                            e.getMessage());
                    throw e;
                }
                LOG.warn("SPM status update of baseline {} kept the new-status row after its"
                        + " old-row delete failed ({}); the stale row is cleaned up by the"
                        + " next flip", id, e.getMessage());
                return true;
            }
            publishStatus(plan, status, newUpdateTime);
            // the committed row's publication may lag: fence the id until a snapshot
            // shows the flip
            recordConfirmedMutationFence(id, status, newUpdateTime);
            return true;
        }
    }

    /** Publishes a CONFIRMED status flip on the live object under the state lock. */
    private void publishStatus(BaselinePlan plan, BaselineStatus status, long updateTime) {
        stateLock.writeLock().lock();
        try {
            plan.setStatus(status);
            plan.setUpdateTime(updateTime);
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Outcome of the durable status probe (see updateStatus). */
    private enum DurableStatusProbe { MATCHES, DIFFERS, ABSENT, UNKNOWN }

    /**
     * The durable status of one baseline (see probeDurableStatus): the outcome plus
     * the WINNING row when it could be read. The winner's update time is what a status flip
     * must stay strictly later than (see the DATETIME second bump in updateStatus).
     */
    private static final class DurableStatus {
        final DurableStatusProbe outcome;
        final BaselinePlan winner;

        DurableStatus(DurableStatusProbe outcome, BaselinePlan winner) {
            this.outcome = outcome;
            this.winner = winner;
        }

        static DurableStatus of(DurableStatusProbe outcome) {
            return new DurableStatus(outcome, null);
        }
    }

    /**
     * Compares the EFFECTIVE durable status of one baseline with the expected one. A
     * failed status flip can leave BOTH rows behind (the old-row delete AND the
     * compensating delete failed): the load path resolves such duplicates with
     * pickDurableWinner (later updateTime wins, DISABLED on a tie), and the
     * probe must apply the SAME rule - a bare “does a row with the cached status exist”
     * count reported MATCHES while the newer row carried the opposite status, so the
     * next ALTER back to the old status reported success without changing the effective
     * state.
     *
     * The count-only status seam cannot decide between two rows (it has no update
     * times): both-present is UNKNOWN (fail closed). A read failure is UNKNOWN as well.
     */
    private static DurableStatus probeDurableStatus(long id, BaselineStatus expected) {
        try {
            if (idAllocatorStoreForTest == null && !persistenceEnabled()
                    && statusProtocolStoreForTest == null) {
                return DurableStatus.of(DurableStatusProbe.ABSENT);
            }
            if (statusProtocolStoreForTest != null && idAllocatorStoreForTest == null) {
                boolean expectedRows =
                        statusProtocolStoreForTest.countByIdAndStatus(id, expected) > 0;
                boolean otherRows = statusProtocolStoreForTest
                        .countByIdAndStatus(id, otherStatus(expected)) > 0;
                if (!expectedRows && !otherRows) {
                    return DurableStatus.of(DurableStatusProbe.ABSENT);
                }
                if (expectedRows && !otherRows) {
                    return DurableStatus.of(DurableStatusProbe.MATCHES);
                }
                if (otherRows && !expectedRows) {
                    return DurableStatus.of(DurableStatusProbe.DIFFERS);
                }
                throw new IllegalStateException(
                        "two durable rows of baseline " + id + " and no update times");
            }
            List<BaselinePlan> rows = readPersistedById(id);
            if (rows.isEmpty()) {
                return DurableStatus.of(DurableStatusProbe.ABSENT);
            }
            BaselinePlan winner = rows.get(0);
            for (int i = 1; i < rows.size(); i++) {
                winner = pickDurableWinner(winner, rows.get(i));
            }
            return new DurableStatus(winner.getStatus() == expected
                    ? DurableStatusProbe.MATCHES : DurableStatusProbe.DIFFERS, winner);
        } catch (Throwable t) {
            LOG.warn("SPM cannot probe the durable status of baseline {}: {}", id, t.getMessage());
            return DurableStatus.of(DurableStatusProbe.UNKNOWN);
        }
    }

    /**
     * Aligns the live object with the DURABLE winner (see pickDurableWinner) before
     * a status update reports its failure: the cache may have missed an earlier flip, or
     * the failed statement may have left a NEWER row behind - leaving the stale status
     * served queries a rewrite context the durable table no longer has, which is exactly
     * what the next refresh repairs. A winner that cannot be read leaves the cache
     * untouched (the failure is reported either way, and a refresh or the next retry
     * resolves it).
     */
    private void reconcileCacheToDurableWinner(long id, BaselinePlan plan) {
        BaselinePlan winner = readDurableWinnerOrNull(id);
        if (winner == null || winner.getStatus() == plan.getStatus()) {
            return;
        }
        publishStatus(plan, winner.getStatus(), winner.getUpdateTime());
        LOG.warn("SPM status update of baseline {} failed; reconciled the cache with the"
                + " durable winner ({})", id, winner.getStatus());
    }

    /**
     * The durable winner row of one id (see pickDurableWinner), or null when the
     * durable rows cannot be read (the count-only status seam / a metadata failure).
     */
    private static BaselinePlan readDurableWinnerOrNull(long id) {
        try {
            List<BaselinePlan> rows = readPersistedById(id);
            if (rows.isEmpty()) {
                return null;
            }
            BaselinePlan winner = rows.get(0);
            for (int i = 1; i < rows.size(); i++) {
                winner = pickDurableWinner(winner, rows.get(i));
            }
            return winner;
        } catch (Throwable t) {
            LOG.warn("SPM cannot read the durable winner of baseline {}: {}", id, t.getMessage());
            return null;
        }
    }

    /** The other status of the binary enable / disable model. */
    private static BaselineStatus otherStatus(BaselineStatus status) {
        return status == BaselineStatus.ENABLED
                ? BaselineStatus.DISABLED : BaselineStatus.ENABLED;
    }

    /**
     * Removes stale in-memory duplicates (see createBaseline / updateStatus):
     * rows whose schema fingerprint no longer matches the incoming key or that the durable
     * table no longer has. Memory-only: the durable side is retired by the caller.
     */
    private void retireStaleInMemory(List<BaselinePlan> rows) {
        if (rows == null || rows.isEmpty()) {
            return;
        }
        stateLock.writeLock().lock();
        try {
            for (BaselinePlan row : rows) {
                BaselinePlan gone = baselines.get(row.getId());
                if (gone != null) {
                    baselines.remove(row.getId());
                    removeFromHashIndex(gone);
                    stateVersion++;
                    LOG.info("SPM retired the stale cached baseline {}", row.getId());
                }
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * One internal-table statement / query body; see inInternalIoMode. */
    @FunctionalInterface
    private interface InternalIo<T> {
        T run() throws Exception;
    }

    /**
     * Runs one internal-table I/O with the DEFAULT parser mode pinned (returns the
     * result). Every internal SPM statement is built with StatisticsUtil.escapeSQL,
     * whose doubled backslashes only decode back to a single backslash when the
     * executor parses under the default mode; the internal context otherwise inherits
     * the GLOBAL sql_mode, so NO_BACKSLASH_ESCAPES would store / compare the doubled
     * bytes literally and the stored SQL / JSON would round-trip with an extra
     * backslash.
     */
    private static <T> T inInternalIoMode(InternalIo<T> io) {
        return SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
            try {
                return io.run();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /** Runs inInternalIoMode and reports the effective parser mode (test seam). */
    @VisibleForTesting
    public static long internalIoModeForTest() {
        return inInternalIoMode(SqlModeHelper::currentMode);
    }

    /** Number of durable rows currently carrying (id, status). */
    private static int durableRowCount(long id, BaselineStatus status) {
        if (statusProtocolStoreForTest != null) {
            return statusProtocolStoreForTest.countByIdAndStatus(id, status);
        }
        if (!persistenceEnabled()) {
            return 0;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        params.put("status", status.name());
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    COUNT_BY_ID_AND_STATUS_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            String count = rows.get(0).getWithDefault(0, "0");
            return count.isEmpty() ? 0 : Integer.parseInt(count.trim());
        } catch (Exception e) {
            throw new RuntimeException("SPM durable status count failed: " + e.getMessage(), e);
        }
    }

    // ==================== query matching (Level 1 + Level 2 + ordering) ====================

    /**
     * Finds candidate baselines (the first two of the three-level filter plus priority
     * ordering).
     *
     * @param digest the parameterized digest of the user query
     * @param hash   the structural hash of the user query
     * @return the candidate baselines sorted by priority (possibly empty)
     */
    public List<BaselinePlan> findCandidateBaselines(String digest, long hash) {
        ensureLoaded();
        List<BaselinePlan> candidates;
        stateLock.readLock().lock();
        try {
            List<Long> candidateIds = hashIndex.get(hash);
            if (candidateIds == null || candidateIds.isEmpty()) {
                return List.of();
            }
            // snapshot the candidate objects while holding the lock: the map itself must
            // never be read concurrently with a writer
            candidates = candidateIds.stream()
                    .map(baselines::get)
                    .filter(p -> p != null)
                    .collect(Collectors.toList());
        } finally {
            stateLock.readLock().unlock();
        }
        // digest filter + priority ordering happen OUTSIDE the lock: the baseline objects
        // are immutable for matching purposes except a status flip, which races as
        // harmlessly as an ALTER landing right after the lock was released
        return candidates.stream()
                .filter(p -> p.getStatus().isActive())
                .filter(p -> p.getBindSqlDigest().equals(digest)) // Level 2 exact digest match
                .sorted(comparator)
                .collect(Collectors.toList());
    }

    /**
     * Finds baselines by hash (internal helper). Only reads the maps and the index, so a
     * READ lock is enough; createBaseline calls it in its phase-1 validation.
     */
    private List<BaselinePlan> findByHash(long hash) {
        List<Long> ids = hashIndex.get(hash);
        if (ids == null) {
            return List.of();
        }
        return ids.stream().map(baselines::get).filter(p -> p != null).collect(Collectors.toList());
    }

    /**
     * Whether any global baseline exists. Used by the rewrite path as a fast path: with
     * no baseline anywhere there is nothing to match, so the whole-tree digest rendering
     * can be skipped entirely.
     *
     * @return whether the global storage currently holds at least one baseline
     */
    public boolean hasBaselines() {
        ensureLoaded();
        stateLock.readLock().lock();
        try {
            return !baselines.isEmpty();
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Returns a baseline by id.
     *
     * @param id the baseline id
     * @return the baseline, or null when not found
     */
    public BaselinePlan getBaseline(long id) {
        ensureLoaded();
        stateLock.readLock().lock();
        try {
            return baselines.get(id);
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Confirms the GLOBAL store is LOADED. getAllBaselines() only STARTS the
     * asynchronous load and returns the current map, so at startup or right after a
     * promotion - when that map was just cleared - a caller reported ZERO global rows even
     * though durable rows existed, and a pending or failed read never converged. This
     * uses the same load (and retryable error) the mutating DDL relies on; query matching
     * keeps ensureLoaded()'s nonblocking degradation.
     *
     * A caller whose answer must reflect GLOBAL DDL completed on ANOTHER FE uses
     * confirmGlobalRowsForShow() instead: "loaded" only means this FE read the
     * table ONCE, so a follower that finished its load before the master committed keeps
     * answering from its old map until the refresh daemon runs.
     */
    public void ensureLoadedConfirmed() {
        ensureLoadedOrThrow();
    }

    /**
     * CONFIRMED durable read for the GLOBAL rows of SHOW BASELINE PLANS (and, in tests,
     * of any caller that must observe a GLOBAL DDL completed on another FE).
     * ensureLoadedConfirmed() returns immediately once loaded=true, and
     * getAllBaselines() then copies this FE's cache, so a follower that loaded
     * BEFORE a GLOBAL DDL completed on the master kept listing its OLD map: a completed
     * CREATE was invisible and a completed DROP stayed listed until the next refresh
     * daemon cycle (and, for a failed read, indefinitely).
     *
     * The GLOBAL portion of SHOW is documented as authoritative, so this performs the
     * module's confirmed read instead:
     *
     *   the master (or a store without table persistence, whose memory IS the durable
     *       state) answers from its own publish - every committed GLOBAL DDL ran locally;
     *   a follower first synchronizes its metadata with the master (the same
     *       strong-consistency mechanism a forwarded DDL and syncJournalIfNeeded
     *       use), then fences every snapshot read that started before that point through
     *       the store generation;
     *   the durable rows are then read FRESH: while the store is still unpublished the
     *       read is performed inline (bounded wait for the in-flight load first, exactly
     *       like the forwarded-DDL refresh), while it is published the fresh snapshot
     *       replaces the cache under writerLock (no local mutation can publish
     *       meanwhile);
     *   a failed read surfaces as a retryable error - SHOW must never print a table it
     *       cannot confirm. The published cache is deliberately NOT invalidated: unlike a
     *       forwarded DDL, no committed write is known to have happened, so query
     *       matching keeps its current state and the read is retried (by SHOW or the
     *       refresh daemon).
     */
    public void confirmGlobalRowsForShow() {
        if (snapshotReaderForTest == null && (!persistenceEnabled()
                || Env.getCurrentEnv() == null || Env.getCurrentEnv().isMaster())) {
            // no durable store behind this cache, or this FE is the one that executes
            // every GLOBAL DDL itself: its memory is at least as fresh as the table
            ensureLoadedOrThrow();
            return;
        }
        synchronized (writerLock) {
            // Fence: a snapshot whose READ started before this point may predate the
            // master's committed DDL, so it must never publish after this method
            // returns (the generation check discards the in-flight load instead).
            storeGeneration.incrementAndGet();
            syncJournalWithMaster(ConnectContext.get());
            if (!loaded) {
                // Wait (bounded) for the in-flight load to finish and discard itself,
                // then load once against the CURRENT table content.
                long deadline = System.currentTimeMillis() + MANAGEMENT_LOAD_WAIT_MILLIS;
                while (!loaded && System.currentTimeMillis() < deadline) {
                    if (loadInProgress.compareAndSet(false, true)) {
                        readAndPublishPossessingLoadSlot();
                        break;
                    }
                    synchronized (loadMonitor) {
                        try {
                            loadMonitor.wait(50L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                if (!loaded) {
                    throw new IllegalStateException("SPM baseline store is not ready yet"
                            + " (the baseline table has not been loaded); please retry later");
                }
                return;
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                throw new IllegalStateException("SPM baseline rows cannot be confirmed"
                        + " (SHOW must not report a stale table); please retry later: "
                        + t.getMessage(), t);
            }
            // No writer can interleave (writerLock is held) and loads return early while
            // loaded, so the snapshot is authoritative for this instant.
            applyRefreshedBaselines(snapshot);
        }
    }

    /**
     * Returns all baselines (for SHOW / tests).
     */
    public List<BaselinePlan> getAllBaselines() {
        ensureLoaded();
        List<BaselinePlan> all;
        stateLock.readLock().lock();
        try {
            all = new ArrayList<>(baselines.values());
        } finally {
            stateLock.readLock().unlock();
        }
        all.sort(Comparator.comparingLong(BaselinePlan::getId));
        return all;
    }

    // ==================== Phase 2: auto-capture helpers ====================

    /**
     * Whether a baseline with the given bindSqlDigest already exists (Phase 2 auto
     * capture dedup; design doc 7.2.5).
     *
     * @param bindSqlDigest the parameterized digest
     * @return true when at least one baseline with this digest exists
     */
    public boolean existsBaselineByDigest(String bindSqlDigest) {
        ensureLoaded();
        if (bindSqlDigest == null) {
            return false;
        }
        stateLock.readLock().lock();
        try {
            return baselines.values().stream()
                    .anyMatch(p -> bindSqlDigest.equals(p.getBindSqlDigest()));
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Whether an identical (digest, planSql) baseline already exists (Phase 2 auto
     * capture dedup; design doc 7.2.5 level 2).
     *
     * @param bindSqlDigest the parameterized digest
     * @param planSql       the frozen plan SQL
     * @return true when an identical baseline already exists
     */
    public boolean existsBaseline(String bindSqlDigest, String planSql) {
        ensureLoaded();
        if (bindSqlDigest == null) {
            return false;
        }
        stateLock.readLock().lock();
        try {
            return baselines.values().stream()
                    .anyMatch(p -> bindSqlDigest.equals(p.getBindSqlDigest())
                            && (planSql == null ? p.getPlanSql() == null
                                    : planSql.equals(p.getPlanSql())));
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * For tests: prepares the store for a LOAD-path test - unloads it and clears the
     * published maps without enabling real table persistence, so
     * loadFromInternalTable() runs through snapshotReaderForTest.
     */
    @VisibleForTesting
    void prepareLoadForTest() {
        stateLock.writeLock().lock();
        try {
            loaded = false;
            baselines.clear();
            hashIndex.clear();
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** For tests: pins the table-persistence gate (see persistenceEnabled()). */
    @VisibleForTesting
    void setPersistToTableForTest(boolean enabled) {
        persistToTable = enabled;
    }

    /** For tests: drives the coalesced background-load scheduling (see #scheduleAsyncLoad). */
    @VisibleForTesting
    void scheduleAsyncLoadForTest() {
        scheduleAsyncLoad();
    }

    /**
     * For tests: clears the storage.
     */
    public void clearForTest() {
        stateLock.writeLock().lock();
        try {
            loaded = true; // tests manage the in-memory storage directly; never touch the table
            persistToTable = false; // and never write the table from a unit test
            pendingCreates.clear(); // pending-create records belong to the dropped state
            pendingMutationFences.clear(); // (the mutation fences belong to it as well)
            pendingDroppedMarkers.clear(); // (and the unappended drop tombstones)
            statusProtocolStoreForTest = null; // and never route through a leaked test seam
            idAllocatorStoreForTest = null; // (the create-time collision seam, same reason)
            hwmRecordReadForTest = null; // (the compact watermark seam, same reason)
            seqTailReadForTest = null; // (the scoped sequence-tail seam, same reason)
            leaderProbeForTest = null; // (the leadership seam, same reason)
            forwardedDdlSyncForTest = null; // (the forwarded-DDL sync seam, same reason)
            durableVisibilityProbeForTest = null; // (the write-visibility seam, same reason)
            snapshotReadStartedHookForTest = null; // (the load-generation seam, same reason)
            snapshotReaderForTest = null; // (the load snapshot seam, same reason)
            asyncLoadSpawnCountForTest = null; // (the load-scheduling seam, same reason)
            baselines.clear();
            hashIndex.clear();
            maxPersistedIdSeen = 0;
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    // ==================== internal table persistence (__internal_schema.spm_baselines) ========

    /** Whether the internal schema db / table is enabled (false in FE unit tests). */
    private static boolean persistenceEnabled() {
        return FeConstants.enableInternalSchemaDb && INSTANCE.persistToTable;
    }

    /**
     * Loads all baselines from the internal table and rebuilds the in-memory hash index.
     * Idempotent. The transient parameterized bind and plan trees of each row are rebuilt on
     * the fly from the stored bindSql / planSql text (pure AST parsing + placeholder
     * parameterization, needs no catalog). After loading, the id generator continues after
     * the largest persisted id.
     */
    public void loadFromInternalTable() {
        if (!persistenceEnabled() && snapshotReaderForTest == null) {
            loaded = true;
            return;
        }
        if (loaded) {
            return;
        }
        tryLoadNow();
    }

    /**
     * Reads the table and publishes the snapshot when the caller OWNS the load slot
     * (loadInProgress). The read runs OUTSIDE the state lock: an internal query
     * can be slow and must not block rewrite lookups. Until the load completes no local
     * mutation can run (every mutator calls ensureLoaded first), so a concurrent second
     * load simply reads again and loses the write section's re-check.
     */
    private void tryLoadNow() {
        if (loaded || !loadInProgress.compareAndSet(false, true)) {
            return; // already loaded, or another thread is reading right now
        }
        readAndPublishPossessingLoadSlot();
    }

    /**
     * Performs the (bounded-timeout) read and atomically publishes the result; the caller
     * must own the load slot. Keeps loaded=false on any failure so the caller can
     * retry (a retry is NOT a permanent "no baselines" decision: DROP ... IF EXISTS would
     * otherwise report success without deleting the durable row).
     */
    private void readAndPublishPossessingLoadSlot() {
        try {
            // Capture the generation BEFORE the read: a promotion reload / invalidation
            // that lands while this snapshot is being read makes the snapshot stale (it
            // may still contain rows deleted by a DROP that completed in between), so the
            // publication below must be rejected instead of resurrecting them.
            final long generationAtRead = storeGeneration.get();
            // Also capture the LOCAL mutation version: a CREATE / ALTER / DROP that passed
            // its own load check while this snapshot was being read has already inserted
            // durably AND published into the maps. The snapshot PREDATES that row, so
            // publishing it would erase the local entry (the durable write stays; the
            // baseline would be invisible on this FE until the next refresh). Keep
            // loaded=false so the next access loads a snapshot that contains it.
            final long versionAtRead = getStateVersion();
            Runnable hook = snapshotReadStartedHookForTest;
            if (hook != null) {
                hook.run();
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                LOG.warn("SPM load baselines from internal table failed (will retry lazily): {}",
                        t.getMessage());
                return;
            }
            // Settle the mutation-fence obligations BEFORE the write lock: the
            // tombstone append is internal-table I/O with its own timeout, and doing
            // it under the store's write lock blocked every concurrent SPM candidate
            // lookup that only needs the read lock.
            final Set<Long> settledFences = resolvePendingMutationFences(snapshot);
            stateLock.writeLock().lock();
            try {
                if (loaded) {
                    return; // a concurrent load won the race
                }
                if (storeGeneration.get() != generationAtRead) {
                    // an invalidation superseded this snapshot (e.g. a DROP committed
                    // while the read was in flight); keep loaded=false so the next access
                    // retries against the CURRENT table content
                    LOG.warn("SPM baseline load discarded: the store was invalidated while the"
                            + " snapshot was being read (will retry)");
                    return;
                }
                if (stateVersion != versionAtRead) {
                    // a local mutation (CREATE / DROP / ALTER, or a merged refresh)
                    // published while the snapshot was being read: the snapshot predates it
                    LOG.warn("SPM baseline load discarded: a local mutation published while the"
                            + " snapshot was being read (will retry against the current table)");
                    return;
                }
                doLoadFromTable(snapshot, settledFences);
                loaded = true;
            } finally {
                stateLock.writeLock().unlock();
            }
        } finally {
            loadInProgress.set(false);
            synchronized (loadMonitor) {
                loadMonitor.notifyAll();
            }
        }
    }

    /**
     * Schedules the first load on a background thread (coalesced): the query path must
     * never read the shared table synchronously, and a failed read is simply retried by
     * the next query / refresh cycle instead of blocking the current one.
     *
     * The load slot is claimed ATOMICALLY here, at scheduling time: checking
     * loadInProgress and starting the thread were separate, so a query burst (or
     * repeated failed reads) could start one throwaway spm-baseline-async-load
     * thread per caller - only the CAS winner inside tryLoadNow() performed the
     * read, every other thread exited immediately. Reserving the slot first means a
     * caller that cannot claim it simply returns: the owner releases the slot when its
     * read finishes (readAndPublishPossessingLoadSlot()), so the next caller
     * retries against the fresh state.
     */
    private void scheduleAsyncLoad() {
        if (loaded || !loadInProgress.compareAndSet(false, true)) {
            return; // already loaded, or the load slot is owned (another caller is loading)
        }
        Thread loader = new Thread(() -> {
            try {
                // the slot is already ours: readAndPublishPossessingLoadSlot releases it
                // (and wakes the waiters) in its own finally, even when the read fails
                readAndPublishPossessingLoadSlot();
            } catch (Throwable t) {
                LOG.warn("SPM baseline background load failed (will retry): {}", t.getMessage());
            }
        }, "spm-baseline-async-load");
        loader.setDaemon(true);
        java.util.concurrent.atomic.AtomicInteger spawnCount = asyncLoadSpawnCountForTest;
        if (spawnCount != null) {
            spawnCount.incrementAndGet();
        }
        try {
            loader.start();
        } catch (Throwable t) {
            // the thread never ran, so nothing will release the slot: release it here,
            // otherwise every later ensureLoaded() fails its CAS and the store never loads
            loadInProgress.set(false);
            synchronized (loadMonitor) {
                loadMonitor.notifyAll();
            }
            LOG.warn("SPM baseline background load could not start (will retry): {}",
                    t.getMessage());
        }
    }

    /**
     * Loads the persisted baselines on first use (called at the head of every public
     * CRUD / query entry point). Cheap once loaded (a volatile read); a query-path caller
     * never blocks on the internal table - while the (background, coalesced) load runs the
     * store simply stays empty and the query runs without SPM.
     */
    public void ensureLoaded() {
        if (loaded) {
            return;
        }
        if (!persistenceEnabled() && snapshotReaderForTest == null) {
            loaded = true;
            return;
        }
        if (snapshotReaderForTest != null) {
            // a unit test replaces the snapshot reader and drives the loads explicitly
            // (loadFromInternalTable): never fast-forward loaded / schedule a background
            // load from under it
            return;
        }
        scheduleAsyncLoad();
    }

    /**
     * Strict variant for management operations (CREATE / ALTER / DROP): when the first
     * load has not succeeded (internal table / BE unavailable), fail with a retryable
     * error instead of silently operating on an empty map - DROP ... IF EXISTS would
     * otherwise report success without deleting the durable row, which then reappears on
     * the next refresh. Query-side callers keep ensureLoaded()'s degradation (no match
     * while the store is unavailable).
     */
    private void ensureLoadedOrThrow() {
        if (!persistenceEnabled()) {
            loaded = true;
            return;
        }
        if (!loaded) {
            // A management operation needs a REAL answer: wait for the in-flight
            // background load (bounded), or perform one inline with the short,
            // purpose-built timeout. Every other management caller coalesces on the same
            // load slot, so an unavailable BE costs ONE bounded attempt, not one per
            // concurrent DDL.
            long deadline = System.currentTimeMillis() + MANAGEMENT_LOAD_WAIT_MILLIS;
            while (!loaded && System.currentTimeMillis() < deadline) {
                if (loadInProgress.compareAndSet(false, true)) {
                    readAndPublishPossessingLoadSlot();
                    break;
                }
                synchronized (loadMonitor) {
                    try {
                        loadMonitor.wait(50L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        if (!loaded) {
            throw new IllegalStateException("SPM baseline store is not ready yet"
                    + " (the baseline table has not been loaded); please retry later");
        }
    }

    /** Replaces the in-memory store with the read snapshot (caller holds the write lock).
     * The fence obligations were settled by the caller BEFORE the lock (see
     * readAndPublishPossessingLoadSlot): appending a drop tombstone is internal-table
     * I/O with its own timeout, and holding the store's write lock across it blocked
     * every concurrent SPM candidate lookup that needs only the read lock. */
    private void doLoadFromTable(Map<Long, BaselinePlan> loadedPlans, Set<Long> fenced) {
        // Pending mutation fences mask contradicting rows here as well: a
        // reload after an invalidation must not republish a row whose dropped / disabled
        // outcome is still owed.
        for (Long fencedId : fenced) {
            loadedPlans.remove(fencedId);
        }
        Map<Long, List<Long>> loadedIndex = new HashMap<>();
        long maxId = 0;
        for (BaselinePlan p : loadedPlans.values()) {
            loadedIndex.computeIfAbsent(p.getBindSqlHash(), k -> new ArrayList<>()).add(p.getId());
            maxId = Math.max(maxId, p.getId());
        }
        baselines.clear();
        hashIndex.clear();
        baselines.putAll(loadedPlans);
        hashIndex.putAll(loadedIndex);
        // the snapshot read every row: no row above this id can exist undiscovered
        maxPersistedIdSeen = Math.max(maxPersistedIdSeen, maxId);
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        stateVersion++;
        LOG.info("SPM loaded {} baselines from internal table", baselines.size());
    }

    // ==================== periodic refresh (cross-FE visibility) ====================

    /**
     * Merges the persisted baselines into the in-memory cache (incremental diff). Called
     * periodically by BaselineRefreshDaemon on EVERY FE: baselines can be created / dropped /
     * altered on any FE (user DDL runs on the FE that receives it, auto capture runs on the
     * Leader), so without this refresh a baseline created on FE A would stay invisible on
     * FE B and whether a query hits a baseline would depend on which FE serves the client.
     *
     * Pure read: never writes the internal table. The table is read OUTSIDE the manager lock
     * (an internal query can take a while and must not block matching queries); the diff is
     * applied under the lock only when no local mutation / load happened in between
     * (stateVersion). A skipped snapshot is simply retried with a fresh read on the next
     * cycle - local mutations persist before they touch the in-memory state, so the skipped
     * snapshot may only be stale, never the other way around.
     */
    public void refreshFromInternalTable() {
        if (!persistenceEnabled()) {
            return; // internal schema db disabled (or a unit test): nothing to merge
        }
        // Serialize refresh with the WRITERS (writerLock, never stateLock): a local
        // mutation publishes its durable write and its in-memory state under this lock,
        // so the read-outside-the-lock + version-guarded apply below cannot interleave
        // with it. Without the serialization updateStatus can persist the new row, a
        // refresh that started BEFORE that write applies the OLD row while the version is
        // still unchanged, and the version bump that follows only records the mutation - it
        // does not republish the status, so this FE keeps matching the stale in-memory
        // row until another refresh. Matching queries are unaffected: they never take
        // writerLock (only stateLock read).
        synchronized (writerLock) {
            if (!loaded) {
                // The first load doubles as the first refresh (downloads the whole table).
                loadFromInternalTable();
                return;
            }
            long versionAtRead;
            stateLock.readLock().lock();
            try {
                if (!loaded) {
                    return; // raced with a concurrent first load: retry next cycle
                }
                versionAtRead = stateVersion; // no writer publishes while writerLock is held
            } finally {
                stateLock.readLock().unlock();
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                LOG.warn("SPM baseline refresh read failed (will retry next cycle): {}", t.getMessage());
                return;
            }
            if (!applyRefreshedSnapshotIfUnchanged(versionAtRead, snapshot)) {
                LOG.debug("SPM baseline refresh skipped: local state changed while reading");
            }
        }
    }

    /**
     * CONFIRMED post-forward refresh for the CREATE / ALTER / DROP hooks
     * (afterForwardToMaster): the DDL already committed on the master, so this FE
     * must publish a state that INCLUDES it (or fail retryably) before the statement
     * returns. refreshFromInternalTable is best-effort and has two holes:
     *
     * - loaded == false: an older load (started BEFORE the DDL) may hold the
     *   loadInProgress slot; the best-effort refresh returns immediately and the
     *   pre-DDL snapshot can publish afterwards - a CREATE stays invisible, a DROP /
     *   disable keeps replaying locally until the daemon refresh;
     * - loaded == true: a transient read failure is swallowed and the stale rows
     *   stay exactly as before the DDL.
     *
     * Fix: fence every snapshot whose read may predate the DDL through the store
     * generation (the in-flight load discards itself, see
     * readAndPublishPossessingLoadSlot) and then obtain a fresh read - an inline
     * load while unpublished, or a snapshot apply while published. On failure the
     * published store is INVALIDATED (fail closed: never keep replaying a possibly
     * dropped / disabled baseline) and a retryable failure surfaces to the caller.
     */
    public void refreshAfterForwardedDdl() {
        refreshAfterForwardedDdl(ConnectContext.get(), null);
    }

    /**
     * The DURABLE outcome a forwarded GLOBAL DDL must have produced on the master
     * . A forward runs with FORWARD_NO_SYNC and the follower's local
     * internal read may STILL return the pre-DDL visible version although the master
     * already committed the DDL: a GLOBAL DISABLE can return success while its DISABLED
     * row is committed but unreadable, and a DROP likewise while its DELETE publication
     * lags. Republishing the old snapshot made subsequent queries on that connection
     * replay a baseline just disabled or dropped.
     */
    @VisibleForTesting
    public static final class ForwardedDdlExpectation {
        private final long id;
        private final BaselineStatus status; // null = the row must be GONE (DROP)
        private final String createdBindSql; // non-null = presence of this identity
        private final String createdPlanSql;
        /**
         * The CREATE statement's query id, recorded on the row by the
         * master; "" / "NaN" = not usable as an identity (fall back to the text match).
         */
        private final String createdQueryId;
        /**
         * The CANONICAL (namespace-qualified, value-free) bind digest this CREATE
         * persists: the stable identity an idempotent duplicate shares with its EXISTING
         * row, whose query id (the FIRST statement's), decompiled plan text and raw bind
         * text (possibly other whitespace) all mismatch. "" = not computable here (fall
         * back to the raw checks).
         */
        private final String createdBindDigest;
        /**
         * The CANONICAL digest of the SUBMITTED plan SQL (see
         * SPMPlanner#canonicalPlanDigest): the bind digest identifies the binding but
         * NOT the plan text, and two baselines may share one bind digest while
         * carrying different plan texts. "" = not computable here (the confirmation
         * falls back to the bind digest alone, as before the column existed).
         */
        private final String createdPlanDigest;

        private ForwardedDdlExpectation(long id, BaselineStatus status) {
            this(id, status, null, null, "", "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql) {
            this(0, null, createdBindSql, createdPlanSql, "", "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId, String createdBindDigest) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, createdBindDigest, "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId, String createdBindDigest, String createdPlanDigest) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, createdBindDigest,
                    createdPlanDigest);
        }

        private ForwardedDdlExpectation(long id, BaselineStatus status, String createdBindSql,
                String createdPlanSql, String createdQueryId, String createdBindDigest,
                String createdPlanDigest) {
            this.id = id;
            this.status = status;
            this.createdBindSql = createdBindSql;
            this.createdPlanSql = createdPlanSql;
            this.createdQueryId = createdQueryId == null ? "" : createdQueryId;
            this.createdBindDigest = createdBindDigest == null ? "" : createdBindDigest;
            this.createdPlanDigest = createdPlanDigest == null ? "" : createdPlanDigest;
        }

        /** The expected outcome of a forwarded DROP: no readable row carries the id. */
        public static ForwardedDdlExpectation absent(long id) {
            return new ForwardedDdlExpectation(id, null);
        }

        /** The expected outcome of a forwarded ALTER: the id's row carries the status. */
        public static ForwardedDdlExpectation status(long id, BaselineStatus status) {
            return new ForwardedDdlExpectation(id, status);
        }

        /**
         * The expected outcome of a forwarded CREATE: a readable row carries
         * this EXACT (bindSql, planSql) identity. The follower cannot know the id - it is
         * allocated on the master - but without a requirement its refresh accepted a
         * stable local snapshot that still LACKED the new row, and the next query on the
         * same connection missed its GLOBAL baseline until the refresh daemon caught up.
         *
         * The TEXT match alone only identifies the raw-fallback rows: the master
         * persists SPMPlan2SQLBuilder's DECOMPILED planSql for an ordinary CREATE, so
         * every follower snapshot failed this comparison and the callback invalidated
         * its cache / reported an error after bounded retries. The
         * statement's query id survives every freezing choice - use
         * created(String, String, String) when it is available.
         *
         * @param bindSql the forwarded CREATE's bind SQL
         * @param planSql the forwarded CREATE's plan SQL
         * @return the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql) {
            return created(bindSql, planSql, "");
        }

        /**
         * As created(String, String) plus the STATEMENT query id of the
         * forwarded CREATE. The master executes the forwarded statement
         * under THIS id (the forward carries ctx.queryId() and the master's
         * execution context adopts it, see FEOpExecutor #buildStmtForwardParams), and the
         * CREATE stores DebugUtil.printId(ctx.queryId()) on the row - so the
         * follower, which still sees its own (pre-adoption) query id here, can identify
         * the committed row even though the persisted plan text is the DECOMPILED one.
         *
         * @param bindSql          the forwarded CREATE's bind SQL
         * @param planSql          the forwarded CREATE's plan SQL
         * @param statementQueryId the statement's query id ("" = match the text only)
         * @return the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId) {
            return new ForwardedDdlExpectation(bindSql == null ? "" : bindSql,
                    planSql == null ? "" : planSql, statementQueryId);
        }

        /**
         * As created(String, String, String) plus the CANONICAL BIND DIGEST of the
         * forwarded CREATE (see SPMPlanner#canonicalBindDigest). The digest is the identity
         * every create of this shape shares: an IDEMPOTENT duplicate CREATE returns the
         * master's EXISTING row, whose query id belongs to the FIRST statement and whose
         * raw plan text is the DECOMPILED one - with different whitespace the raw bind
         * text differs as well - so only the canonical digest confirms the durable
         * outcome and can be computed locally (the raw checks remain as a fallback for
         * callers without a digest).
         *
         * @param bindSql          the forwarded CREATE's bind SQL
         * @param planSql          the forwarded CREATE's plan SQL
         * @param statementQueryId the statement's query id ("" = match the text only)
         * @param bindDigest       the canonical bind digest ("" = not available)
         * @return the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId, String bindDigest) {
            return created(bindSql, planSql, statementQueryId, bindDigest, "");
        }

        /**
         * As created(String, String, String, String) plus the CANONICAL PLAN DIGEST of
         * the forwarded CREATE (see SPMPlanner#canonicalPlanDigest). The bind digest
         * alone does NOT identify the row: two CREATEs may share the binding while
         * carrying different plan texts (one baseline per plan), and the plan text
         * persisted by the master is the DECOMPILED one, so the submitted text cannot
         * be compared. The submitted plan's canonical digest can be computed locally and
         * equals the digest the master stores for this statement's own row; a row whose
         * bind digest agrees but whose plan digest differs belongs to the OTHER plan and
         * must not confirm this statement. Empty = fall back to the bind digest alone.
         *
         * @param bindSql          the forwarded CREATE's bind SQL
         * @param planSql          the forwarded CREATE's plan SQL
         * @param statementQueryId the statement's query id ("" = match the text only)
         * @param bindDigest       the canonical bind digest ("" = not available)
         * @param planDigest       the canonical plan digest ("" = not available)
         * @return the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId, String bindDigest, String planDigest) {
            return new ForwardedDdlExpectation(bindSql == null ? "" : bindSql,
                    planSql == null ? "" : planSql, statementQueryId, bindDigest,
                    planDigest);
        }

        public long getId() {
            return id;
        }

        public BaselineStatus getStatus() {
            return status;
        }

        /** A human-readable description for failure messages. */
        String describe() {
            return createdBindSql != null
                    ? "the forwarded CREATE of '" + createdBindSql + "'"
                    : "baseline " + id;
        }

        boolean isSatisfiedBy(Map<Long, BaselinePlan> snapshot) {
            if (createdBindSql != null) {
                // "" / "NaN" cannot identify a row: the CREATE stores "NaN" when its
                // context carried no query id, and matching that would accept ANY such
                // row.
                boolean queryIdUsable = !createdQueryId.isEmpty()
                        && !"NaN".equals(createdQueryId);
                boolean digestUsable = !createdBindDigest.isEmpty();
                for (BaselinePlan row : snapshot.values()) {
                    // The CANONICAL DIGEST is the identity every create of this shape
                    // shares: an idempotent duplicate returns the EXISTING row, whose
                    // query id, DECOMPILED plan text and (differently spaced) raw bind
                    // text all mismatch while the digest is equal by construction.
                    if (digestUsable && createdBindDigest.equals(row.getBindSqlDigest())) {
                        // The bind digest is only HALF the identity: two baselines
                        // may share the binding while carrying DIFFERENT plan
                        // texts (one baseline per plan), and an idempotent
                        // duplicate of the OTHER plan's CREATE must not be
                        // confirmed by this row. The submitted plan's canonical
                        // digest is stored on every new row; a pre-column row
                        // (NULL digest) keeps the historical bind-only match.
                        String rowPlanDigest = row.getPlanSqlDigest();
                        if (createdPlanDigest.isEmpty() || rowPlanDigest == null
                                || rowPlanDigest.isEmpty()) {
                            return true;
                        }
                        if (createdPlanDigest.equals(rowPlanDigest)) {
                            return true;
                        }
                    }
                    if (!createdBindSql.equals(row.getBindSql())) {
                        continue;
                    }
                    // The master persists the DECOMPILED plan text for an ordinary
                    // (non-fallback) CREATE, so the submitted planSql only matches when
                    // the raw fallback was frozen. The statement's QUERY ID survives
                    // every freezing choice: the forward carries this
                    // statement's ctx.queryId() to the master, whose execution context
                    // adopts it, and the CREATE stores it as the row's query_id.
                    if (createdPlanSql.equals(row.getPlanSql())
                            || (queryIdUsable && createdQueryId.equals(row.getQueryId()))) {
                        return true;
                    }
                }
                return false;
            }
            BaselinePlan row = snapshot.get(id);
            return status == null ? row == null : row != null && row.getStatus() == status;
        }
    }

    /**
     * As refreshAfterForwardedDdl(), with the forwarding statement's context: the
     * journal synchronization below talks to the master through it.
     *
     * @param ctx the context of the statement that was forwarded (may be null in tests)
     */
    public void refreshAfterForwardedDdl(ConnectContext ctx) {
        refreshAfterForwardedDdl(ctx, null);
    }

    /**
     * As refreshAfterForwardedDdl(ConnectContext), additionally CONFIRMING the
     * forwarded DDL's durable outcome before the snapshot is published:
     * the snapshot must show the expected status flip / row removal, re-read within a
     * bounded budget; an outcome that never becomes visible fails CLOSED (the published
     * cache is invalidated and a retryable error surfaces) instead of republishing the
     * pre-DDL row.
     *
     * @param ctx      the forwarded statement's context (may be null in tests)
     * @param expected the durable outcome to confirm, or null to skip the confirmation
     */
    public void refreshAfterForwardedDdl(ConnectContext ctx, ForwardedDdlExpectation expected) {
        if (!persistenceEnabled() && snapshotReaderForTest == null) {
            return;
        }
        synchronized (writerLock) {
            // Fence: no snapshot whose READ started before this point may publish. An
            // older background load holding the slot will be discarded by the generation
            // check instead of resurrecting pre-DDL content after this method returns.
            storeGeneration.incrementAndGet();
            // Follower-lag fence: the generation check only rejects overlapping LOCAL
            // reads. A forwarded global DDL (FORWARD_NO_SYNC) has no journal wait of its
            // own, so without synchronizing to the master first the LOCAL snapshot read
            // below may still see the pre-DDL visible version - after the master completed
            // a DROP / DISABLE this "confirmed" refresh would republish the removed row
            // and the command would report success while this FE kept replaying it.
            //
            // The sync is part of the FAIL-CLOSED path: a timeout exits here, BEFORE the
            // snapshot read below - without invalidating the published cache the
            // follower would keep the pre-DDL row (loaded / baselines / hash index all
            // intact) and its later default-consistency queries would keep replaying a
            // baseline the master already dropped / disabled. The existing "stale
            // snapshot" thread (below) does not cover this branch: it only guards the
            // read, not the synchronization that must precede it.
            try {
                syncJournalWithMaster(ctx);
            } catch (Throwable t) {
                recordForwardedDdlFence(expected);
                invalidatePublishedStore();
                throw new IllegalStateException("SPM cannot synchronize this FE with the"
                        + " master after the forwarded GLOBAL DDL; the local baseline cache"
                        + " was invalidated (please retry later): " + t.getMessage(), t);
            }
            if (!loaded) {
                // Wait (bounded) for the in-flight load to finish and discard itself,
                // then load once against the CURRENT table content.
                long deadline = System.currentTimeMillis() + MANAGEMENT_LOAD_WAIT_MILLIS;
                while (!loaded && System.currentTimeMillis() < deadline) {
                    if (loadInProgress.compareAndSet(false, true)) {
                        readAndPublishPossessingLoadSlot();
                        break;
                    }
                    synchronized (loadMonitor) {
                        try {
                            loadMonitor.wait(50L);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
                if (!loaded) {
                    throw new IllegalStateException("SPM baseline store is not ready yet"
                            + " (the baseline table has not been loaded); please retry later");
                }
                if (expected == null) {
                    // the inline load above just published the CURRENT table content;
                    // nothing further to confirm
                    return;
                }
            }
            // The journal sync orders THIS FE's metadata AFTER the master's DDL, but the
            // internal table's VISIBILITY of the DDL's own row write lags it: the expected
            // outcome is confirmed with bounded re-reads. No writer can
            // interleave (writerLock is held) and loads return early while loaded, so an
            // accepted snapshot is authoritative for this instant.
            for (int attempt = 0; ; attempt++) {
                final Map<Long, BaselinePlan> snapshot;
                try {
                    snapshot = readPersistedSnapshot();
                } catch (Throwable t) {
                    // Never pretend the local cache reflects the committed DDL: fence the
                    // (possibly pre-DDL) published rows out and surface a retryable failure.
                    recordForwardedDdlFence(expected);
                    invalidatePublishedStore();
                    throw new IllegalStateException("SPM baseline cache cannot be confirmed after"
                            + " the forwarded DDL (please retry later): " + t.getMessage(), t);
                }
                if (expected == null || expected.isSatisfiedBy(snapshot)) {
                    // The outcome the caller CONFIRMED supersedes any earlier mutation
                    // fence of the same id: an older fenced write carries a strictly
                    // earlier stored second, so pickDurableWinner keeps this outcome - and
                    // leaving the id masked would hold the local cache at the pre-DDL
                    // state until the fence's own bound expired.
                    if (expected != null) {
                        retireSupersededMutationFence(expected.getId(), snapshot);
                    }
                    applyRefreshedBaselines(snapshot);
                    return;
                }
                if (attempt >= FORWARDED_DDL_CONFIRM_ATTEMPTS) {
                    // The committed DDL's outcome never became visible. Publishing this
                    // snapshot would republish the old ENABLED row after a DISABLE (or the
                    // dropped row), which ordinary queries on this connection keep
                    // replaying - fail CLOSED instead (invalidate + retryable error). The
                    // expectation is RETAINED as a mutation fence: a LATER
                    // read on THIS FE (SHOW's confirmed read, the refresh daemon, a
                    // post-invalidation reload) must keep masking the contradicting old
                    // row until the durable table shows the expected outcome - the
                    // statement-scoped expectation alone protected only the forwarded
                    // statement itself, not the reads that follow it.
                    recordForwardedDdlFence(expected);
                    invalidatePublishedStore();
                    throw new IllegalStateException("SPM cannot confirm the forwarded GLOBAL"
                            + " DDL on this FE yet (" + expected.describe()
                            + " has not reached its expected durable outcome); the local"
                            + " baseline cache was invalidated (please retry later)");
                }
                sleepBeforeVisibilityRetry();
            }
        }
    }

    /**
     * Waits until this FE's metadata includes the FORWARDED global DDL the master already
     * completed: CREATE / ALTER / DROP BASELINE PLAN forward with
     * FORWARD_NO_SYNC, and the checkpoint-free internal reads of the refresh run locally,
     * so a follower's still-visible OLD version would be published as the confirmed
     * post-DDL state. The journal sync is the same mechanism a strong-consistency user
     * query uses (see StmtExecutor#syncJournalIfNeeded): it asks the master for
     * its max journal id and waits locally. A failure surfaces as a retryable error -
     * never as a silently published pre-DDL state.
     *
     * @param ctx the forwarded statement's context (null = skip: no way to talk to the master)
     */
    private static void syncJournalWithMaster(ConnectContext ctx) {
        if (forwardedDdlSyncForTest != null) {
            forwardedDdlSyncForTest.run();
            return;
        }
        if (FeConstants.runningUnitTest || Env.getCurrentEnv() == null
                || Env.getCurrentEnv().isMaster() || ctx == null) {
            // the master executed the DDL on its own metadata - nothing to wait for
            return;
        }
        try {
            new MasterOpExecutor(ctx).syncJournal();
        } catch (Exception e) {
            throw new IllegalStateException("SPM cannot synchronize this FE with the master"
                    + " after the forwarded GLOBAL DDL (please retry later): "
                    + e.getMessage(), e);
        }
    }

    /**
     * Applies a snapshot read by a refresh IF no local mutation published since the read
     * started (the version guard). The guard is what makes a stale snapshot harmless:
     * updateStatus publishes its in-memory flip and its version bump before a refresh can
     * reach this point, so the older row read before that update is rejected instead of
     * overwriting the newer status. Public for unit tests; production callers use
     * refreshFromInternalTable, which additionally serializes with the writers.
     *
     * @param versionAtRead the state version observed when the snapshot read started
     * @param persisted     the snapshot to apply
     * @return whether the snapshot was applied
     */
    public boolean applyRefreshedSnapshotIfUnchanged(long versionAtRead,
            Map<Long, BaselinePlan> persisted) {
        // Settle the fence obligations outside the lock (see doLoadFromTable).
        Set<Long> fenced = resolvePendingMutationFences(persisted);
        stateLock.writeLock().lock();
        try {
            if (stateVersion != versionAtRead) {
                return false;
            }
            applyRefreshedBaselinesLocked(persisted, fenced);
            return true;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * The current in-memory state version: bumped by every local mutation that changes
     * the published store. Public for unit tests (the stale-snapshot guard).
     *
     * @return the state version
     */
    public long getStateVersion() {
        stateLock.readLock().lock();
        try {
            return stateVersion;
        } finally {
            stateLock.readLock().unlock();
        }
    }

    /**
     * Applies a refreshed persisted snapshot (incremental diff):
     *
     * - ids only present in the snapshot are added (created on another FE);
     * - ids only present in memory are removed (dropped on another FE);
     * - ids present in both with changed persisted content (e.g. status changed by an ALTER
     *   on another FE) are replaced; unchanged rows keep their current object, so instance
     *   identity and the transient parameterized trees stay intact;
     * - the id generator is advanced past the largest persisted id.
     *
     * The snapshot is authoritative; callers must ensure it was read while no local mutation
     * was in flight (see refreshFromInternalTable). Public for unit tests.
     */
    public void applyRefreshedBaselines(Map<Long, BaselinePlan> persisted) {
        // Settle the fence obligations outside the lock (see doLoadFromTable).
        Set<Long> fenced = resolvePendingMutationFences(persisted);
        stateLock.writeLock().lock();
        try {
            applyRefreshedBaselinesLocked(persisted, fenced);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Applies a snapshot; the caller must hold the write lock (see refreshFromInternalTable). */
    private void applyRefreshedBaselinesLocked(Map<Long, BaselinePlan> persisted,
            Set<Long> fenced) {
        long maxId = 0;
        int added = 0;
        int updated = 0;
        for (BaselinePlan row : persisted.values()) {
            if (fenced.contains(row.getId())) {
                // keep the local post-mutation state (dropped / disabled / enabled) until
                // the durable table shows the outcome
                continue;
            }
            maxId = Math.max(maxId, row.getId());
            BaselinePlan current = baselines.get(row.getId());
            if (current == null) {
                baselines.put(row.getId(), row);
                addToHashIndex(row);
                added++;
            } else if (persistedContentChanged(current, row)) {
                removeFromHashIndex(current);
                baselines.put(row.getId(), row);
                addToHashIndex(row);
                updated++;
            } else if (copyPersistedTimestamps(current, row)) {
                // Same replay-relevant content, but the row was REWRITTEN in between (a
                // status round trip). Keeping the stale timestamps reported the T0 values
                // forever; REPLACING the object would drop the transient parameterized
                // trees, so only the persisted timestamps are adopted.
                updated++;
            }
        }
        List<Long> vanished = new ArrayList<>();
        for (Long id : baselines.keySet()) {
            if (!persisted.containsKey(id) && !fenced.contains(id)) {
                vanished.add(id);
            }
        }
        for (Long id : vanished) {
            removeFromHashIndex(baselines.remove(id));
        }
        int removed = vanished.size();
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        if (added > 0 || removed > 0 || updated > 0) {
            stateVersion++;
            LOG.info("SPM baseline refresh applied: added={}, removed={}, updated={}, total={}",
                    added, removed, updated, baselines.size());
        }
    }

    /**
     * Records (or refreshes) the fence of one completed / attempted local mutation (see
     * pendingMutationFences).
     *
     * @param id                          the baseline id
     * @param expectedStatus              the durable status the write must reach (null =
     *                                    the row must become absent)
     * @param attemptedUpdateTimeMillis   the stored second the write attempted (0 when
     *                                    unknown, e.g. a delete or a forwarded DDL)
     */
    private void recordPendingMutationFence(long id, BaselineStatus expectedStatus,
            long attemptedUpdateTimeMillis) {
        pendingMutationFences.put(id, new PendingMutationFence(expectedStatus,
                System.currentTimeMillis(), attemptedUpdateTimeMillis));
    }

    /**
     * As recordPendingMutationFence, for a write this FE PROVED committed (see the
     * confirmed flag): the fence is exempt from the age bound, so a long publication lag
     * can never unmask the id and republish the pre-mutation state.
     *
     * @param id                        the baseline id
     * @param expectedStatus            the durable status the committed write carries
     * @param attemptedUpdateTimeMillis the stored second the write carries
     */
    private void recordConfirmedMutationFence(long id, BaselineStatus expectedStatus,
            long attemptedUpdateTimeMillis) {
        pendingMutationFences.put(id, new PendingMutationFence(expectedStatus,
                System.currentTimeMillis(), attemptedUpdateTimeMillis, null, true));
    }

    /**
     * Removes the mutation fence of one id once a LATER statement's outcome was confirmed
     * (see refreshAfterForwardedDdl): the fence described a write whose publication was
     * still unresolved, but the confirmed statement ran AFTER it and its stored second is
     * strictly later, so the fence can no longer win the durable pick - keeping it would
     * only mask the id (and hold the published cache at the pre-DDL state) until the
     * fence's own bound expired.
     *
     * @param id       the id the confirmed expectation belongs to (0 = none)
     * @param snapshot the snapshot that satisfied the expectation (not modified)
     */
    private void retireSupersededMutationFence(long id, Map<Long, BaselinePlan> snapshot) {
        if (id <= 0) {
            return;
        }
        PendingMutationFence fence = pendingMutationFences.get(id);
        if (fence == null) {
            return;
        }
        if (fence.isSatisfiedBy(snapshot.get(id))) {
            // Satisfied fences are resolved by applyRefreshedBaselines below, which also
            // appends a withheld drop tombstone - retiring them here would lose it.
            return;
        }
        if (fence.droppedIdentity != null) {
            // The absence fence still OWES its tombstone proof and the snapshot contradicts
            // it: the load path keeps it masked (bounded) and appends the marker once the
            // absence is proven. A later forwarded DDL does not satisfy that proof.
            return;
        }
        pendingMutationFences.remove(id, fence);
        LOG.info("SPM mutation fence of baseline {} superseded by the confirmed outcome of a"
                + " later forwarded DDL", id);
    }

    /**
     * Fails the caller retryably when an ACTIVE mutation fence of the id was recorded for
     * a DIFFERENT outcome than the requested status. Such a fence carries a write that may
     * still PUBLISH (a committed-but-unreadable flip, or a DROP whose delete is unproven),
     * and its stored second is strictly later than everything the read below can see - so
     * a durable read that happens to MATCH the requested status is NOT the final state:
     * accepting it would publish a status the fenced write (or the next refresh) reverts.
     * The ALTER must instead fail retryably until the fence resolves.
     *
     * @param id        the baseline id
     * @param requested the status this ALTER wants to be durable
     */
    private void requireNoContradictingStatusFence(long id, BaselineStatus requested) {
        PendingMutationFence fence = pendingMutationFences.get(id);
        if (fence != null && fence.expectedStatus != requested) {
            throw new IllegalStateException("SPM cannot confirm the durable status of"
                    + " baseline " + id + "; an earlier write of this id is still"
                    + " unresolved - retry the ALTER");
        }
    }

    /**
     * Records the ABSENCE fence of a DROP whose DELETE outcome is UNCONFIRMED
     * the id is masked locally until a snapshot shows the row gone, and
     * the removed IDENTITY travels with the fence so the deletion marker
     * is appended only AFTER absence is proven - a marker written for an uncommitted
     * delete would make every later load treat the still-live row as deleted (hiding it
     * and re-issuing the delete), silently completing a DROP that reported failure.
     *
     * @param removed the row whose delete is unresolved
     */
    private void recordPendingAbsenceFence(BaselinePlan removed) {
        pendingMutationFences.put(removed.getId(), new PendingMutationFence(null,
                System.currentTimeMillis(), 0, removed));
    }

    /**
     * Retains a forwarded GLOBAL DDL's expectation as a mutation fence so
     * every LATER read on this FE keeps masking a row that contradicts it (see
     * pendingMutationFences). DROP / ALTER fenced by id; the CREATE's identity
     * expectation is NOT id-keyed (the follower never learned the id) - its
     * failure path already invalidates the store, and the daemon converges later.
     */
    private void recordForwardedDdlFence(ForwardedDdlExpectation expected) {
        if (expected != null && expected.getId() > 0) {
            recordPendingMutationFence(expected.getId(), expected.getStatus(), 0);
        }
    }

    /** For tests: whether an id currently carries a pending mutation fence. */
    @VisibleForTesting
    boolean hasPendingMutationFenceForTest(long id) {
        return pendingMutationFences.containsKey(id);
    }

    /**
     * Resolves / applies the pending mutation fences against a fresh snapshot:
     * a fence whose expected outcome the snapshot shows is done and is
     * removed; a fence the snapshot contradicts keeps its id MASKED - neither the stale
     * row is applied nor the local post-mutation state removed - until the bound expires,
     * after which the write is presumed lost and the persisted state wins again.
     *
     * @param persisted the snapshot about to be applied (not modified)
     * @return the ids whose rows the snapshot contradicts and that stay masked
     */
    private Set<Long> resolvePendingMutationFences(Map<Long, BaselinePlan> persisted) {
        if (pendingMutationFences.isEmpty()) {
            return Collections.emptySet();
        }
        Set<Long> masked = new HashSet<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<Long, PendingMutationFence> entry : pendingMutationFences.entrySet()) {
            BaselinePlan row = persisted.get(entry.getKey());
            if (entry.getValue().isSatisfiedBy(row)) {
                // A CONFIRMED absence completes the DROP's deletion marker
                // now. The tombstone was deliberately withheld while the DELETE's outcome
                // was unproven (it could have failed before commit); with the row proven
                // gone it only guards the DELAYED-commit window left - a demoted master's
                // in-flight status INSERT committing after the delete.
                if (entry.getValue().droppedIdentity != null) {
                    noteDroppedSeqState(entry.getValue().droppedIdentity);
                    noteDroppedIdState(entry.getKey());
                    LOG.info("SPM pending delete of baseline {} confirmed absent; the drop"
                            + " tombstone is appended now",
                            entry.getKey());
                }
                pendingMutationFences.remove(entry.getKey(), entry.getValue());
                continue;
            }
            if (now - entry.getValue().sinceMillis > PENDING_MUTATION_FENCE_MILLIS
                    && !entry.getValue().confirmed) {
                LOG.warn("SPM pending mutation fence for baseline {} expired before its"
                        + " durable outcome became visible; the persisted state wins",
                        entry.getKey());
                pendingMutationFences.remove(entry.getKey(), entry.getValue());
                continue;
            }
            masked.add(entry.getKey());
        }
        return masked;
    }

    /**
     * Reads the persistence-layer id watermark (MAX(id) of the baselines table, and
     * SELECT_SEQ_ID_SQL) - the id-source invariant described in the class javadoc.
     * Returns 0 when persistence is disabled (unit tests / internal schema db off). A
     * failed read is rethrown as a retryable error: createBaseline must never allocate an
     * id while the watermark is unknown.
     *
     * The sequence table is what keeps the watermark from going BACKWARDS when the row
     * holding the highest id is dropped (see
     * InternalSchema#SPM_BASELINES_SEQ_TBL_NAME), so the watermark is the greater
     * of the two. The table read is part of the same fail-visible contract: an unreadable
     * sequence fails the CREATE rather than risk handing out a used id.
     */
    private static long readPersistedWatermark() {
        if (idAllocatorStoreForTest != null) {
            return Math.max(idAllocatorStoreForTest.watermark(),
                    idAllocatorStoreForTest.seqWatermark());
        }
        if (!persistenceEnabled()) {
            return 0;
        }
        long tableWatermark;
        try {
            List<ResultRow> rows =
                    StatisticsUtil.executeQuery(SELECT_MAX_ID_SQL, Collections.emptyMap(),
                            INTERNAL_QUERY_TIMEOUT_SECONDS);
            if (rows == null || rows.isEmpty()) {
                tableWatermark = 0;
            } else {
                // an empty table yields one row with a NULL MAX(id)
                tableWatermark = parseWatermark(rows.get(0).getWithDefault(0, ""));
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM baseline id watermark read failed (retry the CREATE): " + e.getMessage(), e);
        }
        // The SEQUENCE table's MAX(last_id) was the only unbounded read on
        // this path (it scans the append-only reservation history, which grows by one row
        // per create forever, and the CREATE failed as the scan outgrew its fixed
        // timeout). The per-allocation record in the compact high-water-mark table
        // answers it in one bounded read; the legacy history is read ONCE on a cluster
        // whose rows predate that table (see readCompactIdWatermark).
        long idHighWater = readCompactIdWatermark();
        return Math.max(tableWatermark, idHighWater);
    }

    /**
     * The compact id high-water mark (see
     * SPM_BASELINES_HWM_TABLE): one bounded read. When the record is absent
     * (a cluster upgraded from before the table existed) the LEGACY full read of the
     * append-only history runs ONCE and seeds the compact record, so every later create
     * stays bounded.
     */
    private static long readCompactIdWatermark() {
        java.util.function.LongSupplier hwmSeam = hwmRecordReadForTest;
        if (hwmSeam != null) {
            // scripted stores: the record value decides, and the scoped tail read answers
            // only for a POSITIVE record (exactly like the internal tables - a cluster
            // without the record takes the legacy history path)
            long hwm = hwmSeam.getAsLong();
            if (hwm <= 0) {
                return 0;
            }
            long tail = seqTailReadForTest == null ? 0 : seqTailReadForTest.getAsLong();
            return Math.max(hwm, tail);
        }
        long hwm;
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_HWM_SQL, Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS));
            hwm = rows == null || rows.isEmpty()
                    ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id high-water-mark read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
        if (hwm > 0) {
            // The compact record is NOT authoritative on its own: it, the sequence
            // reservation and the baseline row are SEPARATE writes, and the record's own
            // write is best effort on the seed path. A reservation already VISIBLE
            // beyond the mark (HWM N committed but unreadable while sequence N is
            // readable; a successor then reads the older HWM N-1 and hands N to another
            // key, whose collision probe also misses the unpublished baseline) must
            // therefore be confirmed against the sequence table before the mark is
            // trusted. Bounded: see SELECT_SEQ_TAIL_SQL.
            long tail = readSeqTail(hwm);
            if (tail > hwm) {
                // best effort, like the seed: raising the record keeps the next create
                // on the bounded path
                writeHwmRecord(tail, false);
                return tail;
            }
            return hwm;
        }
        long legacy;
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_SEQ_ID_SQL, Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS));
            legacy = rows == null || rows.isEmpty()
                    ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id high-water-mark read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
        if (legacy > 0) {
            // best effort: a failed seed only means the next create reads the history again
            writeHwmRecord(legacy, false);
        }
        return legacy;
    }

    /**
     * The highest reservation visible beyond the compact watermark (see
     * readCompactIdWatermark). Fails closed - an unconfirmed allocation source must
     * fail the CREATE retryably rather than let an id be reused.
     */
    private static long readSeqTail(long floor) {
        Map<String, String> params = new HashMap<>();
        params.put("floor", String.valueOf(floor));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_SEQ_TAIL_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows == null || rows.isEmpty()
                    ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id high-water-mark read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * For tests: the durable pending-create lookup SQL - the BOUNDED compact read (see
     * SELECT_COMPACT_SEQ_SQL; the legacy history query it falls back to is
     * SELECT_PENDING_SEQ_SQL): the NEWEST identity must be selected first, with the
     * tombstone / marker priority applied WITHIN that identity.
     */
    @VisibleForTesting
    public static String pendingSeqLookupSqlForTest() {
        return SELECT_COMPACT_SEQ_SQL;
    }

    /** For tests: the CONFIRMED compact id watermark (see readCompactIdWatermark). */
    @VisibleForTesting
    public static long compactIdWatermarkForTest() {
        return readCompactIdWatermark();
    }

    /**
     * Appends (and prunes) the compact id high-water-mark record.
     *
     * @param id     the high-water mark to record
     * @param strict whether a failed write must fail the caller (a create that consumed
     *               an id cannot leave the record behind it); the seed path is best effort
     */
    private static void writeHwmRecord(long id, boolean strict) {
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(id));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_HWM_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            if (strict) {
                throw new RuntimeException("SPM baseline id high-water-mark write failed"
                        + " (retry the CREATE): " + e.getMessage(), e);
            }
            LOG.warn("SPM could not seed the compact id high-water-mark record ({}); the"
                    + " next create reads the append-only history again", e.getMessage());
            return;
        }
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_HWM_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.debug("SPM compact high-water-mark prune skipped: {}", e.getMessage());
        }
    }

    /** One watermark cell of an aggregate read (blank / NULL = 0). */
    private static long parseWatermark(String text) {
        return text == null || text.isEmpty() ? 0L : Long.parseLong(text.trim());
    }

    /**
     * Durably RESERVES an allocated id BEFORE its baseline row is written. The reservation
     * is an append-only row of InternalSchema#SPM_BASELINES_SEQ_TBL_NAME, so the id
     * stays used even when the baseline that held it is later dropped - a Follower that
     * never saw that row (and therefore reads a LOWER MAX(id) from the baselines table)
     * must not hand the id to a different baseline, or a delayed DROP-by-id retry would
     * delete the new one. A failed reservation fails the CREATE retryably and nothing was
     * published (the caller has not inserted the row yet).
     *
     * The reservation ALSO carries the baseline's identity + instant:
     * resolveDurablePendingCreate reads it back when a retry runs on an FE whose
     * in-memory pending registry is empty, so a committed-but-unreadable row still fences
     * the id.
     *
     * @param plan the row about to be written (its id was just allocated)
     */
    private static void reserveAllocatedId(BaselinePlan plan) {
        long id = plan.getId();
        long reserveTime = System.currentTimeMillis();
        String digest = plan.getBindSqlDigest() == null ? "" : plan.getBindSqlDigest();
        long planSqlHash = SPMUtils.hashOf(plan.getPlanSql() == null ? "" : plan.getPlanSql());
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.reserveId(id, digest, planSqlHash, reserveTime);
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(id));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(digest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        params.put("reserveTime", toTs(reserveTime));
        params.put("unconfirmed", "0");
        // Record the compact high-water mark BEFORE the history row: the
        // watermark must never lag behind an id this FE handed out, and the compact
        // record is what the per-create read depends on. A failure here fails the CREATE
        // before any identity resource exists, so the retry has no reservation to defer
        // on.
        writeHwmRecord(id, true);
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_SEQ_ID_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id reservation write failed (retry the"
                    + " CREATE): " + e.getMessage(), e);
        }
        appendCompactSeqState(id, digest, planSqlHash, reserveTime, false, false, true);
    }

    /**
     * Appends the DURABLE marker of an AMBIGUOUS create: the same
     * identity-carrying row with unconfirmed = 1. A retry on another FE (leader
     * handoff / restart, where the in-memory pendingCreates registry is empty)
     * reads it back and DEFERS instead of allocating a second id for a row that may
     * already be committed but not readable yet.
     *
     * Best effort by design: the create is already failing with the original
     * unconfirmed error, and re-masking it with a marker-write failure would lose the
     * real cause. A missing marker does NOT re-open the cross-FE hole:
     * the plain reservation row written before the baseline write carries the same
     * identity and fences the retry for the same bound.
     *
     * @param plan the row whose write is unresolved
     */
    private static void markSeqPendingUnconfirmed(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        long planSqlHash = SPMUtils.hashOf(plan.getPlanSql());
        long now = System.currentTimeMillis();
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.notePendingSeqState(plan.getBindSqlDigest(), planSqlHash,
                    plan.getId(), now);
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(plan.getId()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(plan.getBindSqlDigest()));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        params.put("reserveTime", toTs(now));
        params.put("unconfirmed", "1");
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_SEQ_ID_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.warn("SPM could not persist the unconfirmed-create marker of baseline {}"
                    + " (a retry on ANOTHER FE may allocate a second id): {}",
                    plan.getId(), e.getMessage());
            return;
        }
        appendCompactSeqState(plan.getId(), plan.getBindSqlDigest(), planSqlHash, now,
                true, false);
    }

    /**
     * Appends a DROP TOMBSTONE for one removed baseline (see
     * INSERT_SEQ_DROPPED_SQL). Best effort with a warning: the DROP itself has
     * already been reported / fenced, and failing it now would misreport the delete's
     * outcome - the marker only closes the DELAYED-commit window, and its absence
     * degrades to the previous behavior. It is only ever written for an identity that is
     * KNOWN gone (a completed delete) or CONDEMNED (a deserted write, see
     * condemnAbandonedIdentity) - never for a delete whose outcome is still
     * pending.
     *
     * @param plan the baseline this FE removed (or decided to stop matching)
     */
    private static void noteDroppedSeqState(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        writeDroppedMarker(plan.getId(), plan.getBindSqlDigest(),
                SPMUtils.hashOf(plan.getPlanSql()));
    }

    /**
     * Appends the ID-scoped tombstone of a completed DROP: any row carrying this id -
     * whatever its bind/plan identity - is dead, so a handoff collision partner that
     * was still UNREADABLE when the cleanup ran cannot revive the dropped id once it
     * publishes. Only DROP paths write it (the user asked for the whole id to be gone);
     * an unproven delete must not, or a still-live row would be hidden and re-deleted
     * on every load.
     *
     * @param id the dropped id
     */
    private static void noteDroppedIdState(long id) {
        writeDroppedMarker(id, "", 0L);
    }

    /**
     * Appends one DROP TOMBSTONE row (see INSERT_SEQ_DROPPED_SQL); best effort
     * like noteDroppedSeqState.
     *
     * @param id            the identity's id
     * @param bindSqlDigest the identity's canonical bind digest
     * @param planSqlHash   the identity's plan SQL hash
     */
    private static void writeDroppedMarker(long id, String bindSqlDigest, long planSqlHash) {
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.appendDroppedMarker(id, bindSqlDigest, planSqlHash,
                    System.currentTimeMillis());
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(id));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        params.put("reserveTime", toTs(System.currentTimeMillis()));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_SEQ_DROPPED_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
            pendingDroppedMarkers.remove(id);
        } catch (Exception e) {
            // The tombstone is only BEST EFFORT on the wire, but losing it re-opens the
            // delayed-commit window this FE just closed: keep the marker in memory so the
            // identity stays masked for THIS process, and retry the durable append from
            // the next scoped tombstone read (see readDroppedIdentities). Without the
            // in-memory half a demoted master's in-flight status write reviving the row
            // stayed visible on this FE for as long as the append kept failing.
            pendingDroppedMarkers.put(id, new PendingDropMarker(bindSqlDigest, planSqlHash));
            LOG.warn("SPM could not persist the drop tombstone of baseline {} (an"
                    + " in-flight status write may revive it); the identity stays filtered"
                    + " on this FE and the append is retried: {}", id, e.getMessage());
            return;
        }
        appendCompactSeqState(id, bindSqlDigest, planSqlHash, System.currentTimeMillis(),
                false, true);
    }

    /** One bigint FLAG cell (NULL / blank = false); see SeqReservation. */
    private static boolean isFlagSet(ResultRow row, int index) {
        if (row.getValues().size() <= index) {
            return false;
        }
        String text = row.getWithDefault(index, "");
        return text != null && !text.trim().isEmpty() && !"0".equals(text.trim());
    }

    /**
     * Reads the DROP TOMBSTONES of the GIVEN ids as id|bindSqlDigest|planSqlHash
     * keys (scoped per). A read failure propagates: loads fail
     * closed rather than publishing a row that may be a resurrection.
     *
     * @param ids the ids to look up (an empty collection skips the query entirely)
     */
    private static Set<String> readDroppedIdentities(Collection<Long> ids) {
        if (ids == null || ids.isEmpty()) {
            return Set.of();
        }
        if (idAllocatorStoreForTest != null) {
            return new java.util.HashSet<>(idAllocatorStoreForTest.droppedMarkers());
        }
        if (snapshotReaderForTest != null) {
            // the snapshot seam replaces the WHOLE durable read (a unit test has no
            // internal table): tombstones come from the allocator seam, and without one
            // there is nothing to filter against
            return Set.of();
        }
        if (!persistenceEnabled()) {
            return Set.of();
        }
        Set<String> markers = new java.util.HashSet<>();
        List<Long> chunk = new ArrayList<>(DROPPED_MARKER_ID_CHUNK);
        try {
            for (Long id : ids) {
                if (id == null) {
                    continue;
                }
                chunk.add(id);
                if (chunk.size() == DROPPED_MARKER_ID_CHUNK) {
                    readDroppedIdentitiesChunk(chunk, markers);
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                readDroppedIdentitiesChunk(chunk, markers);
            }
        } catch (Exception e) {
            throw new RuntimeException("SPM durable drop-marker read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
        // The tombstones this FE failed to append durably (see writeDroppedMarker) still
        // mask their identity - the caller asked about these very ids, so without the
        // in-memory half the delayed-commit row revived on THIS FE; the append is also
        // retried here, bounded to the ids being read.
        if (!pendingDroppedMarkers.isEmpty()) {
            Set<Long> scoped = new java.util.HashSet<>(ids);
            for (Map.Entry<Long, PendingDropMarker> entry : pendingDroppedMarkers.entrySet()) {
                if (!scoped.contains(entry.getKey())) {
                    continue;
                }
                markers.add(entry.getKey() + "|" + entry.getValue().bindSqlDigest + "|"
                        + entry.getValue().planSqlHash);
                writeDroppedMarker(entry.getKey(), entry.getValue().bindSqlDigest,
                        entry.getValue().planSqlHash);
            }
        }
        return markers;
    }

    /** One scoped page of the tombstone read (see readDroppedIdentities). */
    private static void readDroppedIdentitiesChunk(List<Long> ids, Set<String> markers)
            throws Exception {
        StringBuilder idList = new StringBuilder();
        for (Long id : ids) {
            if (idList.length() > 0) {
                idList.append(',');
            }
            idList.append(id);
        }
        Map<String, String> params = new HashMap<>();
        params.put("ids", idList.toString());
        List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                SELECT_SEQ_DROPPED_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
        if (rows == null) {
            return;
        }
        for (ResultRow row : rows) {
            List<String> values = row.getValues();
            if (values == null || values.size() < 3 || values.get(0) == null
                    || values.get(1) == null || values.get(2) == null) {
                continue;
            }
            markers.add(values.get(0).trim() + "|" + values.get(1) + "|"
                    + values.get(2).trim());
        }
    }

    /** The ids of one row list (see readDroppedIdentities). */
    private static List<Long> rowIds(List<BaselinePlan> rows) {
        List<Long> ids = new ArrayList<>(rows.size());
        for (BaselinePlan row : rows) {
            if (row != null) {
                ids.add(row.getId());
            }
        }
        return ids;
    }

    /** The tombstone key of one row / plan: id|bindSqlDigest|planSqlHash. */
    private static String droppedIdentityKey(BaselinePlan plan) {
        return droppedIdentityKey(plan.getId(),
                plan.getBindSqlDigest() == null ? "" : plan.getBindSqlDigest(),
                SPMUtils.hashOf(plan.getPlanSql() == null ? "" : plan.getPlanSql()));
    }

    /** The identity key of one id / digest / hash triple (see droppedIdentityKey). */
    private static String droppedIdentityKey(long id, String bindSqlDigest, long planSqlHash) {
        return id + "|" + (bindSqlDigest == null ? "" : bindSqlDigest) + "|" + planSqlHash;
    }

    /** The key of the ID-scoped tombstone (see noteDroppedIdState). */
    private static String droppedIdKey(long id) {
        return id + "||0";
    }

    /**
     * Whether one row's identity carries a readable DROP TOMBSTONE: the
     * user's DROP completed, so the row is not adoptable any more even while its own
     * DELETE lags its publication. A tombstone READ failure propagates (the caller fails
     * retryably instead of adopting a row it cannot clear).
     */
    private static boolean isTombstonedIdentity(BaselinePlan plan) {
        return plan != null && plan.getId() > 0
                && isTombstonedRow(readDroppedIdentities(List.of(plan.getId())), plan);
    }

    /** Whether one row is covered by a read tombstone set: its own identity, or the
     * ID-scoped marker of a completed DROP (see noteDroppedIdState). */
    private static boolean isTombstonedRow(Set<String> markers, BaselinePlan row) {
        return markers.contains(droppedIdentityKey(row))
                || markers.contains(droppedIdKey(row.getId()));
    }

    /**
     * Removes every row whose identity carries a DROP TOMBSTONE: the row
     * was deleted by a completed DROP and revived afterwards by a delayed write (a
     * demoted master's in-flight status INSERT committing after the delete). Matching it
     * would make the dropped baseline ACTIVE again. The rows are also repaired away
     * (best effort - a follower's load cannot write).
     *
     * @param snapshot the rows read from the durable table (not mutated)
     * @return the rows without resurrected ones
     */
    private static Map<Long, BaselinePlan> filterResurrectedRows(
            Map<Long, BaselinePlan> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return snapshot == null ? Map.of() : snapshot;
        }
        // The tombstone read is scoped to the SNAPSHOT's ids - the
        // append-only sequence table keeps every historical drop marker, and an
        // unrestricted read grew with the CREATE / DROP churn until it timed out and
        // follower caches stopped applying later GLOBAL changes.
        Set<String> tombstones = readDroppedIdentities(snapshot.keySet());
        if (tombstones.isEmpty()) {
            return snapshot;
        }
        Map<Long, BaselinePlan> filtered = new java.util.LinkedHashMap<>(snapshot);
        for (BaselinePlan row : snapshot.values()) {
            if (!isTombstonedRow(tombstones, row)) {
                continue;
            }
            filtered.remove(row.getId());
            LOG.warn("SPM ignores and repairs a durable baseline {} revived after its DROP"
                    + " (an in-flight write committed after the delete)", row.getId());
            try {
                persistDeleteByIdentity(row);
            } catch (RuntimeException e) {
                LOG.warn("SPM cannot repair the revived baseline {} yet ({}); it stays"
                        + " hidden until the next leader repairs it", row.getId(),
                        e.getMessage());
            }
        }
        return filtered;
    }

    /**
     * Removes the rows whose IDENTITY was DROPPED from a POINT read taken for adoption
     * . Reading a row back is not enough to make it adoptable: a DROP's own
     * identity delete can lag its tombstone (the tombstone is written first, a demoted
     * master's in-flight status write revived the row, and the delete is not readable
     * yet), so the durable-key dedup of a CREATE or the by-id cache-miss reconciliation
     * could hand back a baseline the user already dropped - the CREATE then reported
     * success for a row that disappears the moment the delete becomes readable, and the
     * ALTER could modify an incarnation that is already gone. The row's repair delete is
     * retried here as well (best effort - a follower's read cannot write), so a point
     * read and the snapshot load of filterResurrectedRows can never disagree.
     *
     * @param rows the rows of one key / id read from the durable table (not mutated)
     * @return the rows without dropped identities
     */
    private static List<BaselinePlan> filterTombstonedDurableRows(List<BaselinePlan> rows) {
        if (rows == null || rows.isEmpty()) {
            return rows == null ? new ArrayList<>() : rows;
        }
        Set<String> tombstones = readDroppedIdentities(rowIds(rows));
        if (tombstones.isEmpty()) {
            return rows;
        }
        List<BaselinePlan> surviving = new ArrayList<>(rows.size());
        for (BaselinePlan row : rows) {
            if (row == null || !isTombstonedRow(tombstones, row)) {
                surviving.add(row);
                continue;
            }
            LOG.warn("SPM ignores and repairs a durable baseline {} revived after its DROP"
                    + " (an in-flight write committed after the delete)", row.getId());
            try {
                persistDeleteByIdentity(row);
            } catch (RuntimeException e) {
                LOG.warn("SPM cannot repair the revived baseline {} yet ({}); it stays"
                        + " hidden until the next leader repairs it", row.getId(),
                        e.getMessage());
            }
        }
        return surviving;
    }

    /**
     * Retires the durable UNCONFIRMED marker(s) of a RESOLVED ambiguous write
     * #7): once the reserved row became readable (or was retired as stale), the identity
     * no longer fences - without this the marker outlived its resolution and rejected a
     * legitimate DROP + immediate re-CREATE of the same bind/plan for up to
     * DURABLE_PENDING_CREATE_FENCE_MILLIS, since the probe saw the old marker
     * and the now-absent row. The DELETE removes only unconfirmed = 1 markers of
     * THAT last_id; the plain reservation row appended before every create (and every
     * other id ever reserved) stays, so the sequence WATERMARK never falls. Best effort:
     * a failed delete leaves the fence in place, which only delays a re-create.
     *
     * @param plan     the baseline whose marker is retired
     * @param markerId the id the marker was appended for
     */
    private static void retireSeqPendingMarker(BaselinePlan plan, long markerId) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        long planSqlHash = SPMUtils.hashOf(plan.getPlanSql());
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.retirePendingSeqState(plan.getBindSqlDigest(),
                    planSqlHash, markerId);
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(plan.getBindSqlDigest()));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        params.put("lastId", String.valueOf(markerId));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(DELETE_PENDING_SEQ_MARKER_SQL,
                    params, BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.warn("SPM could not retire the unconfirmed-create marker of baseline {}"
                    + " (a later re-create of the key may defer until the fence expires): {}",
                    markerId, e.getMessage());
            return;
        }
        // The retired marker must also disappear from the compact slot: appending the
        // resolved (plain) state keeps the slot's newest row the authoritative one.
        appendCompactSeqState(markerId, plan.getBindSqlDigest(), planSqlHash,
                System.currentTimeMillis(), false, false);
    }

    /** The compact `id` of one identity (see COMPACT_SEQ_ID_BASE). */
    private static long compactSeqId(String bindSqlDigest, long planSqlHash) {
        long mixed = SPMUtils.hashOf(bindSqlDigest + '\u0001' + planSqlHash);
        return COMPACT_SEQ_ID_BASE + (mixed & 0x3FFFFFFFFFFFFFFFL);
    }

    /**
     * Appends the COMPACT copy of one identity state (see COMPACT_SEQ_ID_BASE); best
     * effort like the marker / tombstone appends - a missing copy only means the next
     * identity read falls back to the legacy history query once, and the next state
     * change writes a fresh copy anyway. The prune keeps the identity's own slot small.
     *
     * @param lastId        the id this state belongs to
     * @param bindSqlDigest the identity's canonical bind digest
     * @param planSqlHash   the identity's plan SQL hash
     * @param reserveTime   the state's instant (second precision)
     * @param unconfirmed   whether this state is the marker of an ambiguous write
     * @param dropped       whether this state is a drop tombstone
     */
    private static void appendCompactSeqState(long lastId, String bindSqlDigest,
            long planSqlHash, long reserveTime, boolean unconfirmed, boolean dropped) {
        appendCompactSeqState(lastId, bindSqlDigest, planSqlHash, reserveTime, unconfirmed,
                dropped, false);
    }

    /**
     * As appendCompactSeqState(long, String, long, long, boolean, boolean); with strict =
     * true a failed append THROWS instead of degrading: the RESERVATION path uses it,
     * because a compact slot that silently lagged the history let an older tombstone
     * masquerade as this identity's newest state - another FE then allocated a second
     * id for the same key while the first reservation's row could still publish (see
     * resolveDurablePendingCreate). The append runs BEFORE the baseline row is
     * attempted, so failing the CREATE here leaves nothing committed behind the id.
     */
    private static void appendCompactSeqState(long lastId, String bindSqlDigest,
            long planSqlHash, long reserveTime, boolean unconfirmed, boolean dropped,
            boolean strict) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("compactId", String.valueOf(compactSeqId(bindSqlDigest, planSqlHash)));
        params.put("lastId", String.valueOf(lastId));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        params.put("reserveTime", toTs(reserveTime));
        params.put("unconfirmed", unconfirmed ? "1" : "0");
        params.put("dropped", dropped ? "1" : "0");
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_COMPACT_SEQ_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            if (strict) {
                throw new RuntimeException("SPM compact identity reservation write failed"
                        + " (retry the CREATE): " + e.getMessage(), e);
            }
            LOG.warn("SPM could not append the compact identity row of baseline {} (the next"
                    + " identity read falls back to the sequence history): {}", lastId,
                    e.getMessage());
            return;
        }
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_COMPACT_SEQ_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.debug("SPM compact identity prune skipped: {}", e.getMessage());
        }
    }

    /**
     * The DURABLE half of the unconfirmed-create fence. The in-memory
     * pendingCreates registry is per-FE: after a leader handoff (or a restart)
     * the retry of a committed-but-unpublished CREATE runs on an FE that never saw the
     * write, so the durable-key read misses the unreadable row too - the retry then
     * allocated a SECOND id and both rows later published ENABLED. The UNCONFIRMED
     * MARKER the ambiguous create appended to the sequence table (see
     * markSeqPendingUnconfirmed) carries the baseline's identity and IS visible
     * (the reviewer's scenario: "the sequence reservation may already be visible"): a
     * fresh marker whose baseline row is still unreadable DEFERS the retry; once the row
     * publishes it is ADOPTED, and an expired marker is treated as a lost write (see
     * DURABLE_PENDING_CREATE_FENCE_MILLIS).
     *
     * The PLAIN reservation row fences exactly like the marker: it is
     * appended BEFORE the baseline write, so while it is young and its id is unreadable
     * the write it describes may still be in flight - fencing on it closes the hole a
     * FAILED marker append used to leave open (a retry on another FE then allocated a
     * second id next to the possibly-committed write). It never holds back a legitimate
     * re-create: a readable row is adopted (the retry reports the SAME baseline instead
     * of duplicating it), and an unreadable one is condemned once the fence expires.
     *
     * @param plan the CREATE's baseline
     * @return the ADOPTED id when the marker's row became readable, else null (the create
     *         proceeds: no ambiguous write of this key exists, or its fence expired)
     */
    private Long resolveDurablePendingCreate(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return null;
        }
        long planSqlHash = SPMUtils.hashOf(plan.getPlanSql());
        SeqReservation reservation;
        if (idAllocatorStoreForTest != null) {
            reservation = idAllocatorStoreForTest.pendingSeqReservation(plan.getBindSqlDigest(),
                    planSqlHash);
        } else {
            if (!persistenceEnabled()) {
                return null;
            }
            reservation = readPersistedSeqReservation(plan.getBindSqlDigest(), planSqlHash);
        }
        if (reservation == null) {
            return null;
        }
        boolean fenceExpired = System.currentTimeMillis() - reservation.reserveTimeMs
                > DURABLE_PENDING_CREATE_FENCE_MILLIS;
        if ((reservation.dropped || fenceExpired) && idAllocatorStoreForTest == null) {
            // The compact slot may LAG the history (a marker / tombstone append is best
            // effort; only the reservation append is strict). Before acting on a
            // "resolved" or expired compact state, reconcile against the legacy history
            // and continue from whichever state is NEWER: a re-CREATE whose reservation
            // compact append was lost stayed hidden behind an older tombstone, and
            // ANOTHER FE then allocated a second id for the same key - both rows could
            // later publish as ENABLED.
            SeqReservation newest = pickNewerSeqState(reservation,
                    readLegacySeqNewest(plan.getBindSqlDigest(), planSqlHash));
            if (newest != null && !sameSeqState(newest, reservation)) {
                LOG.warn("SPM durable pending create of baseline {}: the compact identity"
                                + " state lagged the history (compact id {}, history state"
                                + " id {}); continuing from the newer state",
                        reservation.id, reservation.id, newest.id);
                reservation = newest;
            }
        }
        if (reservation.dropped) {
            // The identity was RESOLVED by a tombstone (a completed DROP, or a deserted
            // write condemned after its fence expired): the key may be created again
            // under a fresh id, and no row of the old incarnation may be adopted.
            return null;
        }
        if (probeDurableRow(reservation.id, plan.getBindSqlDigest(), plan.getPlanSql())
                == DurablePresence.PRESENT) {
            BaselinePlan readable = readableIdentityRow(reservation.id, plan);
            if (readable == null) {
                // the id no longer carries this baseline: fall through, the normal create
                // path owns the outcome
                return null;
            }
            if (!Objects.equals(readable.getSchemaFingerprint(),
                    plan.getSchemaFingerprint())) {
                // The write committed under schema F1 and an ALTER TABLE
                // changed the schema to F2 while it was still unreadable. Adopting F1
                // would report success for a row every replay rejects as stale - the
                // in-memory registry path replaces exactly this case. Retire the stale
                // row (best effort: the durable-key check below retires it as soon as a
                // read sees it) and let the normal path allocate a fresh row under F2.
                try {
                    persistDeleteByIdentity(readable);
                } catch (RuntimeException e) {
                    LOG.warn("SPM failed to retire the stale durable pending-create row"
                            + " (id={}): {}", reservation.id, e.getMessage());
                }
                // the marker described THAT write; with the row retired it must not fence
                // a later retry either
                retireSeqPendingMarker(plan, reservation.id);
                LOG.warn("SPM durable pending create of baseline {}: its schema fingerprint"
                                + " changed ({} -> {}); replacing the stale committed row",
                        reservation.id, readable.getSchemaFingerprint(),
                        plan.getSchemaFingerprint());
                return null;
            }
            // readable now: ADOPT the reserved row - the same path as the in-memory
            // registry, and the only way the retry returns the id the first write
            // consumed instead of allocating a second one
            publishBaseline(readable);
            // The resolution RETIRES the durable marker. Leaving it behind
            // made a DROP + immediate re-CREATE of the same bind/plan defer for up to
            // DURABLE_PENDING_CREATE_FENCE_MILLIS: the probe saw the old marker and the
            // (dropped) row's absence. The marker's DELETE keeps the plain reservation
            // row, so the id watermark never falls.
            retireSeqPendingMarker(plan, reservation.id);
            LOG.info("SPM durable pending create of baseline {} adopted from the durable"
                    + " table", readable.getId());
            return readable.getId();
        }
        if (System.currentTimeMillis() - reservation.reserveTimeMs
                <= DURABLE_PENDING_CREATE_FENCE_MILLIS) {
            throw new IllegalStateException("SPM cannot create baseline: a previously COMMITTED"
                    + " write of the same baseline (id " + reservation.id + ") is still awaiting"
                    + " publication (its id is consumed); retry the statement");
        }
        // The fence elapsed, but elapsed time is NOT a terminal outcome -
        // the write may still be COMMITTED with its publication lagging, and a fresh id
        // allocated next to it would let BOTH enabled rows publish (a same-key duplicate
        // pair). The abandoned identity is therefore CONDEMNED with an append-only
        // tombstone BEFORE the fresh id is allocated: if its row ever becomes readable,
        // every load treats it as deleted (and repairs it away) instead of publishing a
        // second enabled baseline; if the write was truly lost, the tombstone is inert.
        condemnAbandonedIdentity(reservation.id, plan.getBindSqlDigest(), planSqlHash);
        // The condemnation must be DURABLE before a fresh id is allocated: the marker
        // write is best effort on the wire, and a restart / handoff that only lost the
        // process-local marker would let this FE's retry allocate a second id whose row
        // can publish beside the old COMMITTED-but-unreadable one (both ENABLED).
        // Failing the retry here keeps the id watermarked and re-runs the
        // condensation (idempotent append) until it is readable.
        confirmDroppedMarkerDurable(reservation.id, plan.getBindSqlDigest(), planSqlHash);
        LOG.warn("SPM durable pending create of baseline {}: its identity record is older"
                        + " than {} ms and the row never became readable; condemning the"
                        + " identity (any late publication is repaired away) and allocating a"
                        + " fresh id",
                reservation.id, DURABLE_PENDING_CREATE_FENCE_MILLIS);
        return null;
    }

    /**
     * Condemns one abandoned create identity: the append-only tombstone of
     * a write this FE gave up on. If the underlying INSERT was COMMITTED and its row
     * publishes later, every load filters it (and repairs it away) - the alternative was
     * a second ENABLED row published next to the fresh baseline the retry allocated.
     *
     * @param id           the abandoned id
     * @param bindSqlDigest the baseline's canonical bind digest
     * @param planSqlHash   the hash of the baseline's plan SQL
     */
    private static void condemnAbandonedIdentity(long id, String bindSqlDigest,
            long planSqlHash) {
        writeDroppedMarker(id, bindSqlDigest, planSqlHash);
    }

    /**
     * Confirms one condemnation tombstone is READABLE before the caller allocates a
     * fresh id (see resolveDurablePendingCreate): the marker append is best effort on
     * the wire, so it is retried bounded, and a marker that never becomes readable
     * fails the CREATE retryably - allocating a second id next to a
     * COMMITTED-but-unreadable row whose only protection lived in this process's
     * memory let both rows publish as ENABLED after a restart / handoff.
     */
    private static void confirmDroppedMarkerDurable(long id, String bindSqlDigest,
            long planSqlHash) {
        if (idAllocatorStoreForTest != null || !persistenceEnabled()) {
            return;
        }
        String key = droppedIdentityKey(id, bindSqlDigest, planSqlHash);
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            writeDroppedMarker(id, bindSqlDigest, planSqlHash);
            if (readDroppedIdentities(Set.of(id)).contains(key)) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        throw new IllegalStateException("SPM cannot confirm the condemnation tombstone of"
                + " the abandoned baseline id " + id + "; retry the CREATE");
    }

    /**
     * Reads the latest identity-carrying reservation of one baseline (see
     * SELECT_PENDING_SEQ_SQL); null when none / an unparsable row (a pre-identity
     * row carries NULL and never matches the filter).
     */
    private static SeqReservation readPersistedSeqReservation(String bindSqlDigest,
            long planSqlHash) {
        Map<String, String> params = new HashMap<>();
        params.put("compactId", String.valueOf(compactSeqId(bindSqlDigest, planSqlHash)));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_COMPACT_SEQ_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                // Fallback: the compact slot has no row for this identity - a pre-upgrade
                // row, a failed / lagging compact append, or an id collision whose prune
                // removed the slot. The legacy history query is correct, only unbounded,
                // and it is now the exception instead of the per-create rule.
                rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                        SELECT_PENDING_SEQ_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            }
            if (rows == null || rows.isEmpty()) {
                return null;
            }
            return parseSeqReservation(rows.get(0));
        } catch (Exception e) {
            // fail closed like every other watermark read: without the answer the create
            // cannot prove it is not a duplicate
            throw new RuntimeException("SPM baseline id sequence identity read failed (retry the"
                    + " CREATE): " + e.getMessage(), e);
        }
    }

    /** One identity row's state, from either read (compact and history share the
     * column tuple last_id / reserve_time / unconfirmed / dropped). */
    private static SeqReservation parseSeqReservation(ResultRow row) {
        String idText = row.getWithDefault(0, "");
        if (idText == null || idText.isEmpty()) {
            return null;
        }
        String timeText = row.getValues().size() > 1 ? row.getWithDefault(1, "") : "";
        long reserveTime = timeText == null || timeText.isEmpty()
                ? 0 : fromTs(timeText.trim());
        return new SeqReservation(Long.parseLong(idText.trim()), reserveTime,
                isFlagSet(row, 2), isFlagSet(row, 3));
    }

    /**
     * The newest identity state in the HISTORY table (the legacy identity-scoped query,
     * see SELECT_PENDING_SEQ_SQL); null when the table holds no row for the identity.
     * Only the rare decision points call it (a resolved or expired compact state, see
     * resolveDurablePendingCreate): the compact slot stays the bounded primary read.
     * A failed read fails the caller retryably, like every other identity read.
     */
    private static SeqReservation readLegacySeqNewest(String bindSqlDigest, long planSqlHash) {
        Map<String, String> params = new HashMap<>();
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_PENDING_SEQ_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows == null || rows.isEmpty() ? null : parseSeqReservation(rows.get(0));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id sequence identity read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * The newer of two identity states, by the identity read's own total order (see
     * SELECT_PENDING_SEQ_SQL): last_id, then reserve_time, then the tombstone / marker
     * flags.
     */
    private static SeqReservation pickNewerSeqState(SeqReservation first,
            SeqReservation second) {
        if (first == null) {
            return second;
        }
        if (second == null) {
            return first;
        }
        if (first.id != second.id) {
            return first.id > second.id ? first : second;
        }
        if (first.reserveTimeMs != second.reserveTimeMs) {
            return first.reserveTimeMs > second.reserveTimeMs ? first : second;
        }
        if (first.dropped != second.dropped) {
            return first.dropped ? first : second;
        }
        if (first.unconfirmed != second.unconfirmed) {
            return first.unconfirmed ? first : second;
        }
        return first;
    }

    /** Whether two identity states describe the same row state (see pickNewerSeqState). */
    private static boolean sameSeqState(SeqReservation first, SeqReservation second) {
        return first.id == second.id && first.reserveTimeMs == second.reserveTimeMs
                && first.dropped == second.dropped && first.unconfirmed == second.unconfirmed;
    }

    /**
     * Builds the persisted snapshot from the internal table: one BaselinePlan per row with
     * the transient trees rebuilt exactly like the startup load does. Invalid rows are
     * skipped with a warning (the next cycle retries).
     *
     * The read is PAGINATED and ordered by id: the table has no retention cap, so one
     * SELECT * had to return every row within the fixed per-query timeout - a
     * snapshot that outgrew it failed as a whole and never converged (the follower kept
     * its old published cache, local baseline DDL waited behind the held writer lock, and
     * confirmed SHOW / post-forward refreshes failed on the same path forever). Each page
     * is bounded by SNAPSHOT_PAGE_SIZE rows and its own timeout, and the loop
     * walks the id space forward until a short page ends the snapshot.
     */
    private static Map<Long, BaselinePlan> readPersistedSnapshot() throws Exception {
        Map<Long, BaselinePlan> snapshot = snapshotReaderForTest != null
                ? snapshotReaderForTest.get()
                : readStableSnapshot(BaselineManager::readSnapshotPage,
                        BaselineManager::readSnapshotFence);
        // a durable row whose identity carries a DROP TOMBSTONE must never reach the
        // cache: an in-flight status INSERT of a demoted master can commit
        // after the DROP deleted the row and revive it as an ACTIVE baseline
        return filterResurrectedRows(snapshot);
    }

    /** One page of the whole-table snapshot, read through the internal table. */
    private static List<ResultRow> readSnapshotPage(Long pageStart, long offset) throws Exception {
        return inInternalIoMode(() -> StatisticsUtil.executeQuery(
                snapshotPageSql(pageStart, offset), Collections.emptyMap(),
                INTERNAL_QUERY_TIMEOUT_SECONDS));
    }

    /**
     * Reads the paginated snapshot only if the table was UNCHANGED for the whole read
     * (see SELECT_SNAPSHOT_FENCE_SQL): the fence is read before and after the page
     * loop and the read is retried while it moved. The publisher of the returned map can
     * therefore treat it as a single-point-in-time state.
     *
     * DDL is rare compared to refreshes, so one retry normally converges; a table that
     * never stays stable (a write storm) fails CLOSED with a retryable exception instead of
     * publishing a mixed state - every caller re-reads on the next cycle / retry (the
     * refresh daemon, SHOW, load).
     *
     * @param reader      reads one snapshot page (inclusive lower bound on the id)
     * @param fenceReader reads the SnapshotFence token
     * @return the rows of one stable state, collapsed per id
     * @throws Exception when a read fails or the table never stays stable
     */
    @VisibleForTesting
    static Map<Long, BaselinePlan> readStableSnapshot(SnapshotPageReader reader,
            SnapshotFenceReader fenceReader) throws Exception {
        for (int attempt = 1; ; attempt++) {
            SnapshotFence before = fenceReader.readFence();
            AtomicLong rowsRead = new AtomicLong();
            Map<Long, BaselinePlan> snapshot =
                    collectSnapshotPages(reader, SNAPSHOT_PAGE_SIZE, rowsRead);
            SnapshotFence after = fenceReader.readFence();
            // BOTH conditions are required: the fence proves the table did not change around
            // the read, and the row count proves every row of the fenced state was actually
            // READ. The second check is what turns a silently TRUNCATED read (e.g. an
            // internal-query row limit cancelling a page - a partial result looks exactly
            // like a short, completed page) into a retryable failure instead of a snapshot
            // that drops baselines from the cache.
            if (before.matches(after) && rowsRead.get() == before.rowCount) {
                return snapshot;
            }
            LOG.warn("SPM baseline table changed while its paginated snapshot was read"
                            + " ({} -> {}, rows read {}/{}, attempt {}/{})",
                    before, after, rowsRead.get(), before.rowCount, attempt,
                    SNAPSHOT_STABILITY_ATTEMPTS);
            if (attempt >= SNAPSHOT_STABILITY_ATTEMPTS) {
                throw new IllegalStateException("SPM baseline table kept changing while its"
                        + " paginated snapshot was read (concurrent CREATE / ALTER / DROP,"
                        + " or a truncated read; retry the operation)");
            }
        }
    }

    /** The fence of one paginated snapshot read (see readStableSnapshot). */
    @VisibleForTesting
    static final class SnapshotFence {
        final long maxId;
        final long rowCount;
        final String maxUpdateTime;

        SnapshotFence(long maxId, long rowCount, String maxUpdateTime) {
            this.maxId = maxId;
            this.rowCount = rowCount;
            this.maxUpdateTime = maxUpdateTime == null ? "" : maxUpdateTime;
        }

        boolean matches(SnapshotFence other) {
            return maxId == other.maxId && rowCount == other.rowCount
                    && maxUpdateTime.equals(other.maxUpdateTime);
        }

        @Override
        public String toString() {
            return "(maxId=" + maxId + ", rows=" + rowCount
                    + ", maxUpdateTime=" + maxUpdateTime + ")";
        }
    }

    /** Reads the fence token of the snapshot read (see readStableSnapshot). */
    @FunctionalInterface
    interface SnapshotFenceReader {
        SnapshotFence readFence() throws Exception;
    }

    /** Reads SELECT_SNAPSHOT_FENCE_SQL (one all-NULL row when the table is empty). */
    private static SnapshotFence readSnapshotFence() throws Exception {
        List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                SELECT_SNAPSHOT_FENCE_SQL, Collections.emptyMap(),
                INTERNAL_QUERY_TIMEOUT_SECONDS));
        if (rows == null || rows.isEmpty()) {
            return new SnapshotFence(0, 0, "");
        }
        ResultRow row = rows.get(0);
        String maxId = row.getWithDefault(0, "");
        String count = row.getWithDefault(1, "");
        String maxUpdateTime = row.getValues().size() > 2 ? row.getWithDefault(2, "") : "";
        return new SnapshotFence(
                maxId == null || maxId.isEmpty() ? 0 : Long.parseLong(maxId.trim()),
                count == null || count.isEmpty() ? 0 : Long.parseLong(count.trim()),
                maxUpdateTime == null ? "" : maxUpdateTime.trim());
    }

    /**
     * The pagination loop of readPersistedSnapshot: walks the id space forward
     * until a page comes back shorter than SNAPSHOT_PAGE_SIZE. Every row is read
     * EXACTLY ONCE and no row is ever skipped - the two properties the before/after fence
     * alone cannot see.
     *
     * The next page continues from the LAST ROW READ, not from the row after its id: the
     * inclusive bound is the trailing row's id and the offset is the number of rows of
     * that id group already consumed. An id group larger than one page (a repeated
     * opposite-status ALTER failure leaves one more row under the id every time) is read to
     * its end instead of being cut after the first page - the omitted rows could carry the
     * NEWEST durable status, and the winner resolution would resurrect the old one.
     * Reading each row once also makes the number of rows read a VALID completeness proof:
     * readStableSnapshot compares it with the fence's row count, so a silently
     * truncated page (e.g. an internal-query row limit cancelling the query) fails the
     * refresh closed instead of publishing a partial snapshot.
     *
     * Package-visible with an injectable page reader so the loop (which no unit test can
     * drive through the internal table) is covered directly.
     *
     * @param reader       reads one page: every row with id >= pageStart after
     *                     skipping offset rows of that range, in the TOTAL order
     *                     (id, update_time, status); a null pageStart reads the
     *                     first page (offset 0)
     * @param pageSize     rows per page (the production value is
     *                     SNAPSHOT_PAGE_SIZE)
     * @param rowsReadSink counts every row the loop actually read (see the completeness
     *                     check of readStableSnapshot)
     * @return the accumulated snapshot
     * @throws Exception when a page read fails (the caller retries the whole refresh)
     */
    @VisibleForTesting
    static Map<Long, BaselinePlan> collectSnapshotPages(SnapshotPageReader reader, int pageSize,
            AtomicLong rowsReadSink) throws Exception {
        Map<Long, BaselinePlan> snapshot = new HashMap<>();
        Long bound = null;
        long skipped = 0;
        while (true) {
            List<ResultRow> rows = reader.readPage(bound, skipped);
            if (rows == null || rows.isEmpty()) {
                return snapshot;
            }
            rowsReadSink.addAndGet(rows.size());
            for (ResultRow row : rows) {
                accumulateSnapshotRow(snapshot, row);
            }
            String lastRowId = rows.get(rows.size() - 1).getWithDefault(0, "");
            if (rows.size() < pageSize || lastRowId.isEmpty()) {
                return snapshot; // a short page ends the snapshot
            }
            long pageLastId = Long.parseLong(lastRowId.trim());
            long trailing = 0;
            for (int i = rows.size() - 1; i >= 0; i--) {
                String rowId = rows.get(i).getWithDefault(0, "");
                if (rowId.isEmpty() || Long.parseLong(rowId.trim()) != pageLastId) {
                    break;
                }
                trailing++;
            }
            if (bound != null && pageLastId == bound) {
                skipped += trailing; // still inside the bound's id group
            } else {
                bound = pageLastId;
                skipped = trailing;
            }
        }
    }

    /**
     * collectSnapshotPages(SnapshotPageReader, int, AtomicLong) without the row
     * counter: for tests that only inspect the accumulated snapshot.
     */
    @VisibleForTesting
    static Map<Long, BaselinePlan> collectSnapshotPages(SnapshotPageReader reader, int pageSize)
            throws Exception {
        return collectSnapshotPages(reader, pageSize, new AtomicLong());
    }

    /** Reads one page of the snapshot (see collectSnapshotPages). */
    @FunctionalInterface
    interface SnapshotPageReader {
        List<ResultRow> readPage(Long pageStart, long offset) throws Exception;
    }

    /**
     * The read of ONE snapshot page: every row with id >= pageStart, ordered by
     * the TOTAL order (id, update_time, status) and skipping the first
     * offset rows of that range, or the whole table (in the same order) when the
     * snapshot has just started.
     *
     * @param pageStart inclusive lower bound of the page id range (null = first page)
     * @param offset    rows to skip inside the id range (the continuation within an id
     *                  group larger than one page; 0 for a fresh bound)
     * @return the page SQL
     */
    @VisibleForTesting
    static String snapshotPageSql(Long pageStart, long offset) {
        if (pageStart == null) {
            return SELECT_ALL_ORDERED_SQL + " LIMIT " + SNAPSHOT_PAGE_SIZE;
        }
        return SELECT_PAGE_SQL.replace("${lastId}", Long.toString(pageStart))
                .replace("${pageSize}", Integer.toString(SNAPSHOT_PAGE_SIZE))
                .replace("${offset}", Long.toString(offset));
    }

    /**
     * Merges one raw snapshot row into the accumulated snapshot, resolving an id carried
     * by more than one row deterministically.
     */
    private static void accumulateSnapshotRow(Map<Long, BaselinePlan> snapshot,
            ResultRow row) {
        try {
            BaselinePlan p = parsePersistedRow(row);
            BaselinePlan previous = snapshot.put(p.getId(), p);
            if (previous != null) {
                // Two rows carry the same id (e.g. an ALTER status update whose
                // compensating delete failed, or an out-of-contract manual write): the
                // read order must not decide, so "last read wins" would make refresh /
                // restart decide the status NONDETERMINISTICALLY - a failed DISABLE could
                // be silently re-enabled. Pick a deterministic winner (see
                // pickDurableWinner) so every FE / restart converges on it.
                BaselinePlan winner = pickDurableWinner(previous, p);
                snapshot.put(p.getId(), winner);
                LOG.warn("SPM persisted baseline id {} appears in more than one row"
                                + " (statuses {} / {}, update times {} / {});"
                                + " deterministically keeping the {} row",
                        p.getId(), previous.getStatus(), p.getStatus(),
                        previous.getUpdateTime(), p.getUpdateTime(), winner.getStatus());
            }
        } catch (Throwable t) {
            LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
        }
    }

    /**
     * Rebuilds one persisted row exactly like the startup load does: one BaselinePlan
     * with its transient (parameterized) trees rebuilt from the stored bindSql / planSql.
     *
     * @param row the internal-table row
     * @return the parsed row
     * @throws Exception when the row's bindSql cannot be parsed
     */
    private static BaselinePlan parsePersistedRow(ResultRow row) throws Exception {
        BaselinePlan p = fromRow(row);
        String planSql = p.getPlanSql();
        // Fail closed on legacy temporary-table rows: before the create-time rejection,
        // a GLOBAL baseline over a temporary table froze the CREATOR session's internal
        // table name into planSql (sessionId_#TEMP#_name). Replaying it from another
        // session would read the creator's (possibly still live) temporary table, so such
        // rows are never loaded (the log line names the row).
        if (referencesTemporaryTable(p.getBindSql()) || referencesTemporaryTable(planSql)) {
            throw new RuntimeException("SPM baseline " + p.getId()
                    + " references a temporary table (the frozen plan carries the creator"
                    + " session's internal name); skipping the row");
        }
        // Classify with the PERSISTED provenance first (plan_frozen), falling back to the
        // parse-based classifier for pre-column rows: the fallback path stores the
        // ORIGINAL planSql when the decompiler rejects a node, and that ordinary SQL may
        // merely CONTAIN a placeholder function name inside a string literal / identifier
        // / comment. A raw substring test would skip rebuilding the parameterized plan
        // tree for such a row - after a reload the replay would return the CAPTURED
        // literals and the fallback tree was gone.
        boolean frozen = SPMPlanner.isFrozenPlanSql(planSql, p.getPlanFrozen());
        if (!frozen && Boolean.FALSE.equals(p.getPlanFrozen())
                && SPMPlanner.isFrozenPlanSql(planSql, null)) {
            // A row explicitly flagged NOT frozen whose planSql nonetheless re-parses
            // into REAL placeholder calls (not a mere literal / identifier carrying the
            // name - that is exactly what the flag was added to protect against): the
            // text is SPM's own decompiled rendering and the flag is stale. Pre-provenance
            // rows migrated with a default flag, and older releases recorded false for a
            // successful marker-free decompile (see SPMPlanner#buildBaselineFromSql).
            // Replaying such a row through the parameterized fallback tree would also be
            // WRONG: the tree is rebuilt from an ALREADY parameterized text, so the
            // reconstructed placeholder ids no longer line up with the values extracted
            // from the bind tree, the residue safety net rejects the rewrite and the
            // baseline silently never applies. Treat the TEXT as the authority here.
            LOG.warn("SPM baseline {} is flagged NOT frozen but its planSql re-parses into"
                    + " placeholder calls; treating the row as frozen", p.getId());
            frozen = true;
            // keep the in-memory provenance consistent with the decision: the rewrite
            // path re-checks planFrozen (SPMPlanner#rewriteFromFrozenTree) and would
            // otherwise reject the very text this row depends on
            p.setPlanFrozen(Boolean.TRUE);
        }
        // Rebuild the transient trees with ONE shared builder over both texts in
        // the CREATE order (bind first, then plan), so the placeholder ids of the
        // two trees stay aligned and a value extracted from the bind tree can
        // never be substituted into a literal slot of the other tree. Frozen
        // (placeholder-carrying) planSql is replayed as text - no plan tree. The
        // non-frozen planSql is either the SPM decompiled text (MODE_DEFAULT) or the
        // user's raw fallback text (creator mode) - plan_sql_mode carries which one.
        long planSqlMode = p.getPlanSqlMode() == null
                ? SqlModeHelper.MODE_DEFAULT : p.getPlanSqlMode();
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                p.getBindSql(), frozen ? null : planSql, p.getCreatorSqlMode(), planSqlMode);
        if (trees.first == null) {
            throw new RuntimeException("SPM baseline " + p.getId()
                    + " bindSql cannot be parsed");
        }
        p.setParameterizedBindPlan(trees.first);
        if (!frozen) {
            p.setParameterizedPlanPlan(trees.second);
        }
        return p;
    }

    /**
     * Whether a stored text REFERENCES the creator session's temporary-table name. The
     * marker is looked up in the PARSED relations only - never as a raw substring: an
     * ordinary predicate / value literal (s = '_#TEMP#_') or a comment carries the
     * same characters, and the old substring test rejected such a durable row on every
     * refresh - the baseline silently disappeared from every FE although CREATE had
     * accepted it (the create-time guard inspects the RESOLVED relations).
     */
    private static boolean referencesTemporaryTable(String text) {
        if (text == null || !text.contains(FeNameFormat.TEMPORARY_TABLE_SIGN)) {
            return false;
        }
        try {
            Plan parsed = new NereidsParser().parseSingle(text);
            if (!(parsed instanceof LogicalPlan)) {
                return false;
            }
            final boolean[] referenced = {false};
            SPMPlanTreeSupport.<RuntimeException>walkPlans(parsed, (Plan node) -> {
                if (node instanceof UnboundRelation) {
                    for (String part : ((UnboundRelation) node).getNameParts()) {
                        if (part.contains(FeNameFormat.TEMPORARY_TABLE_SIGN)) {
                            referenced[0] = true;
                        }
                    }
                }
            });
            return referenced[0];
        } catch (RuntimeException e) {
            // unparsable text: fail closed. The row can be neither rebuilt nor replayed,
            // and an unverifiable marker must not be trusted (the previous substring test
            // did reject these rows; only PARSEABLE texts may prove they are clean).
            return true;
        }
    }

    /**
     * For tests: rebuilds one persisted row exactly like the startup load / periodic
     * refresh does (including the hint-bearing bindSql path, which must be parsed WITHOUT a
     * session and without executing the hint's SET_VAR side effects).
     *
     * @param row the internal-table row
     * @return the parsed row
     * @throws Exception when the row's bindSql cannot be parsed
     */
    @VisibleForTesting
    public static BaselinePlan parsePersistedRowForTest(ResultRow row) throws Exception {
        return parsePersistedRow(row);
    }

    /**
     * Durable dedup lookup of the CREATE path: served from the store's key INDEX while
     * the store is complete for this table, and from the durable table only when the
     * table carries a row this store has never seen.
     *
     * Every GLOBAL CREATE used to filter bind_sql_digest / plan_sql in SQL, but the
     * table is keyed and distributed only by id: that predicate scans EVERY bucket and
     * row, while the table grows without a cap (auto capture), so the lookup eventually
     * ran into its fixed timeout and CREATE slowed down / failed as baselines
     * accumulated. The store already holds every row (the load reads them all) plus
     * every local write, and it is invalidated + reloaded on promotion / forwarded DDL,
     * so the in-memory index answers correctly whenever the table has no NEWER id than
     * the store has seen (mustScanDurableForKey); only a newer id - another
     * master's write, an out-of-band insert, a load that could not run - requires the
     * complete (scanned) answer, which is then paid for.
     *
     * @param plan           the baseline being created
     * @param tableWatermark MAX(id) of the durable table, read by the caller
     * @return the rows carrying the plan's (bind_sql_digest, plan_sql) key
     */
    private List<BaselinePlan> readPersistedRowsForCreate(BaselinePlan plan, long tableWatermark) {
        if (mustScanDurableForKey(tableWatermark) || plan.getBindSqlHash() == 0) {
            // the store may miss durable rows (or cannot index this key): the scanned
            // read is the only COMPLETE answer
            return readPersistedByKey(plan.getBindSqlDigest(), plan.getPlanSql());
        }
        return storeRowsByKey(plan.getBindSqlHash(), plan.getBindSqlDigest(), plan.getPlanSql());
    }

    /**
     * Whether the durable by-key scan is unavoidable: true when the table's MAX(id) is
     * above the largest id this store has seen. Ids are allocated upward only (single
     * writer = the master), so a MAX(id) the store has already passed proves every
     * durable row is present in memory; a HIGHER id means at least one row is not.
     *
     * @param tableWatermark the table's MAX(id) (0 for an empty table)
     */
    @VisibleForTesting
    boolean mustScanDurableForKey(long tableWatermark) {
        return tableWatermark > maxPersistedIdSeen;
    }

    /**
     * The store's rows with exactly the given (bind_sql_digest, plan_sql) key, looked up
     * through the same hash index the phase-1 duplicate check uses (an O(1) path - the
     * point of the indexed dedup).
     */
    private List<BaselinePlan> storeRowsByKey(long bindSqlHash, String bindSqlDigest,
            String planSql) {
        List<BaselinePlan> result = new ArrayList<>();
        stateLock.readLock().lock();
        try {
            for (BaselinePlan row : findByHash(bindSqlHash)) {
                if (Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                        && Objects.equals(row.getPlanSql(), planSql)) {
                    result.add(row);
                }
            }
        } finally {
            stateLock.readLock().unlock();
        }
        return result;
    }

    /**
     * Reads every durable row with the given (bind_sql_digest, plan_sql) key. A read
     * failure is rethrown as a retryable error: createBaseline must not fall through to
     * an INSERT while the durable duplicate state is unknown.
     *
     * @param bindSqlDigest the parameterized digest
     * @param planSql       the frozen plan SQL
     * @return the parsed rows (possibly empty)
     */
    private static List<BaselinePlan> readPersistedByKey(String bindSqlDigest, String planSql) {
        Map<String, String> params = new HashMap<>();
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSql", StatisticsUtil.escapeSQL(planSql));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_BY_KEY_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            List<BaselinePlan> result = new ArrayList<>();
            for (ResultRow row : rows) {
                try {
                    result.add(parsePersistedRow(row));
                } catch (Throwable t) {
                    LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
                }
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM durable-key read failed (retry the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * Deterministic winner of duplicate rows carrying one id (the internal table is a
     * DUPLICATE-key table: the ALTER-status protocol INSERTs the new-status row before
     * deleting the old-status one, so a failure of BOTH the old-row delete and the
     * compensating delete leaves both rows behind).
     *
     * Rule: the LATER updateTime wins - it is the user's latest intent and, in the
     * failure case above, the newly inserted row. When the timestamps tie (DATETIME has
     * second precision), prefer DISABLED: silently re-enabling a baseline the user tried
     * to disable is the harmful direction (it would start rewriting query plans again),
     * while a failed ENABLE left disabled only means no rewrite - and a retried ENABLE
     * converges. The selection is total and order-independent, so refresh / restart /
     * every FE agree on the same row.
     *
     * @param first  one of the rows
     * @param second the other row
     * @return the row to keep in memory
     */
    @VisibleForTesting
    static BaselinePlan pickDurableWinner(BaselinePlan first, BaselinePlan second) {
        if (first.getUpdateTime() != second.getUpdateTime()) {
            return first.getUpdateTime() > second.getUpdateTime() ? first : second;
        }
        boolean firstDisabled = !first.getStatus().isActive();
        boolean secondDisabled = !second.getStatus().isActive();
        if (firstDisabled != secondDisabled) {
            return firstDisabled ? first : second;
        }
        // Fully tied (same stored second, same status): the winner must be DETERMINISTIC
        // - two masters can leave two DIFFERENT rows of one id with the
        // same stored second (a delayed INSERT committing after a handoff collision),
        // and an arbitrary pick made refresh / restart / SHOW / the paginated read
        // disagree on which row is authoritative. The CONTENT is a total,
        // order-independent tie-breaker; fully identical rows stay interchangeable.
        int byDigest = String.valueOf(first.getBindSqlDigest())
                .compareTo(String.valueOf(second.getBindSqlDigest()));
        if (byDigest != 0) {
            return byDigest < 0 ? first : second;
        }
        int byPlan = String.valueOf(first.getPlanSql())
                .compareTo(String.valueOf(second.getPlanSql()));
        if (byPlan != 0) {
            return byPlan < 0 ? first : second;
        }
        int byBind = String.valueOf(first.getBindSql())
                .compareTo(String.valueOf(second.getBindSql()));
        return byBind <= 0 ? first : second;
    }

    /**
     * Forces an authoritative reload of the internal table (master acquisition): the
     * in-memory cache may have been loaded long BEFORE this FE became master, so it can
     * miss every row the previous master wrote after that load - the create-time key
     * dedup would then miss an existing durable baseline. loaded is cleared
     * first, so a failed read keeps the lazy-retry state machine intact (the next access
     * retries; mutators fail visibly via ensureLoadedOrThrow until the read succeeds).
     */
    public void forceReloadFromInternalTable() {
        if (!persistenceEnabled()) {
            return;
        }
        invalidatePublishedStore();
        // NEVER read the shared table synchronously here: Env calls this on the
        // master-transfer path, and the read (which inherits StatisticsUtil's temporary
        // context) used to stall master readiness / ordinary queries far beyond the
        // advertised SPM budget when the table's tablet / BE was unavailable. The
        // background load - retried by the refresh daemon as well - publishes the fresh
        // snapshot when it arrives.
        scheduleAsyncLoad();
    }

    /**
     * Invalidates the published store for an authoritative reload: clears the maps
     * together with loaded. Clearing ONLY loaded left the OLD maps visible:
     * if the internal-table read failed, hasBaselines / findCandidateBaselines would retry
     * the load and then still read the populated maps - a newly promoted FE could apply a
     * baseline the previous master had already disabled or dropped. Matching now sees an
     * EMPTY store until a fresh snapshot is atomically published.
     */
    private void invalidatePublishedStore() {
        // Serialize the invalidation with the WRITERS: a DROP / status change holds
        // writerLock across its persist + in-memory removal, and if this method cleared
        // the maps in between, the DROP would find no map entry afterwards (skipping its
        // stateVersion bump) while a load that read the table BEFORE the DROP committed
        // could still republish the deleted row. Taking the writers' lock makes the
        // invalidation impossible to interleave; the generation bump below additionally
        // rejects every snapshot whose READ started before the invalidation.
        synchronized (writerLock) {
            storeGeneration.incrementAndGet();
            stateLock.writeLock().lock();
            try {
                loaded = false;
                baselines.clear();
                hashIndex.clear();
                // Pending-create records are NOT cleared here: they describe
                // writes THIS FE committed, and a reload cannot see an unpublished row.
                // Clearing them let a re-promoted FE load a snapshot before the row
                // published, see no key duplicate and assign a retry a SECOND id - both
                // rows later published ENABLED and dropping the returned id left the
                // other one ACTIVE. A record whose row never becomes readable is retired
                // by the fence bound in resolvePendingCreate.
                stateVersion++;
            } finally {
                stateLock.writeLock().unlock();
            }
        }
    }

    /**
     * For tests: the invalidation half of forceReloadFromInternalTable (the
     * production caller follows it with the reload).
     */
    @VisibleForTesting
    public void invalidatePublishedStoreForTest() {
        invalidatePublishedStore();
    }

    /**
     * Whether a persisted row differs from the in-memory baseline in a REPLAY-RELEVANT
     * way. A dropped highest-id baseline can be recreated under that id after a referenced
     * table's unused column changed, and a follower that keeps its old object (same SQL /
     * measured fields) would then reject the newly valid baseline with the stale
     * fingerprint on every later refresh - so every persisted field that takes part in
     * planning / matching / replay is compared here.
     *
     * The TIMESTAMPS are NOT part of this comparison: they are adopted by
     * copyPersistedTimestamps instead, which keeps the object identity (and with
     * it the transient parameterized trees) while still reporting the persisted values.
     * Comparing them here would REPLACE the object on every rewrite - and IGNORING them
     * completely (the previous behavior) lost a status ROUND TRIP: a follower caches
     * ENABLED at T0, the master completes DISABLE at T1 and ENABLE at T2 before the
     * follower's next refresh, and every compared field is back to its T0 value, so the
     * fresh T2 row was discarded and the follower reported the stale T0 object forever
     * (SHOW included, since the authoritative read merges through here).
     */
    private static boolean persistedContentChanged(BaselinePlan memory, BaselinePlan row) {
        return !Objects.equals(memory.getBindSql(), row.getBindSql())
                || !Objects.equals(memory.getBindSqlDigest(), row.getBindSqlDigest())
                || memory.getBindSqlHash() != row.getBindSqlHash()
                || !Objects.equals(memory.getPlanSql(), row.getPlanSql())
                || !Objects.equals(memory.getQueryId(), row.getQueryId())
                || Double.compare(memory.getCost(), row.getCost()) != 0
                || memory.getQueryTimeMs() != row.getQueryTimeMs()
                || memory.getSource() != row.getSource()
                || memory.getCreatorSqlMode() != row.getCreatorSqlMode()
                || !Objects.equals(memory.getPlanSqlMode(), row.getPlanSqlMode())
                || !Objects.equals(memory.getPlanFrozen(), row.getPlanFrozen())
                || !Objects.equals(memory.getSchemaFingerprint(), row.getSchemaFingerprint())
                || memory.getStatus() != row.getStatus();
    }

    /**
     * Adopts the persisted create / update timestamps into an UNCHANGED cached object (see
     * persistedContentChanged).
     *
     * The comparison is at the internal table's DATETIME (SECOND) precision: memory
     * keeps millis while the row is written / read back truncated, so a raw comparison
     * would report EVERY row as rewritten every cycle.
     *
     * @param memory the cached object (mutated in place)
     * @param row    the persisted row
     * @return whether a timestamp was adopted (i.e. the row had been rewritten)
     */
    private static boolean copyPersistedTimestamps(BaselinePlan memory, BaselinePlan row) {
        boolean copied = false;
        if (!sameStoredSecond(memory.getUpdateTime(), row.getUpdateTime())) {
            memory.setUpdateTime(row.getUpdateTime());
            copied = true;
        }
        if (!sameStoredSecond(memory.getCreateTime(), row.getCreateTime())) {
            memory.setCreateTime(row.getCreateTime());
            copied = true;
        }
        return copied;
    }

    /**
     * Whether two epoch-millis values are the same at the internal table's DATETIME
     * (SECOND) precision (see copyPersistedTimestamps).
     */
    private static boolean sameStoredSecond(long memoryMillis, long rowMillis) {
        return memoryMillis / 1000L == rowMillis / 1000L;
    }

    private void addToHashIndex(BaselinePlan p) {
        hashIndex.computeIfAbsent(p.getBindSqlHash(), k -> new ArrayList<>()).add(p.getId());
    }

    private void removeFromHashIndex(BaselinePlan p) {
        if (p == null) {
            return;
        }
        List<Long> ids = hashIndex.get(p.getBindSqlHash());
        if (ids != null) {
            ids.remove(p.getId());
            if (ids.isEmpty()) {
                hashIndex.remove(p.getBindSqlHash());
            }
        }
    }

    /** Rebuilds a BaselinePlan from one internal-table row (column order = schema). */
    private static BaselinePlan fromRow(ResultRow row) throws Exception {
        BaselinePlan p = new BaselinePlan();
        p.setId(Long.parseLong(row.get(0)));
        p.setBindSql(row.get(1));
        p.setBindSqlDigest(row.get(2));
        p.setBindSqlHash(Long.parseLong(row.get(3)));
        p.setPlanSql(row.get(4));
        p.setQueryId(row.get(5));
        p.setCost(Double.parseDouble(row.get(6)));
        p.setQueryTimeMs(Long.parseLong(row.get(7)));
        p.setSource(BaselineSource.fromString(row.get(8)));
        p.setStatus(BaselineStatus.fromString(row.get(9)));
        p.setCreateTime(fromTs(row.get(10)));
        p.setUpdateTime(fromTs(row.get(11)));
        // older rows (or a hand-built test row) may not carry the column yet
        p.setCreatorSqlMode(row.getValues().size() > 12 ? parseSqlMode(row.get(12))
                : SqlModeHelper.MODE_DEFAULT);
        // provenance columns added later: a legacy row keeps null (no explicit
        // classification / no schema binding), which the load path handles as "classify
        // by parsing" / "cannot validate".
        p.setPlanSqlMode(row.getValues().size() > 13 ? parseNullableLong(row.get(13)) : null);
        p.setPlanFrozen(row.getValues().size() > 14 ? parseNullableBoolean(row.get(14)) : null);
        p.setSchemaFingerprint(row.getValues().size() > 15 ? row.get(15) : null);
        // NULL / empty on a pre-column row: the plan-side identity of the row is not
        // usable, callers fall back to the bind digest alone.
        p.setPlanSqlDigest(row.getValues().size() > 16 ? row.get(16) : null);
        return p;
    }

    /** A NULL / empty / unparsable sql_mode column means "created before the column". */
    private static long parseSqlMode(String text) {
        Long value = parseNullableLong(text);
        return value == null ? SqlModeHelper.MODE_DEFAULT : value;
    }

    /** A NULL / empty / unparsable BIGINT column decodes to null ("not persisted"). */
    private static Long parseNullableLong(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** A NULL / empty / unparsable BOOLEAN column decodes to null ("not persisted"). */
    private static Boolean parseNullableBoolean(String text) {
        if (text == null || text.isEmpty()) {
            return null;
        }
        String trimmed = text.trim();
        if ("true".equalsIgnoreCase(trimmed) || "1".equals(trimmed)) {
            return Boolean.TRUE;
        }
        if ("false".equalsIgnoreCase(trimmed) || "0".equals(trimmed)) {
            return Boolean.FALSE;
        }
        return null;
    }

    /**
     * The CONDITIONAL INSERT half of a status flip (see
     * INSERT_IF_PREVIOUS_STATUS_SQL): the new-status row is coupled to the
     * PREVIOUS-status row still being durable.
     *
     * An ALTER dispatched before a handoff - or merely stalled - could otherwise
     * INSERT the requested status AFTER the new master completed a DROP of that very
     * baseline: nothing was left to refuse it (the old row was gone, so the delete-old
     * step removed zero rows and the visibility confirmation passed), the resurrected row
     * survived the completed DROP and the next refresh restored an ACTIVE baseline. The
     * conditional statement performs the presence check INSIDE the same durable write, so
     * a vanished previous row writes nothing; the conflict is then reported as a
     * retryable failure instead of a silent success.
     */
    private static boolean persistTransitionInsert(BaselinePlan p, BaselineStatus previousStatus) {
        if (!persistenceEnabled() && idAllocatorStoreForTest == null
                && statusProtocolStoreForTest == null) {
            return true; // in-memory only (no durable status rows exist)
        }
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.insert(p);
        } else if (statusProtocolStoreForTest != null) {
            if (!statusProtocolStoreForTest.insertIfPreviousPresent(p, previousStatus)) {
                return false; // nothing written: the previous-status row was gone
            }
        } else {
            if (!writeConditionalStatusInsert(p, previousStatus)) {
                return false; // nothing written: the conditional SELECT matched no row
            }
        }
        confirmInsertVisible(p);
        return true;
    }

    /**
     * Whether the conditional status INSERT actually WROTE a row: a conditional
     * INSERT ... SELECT ... WHERE ... that matches no row reports SQL OK with ZERO
     * affected rows, and that count is the only durable signal that separates "wrote
     * nothing" from "written but not yet readable" (see updateStatus). An
     * unknown count (-1) is NOT proof of a zero write, so it counts as written and is then
     * subject to the visibility confirmation.
     */
    @VisibleForTesting
    static boolean insertWroteRows(long affectedRows) {
        return affectedRows != 0;
    }

    /**
     * Runs the conditional status INSERT and reports whether a row was actually WRITTEN.
     *
     * INSERT ... SELECT ... WHERE status = previousStatus reports SQL OK with
     * ZERO affected rows when the previous-status row is gone (a concurrent DROP, or a
     * handoff flip that already moved the status away and back): the requested status was
     * NOT written and a plain visibility confirmation would misread the no-op as a
     * publication lag (the reviewer's old-leader example). The affected-row count is the
     * only durable signal that separates the two, so the caller can refuse to publish an
     * unobserved status.
     *
     * @return true when the new-status row was written; false when the statement matched no
     *         previous-status row and wrote nothing
     */
    private static boolean writeConditionalStatusInsert(BaselinePlan p,
            BaselineStatus previousStatus) {
        Map<String, String> params = insertParams(p);
        params.put("previousStatus", previousStatus.name());
        try {
            QueryState state = inInternalIoMode(() -> StatisticsUtil.execUpdate(
                    INSERT_IF_PREVIOUS_STATUS_SQL, params, BASELINE_WRITE_TIMEOUT_SECONDS));
            long affectedRows = state == null ? -1 : state.getAffectedRows();
            if (!insertWroteRows(affectedRows)) {
                LOG.warn("SPM persist (status insert) wrote no row for baseline {}: no durable"
                        + " {} row was left to flip (a concurrent DROP / status flip won)",
                        p.getId(), previousStatus);
                return false;
            }
            return true;
        } catch (Exception e) {
            // An INSERT that reports an error (typically a statement timeout) may still
            // have COMMITTED: the row carrying this id + key + the REQUESTED status AT
            // THE ATTEMPTED STORED SECOND is the proof it landed (the previous-status row
            // would not prove it - it is exactly the row the conditional statement must
            // not have matched; and the status ALONE could be a STALE row left by a
            // previously failed old-row delete - ).
            if (observedInsertRowIsOurs(p)) {
                LOG.warn("SPM persist (status insert) reported {} but the row is durable"
                        + " (id={}); keeping it", e.getMessage(), p.getId());
                return true;
            }
            throw new RuntimeException("SPM persist (status insert) failed: " + e.getMessage(), e);
        }
    }

    /** The retryable conflict of a status flip whose conditional INSERT matched no row. */
    private static IllegalStateException statusConflict(long id, BaselineStatus previousStatus) {
        return new IllegalStateException("SPM cannot change the status of baseline " + id
                + ": its " + previousStatus + " row was gone when the conditional flip"
                + " wrote (a concurrent DROP or status flip won); retry the statement");
    }

    private static void persistInsert(BaselinePlan p) {
        if (idAllocatorStoreForTest != null) {
            try {
                idAllocatorStoreForTest.insert(p);
            } catch (RuntimeException e) {
                // an ambiguous SIMULATOR outcome takes the same path as the real one
                // the write may have landed, so it must not fail like a
                // genuine non-commit
                throwAmbiguousInsertUnlessDurable(p, e);
            }
            confirmInsertVisible(p);
            return;
        }
        if (statusProtocolStoreForTest != null) {
            try {
                statusProtocolStoreForTest.insert(p);
            } catch (RuntimeException e) {
                throwAmbiguousInsertUnlessDurable(p, e);
            }
            confirmInsertVisible(p);
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = insertParams(p);
        try {
            inInternalIoMode(() -> {
                StatisticsUtil.execUpdate(INSERT_SQL, params, BASELINE_WRITE_TIMEOUT_SECONDS);
                return null;
            });
        } catch (Exception e) {
            // An INSERT that reports an error (typically a statement timeout) may still
            // have COMMITTED: reconcile against the durable table before failing the
            // write. The row carrying this id + key + REQUESTED STATUS + the ATTEMPTED
            // stored SECOND is the proof it landed: matching only (id, key) treated the
            // still-present OLD-status row of an ALTER as the freshly written new-status
            // row, and the status alone could be satisfied by a STALE row a previously
            // failed old-row delete left behind. An outcome that cannot be
            // PROVEN counts as AMBIGUOUS, never as a genuine failure.
            throwAmbiguousInsertUnlessDurable(p, e);
        }
        // A reported SUCCESS still does not prove the row is READABLE: the default insert
        // return mode accepts SQL OK with the transaction merely COMMITTED (publication
        // timed out). Publishing the id into this FE's cache and returning it would let a
        // leadership change before publication re-allocate the same id from an older
        // MAX(id) - both rows then become visible and the winner rule silently discards
        // one version.
        confirmInsertVisible(p);
    }

    /** The ${...}-parameter map of one INSERT statement (shared by both insert shapes). */
    private static Map<String, String> insertParams(BaselinePlan p) {
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(p.getId()));
        params.put("bindSql", StatisticsUtil.escapeSQL(p.getBindSql()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("bindSqlHash", String.valueOf(p.getBindSqlHash()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        params.put("queryId", StatisticsUtil.escapeSQL(p.getQueryId() == null ? "" : p.getQueryId()));
        params.put("cost", String.valueOf(p.getCost()));
        params.put("queryTimeMs", String.valueOf(p.getQueryTimeMs()));
        params.put("source", p.getSource().name());
        params.put("status", p.getStatus().name());
        params.put("createTime", toTs(p.getCreateTime()));
        params.put("updateTime", toTs(p.getUpdateTime()));
        params.put("sqlMode", String.valueOf(p.getCreatorSqlMode()));
        // NULL (not "MODE_DEFAULT") for a row that predates the provenance columns: the
        // load path must keep classifying such rows by parsing / by the creator mode.
        params.put("planSqlMode",
                p.getPlanSqlMode() == null ? "NULL" : String.valueOf(p.getPlanSqlMode()));
        params.put("planFrozen", p.getPlanFrozen() == null ? "NULL" : p.getPlanFrozen().toString());
        params.put("schemaFingerprint",
                StatisticsUtil.escapeSQL(p.getSchemaFingerprint() == null
                        ? "" : p.getSchemaFingerprint()));
        params.put("planSqlDigest",
                StatisticsUtil.escapeSQL(p.getPlanSqlDigest() == null
                        ? "" : p.getPlanSqlDigest()));
        return params;
    }

    /**
     * A reported-successful INSERT whose row is not READABLE yet (see
     * confirmInsertVisible). The write itself carries EVIDENCE - the affected-row
     * count of the conditional status INSERT, or the committed-write probe - so the caller
     * may treat it as durable: a CREATE must not hand the id out again (its retry defers
     * instead of allocating a second id, see pendingCreates), and a status flip
     * may publish the row it proves (see updateStatus).
     */
    private static final class UnconfirmedInsertException extends IllegalStateException {
        UnconfirmedInsertException(String message) {
            super(message);
        }

        /**
         * The ambiguous-output form: the INSERT reported an ERROR that may
         * have been raised AFTER a commit, so the original cause travels with the
         * unconfirmed report.
         */
        UnconfirmedInsertException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Confirms a reported-successful INSERT (or the insert half of a status flip) is
     * READABLE before its id may be published / returned. Bounded retries cover a short
     * publication lag; a row that never becomes readable fails the write RETRYABLY while
     * the caller keeps no in-memory state (the row itself may still become visible, and
     * a retry adopts it through the durable-key dedup). Simulator stores are synchronous
     * and confirm nothing unless the visibility seam is set (that is where a test
     * simulates the committed-but-invisible window).
     *
     * @param p the row that was just written
     * @throws UnconfirmedInsertException the write reported success but no read sees it
     */
    private static void confirmInsertVisible(BaselinePlan p) {
        if (durableVisibilityProbeForTest == null
                && (idAllocatorStoreForTest != null || statusProtocolStoreForTest != null)) {
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            boolean readable = durableVisibilityProbeForTest != null
                    ? durableVisibilityProbeForTest.isReadable(p.getId(), p.getStatus(),
                            p.getUpdateTime())
                    : observedInsertRowIsOurs(p);
            if (readable) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        throw new UnconfirmedInsertException("SPM persist (insert) reported success but baseline "
                + p.getId() + " is not readable yet; an id no read can see could be"
                + " re-allocated after a leadership change - retry the statement");
    }

    /**
     * Confirms a reported-successful identity DELETE left no READABLE row behind (the
     * caller removes its cache entry only afterwards).
     *
     * An elapsed probe budget is NOT a failed delete: the statement reported SQL OK
     * with the transaction COMMITTED (the default return mode), so the row is durably
     * GONE and only its publication lags behind the probes. Failing the DROP here was the
     * worse choice in both directions: the master kept an ACTIVE cache entry that
     * ordinary queries kept replaying until the next refresh (the DROP had already
     * landed), and the retried DROP tried to delete a row that no longer existed. Treat
     * the reported success as the durable outcome - fail CLOSED - and log the lag.
     *
     * @param p the deleted row
     */
    private static void confirmIdentityGone(BaselinePlan p) {
        if (durableVisibilityProbeForTest == null
                && (idAllocatorStoreForTest != null || statusProtocolStoreForTest != null)) {
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            boolean gone = durableVisibilityProbeForTest != null
                    ? !durableVisibilityProbeForTest.isReadable(p.getId(), null)
                    : probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql())
                            == DurablePresence.ABSENT;
            if (gone) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        LOG.warn("SPM persist (delete) reported success and baseline {} is still readable"
                + " after {} probes; the committed delete is the durable outcome, removing"
                + " the row from the cache", p.getId(), BASELINE_VISIBILITY_ATTEMPTS);
    }

    /**
     * Confirms the OLD-status row of a reported-successful status flip is gone durably
     * (the caller then publishes the flip). Real-store only: the status seam simulators
     * are synchronous.
     *
     * Like confirmIdentityGone, an elapsed probe budget is NOT a failure: the
     * delete reported SQL OK with the transaction COMMITTED, so the row is durably gone
     * and only its publication lags. Failing here instead bounced the caller into the
     * ambiguous-outcome reconciliation (which kept both rows and reported a spurious
     * failure) although the flip had already landed - the durable winner is the
     * freshly inserted new-status row.
     *
     * @param id     the baseline id
     * @param status the status whose row must be gone
     */
    private static void confirmStatusRowGone(long id, BaselineStatus status) {
        if (durableVisibilityProbeForTest == null
                && (idAllocatorStoreForTest != null || statusProtocolStoreForTest != null)) {
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            boolean gone;
            try {
                gone = durableVisibilityProbeForTest != null
                        ? !durableVisibilityProbeForTest.isReadable(id, status)
                        : durableRowCount(id, status) == 0;
            } catch (RuntimeException e) {
                gone = false; // unconfirmable: retry, then treat the reported success as final
            }
            if (gone) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        LOG.warn("SPM persist (delete by status) reported success and baseline {} still carries"
                + " status {} after {} probes; the committed delete is the durable outcome",
                id, status, BASELINE_VISIBILITY_ATTEMPTS);
    }

    private static void sleepBeforeVisibilityRetry() {
        try {
            Thread.sleep(BASELINE_VISIBILITY_RETRY_MILLIS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Outcome of one ambiguous-write reconciliation read (see probeDurableRow).
     */
    private enum DurablePresence { PRESENT, ABSENT, UNKNOWN }

    /**
     * Reconciles an ambiguous write: whether the durable table holds the row with this
     * (id, key). Identity deletes need no status constraint - DELETE_BY_IDENTITY removes
     * every status row of the key, so the presence of any of them proves the delete did
     * NOT land; INSERT reconciliation passes the status it wrote (see below).
     */
    private static DurablePresence probeDurableRow(long id, String bindSqlDigest, String planSql) {
        return probeDurableRow(id, bindSqlDigest, planSql, null);
    }

    /**
     * Reconciles an ambiguous write: whether the durable table holds a row with this
     * (id, key) AND - when status is given - the EXPECTED status. A read FAILURE
     * answers UNKNOWN - never ABSENT: an absent row is the ONLY proof a DELETE landed,
     * and treating an unconfirmable read as proof let dropBaseline remove the cached row
     * and report success while the durable row stayed (the next refresh / restart
     * resurrected the dropped baseline).
     *
     * The status constraint is what makes an INSERT reconciliation sound: during an
     * ALTER, persistInsert(durablePlan) can throw BEFORE committing while the old-status
     * row is still present under the SAME (id, digest, planSql) key - an unconstrained
     * probe declared the INSERT successful, updateStatus deleted the old row and the
     * baseline was left with NO durable row at all.
     */
    private static DurablePresence probeDurableRow(long id, String bindSqlDigest, String planSql,
            BaselineStatus status) {
        return probeDurableRow(id, bindSqlDigest, planSql, status, null);
    }

    /**
     * As above, with an optional SCHEMA FINGERPRINT constraint: the duplicate fast path
     * of a CREATE must not accept a REUSED id whose row carries the same key but a
     * different incarnation (it may have been recreated after the referenced schema
     * changed, which is exactly what the baseline would then reject at replay).
     */
    private static DurablePresence probeDurableRow(long id, String bindSqlDigest, String planSql,
            BaselineStatus status, String schemaFingerprint) {
        return probeDurableRow(id, bindSqlDigest, planSql, status, schemaFingerprint, null);
    }

    /**
     * As above, with an optional STORED-SECOND constraint. The requested status ALONE is
     * weak evidence of a status-flip INSERT: a previously failed old-row DELETE leaves a
     * STALE row of that very status beside the winner ( - a leftover ENABLED
     * row made an ENABLE whose conditional INSERT aborted look "written"), and
     * confirmInsertVisible then accepted the old row as the new row's
     * publication. A flip stores its row STRICTLY LATER than every row it met (see
     * updateStatus' stored-second bump), so requiring the ATTEMPTED second distinguishes
     * the row THIS write would have produced from any older same-status row.
     */
    private static DurablePresence probeDurableRow(long id, String bindSqlDigest, String planSql,
            BaselineStatus status, String schemaFingerprint, Long requiredUpdateTimeMs) {
        if (idAllocatorStoreForTest != null) {
            try {
                for (BaselinePlan row : idAllocatorStoreForTest.readById(id)) {
                    if (row.getId() == id
                            && Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                            && Objects.equals(row.getPlanSql(), planSql)
                            && (status == null || row.getStatus() == status)
                            && (schemaFingerprint == null || Objects.equals(
                                    row.getSchemaFingerprint(), schemaFingerprint))
                            && (requiredUpdateTimeMs == null || sameStoredSecond(
                                    row.getUpdateTime(), requiredUpdateTimeMs))) {
                        return DurablePresence.PRESENT;
                    }
                }
                return DurablePresence.ABSENT;
            } catch (RuntimeException e) {
                LOG.warn("SPM cannot reconcile the ambiguous write of baseline {}: {}",
                        id, e.getMessage());
                return DurablePresence.UNKNOWN;
            }
        }
        try {
            for (BaselinePlan row : readPersistedByKey(bindSqlDigest, planSql)) {
                if (row.getId() == id && (status == null || row.getStatus() == status)
                        && (schemaFingerprint == null || Objects.equals(
                                row.getSchemaFingerprint(), schemaFingerprint))
                        && (requiredUpdateTimeMs == null || sameStoredSecond(
                                row.getUpdateTime(), requiredUpdateTimeMs))) {
                    return DurablePresence.PRESENT;
                }
            }
            return DurablePresence.ABSENT;
        } catch (Throwable t) {
            LOG.warn("SPM cannot reconcile the ambiguous write of baseline {}: {}",
                    id, t.getMessage());
            return DurablePresence.UNKNOWN;
        }
    }

    /**
     * Whether a READABLE durable row proves the row THIS write attempted landed: the
     * identity AND the attempted STORED SECOND. The status alone was accepted
     * before, and a previously failed old-row delete can leave a STALE row of that very
     * status beside the winner - the ENABLE of found the leftover ENABLED
     * row of the failed DISABLE and treated its aborted conditional INSERT as written,
     * after which a failed DISABLE delete published a status no durable winner carried.
     * The flip's stored second is strictly later than every row it met (the bump in
     * updateStatus), so only the row this write would have produced can match it.
     */
    @VisibleForTesting
    static boolean observedInsertRowIsOurs(BaselinePlan p) {
        return probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(), p.getStatus(),
                null, p.getUpdateTime()) == DurablePresence.PRESENT;
    }

    /**
     * Fails an INSERT whose outcome is AMBIGUOUS. A reported error may have
     * been raised AFTER a commit (a statement timeout: the row is committed and merely
     * waiting for publication), so it must travel as UnconfirmedInsertException:
     * the create path then REMEMBERS the attempted identity and the retry defers /
     * adopts instead of allocating a SECOND id for the same baseline (whose unreadable
     * row the durable-key read cannot see). An ordinary exception left that handler
     * unreachable for this path. A probe that already SEES the row treats the write as
     * landed.
     *
     * @param p     the row the write attempted
     * @param cause the reported error
     */
    private static void throwAmbiguousInsertUnlessDurable(BaselinePlan p, Exception cause) {
        if (observedInsertRowIsOurs(p)) {
            LOG.warn("SPM persist (insert) reported {} but the row is durable (id={});"
                    + " keeping it", cause.getMessage(), p.getId());
            return;
        }
        throw new UnconfirmedInsertException("SPM persist (insert) of baseline " + p.getId()
                + " reported an error and may still have COMMITTED (a committed row is"
                + " only waiting for publication): " + cause.getMessage()
                + " - its id is retained until the outcome is resolved; retry the"
                + " statement", cause);
    }

    /**
     * The newest stored update_time of the WHOLE durable table, in epoch SECONDS (0 when
     * the table holds no row / the read is unavailable). The status-flip bump advances a
     * new row past this value, so EVERY flip moves MAX(update_time) - the snapshot
     * fence (see SELECT_MAX_UPDATE_TIME_SQL).
     *
     * @return the newest stored second
     */
    private static long readNewestStoredUpdateSecond() {
        if (idAllocatorStoreForTest != null) {
            return idAllocatorStoreForTest.newestStoredUpdateSecond();
        }
        if (statusProtocolStoreForTest != null) {
            return statusProtocolStoreForTest.newestStoredUpdateSecond();
        }
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_MAX_UPDATE_TIME_SQL, Collections.emptyMap(),
                    INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            String text = rows.get(0).getWithDefault(0, "");
            if (text == null || text.isEmpty()) {
                return 0; // an empty table yields one NULL MAX(update_time)
            }
            return fromTs(text.trim()) / 1000L;
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline update_time watermark read failed"
                    + " (retry the statement): " + e.getMessage(), e);
        }
    }

    private static void persistDeleteByIdentity(BaselinePlan p) {
        // The repair deletes can be dispatched by a demoted master (an in-flight command
        // or a stale-cache reconciliation runs its own read first): deleting AFTER the
        // handoff could erase a row the NEW master just created once the id was reused.
        // Fence like the create path.
        assertLeaderForWrite();
        try {
            if (idAllocatorStoreForTest != null) {
                idAllocatorStoreForTest.deleteByIdentity(p);
            } else {
                if (!persistenceEnabled()) {
                    return;
                }
                Map<String, String> params = new HashMap<>();
                params.put("id", String.valueOf(p.getId()));
                params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
                params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
                inInternalIoMode(() -> {
                    StatisticsUtil.execUpdate(DELETE_BY_IDENTITY_SQL, params,
                            BASELINE_WRITE_TIMEOUT_SECONDS);
                    return null;
                });
            }
        } catch (Exception e) {
            // A reported error may hide a committed DELETE: the row being GONE is the
            // ONLY proof the drop landed. An unconfirmable read (UNKNOWN) must NOT be
            // treated as proof of absence - removing the cache entry then reported
            // success while the durable row stayed and a refresh / restart resurrected
            // the dropped baseline.
            DurablePresence presence = probeDurableRow(p.getId(), p.getBindSqlDigest(),
                    p.getPlanSql());
            if (presence == DurablePresence.ABSENT) {
                LOG.warn("SPM persist (delete) reported {} but the row is gone (id={});"
                        + " treating it as deleted", e.getMessage(), p.getId());
                return;
            }
            throw new RuntimeException("SPM persist (delete) failed: " + e.getMessage()
                    + (presence == DurablePresence.UNKNOWN
                            ? " (the durable row could not be confirmed deleted)" : ""), e);
        }
        // A reported SUCCESS may still be an unpublished COMMITTED transaction: report
        // the drop only once no read can see the row any more.
        confirmIdentityGone(p);
    }

    /**
     * Removes the row(s) with the given id whose status matches the previous status AND
     * whose content matches the plan's identity (bind_sql_digest + plan_sql).
     *
     * The leadership fence matters here: an ALTER of the OLD master can reach THIS
     * DELETE after a handoff while the new master already completed the opposite flip
     * (both ALTERs pass their early checks). The delayed DELETE ... WHERE id AND
     * status = the old status then removed the ONLY durable row the new master had just
     * written - both ALTERs reported success and the baseline was durably gone. The
     * identity key keeps the same statement from touching a REUSED id's row as well.
     */
    private static void persistDeleteByIdAndStatus(BaselinePlan p, BaselineStatus status) {
        assertLeaderForWrite();
        long id = p.getId();
        if (statusProtocolStoreForTest != null) {
            statusProtocolStoreForTest.deleteByIdAndStatus(id, status);
            return;
        }
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        params.put("status", status.name());
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        try {
            inInternalIoMode(() -> {
                StatisticsUtil.execUpdate(DELETE_BY_ID_AND_STATUS_SQL, params,
                        BASELINE_WRITE_TIMEOUT_SECONDS);
                return null;
            });
        } catch (Exception e) {
            // Ambiguous commit: only a READABLE zero row count proves the delete landed.
            // An unconfirmable probe must surface as the original write failure.
            try {
                if (durableRowCount(id, status) == 0) {
                    LOG.warn("SPM persist (delete by status) reported {} but no row carries"
                            + " ({}, {}); treating it as deleted", e.getMessage(), id, status);
                    return;
                }
            } catch (Throwable probeFailure) {
                throw new RuntimeException("SPM persist (delete by status) failed: "
                        + e.getMessage() + " (the durable row could not be confirmed"
                        + " deleted)", e);
            }
            throw new RuntimeException("SPM persist (delete by status) failed: " + e.getMessage(), e);
        }
        // Success is confirmed like the insert half: the flip may only be published once
        // the old-status row is provably gone from every read - and a lagging publication
        // is treated as the committed delete it is (see confirmStatusRowGone).
        confirmStatusRowGone(id, status);
    }

    /**
     * Epoch millis -> internal-table DATETIME literal ('yyyy-MM-dd HH:mm:ss'), rendered
     * in UTC.
     *
     * The columns are zone-free, and the FEs of one cluster do not share a host zone:
     * rendering in ZoneId.systemDefault() made the stored value depend on the
     * writer's host zone, so the duplicate-row recovery (pickDurableWinner, which
     * keeps the row with the LATER updateTime) compared instants written by different
     * hosts as if they were one clock - a UTC master's 12:00 ENABLED row outranked a
     * UTC-8 successor's 12:01 DISABLED row (stored 04:01), and a DST fall-back inverted
     * the order of two writes of the same FE. UTC makes the values absolute and totally
     * ordered.
     */
    @VisibleForTesting
    static String toTs(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC)
                .format(TS_FORMAT);
    }

    /** Internal-table DATETIME literal (UTC, see toTs) -> epoch millis. */
    @VisibleForTesting
    static long fromTs(String ts) {
        return LocalDateTime.parse(ts, TS_FORMAT)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
