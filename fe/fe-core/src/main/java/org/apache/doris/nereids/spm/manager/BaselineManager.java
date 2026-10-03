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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.MasterOpExecutor;
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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
 * - hashIndex: {@code Map<Long, List<Long>>}, bindSqlHash -> baseline id list. Level 1
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
         * {@code INSERT ... SELECT ... WHERE id / status} statement - the new-status row
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

        void insert(BaselinePlan plan);

        List<BaselinePlan> readById(long id);

        void deleteByIdentity(BaselinePlan plan);
    }

    @VisibleForTesting
    public static volatile StatusProtocolStoreForTest statusProtocolStoreForTest;

    @VisibleForTesting
    public static volatile IdAllocatorStoreForTest idAllocatorStoreForTest;

    /**
     * Test seam replacing the live leadership probe of {@link #assertLeaderForWrite}
     * (null in production). The store simulators bypass the live fence by design, so
     * without this seam a unit test cannot interleave a master handoff with an in-flight
     * write (the insert / delete halves of a status flip).
     */
    @VisibleForTesting
    public static volatile java.util.function.BooleanSupplier leaderProbeForTest;

    /**
     * Test seam for the read-back visibility confirmation of a reported-successful write
     * (see {@link #confirmInsertVisible}): one call is ONE probe attempt, true = the row
     * (insert) or its status row is READABLE, false = not yet visible. A test
     * decrements an invisible window here to simulate the COMMITTED-but-not-yet-published
     * state the real store exposes. Null in production.
     */
    @VisibleForTesting
    interface DurableVisibilityProbeForTest {
        boolean isReadable(long id, BaselineStatus status);
    }

    @VisibleForTesting
    public static volatile DurableVisibilityProbeForTest durableVisibilityProbeForTest;

    /**
     * Test seam replacing the snapshot READ of the load path (loadFromInternalTable /
     * the promotion reload): lets a unit test return a controlled snapshot and, together
     * with {@link #snapshotReadStartedHookForTest}, invalidate the store WHILE a load is
     * still inside its read - the stale snapshot must then be discarded instead of
     * republished. Null in production.
     */
    @VisibleForTesting
    public static volatile Supplier<Map<Long, BaselinePlan>> snapshotReaderForTest;

    /**
     * Test seam counting the background load threads that were actually STARTED by
     * {@link #scheduleAsyncLoad} (one per load-slot claim). A query burst must coalesce
     * onto the in-flight load instead of starting one thread per caller, which this
     * counter makes observable. Null in production.
     */
    @VisibleForTesting
    public static volatile java.util.concurrent.atomic.AtomicInteger asyncLoadSpawnCountForTest;

    /**
     * Test seam replacing the journal synchronization of
     * {@link #refreshAfterForwardedDdl} and {@link #confirmGlobalRowsForShow} (null in
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

    /** Column order follows InternalSchema.SPM_BASELINES_SCHEMA (unpaged selection). */
    private static final String SNAPSHOT_COLUMNS =
            "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint` FROM ";

    /**
     * First page of a whole-table snapshot: ordered by id so the pagination can continue
     * with {@link #SELECT_PAGE_SQL} from the last row read (and so the duplicate-id
     * resolution sees a stable order).
     */
    private static final String SELECT_ALL_ORDERED_SQL =
            SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE + " ORDER BY `id`";

    /**
     * One continuation page of a whole-table snapshot: every row with {@code id >=
     * &#36;{lastId}} - the boundary id group is re-read as a whole - ordered by id.
     */
    private static final String SELECT_PAGE_SQL = SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE
            + " WHERE `id` >= ${lastId} ORDER BY `id` LIMIT ${pageSize}";

    /**
     * Rows per snapshot page (see {@link #readPersistedSnapshot}). Bounds what ONE
     * internal query has to return, so a growing table can no longer make the whole
     * snapshot read fail against a fixed timeout.
     */
    private static final int SNAPSHOT_PAGE_SIZE = 2000;

    /** The persistence-layer id watermark (see the class javadoc "Id source"): read
     *  before every id allocation. MAX over an aggregate is a light single-row query. */
    private static final String SELECT_MAX_ID_SQL = "SELECT MAX(`id`) FROM " + SPM_BASELINES_TABLE;

    /**
     * Reads every durable row carrying ONE id - the collision probe of a create (see
     * {@link #createBaseline}): the table is DUPLICATE KEY(id), so an out-of-band writer
     * or a second master that started from the same watermark can have inserted a
     * DIFFERENT baseline under the id this create just allocated. Snapshot loading would
     * later collapse the two rows nondeterministically (pickDurableWinner), and dropping
     * the visible row could expose the other; the collision is therefore detected and
     * resolved at create time.
     */
    private static final String SELECT_BY_ID_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, `create_time`, `update_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint` FROM " + SPM_BASELINES_TABLE
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
            + " `plan_frozen`, `schema_fingerprint` FROM " + SPM_BASELINES_TABLE
            + " WHERE `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'";

    private static final String INSERT_SQL = "INSERT INTO " + SPM_BASELINES_TABLE
            + " VALUES (${id}, '${bindSql}', '${bindSqlDigest}', ${bindSqlHash},"
            + " '${planSql}', '${queryId}', ${cost}, ${queryTimeMs}, '${source}', '${status}',"
            + " '${createTime}', '${updateTime}', ${sqlMode}, ${planSqlMode}, ${planFrozen},"
            + " '${schemaFingerprint}')";

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
     */
    private static final String INSERT_IF_PREVIOUS_STATUS_SQL = "INSERT INTO "
            + SPM_BASELINES_TABLE + " SELECT ${id}, '${bindSql}', '${bindSqlDigest}',"
            + " ${bindSqlHash}, '${planSql}', '${queryId}', ${cost}, ${queryTimeMs},"
            + " '${source}', '${status}', '${createTime}', '${updateTime}', ${sqlMode},"
            + " ${planSqlMode}, ${planFrozen}, '${schemaFingerprint}' FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id} AND `status` = '${previousStatus}'"
            + " LIMIT 1";

    /** Reconciliation read of the ambiguous status-update path: how many durable rows
     *  currently carry (id, status). */
    private static final String COUNT_BY_ID_AND_STATUS_SQL = "SELECT COUNT(*) FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id} AND `status` = '${status}'";

    /**
     * DATETIME column format (internal table create_time / update_time). The columns are
     * zone-free DATETIME, so they are written and read in UTC: the stored value denotes
     * the SAME instant on every FE, in every host zone and across DST changes (see
     * {@link #toTs}).
     */
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** Message of the leadership fence (see {@link #assertLeaderForWrite}). */
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
     * {@link #confirmInsertVisible}): the attempts times the retry delay must cover a
     * normal publication lag; a longer invisible window fails the write retryably
     * instead of publishing an id no read can ever confirm.
     */
    private static final int BASELINE_VISIBILITY_ATTEMPTS = 5;

    /** Delay between two visibility probes of a reported-successful write (ms). */
    private static final long BASELINE_VISIBILITY_RETRY_MILLIS = 200L;

    /** How long a management caller waits for an in-flight background load. */
    private static final long MANAGEMENT_LOAD_WAIT_MILLIS = 5_000L;

    /**
     * Bounded id-collision retries of one create (see createBaseline): reaching the cap
     * means at least MAX_ID_COLLISION_RETRIES competing writes landed on every id this
     * FE allocated - a retryable condition, never a silent success.
     */
    private static final int MAX_ID_COLLISION_RETRIES = 8;

    /**
     * Bound of {@link #pendingCreates}: an unconfirmed create is a rare (and manually
     * retried) event, so a small registry is enough. The OLDEST entry is dropped when the
     * bound is reached, which re-exposes that single write to a duplicate id (the state
     * before this registry existed) - the log line names the baseline.
     */
    private static final int MAX_PENDING_CREATES = 64;

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
     * just requests a background load; a failed read keeps {@code loaded=false} and is
     * retried by the next access / refresh cycle.
     */
    private final AtomicBoolean loadInProgress = new AtomicBoolean(false);

    /** Notified when an in-flight load finishes (management callers wait on it). */
    private final Object loadMonitor = new Object();

    /** Whether CRUD writes to the internal table (disabled by clearForTest for tests). */
    private volatile boolean persistToTable = true;

    /**
     * Guards the in-memory store: {@code baselines} / {@code hashIndex} /
     * {@code stateVersion} and the {@code loaded} state machine. Only accesses to those
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
     * two-phase structure of {@link #createBaseline} / {@link #dropBaseline} /
     * {@link #updateStatus}. Readers never touch this lock.
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
     * scanning the whole table (see {@link #readPersistedRowsForCreate}).
     */
    private volatile long maxPersistedIdSeen = 0;

    /**
     * Creates whose INSERT reported SUCCESS but whose row was not READABLE yet (see
     * {@link #createBaseline}): the id is consumed but invisible, so neither MAX(id) nor
     * the durable-key read can see it. A RETRY of the same CREATE must not allocate a
     * SECOND id for the same baseline - both rows would later publish under different ids
     * and dropping the id the client was told about would leave the other ACTIVE. The
     * retry ADOPTS the remembered row once it becomes readable and is DEFERRED until then.
     * Guarded by writerLock (every access runs inside it). Bounded by
     * {@link #MAX_PENDING_CREATES}.
     */
    private final List<BaselinePlan> pendingCreates = new ArrayList<>();

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
            // Id watermark first (see the class javadoc "Id source"): the generator must be
            // advanced past the persistence layer BEFORE an id is handed out. A create whose
            // watermark read fails fails visibly and allocates nothing, instead of silently
            // colliding with a row written by a newer master.
            final long watermark = readPersistedWatermark();
            // An earlier CREATE of the SAME baseline may have reported a retryable failure
            // AFTER its INSERT reported success (the row was committed but not readable).
            // That id is consumed and INVISIBLE, so neither MAX(id) nor the durable-key read
            // below can see it: allocating a second id here would publish both rows later
            // (different ids) and dropping the id the client was told about would leave the
            // other one ACTIVE. Resolve the remembered write first - adopt it once it is
            // readable, otherwise DEFER the retry until it is.
            Long adoptedId = resolvePendingCreate(plan);
            if (adoptedId != null) {
                return adoptedId;
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
                if (!persistenceEnabled() && statusProtocolStoreForTest == null
                        && idAllocatorStoreForTest == null) {
                    return duplicate.getId();
                }
                if (idAllocatorStoreForTest == null && statusProtocolStoreForTest != null) {
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
                }
                LOG.warn("SPM baseline create: the cached duplicate {} is gone durably"
                        + " (promotion window); creating a fresh row", duplicate.getId());
                staleCacheRows.add(duplicate);
            }
            // Durable-key check: the in-memory index can be stale (e.g. a follower that
            // loaded=true before becoming master missed rows written after its last
            // refresh). Without this check the INSERT below would REPLACE a durable
            // baseline - changing its id and, on an INSERT failure, losing the old row.
            // A durable duplicate returns its id and is adopted into memory instead;
            // extra same-key rows (partial-state survivors) are repaired away idempotently.
            // writerLock keeps another writer's INSERT/DELETE pair out of this window.
            if (persistenceEnabled() && plan.getBindSqlDigest() != null) {
                List<BaselinePlan> durable =
                        readPersistedRowsForCreate(plan, watermark);
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
                // persist first so a persist failure leaves the in-memory state untouched
                // and fails the DDL visibly; no same-key row can exist here (the
                // durable-key check above returned any), so the INSERT cannot overwrite an
                // existing baseline
                try {
                    persistInsert(plan);
                } catch (UnconfirmedInsertException unconfirmed) {
                    rememberPendingCreate(plan);
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
     * (see {@link #pendingCreates}).
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
     * Applies {@link #pendingCreates} to a CREATE of the same baseline (see the call site
     * in {@link #createBaseline}): a remembered write that has become READABLE is ADOPTED
     * (its id is published and returned) while a still-invisible write DEFERS the create
     * with a retryable error instead of consuming a second id.
     *
     * <p>The adoption reads the row back by id instead of relying on the durable-key dedup:
     * a failed create never published into this FE's in-memory key index, so the indexed
     * dedup would not see the committed row and would allocate a second id for the same
     * baseline.
     *
     * @param plan the CREATE's baseline
     * @return the adopted id, or null when no remembered write of this baseline exists
     */
    private Long resolvePendingCreate(BaselinePlan plan) {
        if (pendingCreates.isEmpty()) {
            return null;
        }
        Iterator<BaselinePlan> iterator = pendingCreates.iterator();
        while (iterator.hasNext()) {
            BaselinePlan pending = iterator.next();
            if (!sameIdentity(pending, plan)) {
                continue;
            }
            if (!durableRowReadable(pending)) {
                throw new IllegalStateException("SPM cannot create baseline " + pending.getId()
                        + ": a previously COMMITTED write of the same baseline is still"
                        + " awaiting publication (its id is consumed); retry the statement");
            }
            iterator.remove();
            BaselinePlan winner = null;
            for (BaselinePlan row : readPersistedParsedById(pending.getId())) {
                if (!sameIdentity(row, plan)) {
                    continue; // the id carries a DIFFERENT baseline: never adopt it
                }
                winner = winner == null ? row : pickDurableWinner(winner, row);
            }
            if (winner == null) {
                LOG.warn("SPM pending create of baseline {}: the id no longer carries this"
                        + " baseline; allocating a fresh id", pending.getId());
                continue;
            }
            publishBaseline(winner);
            LOG.info("SPM pending create of baseline {} adopted from the durable table",
                    winner.getId());
            return winner.getId();
        }
        return null;
    }

    /**
     * Remembers a create whose INSERT reported success but is not readable yet (see
     * {@link #confirmInsertVisible}): the next CREATE of the same baseline must not
     * allocate a second id for it.
     *
     * @param plan the row that was written
     */
    private void rememberPendingCreate(BaselinePlan plan) {
        for (BaselinePlan pending : pendingCreates) {
            if (sameIdentity(pending, plan)) {
                return; // already remembered by an earlier attempt
            }
        }
        if (pendingCreates.size() >= MAX_PENDING_CREATES) {
            BaselinePlan evicted = pendingCreates.remove(0);
            LOG.warn("SPM dropped the pending-create record of baseline {} (registry bound {});"
                            + " a retry may allocate a second id for it",
                    evicted.getId(), MAX_PENDING_CREATES);
        }
        pendingCreates.add(plan);
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
            persistDeleteByIdentity(removed);
            // The cached object can predate a promotion reload AND the durable row under
            // this id may be a DIFFERENT incarnation (an id reused after the old row was
            // dropped): DROP is keyed by the user-facing id, so no row of this id may
            // survive - identity-deleting only the stale object would report success
            // while the real row stays and keeps matching later reads.
            wipeDurableRowsById(id, removed);
            stateLock.writeLock().lock();
            try {
                BaselinePlan gone = baselines.remove(id);
                if (gone == null) {
                    // raced with a concurrent drop / refresh: the row is gone either way
                    return true;
                }
                removeFromHashIndex(gone);
                stateVersion++;
                return true;
            } finally {
                stateLock.writeLock().unlock();
            }
        }
    }

    /**
     * See {@link #dropBaseline}: reconciles a cache miss against the durable table. The
     * rows found (a reload window can leave several) are removed by IDENTITY, so a stale
     * id can never delete an unrelated row.
     */
    private boolean dropDurableRowByIdIfAbsentFromCache(long id) {
        List<BaselinePlan> durable = readPersistedById(id);
        if (durable.isEmpty()) {
            return false;
        }
        for (BaselinePlan row : durable) {
            persistDeleteByIdentity(row);
        }
        LOG.info("SPM dropped baseline {} from the durable table while the local cache did"
                + " not have it (promotion reload / stale snapshot window)", id);
        return true;
    }

    /**
     * Removes any durable row of the given id that is NOT the identity just deleted (see
     * {@link #dropBaseline}): a promotion-window snapshot can carry an old object whose
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
        }
    }

    /**
     * ALTER counterpart of {@link #dropDurableRowByIdIfAbsentFromCache}: reconciles a
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
        List<BaselinePlan> durable = readPersistedParsedById(id);
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
     * Shared reader of {@link #readPersistedById} / {@link #readPersistedParsedById}:
     * seam-aware and fail-closed on read errors.
     *
     * @param id          the baseline id
     * @param rebuildTrees whether each row is parsed like a load ({@link #parsePersistedRow})
     *                     instead of decoded as plain scalars ({@link #fromRow})
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
            if (durableReconcile) {
                // Reconcile against the durable incarnation BEFORE touching anything: a
                // promoted FE can serve a snapshot that predates the previous master's
                // DROP, and an opposite-status ALTER over that stale cache would INSERT
                // the requested status back - resurrecting a baseline the previous
                // master dropped. The probe also decides the REAL previous status (the
                // cache may have missed an earlier flip).
                DurableStatusProbe probe = probeDurableStatus(id, status);
                if (probe == DurableStatusProbe.UNKNOWN) {
                    // The durable state cannot be CONFIRMED: reporting a no-op success
                    // could leave a durably DISABLED / DROPPED row while this FE serves
                    // its stale snapshot, and the next refresh restores the opposite
                    // state. Fail retryably until the requested status is provable.
                    throw new IllegalStateException("SPM cannot confirm the durable status of"
                            + " baseline " + id + "; retry the ALTER");
                }
                if (probe == DurableStatusProbe.ABSENT) {
                    // the row is gone durably (the previous master dropped it): never
                    // report a successful ALTER for a baseline that does not exist
                    LOG.warn("SPM status update of baseline {}: the row is gone durably;"
                            + " dropping it from the cache", id);
                    retireStaleInMemory(List.of(plan));
                    return false;
                }
                if (probe == DurableStatusProbe.MATCHES) {
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
            // Persist a DETACHED snapshot carrying the new status: the live object keeps
            // the old status until the durable write (or the confirmed reconciliation)
            // succeeded - matching readers do not take the writer lock, so publishing
            // early would let a concurrent query replay a baseline whose durable row is
            // still DISABLED when the write later fails, while ALTER still reports
            // failure.
            BaselinePlan durablePlan = plan.copyPersistedScalars();
            durablePlan.setStatus(status);
            durablePlan.setUpdateTime(newUpdateTime);
            try {
                assertLeaderForWrite();
                persistTransitionInsert(durablePlan, previousStatus);
                persistDeleteByIdAndStatus(durablePlan, previousStatus);
            } catch (RuntimeException e) {
                // The INSERT(new) / DELETE(old) pair spans two statements whose outcomes
                // can be AMBIGUOUS: a delete may commit but report KV_TXN_MAYBE_COMMITTED,
                // or report SQL OK while its publication lags past every confirmation
                // probe. Reconcile against the durable table before deciding:
                //  - the old row is GONE and the new row is readable -> the delete
                //    committed; PUBLISH the flip and report success;
                //  - anything else (the old row still readable, or an unconfirmable read)
                //    is an UNKNOWN outcome: KEEP the new-status row and report the
                //    original failure. A compensating DELETE of the new row is UNSAFE
                //    here: when the old-row delete actually committed and only its
                //    PUBLICATION lagged, the compensation removes the new row and the
                //    committed old-row delete then removes the old one - the GLOBAL
                //    baseline disappears entirely. Both rows carry DIFFERENT statuses, so
                //    the load path resolves the duplicate deterministically
                //    ({@link #pickDurableWinner}: the later updateTime wins) and the next
                //    refresh / ALTER retry reconciles the cache with the winner - at least
                //    one version ALWAYS survives.
                if (oldRowDeletedDurably(id, previousStatus, status)) {
                    publishStatus(plan, status, newUpdateTime);
                    LOG.warn("SPM status update of baseline {} committed despite an ambiguous"
                            + " persist error; keeping the new-status row", id, e);
                    return true;
                }
                LOG.warn("SPM status update of baseline {} has an UNKNOWN durable outcome ({});"
                        + " keeping the new-status row and reconciling on the next refresh",
                        id, e.getMessage());
                throw e;
            }
            publishStatus(plan, status, newUpdateTime);
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

    /** Outcome of the durable status probe (see {@link #updateStatus}). */
    private enum DurableStatusProbe { MATCHES, DIFFERS, ABSENT, UNKNOWN }

    /**
     * Compares the EFFECTIVE durable status of one baseline with the expected one. A
     * failed status flip can leave BOTH rows behind (the old-row delete AND the
     * compensating delete failed): the load path resolves such duplicates with
     * {@link #pickDurableWinner} (later updateTime wins, DISABLED on a tie), and the
     * probe must apply the SAME rule - a bare “does a row with the cached status exist”
     * count reported MATCHES while the newer row carried the opposite status, so the
     * next ALTER back to the old status reported success without changing the effective
     * state.
     *
     * The count-only status seam cannot decide between two rows (it has no update
     * times): both-present is UNKNOWN (fail closed). A read failure is UNKNOWN as well.
     */
    private static DurableStatusProbe probeDurableStatus(long id, BaselineStatus expected) {
        try {
            if (idAllocatorStoreForTest == null && !persistenceEnabled()
                    && statusProtocolStoreForTest == null) {
                return DurableStatusProbe.ABSENT;
            }
            if (statusProtocolStoreForTest != null && idAllocatorStoreForTest == null) {
                boolean expectedRows =
                        statusProtocolStoreForTest.countByIdAndStatus(id, expected) > 0;
                boolean otherRows = statusProtocolStoreForTest
                        .countByIdAndStatus(id, otherStatus(expected)) > 0;
                if (!expectedRows && !otherRows) {
                    return DurableStatusProbe.ABSENT;
                }
                if (expectedRows && !otherRows) {
                    return DurableStatusProbe.MATCHES;
                }
                if (otherRows && !expectedRows) {
                    return DurableStatusProbe.DIFFERS;
                }
                throw new IllegalStateException(
                        "two durable rows of baseline " + id + " and no update times");
            }
            List<BaselinePlan> rows = readPersistedById(id);
            if (rows.isEmpty()) {
                return DurableStatusProbe.ABSENT;
            }
            BaselinePlan winner = rows.get(0);
            for (int i = 1; i < rows.size(); i++) {
                winner = pickDurableWinner(winner, rows.get(i));
            }
            return winner.getStatus() == expected
                    ? DurableStatusProbe.MATCHES : DurableStatusProbe.DIFFERS;
        } catch (Throwable t) {
            LOG.warn("SPM cannot probe the durable status of baseline {}: {}", id, t.getMessage());
            return DurableStatusProbe.UNKNOWN;
        }
    }

    /** The other status of the binary enable / disable model. */
    private static BaselineStatus otherStatus(BaselineStatus status) {
        return status == BaselineStatus.ENABLED
                ? BaselineStatus.DISABLED : BaselineStatus.ENABLED;
    }

    /**
     * Removes stale in-memory duplicates (see {@link #createBaseline} / {@link #updateStatus}):
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
     * Reconciles an ambiguous status-update failure: whether the OLD-row delete actually
     * committed although it reported an error (e.g. KV_TXN_MAYBE_COMMITTED). Reads the
     * durable rows back: the old row missing while the new one is present means the
     * delete committed and the update succeeded. When the durable state cannot be read
     * the answer is "no" and the caller keeps BOTH rows instead of a blind compensating
     * delete - the load path resolves duplicate rows deterministically
     * (pickDurableWinner), so at least one version survives.
     */
    private static boolean oldRowDeletedDurably(long id, BaselineStatus previousStatus,
            BaselineStatus newStatus) {
        try {
            return durableRowCount(id, previousStatus) == 0
                    && durableRowCount(id, newStatus) > 0;
        } catch (Throwable t) {
            LOG.warn("SPM cannot reconcile the status update of baseline {}, keeping both rows:"
                    + " {}", id, t.getMessage());
            return false;
        }
    }

    /**
     * One internal-table statement / query body; see {@link #inInternalIoMode}. */
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

    /** Runs {@link #inInternalIoMode} and reports the effective parser mode (test seam). */
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
     * READ lock is enough; {@link #createBaseline} calls it in its phase-1 validation.
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
     * Confirms the GLOBAL store is LOADED. {@link #getAllBaselines()} only STARTS the
     * asynchronous load and returns the current map, so at startup or right after a
     * promotion - when that map was just cleared - a caller reported ZERO global rows even
     * though durable rows existed, and a pending or failed read never converged. This
     * uses the same load (and retryable error) the mutating DDL relies on; query matching
     * keeps ensureLoaded()'s nonblocking degradation.
     *
     * <p>A caller whose answer must reflect GLOBAL DDL completed on ANOTHER FE uses
     * {@link #confirmGlobalRowsForShow()} instead: "loaded" only means this FE read the
     * table ONCE, so a follower that finished its load before the master committed keeps
     * answering from its old map until the refresh daemon runs.
     */
    public void ensureLoadedConfirmed() {
        ensureLoadedOrThrow();
    }

    /**
     * CONFIRMED durable read for the GLOBAL rows of SHOW BASELINE PLANS (and, in tests,
     * of any caller that must observe a GLOBAL DDL completed on another FE).
     * {@link #ensureLoadedConfirmed()} returns immediately once {@code loaded=true}, and
     * {@link #getAllBaselines()} then copies this FE's cache, so a follower that loaded
     * BEFORE a GLOBAL DDL completed on the master kept listing its OLD map: a completed
     * CREATE was invisible and a completed DROP stayed listed until the next refresh
     * daemon cycle (and, for a failed read, indefinitely).
     *
     * <p>The GLOBAL portion of SHOW is documented as authoritative, so this performs the
     * module's confirmed read instead:
     *
     * <ul>
     *   <li>the master (or a store without table persistence, whose memory IS the durable
     *       state) answers from its own publish - every committed GLOBAL DDL ran locally;
     *   <li>a follower first synchronizes its metadata with the master (the same
     *       strong-consistency mechanism a forwarded DDL and {@code syncJournalIfNeeded}
     *       use), then fences every snapshot read that started before that point through
     *       the store generation;
     *   <li>the durable rows are then read FRESH: while the store is still unpublished the
     *       read is performed inline (bounded wait for the in-flight load first, exactly
     *       like the forwarded-DDL refresh), while it is published the fresh snapshot
     *       replaces the cache under {@code writerLock} (no local mutation can publish
     *       meanwhile);
     *   <li>a failed read surfaces as a retryable error - SHOW must never print a table it
     *       cannot confirm. The published cache is deliberately NOT invalidated: unlike a
     *       forwarded DDL, no committed write is known to have happened, so query
     *       matching keeps its current state and the read is retried (by SHOW or the
     *       refresh daemon).
     * </ul>
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
     * {@link #loadFromInternalTable()} runs through {@link #snapshotReaderForTest}.
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

    /** For tests: pins the table-persistence gate (see {@link #persistenceEnabled()}). */
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
            statusProtocolStoreForTest = null; // and never route through a leaked test seam
            idAllocatorStoreForTest = null; // (the create-time collision seam, same reason)
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
     * ({@link #loadInProgress}). The read runs OUTSIDE the state lock: an internal query
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
     * must own the load slot. Keeps {@code loaded=false} on any failure so the caller can
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
                doLoadFromTable(snapshot);
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
     * <p>The load slot is claimed ATOMICALLY here, at scheduling time: checking
     * {@code loadInProgress} and starting the thread were separate, so a query burst (or
     * repeated failed reads) could start one throwaway {@code spm-baseline-async-load}
     * thread per caller - only the CAS winner inside {@link #tryLoadNow()} performed the
     * read, every other thread exited immediately. Reserving the slot first means a
     * caller that cannot claim it simply returns: the owner releases the slot when its
     * read finishes ({@link #readAndPublishPossessingLoadSlot()}), so the next caller
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

    /** Replaces the in-memory store with the read snapshot (caller holds the write lock). */
    private void doLoadFromTable(Map<Long, BaselinePlan> loadedPlans) {
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
     * ({@code afterForwardToMaster}): the DDL already committed on the master, so this FE
     * must publish a state that INCLUDES it (or fail retryably) before the statement
     * returns. {@link #refreshFromInternalTable} is best-effort and has two holes:
     *
     * - {@code loaded == false}: an older load (started BEFORE the DDL) may hold the
     *   {@code loadInProgress} slot; the best-effort refresh returns immediately and the
     *   pre-DDL snapshot can publish afterwards - a CREATE stays invisible, a DROP /
     *   disable keeps replaying locally until the daemon refresh;
     * - {@code loaded == true}: a transient read failure is swallowed and the stale rows
     *   stay exactly as before the DDL.
     *
     * Fix: fence every snapshot whose read may predate the DDL through the store
     * generation (the in-flight load discards itself, see
     * {@link #readAndPublishPossessingLoadSlot}) and then obtain a fresh read - an inline
     * load while unpublished, or a snapshot apply while published. On failure the
     * published store is INVALIDATED (fail closed: never keep replaying a possibly
     * dropped / disabled baseline) and a retryable failure surfaces to the caller.
     */
    public void refreshAfterForwardedDdl() {
        refreshAfterForwardedDdl(ConnectContext.get());
    }

    /**
     * As {@link #refreshAfterForwardedDdl()}, with the forwarding statement's context: the
     * journal synchronization below talks to the master through it.
     *
     * @param ctx the context of the statement that was forwarded (may be null in tests)
     */
    public void refreshAfterForwardedDdl(ConnectContext ctx) {
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
                return;
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                // Never pretend the local cache reflects the committed DDL: fence the
                // (possibly pre-DDL) published rows out and surface a retryable failure.
                invalidatePublishedStore();
                throw new IllegalStateException("SPM baseline cache cannot be confirmed after"
                        + " the forwarded DDL (please retry later): " + t.getMessage(), t);
            }
            // No writer can interleave (writerLock is held) and loads return early while
            // loaded, so the snapshot is authoritative for this instant.
            applyRefreshedBaselines(snapshot);
        }
    }

    /**
     * Waits until this FE's metadata includes the FORWARDED global DDL the master already
     * completed: {@code CREATE / ALTER / DROP BASELINE PLAN} forward with
     * FORWARD_NO_SYNC, and the checkpoint-free internal reads of the refresh run locally,
     * so a follower's still-visible OLD version would be published as the confirmed
     * post-DDL state. The journal sync is the same mechanism a strong-consistency user
     * query uses (see {@code StmtExecutor#syncJournalIfNeeded}): it asks the master for
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
     * {@link #refreshFromInternalTable}, which additionally serializes with the writers.
     *
     * @param versionAtRead the state version observed when the snapshot read started
     * @param persisted     the snapshot to apply
     * @return whether the snapshot was applied
     */
    public boolean applyRefreshedSnapshotIfUnchanged(long versionAtRead,
            Map<Long, BaselinePlan> persisted) {
        stateLock.writeLock().lock();
        try {
            if (stateVersion != versionAtRead) {
                return false;
            }
            applyRefreshedBaselinesLocked(persisted);
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
        stateLock.writeLock().lock();
        try {
            applyRefreshedBaselinesLocked(persisted);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /** Applies a snapshot; the caller must hold the write lock (see refreshFromInternalTable). */
    private void applyRefreshedBaselinesLocked(Map<Long, BaselinePlan> persisted) {
        long maxId = 0;
        int added = 0;
        int updated = 0;
        for (BaselinePlan row : persisted.values()) {
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
            if (!persisted.containsKey(id)) {
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
     * Reads the persistence-layer id watermark (MAX(id)) - the id-source invariant described
     * in the class javadoc. Returns 0 when persistence is disabled (unit tests / internal
     * schema db off). A failed read is rethrown as a retryable error: createBaseline must
     * never allocate an id while the watermark is unknown.
     */
    private static long readPersistedWatermark() {
        if (idAllocatorStoreForTest != null) {
            return idAllocatorStoreForTest.watermark();
        }
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows =
                    StatisticsUtil.executeQuery(SELECT_MAX_ID_SQL, Collections.emptyMap(),
                            INTERNAL_QUERY_TIMEOUT_SECONDS);
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            // an empty table yields one row with a NULL MAX(id)
            String maxId = rows.get(0).getWithDefault(0, "");
            return maxId.isEmpty() ? 0 : Long.parseLong(maxId.trim());
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM baseline id watermark read failed (retry the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * Builds the persisted snapshot from the internal table: one BaselinePlan per row with
     * the transient trees rebuilt exactly like the startup load does. Invalid rows are
     * skipped with a warning (the next cycle retries).
     *
     * <p>The read is PAGINATED and ordered by id: the table has no retention cap, so one
     * {@code SELECT *} had to return every row within the fixed per-query timeout - a
     * snapshot that outgrew it failed as a whole and never converged (the follower kept
     * its old published cache, local baseline DDL waited behind the held writer lock, and
     * confirmed SHOW / post-forward refreshes failed on the same path forever). Each page
     * is bounded by {@link #SNAPSHOT_PAGE_SIZE} rows and its own timeout, and the loop
     * walks the id space forward until a short page ends the snapshot.
     */
    private static Map<Long, BaselinePlan> readPersistedSnapshot() throws Exception {
        if (snapshotReaderForTest != null) {
            return snapshotReaderForTest.get();
        }
        return collectSnapshotPages(BaselineManager::readSnapshotPage, SNAPSHOT_PAGE_SIZE);
    }

    /** One page of the whole-table snapshot, read through the internal table. */
    private static List<ResultRow> readSnapshotPage(Long pageStart) throws Exception {
        return inInternalIoMode(() -> StatisticsUtil.executeQuery(
                snapshotPageSql(pageStart), Collections.emptyMap(),
                INTERNAL_QUERY_TIMEOUT_SECONDS));
    }

    /**
     * The pagination loop of {@link #readPersistedSnapshot}: walks the id space forward
     * until a page comes back shorter than {@link #SNAPSHOT_PAGE_SIZE}. The boundary id
     * group is re-read as a whole (the page reader is called with an INCLUSIVE lower
     * bound), so a duplicate id pair of an interrupted status flip is never split across
     * pages - the winner resolution must see BOTH rows. Package-visible with an
     * injectable page reader so the loop (which no unit test can drive through the
     * internal table) is covered directly.
     *
     * @param reader   reads one page: every row with {@code id >= pageStart}, ordered by
     *                 id; a null pageStart reads the first page
     * @param pageSize rows per page (the production value is
     *                 {@link #SNAPSHOT_PAGE_SIZE})
     * @return the accumulated snapshot
     * @throws Exception when a page read fails (the caller retries the whole refresh)
     */
    @VisibleForTesting
    static Map<Long, BaselinePlan> collectSnapshotPages(SnapshotPageReader reader, int pageSize)
            throws Exception {
        Map<Long, BaselinePlan> snapshot = new HashMap<>();
        Long pageStart = null;
        while (true) {
            List<ResultRow> rows = reader.readPage(pageStart);
            if (rows == null || rows.isEmpty()) {
                return snapshot;
            }
            for (ResultRow row : rows) {
                accumulateSnapshotRow(snapshot, row);
            }
            String lastRowId = rows.get(rows.size() - 1).getWithDefault(0, "");
            if (rows.size() < pageSize || lastRowId.isEmpty()) {
                return snapshot; // a short page ends the snapshot
            }
            long pageLastId = Long.parseLong(lastRowId.trim());
            if (pageStart != null && pageLastId == pageStart) {
                // The page consists of ONE id group larger than a page (only an
                // out-of-contract writer can produce that): advance strictly past it so
                // the loop terminates; the create / ALTER protocols never put more than
                // two rows under one id.
                pageStart = pageLastId + 1;
            } else {
                // INCLUSIVE lower bound: the boundary id group is re-read as a whole
                // instead of being cut in the middle (a missed row of an interrupted
                // status flip would let the winner resolution resurrect the old status).
                pageStart = pageLastId;
            }
        }
    }

    /** Reads one page of the snapshot (see {@link #collectSnapshotPages}). */
    @FunctionalInterface
    interface SnapshotPageReader {
        List<ResultRow> readPage(Long pageStart) throws Exception;
    }

    /**
     * The read of ONE snapshot page: every row with {@code id >= pageStart} ordered by
     * id, or the whole table (in id order) when the snapshot has just started.
     *
     * @param pageStart inclusive lower bound of the page id range (null = first page)
     * @return the page SQL
     */
    private static String snapshotPageSql(Long pageStart) {
        if (pageStart == null) {
            return SELECT_ALL_ORDERED_SQL + " LIMIT " + SNAPSHOT_PAGE_SIZE;
        }
        return SELECT_PAGE_SQL.replace("${lastId}", Long.toString(pageStart))
                .replace("${pageSize}", Integer.toString(SNAPSHOT_PAGE_SIZE));
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
     * ordinary predicate / value literal ({@code s = '_#TEMP#_'}) or a comment carries the
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
     * <p>Every GLOBAL CREATE used to filter bind_sql_digest / plan_sql in SQL, but the
     * table is keyed and distributed only by id: that predicate scans EVERY bucket and
     * row, while the table grows without a cap (auto capture), so the lookup eventually
     * ran into its fixed timeout and CREATE slowed down / failed as baselines
     * accumulated. The store already holds every row (the load reads them all) plus
     * every local write, and it is invalidated + reloaded on promotion / forwarded DDL,
     * so the in-memory index answers correctly whenever the table has no NEWER id than
     * the store has seen ({@link #mustScanDurableForKey}); only a newer id - another
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
        return first;
    }

    /**
     * Forces an authoritative reload of the internal table (master acquisition): the
     * in-memory cache may have been loaded long BEFORE this FE became master, so it can
     * miss every row the previous master wrote after that load - the create-time key
     * dedup would then miss an existing durable baseline. {@code loaded} is cleared
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
     * together with {@code loaded}. Clearing ONLY {@code loaded} left the OLD maps visible:
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
                // the pending-create records describe writes of the INVALIDATED state: a
                // reload sees a committed row once it publishes, and a create that still
                // cannot see it re-remembers the identity itself
                pendingCreates.clear();
                stateVersion++;
            } finally {
                stateLock.writeLock().unlock();
            }
        }
    }

    /**
     * For tests: the invalidation half of {@link #forceReloadFromInternalTable} (the
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
     * <p>The TIMESTAMPS are NOT part of this comparison: they are adopted by
     * {@link #copyPersistedTimestamps} instead, which keeps the object identity (and with
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
     * {@link #persistedContentChanged}).
     *
     * <p>The comparison is at the internal table's DATETIME (SECOND) precision: memory
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
     * (SECOND) precision (see {@link #copyPersistedTimestamps}).
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
     * {@link #INSERT_IF_PREVIOUS_STATUS_SQL}): the new-status row is coupled to the
     * PREVIOUS-status row still being durable.
     *
     * <p>An ALTER dispatched before a handoff - or merely stalled - could otherwise
     * INSERT the requested status AFTER the new master completed a DROP of that very
     * baseline: nothing was left to refuse it (the old row was gone, so the delete-old
     * step removed zero rows and the visibility confirmation passed), the resurrected row
     * survived the completed DROP and the next refresh restored an ACTIVE baseline. The
     * conditional statement performs the presence check INSIDE the same durable write, so
     * a vanished previous row writes nothing; the conflict is then reported as a
     * retryable failure instead of a silent success.
     */
    private static void persistTransitionInsert(BaselinePlan p, BaselineStatus previousStatus) {
        if (!persistenceEnabled() && idAllocatorStoreForTest == null
                && statusProtocolStoreForTest == null) {
            return; // in-memory only (no durable status rows exist)
        }
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.insert(p);
        } else if (statusProtocolStoreForTest != null) {
            if (!statusProtocolStoreForTest.insertIfPreviousPresent(p, previousStatus)) {
                throw statusConflict(p.getId(), previousStatus);
            }
        } else {
            writeConditionalStatusInsert(p, previousStatus);
            // A conditional insert whose WHERE matched no row writes NOTHING: the plain
            // visibility confirmation would report it as a publication lag. Check while
            // the absence is still unambiguous - the previous row is gone AND the new row
            // is absent (a still-present previous row means the insert simply has not
            // become readable yet).
            if (probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(), p.getStatus())
                    == DurablePresence.ABSENT
                    && durableRowCount(p.getId(), previousStatus) == 0) {
                throw statusConflict(p.getId(), previousStatus);
            }
        }
        confirmInsertVisible(p);
    }

    /** Runs the conditional status INSERT and hides an ambiguous commit behind a read. */
    private static void writeConditionalStatusInsert(BaselinePlan p, BaselineStatus previousStatus) {
        Map<String, String> params = insertParams(p);
        params.put("previousStatus", previousStatus.name());
        try {
            inInternalIoMode(() -> {
                StatisticsUtil.execUpdate(INSERT_IF_PREVIOUS_STATUS_SQL, params,
                        BASELINE_WRITE_TIMEOUT_SECONDS);
                return null;
            });
        } catch (Exception e) {
            // An INSERT that reports an error (typically a statement timeout) may still
            // have COMMITTED: the row carrying this id + key + the REQUESTED status is the
            // proof it landed (the previous-status row would not prove it - it is exactly
            // the row the conditional statement must not have matched).
            if (probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(), p.getStatus())
                    == DurablePresence.PRESENT) {
                LOG.warn("SPM persist (status insert) reported {} but the row is durable"
                        + " (id={}); keeping it", e.getMessage(), p.getId());
                return;
            }
            throw new RuntimeException("SPM persist (status insert) failed: " + e.getMessage(), e);
        }
    }

    /** The retryable conflict of a status flip whose previous-status row disappeared. */
    private static IllegalStateException statusConflict(long id, BaselineStatus previousStatus) {
        return new IllegalStateException("SPM cannot change the status of baseline " + id
                + ": its " + previousStatus + " row is gone (a concurrent DROP or status"
                + " flip won); retry the statement");
    }

    private static void persistInsert(BaselinePlan p) {
        if (idAllocatorStoreForTest != null) {
            idAllocatorStoreForTest.insert(p);
            confirmInsertVisible(p);
            return;
        }
        if (statusProtocolStoreForTest != null) {
            statusProtocolStoreForTest.insert(p);
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
            // write. The row carrying this id + key + STATUS is the proof it landed:
            // matching only (id, key) treated the still-present OLD-status row of an
            // ALTER as the freshly written new-status row - updateStatus then deleted
            // the old row and NO durable version remained (refresh / restart lost the
            // baseline). Any other outcome (absent or unconfirmable) reports the
            // original failure.
            if (probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(),
                    p.getStatus()) == DurablePresence.PRESENT) {
                LOG.warn("SPM persist (insert) reported {} but the row is durable (id={});"
                        + " keeping it", e.getMessage(), p.getId());
                return;
            }
            throw new RuntimeException("SPM persist (insert) failed: " + e.getMessage(), e);
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
        return params;
    }

    /**
     * A reported-successful INSERT whose row is not READABLE yet (see
     * {@link #confirmInsertVisible}). The write IS committed - the id it consumed must not
     * be handed out, and a retry of the SAME CREATE must defer instead of allocating a
     * second id (see {@link #pendingCreates}).
     */
    private static final class UnconfirmedInsertException extends IllegalStateException {
        UnconfirmedInsertException(String message) {
            super(message);
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
                    ? durableVisibilityProbeForTest.isReadable(p.getId(), p.getStatus())
                    : probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(),
                            p.getStatus()) == DurablePresence.PRESENT;
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
     * <p>An elapsed probe budget is NOT a failed delete: the statement reported SQL OK
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
     * <p>Like {@link #confirmIdentityGone}, an elapsed probe budget is NOT a failure: the
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
     * Outcome of one ambiguous-write reconciliation read (see {@link #probeDurableRow}).
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
     * (id, key) AND - when {@code status} is given - the EXPECTED status. A read FAILURE
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
        if (idAllocatorStoreForTest != null) {
            try {
                for (BaselinePlan row : idAllocatorStoreForTest.readById(id)) {
                    if (row.getId() == id
                            && Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                            && Objects.equals(row.getPlanSql(), planSql)
                            && (status == null || row.getStatus() == status)
                            && (schemaFingerprint == null || Objects.equals(
                                    row.getSchemaFingerprint(), schemaFingerprint))) {
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
                                row.getSchemaFingerprint(), schemaFingerprint))) {
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
     * <p>The leadership fence matters here: an ALTER of the OLD master can reach THIS
     * DELETE after a handoff while the new master already completed the opposite flip
     * (both ALTERs pass their early checks). The delayed {@code DELETE ... WHERE id AND
     * status=<old status>} then removed the ONLY durable row the new master had just
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
     * <p>The columns are zone-free, and the FEs of one cluster do not share a host zone:
     * rendering in {@code ZoneId.systemDefault()} made the stored value depend on the
     * writer's host zone, so the duplicate-row recovery ({@link #pickDurableWinner}, which
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

    /** Internal-table DATETIME literal (UTC, see {@link #toTs}) -> epoch millis. */
    @VisibleForTesting
    static long fromTs(String ts) {
        return LocalDateTime.parse(ts, TS_FORMAT)
                .toInstant(ZoneOffset.UTC).toEpochMilli();
    }
}
