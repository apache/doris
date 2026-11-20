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
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * Manages GLOBAL baselines, durable mutations, snapshot publication and candidate lookup. The live store uses the
 * baseline table, an id-reservation table and a high-water-mark/mutation-clock table. A bounded cache holds
 * complete hot hash buckets; a copy-on-write hash map provides the coarse lookup filter.
 *
 * GLOBAL DDL is routed to the master. Allocation reconciles durable row and reservation watermarks and confirms
 * reservations before inserting rows. Collision handling, tombstones, mutation windows and publication fences
 * handle delayed writes and leadership changes.
 *
 * writerLock serializes local mutation protocols. stateLock protects in-memory publication, and the row cache has
 * its own lock. Internal-table I/O stays outside stateLock. Matching first checks the volatile hash map and cache;
 * periodic refresh publishes durable changes across FEs.
 */
public class BaselineManager {

    private static final Logger LOG = LogManager.getLogger(BaselineManager.class);

    /** Singleton. */
    private static final BaselineManager INSTANCE = new BaselineManager();

    // internal-table persistence (__internal_schema.spm_baselines).

    /** Fully qualified internal table (FeConstants.INTERNAL_DB_NAME == "__internal_schema"). */
    private static final String SPM_BASELINES_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_TBL_NAME;

    /** The append-only id reservation table (see InternalSchema.SPM_BASELINES_SEQ_TBL_NAME). */
    private static final String SPM_BASELINES_SEQ_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_SEQ_TBL_NAME;

    /** The compact id high-water-mark table (see InternalSchema.SPM_BASELINES_HWM_TBL_NAME). */
    private static final String SPM_BASELINES_HWM_TABLE =
            FeConstants.INTERNAL_DB_NAME + "." + InternalSchema.SPM_BASELINES_HWM_TBL_NAME;

    /** Column order follows InternalSchema.SPM_BASELINES_SCHEMA (unpaged selection). */
    private static final String SNAPSHOT_COLUMNS =
            "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, " + storedMicros("`create_time`") + ", " + storedMicros("`update_time`")
            + ", `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest` FROM ";

    /**
     * Shared page ordering over every stored scalar column. Rows that tie on id, update time, status and SQL text
     * can still differ in parser modes, fingerprints, costs or other replay metadata. Ordering all columns makes
     * inclusive-id pagination stable; fully identical rows remain interchangeable.
     */
    private static final String SNAPSHOT_ORDER_BY = " ORDER BY `id`, `update_time`, `status`,"
            + " `bind_sql_digest`, `plan_sql`, `bind_sql`, `bind_sql_hash`, `query_id`,"
            + " `cost`, `query_time_ms`, `source`, `create_time`, `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest`";

    private static final String SELECT_ALL_ORDERED_SQL =
            SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE + SNAPSHOT_ORDER_BY;

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
            + " WHERE `id` >= ${lastId}" + SNAPSHOT_ORDER_BY
            + " LIMIT ${pageSize} OFFSET ${offset}";

    /**
     * Rows per snapshot page (see readPersistedSnapshot). Bounds what ONE
     * internal query has to return, so a growing table can no longer make the whole
     * snapshot read fail against a fixed timeout.
     */
    private static final int SNAPSHOT_PAGE_SIZE = 2000;

    /**
     * Fence re-reads a paginated snapshot read is allowed before it fails closed (see
     * readStableRawSnapshot): one DDL overlapping the loop then converges on the
     * retry, while a table that never stays stable must not be published as a snapshot.
     */
    private static final int SNAPSHOT_STABILITY_ATTEMPTS = 3;

    /**
     * The persistence-layer id watermark (see reserveAllocatedId): read
     *  before every id allocation. MAX over an aggregate is a light single-row query.
     */
    private static final String SELECT_MAX_ID_SQL = "SELECT MAX(`id`) FROM " + SPM_BASELINES_TABLE;

    /**
     * The compact id high-water mark: a tiny append-only table whose rows carry the
     * newest allocated id AND the newest MUTATION CLOCK tick (see SELECT_CLOCK_SQL). The
     * append-only HISTORY table (InternalSchema.SPM_BASELINES_SEQ_TBL_NAME) grows by one
     * row per create forever, so its MAX(baseline_id) - the only unbounded read on the create
     * path - was replaced: every allocation also records itself here (pruning the
     * superseded rows), and a pre-upgrade cluster only pays the legacy full read ONCE
     * (see readPersistedWatermark).
     */
    private static final String SELECT_HWM_SQL = "SELECT MAX(`watermark`) FROM "
            + SPM_BASELINES_HWM_TABLE;

    /**
     * Maximum age in seconds for an open mutation-window row to participate in the snapshot fence. An abandoned
     * row eventually expires so a failed FE cannot block snapshots indefinitely.
     * The bound covers opening, pruning and visibility probes before the row statement, plus that statement's
     * write timeout. After those probes, beginRowMutation also checks that enough lifetime remains before
     * dispatching the statement. Successful completion explicitly closes the writer's window.
     */
    private static final long MUTATION_WINDOW_MAX_AGE_SECONDS = 120L;
    // Use the column's TIMESTAMPTZ type so the duplicate-key DELETE compares a bare column with a literal.
    private static final String MUTATION_WINDOW_CUTOFF = "CAST(DATE_SUB(NOW(), INTERVAL "
            + MUTATION_WINDOW_MAX_AGE_SECONDS + " SECOND) AS TIMESTAMPTZ(" + InternalSchema.SPM_TIMESTAMP_SCALE + "))";

    /**
     * Maximum allowed lead of a stored update timestamp over wall time, in microseconds. Status writers use this
     * bound when incrementing an already-ahead timestamp so stored ordering remains within the shared fence
     * lifetime assumptions.
     */
    private static final long MAX_STORED_UPDATE_LEAD_MICROS = 60_000_000L;

    /**
     * How many low bits of a mutation tick carry the window's FE identity (see
     * mutationTickSalt): enough range that two FEs writing in the same millisecond almost never
     * collide, small enough that the millisecond stays in the high bits (the clock's
     * cluster-wide ordering: the remaining 43 positive bits cover roughly 279 years of millis).
     *
     * The salt only makes a collision UNLIKELY; beginRowMutation therefore VERIFIES that the
     * marker it appended is the only one carrying its tick (see MUTATION_TICK_OPEN_ATTEMPTS).
     */
    private static final int MUTATION_TICK_FE_BITS = 20;

    /**
     * How many fresh ticks one window open may try when the marker it appended shares its tick
     * with another FE's window (see beginRowMutation): a random salt is not a guarantee, and two
     * windows sharing one identity would let either writer's exact-row close delete the OTHER's
     * still-open marker - turning an in-flight row statement into a quiet clock.
     */
    private static final int MUTATION_TICK_OPEN_ATTEMPTS = 3;

    /** The value mask of MUTATION_TICK_FE_BITS (see nextMutationTick). */
    private static final long MUTATION_TICK_FE_MASK = (1L << MUTATION_TICK_FE_BITS) - 1;

    /**
     * The bounded MUTATION CLOCK of the paginated snapshot fence (see
     * readStableRawSnapshot): MAX(tick) plus the slot's row count / tick sum. Every
     * spm_baselines mutation (create / delete / status flip) appends a clock row with a
     * strictly increasing tick BEFORE its own statement, so the token changes whenever
     * the durable table changes - without any table-wide aggregate on the read path. The
     * count / sum stay as tie-breakers for an append whose tick collides with the current
     * maximum (skewed clocks): an append raises MAX(tick) or the pair (count, sum), so a
     * fence read around a paginated snapshot observes the mutation and retries instead of
     * publishing a mixed state (a residual collision of two same-instant appends from
     * different FEs converges on the next refresh, like the fence's second-precision
     * predecessor).
     *
     * The fourth column is the OPEN-WINDOW marker: MAX(pending) over the rows written
     * within MUTATION_WINDOW_MAX_AGE_SECONDS. Any unclosed window poisons the fence (that
     * is the point - a window is only closed by its own completion write, see
     * beginRowMutation / endRowMutation), but a window WHOSE FE DIED before its completion
     * became a PERMANENT poison: pending stayed non-null forever and every snapshot read
     * failed, so the store could never load again. A mutation's own WINDOW - the pre-write
     * budget AND the row statement it fences, see MUTATION_WINDOW_MAX_AGE_SECONDS, whose
     * bound beginRowMutation verifies before dispatching anything - fits inside the window
     * bound, so a begin row older than it cannot still be mid-statement and is ignored.
     */
    private static final String SELECT_CLOCK_SQL = "SELECT MAX(`tick`), COUNT(*),"
            + " SUM(`tick`), MAX(CASE WHEN `update_time` > " + MUTATION_WINDOW_CUTOFF + " THEN `pending` ELSE 0 END)"
            + " FROM " + SPM_BASELINES_HWM_TABLE;

    /**
     * The slot's newest watermark AND clock tick; the pre-mutation build of the mutation
     * window (and the completion append) reads it to keep the recorded watermark untouched
     * while advancing the clock (see beginRowMutation). One bounded keyed read.
     */
    private static final String SELECT_SLOT_STATE_SQL = "SELECT MAX(`watermark`),"
            + " MAX(`tick`) FROM " + SPM_BASELINES_HWM_TABLE;

    /**
     * Whether the OPEN mutation window just appended is READABLE: the row carrying the very
     * tick this process wrote in pending (see beginRowMutation). A plain append can
     * report SQL OK while its row is still unreadable - and with the window marker only
     * readable LOCALLY, a successor's fence read (SELECT_CLOCK_SQL, the MATCHES path of
     * updateStatus) sees a QUIET clock while this process's row statement is already in
     * flight: the statement then publishes AFTER the successor accepted a matching read
     * and reversed it. The bounded read-back makes the statement's dispatch
     * conditional on the window being SHARED.
     */
    private static final String SELECT_PENDING_TICK_SQL = "SELECT COUNT(*) FROM "
            + SPM_BASELINES_HWM_TABLE
            + " WHERE `tick` = ${tick} AND `pending` = ${tick}";

    /** Append one high-water-mark / clock row (see SELECT_HWM_SQL, SELECT_CLOCK_SQL). */
    private static final String INSERT_HWM_SQL = "INSERT INTO " + SPM_BASELINES_HWM_TABLE
            + " (`watermark`, `update_time`, `tick`, `pending`)"
            + " VALUES (${lastId}, NOW(), ${tick}, ${pending})";

    /**
     * Best-effort pruning of completed high-watermark rows superseded by the latest watermark or tick. Higher
     * watermarks and open mutation-window rows are retained. Open rows are shared evidence of in-flight mutations
     * and only their own completion may close them. The pending = 0 predicate also leaves legacy NULL rows
     * untouched.
     */
    private static final String PRUNE_HWM_SQL = "DELETE FROM " + SPM_BASELINES_HWM_TABLE
            + " WHERE `watermark` < ${lastId} AND `tick` < ${tick}"
            + " AND `pending` = 0";

    /** The same-watermark half of PRUNE_HWM_SQL (rows superseded by the newer tick). */
    private static final String PRUNE_HWM_SAME_WATERMARK_SQL = "DELETE FROM "
            + SPM_BASELINES_HWM_TABLE
            + " WHERE `watermark` = ${lastId} AND `tick` < ${tick}"
            + " AND `pending` = 0";

    /**
     * Deletes the completed mutation's own open-window row using both its tick and pending token. General
     * watermark pruning keeps open rows, so only this writer's completion closes this window. Tick generation uses
     * a random FE salt; beginRowMutation checks for readable collisions before dispatching the row mutation.
     */
    private static final String PRUNE_HWM_OPEN_SQL = "DELETE FROM " + SPM_BASELINES_HWM_TABLE
            + " WHERE `tick` = ${windowTick} AND `pending` = ${windowTick}";

    private static final String EXPIRED_MUTATION_WINDOWS = "`pending` > 0 AND `update_time` <= "
            + MUTATION_WINDOW_CUTOFF;
    private static final int MUTATION_WINDOW_SWEEP_BATCH_SIZE = 1024;
    private static final String SELECT_EXPIRED_WINDOWS_SQL = "SELECT MAX(`watermark`), MAX(`tick`) FROM"
            + " (SELECT `watermark`, `tick` FROM " + SPM_BASELINES_HWM_TABLE
            + " WHERE " + EXPIRED_MUTATION_WINDOWS + " ORDER BY `tick` LIMIT "
            + MUTATION_WINDOW_SWEEP_BATCH_SIZE + ") expired";
    private static final String SELECT_COVERING_CLOCK_SQL = "SELECT MAX(`tick`) FROM "
            + SPM_BASELINES_HWM_TABLE + " WHERE `pending` = 0 AND `watermark` >= ${lastId}";
    private static final String PRUNE_EXPIRED_WINDOWS_SQL = "DELETE FROM " + SPM_BASELINES_HWM_TABLE
            + " WHERE " + EXPIRED_MUTATION_WINDOWS + " AND `watermark` <= ${lastId} AND `tick` <= ${tick}";

    /**
     * The scoped CONFIRMATION of SELECT_HWM_SQL (see readCompactIdWatermark): the
     * highest reservation VISIBLE beyond the recorded mark. The sequence table's key
     * starts with (id, baseline_id), so the read scans only the rows past the mark - a
     * healthy cluster has none, and a stale record never hides a reservation whose
     * HWM write was lost or is still unreadable.
     */
    private static final String SELECT_SEQ_TAIL_SQL = "SELECT MAX(`baseline_id`) FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = 1 AND `baseline_id` > ${floor}";

    /**
     * The id high-water mark that OUTLIVES the rows (see
     * InternalSchema.SPM_BASELINES_SEQ_TBL_NAME): MAX(baseline_id) over the append-only
     * reservation rows. Kept as the ONE-TIME legacy fallback of
     * readPersistedWatermark (a cluster created before the compact high-water
     * mark table exists); the per-create path reads the bounded
     * SELECT_HWM_SQL instead. The baselines table's own MAX(id) falls back to a
     * lower value as soon as its highest row is DROPped, and an id reused for a DIFFERENT
     * baseline would let a delayed DROP BASELINE PLAN IF EXISTS N retry delete the
     * new baseline.
     */
    private static final String SELECT_SEQ_ID_SQL = "SELECT MAX(`baseline_id`) FROM "
            + SPM_BASELINES_SEQ_TABLE;

    /**
     * Publishes the identity state and its allocation-history or id-scoped DROP record in one
     * transaction. The keyed identity read can therefore establish the state on its own.
     */
    private static final String INSERT_SEQ_STATE_SQL = "INSERT INTO " + SPM_BASELINES_SEQ_TABLE
            + " (`id`, `baseline_id`, `bind_sql_digest`, `plan_sql_hash`, `reserve_time`,"
            + " `unconfirmed`, `dropped`)"
            + " VALUES (${seqId}, ${lastId}, '${bindSqlDigest}', ${planSqlHash},"
            + " '${reserveTime}', 0, ${dropped}),"
            + " (${compactId}, ${lastId}, '${bindSqlDigest}', ${planSqlHash},"
            + " '${reserveTime}', 0, ${dropped})";

    /**
     * The id key of one DROP TOMBSTONE (see INSERT_SEQ_STATE_SQL): a dedicated NEGATIVE
     * id space offset by the baseline id, disjoint from every other row of the sequence
     * table (the historical reservations and the watermark carry the constant id = 1, the
     * compact identity rows carry COMPACT_SEQ_ID_BASE + a per-identity hash).
     *
     * Keying the tombstones by the BASELINE id is what BOUNDS the tombstone read: the
     * duplicate key is id, so id IN (...) seeks exactly the requested identities
     * instead of scanning the whole append-only history once per id chunk. The previous
     * dropped = 1 AND baseline_id IN (...) form constrained neither key column, so with a
     * large CREATE / DROP history every chunk re-read the entire table and eventually
     * exhausted the fixed query timeout - and BOTH the refresh and the confirmed reads go
     * through it, so GLOBAL SHOW / management failed with them.
     */
    private static final long TOMBSTONE_SEQ_ID_BASE = -1L;

    /**
     * Reads the DROP TOMBSTONES of the GIVEN ids (see INSERT_SEQ_STATE_SQL,
     * the append-only sequence table retains every dropped = 1 row
     * forever, so an unrestricted read built a HashSet of the FULL historical drop set on
     * every load / refresh - with a small active set and heavy CREATE / DROP churn the
     * read grew without bound and, once it timed out, follower caches stopped
     * incorporating later GLOBAL changes. The read is scoped to the ids the caller is
     * actually filtering (the snapshot / point-read ids), chunked to keep each statement
     * bounded, and KEYED by the tombstone id space (see TOMBSTONE_SEQ_ID_BASE): the
     * id IN (...) seek is what keeps each chunk's cost independent of the history's
     * length.
     */
    private static final String SELECT_SEQ_DROPPED_SQL = "SELECT `baseline_id`, `bind_sql_digest`,"
            + " `plan_sql_hash` FROM " + SPM_BASELINES_SEQ_TABLE
            + " WHERE `id` IN (${tombstoneIds}) AND `baseline_id` IN (${ids})"
            + " AND `dropped` = 1";

    /** How many ids one scoped tombstone read carries (see SELECT_SEQ_DROPPED_SQL). */
    private static final int DROPPED_MARKER_ID_CHUNK = 256;

    /**
     * The stable key for one identity's current reservation and lifecycle markers. Positive
     * slots above 1 are disjoint from the id allocation history and the negative DROP keys.
     */
    private static final long COMPACT_SEQ_ID_BASE = 2L;

    /** Appends the compact copy of one identity state (see COMPACT_SEQ_ID_BASE). */
    private static final String INSERT_COMPACT_SEQ_SQL = "INSERT INTO " + SPM_BASELINES_SEQ_TABLE
            + " (`id`, `baseline_id`, `bind_sql_digest`, `plan_sql_hash`, `reserve_time`,"
            + " `unconfirmed`, `dropped`)"
            + " VALUES (${compactId}, ${lastId}, '${bindSqlDigest}', ${planSqlHash},"
            + " '${reserveTime}', ${unconfirmed}, ${dropped})";

    /** Older incarnations of this identity can be pruned after publishing the newer one. */
    private static final String PRUNE_COMPACT_SEQ_SQL = "DELETE FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = ${compactId} AND `baseline_id` < ${lastId}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql_hash` = ${planSqlHash}";

    /**
     * Reads the newest state within one identity. The baseline id orders successive incarnations.
     * Within one id a DROP is terminal, even when a delayed pending marker is timestamped later.
     */
    private static final String SELECT_COMPACT_SEQ_SQL = "SELECT `baseline_id`, "
            + storedMicros("`reserve_time`") + ", `unconfirmed`, `dropped` FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = ${compactId}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql_hash` = ${planSqlHash}"
            + " ORDER BY `baseline_id` DESC, `dropped` DESC, `reserve_time` DESC,"
            + " `unconfirmed` ASC LIMIT 1";

    /**
     * The BATCHED form of SELECT_COMPACT_SEQ_SQL (see
     * #BaselinePersistence.readLatestReservations): the newest compact row of every
     * requested slot in ONE read. The rows are ordered so the caller can keep the FIRST row of
     * each slot - the same newest-first order the single read takes.
     */
    private static final String SELECT_COMPACT_SEQ_BATCH_SQL = "SELECT `id`, `baseline_id`, "
            + storedMicros("`reserve_time`") + ", `unconfirmed`, `dropped` FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` IN (${compactIds})"
            + " ORDER BY `id` ASC, `baseline_id` DESC, `dropped` DESC, `reserve_time` DESC,"
            + " `unconfirmed` ASC";

    /**
     * How many identity slots one batched compact read asks for (see
     * withIdentityDrops): a snapshot of 2,000 live baselines then costs 20 bounded queries
     * instead of 2,000 distributed ones, while the IN list stays short enough for the planner
     * of the duplicate-key delete/read fallbacks.
     */
    private static final int IDENTITY_STATE_BATCH_SIZE = 100;

    /**
     * Retires the UNCONFIRMED marker(s) of ONE resolved ambiguous write:
     * flipped by retireSeqPendingMarker. The DELETE touches only
     * unconfirmed = 1 rows of that baseline_id - the plain reservation row appended
     * before every create (and every other id ever reserved) stays, so MAX(baseline_id) and
     * with it the id watermark never fall.
     */
    private static final String DELETE_PENDING_SEQ_MARKER_SQL = "DELETE FROM "
            + SPM_BASELINES_SEQ_TABLE + " WHERE `id` = ${compactId} AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql_hash` = ${planSqlHash} AND `unconfirmed` = 1"
            + " AND `baseline_id` = ${lastId}";

    // The consistency fence of the paginated snapshot read USED to live here as the
    // table-wide "SELECT MAX(id), COUNT(*), MAX(update_time)" aggregate: an internal
    // paginated snapshot issues one SELECT per page and the internal table has no
    // long-lived read view, so a CREATE / ALTER / DROP committing between two pages would
    // otherwise be merged into a state no single point in time ever had. The aggregate
    // itself had to scan the WHOLE table on every refresh / status change, so it is
    // replaced by the bounded mutation clock (SELECT_CLOCK_SQL) of
    // SPM_BASELINES_HWM_TABLE, which every mutation advances BEFORE its own statement,
    // and by the end probe of the pagination loop (see readStableRawSnapshot).

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
            + " `status`, " + storedMicros("`create_time`") + ", " + storedMicros("`update_time`")
            + ", `sql_mode`, `plan_sql_mode`,"
            + " `plan_frozen`, `schema_fingerprint`, `plan_sql_digest` FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id}";

    /**
     * The COMPLETE row set of one bindSqlHash (the matching-path cache-miss read): every
     * baseline of that bind_sql, whatever its status / plan text (see
     * completeRowsForHash). The hash column is not a key column, so this is a scan of the
     * (bounded) baseline table - the LRU exists exactly to keep it off the query hot path.
     */
    private static final String SELECT_BY_HASH_SQL = SNAPSHOT_COLUMNS + SPM_BASELINES_TABLE
            + " WHERE `bind_sql_hash` = ${hash}";

    /**
     * Durable-key lookup used by the create-time dedup: the in-memory index can be stale
     * (a follower that loaded=true before becoming master missed rows written afterwards),
     * so the authoritative duplicate check reads the (bind_sql_digest, plan_sql) key back
     * from the table before a new row is inserted.
     */
    private static final String SELECT_BY_KEY_SQL = "SELECT `id`, `bind_sql`, `bind_sql_digest`,"
            + " `bind_sql_hash`, `plan_sql`, `query_id`, `cost`, `query_time_ms`, `source`,"
            + " `status`, " + storedMicros("`create_time`") + ", " + storedMicros("`update_time`")
            + ", `sql_mode`, `plan_sql_mode`,"
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
            + " AND `plan_sql` = '${planSql}'"
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'";

    /**
     * The EMPTY-fingerprint form of DELETE_BY_IDENTITY_SQL (a TVF-only baseline has no
     * catalog relation / scalar function, so its stored fingerprint is empty): the NULL
     * and empty-string forms are TWO SIMPLE predicates, because
     * IFNULL(schema_fingerprint, '') = '' requires IS NULL OR = '' on the nullable
     * column and DeleteFromCommand rejects both that function form and its OR rewrite
     * (and the USING fallback only supports UNIQUE KEY tables) - the baseline could
     * never be dropped.
     */
    private static final String DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` IS NULL";

    /**
     * See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the empty-STRING form (pre-column
     * rows store NULL, current writes store the empty string).
     */
    private static final String DELETE_BY_IDENTITY_SQL_EMPTY_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `bind_sql_digest` = '${bindSqlDigest}'"
            + " AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` = ''";

    /**
     * Removes one baseline row by id + previous status + its identity (status UPDATE
     * support). The SCHEMA FINGERPRINT completes the identity like it does in
     * DELETE_BY_IDENTITY_SQL: matching (id, status, digest, planSql) alone deleted a
     * NEWER incarnation's row that shared the same id / digest / planSql under a
     * different fingerprint.
     *
     * The stored VERSION of the old row joins the predicate as well (see
     * deleteByIdAndStatusSql, for example: a demoted master's delayed
     * cleanup DELETE matched (id, old status, identity) alone and erased the row a LATER
     * flip BACK to that status had written - both ALTERs reported success and the baseline
     * disappeared.
     */
    private static final String DELETE_BY_ID_AND_STATUS_SQL = "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `status` = '${status}'"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'";

    /**
     * See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the empty-fingerprint (TVF-only)
     * variants of the status-cleanup DELETE, as two SUPPORTED simple predicates.
     */
    private static final String DELETE_BY_ID_AND_STATUS_SQL_NULL_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `status` = '${status}'"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` IS NULL";

    /** See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the empty-STRING form. */
    private static final String DELETE_BY_ID_AND_STATUS_SQL_EMPTY_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE
            + " WHERE `id` = ${id} AND `status` = '${status}'"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` = ''";

    /**
     * Reclaims the SUPERSEDED versions of one baseline identity (see
     * BaselineManager.pruneSupersededVersionsFromTable): a successful no-op ALTER appends an
     * ordering-intent row and nothing ever removed the earlier ones, so one baseline
     * accumulated unbounded durable rows - and every cold match re-parsed ALL of them before
     * collapsing by id. update_time is a BARE column on the left of the predicate (the
     * duplicate-key delete fallback rejects a functional left side, see
     * deleteByIdAndStatusSql), and only strictly EARLIER versions are removed, so the intent
     * row itself and every NEWER concurrent version survive.
     */
    private static final String DELETE_SUPERSEDED_VERSIONS_SQL = "DELETE FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'"
            + " AND `update_time` < '${keepFrom}'";

    /** See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the NULL-fingerprint variant. */
    private static final String DELETE_SUPERSEDED_VERSIONS_SQL_NULL_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` IS NULL"
            + " AND `update_time` < '${keepFrom}'";

    /** See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the empty-STRING variant. */
    private static final String DELETE_SUPERSEDED_VERSIONS_SQL_EMPTY_FINGERPRINT =
            "DELETE FROM " + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` = ''"
            + " AND `update_time` < '${keepFrom}'";

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
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'"
            + " LIMIT 1";

    /**
     * Reconciliation read of the ambiguous status-update path: how many durable rows
     *  currently carry (id, status) AND the identity's schema fingerprint.
     */
    private static final String COUNT_BY_ID_AND_STATUS_SQL = "SELECT COUNT(*) FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id} AND `status` = '${status}'"
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'";

    /**
     * Visibility probe of a version PRUNE (see BaselineManager.confirmPruneVisible): how many
     * of the identity's versions are still readable below the retained stored instant. The
     * prune's DELETE removing them is update_time < the same bound.
     */
    private static final String COUNT_VERSIONS_BEFORE_SQL = "SELECT COUNT(*) FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND IFNULL(`schema_fingerprint`, '') = '${schemaFingerprint}'"
            + " AND `update_time` < '${keepFrom}'";

    /** See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the NULL-fingerprint variant. */
    private static final String COUNT_VERSIONS_BEFORE_SQL_NULL_FINGERPRINT =
            "SELECT COUNT(*) FROM " + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` IS NULL"
            + " AND `update_time` < '${keepFrom}'";

    /** See DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT: the empty-STRING variant. */
    private static final String COUNT_VERSIONS_BEFORE_SQL_EMPTY_FINGERPRINT =
            "SELECT COUNT(*) FROM " + SPM_BASELINES_TABLE + " WHERE `id` = ${id}"
            + " AND `bind_sql_digest` = '${bindSqlDigest}' AND `plan_sql` = '${planSql}'"
            + " AND `schema_fingerprint` = ''"
            + " AND `update_time` < '${keepFrom}'";

    /**
     * The newest stored update_time of THIS id's rows: the status-flip bump advances the
     * new row past this value (see updateStatus), which keeps the flipped row the durable
     * winner among the id's lingering rows (pickDurableWinner prefers the later stored
     * instant). A former table-wide read served the OLD snapshot fence (which compared
     * MAX(update_time) around its page loop); the fence is now the bounded mutation clock
     * (see SELECT_CLOCK_SQL), so the bump read only needs this id's key prefix.
     *
     * The value is read at the column's OWN precision (see storedMicros): the MICROSECOND
     * difference from the epoch of a TIMESTAMPTZ(6) IS the stored instant, without any civil-time
     * rendering in between - scaling it to a coarser unit here is what used to throw that
     * precision away.
     */
    private static final String SELECT_MAX_UPDATE_TIME_SQL = "SELECT "
            + storedMicros("MAX(`update_time`)") + " FROM "
            + SPM_BASELINES_TABLE + " WHERE `id` = ${id}";

    /**
     * DateTimeFormatter of the internal TIMESTAMPTZ columns: literals are written with an
     * explicit UTC offset at the column's MICROSECOND scale (see toTs); reads never parse the
     * rendering - every SELECT list reads the stored instant through storedMicros instead.
     */
    private static final DateTimeFormatter TS_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSxxx");

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
     * createBaseline runs synchronously under writerLock,
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
     * the explicit unconfirmed marker and the plain reservation row appended before
     * every baseline write; when it elapses with the row still unreadable,
     * the identity is condemned with a tombstone before a fresh id is allocated
     */
    private static final long DURABLE_PENDING_CREATE_FENCE_MICROS = 300_000_000L;

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
        final long id;
        final String bindSqlDigest;
        final long planSqlHash;

        PendingDropMarker(long id, String bindSqlDigest, long planSqlHash) {
            this.id = id;
            this.bindSqlDigest = bindSqlDigest;
            this.planSqlHash = planSqlHash;
        }

        String key() {
            return droppedIdentityKey(id, bindSqlDigest, planSqlHash);
        }
    }

    /**
     * One retained DROP tombstone obligation (see confirmDropTombstonesDurable): the
     * identities this DROP removed, INCLUDING the same-key twins retired with it (see
     * retireSupersededKeyTwins). Each complete scope key retains its marker, so identity and id-scoped markers
     * of one id coexist, including multiple identities left by an id collision. The twins share this obligation: their
     * DELETE is durably confirmed, but it left a process-local marker only - if that
     * append failed while the removed row's markers were readable, the DROP used to
     * report success and a demoted master's already-dispatched status INSERT for the twin
     * could publish, reviving the dropped baseline on every other FE / after a restart.
     */
    private static final class PendingDropObligation {
        final Map<String, PendingDropMarker> owed = new java.util.LinkedHashMap<>();
    }

    /**
     * Drop tombstones whose durable append has not succeeded yet, complete scope key -> marker (see
     * writeDroppedMarker). Process-local by nature: another FE never saw the failed write,
     * but it also lost this tombstone's delayed-commit protection entirely, which is the
     * pre-existing exposure the retry shrinks.
     */
    private static final Map<String, PendingDropMarker> pendingDroppedMarkers =
            new ConcurrentHashMap<>();

    /**
     * The drop ids whose tombstone READABILITY confirmation failed (see
     * confirmDropTombstonesDurable): the DROP was reported retryably with its row already
     * deleted, and a retry that finds the row ABSENT - which never reaches the removed
     * identity objects again - must re-append and confirm these markers before it may
     * report the absent row as "nothing to drop". Dropping that obligation let the
     * unreadable markers stay unappended while a demoted master's delayed status INSERT
     * revived the baseline. id -> the identities owed for that
     * DROP (the removed row AND the same-key twins retired with it).
     */
    private static final Map<Long, PendingDropObligation> pendingDropTombstoneConfirmations =
            new ConcurrentHashMap<>();

    /**
     * The drop ids whose KEY CLEANUP (the same-key twin retirement of retireSupersededKeyTwins)
     * is still unfinished, id -> the retained identities and their allocation upper bounds. An id collision can
     * leave several identities under the same id; retry must retain every identity the DROP removed.
     *
     * A DROP deletes its own row FIRST and retires the same-key twins afterwards. When a twin's
     * delete throws, the retry finds the id's row ABSENT - so it never reaches
     * retireSupersededKeyTwins with an identity again - and the STILL DURABLE twin keeps
     * matching the dropped key (with same-key handoff rows 40/41/42 it is 42: its higher
     * allocation is the newest non-dropped compact reservation, so the identity filter does not
     * hide it). The retained identity lets an absent-row retry RESUME the cleanup (see
     * resumePendingDropKeyCleanup) before it settles the tombstone obligation.
     *
     * The retention is SCOPE-BOUND: a resumed cleanup re-discovers twins with the
     * (digest, planSql, fingerprint) key check, and a CREATE of the SAME SQL that ran AFTER this
     * DROP is durable is exactly such a row - retiring it deleted a baseline the user had just
     * created. The retained allocation watermark therefore bounds retirement to the rows that already
     * existed when the DROP removed its identity (see retireSupersededKeyTwins).
     */
    private static final Map<Long, List<PendingDropKeyCleanup>> pendingDropKeyCleanups =
            new ConcurrentHashMap<>();

    /**
     * A DROP's same-key retirement obligation, bounded by the allocation high-water mark read before deleting the
     * original rows. Retried cleanup removes only twins at or below this id, preserving a later CREATE of the same
     * identity. The bound uses allocation order rather than mutable update timestamps.
     */
    private static final class PendingDropKeyCleanup {
        private final BaselinePlan identity;
        /**
         * The ID high-water mark captured before the DROP dispatches its deletes: a resumed
         * retirement only removes twins allocated AT OR BELOW it. A CREATE of the same SQL that
         * lands afterwards carries a LARGER id (ids are allocated monotonically) and is left
         * alone.
         *
         * The bound is the ID, not a timestamp. A timestamp bound compared against
         * the twin's stored update time is unreliable in both directions: the status flip pushes
         * a row's stored instant past wall time, so a PREEXISTING twin carrying a future-dated
         * update time was skipped, while a later CREATE with an update time at or below the bound was deleted.
         * Allocation order is exactly what "existed at DROP time"
         * means, and it is monotonic cluster-wide.
         */
        private final long retainedMaxId;

        PendingDropKeyCleanup(BaselinePlan identity, long retainedMaxId) {
            this.identity = identity;
            this.retainedMaxId = retainedMaxId;
        }
    }

    /**
     * A mutation window this FE holds OPEN after its row statement was DISPATCHED but its
     * committed row is still UNREADABLE (see holdUnresolvedMutationWindow): the window -
     * not the process-local fence below - is the SHARED evidence other FEs consult, so a
     * successor that reads the OLD status row as final fails retryably instead of
     * accepting a no-op the committed row then reverses.
     */
    private static final class UnresolvedMutationWindow {
        final long id;
        final BaselineStatus expectedStatus; // null = the row must become absent
        final long attemptedUpdateTimeMicros;
        final long sinceMillis;
        /**
         * When non-null, the window ALSO fences the version PRUNE the statement ran
         * (persistNoOpStatusIntent): a pruned row a paginated reader still holds lets its
         * stale page be accepted, so the window may only close once the reclaimed versions
         * are provably gone as well.
         */
        final BaselinePlan pruneIdentity;
        final long pruneKeepFromMicros;

        UnresolvedMutationWindow(long id, BaselineStatus expectedStatus,
                long attemptedUpdateTimeMicros, long sinceMillis) {
            this(id, expectedStatus, attemptedUpdateTimeMicros, sinceMillis, null, 0L);
        }

        UnresolvedMutationWindow(long id, BaselineStatus expectedStatus,
                long attemptedUpdateTimeMicros, long sinceMillis, BaselinePlan pruneIdentity,
                long pruneKeepFromMicros) {
            this.id = id;
            this.expectedStatus = expectedStatus;
            this.attemptedUpdateTimeMicros = attemptedUpdateTimeMicros;
            this.sinceMillis = sinceMillis;
            this.pruneIdentity = pruneIdentity;
            this.pruneKeepFromMicros = pruneKeepFromMicros;
        }

        /** Whether the snapshot shows the committed outcome (mirrors PendingMutationFence). */
        boolean isSatisfiedBy(BaselinePlan row) {
            if (expectedStatus == null) {
                return row == null;
            }
            if (row == null) {
                return false;
            }
            if (attemptedUpdateTimeMicros > 0) {
                // The STATUS alone is not the outcome: a successful NO-OP intent carries the
                // SAME status as the row it supersedes, and a snapshot still showing the OLDER
                // same-status row closed the window while the newer intent was unreadable - the
                // successor it was fencing could then qualify an opposite-status write.
                return row.getStatus() == expectedStatus
                        && row.getUpdateTime() >= attemptedUpdateTimeMicros;
            }
            return row.getStatus() == expectedStatus;
        }

        /**
         * Whether the window's PRUNE half is settled: no version it reclaimed is readable
         * any more (a store that models no versions answers 0 - nothing to prune).
         *
         * Parameter store: the persistence the probe reads
         * Returns: whether the reclaimed versions are provably gone
         */
        boolean isPruneProbeSatisfied(BaselinePersistence store) {
            if (pruneIdentity == null) {
                return true;
            }
            try {
                return store.countVersionsBefore(pruneIdentity, pruneKeepFromMicros) == 0;
            } catch (RuntimeException e) {
                return false; // the outcome cannot be read: the window stays held
            }
        }

        /**
         * Both halves of the window: the committed row readable AND (when the statement
         * pruned) the reclaimed versions provably gone.
         */
        boolean isSettled(BaselinePlan row, BaselinePersistence store) {
            return isSatisfiedBy(row) && isPruneProbeSatisfied(store);
        }
    }

    /**
     * The mutation windows this FE holds open because the statement they fence reported
     * success while its committed row stayed unreadable (tick -> the outcome the write
     * proves, see holdUnresolvedMutationWindow). Process-local bookkeeping of a SHARED
     * fence: the pending row in the clock table is what every other FE's fence read sees,
     * and it is closed once a snapshot read here shows the outcome (see
     * resolveHeldMutationWindows) or the window ages past
     * MUTATION_WINDOW_MAX_AGE_SECONDS, after which every fence read ignores it anyway.
     */
    private static final Map<Long, UnresolvedMutationWindow> unresolvedMutationWindows =
            new ConcurrentHashMap<>();

    /**
     * The instant each LOCALLY opened mutation window was opened (tick -> epoch millis, see
     * beginRowMutation): a row statement is dispatched with the window's age measured
     * against it (see assertMutationWindowDispatchable) - the window was verified when it
     * OPENED, and a stalled caller must not dispatch a statement into a window every
     * successor already ignores. Bounded by the number of in-flight writes of this FE;
     * endRowMutation removes the entry (a window held open by
     * holdUnresolvedMutationWindow keeps it, so its resolution / age-out removes it).
     */
    private static final Map<Long, Long> mutationWindowOpenedAt = new ConcurrentHashMap<>();

    /** The process-local monotonic source of mutation-clock ticks (see SELECT_CLOCK_SQL). */
    private static final java.util.concurrent.atomic.AtomicLong lastMutationTick =
            new java.util.concurrent.atomic.AtomicLong();

    /**
     * The process-unique low bits of every mutation tick (see nextMutationTick). The
     * millisecond alone does NOT identify a window: two FEs can open one within the SAME
     * millisecond, and a tick-only identity made their markers indistinguishable - a
     * visibility probe could confirm ANOTHER FE's already-readable marker as its own,
     * reporting the window shared while this FE's own marker was still unreadable, and
     * either FE's completion DELETE (PRUNE_HWM_OPEN_SQL) removed BOTH markers, so
     * SELECT_CLOCK_SQL reported a QUIET clock while the other FE's DISABLE / DELETE was
     * still in flight - a successor could then report a matching ENABLE as successful or
     * accept a paginated snapshot before that write committed. The salt keeps the windows
     * of concurrently writing FEs apart; the millisecond in the high bits preserves the
     * cluster-wide ORDER of the clock (the count / sum tie-breakers still normalize a
     * residual same-millisecond ordering).
     */
    private static volatile long mutationTickSalt = initialMutationTickSalt();

    // priority ordering.

    /**
     * Candidate ordering uses shorter execution time when both times are known, and lower estimated cost when both
     * are unknown. A candidate with unknown time precedes one with known time. Negative times mean unknown,
     * including the default -1; zero is known. Equal ranking values compare equal, without an id or source
     * tie-breaker.
     */
    private static final Comparator<BaselinePlan> comparator = (o1, o2) -> {
        // Negative times are unknown; zero is a valid known measurement, so known is
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

    /** Deterministic content ordering after update-time and active-status comparisons. */
    private static final Comparator<BaselinePlan> durableContentComparator = Comparator
            .comparing(BaselinePlan::getBindSqlDigest, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparing(BaselinePlan::getPlanSql, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparing(BaselinePlan::getBindSql, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparingLong(BaselinePlan::getBindSqlHash)
            .thenComparing(BaselinePlan::getQueryId, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparingDouble(BaselinePlan::getCost)
            .thenComparingLong(BaselinePlan::getQueryTimeMs)
            .thenComparing(BaselinePlan::getSource, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparingLong(BaselinePlan::getCreateTime)
            .thenComparingLong(BaselinePlan::getCreatorSqlMode)
            .thenComparing(BaselinePlan::getPlanSqlMode, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparing(BaselinePlan::getPlanFrozen, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparing(BaselinePlan::getSchemaFingerprint, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparing(BaselinePlan::getPlanSqlDigest, Comparator.nullsFirst(Comparator.naturalOrder()))
            .thenComparingLong(BaselinePlan::getId);

    /** Auto-increment id counter. */
    private final AtomicLong idGenerator = new AtomicLong(1);

    /**
     * Whether the persisted baselines have been loaded from the internal table (or the
     *  internal schema db is disabled). Set by loadFromInternalTable and
     *  the store's reset.
     */
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

    /** Whether CRUD writes to the internal table (a unit test keeps the store memory-only). */
    private volatile boolean persistToTable = true;

    /**
     * Guards the match-index publication: knownHashes / stateVersion and the loaded state
     * machine. Only accesses to those structures are critical sections - internal-table
     * I/O and the CPU work derived from a snapshot (digest filter, priority sort) run
     * OUTSIDE the lock. Rewrite lookups (hasBaselines + findCandidateBaselines on every
     * query) read the volatile hash index without any lock; create / drop / status / load /
     * refresh take the write lock for their in-memory publication only. Cold matching reads briefly take the
     * read lock to capture a consistent generation and hash epoch, then the write lock to validate publication.
     */
    private final ReentrantReadWriteLock stateLock = new ReentrantReadWriteLock();

    /**
     * Serializes local CREATE, DROP, ALTER and repair protocols without holding stateLock across internal-table
     * I/O. Matching does not acquire writerLock. stateLock protects in-memory publication; the row cache uses its
     * own monitor.
     */
    private final Object writerLock = new Object();

    /**
     * Monotonic version of the in-memory state, bumped by every local mutation (create /
     * drop / status change / load) and by every accepted refresh, including content-equal snapshots. The periodic
     * refresh compares it to skip a table snapshot whose read overlapped another publication
     * (see refreshFromInternalTable). Guarded by stateLock.
     */
    private long stateVersion = 0;

    /**
     * The largest id observed by an initial load or local row publication. A larger durable watermark requires
     * a fresh CREATE dedup read. A watermark at or below this value only permits using a complete cached hash
     * bucket; it does not prove that an evicted bucket's rows remain in memory.
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
     * Local expectations for durable mutations whose outcome is not yet visible to reads. Contradicting rows are
     * masked while the fence is active. Unconfirmed fences expire after a bound; confirmed fences remain until a
     * read satisfies or supersedes them. Invalidation retains these obligations.
     */
    private final Map<Long, PendingMutationFence> pendingMutationFences =
            new ConcurrentHashMap<>();

    /**
     * The bindSql hashes with a matching-path cache-miss load IN FLIGHT (see
     * completeRowsForHash): N concurrent cold queries of one evicted hash used to issue N
     * separate synchronous by-hash reads and stampede the internal table. Only the first
     * attempt performs the (budget-bounded) read; every concurrent attempt of that hash
     * degrades WITHOUT its own read, and the loader's atomic publication lets the next
     * statements hit the cache. The marker is per hash (a miss for another hash loads
     * independently) and is always released when the load ends, however it ended.
     */
    private final Set<Long> inflightHashLoads = ConcurrentHashMap.newKeySet();

    /**
     * One pending durable outcome of a local mutation (see pendingMutationFences).
     *
     * An ABSENCE fence may travel with the removed row's identity. The
     * DELETE that produced it is unconfirmed (it may have failed BEFORE commit), so NO
     * deletion marker may be written yet - a marker for a still-live row would make every
     * later load hide that row and re-issue the delete, silently completing a DROP that
     * reported failure. The identity is kept here instead: the marker is appended only
     * once a readable snapshot proves the row is gone. The fence then stays
     * confirmed until both tombstone scopes can be read back.
     */
    private static final class PendingMutationFence {
        private final BaselineStatus expectedStatus; // null = the row must be absent
        private final long sinceMillis;
        private final long attemptedUpdateTimeMicros; // 0 = unknown (delete / forwarded DDL)
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
                long attemptedUpdateTimeMicros) {
            this(expectedStatus, sinceMillis, attemptedUpdateTimeMicros, null, false);
        }

        PendingMutationFence(BaselineStatus expectedStatus, long sinceMillis,
                long attemptedUpdateTimeMicros, BaselinePlan droppedIdentity) {
            this(expectedStatus, sinceMillis, attemptedUpdateTimeMicros, droppedIdentity, false);
        }

        PendingMutationFence(BaselineStatus expectedStatus, long sinceMillis,
                long attemptedUpdateTimeMicros, BaselinePlan droppedIdentity,
                boolean confirmed) {
            this.expectedStatus = expectedStatus;
            this.sinceMillis = sinceMillis;
            this.attemptedUpdateTimeMicros = attemptedUpdateTimeMicros;
            this.droppedIdentity = droppedIdentity;
            this.confirmed = confirmed;
        }

        /**
         * Whether an UNCONFIRMED fence has aged past its bound: after
         * PENDING_MUTATION_FENCE_MILLIS the write is presumed lost and the persisted
         * state wins again (a CONFIRMED fence never expires - its committed row can stay
         * unreadable past every bound).
         */
        boolean isExpired(long nowMillis) {
            return !confirmed && nowMillis - sinceMillis > PENDING_MUTATION_FENCE_MILLIS;
        }

        /**
         * Whether the snapshot satisfies this fence: absence for an absence fence, the expected status, or an
         * opposite-status row whose update time is at least the known attempt time. With an unknown attempt
         * time, only the expected status satisfies a status fence.
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
            return attemptedUpdateTimeMicros > 0
                    && row.getUpdateTime() >= attemptedUpdateTimeMicros;
        }
    }

    /**
     * Advances when a snapshot is published, the store is invalidated, or forwarded DDL requires a fresh read.
     * A read that started before any of these events must not overwrite the newer state. Snapshot publication
     * advances it under stateLock; invalidation and forwarded-DDL fencing advance it under writerLock. Cold
     * hash reads capture it with their hash epoch under stateLock and validate both before caching. Content
     * signatures can collide, so even a snapshot with equal signatures advances this generation.
     */
    private final AtomicLong storeGeneration = new AtomicLong();

    /**
     * Level-1 match index: every bindSqlHash that currently has at least one baseline row.
     * The VALUE is the local mutation sequence of that hash (see publishHash): a hash load
     * that started before a local publish / drop of the same hash detects the race and
     * refuses to cache its stale row set (see completeRowsForHash). This map is the ENTIRE
     * per-baseline memory of the matching path - the rows themselves live in
     * baselineCache (a bounded LRU) or in the internal table.
     *
     * Copy-on-write under stateLock, read lock-free through the volatile field: plain
     * reads (every query) must never serialize on a writer, and a mutation is a rare DDL.
     */
    private volatile Map<Long, Long> knownHashes = new ConcurrentHashMap<>();

    /** Monotonic source of the per-hash mutation sequences (see knownHashes). */
    private final AtomicLong hashMutationSeq = new AtomicLong();

    /**
     * Content signatures of all snapshot hashes, including cold ones. A changed signature advances that hash's
     * epoch without retaining every durable row in memory. Equality does not prove equal content: exact cache
     * changes also advance the hash epoch, and storeGeneration fences overlapping reads on every publication.
     */
    private volatile Map<Long, Long> knownHashSignatures = new ConcurrentHashMap<>();

    /**
     * Bounded LRU cache of COMPLETE per-bindSql row sets for the matching path: one entry
     * holds EVERY baseline row of ONE bindSqlHash (all statuses / plan texts), so the
     * digest filter / priority ordering always see the full candidate set. A cache miss is
     * answered by the internal table (see completeRowsForHash); the capacity is
     * spm_baseline_cache_size (default 2000).
     *
     * When no durable table backs the store (FE unit tests / internal schema db disabled)
     * the cache IS the storage: entries are populated by publishBaseline and never
     * evicted, exactly like the former full map.
     */
    private final BaselineRowCache baselineCache = new BaselineRowCache();

    /** The durable layer every internal-table operation goes through (see the interface). */
    private final BaselinePersistence persistence;

    /** The leadership fence / journal synchronization of every durable write. */
    private final BaselineClusterGateway cluster;
    private final java.util.concurrent.atomic.AtomicLong nextMutationWindowSweepMillis =
            new java.util.concurrent.atomic.AtomicLong();

    /** The wall clock the mutation window's age is measured against. */
    private final java.util.function.LongSupplier mutationWindowClock;

    private BaselineManager() {
        this.persistence = new TablePersistence(this);
        this.cluster = new TableClusterGateway();
        this.mutationWindowClock = System::currentTimeMillis;
    }

    /**
     * Creates an ISOLATED manager whose durable layer and cluster collaboration are the
     * given implementations: the production singleton binds the internal-table
     * persistence and the live cluster gateway, while unit tests bind a substituted
     * implementation (a mock or a test-local fixture) to drive their scenario without
     * shared state - the same way the store is injected into any other collaborator.
     *
     * Parameter persistence: the durable layer of this manager
     * Parameter cluster: the leadership / journal collaboration
     * Parameter mutationWindowClock: the clock the mutation window's age is measured against
     */
    public BaselineManager(BaselinePersistence persistence, BaselineClusterGateway cluster,
            java.util.function.LongSupplier mutationWindowClock) {
        this.persistence = persistence;
        this.cluster = cluster;
        this.mutationWindowClock = mutationWindowClock;
    }

    /**
     * Whether a COMPLETE per-bindSql row set can be loaded from the internal table (the
     * matching-path cache miss). A scripted test store stands in for the whole protocol and
     * cannot answer a by-hash read, and a disabled internal schema db has no table at all -
     * in both cases the LRU cache IS the storage: publishes populate it, snapshots warm
     * every hash and nothing is ever evicted.
     */
    private boolean durableHashReadAvailable() {
        return persistence.hashReadsAvailable();
    }

    /**
     * One bindSqlHash -> its COMPLETE row set, as an access-ordered LRU. Every row in an
     * entry is DEEP (its transient parameterized trees are built): publishes carry them by
     * construction and the snapshot merge rebuilds them before a row enters an entry (see
     * deepenPersistedRow) - a row without trees must never serve a match / rewrite. All
     * methods are synchronized on the cache itself: the matching path touches only this
     * lock (never stateLock), and every caller that also holds stateLock acquires it BEFORE
     * this one (stateLock -> cache order, never the reverse).
     */
    private final class BaselineRowCache {
        private final LinkedHashMap<Long, LinkedHashMap<Long, BaselinePlan>> entries =
                new LinkedHashMap<>(16, 0.75f, true);

        /** rowId -> the hash of the entry holding it (reverse index for by-id lookups). */
        private final Map<Long, Long> rowIndex = new HashMap<>();

        /** The complete rows of one hash, or null on a miss. */
        synchronized List<BaselinePlan> getComplete(long hash) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            return rows == null ? null : new ArrayList<>(rows.values());
        }

        /** One row by id (the reverse index avoids walking the entries). */
        synchronized BaselinePlan getById(long id) {
            Long hash = rowIndex.get(id);
            if (hash == null) {
                return null;
            }
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            return rows == null ? null : rows.get(id);
        }

        /**
         * Reference copies of the cached rows of the given ids: lets a refresh decide
         * OUTSIDE this monitor which snapshot rows need their transient SQL trees rebuilt
         * (see deepenRefreshedRowsOutsideLocks). Nothing is mutated and no I/O happens under
         * the monitor.
         */
        synchronized Map<Long, BaselinePlan> cachedRowRefs(Collection<Long> ids) {
            Map<Long, BaselinePlan> found = new HashMap<>();
            for (Long id : ids) {
                Long hash = rowIndex.get(id);
                if (hash == null) {
                    continue;
                }
                LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
                BaselinePlan row = rows == null ? null : rows.get(id);
                if (row != null) {
                    found.put(id, row);
                }
            }
            return found;
        }

        /** Replaces one hash's entry with a COMPLETE row set. */
        synchronized void putComplete(long hash, List<BaselinePlan> rows) {
            removeEntry(hash);
            LinkedHashMap<Long, BaselinePlan> map = new LinkedHashMap<>();
            for (BaselinePlan row : rows) {
                map.put(row.getId(), row);
                rowIndex.put(row.getId(), hash);
            }
            entries.put(hash, map);
            evictIfNeeded();
        }

        /** Inserts / replaces ONE row of an EXISTING complete entry (no-op on a miss). */
        synchronized void insertIfPresent(long hash, BaselinePlan row) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            if (rows == null) {
                return;
            }
            Long oldHash = rowIndex.get(row.getId());
            if (oldHash != null && oldHash != hash) {
                removeRowLocked(oldHash, row.getId());
            }
            rows = entries.get(hash);
            if (rows == null) {
                return; // the removal above emptied the entry (single-row set)
            }
            rows.put(row.getId(), row);
            rowIndex.put(row.getId(), hash);
        }

        /**
         * Inserts / replaces ONE row, creating the entry when absent: the in-memory store
         * keeps every published row here (see the class javadoc), so a one-row entry IS
         * the complete set there.
         */
        synchronized void insertOrCreate(long hash, BaselinePlan row) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            if (rows == null) {
                rows = new LinkedHashMap<>();
                entries.put(hash, rows);
            }
            rows.put(row.getId(), row);
            rowIndex.put(row.getId(), hash);
        }

        /** Removes one row; returns the hash it lived under, or null when not cached. */
        synchronized Long removeRow(long id) {
            Long hash = rowIndex.remove(id);
            if (hash == null) {
                return null;
            }
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            if (rows != null) {
                rows.remove(id);
                if (rows.isEmpty()) {
                    entries.remove(hash);
                }
            }
            return hash;
        }

        private void removeRowLocked(long hash, long id) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            if (rows != null) {
                rows.remove(id);
                if (rows.isEmpty()) {
                    entries.remove(hash);
                }
            }
            rowIndex.remove(id);
        }

        private void removeEntry(long hash) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.remove(hash);
            if (rows != null) {
                for (Long id : rows.keySet()) {
                    rowIndex.remove(id);
                }
            }
        }

        synchronized boolean hasHash(long hash) {
            return entries.containsKey(hash);
        }

        synchronized List<BaselinePlan> allRows() {
            List<BaselinePlan> all = new ArrayList<>();
            for (LinkedHashMap<Long, BaselinePlan> rows : entries.values()) {
                all.addAll(rows.values());
            }
            return all;
        }

        synchronized void clear() {
            entries.clear();
            rowIndex.clear();
        }

        /**
         * Applies one snapshot (load / refresh): entries whose hash vanished are evicted,
         * entries whose hash survives are replaced by the snapshot's COMPLETE row set
         * (unchanged rows keep their objects, their transient trees and their timestamps
         * are adopted - see mergeEntry). When warmAll (the in-memory store), every
         * snapshot hash gets an entry, mirroring the former full map.
         *
         * The DURABLE content signature of every snapshot hash is compared here as well, so the
         * caller receives ONE changed-hash set covering hashes with no cached bucket too (see
         * ApplyResult). The caller MUST hold stateLock.writeLock: the signature table is
         * read and replaced in this critical section, and it has to stay consistent with the
         * knownHashes the same caller publishes.
         *
         * Parameter byHash: the snapshot's rows per hash
         * Parameter warmAll: whether hashes without an entry are warmed (the in-memory store)
         * Returns: the changed hashes, the cache-internal changes and the cached row count
         */
        synchronized ApplyResult applySnapshot(Map<Long, List<BaselinePlan>> byHash,
                boolean warmAll) {
            int cacheRowsChanged = 0;
            Set<Long> cacheChangedHashes = new HashSet<>();
            // collect the vanished hashes FIRST: merging below may add / remove inner rows,
            // and mutating while an entries-iterator is live is not allowed
            List<Long> vanished = null;
            for (Map.Entry<Long, LinkedHashMap<Long, BaselinePlan>> entry : entries.entrySet()) {
                if (!byHash.containsKey(entry.getKey())) {
                    if (vanished == null) {
                        vanished = new ArrayList<>();
                    }
                    vanished.add(entry.getKey());
                }
            }
            if (vanished != null) {
                for (Long hash : vanished) {
                    removeEntry(hash);
                    cacheRowsChanged++;
                    cacheChangedHashes.add(hash);
                }
            }
            for (Map.Entry<Long, List<BaselinePlan>> e : byHash.entrySet()) {
                if (entries.containsKey(e.getKey())) {
                    int merged = mergeEntry(e.getKey(), e.getValue());
                    if (merged > 0) {
                        cacheRowsChanged += merged;
                        cacheChangedHashes.add(e.getKey());
                    }
                } else if (warmAll) {
                    putCompleteLocked(e.getKey(), e.getValue());
                    cacheRowsChanged += e.getValue().size();
                    cacheChangedHashes.add(e.getKey());
                }
            }
            evictIfNeeded();

            // Include cold hashes in change accounting without retaining their rows. Signatures are only
            // change detectors; storeGeneration protects publication even when their values collide.
            Set<Long> changedHashes = new HashSet<>();
            Map<Long, Long> nextSignatures = new HashMap<>();
            for (Map.Entry<Long, List<BaselinePlan>> entry : byHash.entrySet()) {
                long signature = hashContentSignature(entry.getValue());
                nextSignatures.put(entry.getKey(), signature);
                Long previous = knownHashSignatures.get(entry.getKey());
                if (previous == null || previous != signature) {
                    changedHashes.add(entry.getKey());
                }
            }
            knownHashSignatures = nextSignatures;

            changedHashes.addAll(cacheChangedHashes);
            return new ApplyResult(changedHashes, cacheRowsChanged);
        }

        private void putCompleteLocked(long hash, List<BaselinePlan> rows) {
            LinkedHashMap<Long, BaselinePlan> map = new LinkedHashMap<>();
            for (BaselinePlan row : rows) {
                // an entry of the in-memory store serves matches directly, so every row kept
                // here must be DEEP (see deepenPersistedRow); a row whose trees cannot be
                // rebuilt is skipped exactly like the former eager snapshot parse did
                BaselinePlan deepened = deepenPersistedRow(row);
                if (deepened == null) {
                    continue;
                }
                map.put(deepened.getId(), deepened);
                rowIndex.put(deepened.getId(), hash);
            }
            entries.put(hash, map);
        }

        private int mergeEntry(long hash, List<BaselinePlan> freshRows) {
            LinkedHashMap<Long, BaselinePlan> rows = entries.get(hash);
            int changed = 0;
            Set<Long> seen = new HashSet<>();
            for (BaselinePlan fresh : freshRows) {
                BaselinePlan current = rows.get(fresh.getId());
                if (current != null && !persistedContentChanged(current, fresh)) {
                    // unchanged: the cached object - with its transient trees - stays, and
                    // only the persisted timestamps are adopted
                    seen.add(fresh.getId());
                    if (copyPersistedTimestamps(current, fresh)) {
                        changed++;
                    }
                    continue;
                }
                if (current != null && !persistedTreesChanged(current, fresh)) {
                    // STATUS / timing-only change: the cached object's SQL trees are still
                    // valid, so adopt the persisted scalars IN PLACE instead of reparsing
                    // the row under the cache monitor (see
                    // deepenRefreshedRowsOutsideLocks) - a follower refreshing many
                    // disabled baselines used to rebuild their SQL serially here, holding
                    // the very monitor every warm query needs (getComplete).
                    seen.add(fresh.getId());
                    if (adoptPersistedScalars(current, fresh)) {
                        changed++;
                    }
                    continue;
                }
                // A row that is ADDED or CHANGED takes the place of its cached twin, so it
                // must be DEEP before it may serve a match: the snapshot decode is shallow
                // (see shallowPersistedRow) and the trees are rebuilt HERE, the moment the
                // row enters the cache (see deepenPersistedRow). An unusable row stays out
                // of seen on purpose: it must not become a candidate, and a cached twin
                // it replaces leaves the entry - exactly like the former eager snapshot
                // parse, which never let such a row reach the cache at all.
                BaselinePlan deepened = deepenPersistedRow(fresh);
                if (deepened == null) {
                    continue;
                }
                rows.put(deepened.getId(), deepened);
                rowIndex.put(deepened.getId(), hash);
                seen.add(deepened.getId());
                changed++;
            }
            for (Iterator<Map.Entry<Long, BaselinePlan>> it = rows.entrySet().iterator();
                    it.hasNext();) {
                Map.Entry<Long, BaselinePlan> row = it.next();
                if (!seen.contains(row.getKey())) {
                    rowIndex.remove(row.getKey());
                    it.remove();
                    changed++;
                }
            }
            if (rows.isEmpty()) {
                entries.remove(hash);
            }
            return changed;
        }

        private void evictIfNeeded() {
            if (!durableHashReadAvailable()) {
                return; // the cache IS the storage: eviction would lose rows
            }
            int capacity = baselineCacheCapacity();
            while (entries.size() > capacity) {
                Iterator<Map.Entry<Long, LinkedHashMap<Long, BaselinePlan>>> it =
                        entries.entrySet().iterator();
                Map.Entry<Long, LinkedHashMap<Long, BaselinePlan>> eldest = it.next();
                for (Long id : eldest.getValue().keySet()) {
                    rowIndex.remove(id);
                }
                it.remove();
            }
        }
    }

    /**
     * Registers one hash in the match index with a fresh mutation sequence (copy-on-write;
     * see knownHashes). The caller holds stateLock (or is the single-threaded test path).
     */
    private void publishHash(long hash) {
        Map<Long, Long> next = new HashMap<>(knownHashes);
        next.put(hash, hashMutationSeq.incrementAndGet());
        knownHashes = next;
    }

    /**
     * Drops one hash from the match index (copy-on-write; see knownHashes). The caller
     * holds stateLock (see completeRowsForHash's atomic removal): the plain
     * read-copy-write used to LOSE a concurrent publisher's unrelated hash - a removal
     * copying {A} while a CREATE published {A, B} assigned its old copy minus A, so B
     * became invisible to hasBaselines / candidate lookup until the next refresh.
     */
    private void dropHash(long hash) {
        if (!knownHashes.containsKey(hash)) {
            return;
        }
        Map<Long, Long> next = new HashMap<>(knownHashes);
        next.remove(hash);
        knownHashes = next;
    }

    /**
     * Advances one hash's mutation epoch in place (copy-on-write; the caller holds
     * stateLock). A CONTENT change under an existing hash does not change the knownHashes
     * key set, so a matching-path cache-miss read that raced it used to pass the equality
     * check and publish its pre-change rows over the completed state - a slow miss that
     * had read ENABLED while a DISABLE published DISABLED replaced the completed flip.
     *
     * Parameter hash: the hash whose rows changed (absent = no-op: a removed hash is already
     *             detected as a change by the missing-key comparison)
     */
    private void bumpHashEpochLocked(long hash) {
        if (hash == 0 || !knownHashes.containsKey(hash)) {
            return;
        }
        Map<Long, Long> next = new HashMap<>(knownHashes);
        next.put(hash, hashMutationSeq.incrementAndGet());
        knownHashes = next;
    }

    /**
     * Computes a rolling signature of all persisted scalar fields in the supplied row order. Refresh compares it
     * to detect changes in cold as well as cached hash buckets. It is a change detector rather than a
     * collision-free encoding.
     */
    private static long hashContentSignature(List<BaselinePlan> rows) {
        long signature = 1L;
        for (BaselinePlan row : rows) {
            long rowSignature = java.util.Objects.hash(row.getId(), row.getStatus(),
                    row.getUpdateTime(), row.getCreateTime(), row.getBindSql(),
                    row.getBindSqlDigest(), row.getBindSqlHash(), row.getPlanSql(),
                    row.getPlanSqlDigest(), row.getPlanSqlMode(), row.getPlanFrozen(),
                    row.getSchemaFingerprint(), row.getCreatorSqlMode(), row.getSource(),
                    row.getQueryId(), row.getCost(), row.getQueryTimeMs());
            signature = signature * 31L + rowSignature;
        }
        return signature;
    }

    /**
     * Snapshot change accounting: the union of signature changes and exact cache changes determines which
     * hash epochs advance. Every accepted snapshot independently advances stateVersion and storeGeneration.
     */
    static final class ApplyResult {
        /** Hashes with a changed signature or cache entry, including vanished cached hashes. */
        final Set<Long> changedHashes;
        /** Rows added / changed / removed inside cached entries (logging only). */
        final int cacheRowsChanged;

        ApplyResult(Set<Long> changedHashes, int cacheRowsChanged) {
            this.changedHashes = changedHashes;
            this.cacheRowsChanged = cacheRowsChanged;
        }
    }

    /** The LRU capacity (spm_baseline_cache_size, test-scriptable via the seam). */
    private static int baselineCacheCapacity() {
        return Math.max(1, VariableMgr.getDefaultSessionVariable().getSpmBaselineCacheSize());
    }

    public static BaselineManager getInstance() {
        return INSTANCE;
    }

    /**
     * Compares candidate execution-time and cost priority. Equal priority can compare equal for different baseline
     * ids.
     */
    public static int compareCandidates(BaselinePlan o1, BaselinePlan o2) {
        return comparator.compare(o1, o2);
    }

    // CRUD.

    /**
     * Creates or adopts a GLOBAL baseline. Duplicate checks include the bind key, stored plan SQL and schema
     * fingerprint. The method resolves prior writes, allocates and confirms a durable id reservation, writes the
     * row and publishes it when confirmed. Failures can retain pending-write obligations. Returns the created or
     * existing id, assigning it to a newly inserted plan.
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
            // Id watermark first (see reserveAllocatedId): the generator must be
            // advanced past the persistence layer BEFORE an id is handed out. A create whose
            // watermark read fails fails visibly and allocates nothing, instead of silently
            // colliding with a row written by a newer master.
            final long tableWatermark = readPersistedTableMaxId();
            // The allocation bound combines the table maximum and the reservation watermark so dropped or
            // reserved-but-unwritten ids remain consumed. The durable key lookup below also uses this bound when
            // deciding whether a complete cached bucket may answer dedup.
            final long watermark = Math.max(tableWatermark, readPersistedIdHighWater());
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
            // Check cached duplicates under the read lock. A row with a different schema fingerprint is stale
            // and must not be returned as the duplicate; later steps retire its cache entry and attempt durable
            // cleanup before creating a replacement.
            BaselinePlan duplicate = null;
            List<BaselinePlan> staleCacheRows = new ArrayList<>();
            // ids the durable state itself just proved absent (a tombstoned identity or a
            // row the probe no longer sees): the durable-key answer must not serve them
            // back from the local index
            java.util.Set<Long> durableGoneIds = new java.util.HashSet<>();
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
                    durableGoneIds.add(duplicate.getId());
                } else if (!persistence.isDurable()) {
                    return duplicate.getId();
                } else {
                    // Existence is independent of status: an ALTER on another FE may have disabled this
                    // identity since it was cached. UNKNOWN cannot justify allocating a replacement.
                    BaselinePersistence.DurablePresence presence = probeDurableRow(duplicate.getId(),
                            duplicate.getBindSqlDigest(), duplicate.getPlanSql(), null,
                            duplicate.getSchemaFingerprint(), null);
                    if (presence == BaselinePersistence.DurablePresence.UNKNOWN) {
                        throw new IllegalStateException("SPM cannot create baseline: the cached duplicate "
                                + duplicate.getId() + " has UNKNOWN durable presence; retry the statement");
                    }
                    if (presence == BaselinePersistence.DurablePresence.PRESENT) {
                        BaselinePlan readable = readableIdentityRow(duplicate.getId(), plan);
                        if (readable == null) {
                            throw new IllegalStateException("SPM cannot create baseline: the durable duplicate "
                                    + duplicate.getId() + " cannot be read consistently; retry the statement");
                        }
                        publishBaseline(readable);
                        return readable.getId();
                    }
                    LOG.warn("SPM baseline create: the cached duplicate {} is gone durably"
                            + " (promotion window); creating a fresh row", duplicate.getId());
                    staleCacheRows.add(duplicate);
                    durableGoneIds.add(duplicate.getId());
                }
            }
            // Check the durable key as well because a local cache can lag a leadership change or refresh. Adopt
            // an existing matching incarnation rather than insert another row. Repair extra same-key rows while
            // writerLock serializes local writers.
            if (persistence.isDurable() && plan.getBindSqlDigest() != null) {
                // The KEY read is a point read too - a row revived after its
                // own DROP must not be adopted (and must be repaired away) here either.
                // The gate uses the RESERVATION watermark (>= MAX(id) of the table): it is
                // the bound the id allocator just proved, and unlike the raw MAX(id) read it
                // is also answered by a scripted store - the point read's index is populated
                // by ordinary refreshes and would silently miss those rows.
                List<BaselinePlan> durable = filterTombstonedDurableRows(
                        readPersistedRowsForCreate(plan, watermark));
                if (!durableGoneIds.isEmpty()) {
                    durable.removeIf(row -> durableGoneIds.contains(row.getId()));
                }
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
            // Rows created through this manager use GLOBAL scope; the allocator enforces its separate id range.
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
                long now = SPMUtils.nowMicros();
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
                } catch (AmbiguousInsertException | UnconfirmedVisibleException unconfirmed) {
                    // Both forms leave the SAME obligation: the write may be (or IS) durable
                    // without being readable, so the attempted identity is remembered and
                    // the retry defers / adopts instead of allocating a second id.
                    rememberPendingCreate(plan);
                    // persist the SAME identity durably: a retry on ANOTHER
                    // FE has no in-memory registry to consult
                    markSeqPendingUnconfirmed(plan);
                    throw unconfirmed;
                }
                // The INSERT can COMMIT after the check above: an internal write of a
                // demoted FE FORWARDS to the new master and lands there, so a
                // successful statement is not proof that THIS FE still owns the write
                // epoch. The promoted master that read MAX(id) concurrently gave its
                // own create for the SAME key id N+1, and both rows are durable now -
                // publishing / returning the id here would place an UNOWNED row into
                // this cache on top of (the same-key twins are hidden by
                // collapseDuplicateKeyRows and repaired on a master-side publish, and
                // this check keeps this side from publishing its half; the retry then
                // adopts the surviving row).
                assertLeaderForWrite();
                if (!persistence.isDurable()) {
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
     * Parameter id: the just-inserted id
     * Parameter own: the row this create inserted
     * Returns: the competing row, or null
     */
    private BaselinePlan readForeignRowWithSameId(long id, BaselinePlan own) {
        try {
            return pickForeignRow(persistence.readRowsById(id, false), own);
        } catch (RuntimeException e) {
            throw new RuntimeException(
                    "SPM baseline id collision probe failed (retry the CREATE): "
                            + e.getMessage(), e);
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

    /**
     * Whether two rows describe the same baseline INCARNATION: the durable dedup /
     * collision / adoption key is (bindSqlDigest, planSql) AND the schema fingerprint.
     * The fingerprint is part of the identity - after ALTER TABLE t ADD COLUMN x the
     * SAME SQL under the new schema is a DIFFERENT baseline, and the old row is
     * unreachable for matching (schemaFingerprintBindSideContained / verifyReplayMetadata
     * reject it). Without it every identity comparison that hangs off this method treated
     * the two incarnations as one:
     *   the id-collision probe skipped the OTHER incarnation's row as "own", so two
     *     enabled rows could survive under one id;
     *   the pending-create registry matched a retry under the NEW fingerprint against
     *     the abandoned write of the OLD one and deferred it (or adopted the other
     *     incarnation's row);
     *   the status-update identity check completed an ALTER against a row the durable
     *     table no longer favors.
     *
     * Parameter a: one row
     * Parameter b: the other row
     * Returns: true when both rows are the same incarnation of the same baseline
     */
    private static boolean sameIdentity(BaselinePlan a, BaselinePlan b) {
        return sameKey(a, b) && Objects.equals(normalizedFingerprint(a),
                normalizedFingerprint(b));
    }

    /** Whether two rows carry the same durable KEY (bindSqlDigest + planSql). */
    private static boolean sameKey(BaselinePlan a, BaselinePlan b) {
        return Objects.equals(a.getBindSqlDigest(), b.getBindSqlDigest())
                && Objects.equals(a.getPlanSql(), b.getPlanSql());
    }

    /** One row's schema fingerprint with NULL and the empty string unified (IFNULL in SQL). */
    private static String normalizedFingerprint(BaselinePlan plan) {
        return plan.getSchemaFingerprint() == null ? "" : plan.getSchemaFingerprint();
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
     * Parameter plan: the CREATE's baseline
     * Returns: the resolution (see PendingResolution)
     */
    private PendingResolution resolvePendingCreate(BaselinePlan plan) {
        if (pendingCreates.isEmpty()) {
            return PendingResolution.NONE;
        }
        boolean handled = false;
        Iterator<PendingCreate> iterator = pendingCreates.iterator();
        while (iterator.hasNext()) {
            PendingCreate pending = iterator.next();
            if (!sameKey(pending.plan, plan)) {
                continue;
            }
            if (!sameIdentity(pending.plan, plan)) {
                // The SCHEMA CHANGED while that write awaited publication (e.g.
                // ALTER TABLE t ADD COLUMN x turned the fingerprint F1 into F2, and the
                // promoted master created the same SQL under F2): the pending row is a
                // DIFFERENT incarnation - it can never satisfy this create (matching /
                // replay reject it, and its id may already carry the new incarnation's
                // SUCCESSFUL row) - so its fence must not defer THIS create, and this
                // create must not adopt the other incarnation's row. Retire the stale
                // incarnation ONCE ITS OWN ROW IS READABLE (its own identity, fingerprint
                // included: the delete can never touch the new incarnation's row, and
                // neither can the marker retirement - both are keyed by the folded
                // per-incarnation hash) and fall through: the normal create path either
                // dedups against the new incarnation's durable row or allocates a fresh
                // one. handled stays FALSE: the durable fence belongs to THIS
                // incarnation and must still run.
                iterator.remove();
                retireStaleIncarnation(pending.plan, plan);
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
                // The tombstone must be DURABLE and READABLE before this entry is retired
                // and a fresh id can be allocated: noteDroppedSeqState is best effort on the
                // wire, and a retry that only lost the process-local marker would insert a
                // second id while the first COMMITTED row is still unreadable - after a
                // restart BOTH rows could publish ENABLED. A confirmation failure throws
                // retryably and KEEPS the entry pending.
                condemnPendingIdentity(pending.plan);
                iterator.remove();
                LOG.warn("SPM pending create of baseline {}: still not readable after {} ms;"
                                + " condemning the identity (any late publication is repaired"
                                + " away) and allocating a fresh id",
                        pending.plan.getId(), PENDING_CREATE_FENCE_MILLIS);
                continue;
            }
            iterator.remove();
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
     * Retires the row and the durable fence of a pending create whose SCHEMA FINGERPRINT
     * no longer matches the incoming CREATE (see resolvePendingCreate): the abandoned
     * write is a DIFFERENT incarnation, unreachable for matching / replay, and its
     * identity-scoped records - the row delete and the seq marker retirement, both keyed
     * by the per-incarnation plan-SQL hash - can never touch the current incarnation's
     * row or marker.
     *
     * Both writes are best effort: the row is unreachable for matching either way (the
     * durable-key check of the create retires it as soon as a read sees it), and a
     * marker retirement that fails only means the OLD incarnation's fence survives, which
     * no longer matches any create under the current schema.
     *
     * Parameter staleIncarnation: the abandoned write's plan (its OWN fingerprint scopes both
     *                         writes)
     * Parameter plan: the CREATE being resolved
     */
    private void retireStaleIncarnation(BaselinePlan staleIncarnation, BaselinePlan plan) {
        boolean readable;
        try {
            readable = durableRowReadable(staleIncarnation);
        } catch (RuntimeException e) {
            // An unconfirmable probe must NOT delete: this branch only decides whether the
            // stale row is worth retiring, and the create path retires it later.
            LOG.warn("SPM pending create of baseline {}: its stale incarnation's readability"
                            + " could not be confirmed ({}); leaving it to the create path",
                    staleIncarnation.getId(), e.getMessage());
            return;
        }
        if (!readable) {
            // The abandoned row is still invisible: nothing to delete yet, and the
            // creation this resolution belongs to must proceed without waiting for it.
            return;
        }
        try {
            persistDeleteByIdentity(staleIncarnation);
        } catch (RuntimeException e) {
            // best effort: the durable-key check of the create below retires the
            // row as soon as a read sees it
            LOG.warn("SPM failed to retire the stale pending-create row (id={}): {}",
                    staleIncarnation.getId(), e.getMessage());
        }
        // the marker described THAT write; with the row retired it must not fence a later
        // retry under ITS OWN fingerprint either
        retireSeqPendingMarker(staleIncarnation, staleIncarnation.getId());
        LOG.warn("SPM pending create of baseline {}: its schema fingerprint changed"
                        + " ({} -> {}); replacing the stale committed row",
                staleIncarnation.getId(), staleIncarnation.getSchemaFingerprint(),
                plan.getSchemaFingerprint());
    }

    /**
     * Adopts the READABLE durable row of one pending create: the row carrying the pending
     * id with THIS baseline's identity is published and its id returned; null when the id
     * no longer carries the identity (the caller falls through / allocates a fresh id).
     * Shared by the in-memory registry (resolvePendingCreate) and the durable
     * reservation fence (resolveDurablePendingCreate).
     *
     * Parameter pendingId: the id the pending write consumed
     * Parameter plan: the CREATE's baseline
     * Returns: the adopted id, or null when the id carries no matching row
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
    private BaselinePlan readableIdentityRow(long pendingId, BaselinePlan plan) {
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
                // Condemn before a fresh id can be allocated - a later publication of this
                // identity must not surface as a second enabled row. The tombstone must be
                // CONFIRMED readable before the record stops fencing (the capacity-cleanup
                // path has the same gate as resolvePendingCreate): the append is best
                // effort, and a marker that never became readable would let the next retry
                // of this key allocate a second id beside the still-unreadable COMMITTED
                // row (both ENABLED after a late publication).
                condemnPendingIdentity(pending.plan);
                iterator.remove();
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
     * Parameter plan: the row that was written
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
     * Parameter p: the pending row
     * Returns: true when a read sees the row
     */
    private boolean durableRowReadable(BaselinePlan p) {
        if (!persistence.isDurable()) {
            return true;
        }
        // The SCHEMA FINGERPRINT is part of the incarnation: after an ALTER TABLE the
        // SAME SQL under the new schema is a DIFFERENT baseline, and without the
        // constraint this probe treated another incarnation's readable row as THIS
        // write's proof - the stale-fingerprint branch below then deleted that (valid)
        // row by (id, digest, planSql) and a retry of this FE's abandoned CREATE removed
        // a successful baseline. The status stays unconstrained (the row carries the
        // status the create wrote; an unconfirmable answer must defer).
        return persistence.isRowReadable(p);
    }

    /**
     * Deterministic tie-break of an id collision: every observer sees the same two
     * identities, so ordering by (digest, planSql) makes both masters yield to the SAME
     * winner (the lexicographically smaller identity) - exactly one of the two rows
     * survives, no matter which create runs the probe.
     *
     * A pair whose KEY (digest + planSql) is equal but whose SCHEMA FINGERPRINT differs is
     * NOT a tie either side may win: the two rows are two incarnations of one baseline
     * (an ALTER TABLE landed between their creates), the equal keys make this comparison
     * report "own loses" for BOTH observers, and each side then takes back only ITS OWN
     * row (see the caller's loser branch) while re-allocating a fresh id. Neither create
     * can therefore delete the other incarnation's - possibly already
     * REPORTED-successful - row, which a fingerprint tie-break would have allowed.
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
     * a handoff moved the master elsewhere (the capture daemon checks isMaster only
     * once per cycle), and the new master allocates ids from the same MAX(id) with its
     * own process-local generator. Refusing the write from a non-master keeps the
     * allocator single-writer at the moment of the write; the collision probe above
     * covers the unavoidable tail where the demotion lands mid-write.
     */
    private void assertLeaderForWrite() {
        if (!persistence.isDurable()) {
            return;
        }
        if (!cluster.isLeader()) {
            throw new IllegalStateException(NO_LONGER_MASTER);
        }
    }

    /**
     * Phase 2 of a writer: publishes a baseline into the match index / cache. No I/O - the
     * hash index copy-on-write and the cache insert happen under their own locks. The cache
     * is updated only when the hash already has a COMPLETE entry (a single-row entry for a
     * hash whose other rows are not loaded would violate the completeness rule), except in
     * the in-memory store where the cache IS the storage and every published row belongs in
     * it.
     */
    private void publishBaseline(BaselinePlan plan) {
        stateLock.writeLock().lock();
        try {
            maxPersistedIdSeen = Math.max(maxPersistedIdSeen, plan.getId());
            publishHash(plan.getBindSqlHash());
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
        if (durableHashReadAvailable()) {
            baselineCache.insertIfPresent(plan.getBindSqlHash(), plan);
        } else {
            baselineCache.insertOrCreate(plan.getBindSqlHash(), plan);
        }
    }

    /**
     * Drops a baseline.
     *
     * Parameter id: the baseline id
     * Returns: whether the drop succeeded
     */
    public boolean dropBaseline(long id) {
        ensureLoadedOrThrow();
        // Two-phase drop: the DELETE I/O runs under writerLock but NOT under stateLock;
        // the state lock only removes the in-memory entry afterwards.
        synchronized (writerLock) {
            BaselinePlan removed = baselineCache.getById(id);
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
            // Delete the cached identity durably before the normal cache-removal path. An ambiguous failure
            // below evicts the row and retains an absence fence; it does not prove the durable row was kept.
            assertLeaderForWrite();
            // Retain the KEY CLEANUP BEFORE the identity delete can land.
            // Retaining afterwards meant an unreadable id watermark failed the DROP only AFTER
            // the row was already gone: nothing was retained, so an IF EXISTS retry found the
            // row ABSENT and reported success while the newer same-key twins of that id were
            // still durable and could match cold (resumePendingDropKeyCleanup had no identity to
            // resume from). The watermark read is also the only failing step of the retention, so
            // it now fails with the row STILL PRESENT - the retry repeats the whole DROP.
            retainDropKeyCleanup(removed);
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
                    removeCachedBaseline(removed);
                    recordPendingAbsenceFence(removed);
                    // a cold read that started between the removal and the fence can have
                    // re-cached the row it still found durable: it must leave again NOW that the
                    // fence makes the identity unmatchable (see
                    // evictReWarmedAfterAbsenceFence)
                    evictReWarmedAfterAbsenceFence(List.of(removed));
                }
                throw e;
            }
            // The IDENTITY delete is CONFIRMED from here on (persistDeleteByIdentity only
            // returns once the row is gone), so the obligations below are settled in an order
            // that keeps every one of them recoverable: the KEY CLEANUP was already retained
            // above, the cache entry leaves first, then the two tombstone scopes and the
            // CONFIRMED absence fence, and only then are the lingering rows of this id wiped.
            removeCachedBaseline(removed);
            // Two tombstone SCOPES are written on purpose, because the DROP has to defeat two
            // DIFFERENT revivals: each path writes the scope it means, and every
            // reader asks for both):
            //
            // the IDENTITY scope (id|digest|planSqlHash) defeats a DELAYED status INSERT of a
            //   demoted master that commits AFTER this delete and carries the SAME identity -
            //   including the handoff case where it lands under an OLDER id (withIdentityDrops
            //   resolves the identity's newest state, which this marker is).
            // the ID scope (id||0) defeats a DIFFERENT incarnation of the same id (a handoff
            //   collision wrote another digest under this id and it was unreadable when the
            //   DROP ran): its distinct digest matches no identity marker, and the id is
            //   condemned because the user dropped it.
            noteDroppedSeqState(removed);
            noteDroppedIdState(id);
            oweDropTombstone(id, removed);
            oweDropIdTombstone(id);
            // Even a CONFIRMED identity delete can stay unreadable to a later local read
            // (its publication lags the confirmation): the fence keeps the refresh / a
            // SHOW reload from re-adding the dropped row until the table shows it gone.
            //
            // The fence is CONFIRMED, exactly like an ALTER whose conditional
            // UPDATE reported its row: the DELETE committed, so absence is IRREVERSIBLE and
            // only its publication lags - an EXPIRING fence re-published the pre-DROP row as
            // soon as the bound elapsed.
            recordConfirmedAbsenceFence(id);
            // The first eviction precedes this fence. A cold read can rewarm the row during that interval, so
            // evict it again after publishing the fence.
            evictReWarmedAfterAbsenceFence(List.of(removed));
            List<BaselinePlan> retiredTwins = new ArrayList<>();
            try {
                retiredTwins = wipeDurableRowsById(id, removed, retiredTwins);
            } catch (RuntimeException e) {
                // The identity delete (and its tombstones / fence) is already settled above:
                // the lingering incarnation could not be READ, so no more can be done for it -
                // the failure propagates (retryable) and the retry takes the cache-miss path
                // which deletes the lingering rows. The twins retired BEFORE the failing
                // statement are already GONE durably: a warm bucket that still held one of
                // them would keep replaying a row the DROP removed (completeRowsForHash
                // serves a warm bucket before any tombstone reconciliation), so they leave the
                // cache here with their publication epoch.
                evictRetiredRows(retiredTwins);
                throw e;
            }
            // The DROP also retired rows DURABLY (same-key twins, lingering incarnations):
            // a warm bucket that still held one of them would keep replaying it until the
            // next refresh (see evictRetiredRows).
            evictRetiredRows(retiredTwins);
            // The tombstone appends above only WARN on the wire: report the DROP only
            // once they are readable, or a delayed write of the removed row - or the
            // unreadable loser of a handoff collision - revives the dropped baseline on
            // every FE whose filter lived in process memory (see
            // confirmDropTombstonesDurable). The SAME-KEY TWINS retired with this DROP are
            // confirmed in the same obligation: their DELETE is done, but their own
            // resurrection fence was only best effort.
            //
            // The KEY CLEANUP retention is released BEFORE this confirmation:
            // the retirement above already FINISHED, and a retention that outlived a FAILED
            // confirmation made the absent-row retry resume a cleanup whose key now names a
            // baseline the user re-CREATEd - deleting the new row. Only a retirement that
            // actually threw keeps the retention (see the catch block above).
            pendingDropKeyCleanups.remove(id);
            List<BaselinePlan> droppedRows = new ArrayList<>(retiredTwins.size() + 1);
            droppedRows.add(removed);
            droppedRows.addAll(retiredTwins);
            confirmDropTombstonesDurable(droppedRows, id);
            return true;
        }
    }

    /**
     * Removes one baseline from the cached row set and ADVANCES its hash's mutation
     * epoch (under stateLock). Shared by dropBaseline's happy path and its lingering-row
     * cleanup failure path: once the identity delete is confirmed, the cached row must
     * stop being matchable / replayable no matter what the cleanup of a DIFFERENT
     * incarnation reported. In the in-memory store a hash whose last row left also
     * leaves the hash index.
     *
     * The epoch advance runs for the UNCACHED case too (the row was evicted / already
     * gone): a by-hash miss that read this row BEFORE the DROP pauses between its
     * reconciliation and the publication lock, and the unchanged epoch let it publish
     * and cache the dropped ENABLED row - every later cache hit then served a baseline
     * the completed DROP had removed (until the next refresh). The bump is the same
     * barrier the status flip / refresh updates use.
     */
    private void removeCachedBaseline(BaselinePlan plan) {
        long id = plan.getId();
        // The removal and the epoch advance MUST be one critical section: taking the row out
        // first left a window in which a by-hash miss that read this row BEFORE the DROP
        // could publish (and CACHE) it, because the epoch it compares against was still the
        // pre-DROP one - the stale row became a warm hit the completed DROP had removed.
        // Under the lock, any publication that observed the old epoch either ran before the
        // removal (and is followed by it) or is rejected by the bumped epoch.
        stateLock.writeLock().lock();
        try {
            Long cachedHash = baselineCache.removeRow(id);
            long hash = cachedHash != null ? cachedHash : plan.getBindSqlHash();
            if (hash != 0) {
                bumpHashEpochLocked(hash);
            }
            if (hash != 0 && !durableHashReadAvailable() && !baselineCache.hasHash(hash)) {
                dropHash(hash);
            }
            stateVersion++;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Evicts the DROP's own rows ONE MORE TIME, immediately AFTER its ABSENCE FENCE became
     * visible.
     *
     * The DROP evicts the row before the durable delete / fence and serves the guarantee
     * "an evicted bucket cannot come back": a by-hash COLD READ that started in the interval
     * between that eviction and the fence read the row while it was still durable, and it passes
     * its own pre-fence epoch check - so it could re-warm the bucket with the OLD ENABLED row
     * after the removal. Installing the fence never looked at the cache again, and
     * completeRowsForHash serves a WARM bucket BEFORE any tombstone / fence reconciliation, so
     * the dropped baseline could keep matching until the next refresh or eviction.
     *
     * Re-evicting after the fence closes that window in the right order: every loader that
     * observed the pre-fence state either publishes BEFORE this second removal (and is then
     * removed) or is rejected by the epoch this removal bumps. A loader that starts after the
     * fence reconciles against it and never publishes the row at all.
     *
     * Parameter rows: the rows the DROP just retired (its own row, plus any identity already
     *             evicted earlier in the same DROP)
     */
    private void evictReWarmedAfterAbsenceFence(List<BaselinePlan> rows) {
        evictRetiredRows(rows);
    }

    /**
     * Evicts the rows a DROP retired DURABLY (the same-key twins of the dropped key, and any
     * lingering incarnation of the same id) from the local cache, with their publication
     * epochs (see removeCachedBaseline).
     *
     * A warm hash bucket can hold ONLY the twin (id40) while the row being dropped
     * (id41) is not cached at all: the cache-miss drop path deleted and tombstoned the
     * twin durably but left id40 in the complete bucket, and completeRowsForHash serves
     * that bucket BEFORE any tombstone / fence reconciliation - so later queries kept
     * replaying a baseline the DROP had retired until the next refresh or eviction.
     *
     * Parameter retired: the durably retired rows (may be null / empty)
     */
    private void evictRetiredRows(List<BaselinePlan> retired) {
        if (retired == null) {
            return;
        }
        for (BaselinePlan row : retired) {
            if (row == null || row.getId() <= 0) {
                continue;
            }
            removeCachedBaseline(row);
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
            // A previous DROP attempt can leave the row absent while tombstone confirmation or same-key cleanup
            // is incomplete. Resume the retained key cleanup before confirming its tombstones. Any unreadable
            // marker keeps the DROP retryable; resumed retirements add their own markers to the same obligation.
            resumePendingDropKeyCleanup(id);
            PendingMutationFence fence = pendingMutationFences.get(id);
            if (fence != null && fence.droppedIdentity != null) {
                confirmObservedDrop(id, fence);
            } else if (pendingDropTombstoneConfirmations.containsKey(id)) {
                confirmPendingDropTombstones(id);
            }
            return false;
        }
        // Retain the KEY CLEANUP of every row this DROP is about to delete BEFORE any delete can
        // land: the bound is a READ of the id watermark and is the only failing
        // step, so an unreadable watermark now fails with every row STILL PRESENT and the retry
        // repeats the whole DROP - it used to fail after the deletes, leaving no retention at all,
        // so an absent-row retry reported success while a newer same-key twin stayed durable.
        for (BaselinePlan row : durable) {
            retainDropKeyCleanup(row);
        }
        try {
            for (BaselinePlan row : durable) {
                persistDeleteByIdentity(row);
                // This identity is confirmed gone even if a later incarnation's delete fails.
                noteDroppedSeqState(row);
                oweDropTombstone(id, row);
            }
        } catch (RuntimeException e) {
            if (!NO_LONGER_MASTER.equals(e.getMessage())) {
                // A failed delete can have committed with delayed visibility. Retain absence fences for these
                // identities and defer tombstones until a read confirms absence; a pre-commit failure must not
                // become an irrevocable DROP marker.
                for (BaselinePlan row : durable) {
                    recordPendingAbsenceFence(row);
                }
            }
            throw e;
        }
        // The identities this DROP retires: the removed rows PLUS the same-key twins the
        // retirement below deletes as well - both ride into the durable confirmation (see
        // confirmDropTombstonesDurable), or a demoted master's delayed status INSERT of a
        // twin revives the dropped baseline on another FE.
        List<BaselinePlan> droppedRows = new ArrayList<>(durable);
        // The hash-epoch barrier covers this CACHE-MISS drop too (see
        // removeCachedBaseline): a by-hash load that read one of these rows BEFORE the
        // DROP pauses between its reconciliation and the publication lock, and both the
        // cached path's pre-fix removeRow (which returned null here) and this path used
        // to leave the epoch unchanged - the loader republished and CACHED the dropped
        // ENABLED row, so every later cache hit served a baseline the completed DROP had
        // removed until the next refresh.
        for (BaselinePlan row : durable) {
            removeCachedBaseline(row);
        }
        // The ID-scope tombstone and the confirmed absence fence are settled right after the
        // deletes, like the cached path (see dropBaseline). The KEY CLEANUP was already retained
        // BEFORE them, so every obligation of this DROP is recoverable from the
        // moment its first delete can land.
        noteDroppedIdState(id);
        // Same CONFIRMED absence fence as dropBaseline: every identity delete of
        // this branch returned OK, so absence is irreversible and only its publication lags.
        recordConfirmedAbsenceFence(id);
        // ... and the same post-fence re-eviction: this branch found the row UNCACHED,
        // so only a concurrent cold read can have re-cached it while the deletes were in flight -
        // which is exactly what the fence + this second removal now rule out.
        evictReWarmedAfterAbsenceFence(durable);
        // retire the KEY's superseded same-key twins as well: this cache-miss DROP path
        // must not leave the loser of a handoff twin pair durable (see
        // retireSupersededKeyTwins). A twin delete that throws leaves this id ABSENT durably,
        // so the retention recorded before the deletes is the only source of the identity whose
        // twins still have to be retired (see resumePendingDropKeyCleanup).
        List<BaselinePlan> retiredKeys = new ArrayList<>();
        try {
            retirePendingDropKeyTwins(id, retiredKeys);
        } catch (RuntimeException e) {
            // The twins retired BEFORE the failing statement are already GONE durably: a
            // warm bucket that still held one of them would keep replaying it until the
            // next refresh (completeRowsForHash serves a warm bucket BEFORE any tombstone /
            // fence reconciliation), so they leave the cache here with their publication
            // epoch. Their confirmation obligation was recorded with the retirement.
            evictRetiredRows(retiredKeys);
            throw e;
        }
        // A warm hash bucket can hold ONLY the twin while the dropped id is not cached at
        // all: the durable retirement above is invisible to the local cache, and
        // completeRowsForHash serves a warm bucket BEFORE any tombstone / fence
        // reconciliation - so the twin must leave the cache here, with its epoch (see
        // evictRetiredRows).
        evictRetiredRows(retiredKeys);
        droppedRows.addAll(retiredKeys);
        // The KEY CLEANUP retention is released BEFORE the tombstone confirmation
        // : the twins above are already retired, and a retention surviving a
        // FAILED confirmation made the absent-row retry resume the cleanup against a key the
        // user may have re-CREATEd - the resumed retirement then deleted the NEW baseline.
        pendingDropKeyCleanups.remove(id);
        // prove the tombstones readable before reporting the DROP (see dropBaseline)
        confirmDropTombstonesDurable(droppedRows, id);
        LOG.info("SPM dropped baseline {} from the durable table while the local cache did"
                + " not have it (promotion reload / stale snapshot window)", id);
        return true;
    }

    /**
     * Retains the identity of a DROP whose KEY CLEANUP is about to be dispatched (see
     * pendingDropKeyCleanups). The snapshot is scalar-only: the transient trees are not needed
     * to re-discover the twins, and a cached row's trees may be stale.
     *
     * Parameter identity: the row this DROP is about to remove
     */
    private void retainDropKeyCleanup(BaselinePlan identity) {
        if (!persistence.isDurable()) {
            return;
        }
        // The bound is the ALLOCATION high-water mark at this instant: every row
        // that already existed carries an id at or below it, and every later allocation gets
        // a LARGER id, so a resumed retirement can never touch a baseline the user created after
        // the DROP. An unreadable mark fails the DROP retryably - guessing it would either skip a
        // preexisting twin or delete a fresh one.
        List<PendingDropKeyCleanup> retained = pendingDropKeyCleanups.getOrDefault(identity.getId(), List.of());
        long retainedMaxId;
        if (retained.isEmpty()) {
            try {
                retainedMaxId = Math.max(identity.getId(), readCompactIdWatermark());
            } catch (RuntimeException e) {
                throw new IllegalStateException("SPM cannot drop baseline " + identity.getId()
                        + ": the id watermark that bounds its key cleanup cannot be read; retry the"
                        + " statement", e);
            }
        } else {
            // Every identity of this DROP and every retry reuse the original allocation bound.
            retainedMaxId = retained.get(0).retainedMaxId;
        }
        boolean alreadyRetained = false;
        for (PendingDropKeyCleanup cleanup : retained) {
            alreadyRetained |= sameIdentity(cleanup.identity, identity);
        }
        if (!alreadyRetained) {
            List<PendingDropKeyCleanup> next = new ArrayList<>(retained);
            next.add(new PendingDropKeyCleanup(identity.copyPersistedScalars(), retainedMaxId));
            pendingDropKeyCleanups.put(identity.getId(), List.copyOf(next));
        }
    }

    /** Retires twins of every retained identity, including identities removed by a previous partial attempt. */
    private void retirePendingDropKeyTwins(long id, List<BaselinePlan> retiredOut) {
        if (!persistence.isDurable()) {
            return;
        }
        for (PendingDropKeyCleanup cleanup : pendingDropKeyCleanups.get(id)) {
            retireSupersededKeyTwins(cleanup.identity, retiredOut, cleanup.retainedMaxId);
        }
    }

    /**
     * Resumes the KEY CLEANUP a failed DROP attempt left unfinished (see
     * pendingDropKeyCleanups), called by the ABSENT-row retry: that retry has no identity row
     * any more, so it re-discovers the same-key twins from every retained identity and retires them.
     *
     * The retirement is idempotent from the durable side (already retired twins are absent and
     * drop out of the discovery read), and both markers and cache eviction follow the normal
     * path: every resumed twin is tombstoned into this DROP's obligation (oweDropTombstone) and
     * leaves the warm cache (evictRetiredRows). A failure keeps the retention, so the next
     * retry resumes again.
     *
     * Parameter id: the dropped id
     */
    private void resumePendingDropKeyCleanup(long id) {
        List<PendingDropKeyCleanup> cleanups = pendingDropKeyCleanups.get(id);
        if (cleanups == null) {
            return;
        }
        List<BaselinePlan> retired = new ArrayList<>();
        try {
            // The retirement is bounded to the rows that already existed when the DROP removed
            // its identity: a CREATE of the SAME SQL that landed afterwards is a NEW baseline
            // the retry must not retire.
            retirePendingDropKeyTwins(id, retired);
        } catch (RuntimeException e) {
            // the twins retired by THIS attempt are gone durably: they must leave the warm
            // cache and keep their markers, and the retention makes the next retry resume
            evictRetiredRows(retired);
            throw e;
        }
        evictRetiredRows(retired);
        pendingDropKeyCleanups.remove(id);
        if (!retired.isEmpty()) {
            LOG.info("SPM resumed the key cleanup of dropped baseline {} and retired {} same-key"
                    + " twin(s) the failed attempt left behind", id, retired.size());
        }
    }

    /**
     * Removes any durable row of the given id that is NOT the identity just deleted (see
     * dropBaseline): a promotion-window snapshot can carry an old object whose
     * id now names a DIFFERENT baseline (the id was dropped and reused), and the DROP is
     * keyed by the user-facing id - identity-deleting only the stale object would report
     * success while the real row stays.
     */
    private List<BaselinePlan> wipeDurableRowsById(long id, BaselinePlan alreadyDeleted,
            List<BaselinePlan> retiredOut) {
        if (!persistence.isDurable()) {
            return retiredOut;
        }
        List<BaselinePlan> rows = readPersistedById(id);
        for (BaselinePlan row : rows) {
            if (sameIdentity(row, alreadyDeleted)) {
                continue; // the row this DROP already deleted
            }
            LOG.warn("SPM drop of baseline {} removed a lingering row of a different"
                    + " incarnation (digest {})", id, row.getBindSqlDigest());
            retainDropKeyCleanup(row);
            persistDeleteByIdentity(row);
            // the wiped incarnation gets its own tombstone: a still-in-flight write of
            // THAT identity must not revive it either
            noteDroppedSeqState(row);
            oweDropTombstone(id, row);
            // ... and it leaves the local cache with its publication epoch (see
            // evictRetiredRows): a cached copy would keep serving rows this DROP removed
            retiredOut.add(row);
        }
        // Retire the KEY's superseded twins as well (see retireSupersededKeyTwins); their
        // identities ride into the DROP's durable confirmation (see
        // confirmDropTombstonesDurable)
        retirePendingDropKeyTwins(id, retiredOut);
        return retiredOut;
    }

    /**
     * Retires every durable row that carries the IDENTICAL (bind_sql_digest, plan_sql,
     * schema_fingerprint) key under a DIFFERENT id: a leader handoff can leave the same
     * baseline enabled at two ids, matching collapses them to the LARGER id, and the
     * leader-side repair is best-effort (a follower cannot repair at all). If a DROP
     * removes the winner while the loser stays durable, the next cold lookup sees the
     * loser ALONE - even the corrected collapse keeps replaying the baseline that was
     * just dropped. The loser is retired durably (its identity DELETE is confirmed
     * before this returns) with its own tombstone; a row under a DIFFERENT fingerprint
     * is a DIFFERENT incarnation and stays untouched. Shared by the cached and the
     * cache-miss DROP paths, including retries. Only rows at or below maxRetainedId are retired.
     *
     * A cleanup can run after the DROP removed its identity, and by then the key can
     * name a NEWER baseline: the user re-CREATEd the very same SQL, the durable row carries the
     * same (digest, planSql, fingerprint) and a fresh id - the key check alone cannot tell it
     * from a handoff twin, and retiring it deleted the baseline the user had just created
     * . The bound is the ALLOCATION high-water mark the DROP recorded, so every twin
     * that existed then is still retired and every later CREATE is kept.
     *
     * The bound is an ID, not a timestamp: the status flip pushes a row's stored
     * timestamp past wall time, so a timestamp bound skipped a PREEXISTING twin that carried a
     * future-dated update time and deleted a CREATE that landed in the same stored instant.
     *
     * Parameter alreadyDeleted: the row the DROP removed
     * Parameter retiredOut: collects the durably retired rows
     * Parameter maxRetainedId: the inclusive allocation upper bound recorded before the identity delete
     */
    private void retireSupersededKeyTwins(BaselinePlan alreadyDeleted,
            List<BaselinePlan> retiredOut, long maxRetainedId) {
        if (!persistence.isDurable()) {
            return;
        }
        // The key's rows are read from the DURABLE side, never from the in-memory
        // complete cache: the snapshot / matching paths COLLAPSE the pair to the winner
        // before it is cached (reconcileMatchingRows -> collapseDuplicateKeyRows), so the
        // cache cannot witness the loser this cleanup exists for. The raw by-hash read is
        // the same read the matching-path miss uses; its failure fails the DROP retryably.
        // The TIMED overload is what the live store implements (the plain one defaults to
        // an empty list): calling the plain overload saw no twins at all on production
        // and the handoff loser stayed durable.
        List<BaselinePlan> keyRows;
        try {
            keyRows = persistence.readRowsByHash(alreadyDeleted.getBindSqlHash(),
                    INTERNAL_QUERY_TIMEOUT_SECONDS);
        } catch (RuntimeException e) {
            throw new IllegalStateException("SPM cannot drop baseline " + alreadyDeleted.getId()
                    + ": the durable rows of its key cannot be read to retire superseded"
                    + " same-key twins; retry the statement", e);
        }
        for (BaselinePlan twin : keyRows) {
            if (twin.getId() == alreadyDeleted.getId()
                    || !Objects.equals(twin.getBindSqlDigest(), alreadyDeleted.getBindSqlDigest())
                    || !Objects.equals(twin.getPlanSql(), alreadyDeleted.getPlanSql())
                    || !Objects.equals(twin.getSchemaFingerprint(),
                            alreadyDeleted.getSchemaFingerprint())) {
                continue;
            }
            if (twin.getId() > maxRetainedId) {
                // A baseline CREATED after the DROP removed its rows: same SQL, new
                // incarnation the user owns - never a handoff twin of this DROP.
                LOG.info("SPM kept baseline {} while retiring the same-key twins of dropped"
                        + " baseline {}: it was created after the drop", twin.getId(),
                        alreadyDeleted.getId());
                continue;
            }
            LOG.warn("SPM drop of baseline {} retired its superseded same-key twin at id"
                    + " {}", alreadyDeleted.getId(), twin.getId());
            persistDeleteByIdentity(twin);
            noteDroppedSeqState(twin);
            // The obligation is recorded HERE, before the loop can reach a twin whose
            // delete throws: the twins retired so far are already GONE durably, and the
            // retry finds them ABSENT - only the obligation keeps their markers owed (see
            // confirmPendingDropTombstones).
            oweDropTombstone(alreadyDeleted.getId(), twin);
            retiredOut.add(twin);
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
        if (!persistence.isDurable()) {
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
            BaselinePlan raced = baselineCache.getById(id);
            if (raced != null) {
                return raced;
            }
            if (durableHashReadAvailable()) {
                baselineCache.insertIfPresent(winner.getBindSqlHash(), winner);
            } else {
                baselineCache.insertOrCreate(winner.getBindSqlHash(), winner);
            }
            publishHash(winner.getBindSqlHash());
            stateVersion++;
            LOG.info("SPM adopted durable baseline {} into the cache after a promotion"
                    + " cache miss", id);
            return winner;
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Reads every durable row carrying the given id (DROP reconcile / identity delete /
     * the ambiguous-write probe of probeDurableRow): the id is the table's duplicate key,
     * so this read is keyed, unlike the identity-filtered readPersistedByKey whose scan
     * grows with the (uncapped) table.
     * A read FAILURE propagates: “no row” may only be reported from a successful read.
     *
     * Parameter id: the baseline id
     * Returns: the durable rows (empty when none)
     * Throws RuntimeException: when the durable state cannot be read
     */
    private List<BaselinePlan> readPersistedById(long id) {
        return readPersistedRowsById(id, false);
    }

    /**
     * Reads the durable rows of one id with their transient trees REBUILT (like a load):
     * the ALTER adoption path needs fully usable rows. Unparsable rows are skipped with a
     * warning; when every row of a NON-EMPTY result is unparsable the read fails
     * retryably - the FE would never load such a row, so silently reporting “not found”
     * would be wrong.
     *
     * Parameter id: the baseline id
     * Returns: the parsed durable rows (empty when none)
     * Throws RuntimeException: when the durable state cannot be read / parsed
     */
    private List<BaselinePlan> readPersistedParsedById(long id) {
        return readPersistedRowsById(id, true);
    }

    /**
     * Shared reader of readPersistedById / readPersistedParsedById:
     * seam-aware and fail-closed on read errors.
     *
     * Parameter id: the baseline id
     * Parameter rebuildTrees: whether each row is parsed like a load (parsePersistedRow)
     *                     instead of decoded as plain scalars (fromRow)
     * Returns: the rows (possibly empty)
     */
    private List<BaselinePlan> readPersistedRowsById(long id, boolean rebuildTrees) {
        return persistence.readRowsById(id, rebuildTrees);
    }

    /**
     * The live implementation half of BaselinePersistence.readRowsById: the
     * seam-free body of the former readPersistedRowsById (SQL read with the transient
     * trees optionally rebuilt, failing when EVERY row of a non-empty result is
     * unparsable).
     */
    private static List<BaselinePlan> readRowsByIdFromTable(long id, boolean rebuildTrees) {
        if (!persistenceEnabled()) {
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
                    try {
                        result.add(rebuildTrees ? parsePersistedRow(row) : fromRow(row));
                    } catch (Throwable t) {
                        LOG.warn("SPM skip an unparsable persisted baseline row (id {}): {}",
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
     * Parameter id: the baseline id
     * Parameter status: the target status
     * Returns: whether the update succeeded
     */
    public boolean updateStatus(long id, BaselineStatus status) {
        ensureLoadedOrThrow();
        // Two-phase status change: the INSERT(new) + DELETE(old) pair is internal-table I/O
        // and runs under writerLock, never under stateLock. Matching tolerates a status
        // flip racing with a lookup exactly like an ALTER landing right after the lookup
        // (see findCandidateBaselines); on failure the in-memory flip is reverted.
        synchronized (writerLock) {
            if (hasObservedDropAwaitingTombstones(id)) {
                return false;
            }
            BaselinePlan plan = baselineCache.getById(id);
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
            boolean durableReconcile = persistence.isDurable();
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
                    // appeared and won the equal-timestamp digest tie): completing the ALTER
                    // against B would report success for an identity the durable table no
                    // longer favors, and a differing-status flip would even write a newer
                    // B row and switch the durable winner. Adopt the winner that IS
                    // durable and fail retryably.
                    LOG.warn("SPM status update of baseline {}: the durable winner of this id"
                            + " is a DIFFERENT baseline (a handoff collision); adopting it and"
                            + " failing the ALTER retryably", id);
                    removeCachedBaseline(plan);
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
                    // A SUCCESSFUL no-op must still MOVE the durable intent (see
                    // persistNoOpStatusIntent): an opposite-status write dispatched earlier
                    // stalled past its shared window, possibly across a handoff - carries a
                    // stored instant derived from the row it saw and would otherwise win the
                    // durable pick when it finally lands, reversing this successful ALTER.
                    long intentTime = persistNoOpStatusIntent(plan, status);
                    // the durable row already carries the requested status (the cache
                    // missed an earlier flip): repair the live object and report success
                    if (previousStatus != status) {
                        publishStatusAndFence(plan, status, intentTime);
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
                // A REPAIR (the cache already carried the requested status, the durable row
                // contradicts it) is the signature of a HANDOFF: this FE's state came from a
                // snapshot the previous master's in-flight write had not reached yet, so the very
                // read that selected this branch may be missing rows of this id (see
                // readNewestStoredUpdateMicrosAroundQuietClock).
                previousStatus = otherStatus(status);
            } else if (previousStatus == status) {
                // nothing durable to reconcile (unit tests / disabled persistence) and
                // the status is already the requested one
                return true;
            }
            // A REAL flip must pass the same gate as the no-op path. The durable
            // read that produced DIFFERS saw the OLD row because a NEWER write of this id - a
            // committed no-op DISABLE at T+1, say - is still UNPUBLISHED; its own shared window
            // and confirmed local fence are the cluster-visible evidence of that. Dispatching
            // without the gate let the flip read the old second, write ENABLED at the same T+1,
            // and qualify its conditional INSERT against the very stale row the hidden write
            // will replace: once both publish, the presence-only confirmation accepts ENABLE
            // while the cleanup deletes only DISABLED T, leaving DISABLED T+1 to win the
            // timestamp tie on the next refresh - both ALTERs reported success and ENABLE is
            // reversed. Failing retryably here is the same rule the MATCHES branch already
            // applies; a flip that orders itself ABOVE the unresolved instant passes instead
            // (see requireNoContradictingStatusFence), which keeps the ordinary sequences
            // working.
            // The internal table is a DUPLICATE-key table on which UPDATE is not supported, so a
            // status change is persisted as INSERT (new status) + DELETE (old status). The INSERT
            // runs FIRST so the durable new row exists before any delete: a failure can never
            // leave the in-memory state "old" while the only table row was already removed (the
            // delete-then-insert gap, where the next refresh / restart silently dropped the
            // baseline). Deleting by the PREVIOUS status can never touch the freshly inserted
            // row (the statuses differ).
            // Two committed flips can share a wall-clock microsecond, and pickDurableWinner prefers
            // DISABLED when stored timestamps tie. A later ENABLE would then be reversed by refresh.
            // Advancing the new row past the
            // newest stored instant OF THIS ID keeps the durable order ("the later intent
            // wins") exact at the stored precision among the id's own rows, so the row
            // this statement writes is always the durable winner once it is written.
            //
            // The bump uses the SAME producer as the no-op intent (see
            // nextStoredUpdateMicros), INCLUDING its lead bound: advancing the
            // stored instant past wall time is what makes rapid alternating real flips order
            // correctly, but an UNBOUNDED advance left visible rows whose stored instant was
            // more than a window bound ahead of wall time - and every shared fence that
            // masks an unreadable committed write is wall-clock bounded. After a handoff the
            // old ENABLED row could then stay visible while its DISABLE (U+1) was unreadable,
            // and a no-op ENABLE wrote ENABLED at that very U+1: with both rows publishing in
            // version order the presence-only confirmation accepted ENABLE, pruning strictly
            // older timestamps preserved DISABLED U+1, and pickDurableWinner chose DISABLED
            // on the tie - reversing the successful ENABLE. A statement whose stored instant
            // would exceed the bound therefore fails retryably BEFORE it is dispatched, so
            // no row it writes can outlive the fences that cover its publication.
            // ONE bounded keyed read feeds both the stored instant this statement writes and the
            // CAS bound of the conditional INSERT / the old-row delete below. It is bracketed by a
            // QUIET shared mutation clock on EVERY flip, not only on a repair: the
            // keyed read only sees READABLE rows, so a FOREIGN write that committed but is still
            // unpublished is invisible to it although its stored instant may already be later
            // than the one this statement is about to write. With the supported clock skew
            // (max_bdbje_clock_delta_ms) an old leader's ordinary DISABLE easily lands 30s ahead
            // of the successor's wall clock: the successor reads the visible OLD row, writes
            // ENABLE at T+1us, its conditional INSERT reads T before the hidden DISABLE
            // publishes, both then publish, the presence confirmation accepts ENABLE while the
            // cleanup deletes only <= T, and the hidden DISABLE wins the durable pick - the next
            // refresh reverts the successful ENABLE. A window is open for the whole lifetime of
            // every foreign statement (from before dispatch until its row is readable), so
            // "quiet before AND after" proves nothing was hidden from this read.
            long newestStoredMicros = readNewestStoredUpdateMicrosAroundQuietClock(id);
            long newUpdateTime = nextStoredUpdateMicros(id, newestStoredMicros,
                    SPMUtils.nowMicros());
            requireNoContradictingStatusFence(id, status, newUpdateTime);
            // Persist a DETACHED snapshot carrying the new status: the live object keeps
            // the old status until the durable write (or the confirmed reconciliation)
            // succeeded - matching readers do not take the writer lock, so publishing
            // early would let a concurrent query replay a baseline whose durable row is
            // still DISABLED when the write later fails, while ALTER still reports
            // failure.
            BaselinePlan durablePlan = plan.copyPersistedScalars();
            durablePlan.setStatus(status);
            durablePlan.setUpdateTime(newUpdateTime);
            // FENCE FROM DISPATCH: the shared mutation window opens with the statement, but
            // the PROCESS-LOCAL fence used to be recorded only when the statement FAILED.
            // A successor ALTER that read a MATCHING status while this write was still on
            // its way therefore accepted a no-op success that this row could reverse. The id
            // is unresolved from the moment the statement may commit; the outcome below
            // replaces / removes the entry.
            boolean insertWritten;
            PendingMutationFence dispatchFence = null;
            try {
                assertLeaderForWrite();
                // FENCE FROM DISPATCH: the statement may commit from the moment it is sent,
                // but the PROCESS-LOCAL fence used to be recorded only when it FAILED. A
                // successor ALTER that read a MATCHING status while this write was still on
                // its way therefore accepted a no-op success that this row could reverse.
                // Installed after the leadership check (a rejected ALTER never ran and must
                // not mask the id) and BEFORE the statement is dispatched; the outcome below
                // replaces / removes the entry.
                dispatchFence = new PendingMutationFence(status,
                        System.currentTimeMillis(), newUpdateTime);
                pendingMutationFences.put(id, dispatchFence);
                insertWritten = persistTransitionInsert(durablePlan, previousStatus,
                        newestStoredMicros);
            } catch (MutationWindowUnavailableException e) {
                // The window of this flip could not be made VISIBLE, so its INSERT was never
                // dispatched (see beginRowMutation): nothing changed, there is no ambiguous
                // outcome to reconcile, and no fence may be kept for a statement that never
                // ran - recording one would mask the id for the fence's whole bound.
                if (dispatchFence != null) {
                    pendingMutationFences.remove(id, dispatchFence);
                }
                throw e;
            } catch (UnconfirmedVisibleException e) {
                // The conditional INSERT WROTE the new-status row (the affected-row count /
                // the committed-write probe is the evidence) - only its PUBLICATION lags
                // past every probe. The row is durable, and its stored instant is strictly
                // later than every row the write met (see the bump above), so it IS the
                // durable winner (pickDurableWinner). Keeping the OLD cached status here
                // let ordinary queries keep replaying a baseline whose durable winner is
                // already the requested status. Publish the proven flip.
                publishStatusAndFence(plan, status, newUpdateTime);
                LOG.warn("SPM status update of baseline {} reported success but its row is"
                        + " not READABLE yet; the committed flip is the durable winner -"
                        + " publishing it", id);
                // the row may stay unreadable for a while: the CONFIRMED fence installed
                // with the publication (see publishStatusAndFence) keeps a stale snapshot
                // from reverting the committed flip
                return true;
            } catch (AmbiguousInsertException e) {
                // The conditional INSERT reported an ERROR that may have been raised AFTER
                // a commit: the outcome is UNPROVEN, so an unobserved status is never
                // published - the cache follows a winner that IS readable, the id stays
                // fenced, and the failure travels to the client.
                handleUnprovenStatusInsert(id, plan, status, newUpdateTime);
                throw e;
            } catch (RuntimeException e) {
                // Any other failure of the INSERT half: same rule - never publish an
                // unobserved status.
                handleUnprovenStatusInsert(id, plan, status, newUpdateTime);
                throw e;
            }
            if (!insertWritten) {
                // The conditional statement matched no previous-status row, so it reported
                // SQL OK while writing NOTHING (a concurrent DROP or status flip won the
                // race - the concurrent-write scenario). The requested status may still
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
                // The cleanup is bound to the OLD row's stored VERSION (see
                // deleteByIdAndStatusSql): newestStoredMicros is the stored INSTANT of the
                // row this flip completes (the durable winner of the id, which the INSERT
                // above was conditional on), so a row a LATER flip wrote cannot be matched
                // by it. MICROSECOND storage makes the instant exact - a second-truncated
                // binding matched no row at all once the stored value carried a
                // sub-second part.
                persistDeleteByIdAndStatus(durablePlan, previousStatus,
                        Math.max(0L, newestStoredMicros));
            } catch (RuntimeException e) {
                // The INSERT half is CONFIRMED (its row is READABLE) and carries a strictly
                // later stored instant than every row it met, so the new-status row IS the
                // durable winner whether the old-row delete committed, failed, or merely
                // lags its publication. The cache MUST follow that winner: leaving the OLD
                // status let this FE keep replaying a baseline the durable table has
                // already flipped until the next refresh.
                publishStatusAndFence(plan, status, newUpdateTime);
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
            publishStatusAndFence(plan, status, newUpdateTime);
            return true;
        }
    }

    /**
     * The reconciliation of a status flip whose INSERT outcome cannot be PROVEN (an
     * AmbiguousInsertException, or any other failure while the row may have
     * committed): never publish an unobserved status. The cache is aligned with a durable
     * winner that IS readable, the id stays FENCED (a successful read CANNOT prove the
     * write landed nothing - the publication of a committed row lags every immediate probe
     * so the committed row may publish at any moment and the reconciled OLD-status entry
     * would keep being replayed until a refresh), and the obsolete entry leaves matching.
     */
    private void handleUnprovenStatusInsert(long id, BaselinePlan plan, BaselineStatus status,
            long newUpdateTime) {
        reconcileCacheToDurableWinner(id, plan);
        recordPendingMutationFence(id, status, newUpdateTime);
        if (plan.getStatus() != status) {
            removeCachedBaseline(plan);
        }
    }

    /** Publishes a CONFIRMED status flip on the live object under the state lock. */
    private void publishStatus(BaselinePlan plan, BaselineStatus status, long updateTime) {
        publishStatus(plan, status, updateTime, false);
    }

    private void publishStatus(BaselinePlan plan, BaselineStatus status, long updateTime,
            boolean installConfirmedFence) {
        stateLock.writeLock().lock();
        try {
            // The object the flip STARTED on may no longer be the row the cache holds: an
            // LRU eviction / promotion reload between the initial read and this
            // publication can reload the SAME id into a DIFFERENT object (still
            // ENABLED). Mutating only the original, evicted object left the current
            // cache entry ENABLED, and warm matching returns a cached row directly - the
            // confirmed mutation fence is consulted on a cache MISS only - so queries
            // kept replaying the baseline after a successful DISABLE. Publish to the row
            // the cache holds NOW; a row evicted again needs nothing (the next load reads
            // the flipped durable state, which the epoch bump below guards).
            BaselinePlan current = baselineCache.getById(plan.getId());
            if (current != null && current != plan) {
                current.setStatus(status);
                current.setUpdateTime(updateTime);
            }
            plan.setStatus(status);
            plan.setUpdateTime(updateTime);
            stateVersion++;
            // The flip changed the rows BEHIND this hash without changing the key set:
            // advance the epoch so a matching-path cache-miss read that started before
            // this point cannot publish its pre-flip rows over the completed state.
            bumpHashEpochLocked(plan.getBindSqlHash());
            if (installConfirmedFence) {
                // same critical section (see publishStatusAndFence): no load may see the
                // flushed cache without the fence that masks the lagging row
                recordConfirmedMutationFence(plan.getId(), status, updateTime);
            }
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Publishes a CONFIRMED status flip AND installs its confirmed mutation fence in the SAME
     * critical section (see publishStatus).
     *
     * The two steps used to be separate: the epoch bump / cache mutation ran under the state
     * lock, the fence was installed afterwards. A by-hash load that started AFTER the bump but
     * read a pre-flip row (its publication lags) compared an UNCHANGED epoch, found no fence
     * yet and published that stale row - turning the stale miss into a warm hit that served
     * the pre-flip status. Installing the fence where the flip becomes visible closes that
     * window: a load that could observe the stale row must take the state lock after this
     * section, where the fence is already installed and masks the id.
     *
     * Parameter plan: the cached row the flip started on
     * Parameter status: the confirmed status
     * Parameter updateTime: the stored instant the confirmed row carries
     */
    private void publishStatusAndFence(BaselinePlan plan, BaselineStatus status,
            long updateTime) {
        publishStatus(plan, status, updateTime, true);
    }

    /**
     * Appends a newer same-status intent and returns its epoch-microsecond update time. A shared mutation window
     * covers the insert and version pruning; pruning can use a renewed window when required. Confirmed but
     * unreadable intent rows retain fences, while an ambiguous insert remains a retryable failure. Pruning is best
     * effort and can retain an unresolved prune window.
     */
    private long persistNoOpStatusIntent(BaselinePlan plan, BaselineStatus status) {
        long intentTime = nextStoredUpdateMicros(plan.getId());
        if (!persistence.isDurable()) {
            return intentTime;
        }
        BaselinePlan intent = plan.copyPersistedScalars();
        intent.setStatus(status);
        intent.setUpdateTime(intentTime);
        // ONE shared mutation window brackets the INSERT *and* the prune. The prune is a
        // DELETE that changes the row set a PAGINATED reader walks (id >= lastId LIMIT n
        // OFFSET m, see SELECT_PAGE_SQL): a follower whose page 1 ended on the row the
        // DELETE removes continues past the row it still holds, its quiet clock and empty
        // end probe accept the stale page and overwrite a warm cache with pre-intent rows.
        // Inside the window that overlap is retryable, exactly like the flip's own writes.
        assertLeaderForWrite();
        long mutation = beginRowMutation();
        boolean windowHeld = false;
        try {
            try {
                persistence.insert(intent);
            } catch (RuntimeException e) {
                // an ERROR that may have been raised AFTER a commit is AMBIGUOUS: the
                // attempted row is adopted instead of failing the write outright
                throwAmbiguousStatusInsertUnlessDurable(intent, e);
            }
            confirmInsertVisible(intent);
            long keepFromMicros = Math.max(0L, intentTime);
            // The intent's read-back confirmation can consume most of the window's
            // successor-facing age bound (BASELINE_VISIBILITY_ATTEMPTS probes). A window past
            // that bound is IGNORED by every other FE's fence read, so the DELETE below would
            // run with nothing left to reject a paginated reader's stale page - the prune had
            // no renewed lease and no dispatch-age check of its own (the.
            try {
                assertMutationWindowDispatchable(mutation);
            } catch (MutationWindowUnavailableException agedWindow) {
                // The intent is CONFIRMED READABLE, so the ordering it proves is durable (its
                // stored instant is the newest one) and its window may close now. The prune gets
                // a FRESH window: a renewed lease, not a reused one.
                endRowMutation(mutation);
                mutation = 0;
                try {
                    mutation = beginRowMutation();
                    LOG.info("SPM renewed the mutation window of the no-op status statement of"
                            + " baseline {} before its version prune (new window {})",
                            plan.getId(), mutation);
                } catch (RuntimeException renewFailure) {
                    // The prune is best effort by design (the next statement reclaims the
                    // versions); the status itself is already durable and must not fail for it.
                    LOG.warn("SPM leaves the superseded-version prune of baseline {} to the"
                            + " next statement: its renewed mutation window could not be opened"
                            + " ({})", plan.getId(), renewFailure.getMessage());
                    return intentTime;
                }
            }
            try {
                persistence.pruneSupersededVersions(intent, keepFromMicros);
                // SQL OK is not PUBLICATION: the DELETE may have committed while readers still
                // see the pruned rows, and a paginated reader positioned on one of them steps
                // past the intent row behind it (id >= X OFFSET 1). Prove the versions gone; a
                // prune no read can prove leaves the shared window OPEN (the age bound heals
                // it) instead of closing it and letting the stale page be accepted.
                if (!confirmPruneVisible(intent, keepFromMicros)) {
                    windowHeld = holdUnresolvedPruneWindow(mutation, intent, keepFromMicros);
                    if (!windowHeld) {
                        LOG.warn("SPM cannot fence the reclaimed versions of baseline {}: no"
                                + " shared window exists", plan.getId());
                    }
                }
            } catch (RuntimeException e) {
                // A thrown DELETE may have COMMITTED with its publication lagging, exactly
                // like an unprovable prune above: closing the window here let a paginated
                // reader step past the intent row while the versions that DELETE removes
                // were still readable, and its quiet clock then accepted the mixed
                // snapshot. The prune itself must not fail the ALTER, but the SHARED
                // window stays OPEN until a read proves the reclaim (or its age bound
                // heals it); a prune that never dispatched is released by the same
                // settlement probe.
                windowHeld = holdUnresolvedPruneWindow(mutation, intent, keepFromMicros);
                LOG.warn("SPM could not reclaim the superseded versions of baseline {}"
                        + " ({}); the next statement prunes them", plan.getId(), e.getMessage());
            }
        } catch (UnconfirmedVisibleException e) {
            // The row IS durable (the insert is proven to have written) and only its
            // publication lags. persistInsert used to CLOSE the window right here, leaving
            // only this FE's local fence: a successor sees the previous same-status row with
            // a quiet clock and qualifies its opposite-status INSERT against it, while the
            // unreadable intent carries a LATER stored instant - both rows publish, the intent
            // wins the durable pick and the successful ALTER is reversed. Keep the SHARED
            // window open until a read shows the row (or its age bound), like a lagging flip.
            windowHeld = holdUnresolvedMutationWindow(mutation, intent);
            recordConfirmedMutationFence(plan.getId(), status, intentTime);
        } catch (AmbiguousInsertException e) {
            // The INSERT reported an ERROR that may have been raised AFTER a commit, and no
            // read can prove it. The shared window MUST stay unresolved (like the unreadable
            // success above): a successor that reads the previous row with a quiet clock
            // could otherwise qualify an opposite-status write that this possibly-committed
            // row then reverses.
            windowHeld = holdUnresolvedMutationWindow(mutation, intent);
            recordPendingMutationFence(plan.getId(), status, intentTime);
            throw e;
        } catch (RuntimeException e) {
            // The intent was NOT written. Reporting success here let an opposite-status
            // statement - dispatched before this one, its shared mutation window long expired
            // land later with a stored instant derived from the row it saw, win the durable
            // pick and REVERSE this ALTER although it reported success. The ordering
            // guarantee the no-op needs is missing, so fail retryably instead.
            throw new IllegalStateException("SPM cannot record the ordering intent of the"
                    + " status statement of baseline " + plan.getId()
                    + "; retry the ALTER", e);
        } finally {
            if (!windowHeld) {
                endRowMutation(mutation);
            }
        }
        return intentTime;
    }

    /**
     * Whether the reclaim of the superseded versions is VISIBLE yet: SQL OK only proves the
     * DELETE was dispatched, so a paginated snapshot reader could still be positioned on a
     * row it removes. Polls the durable count of older versions; a store that cannot answer
     * reports 0 (nothing to prune) and the caller's window stays as it is.
     *
     * Parameter identity: the identity whose older versions were pruned
     * Parameter keepFromMicros: the stored instant the prune keeps from
     * Returns: whether no version older than keepFromMicros remains readable
     */
    private boolean confirmPruneVisible(BaselinePlan identity, long keepFromMicros) {
        if (keepFromMicros <= 0) {
            return true;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            long older = persistence.countVersionsBefore(identity, keepFromMicros);
            if (older == 0) {
                return true;
            }
            if (older < 0) {
                return false; // the store cannot answer: fail closed
            }
            sleepBeforeVisibilityRetry();
        }
        return false;
    }

    /**
     * The next stored instant (epoch microseconds) a row of this id may carry: strictly later
     * than every stored instant of the id, so the row this statement writes is the durable
     * winner among the id's own rows (see the bump in updateStatus).
     *
     * Parameter id: the baseline id
     * Returns: the update time to store
     */
    private long nextStoredUpdateMicros(long id) {
        long newestMicros;
        try {
            newestMicros = readNewestStoredUpdateMicros(id);
        } catch (RuntimeException e) {
            // Rapid ALTERs advance the stored instant BEYOND wall time, so an unreadable
            // newest instant makes this statement unable to prove its intent is the newest
            // one: an older delayed opposite-status row (stored instant T+1 us, far in the
            // future) would stay eligible and could win after the window expired and a
            // handoff happened. The wall-clock fallback silently accepted that reversal;
            // fail closed and let the caller retry.
            throw new IllegalStateException("SPM cannot read the newest stored instant of"
                    + " baseline " + id + "; retry the ALTER", e);
        }
        return nextStoredUpdateMicros(id, newestMicros, SPMUtils.nowMicros());
    }

    /**
     * Chooses wall time when it is later than the newest stored epoch-microsecond timestamp, otherwise increments
     * that timestamp by one microsecond. Rejects a lead beyond MAX_STORED_UPDATE_LEAD_MICROS before a status write
     * is dispatched.
     */
    private long nextStoredUpdateMicros(long id, long newestMicros, long now) {
        if (now <= newestMicros) {
            // ONE MICROSECOND past the id's newest stored instant: the column keeps six
            // fractional digits, so a single step already makes this row the durable winner
            // without pushing it anywhere near the wall-clock-bounded fences.
            long bumped = newestMicros + 1;
            long leadMicros = bumped - now;
            if (leadMicros > MAX_STORED_UPDATE_LEAD_MICROS) {
                // The stored instant may only lead wall time by a BOUNDED
                // amount, because the SHARED fences that mask an unreadable committed write are
                // time-bounded by wall clock: a mutation window is ignored after
                // MUTATION_WINDOW_MAX_AGE_SECONDS, and an unconfirmed fence expires after
                // PENDING_MUTATION_FENCE_MILLIS. A stored instant that leads wall time by MORE
                // than the shortest of those bounds silently breaks the inference "the fence
                // expired, so the write is lost": the fenced row - an ALREADY COMMITTED no-op
                // status intent, or the surviving version a prune publication still hides -
                // would outlive the fence, still win the durable pick and reverse a successor's
                // successful ALTER (a paginated reader then keeps the old status and skips the
                // surviving newer version). The bound must hold for the LIFETIME of the row,
                // not merely at the moment it is written, so a statement whose stored instant
                // would exceed it fails retryably instead of writing a row no fence can cover.
                throw new IllegalStateException("SPM cannot make the status statement of"
                        + " baseline " + id + " the durable winner: its stored instant would lead"
                        + " wall time by " + leadMicros + " us, beyond the "
                        + MAX_STORED_UPDATE_LEAD_MICROS + " us every shared fence is bounded by;"
                        + " nothing was changed - retry the ALTER");
            }
            return bumped;
        }
        return now;
    }

    /** Outcome of the durable status probe (see updateStatus). */
    private enum DurableStatusProbe { MATCHES, DIFFERS, ABSENT, UNKNOWN }

    /**
     * The durable status of one baseline (see probeDurableStatus): the outcome plus
     * the WINNING row when it could be read. The winner's update time is what a status flip
     * must stay strictly later than (see the microsecond bump in updateStatus).
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
    private DurableStatus probeDurableStatus(long id, BaselineStatus expected) {
        try {
            if (!persistence.isDurable()) {
                return DurableStatus.of(DurableStatusProbe.ABSENT);
            }
            List<BaselinePlan> rows = filterTombstonedDurableRows(readPersistedById(id));
            if (rows.isEmpty()) {
                if (persistence instanceof TablePersistence) {
                    return DurableStatus.of(DurableStatusProbe.ABSENT);
                }
                // The by-id read of a SUBSTITUTED store handed out no row: a store whose
                // status rows carry no identity can still classify the outcome from its
                // COUNTS (the same answer the historic status-protocol probe gave); a
                // store without such rows answers 0 / 0 and the outcome stays ABSENT.
                return classifyDurableStatusByCounts(id, expected);
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
     * Count-only classification of one id's durable status (see probeDurableStatus): the
     * by-id read handed out no rows, so the outcome rests on the per-status counts of the
     * id. Both statuses present at once cannot be ranked without update times - the
     * caller must retry.
     */
    private DurableStatus classifyDurableStatusByCounts(long id, BaselineStatus expected) {
        BaselineStatus other = otherStatus(expected);
        int expectedRows = persistence.countByIdAndStatus(id, null, expected);
        int otherRows = persistence.countByIdAndStatus(id, null, other);
        if (expectedRows == 0 && otherRows == 0) {
            return DurableStatus.of(DurableStatusProbe.ABSENT);
        }
        if (expectedRows > 0 && otherRows == 0) {
            return DurableStatus.of(DurableStatusProbe.MATCHES);
        }
        if (otherRows > 0 && expectedRows == 0) {
            return DurableStatus.of(DurableStatusProbe.DIFFERS);
        }
        throw new IllegalStateException(
                "two durable rows of baseline " + id + " and no update times");
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
    private BaselinePlan readDurableWinnerOrNull(long id) {
        try {
            // A TOMBSTONED row must not win: the snapshot load filters resurrected rows
            // (see filterResurrectedRows), and this raw point read must apply the SAME
            // filter - otherwise a late same-id row A that the user already dropped was
            // picked over a valid B and the cache was reconciled to a condemned status.
            List<BaselinePlan> rows = filterTombstonedDurableRows(readPersistedById(id));
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
        for (BaselinePlan row : rows) {
            Long hash = baselineCache.removeRow(row.getId());
            if (hash == null) {
                continue;
            }
            if (!durableHashReadAvailable() && !baselineCache.hasHash(hash)) {
                dropHash(hash);
            }
            stateLock.writeLock().lock();
            try {
                stateVersion++;
            } finally {
                stateLock.writeLock().unlock();
            }
            LOG.info("SPM retired the stale cached baseline {}", row.getId());
        }
    }

    /**
     * One internal-table statement / query body; see inInternalIoMode.
     */
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

    /**
     * Number of durable rows currently carrying (id, status) AND the identity's schema
     * fingerprint: the status protocol must never read a REUSED id's newer incarnation
     * as if it were the row it wrote (see DELETE_BY_ID_AND_STATUS_SQL).
     */
    private int durableRowCount(long id, String schemaFingerprint, BaselineStatus status) {
        return persistence.countByIdAndStatus(id, schemaFingerprint, status);
    }

    /**
     * The live implementation half of BaselinePersistence.countByIdAndStatus: the
     * seam-free body of the former durableRowCount.
     */
    private static int countByIdAndStatusFromTable(long id, String schemaFingerprint,
            BaselineStatus status) {
        if (!persistenceEnabled()) {
            return 0;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        params.put("status", status.name());
        params.put("schemaFingerprint", StatisticsUtil.escapeSQL(
                schemaFingerprint == null ? "" : schemaFingerprint));
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

    // query matching (Level 1 + Level 2 + ordering).

    /**
     * Finds candidate baselines (the first two of the three-level filter plus priority
     * ordering).
     *
     * Level 1 is answered from the in-memory hash index alone; the rows behind a matching
     * hash come from the LRU cache or - on a miss - from the internal table (the COMPLETE
     * row set of that bind_sql, see completeRowsForHash).
     *
     * Parameter digest: the parameterized digest of the user query
     * Parameter hash: the structural hash of the user query
     * Returns: the candidate baselines sorted by priority (possibly empty)
     */
    public List<BaselinePlan> findCandidateBaselines(String digest, long hash) {
        return findCandidateBaselines(digest, hash, Long.MAX_VALUE);
    }

    /**
     * As findCandidateBaselines(String, long), bounded by the caller's rewrite deadline:
     * a cache miss reads the internal table SYNCHRONOUSLY, and the fixed ten-second
     * statement timeout used to outlive the whole spm_rewrite_timeout_ms budget (one
     * second by default) - a slow / unavailable baseline tablet stalled every cold query
     * far past its requested fallback budget. The remaining budget caps the miss read
     * (rows AND tombstone filter); an exhausted budget degrades THIS attempt without a
     * read (the next statement retries).
     *
     * Parameter deadlineMillis: the rewrite deadline (epoch millis; Long.MAX_VALUE = none)
     */
    public List<BaselinePlan> findCandidateBaselines(String digest, long hash,
            long deadlineMillis) {
        ensureLoaded();
        if (!knownHashes.containsKey(hash)) {
            return List.of();
        }
        // digest filter + priority ordering happen on a snapshot of the rows: the cached
        // objects are immutable for matching purposes except a status flip, which races as
        // harmlessly as an ALTER landing right after the read
        return completeRowsForHash(hash, deadlineMillis).stream()
                .filter(p -> p.getStatus().isActive())
                .filter(p -> p.getBindSqlDigest().equals(digest)) // Level 2 exact digest match
                .sorted(comparator)
                .collect(Collectors.toList());
    }

    /**
     * The COMPLETE row set of one bindSqlHash for matching: the LRU entry when cached,
     * otherwise the internal table (all rows of the bind_sql - never a partial set, or the
     * digest filter / priority ordering could pick the wrong candidate).
     *
     * The miss read is reconciled BEFORE it may match or be cached (see
     * reconcileMatchingRows): one deterministic winner per id (a failed old-row cleanup
     * leaves BOTH physical versions behind, and serving the stale one resurrected a
     * DISABLED baseline) and the active mutation fences (an id whose committed flip is
     * still unreadable must not be served from its pre-flip row).
     *
     * Publication is validated under stateLock against both the hash's mutation epoch and storeGeneration.
     * Local publish / drop / status changes advance the hash epoch. Every accepted snapshot advances the
     * generation, including snapshots whose signatures collide or whose content is unchanged. A raced miss discards
     * its rows for the current attempt too (the next match retries against the newer state)
     * instead of overwriting the completed state. The same lock serializes the empty-read
     * index removal with the other index publishers: the copy-on-write dropHash used to
     * lose a concurrent CREATE's hash under un-synchronized removal.
     *
     * The read is bounded by the caller's remaining rewrite budget (see
     * findCandidateBaselines(String, long, long)); a failed read degrades to "no match"
     * (SPM is best-effort; the query runs normally) without caching anything.
     *
     * CONCURRENT misses of the SAME hash COALESCE into one read: the first attempt loads
     * synchronously (bounded by its budget, see above) while the hash is marked in
     * flight, and every concurrent attempt of that hash degrades immediately by the same
     * "no match" rule - N cold queries no longer issue N reads against the internal
     * table, and the completed load serves the following statements through the cache. A
     * miss of a DIFFERENT hash is not affected (the marker is per hash).
     */
    private List<BaselinePlan> completeRowsForHash(long hash, long deadlineMillis) {
        List<BaselinePlan> cached = baselineCache.getComplete(hash);
        if (cached != null) {
            return cached;
        }
        if (!durableHashReadAvailable()) {
            return List.of();
        }
        final Long seqBefore;
        final long generationBefore;
        stateLock.readLock().lock();
        try {
            seqBefore = knownHashes.get(hash);
            generationBefore = storeGeneration.get();
        } finally {
            stateLock.readLock().unlock();
        }
        if (seqBefore == null) {
            return List.of();
        }
        if (System.currentTimeMillis() >= deadlineMillis) {
            return List.of();
        }
        if (!inflightHashLoads.add(hash)) {
            // Another attempt is already loading THIS hash: degrade without a second
            // internal read (the loader publishes atomically below, so the next
            // statements hit the cache). N concurrent cold queries used to issue N
            // separate reads and stampede the internal table.
            return List.of();
        }
        try {
            // The in-flight load may have completed between the probe at the top and the
            // marker: re-check before reading (a just-published entry serves directly).
            List<BaselinePlan> raced = baselineCache.getComplete(hash);
            if (raced != null) {
                return raced;
            }
            List<BaselinePlan> rows;
            try {
                rows = readPersistedRowsByHash(hash, deadlineMillis);
            } catch (RuntimeException e) {
                LOG.warn("SPM cannot load the baselines of one bindSql for matching (the query"
                        + " runs without SPM): {}", e.getMessage());
                return List.of();
            }
            rows = reconcileMatchingRows(rows);
            stateLock.writeLock().lock();
            try {
                if (System.currentTimeMillis() >= deadlineMillis
                        || storeGeneration.get() != generationBefore
                        || !Objects.equals(seqBefore, knownHashes.get(hash))) {
                    // A snapshot or mutation superseded the read. Discard it from both the cache and result.
                    return List.of();
                }
                if (rows.isEmpty()) {
                    // the hash is stale (its last row was dropped / tombstoned / masked): it
                    // must stop short-circuiting the hash index (a refresh or a local publish
                    // re-adds it)
                    dropHash(hash);
                } else {
                    baselineCache.putComplete(hash, rows);
                }
            } finally {
                stateLock.writeLock().unlock();
            }
            return rows;
        } finally {
            inflightHashLoads.remove(hash);
        }
    }

    /**
     * Reconciles one RAW by-hash read against the SNAPSHOT rules before it may match or
     * be cached:
     *
     * one deterministic winner per id (pickDurableWinner): an ALTER whose old-row
     *   cleanup failed publishes the new-status row while BOTH physical versions stay -
     *   an evicted hash then returned them unfiltered and the active-status filter picked
     *   the STALE row, replaying a baseline the durable winner had already flipped away;
     * active mutation fences: a fence whose expected outcome this read does not show
     *   (e.g. a DISABLE whose committed row is still unreadable) masks its id - the
     *   pre-flip row is NOT the final state and must not match until a snapshot shows the
     *   outcome. An expired UNCONFIRMED fence is presumed lost and stops masking, exactly
     *   like resolvePendingMutationFences.
     *
     * Rows covered by a readable DROP TOMBSTONE were already filtered by the reader
     * (see readRowsByHashFromTable).
     */
    private List<BaselinePlan> reconcileMatchingRows(List<BaselinePlan> rows) {
        if (rows.isEmpty()) {
            return rows;
        }
        Map<Long, BaselinePlan> winners = new java.util.LinkedHashMap<>();
        for (BaselinePlan row : rows) {
            BaselinePlan current = winners.get(row.getId());
            winners.put(row.getId(), current == null ? row : pickDurableWinner(current, row));
        }
        // Cross-id same-key twins (a handoff can leave one key at ids 40 and 41): the
        // snapshot publication hides the earlier allocation via collapseDuplicateKeyRows,
        // but this RAW by-hash read only chose per-id winners - BOTH ids survived here,
        // the active-status filter then dropped the newer DISABLED winner and the
        // superseded ENABLED row was replayed although ALTER BASELINE PLAN 41 DISABLE
        // had completed. Apply the SNAPSHOT's cross-id collapse before serving / caching;
        // the durable repair stays out of the matching path (read-only, no I/O).
        winners = collapseDuplicateKeyRows(winners);
        if (pendingMutationFences.isEmpty()) {
            return new ArrayList<>(winners.values());
        }
        long now = System.currentTimeMillis();
        List<BaselinePlan> reconciled = new ArrayList<>(winners.size());
        for (BaselinePlan winner : winners.values()) {
            PendingMutationFence fence = pendingMutationFences.get(winner.getId());
            if (fence != null && !fence.isSatisfiedBy(winner) && !fence.isExpired(now)) {
                continue;
            }
            reconciled.add(winner);
        }
        return reconciled;
    }

    /**
     * Finds baselines by hash (internal helper): the CACHE only, no table read - the create
     * path's phase-1 duplicate validation uses it, and a miss there simply falls through to
     * the authoritative durable-key check.
     */
    private List<BaselinePlan> findByHash(long hash) {
        List<BaselinePlan> rows = baselineCache.getComplete(hash);
        return rows == null ? List.of() : rows;
    }

    /**
     * Whether any global baseline exists. Used by the rewrite path as a fast path: with
     * no baseline anywhere there is nothing to match, so the whole-tree digest rendering
     * can be skipped entirely.
     *
     * Returns: whether the global hash index currently holds at least one hash
     */
    public boolean hasBaselines() {
        ensureLoaded();
        return !knownHashes.isEmpty();
    }

    /**
     * Returns a baseline by id: the LRU cache when the row is cached, otherwise a KEYED
     * durable read of that id (the deterministic durable winner, tombstoned rows filtered
     * out). No caching on the miss: a single-row entry would not be a COMPLETE bind_sql
     * set.
     *
     * Parameter id: the baseline id
     * Returns: the baseline, or null when not found
     */
    public BaselinePlan getBaseline(long id) {
        ensureLoaded();
        if (hasObservedDropAwaitingTombstones(id)) {
            return null;
        }
        BaselinePlan cached = baselineCache.getById(id);
        if (cached != null) {
            return cached;
        }
        if (!durableHashReadAvailable()) {
            return null;
        }
        try {
            List<BaselinePlan> rows = filterTombstonedDurableRows(readPersistedParsedById(id));
            if (rows.isEmpty()) {
                return null;
            }
            BaselinePlan winner = rows.get(0);
            for (int i = 1; i < rows.size(); i++) {
                winner = pickDurableWinner(winner, rows.get(i));
            }
            return winner;
        } catch (RuntimeException e) {
            LOG.warn("SPM cannot read baseline {} (treating it as not cached): {}",
                    id, e.getMessage());
            return null;
        }
    }

    /**
     * Returns confirmed GLOBAL rows for SHOW. Followers synchronize the journal and publish the snapshot
     * under writer serialization. The response uses those same rows, including rows outside the match cache.
     * A failed read raises a retryable error.
     */
    public List<BaselinePlan> getConfirmedGlobalRowsForShow() {
        if (!persistence.isDurable() || (cluster.isLeader() && persistence.hashReadsAvailable())) {
            Map<Long, BaselinePlan> initialSnapshot = ensureLoadedOrThrow();
            return initialSnapshot == null ? getAllBaselines() : sortedRows(initialSnapshot.values());
        }
        synchronized (writerLock) {
            // Fence: a snapshot whose READ started before this point may predate the
            // master's committed DDL, so it must never publish after this method
            // returns (the generation check discards the in-flight load instead).
            storeGeneration.incrementAndGet();
            syncJournalWithMaster(ConnectContext.get());
            Map<Long, BaselinePlan> initialSnapshot = ensureLoadedOrThrow();
            if (initialSnapshot != null) {
                return sortedRows(initialSnapshot.values());
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
            return sortedRows(applyRefreshedBaselines(snapshot).values());
        }
    }

    /**
     * Returns all baselines (for SHOW / tests).
     *
     * The durable rows are read FRESH: the store no longer keeps every baseline in memory,
     * and a table iteration is exactly what this management read wants (fences mask the
     * ids whose outcome is still owed here, tombstoned rows are filtered, same-key twins
     * collapse deterministically). Without a durable table (unit tests / internal schema db
     * disabled) the cache IS the storage and is returned.
     */
    public List<BaselinePlan> getAllBaselines() {
        ensureLoaded();
        List<BaselinePlan> all;
        if (!durableHashReadAvailable()) {
            all = baselineCache.allRows();
        } else {
            synchronized (writerLock) {
                final Map<Long, BaselinePlan> snapshotRead;
                try {
                    snapshotRead = readPersistedSnapshot();
                } catch (Throwable t) {
                    throw new IllegalStateException("SPM baseline rows cannot be read"
                            + " (SHOW must not report a stale table); please retry later: "
                            + t.getMessage(), t);
                }
                Map<Long, BaselinePlan> snapshot = visibleSnapshotRows(snapshotRead,
                        resolvePendingMutationFences(snapshotRead));
                return sortedRows(snapshot.values());
            }
        }
        return sortedRows(all);
    }

    private static List<BaselinePlan> sortedRows(Collection<BaselinePlan> rows) {
        List<BaselinePlan> sorted = new ArrayList<>(rows);
        sorted.sort(Comparator.comparingLong(BaselinePlan::getId));
        return sorted;
    }

    private static Map<Long, BaselinePlan> visibleSnapshotRows(Map<Long, BaselinePlan> rows, Set<Long> fenced) {
        Map<Long, BaselinePlan> visible = collapseDuplicateKeyRows(rows);
        if (!fenced.isEmpty()) {
            visible = new HashMap<>(visible);
            fenced.forEach(visible.keySet()::remove);
        }
        return visible;
    }

    // durable reads by hash (the matching-path miss).

    /** Converts the remaining matching budget to the internal SQL statement timeout. */
    static int remainingReadTimeout(long deadlineMillis) {
        if (deadlineMillis == Long.MAX_VALUE) {
            return INTERNAL_QUERY_TIMEOUT_SECONDS;
        }
        long remaining = deadlineMillis - System.currentTimeMillis();
        if (remaining <= 0) {
            throw new IllegalStateException("SPM rewrite deadline expired");
        }
        return (int) Math.min(INTERNAL_QUERY_TIMEOUT_SECONDS, 1 + (remaining - 1) / 1000);
    }

    /** Reads a complete hash bucket and verifies its tombstones before publishing it. */
    private List<BaselinePlan> readPersistedRowsByHash(long hash, long deadlineMillis) {
        List<BaselinePlan> rows = new ArrayList<>(
                persistence.readRowsByHash(hash, remainingReadTimeout(deadlineMillis)));
        return filterTombstonedDurableRows(rows, deadlineMillis);
    }

    /** The live implementation half of BaselinePersistence.readRowsByHash(long, int). */
    private List<BaselinePlan> readRowsByHashFromTable(long hash, int timeoutSeconds) {
        if (!durableWritesEnabled()) {
            return List.of();
        }
        Map<String, String> params = new HashMap<>();
        params.put("hash", String.valueOf(hash));
        List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                SELECT_BY_HASH_SQL, params, timeoutSeconds));
        List<BaselinePlan> parsed = new ArrayList<>();
        if (rows != null) {
            for (ResultRow row : rows) {
                try {
                    parsed.add(parsePersistedRow(row));
                } catch (Throwable t) {
                    LOG.warn("SPM skip an unparsable persisted baseline row (hash {}): {}",
                            hash, t.getMessage());
                }
            }
        }
        return parsed;
    }

    // internal table persistence (__internal_schema.spm_baselines).

    /**
     * The persistence gate of THIS manager (the singleton reads the same field the
     * historic static gate read; an isolated manager honors its own pin, see
     * the store's own durability).
     */
    private boolean durableWritesEnabled() {
        return FeConstants.enableInternalSchemaDb && persistToTable;
    }

    private static boolean persistenceEnabled() {
        return FeConstants.enableInternalSchemaDb && INSTANCE.persistToTable;
    }

    /**
     * Loads all baselines from the internal table and rebuilds the in-memory hash index.
     * Idempotent. Only the persisted SCALARS of each row are decoded: the transient
     * parameterized trees are rebuilt LAZILY, the moment a row enters the per-bindSql match
     * cache (see deepenPersistedRow) - a load keeps the hash index, and parsing every row's
     * SQL was its dominant cost. After loading, the id generator continues after
     * the largest persisted id.
     */
    public void loadFromInternalTable() {
        if (!persistence.isDurable()) {
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
     * retry (a retry is NOT a permanent "no baselines" decision: DROP... IF EXISTS would
     * otherwise report success without deleting the durable row).
     */
    private Map<Long, BaselinePlan> readAndPublishPossessingLoadSlot() {
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
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = readPersistedSnapshot();
            } catch (Throwable t) {
                LOG.warn("SPM load baselines from internal table failed (will retry lazily): {}",
                        t.getMessage());
                return null;
            }
            // Converge the same-key twins of a handoff BEFORE the snapshot is settled /
            // published: the durable repair as well as the fence settlement below run I/O
            // and therefore stay outside the state lock (see collapseDuplicateKeyRows).
            repairDuplicateKeyLosers(snapshot);
            // Settle the mutation-fence obligations BEFORE the write lock: the
            // tombstone append is internal-table I/O with its own timeout, and doing
            // it under the store's write lock blocked every concurrent SPM candidate
            // lookup that only needs the read lock.
            final Set<Long> settledFences = resolvePendingMutationFences(snapshot);
            stateLock.writeLock().lock();
            try {
                if (loaded) {
                    return null; // a concurrent load won the race
                }
                if (storeGeneration.get() != generationAtRead) {
                    // an invalidation superseded this snapshot (e.g. a DROP committed
                    // while the read was in flight); keep loaded=false so the next access
                    // retries against the CURRENT table content
                    LOG.warn("SPM baseline load discarded: the store was invalidated while the"
                            + " snapshot was being read (will retry)");
                    return null;
                }
                if (stateVersion != versionAtRead) {
                    // a local mutation (CREATE / DROP / ALTER, or a merged refresh)
                    // published while the snapshot was being read: the snapshot predates it
                    LOG.warn("SPM baseline load discarded: a local mutation published while the"
                            + " snapshot was being read (will retry against the current table)");
                    return null;
                }
                Map<Long, BaselinePlan> visible = publishLoadedSnapshot(snapshot, settledFences);
                loaded = true;
                return visible;
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
     * thread per caller - only the CAS winner inside tryLoadNow performed the
     * read, every other thread exited immediately. Reserving the slot first means a
     * caller that cannot claim it simply returns: the owner releases the slot when its
     * read finishes (readAndPublishPossessingLoadSlot), so the next caller
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
        try {
            loader.start();
        } catch (Throwable t) {
            // the thread never ran, so nothing will release the slot: release it here,
            // otherwise every later ensureLoaded fails its CAS and the store never loads
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
        if (!persistence.isDurable()) {
            loaded = true;
            return;
        }
        if (!(persistence instanceof TablePersistence)) {
            // a substituted store drives its loads explicitly (loadFromInternalTable):
            // never fast-forward loaded / schedule a background load from under it
            return;
        }
        scheduleAsyncLoad();
    }

    /**
     * Strict variant for management operations (CREATE / ALTER / DROP): when the first
     * load has not succeeded (internal table / BE unavailable), fail with a retryable
     * error instead of silently operating on an empty map - DROP... IF EXISTS would
     * otherwise report success without deleting the durable row, which then reappears on
     * the next refresh. Query-side callers keep ensureLoaded's degradation (no match
     * while the store is unavailable).
     */
    private Map<Long, BaselinePlan> ensureLoadedOrThrow() {
        if (!persistence.isDurable()) {
            loaded = true;
            return null;
        }
        Map<Long, BaselinePlan> initialSnapshot = null;
        if (!loaded) {
            // A management operation needs a REAL answer: wait for the in-flight
            // background load (bounded), or perform one inline with the short,
            // purpose-built timeout. Every other management caller coalesces on the same
            // load slot, so an unavailable BE costs ONE bounded attempt, not one per
            // concurrent DDL.
            long deadline = System.currentTimeMillis() + MANAGEMENT_LOAD_WAIT_MILLIS;
            while (!loaded && System.currentTimeMillis() < deadline) {
                if (loadInProgress.compareAndSet(false, true)) {
                    initialSnapshot = readAndPublishPossessingLoadSlot();
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
        return initialSnapshot;
    }

    /**
     * Replaces the match index / cache with the read snapshot (caller holds the write
     * lock). The fence obligations were settled by the caller BEFORE the lock (see
     * readAndPublishPossessingLoadSlot): appending a drop tombstone is internal-table
     * I/O with its own timeout, and holding the store's write lock across it blocked
     * every concurrent SPM candidate lookup. The rows themselves are NOT kept in memory:
     * the snapshot feeds the per-bindSql LRU (existing entries are reconciled with their
     * complete durable row sets; the in-memory store warms every hash) and the hash index.
     * The snapshot rows are decoded SHALLOWLY, so the trees of a row that actually enters a
     * cache entry (add / change) are rebuilt by the merge (see deepenPersistedRow) - the
     * fence settlement / tombstone filter / duplicate collapse / id watermark above need
     * the scalars only.
     */
    private Map<Long, BaselinePlan> publishLoadedSnapshot(Map<Long, BaselinePlan> loadedPlans, Set<Long> fenced) {
        // Pending mutation fences mask contradicting rows here as well: a
        // reload after an invalidation must not republish a row whose dropped / disabled
        // outcome is still owed.
        // No FE may match / show two baselines for one key (see
        // collapseDuplicateKeyRows): the twins a handoff can leave behind collapse to the
        // deterministic owner here - the repair already ran outside the lock, but the
        // loser must leave the published state even when that best-effort repair failed.
        loadedPlans = visibleSnapshotRows(loadedPlans, fenced);
        Map<Long, List<BaselinePlan>> byHash = new HashMap<>();
        long maxId = 0;
        for (BaselinePlan p : loadedPlans.values()) {
            byHash.computeIfAbsent(p.getBindSqlHash(), k -> new ArrayList<>()).add(p);
            maxId = Math.max(maxId, p.getId());
        }
        Map<Long, Long> next = new HashMap<>();
        for (Long hash : byHash.keySet()) {
            next.put(hash, hashMutationSeq.incrementAndGet());
        }
        // A load is an AUTHORITATIVE replacement: every hash gets a fresh epoch above, so the
        // signature table applySnapshot stores is the new baseline for the next diff and its
        // changed-hash set is not needed here. ONE call is enough - the second of the former pair
        // re-diffed against the signatures the first had just written, so it could only ever
        // report "nothing changed" (a full extra pass over every cached row).
        baselineCache.applySnapshot(byHash, !durableHashReadAvailable());
        knownHashes = next;
        // the snapshot read every row: no row above this id can exist undiscovered
        maxPersistedIdSeen = Math.max(maxPersistedIdSeen, maxId);
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        stateVersion++;
        storeGeneration.incrementAndGet();
        LOG.info("SPM loaded {} baselines from internal table", loadedPlans.size());
        return loadedPlans;
    }

    // periodic refresh (cross-FE visibility).

    /**
     * Refreshes GLOBAL state from durable storage. Reads raw pages outside writerLock and stateLock, then
     * serializes resolution and repair with writers. Publication checks the captured generation and state version
     * so a stale snapshot does not replace newer local state. This path can perform leadership-gated durable
     * repairs and mutation settlement; it is not purely read-only.
     */
    public void refreshFromInternalTable() {
        if (!durableWritesEnabled()) {
            return; // internal schema db disabled (or a unit test): nothing to merge
        }
        if (cluster.isLeader()) {
            maybePruneExpiredMutationWindows();
        }
        if (!ensureLoadedForRefresh()) {
            return;
        }
        long versionAtRead;
        long generationAtRead;
        stateLock.readLock().lock();
        try {
            if (!loaded) {
                return; // raced with a concurrent load / reload: retry next cycle
            }
            versionAtRead = stateVersion;
            // Capture the store GENERATION as well (see readAndPublishPossessingLoadSlot):
            // a promotion reload / invalidation that lands while the snapshot is being read
            // makes it stale - it may still contain rows deleted by a DROP that completed in
            // between - so the publication below must be rejected instead of resurrecting
            // them. The read is retried against the CURRENT table content next cycle.
            generationAtRead = storeGeneration.get();
        } finally {
            stateLock.readLock().unlock();
        }
        // The READ itself (one paginated query per page) runs OUTSIDE the writer lock: it
        // is a pure read of the durable table, so holding the writers' lock across the
        // whole page loop only made a big table block every local baseline mutation on this
        // FE. Its side-effect-free half (readRawRefreshSnapshot) is safe to run there.
        final RawRefreshSnapshot raw;
        try {
            raw = readRawRefreshSnapshot();
        } catch (Throwable t) {
            LOG.warn("SPM baseline refresh read failed (will retry next cycle): {}", t.getMessage());
            return;
        }
        // Only the REPAIR half of the read (the tombstone match and its best-effort delete)
        // and the PUBLICATION are serialized with the WRITERS (writerLock, never stateLock).
        // The repair must stay here: running it outside would let a concurrent DROP /
        // revive interleave between the tombstone read and the repair delete.
        synchronized (writerLock) {
            if (!loaded) {
                return; // a concurrent reload unpublished the store: retry next cycle
            }
            if (storeGeneration.get() != generationAtRead) {
                LOG.debug("SPM baseline refresh skipped: the store was invalidated while reading");
                return;
            }
            final Map<Long, BaselinePlan> snapshot;
            try {
                snapshot = resolveRefreshSnapshot(raw);
            } catch (Throwable t) {
                LOG.warn("SPM baseline refresh read failed (will retry next cycle): {}", t.getMessage());
                return;
            }
            // The version guard is what makes a snapshot read without the writer lock
            // harmless: every local mutation flips its in-memory row AND bumps stateVersion
            // inside ONE stateLock write section, and a mutation persists durably BEFORE
            // that section, so
            // a snapshot read that STARTED after the bump sees the durable write, and
            // a snapshot read that started BEFORE it is rejected here, or - when the
            //   mutation has not published yet - applied first and then overwritten by the
            //   mutation's own (newer) publication.
            // Matching queries never take writerLock; cold reads use stateLock for publication validation.
            if (!applyRefreshedSnapshotIfUnchanged(versionAtRead, snapshot)) {
                LOG.debug("SPM baseline refresh skipped: local state changed while reading");
            }
        }
    }

    private void maybePruneExpiredMutationWindows() {
        long now = mutationWindowNow();
        long next = nextMutationWindowSweepMillis.get();
        if (now < next || !nextMutationWindowSweepMillis.compareAndSet(
                next, now + MUTATION_WINDOW_MAX_AGE_SECONDS * 1000L)) {
            return;
        }
        try {
            pruneExpiredMutationWindows();
        } catch (RuntimeException e) {
            LOG.debug("SPM expired mutation window sweep skipped: {}", e.getMessage());
        }
    }

    void pruneExpiredMutationWindows() {
        long[] expired = persistence.expiredMutationWindowState();
        long watermark = expired[0];
        long tick = expired[1];
        if (tick == 0) {
            return;
        }
        if (persistence.coveringCompletedTick(watermark) <= tick) {
            // A crashed writer can leave its window as the only copy of either high-water mark.
            long completionTick = lastMutationTick.accumulateAndGet(Math.max(nextMutationTick(), tick + 1), Math::max);
            writeHwmRecord(watermark, completionTick, 0, true);
            if (persistence.coveringCompletedTick(watermark) <= tick) {
                return;
            }
        }
        persistence.pruneExpiredMutationWindows(watermark, tick);
    }

    /**
     * Makes sure the store is loaded before a refresh reads a snapshot: the FIRST load
     * doubles as the first refresh (it downloads the whole table), so the caller returns
     * without a second read afterwards.
     *
     * Returns: whether the store is loaded and the caller should continue with a refresh
     */
    private boolean ensureLoadedForRefresh() {
        synchronized (writerLock) {
            if (loaded) {
                return true;
            }
            loadFromInternalTable();
            return false;
        }
    }

    /**
     * CONFIRMED post-forward refresh for the CREATE / ALTER / DROP hooks
     * (afterForwardToMaster): the DDL already committed on the master, so this FE
     * must publish a state that INCLUDES it (or fail retryably) before the statement
     * returns. refreshFromInternalTable is best-effort and has two holes:
     *
     * loaded == false: an older load (started BEFORE the DDL) may hold the
     *   loadInProgress slot; the best-effort refresh returns immediately and the
     *   pre-DDL snapshot can publish afterwards - a CREATE stays invisible, a DROP /
     *   disable keeps replaying locally until the daemon refresh;
     * loaded == true: a transient read failure is swallowed and the stale rows
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
         * SPMPlanner.canonicalPlanDigest): the bind digest identifies the binding but
         * NOT the plan text, and two baselines may share one bind digest while
         * carrying different plan texts. "" = not computable here (the confirmation
         * falls back to the bind digest alone, as before the column existed).
         */
        private final String createdPlanDigest;
        /**
         * The CURRENT bind-side schema fingerprint of the submitted bind SQL (see
         * SPMPlanTreeSupport.schemaFingerprint), computed on the follower at forward
         * time. A row that matches the bind / plan identity but whose STORED fingerprint
         * no longer contains these entries is STALE: after a schema change the repeated
         * CREATE retires it and writes a new row under the current fingerprint, and
         * confirming the old row republished a baseline whose replay the schema guard
         * then rejects. "" = not computable here (the check is skipped).
         */
        private final String createdSchemaFingerprint;

        private ForwardedDdlExpectation(long id, BaselineStatus status) {
            this(id, status, null, null, "", "", "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql) {
            this(0, null, createdBindSql, createdPlanSql, "", "", "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, "", "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId, String createdBindDigest) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, createdBindDigest,
                    "", "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId, String createdBindDigest, String createdPlanDigest) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, createdBindDigest,
                    createdPlanDigest, "");
        }

        private ForwardedDdlExpectation(String createdBindSql, String createdPlanSql,
                String createdQueryId, String createdBindDigest, String createdPlanDigest,
                String createdSchemaFingerprint) {
            this(0, null, createdBindSql, createdPlanSql, createdQueryId, createdBindDigest,
                    createdPlanDigest, createdSchemaFingerprint);
        }

        private ForwardedDdlExpectation(long id, BaselineStatus status, String createdBindSql,
                String createdPlanSql, String createdQueryId, String createdBindDigest,
                String createdPlanDigest, String createdSchemaFingerprint) {
            this.id = id;
            this.status = status;
            this.createdBindSql = createdBindSql;
            this.createdPlanSql = createdPlanSql;
            this.createdQueryId = createdQueryId == null ? "" : createdQueryId;
            this.createdBindDigest = createdBindDigest == null ? "" : createdBindDigest;
            this.createdPlanDigest = createdPlanDigest == null ? "" : createdPlanDigest;
            this.createdSchemaFingerprint = createdSchemaFingerprint == null
                    ? "" : createdSchemaFingerprint;
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
         * Parameter bindSql: the forwarded CREATE's bind SQL
         * Parameter planSql: the forwarded CREATE's plan SQL
         * Returns: the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql) {
            return created(bindSql, planSql, "");
        }

        /**
         * As created(String, String) plus the STATEMENT query id of the
         * forwarded CREATE. The master executes the forwarded statement
         * under THIS id (the forward carries ctx.queryId and the master's
         * execution context adopts it, see FEOpExecutor #buildStmtForwardParams), and the
         * CREATE stores DebugUtil.printId(ctx.queryId) on the row - so the
         * follower, which still sees its own (pre-adoption) query id here, can identify
         * the committed row even though the persisted plan text is the DECOMPILED one.
         *
         * Parameter bindSql: the forwarded CREATE's bind SQL
         * Parameter planSql: the forwarded CREATE's plan SQL
         * Parameter statementQueryId: the statement's query id ("" = match the text only)
         * Returns: the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId) {
            return new ForwardedDdlExpectation(bindSql == null ? "" : bindSql,
                    planSql == null ? "" : planSql, statementQueryId);
        }

        /**
         * As created(String, String, String) plus the CANONICAL BIND DIGEST of the
         * forwarded CREATE (see SPMPlanner.canonicalBindDigest). The digest is the identity
         * every create of this shape shares: an IDEMPOTENT duplicate CREATE returns the
         * master's EXISTING row, whose query id belongs to the FIRST statement and whose
         * raw plan text is the DECOMPILED one - with different whitespace the raw bind
         * text differs as well - so only the canonical digest confirms the durable
         * outcome and can be computed locally (the raw checks remain as a fallback for
         * callers without a digest).
         *
         * Parameter bindSql: the forwarded CREATE's bind SQL
         * Parameter planSql: the forwarded CREATE's plan SQL
         * Parameter statementQueryId: the statement's query id ("" = match the text only)
         * Parameter bindDigest: the canonical bind digest ("" = not available)
         * Returns: the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId, String bindDigest) {
            return created(bindSql, planSql, statementQueryId, bindDigest, "");
        }

        /**
         * As created(String, String, String, String) plus the CANONICAL PLAN DIGEST of
         * the forwarded CREATE (see SPMPlanner.canonicalPlanDigest). The bind digest
         * alone does NOT identify the row: two CREATEs may share the binding while
         * carrying different plan texts (one baseline per plan), and the plan text
         * persisted by the master is the DECOMPILED one, so the submitted text cannot
         * be compared. The submitted plan's canonical digest can be computed locally and
         * equals the digest the master stores for this statement's own row; a row whose
         * bind digest agrees but whose plan digest differs belongs to the OTHER plan and
         * must not confirm this statement. Empty = fall back to the bind digest alone.
         *
         * Parameter bindSql: the forwarded CREATE's bind SQL
         * Parameter planSql: the forwarded CREATE's plan SQL
         * Parameter statementQueryId: the statement's query id ("" = match the text only)
         * Parameter bindDigest: the canonical bind digest ("" = not available)
         * Parameter planDigest: the canonical plan digest ("" = not available)
         * Returns: the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId, String bindDigest, String planDigest) {
            return created(bindSql, planSql, statementQueryId, bindDigest, planDigest, "");
        }

        /**
         * As created(String, String, String, String, String) plus the CURRENT bind-side
         * schema fingerprint of the submitted bind SQL (see
         * SPMPlanner.canonicalSchemaFingerprint). The digest identity alone cannot tell
         * two rows of IDENTICAL SQL apart: after a schema change the repeated CREATE
         * retires the old row and writes a new one under the current fingerprint, and
         * while only the OLD row is visible (its retirement / replacement not published
         * yet) the digests still match - confirming it republished a baseline whose
         * replay the schema guard rejects.
         *
         * Parameter bindSql: the forwarded CREATE's bind SQL
         * Parameter planSql: the forwarded CREATE's plan SQL
         * Parameter statementQueryId: the statement's query id ("" = match the text only)
         * Parameter bindDigest: the canonical bind digest ("" = not available)
         * Parameter planDigest: the canonical plan digest ("" = not available)
         * Parameter schemaFingerprint: the current bind-side fingerprint ("" = not available)
         * Returns: the presence expectation
         */
        public static ForwardedDdlExpectation created(String bindSql, String planSql,
                String statementQueryId, String bindDigest, String planDigest,
                String schemaFingerprint) {
            return new ForwardedDdlExpectation(bindSql == null ? "" : bindSql,
                    planSql == null ? "" : planSql, statementQueryId, bindDigest,
                    planDigest, schemaFingerprint);
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
                for (BaselinePlan row : snapshot.values()) {
                    if (!matchesCreatedIdentity(row)) {
                        continue;
                    }
                    if (!createdSchemaFingerprint.isEmpty()
                            && !SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                                    row.getSchemaFingerprint(), createdSchemaFingerprint)) {
                        // The row matches the statement's identity but its STORED
                        // fingerprint no longer describes the current metadata: after a
                        // schema change the repeated CREATE retires this row and writes
                        // the replacement under the CURRENT fingerprint, and confirming
                        // the stale row republished a baseline whose replay the schema
                        // guard rejects until the next refresh. Wait for the new row.
                        continue;
                    }
                    return true;
                }
                return false;
            }
            BaselinePlan row = snapshot.get(id);
            return status == null ? row == null : row != null && row.getStatus() == status;
        }

        /**
         * Whether one row carries the identity this CREATE persisted (see the factory
         * docs). The BIND digest identifies the binding; the PLAN digest - required
         * whenever it is computable here - separates two baselines that share the
         * binding while carrying different plan texts (one baseline per plan). A row
         * whose plan digest is absent / empty therefore does NOT confirm: it cannot be
         * attributed to this statement's plan. The raw arm covers the frozen fallback,
         * whose stored text is the submitted one.
         */
        private boolean matchesCreatedIdentity(BaselinePlan row) {
            if (!createdBindDigest.isEmpty()
                    && createdBindDigest.equals(row.getBindSqlDigest())) {
                if (createdPlanDigest.isEmpty()) {
                    return true; // the plan digest is not computable here: bind identity
                }
                return createdPlanDigest.equals(row.getPlanSqlDigest());
            }
            if (!createdBindSql.equals(row.getBindSql())) {
                return false;
            }
            // The master persists the DECOMPILED plan text for an ordinary
            // (non-fallback) CREATE, so the submitted planSql only matches when the raw
            // fallback was frozen. The statement's QUERY ID survives every freezing
            // choice: the forward carries this statement's ctx.queryId to the master,
            // whose execution context adopts it, and the CREATE stores it as the row's
            // query_id.
            boolean queryIdUsable = !createdQueryId.isEmpty()
                    && !"NaN".equals(createdQueryId);
            return createdPlanSql.equals(row.getPlanSql())
                    || (queryIdUsable && createdQueryId.equals(row.getQueryId()));
        }
    }

    /**
     * As refreshAfterForwardedDdl, with the forwarding statement's context: the
     * journal synchronization below talks to the master through it.
     *
     * Parameter ctx: the context of the statement that was forwarded (may be null in tests)
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
     * Parameter ctx: the forwarded statement's context (may be null in tests)
     * Parameter expected: the durable outcome to confirm, or null to skip the confirmation
     */
    public void refreshAfterForwardedDdl(ConnectContext ctx, ForwardedDdlExpectation expected) {
        if (!persistence.isDurable()) {
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
                    // earlier stored instant, so pickDurableWinner keeps this outcome - and
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
     * Synchronizes FE metadata before a forwarded-DDL callback computes its schema fingerprint. A journal
     * synchronization failure propagates so the caller does not build an expectation from metadata known to be
     * unsynchronized. Internal-table row visibility is checked by the subsequent baseline readback.
     */
    public void syncWithMasterForDdlCallback(ConnectContext ctx) {
        syncJournalWithMaster(ctx);
    }

    /**
     * Delegates FE journal synchronization to the cluster gateway. The live gateway can skip synchronization for a
     * null context or the leader, and otherwise synchronizes with the master. Baseline internal-table rows require
     * separate visibility checks.
     */
    private void syncJournalWithMaster(ConnectContext ctx) {
        cluster.syncJournalWithMaster(ctx);
    }

    /**
     * Applies a snapshot read by a refresh IF no local mutation published since the read
     * started (the version guard). The guard is what makes a stale snapshot harmless:
     * updateStatus publishes its in-memory flip and its version bump before a refresh can
     * reach this point, so the older row read before that update is rejected instead of
     * overwriting the newer status. Public for unit tests; production callers use
     * refreshFromInternalTable, which additionally serializes with the writers.
     *
     * Parameter versionAtRead: the state version observed when the snapshot read started
     * Parameter persisted: the snapshot to apply
     * Returns: whether the snapshot was applied
     */
    public boolean applyRefreshedSnapshotIfUnchanged(long versionAtRead,
            Map<Long, BaselinePlan> persisted) {
        // Converge the same-key twins outside the lock (the repair I/O must not run under
        // the state lock; see collapseDuplicateKeyRows), then settle the fence
        // obligations (see publishLoadedSnapshot) and rebuild the replacement SQL trees
        // BEFORE any lock is taken (see deepenRefreshedRowsOutsideLocks).
        repairDuplicateKeyLosers(persisted);
        persisted = deepenRefreshedRowsOutsideLocks(persisted);
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
     * The current publication version: advanced by local mutations and every accepted snapshot, even when
     * content signatures are unchanged. Public for unit tests of the stale-snapshot guard.
     *
     * Returns: the state version
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
     * ids only present in the snapshot are added (created on another FE);
     * ids only present in memory are removed (dropped on another FE);
     * ids present in both with changed persisted content (e.g. status changed by an ALTER
     *   on another FE) are replaced; unchanged rows keep their current object, so instance
     *   identity and the transient parameterized trees stay intact;
     * the id generator is advanced past the largest persisted id.
     *
     * The snapshot is authoritative; callers must ensure it was read while no local mutation
     * was in flight (see refreshFromInternalTable). Public for unit tests.
     */
    public Map<Long, BaselinePlan> applyRefreshedBaselines(Map<Long, BaselinePlan> persisted) {
        // Converge the same-key twins outside the lock (the repair I/O must not run under
        // the state lock; see collapseDuplicateKeyRows), then settle the fence obligations
        // (see publishLoadedSnapshot) and rebuild the replacement SQL trees BEFORE any lock
        // is taken (see deepenRefreshedRowsOutsideLocks).
        repairDuplicateKeyLosers(persisted);
        persisted = deepenRefreshedRowsOutsideLocks(persisted);
        Set<Long> fenced = resolvePendingMutationFences(persisted);
        stateLock.writeLock().lock();
        try {
            return applyRefreshedBaselinesLocked(persisted, fenced);
        } finally {
            stateLock.writeLock().unlock();
        }
    }

    /**
     * Rebuilds the TRANSIENT SQL trees of the snapshot rows that will REPLACE a cached row
     * (or be inserted) BEFORE any cache / publication lock is taken.
     *
     * The merge used to reparse them under the cache monitor (mergeEntry ->
     * deepenPersistedRow), and a follower refreshing many disabled cached baselines rebuilt
     * their SQL serially under the very monitor every warm query needs (getComplete),
     * stalling unrelated hashes before their rewrite deadline could be rechecked. A row
     * whose change is a persisted SCALAR only (status / timing / cost / query id) reuses the
     * cached object's trees in place (see mergeEntry / persistedTreesChanged) and is not
     * deepened here. The snapshot version guard (applyRefreshedSnapshotIfUnchanged) still
     * validates the publish under the lock; the merge re-evaluates every row against the
     * LIVE cache, so a pre-deepened row that became unchanged meanwhile is simply
     * discarded.
     *
     * The replacements are written into a COPY: the snapshot may be an immutable map (a
     * caller assembling it by hand, as the unit tests do), and only the caller's own
     * reference is replaced - nothing observable is mutated before the publication lock.
     */
    private Map<Long, BaselinePlan> deepenRefreshedRowsOutsideLocks(
            Map<Long, BaselinePlan> persisted) {
        if (!persistence.isDurable() || persisted.isEmpty()) {
            return persisted;
        }
        Map<Long, BaselinePlan> cached = baselineCache.cachedRowRefs(persisted.keySet());
        boolean warmAll = !durableHashReadAvailable();
        Map<Long, BaselinePlan> deepenedRows = null;
        for (Map.Entry<Long, BaselinePlan> entry : persisted.entrySet()) {
            BaselinePlan current = cached.get(entry.getKey());
            if (current != null && !persistedTreesChanged(current, entry.getValue())) {
                continue; // the merge adopts the scalars into the cached object
            }
            if (current == null && !warmAll
                    && !baselineCache.hasHash(entry.getValue().getBindSqlHash())) {
                // The hash has NO cache entry: outside the in-memory store the snapshot merge
                // never inserts one (applySnapshot's warmAll=false path only reconciles EXISTING
                // entries), so a rebuilt tree would be discarded right here - while the reparse
                // itself runs on every refresh, for every cold baseline, holding the refresh's
                // writer lock and delaying GLOBAL DDL, with Java parsing outside the internal
                // SELECT timeouts. A production snapshot carries thousands of cold
                // rows, so keep them SHALLOW: the scalars are all matching needs, and the row is
                // deepened when a warm path actually admits it.
                continue;
            }
            BaselinePlan deepened = deepenPersistedRow(entry.getValue());
            if (deepened != null && deepened != entry.getValue()) {
                if (deepenedRows == null) {
                    deepenedRows = new HashMap<>(persisted);
                }
                deepenedRows.put(entry.getKey(), deepened);
            }
        }
        return deepenedRows == null ? persisted : deepenedRows;
    }

    /**
     * Converges the SAME-KEY TWINS a leader handoff can leave behind: an already-demoted
     * master's forwarded CREATE can commit right after its last leadership check while the
     * promoted master completes the same key under its own id - its MAX(id) read happened
     * before the old reservation landed, so each FE's by-id collision probe saw only its
     * own id and BOTH enabled rows were durable under one key. Every publish path routes
     * the snapshot through here: the deterministic owner is the LATER allocation (the
     * larger id - the allocation order every writer agrees on), the other row(s) leave the
     * published map so no FE matches / shows two baselines for one key, and a master
     * additionally repairs the losers away durably (a follower read cannot write; the next
     * master-side publish converges - until then the loser stays hidden by this collapse,
     * never resurrected).
     *
     * Only rows with an identical (bind_sql_digest, plan_sql, schema_fingerprint) collapse:
     * a different fingerprint is a DIFFERENT incarnation (the staleness rules own it), and
     * rows a completed DROP covered were already removed by the tombstone filter
     * (filterResurrectedRowsById runs before this collapse).
     */
    private static Map<Long, BaselinePlan> collapseDuplicateKeyRows(
            Map<Long, BaselinePlan> snapshot) {
        if (snapshot == null || snapshot.size() < 2) {
            return snapshot;
        }
        Map<String, List<BaselinePlan>> groups = null;
        for (BaselinePlan row : snapshot.values()) {
            if (row.getBindSqlDigest() == null || row.getPlanSql() == null) {
                continue;
            }
            String key = row.getBindSqlDigest() + '\u0000' + row.getPlanSql() + '\u0000'
                    + (row.getSchemaFingerprint() == null ? "" : row.getSchemaFingerprint());
            if (groups == null) {
                groups = new HashMap<>();
            }
            groups.computeIfAbsent(key, k -> new ArrayList<>()).add(row);
        }
        if (groups == null) {
            return snapshot;
        }
        Map<Long, BaselinePlan> collapsed = null;
        for (List<BaselinePlan> group : groups.values()) {
            if (group.size() < 2) {
                continue;
            }
            BaselinePlan owner = group.get(0);
            for (BaselinePlan row : group) {
                if (row.getId() > owner.getId()) {
                    owner = row;
                }
            }
            for (BaselinePlan row : group) {
                if (row.getId() == owner.getId()) {
                    continue;
                }
                if (collapsed == null) {
                    collapsed = new HashMap<>(snapshot);
                }
                collapsed.remove(row.getId());
                LOG.warn("SPM same-key twin of baseline (digest {}) at id {} is superseded"
                                + " by the later allocation at id {}; hiding the loser",
                        row.getBindSqlDigest(), row.getId(), owner.getId());
            }
        }
        return collapsed == null ? snapshot : collapsed;
    }

    /** Best-effort durable repair of the twins hidden by collapseDuplicateKeyRows. */
    private void repairDuplicateKeyLosers(Map<Long, BaselinePlan> snapshot) {
        if (snapshot == null || snapshot.size() < 2) {
            return;
        }
        Map<Long, BaselinePlan> collapsed = collapseDuplicateKeyRows(snapshot);
        if (collapsed == snapshot) {
            return; // no twins
        }
        for (Map.Entry<Long, BaselinePlan> entry : snapshot.entrySet()) {
            if (!collapsed.containsKey(entry.getKey())) {
                repairDuplicateKeyLoser(entry.getValue());
            }
        }
    }

    /** Repairs one hidden twin away durably when this FE may write (see the collapse). */
    private void repairDuplicateKeyLoser(BaselinePlan loser) {
        if (!persistence.isDurable() || !cluster.isLeader()) {
            return; // a follower read cannot write; the next master refresh converges
        }
        try {
            persistDeleteByIdentity(loser);
            // The delete is CONFIRMED, so the loser's own tombstone may be appended NOW
            // (never before: an unproven delete must not hide a live row): a demoted
            // master's delayed status INSERT of the twin ran its precondition against the
            // pre-delete snapshot and can commit AFTER it, and without a marker the
            // revived twin re-materialized the same-key duplicate this repair removed.
            // The WINNER is unaffected - the tombstone names the LOSER's id.
            noteDroppedSeqState(loser);
            LOG.warn("SPM repaired the same-key twin of baseline (digest {}) away"
                    + " (superseded id {})", loser.getBindSqlDigest(), loser.getId());
        } catch (RuntimeException e) {
            LOG.warn("SPM could not repair the same-key twin of baseline (digest {}, id {})"
                    + " (retried on the next refresh): {}",
                    loser.getBindSqlDigest(), loser.getId(), e.getMessage());
        }
    }

    /** Applies a snapshot; the caller must hold the write lock (see refreshFromInternalTable). */
    private Map<Long, BaselinePlan> applyRefreshedBaselinesLocked(Map<Long, BaselinePlan> persisted,
            Set<Long> fenced) {
        // Never merge two rows of one key into the cache (see collapseDuplicateKeyRows;
        // the durable repair ran before the lock).
        Map<Long, BaselinePlan> visible = visibleSnapshotRows(persisted, fenced);
        Map<Long, List<BaselinePlan>> byHash = new HashMap<>();
        long maxId = 0;
        for (BaselinePlan row : visible.values()) {
            byHash.computeIfAbsent(row.getBindSqlHash(), k -> new ArrayList<>()).add(row);
            maxId = Math.max(maxId, row.getId());
        }
        Map<Long, Long> previous = knownHashes;
        Map<Long, Long> next = new HashMap<>();
        int added = 0;
        int removed = 0;
        for (Long hash : byHash.keySet()) {
            Long seq = previous.get(hash);
            next.put(hash, seq == null ? hashMutationSeq.incrementAndGet() : seq);
            if (seq == null) {
                added++;
            }
        }
        for (Long hash : previous.keySet()) {
            if (!next.containsKey(hash)) {
                removed++;
            }
        }
        // Exact cache changes and signature changes advance the affected hash epochs. Equal signatures can
        // collide, so the global publication version and generation advance independently below.
        ApplyResult applied = baselineCache.applySnapshot(byHash, !durableHashReadAvailable());
        for (Long hash : applied.changedHashes) {
            if (next.containsKey(hash)) {
                next.put(hash, hashMutationSeq.incrementAndGet());
            }
        }
        knownHashes = next;
        if (maxId >= idGenerator.get()) {
            idGenerator.set(maxId + 1);
        }
        stateVersion++;
        storeGeneration.incrementAndGet();
        if (added > 0 || removed > 0 || !applied.changedHashes.isEmpty()) {
            LOG.info("SPM baseline refresh applied: added={}, removed={}, changedHashes={},"
                    + " cacheRowsChanged={}, knownHashes={}", added, removed,
                    applied.changedHashes.size(), applied.cacheRowsChanged, knownHashes.size());
        }
        return visible;
    }

    /**
     * Records (or refreshes) the fence of one completed / attempted local mutation (see
     * pendingMutationFences).
     *
     * Parameter id: the baseline id
     * Parameter expectedStatus: the durable status the write must reach (null =
     *                                    the row must become absent)
     * Parameter attemptedUpdateTimeMicros: the stored instant the write attempted (0 when
     *                                    unknown, e.g. a delete or a forwarded DDL)
     */
    private void recordPendingMutationFence(long id, BaselineStatus expectedStatus,
            long attemptedUpdateTimeMicros) {
        pendingMutationFences.put(id, new PendingMutationFence(expectedStatus,
                System.currentTimeMillis(), attemptedUpdateTimeMicros));
    }

    /**
     * As recordPendingMutationFence, for a write this FE PROVED committed (see the
     * confirmed flag): the fence is exempt from the age bound, so a long publication lag
     * can never unmask the id and republish the pre-mutation state.
     *
     * Parameter id: the baseline id
     * Parameter expectedStatus: the durable status the committed write carries
     * Parameter attemptedUpdateTimeMicros: the stored instant the write carries
     */
    private void recordConfirmedMutationFence(long id, BaselineStatus expectedStatus,
            long attemptedUpdateTimeMicros) {
        pendingMutationFences.put(id, new PendingMutationFence(expectedStatus,
                System.currentTimeMillis(), attemptedUpdateTimeMicros, null, true));
    }

    /**
     * Removes the mutation fence of one id once a LATER statement's outcome was confirmed
     * (see refreshAfterForwardedDdl): the fence described a write whose publication was
     * still unresolved, but the confirmed statement ran AFTER it and its stored instant is
     * strictly later, so the fence can no longer win the durable pick - keeping it would
     * only mask the id (and hold the published cache at the pre-DDL state) until the
     * fence's own bound expired.
     *
     * Parameter id: the id the confirmed expectation belongs to (0 = none)
     * Parameter snapshot: the snapshot that satisfied the expectation (not modified)
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
     * and its stored instant is strictly later than everything the read below can see - so
     * a durable read that happens to MATCH the requested status is NOT the final state:
     * accepting it would publish a status the fenced write (or the next refresh) reverts.
     * The ALTER must instead fail retryably until the fence resolves.
     *
     * The SHARED mutation clock is consulted as well: the registry above only
     * knows THIS FE's unresolved writes, while another FE's row statement may be in flight
     * right now - its open window is exactly what tells a MATCHING read apart from a final
     * one. Master A's DISABLE can be dispatched after a handoff (its forwarded INSERT runs
     * on the new master) and commit with a newer update_time than the ENABLED row the new
     * master just accepted; the pending marker A wrote BEFORE its statement is the only
     * shared evidence that its outcome is still open, so a matching read must not be
     * accepted while the clock reports one. Fail CLOSED and retry (the window is one
     * statement long, and an abandoned one stops poisoning the clock after
     * MUTATION_WINDOW_MAX_AGE_SECONDS).
     *
     * Parameter id: the baseline id
     * Parameter requested: the status this ALTER wants to be durable
     */
    private void requireNoContradictingStatusFence(long id, BaselineStatus requested) {
        requireNoContradictingStatusFence(id, requested, 0L);
    }

    /**
     * As requireNoContradictingStatusFence(long, BaselineStatus), also accepting a flip
     * that ORDERS ITSELF ABOVE the unresolved intent.
     *
     * Failing the ALTER is the safe answer only when the pending write could still WIN the
     * durable pick. A flip whose own stored instant is STRICTLY LATER than the instant the
     * fenced write carries does win it - pickDurableWinner keeps the later row - so its
     * caller's status is the durable outcome once both publish, and refusing it would only make
     * every ordinary CREATE / ENABLE / DISABLE / ENABLE sequence fail once for no reason
     * 's "order them above those intents"). Only an instant that is EARLIER OR EQUAL
     * (a full tie resolves DISABLED-first) stays refusable.
     *
     * Parameter id: the baseline id
     * Parameter requested: the status this ALTER wants to be durable
     * Parameter plannedUpdateTimeMicros: the stored instant this statement is about to write
     *                              (0 = unknown, never ordered above the intent)
     */
    private void requireNoContradictingStatusFence(long id, BaselineStatus requested,
            long plannedUpdateTimeMicros) {
        PendingMutationFence fence = pendingMutationFences.get(id);
        if (fence != null && fence.expectedStatus != requested
                && plannedUpdateTimeMicros <= fence.attemptedUpdateTimeMicros) {
            throw new IllegalStateException("SPM cannot confirm the durable status of"
                    + " baseline " + id + "; an earlier write of this id is still"
                    + " unresolved - retry the ALTER");
        }
        if (fence != null && fence.expectedStatus != requested) {
            // this statement's row is strictly later than the unresolved write, so it is the
            // durable winner whatever that write finally publishes
            return;
        }
        if (plannedUpdateTimeMicros > 0) {
            // A REAL flip stops here: the durable read that selected this branch proved the
            // stored status is the OPPOSITE of the requested one, so a write that is still in
            // FLIGHT elsewhere cannot make this statement a silent no-op - and blocking on the
            // shared clock HERE would refuse a flip that has already been ordered against a
            // QUIET read (see readNewestStoredUpdateMicrosAroundQuietClock), which is the only
            // part of the fence a flip needs: its instant is newer than every DURABLE row of the
            // id, so the row it writes is the durable winner whatever a foreign write publishes.
            //
            // "already DURABLE" is exactly what the bare keyed read could miss: a
            // committed row that no read shows yet - the old leader's write left behind by a
            // handoff - is invisible to it, so the instant it returned could be OLDER than that
            // hidden row and the flip it ordered would lose the durable pick to a write nothing
            // here ever saw (the successor's successful ENABLE reverted to DISABLED by the next
            // refresh).  closed the remaining hole: the hidden row of an ORDINARY foreign
            // flip carries a stored instant this FE's wall clock may lag behind, so the quiet
            // bracket above now guards EVERY flip - an open window fails retryably before this
            // method is even reached with an instant computed from an incomplete read.
            return;
        }
        if (!sharedMutationClockAvailable()) {
            return;
        }
        SnapshotFence clock;
        try {
            clock = readSnapshotFence();
        } catch (Exception e) {
            throw new IllegalStateException("SPM cannot read the shared mutation clock of"
                    + " baseline " + id + ", so a concurrent write of the baseline table"
                    + " cannot be ruled out - retry the ALTER", e);
        }
        if (!clock.quiet()) {
            throw new IllegalStateException("SPM cannot confirm the durable status of"
                    + " baseline " + id + ": a mutation of the baseline table is still in"
                    + " flight (" + clock + ") - retry the ALTER");
        }
    }

    /**
     * Whether the SHARED mutation clock can be consulted at all. It lives in the internal
     * table (SELECT_CLOCK_SQL), which only fences a caller whose durable protocol really
     * goes through that table: a process with persistence disabled has no durable status a
     * concurrent writer could contradict, and a SCRIPTED store stands in for the whole
     * protocol (the same convention beginRowMutation uses), so neither may be answered
     * with a read of a table that does not exist there. The dedicated clock seam opts IN
     * to the check.
     */
    private boolean sharedMutationClockAvailable() {
        return persistence.isDurable();
    }

    /**
     * Records the ABSENCE fence of a DROP whose DELETE outcome is UNCONFIRMED
     * the id is masked locally until a snapshot shows the row gone, and
     * the removed IDENTITY travels with the fence so the deletion marker
     * is appended only AFTER absence is proven - a marker written for an uncommitted
     * delete would make every later load treat the still-live row as deleted (hiding it
     * and re-issuing the delete), silently completing a DROP that reported failure.
     *
     * Parameter removed: the row whose delete is unresolved
     */
    private void recordPendingAbsenceFence(BaselinePlan removed) {
        pendingMutationFences.compute(removed.getId(), (id, prior) ->
                prior != null && prior.confirmed && prior.droppedIdentity != null ? prior
                        : new PendingMutationFence(null, System.currentTimeMillis(), 0, removed));
    }

    /**
     * Records the ABSENCE fence of a DROP whose DELETE this FE PROVED committed - the exact
     * counterpart of recordConfirmedMutationFence for a deletion.
     *
     * The DROP's DELETE returned OK, so the transaction committed and the row is gone
     * IRREVERSIBLY: only its PUBLICATION can still lag a local read. That is the same state an
     * ALTER is in after its conditional UPDATE reported its row (recordConfirmedMutationFence),
     * and it must be fenced the same way - CONFIRMED, i.e. EXEMPT from the age bound:
     *
     * An EXPIRING fence was dropped after PENDING_MUTATION_FENCE_MILLIS, and the next refresh
     *   then re-applied the still-unreadable pre-DROP row: a DROP the master reported successful
     *   was silently replayed while the deleted row was still in every local read
     *   (noteDroppedSeqState keeps the row unusable, but the mask is what keeps the FE's
     *   cache at "dropped" instead of re-publishing it).
     * The fence carries NO droppedIdentity: the tombstone of the removed row was already
     *   appended on the success path (noteDroppedSeqState / noteDroppedIdState), and
     *   resolvePendingMutationFences must not append it a second time.
     *
     * Only an outcome the snapshot shows (the id absent) removes it.
     *
     * Parameter id: the dropped baseline id
     */
    private void recordConfirmedAbsenceFence(long id) {
        pendingMutationFences.put(id, new PendingMutationFence(null,
                System.currentTimeMillis(), 0, null, true));
    }

    /**
     * Retains a forwarded GLOBAL DDL's expectation as a mutation fence so
     * every LATER read on this FE keeps masking a row that contradicts it (see
     * pendingMutationFences). DROP / ALTER fenced by id; the CREATE's identity
     * expectation is NOT id-keyed (the follower never learned the id) - its
     * failure path already invalidates the store, and the daemon converges later.
     *
     * The fence is CONFIRMED, not pending: every caller runs after the MASTER reported
     * the DDL successful (afterForwardToMaster), so the outcome IS durable and only its
     * publication on this FE lags. An EXPIRING fence was dropped after
     * PENDING_MUTATION_FENCE_MILLIS and the next refresh republished the pre-DDL row - a
     * DISABLE / DROP the master completed was then replayed while this FE kept serving
     * the old row. The confirmed fence lives until a snapshot
     * shows the outcome (or a later winner supersedes it), the same rule a locally proven
     * write gets.
     */
    private void recordForwardedDdlFence(ForwardedDdlExpectation expected) {
        if (expected != null && expected.getId() > 0) {
            recordConfirmedMutationFence(expected.getId(), expected.getStatus(), 0);
        }
    }

    /**
     * Masks stale snapshot rows until their mutation is visible or an unconfirmed fence expires.
     * An observed DROP keeps its fence until both tombstone scopes are durable.
     *
     * Parameter persisted: the snapshot about to be applied (not modified)
     * Returns: the ids masked while applying this snapshot
     */
    private Set<Long> resolvePendingMutationFences(Map<Long, BaselinePlan> persisted) {
        if (pendingMutationFences.isEmpty()) {
            resolveHeldMutationWindows(persisted);
            return Collections.emptySet();
        }
        Set<Long> masked = new HashSet<>();
        long now = System.currentTimeMillis();
        for (Map.Entry<Long, PendingMutationFence> entry : pendingMutationFences.entrySet()) {
            long id = entry.getKey();
            PendingMutationFence fence = entry.getValue();
            BaselinePlan row = persisted.get(id);
            if (fence.droppedIdentity != null && (fence.confirmed || fence.isSatisfiedBy(row))) {
                // Even after confirmation, this snapshot may contain a later status INSERT.
                boolean durable = tryConfirmObservedDrop(id, fence);
                if (!durable || row != null) {
                    masked.add(id);
                }
                continue;
            }
            if (fence.isSatisfiedBy(row)) {
                pendingMutationFences.remove(id, fence);
                continue;
            }
            if (fence.isExpired(now)) {
                LOG.warn("SPM pending mutation fence for baseline {} expired before its"
                        + " durable outcome became visible; the persisted state wins",
                        id);
                pendingMutationFences.remove(id, fence);
                continue;
            }
            masked.add(id);
        }
        resolveHeldMutationWindows(persisted);
        return masked;
    }

    /** Confirms both DROP tombstones while the proven absence remains fenced. */
    private boolean confirmObservedDrop(long id, PendingMutationFence fence) {
        synchronized (writerLock) {
            if (pendingMutationFences.get(id) != fence) {
                return false;
            }
            PendingMutationFence confirmed = fence;
            if (!fence.confirmed) {
                confirmed = new PendingMutationFence(null, fence.sinceMillis, 0, fence.droppedIdentity, true);
                if (!pendingMutationFences.replace(id, fence, confirmed)) {
                    return false;
                }
            }
            confirmDropTombstonesDurable(List.of(confirmed.droppedIdentity), id);
            pendingMutationFences.remove(id, confirmed);
            return true;
        }
    }

    private boolean tryConfirmObservedDrop(long id, PendingMutationFence fence) {
        try {
            return confirmObservedDrop(id, fence);
        } catch (RuntimeException e) {
            LOG.warn("SPM keeps baseline {} fenced until its DROP tombstones are readable: {}",
                    id, e.getMessage());
            return false;
        }
    }

    /** Once absence is proven, later rows for this ID must remain hidden during confirmation. */
    private boolean hasObservedDropAwaitingTombstones(long id) {
        PendingMutationFence fence = pendingMutationFences.get(id);
        return fence != null && fence.confirmed && fence.droppedIdentity != null;
    }

    /**
     * Reads the largest visible baseline row id, separately from the reservation high-water mark. CREATE
     * reconciles both before allocating: deleted and reserved-but-unwritten ids remain consumed even when the
     * baseline table's own maximum falls. A read failure propagates so allocation cannot guess this bound.
     */
    private long readPersistedTableMaxId() {
        return persistence.tableWatermark();
    }

    /**
     * The live implementation half of BaselinePersistence.tableWatermark: the
     * seam-free body of the former readPersistedTableMaxId.
     */
    private static long tableWatermarkFromTable() {
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows = StatisticsUtil.executeQuery(SELECT_MAX_ID_SQL,
                    Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS);
            if (rows == null || rows.isEmpty()) {
                return 0; // an empty table yields one row with a NULL MAX(id)
            }
            return parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(
                    "SPM baseline id watermark read failed (retry the CREATE): "
                            + e.getMessage(), e);
        }
    }

    /**
     * The append-only reservation half of the id watermark (see
     * readPersistedTableMaxId): the sequence table is what keeps the watermark from going
     * BACKWARDS when the row holding the highest id is dropped, so the ALLOCATION mark is
     * the greater of the two - while the dedup GATE must use the table's own MAX(id).
     */
    private long readPersistedIdHighWater() {
        return persistence.sequenceWatermark();
    }

    private long readPersistedWatermark() {
        return Math.max(readPersistedTableMaxId(), readPersistedIdHighWater());
    }

    /**
     * The compact id high-water mark (see
     * SPM_BASELINES_HWM_TABLE): one bounded read. When the record is absent
     * (a cluster upgraded from before the table existed) the LEGACY full read of the
     * append-only history runs ONCE and seeds the compact record, so every later create
     * stays bounded.
     */
    private long readCompactIdWatermark() {
        long hwm = persistence.readHwmRecord();
        if (hwm > 0) {
            // The compact record is NOT authoritative on its own: it, the sequence
            // reservation and the baseline row are SEPARATE writes, and the record's own
            // write is best effort on the seed path. A reservation already VISIBLE
            // beyond the mark therefore confirms the mark against the sequence table
            // before it is trusted. Bounded: see SELECT_SEQ_TAIL_SQL.
            long tail = persistence.readSequenceTail(hwm);
            if (tail > hwm) {
                // best effort, like the seed: raising the record keeps the next create
                // on the bounded path
                writeHwmRecord(tail, nextMutationTick(), 0L, false);
                return tail;
            }
            return hwm;
        }
        long legacy = persistence.readLegacySequenceWatermark();
        if (legacy > 0) {
            // best effort: a failed seed only means the next create reads the history again
            writeHwmRecord(legacy, nextMutationTick(), 0L, false);
        }
        return legacy;
    }

    /** The live implementation half of BaselinePersistence.readHwmRecord. */
    private static long readHwmRecordFromTable() {
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_HWM_SQL, Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows == null || rows.isEmpty()
                    ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id high-water-mark read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
    }

    /** The live implementation half of BaselinePersistence.readLegacySequenceWatermark. */
    private static long readLegacySequenceWatermarkFromTable() {
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_SEQ_ID_SQL, Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows == null || rows.isEmpty()
                    ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id high-water-mark read failed (retry"
                    + " the CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * The highest reservation visible beyond the compact watermark (see
     * readCompactIdWatermark). Fails closed - an unconfirmed allocation source must
     * fail the CREATE retryably rather than let an id be reused.
     */
    private static long readSequenceTailFromTable(long floor) {
        if (!persistenceEnabled()) {
            return 0;
        }
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

    /** The initial random salt of this process (see mutationTickSalt). */
    private static long initialMutationTickSalt() {
        return java.util.concurrent.ThreadLocalRandom.current()
                .nextLong(1L << MUTATION_TICK_FE_BITS) & MUTATION_TICK_FE_MASK;
    }

    /**
     * The sequence-table id key of the tombstone of one baseline id (see
     * TOMBSTONE_SEQ_ID_BASE): a dedicated NEGATIVE id space offset by the baseline id.
     */
    private static long tombstoneSeqId(long baselineId) {
        return TOMBSTONE_SEQ_ID_BASE - baselineId;
    }

    /**
     * Appends (and prunes) the compact id high-water-mark + mutation-clock record.
     *
     * Parameter id: the high-water mark to record
     * Parameter tick: the clock tick to record (see beginRowMutation)
     * Parameter pending: the open mutation window to record (the window's tick, 0 when closed)
     * Parameter strict: whether a failed write must fail the caller (a create that consumed
     *                an id cannot leave the record behind it, and a mutation window may not
     *                open without its clock tick); the seed path is best effort
     */
    private void writeHwmRecord(long id, long tick, long pending, boolean strict) {
        try {
            persistence.appendHwmRecord(id, tick, pending);
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
            persistence.pruneHwmRecords(id, tick);
        } catch (Exception e) {
            LOG.debug("SPM compact high-water-mark prune skipped: {}", e.getMessage());
        }
    }

    /**
     * The slot's (newest watermark, newest tick) - the pre-mutation read of
     * beginRowMutation / endRowMutation (see SELECT_SLOT_STATE_SQL), or the simulator's
     * equivalent.
     *
     * Returns: [watermark, tick]
     */
    private long[] readSlotState() throws Exception {
        return persistence.slotState();
    }

    /** The live implementation half of BaselinePersistence.slotState. */
    private static long[] slotStateFromTable() {
        return readSlotStateFromTable(SELECT_SLOT_STATE_SQL);
    }

    private static long[] readSlotStateFromTable(String sql) {
        if (!persistenceEnabled()) {
            return new long[] {0L, 0L};
        }
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    sql, Collections.emptyMap(),
                    INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return new long[] {0L, 0L};
            }
            return new long[] {parseWatermark(rows.get(0).getWithDefault(0, "")),
                    parseWatermark(rows.get(0).getWithDefault(1, ""))};
        } catch (Exception e) {
            throw new RuntimeException("SPM mutation clock read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
    }

    /**
     * How many READABLE markers carry the open-window tick beginRowMutation just appended (see
     * SELECT_PENDING_TICK_SQL): the confirmation that the window it opens is SHARED before its
     * statement is dispatched - and, because a window identity must be unique cluster-wide, the
     * count of the windows that identity currently names (see tryOpenRowMutationWindow).
     *
     * Parameter tick: the window tick to probe
     * Returns: the number of visible rows carrying this tick in pending
     */
    private long pendingWindowVisible(long tick) throws Exception {
        return persistence.countVisiblePending(tick);
    }

    /** The live implementation half of BaselinePersistence.countVisiblePending. */
    private static long countVisiblePendingFromTable(long tick) {
        if (!persistenceEnabled()) {
            return 0;
        }
        try {
            Map<String, String> params = new HashMap<>();
            params.put("tick", String.valueOf(tick));
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_PENDING_TICK_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows != null && !rows.isEmpty()
                    ? parseWatermark(rows.get(0).getWithDefault(0, "")) : 0;
        } catch (Exception e) {
            throw new RuntimeException("SPM mutation clock read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
    }

    /** The window clock in force (system time in production, see the constructor). */
    private long mutationWindowNow() {
        return mutationWindowClock.getAsLong();
    }

    /**
     * Generates a process-monotonic tick from wall-clock milliseconds and a random FE salt. The salt reduces
     * cross-FE collisions but does not guarantee uniqueness. Window opening and closing probe visible marker
     * counts before relying on tick ownership.
     */
    private long nextMutationTick() {
        long now = (mutationWindowNow() << MUTATION_TICK_FE_BITS)
                | (mutationTickSalt & MUTATION_TICK_FE_MASK);
        long last;
        long next;
        do {
            last = lastMutationTick.get();
            next = Math.max(now, last + 1);
        } while (!lastMutationTick.compareAndSet(last, next));
        return next;
    }

    /**
     * Opens the mutation WINDOW of one durable baseline-row mutation (see
     * SELECT_CLOCK_SQL): a strictly increasing tick with pending set to it is recorded
     * BEFORE the mutation's own statement, and endRowMutation(long) closes the
     * window with a NEWER tick and pending = 0 after it. The snapshot fence requires BOTH
     * fence reads to show pending = 0 AND an unchanged tick tuple: the tick alone only
     * proves the bump was visible before the read, NOT that the row statement between
     * bump and completion had finished - a DROP could bump (page 1 ends on an old
     * flipped row X), let both fence reads pass, and delete X's whole row group between
     * the pages; the id >= X OFFSET n continuation then skipped the removed group's
     * successor while the end probe accepted the mixed snapshot, publishing a status
     * that never existed in the durable table. With the window open ACROSS the
     * statement, any read overlapping the mutation sees pending != 0 (or the completion
     * tick) and retries.
     *
     * Strict: a mutation whose window could not open fails retryably before anything
     * changed. Bounded: one keyed read of the slot + one append + one prune + the
     * visibility probes of the append.
     *
     * The window must also be READABLE before this returns. An append can
     * report SQL OK while its row is still unreadable, and a successor that reads the
     * clock as QUIET while the row statement is ALREADY in flight will accept a MATCHING
     * durable read as final (see requireNoContradictingStatusFence): master A's DISABLE
     * gets SQL OK for pending=tA, its row is invisible, the ALTER is dispatched and
     * outlives A's demotion - the new master loads the old ENABLED row, ALTER... ENABLE
     * takes its MATCHES branch and reports success without writing anything, and A's
     * DISABLED row then commits with a newer update_time and reverses it. Requiring the
     * marker's visibility BEFORE the statement is dispatched makes the window that fences
     * the statement shared, so the MATCHES branch of every other FE sees pending != 0 and
     * fails retryably instead of concluding.
     */
    private long beginRowMutation() {
        if (!persistence.isDurable()) {
            return 0;
        }
        long watermark;
        try {
            watermark = readSlotState()[0];
        } catch (Exception e) {
            throw new RuntimeException("SPM mutation clock read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
        for (int attempt = 0; attempt < MUTATION_TICK_OPEN_ATTEMPTS; attempt++) {
            long opened = tryOpenRowMutationWindow(watermark);
            if (opened != 0) {
                return opened;
            }
        }
        throw new MutationWindowUnavailableException("SPM cannot open a mutation window whose"
                + " identity is unique cluster-wide (every attempted tick is already held by"
                + " another FE's window); nothing was changed - retry the operation");
    }

    /**
     * One attempt of beginRowMutation: appends the window marker for a fresh tick,
     * waits for it to become READABLE and verifies that it is UNIQUE.
     *
     * The uniqueness check is what makes the window identity trustworthy: the tick carries this
     * FE's random salt (see mutationTickSalt), and two processes drawing the SAME salt in the
     * SAME millisecond would open two windows with one identity - either writer's exact-row
     * close (PRUNE_HWM_OPEN_SQL) would then delete the OTHER's still-open marker, so a
     * successor reading the clock after that close sees QUIET while the other row statement is
     * still in flight. A collision is detected by the marker read itself (the probe counts the
     * rows carrying this tick), the collided tick is ABANDONED - closing it would delete the
     * other FE's marker - and the caller retries with a NEWER tick. The abandoned row carries a
     * pending marker of its own and therefore ages out like any other window.
     *
     * Parameter watermark: the slot watermark the marker records
     * Returns: the open window tick, or 0 when this attempt's tick collided
     */
    private long tryOpenRowMutationWindow(long watermark) {
        long tick = nextMutationTick();
        // The instant the window's marker is about to be written: the pre-write budget below
        // (marker INSERT, its prunes, the visibility probes) already consumes part of the
        // successor-facing bound, and the guard after the probes refuses to dispatch a
        // statement whose window would expire while it runs (see
        // MUTATION_WINDOW_MAX_AGE_SECONDS).
        long windowOpenedAt = mutationWindowNow();
        writeHwmRecord(watermark, tick, tick, true);
        long sharedWindows;
        try {
            sharedWindows = awaitMutationWindowVisible(tick);
        } catch (MutationWindowUnavailableException e) {
            // The statement will NOT be dispatched, so the window must not stay open: close
            // it best effort exactly like a completed mutation would (the completion append
            // plus the exact-row delete of THIS window). A failed append of the marker is no
            // reason to leave every snapshot read retrying for the age bound.
            endRowMutation(tick);
            throw e;
        }
        if (sharedWindows > 1) {
            LOG.warn("SPM abandoned mutation window {}: {} rows carry that tick, so another"
                    + " FE opened a window with the same identity (equal salt in the same"
                    + " millisecond); retrying with a fresh tick - the abandoned row ages out",
                    tick, sharedWindows);
            return 0;
        }
        long elapsedMillis = mutationWindowNow() - windowOpenedAt;
        if (elapsedMillis + BASELINE_WRITE_TIMEOUT_SECONDS * 1000L
                > MUTATION_WINDOW_MAX_AGE_SECONDS * 1000L) {
            // The window is ALREADY too close to its successor-facing age bound for the row
            // statement to run INSIDE it: a successor reading the clock from here on could
            // see QUIET (the marker row has aged past the bound) while this statement - or the
            // statement of another mutation whose window is equally old - is still in flight,
            // and would accept a matching durable row as final. The elapsed time is measured
            // from just BEFORE the marker write, i.e. it is an UPPER bound of the stored row's
            // age (the row is written after it, and FE clocks are synced): refusing here is
            // therefore the conservative side.
            //
            // RETRYABLE, and nothing has been written yet: endRowMutation closes the window
            // exactly like a completed mutation, so the successors see the completion instead
            // of a stale pending row.
            endRowMutation(tick);
            throw new MutationWindowUnavailableException("SPM mutation window " + tick
                    + " took " + elapsedMillis + " ms to become readable, leaving less than its"
                    + " " + BASELINE_WRITE_TIMEOUT_SECONDS + " s row-statement budget inside the "
                    + MUTATION_WINDOW_MAX_AGE_SECONDS + " s window bound; nothing was changed -"
                    + " retry the operation");
        }
        mutationWindowOpenedAt.put(tick, windowOpenedAt);
        return tick;
    }

    /**
     * The bounded read-back of the open-window marker beginRowMutation appended: a
     * mutation whose window never becomes READABLE fails retryably BEFORE its statement
     * runs, because the fence it opened is invisible to every other FE - exactly the
     * unshared window that let a successor accept a matching read while this statement was
     * in flight (see beginRowMutation). The row is never pruned while it is open
     * (PRUNE_HWM_SQL), so the confirmation cannot be raced away by a concurrent append.
     *
     * Parameter tick: the window tick just written
     * Returns: the number of visible markers carrying this tick (1 = this process's window
     *         alone, more = the identity is shared with another FE, see
     *         tryOpenRowMutationWindow)
     */
    private long awaitMutationWindowVisible(long tick) {
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            try {
                long visible = pendingWindowVisible(tick);
                if (visible > 0) {
                    return visible;
                }
            } catch (Exception e) {
                LOG.debug("SPM mutation window visibility probe failed: {}", e.getMessage());
            }
            try {
                Thread.sleep(BASELINE_VISIBILITY_RETRY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        throw new MutationWindowUnavailableException("SPM mutation window " + tick
                + " is not READABLE yet, so a concurrent ALTER / refresh could not see the"
                + " row statement that is about to run; nothing was changed - retry the"
                + " operation");
    }

    /**
     * Raised when a mutation's WINDOW could not be made readable before its row statement
     * (see beginRowMutation / awaitMutationWindowVisible): the statement was NOT
     * dispatched. Callers must fail retryably WITHOUT the ambiguous-write handling - there
     * is no outcome to reconcile, so no pending / confirmed mutation fence may be recorded
     * for a write that never ran (that fence would mask the id for its whole bound).
     */
    private static final class MutationWindowUnavailableException extends RuntimeException {
        MutationWindowUnavailableException(String message) {
            super(message);
        }
    }

    /**
     * Re-checks a mutation window right BEFORE its row statement is dispatched (see
     * persistDeleteByIdAndStatus): the window was verified when it OPENED (see
     * beginRowMutation), but a stalled caller can pause between that verification and the
     * dispatch long enough for the window to age past MUTATION_WINDOW_MAX_AGE_SECONDS -
     * after which every successor's fence read ignores its marker and may accept a
     * matching durable row the delayed statement then changes. The window's own bound must
     * also cover the statement's whole write budget, or its marker ages out WHILE the
     * statement runs (the same refusal beginRowMutation applies before the open).
     *
     * Only windows opened by THIS process are known: an unknown / absent tick (0, or a
     * window opened by another FE before a forward) skips the check - there is no local
     * evidence of its age.
     *
     * Parameter tick: the window tick the statement would be dispatched under
     */
    private void assertMutationWindowDispatchable(long tick) {
        Long openedAtMillis = mutationWindowOpenedAt.get(tick);
        if (openedAtMillis == null) {
            return;
        }
        long elapsedMillis = mutationWindowNow() - openedAtMillis;
        if (elapsedMillis + BASELINE_WRITE_TIMEOUT_SECONDS * 1000L
                > MUTATION_WINDOW_MAX_AGE_SECONDS * 1000L) {
            throw new MutationWindowUnavailableException("SPM mutation window " + tick
                    + " was opened " + elapsedMillis + " ms ago, leaving less than its "
                    + BASELINE_WRITE_TIMEOUT_SECONDS + " s row-statement budget inside the "
                    + MUTATION_WINDOW_MAX_AGE_SECONDS + " s window bound; the delayed"
                    + " statement was NOT dispatched and nothing was changed - retry the"
                    + " operation");
        }
    }

    /**
     * Closes the mutation window opened by beginRowMutation: writes a NEWER
     * tick with pending = 0, so a fence read that overlapped the window observes the
     * completion in one of its two reads, and then removes the window's own pending row
     * (see PRUNE_HWM_OPEN_SQL) - the history sweeps keep it (a foreign append must never
     * erase another master's open marker), so only this writer may close it.
     *
     * Both statements are best effort: the row statement's outcome is what the caller
     * reports, and a lost completion only keeps the snapshot fence retrying - fail CLOSED -
     * until the window bound elapses (see MUTATION_WINDOW_MAX_AGE_SECONDS), never forever.
     */
    private void endRowMutation(long tick) {
        if (tick == 0) {
            return;
        }
        mutationWindowOpenedAt.remove(tick);
        if (!persistence.isDurable()) {
            return;
        }
        long watermark;
        try {
            watermark = readSlotState()[0];
        } catch (Exception e) {
            LOG.warn("SPM mutation clock completion read failed; the snapshot fence stays"
                    + " retrying until the next mutation: {}", e.getMessage());
            return;
        }
        try {
            writeHwmRecord(watermark, nextMutationTick(), 0, true);
        } catch (RuntimeException e) {
            LOG.warn("SPM mutation clock completion write failed; the snapshot fence may"
                    + " keep retrying until the next mutation: {}", e.getMessage());
            return;
        }
        // the completion append does NOT prune the open row any more (the OPEN-WINDOW guard
        // keeps every pending row alive): close THIS window with the exact-row delete (see
        // PRUNE_HWM_OPEN_SQL). Best effort like the append itself: a lost delete only keeps
        // the fence retrying until the age bound elapses.
        //
        // CLOSE ONLY A WINDOW THIS PROCESS OWNS: the tick carries this FE's salt
        // plus the millisecond, so two FEs drawing the SAME salt in the SAME millisecond hold
        // ONE identity - and then the exact-row delete removes BOTH markers, including the
        // other writer's still-open one, whose statement may be in flight (a successor reading
        // the clock right after that close sees QUIET and accepts a matching row the other
        // statement then reverses). An OPEN-time probe cannot prove ownership here: the other
        // marker may still be UNREADABLE when it runs, so it sees a single row. The CLOSE-time
        // probe can: by now both markers are published (each writer waited for its own
        // visibility), so a count above one means the identity is shared and neither writer may
        // delete it - the rows then age out together, and the conservative direction (a noisy
        // clock for one window bound) is exactly what the bound exists for.
        try {
            long holders = pendingWindowVisible(tick);
            if (holders > 1) {
                LOG.warn("SPM leaves mutation window {} open: {} rows carry that tick, so the"
                        + " identity is shared with another FE's window and closing it would"
                        + " remove their marker as well; both age out after {} s", tick, holders,
                        MUTATION_WINDOW_MAX_AGE_SECONDS);
                return;
            }
        } catch (Exception e) {
            // an unanswerable count must not authorize the delete: it could be the shared case
            LOG.warn("SPM cannot verify the ownership of mutation window {} ({}); leaving it to"
                    + " its age bound instead of risking another FE's marker", tick,
                    e.getMessage());
            return;
        }
        try {
            persistence.closeMutationWindow(tick);
        } catch (Exception e) {
            LOG.warn("SPM mutation clock window {} could not be closed ({}); the snapshot"
                    + " fence keeps retrying until its age bound", tick, e.getMessage());
        }
    }

    /**
     * Keeps the SHARED mutation window of a DISPATCHED statement open because the write it
     * fences reported success while its committed row is still UNREADABLE (see
     * persistTransitionInsert): the window is the cluster-visible evidence that the
     * mutation is not settled, so every other FE's fence read refuses to conclude - a
     * successor accepting a MATCHING old-status row would be reversed by the committed
     * row publishing later. The window is closed once a snapshot read here shows the
     * outcome (see resolveHeldMutationWindows), or after
     * MUTATION_WINDOW_MAX_AGE_SECONDS - the bound that lets an abandoned window stop
     * poisoning the clock - when the write is presumed lost and only the process-local
     * fence still masks the id.
     *
     * Parameter tick: the window tick the statement was dispatched under
     * Parameter row: the row the statement wrote (its status / stored instant identify the
     *             outcome a snapshot must show)
     * Returns: whether the window was taken over: the caller must NOT close it
     */
    private boolean holdUnresolvedMutationWindow(long tick, BaselinePlan row) {
        if (tick == 0) {
            return false; // no shared window exists (in-memory / scripted store)
        }
        unresolvedMutationWindows.put(tick, new UnresolvedMutationWindow(row.getId(),
                row.getStatus(), row.getUpdateTime(), mutationWindowNow()));
        LOG.warn("SPM keeps mutation window {} OPEN: the committed row of baseline {} is"
                + " not READABLE yet, so a successor must not treat the previous status row"
                + " as final; the window closes once a read shows the row (or after its age"
                + " bound)", tick, row.getId());
        return true;
    }

    /**
     * Keeps the SHARED mutation window of a DISPATCHED no-op ALTER open because the PRUNE it
     * ran is not provably visible yet (see confirmPruneVisible): the DELETE changes the row
     * set a PAGINATED reader walks, so a follower whose page 1 ended on a removed row
     * continues past the intent row behind it and accepts the stale page. The window closes
     * once a read shows the committed row AND no reclaimed version (see
     * UnresolvedMutationWindow.isSettled), or after MUTATION_WINDOW_MAX_AGE_SECONDS.
     *
     * Parameter tick: the window tick the statement was dispatched under
     * Parameter row: the intent row the statement wrote
     * Parameter keepFromMicros: the stored instant the prune keeps from
     * Returns: whether the window was taken over: the caller must NOT close it
     */
    private boolean holdUnresolvedPruneWindow(long tick, BaselinePlan row, long keepFromMicros) {
        if (tick == 0) {
            return false; // no shared window exists (in-memory / scripted store)
        }
        unresolvedMutationWindows.put(tick, new UnresolvedMutationWindow(row.getId(),
                row.getStatus(), row.getUpdateTime(), mutationWindowNow(), row, keepFromMicros));
        LOG.warn("SPM keeps mutation window {} OPEN: the versions of baseline {} reclaimed by"
                + " the no-op ALTER are not provably gone yet, so a paginated reader may still"
                + " hold one of them; the window closes once a read proves the reclaim (or"
                + " after its age bound)", tick, row.getId());
        return true;
    }

    /**
     * Keeps the SHARED mutation window of a dispatched DELETE open because its outcome is
     * AMBIGUOUS (see persistDeleteByIdentity): the statement reported an error that may have
     * been raised after a commit, and no read can prove the row gone yet. The window is the
     * cluster-visible evidence that the DROP is not settled, so every other FE's fence read
     * refuses to conclude - a successor that reads the OLD ENABLED row and reports
     * ALTER... ENABLE as a successful no-op would lose that row when this DELETE publishes.
     * The window closes after a read shows absence and any retained DROP tombstones
     * are readable, or after MUTATION_WINDOW_MAX_AGE_SECONDS.
     *
     * Parameter tick: the window tick the DELETE was dispatched under
     * Parameter id: the baseline id whose row must become absent
     * Returns: whether the window was taken over: the caller must NOT close it
     */
    private boolean holdUnresolvedAbsenceMutationWindow(long tick, long id) {
        if (tick == 0) {
            return false; // no shared window exists (in-memory / scripted store)
        }
        unresolvedMutationWindows.put(tick, new UnresolvedMutationWindow(id, null, 0,
                mutationWindowNow()));
        LOG.warn("SPM keeps mutation window {} OPEN: the DELETE of baseline {} may still have"
                + " COMMITTED while no read can prove the row gone, so a successor must not"
                + " treat the old-status row as final; the window closes after absence and any retained"
                + " DROP tombstones are confirmed (or after its age bound)", tick, id);
        return true;
    }

    /**
     * Probes the ids of every HELD mutation window against the durable table and CLOSES
     * the windows whose committed outcome became readable, BEFORE a snapshot attempt
     * consults the quiet gate (see readStableRawSnapshot). A proven DROP also needs both
     * retained tombstones before its shared window can close.
     *
     * A successful DISABLE whose row appears just after confirmInsertVisible exhausted
     * its probes leaves its shared pending marker visible: the quiet gate rejects that
     * marker, but the settlement that closes the window
     * (resolveHeldMutationWindows) only runs AFTER a successful snapshot - so refresh,
     * GLOBAL SHOW and promotion reload could not settle and failed retryably until the
     * 120-second marker expiry, although the row was readable long since. Best effort: a
     * failed / still-unreadable probe keeps the window held (it is closed by the
     * post-snapshot path or ages out).
     */
    private void settleHeldMutationWindowsBeforeSnapshot() {
        if (unresolvedMutationWindows.isEmpty() || !persistence.isDurable()) {
            return;
        }
        for (Map.Entry<Long, UnresolvedMutationWindow> entry
                : new ArrayList<>(unresolvedMutationWindows.entrySet())) {
            UnresolvedMutationWindow held = entry.getValue();
            boolean readable;
            try {
                List<BaselinePlan> rows = filterTombstonedDurableRows(
                        readPersistedParsedById(held.id));
                BaselinePlan winner = null;
                for (BaselinePlan row : rows) {
                    winner = winner == null ? row : pickDurableWinner(winner, row);
                }
                readable = held.isSettled(winner, persistence);
            } catch (RuntimeException e) {
                continue; // the outcome cannot be read: the window stays held
            }
            PendingMutationFence fence = pendingDropForWindow(held);
            if (fence != null) {
                readable = (readable || fence.confirmed) && tryConfirmObservedDrop(held.id, fence);
            }
            if (!readable) {
                continue;
            }
            if (unresolvedMutationWindows.remove(entry.getKey(), held)) {
                endRowMutation(entry.getKey());
                LOG.info("SPM mutation window {} of baseline {} closed before the snapshot"
                        + " gate: the mutation outcome is confirmed", entry.getKey(), held.id);
            }
        }
    }

    /**
     * Closes the mutation windows this FE HELD OPEN for an unreadable committed outcome
     * (see holdUnresolvedMutationWindow) once a snapshot proves the outcome readable - or
     * once the window aged past MUTATION_WINDOW_MAX_AGE_SECONDS, after which every fence
     * read ignores it anyway. Called by every snapshot publish path (see
     * resolvePendingMutationFences), OUTSIDE the state lock: the completion append and the
     * window delete below are internal-table I/O.
     *
     * Parameter persisted: the snapshot about to be applied (not modified)
     */
    private void resolveHeldMutationWindows(Map<Long, BaselinePlan> persisted) {
        if (unresolvedMutationWindows.isEmpty()) {
            return;
        }
        long now = mutationWindowNow();
        for (Map.Entry<Long, UnresolvedMutationWindow> entry
                : new ArrayList<>(unresolvedMutationWindows.entrySet())) {
            UnresolvedMutationWindow held = entry.getValue();
            boolean readable = held.isSettled(persisted.get(held.id), persistence)
                    && pendingDropForWindow(held) == null;
            boolean agedOut = now - held.sinceMillis
                    > MUTATION_WINDOW_MAX_AGE_SECONDS * 1000L;
            if (!readable && !agedOut) {
                continue;
            }
            unresolvedMutationWindows.remove(entry.getKey(), held);
            endRowMutation(entry.getKey());
            LOG.info("SPM mutation window {} of baseline {} closed: its committed row is {}",
                    entry.getKey(), held.id,
                    readable ? "readable now" : "still unreadable past the window bound");
        }
    }

    private PendingMutationFence pendingDropForWindow(UnresolvedMutationWindow held) {
        if (held.expectedStatus != null) {
            return null;
        }
        // The DELETE can hold its window just before the DROP caller records its local fence.
        synchronized (writerLock) {
            PendingMutationFence fence = pendingMutationFences.get(held.id);
            return fence != null && fence.droppedIdentity != null ? fence : null;
        }
    }

    /** One watermark cell of an aggregate read (blank / NULL = 0). */
    private static long parseWatermark(String text) {
        return text == null || text.isEmpty() ? 0L : Long.parseLong(text.trim());
    }

    /**
     * Durably RESERVES an allocated id BEFORE its baseline row is written. The reservation
     * is an append-only row of InternalSchema.SPM_BASELINES_SEQ_TBL_NAME, so the id
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
     * Parameter plan: the row about to be written (its id was just allocated)
     */
    private void reserveAllocatedId(BaselinePlan plan) {
        if (!persistence.isDurable()) {
            return;
        }
        String digest = plan.getBindSqlDigest() == null ? "" : plan.getBindSqlDigest();
        long planSqlHash = identityPlanSqlHash(plan);
        persistence.reserveId(plan.getId(), digest, planSqlHash, SPMUtils.nowMicros());
        // Prove the reservation's durable evidence is READABLE by the reads a successor /
        // retry uses BEFORE the baseline row write may dispatch (see
        // confirmReservationReadable).
        confirmReservationReadable(plan.getId(), digest, planSqlHash);
    }

    /** Records the allocation watermark, then publishes the allocation and identity together. */
    private void reserveIdIntoTable(long id, String digest, long planSqlHash, long reserveTime) {
        writeHwmRecord(id, nextMutationTick(), 0L, true);
        try {
            appendSequenceState(1L, id, digest, planSqlHash, reserveTime, false);
        } catch (RuntimeException e) {
            throw new RuntimeException("SPM baseline id reservation write failed (retry the"
                    + " CREATE): " + e.getMessage(), e);
        }
    }

    /**
     * Confirms the id reservation just written is READABLE by the same reads a successor
     * or a retry on ANOTHER FE uses, BEFORE the baseline row write may dispatch (see the
     * class javadoc "Id source"): the compact high-water mark, the history row and the
     * compact identity slot are SEPARATE writes whose SQL OK says nothing about
     * publication. A successor that misses the reservation hands the id to a DIFFERENT
     * key (its by-id collision probe cannot see the unreadable row either), and a retry
     * that reads an older compact tombstone treats the identity as resolved and allocates
     * a SECOND id - either way two ENABLED rows can publish later. Bounded; a reservation
     * that never becomes readable fails the CREATE retryably BEFORE any row was written.
     *
     * Parameter id: the just-reserved id
     * Parameter bindSqlDigest: the identity's canonical bind digest
     * Parameter planSqlHash: the identity's plan SQL hash
     */
    private void confirmReservationReadable(long id, String bindSqlDigest,
            long planSqlHash) {
        if (!persistence.isDurable()) {
            return;
        }
        if (!persistence.reservationReadsAvailable()) {
            // stores that do not MODEL the reservation reads (the scripted collision /
            // status stores) cannot answer the confirmation - only a store whose sequence
            // reads exist is asked. The gate is the store CAPABILITY, never a readback
            // VALUE: a lagging readback on a fresh cluster answers 0 while the
            // reservation this CREATE must prove is simply not visible yet, and skipping
            // the proof let a successor hand the same id out twice.
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            // ONE view of the EXACT reads a successor / retry performs: the allocation
            // watermark (compact HWM reconciled with the sequence tail) and the identity
            // lookup (authoritative identity slot). The store
            // assembles the pair so this proof can never drift into a "more reliable" read
            // of its own - it must assert the visibility a successor will have, not a
            // stronger one (see BaselinePersistence.readReservationView).
            BaselinePersistence.ReservationView view =
                    persistence.readReservationView(bindSqlDigest, planSqlHash);
            if (view.showsReservation(id)) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        throw new IllegalStateException("SPM cannot confirm the id reservation of baseline "
                + id + " is readable (an id handed out must never be handed out again);"
                + " retry the CREATE");
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
     * Parameter plan: the row whose write is unresolved
     */
    private void markSeqPendingUnconfirmed(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        if (!persistence.isDurable()) {
            return;
        }
        persistence.notePendingSeqState(plan.getBindSqlDigest(), identityPlanSqlHash(plan),
                plan.getId(), SPMUtils.nowMicros());
    }

    /**
     * The live implementation half of BaselinePersistence.notePendingSeqState:
     * the marker append is best effort by design (the create is already failing with the
     * original unconfirmed error).
     */
    private static void notePendingSeqStateIntoTable(String bindSqlDigest, long planSqlHash,
            long id) {
        // The marker's age decides the durable pending-create fence, which compares
        // SPMUtils.nowMicros against the STORED instant: a millisecond value here would make a
        // just-written marker look decades old and the fence would never hold.
        long now = SPMUtils.nowMicros();
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = sequenceIdentityParams(bindSqlDigest, planSqlHash, id);
        params.put("reserveTime", toTs(now));
        params.put("unconfirmed", "1");
        params.put("dropped", "0");
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_COMPACT_SEQ_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.warn("SPM could not persist the unconfirmed-create marker of baseline {}: {}", id, e.getMessage());
        }
    }

    /**
     * Attempts to append the identity-scoped DROP tombstone after a confirmed deletion. The wire write is best
     * effort; the DROP confirmation path separately checks readable markers and can report a retryable failure.
     */
    private void noteDroppedSeqState(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        writeDroppedMarker(plan.getId(), plan.getBindSqlDigest(),
                identityPlanSqlHash(plan));
    }

    /**
     * Appends the ID-scoped tombstone of a completed DROP: any row carrying this id -
     * whatever its bind/plan identity - is dead, so a handoff collision partner that
     * was still UNREADABLE when the cleanup ran cannot revive the dropped id once it
     * publishes. Only DROP paths write it (the user asked for the whole id to be gone);
     * an unproven delete must not, or a still-live row would be hidden and re-deleted
     * on every load.
     *
     * Parameter id: the dropped id
     */
    private void noteDroppedIdState(long id) {
        writeDroppedMarker(id, "", 0L);
    }

    /**
     * Records that a DROP still OWES the readable tombstone of one retired row (see
     * confirmDropTombstonesDurable), at the moment the row is deleted - not when the
     * DROP happens to reach its confirmation. A later twin's delete can throw, a tombstone
     * READ can fail, and the retry then finds these rows already ABSENT: the obligation
     * map is the only source of their identities anymore (see
     * confirmPendingDropTombstones), and losing it let a demoted master's delayed status
     * INSERT of the twin revive the dropped baseline on another FE / after a restart.
     *
     * Parameter dropId: the user-facing id of the DROP
     * Parameter row: one row (the removed one or a retired twin) the DROP deleted
     */
    private static void oweDropTombstone(long dropId, BaselinePlan row) {
        if (row == null || row.getBindSqlDigest() == null || row.getPlanSql() == null) {
            return;
        }
        PendingDropMarker marker = new PendingDropMarker(row.getId(), row.getBindSqlDigest(),
                identityPlanSqlHash(row));
        pendingDropTombstoneConfirmations.computeIfAbsent(dropId,
                        k -> new PendingDropObligation())
                .owed.putIfAbsent(marker.key(), marker);
    }

    /** Records the id-scoped tombstone the DROP of dropId still owes (see oweDropTombstone). */
    private static void oweDropIdTombstone(long dropId) {
        PendingDropMarker marker = new PendingDropMarker(dropId, "", 0L);
        pendingDropTombstoneConfirmations.computeIfAbsent(dropId,
                        k -> new PendingDropObligation())
                .owed.putIfAbsent(marker.key(), marker);
    }

    /**
     * Appends one DROP TOMBSTONE row (see INSERT_SEQ_STATE_SQL); best effort
     * like noteDroppedSeqState.
     *
     * Parameter id: the identity's id
     * Parameter bindSqlDigest: the identity's canonical bind digest
     * Parameter planSqlHash: the identity's plan SQL hash
     */
    private void writeDroppedMarker(long id, String bindSqlDigest, long planSqlHash) {
        if (!persistence.isDurable()) {
            return;
        }
        PendingDropMarker marker = new PendingDropMarker(id, bindSqlDigest, planSqlHash);
        try {
            if (persistence.appendDroppedMarker(id, bindSqlDigest, planSqlHash,
                    System.currentTimeMillis())) {
                pendingDroppedMarkers.remove(marker.key());
                return;
            }
        } catch (RuntimeException e) {
            // a THROWN append takes the same masking path as a false return
            pendingDroppedMarkers.put(marker.key(), marker);
            LOG.warn("SPM could not persist the drop tombstone of baseline {} (an"
                    + " in-flight status write may revive it); the identity stays filtered"
                    + " on this FE and the append is retried: {}", id, e.getMessage());
            return;
        }
        // The tombstone is only BEST EFFORT on the wire, but losing it re-opens the
        // delayed-commit window this FE just closed: keep the marker in memory so the
        // identity stays masked for THIS process, and retry the durable append from the
        // next scoped tombstone read (see readDroppedIdentities).
        pendingDroppedMarkers.put(marker.key(), marker);
        LOG.warn("SPM could not persist the drop tombstone of baseline {} (an"
                + " in-flight status write may revive it); the identity stays filtered"
                + " on this FE and the append is retried", id);
    }

    /**
     * The live implementation half of BaselinePersistence.appendDroppedMarker:
     * returns false (after no logging) when the append did not land, so the caller keeps
     * its in-memory mask.
     */
    private static boolean appendDroppedMarkerIntoTable(long id, String bindSqlDigest,
            long planSqlHash) {
        if (!persistenceEnabled()) {
            return false;
        }
        try {
            appendSequenceState(tombstoneSeqId(id), id, bindSqlDigest, planSqlHash,
                    SPMUtils.nowMicros(), true);
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    /** One bigint FLAG cell (NULL / blank = false); see BaselinePersistence.SeqReservation. */
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
     * Parameter ids: the ids to look up (an empty collection skips the query entirely)
     */
    private Set<String> readDroppedIdentities(Collection<Long> ids) {
        return readDroppedIdentities(ids, true, Long.MAX_VALUE);
    }

    /**
     * readDroppedIdentities(ids) with the process-local half under a switch (see
     * readDroppedIdentitiesDurable).
     */
    private Set<String> readDroppedIdentities(Collection<Long> ids,
            boolean includeLocalPending) {
        return readDroppedIdentities(ids, includeLocalPending, Long.MAX_VALUE);
    }

    /**
     * As readDroppedIdentities(Collection, boolean), with a deadline:
     * the matching-path cache miss bounds this read by the remaining rewrite budget (see
     * completeRowsForHash).
     */
    private Set<String> readDroppedIdentities(Collection<Long> ids,
            boolean includeLocalPending, long deadlineMillis) {
        if (ids == null || ids.isEmpty()) {
            return Set.of();
        }
        Set<String> markers = new HashSet<>(persistence.readDroppedIdentitiesBefore(ids, deadlineMillis));
        // The tombstones this FE failed to append durably (see writeDroppedMarker) still
        // mask their identity - the caller asked about these very ids, so without the
        // in-memory half the delayed-commit row revived on THIS FE; the append is also
        // retried here, bounded to the ids being read. A DURABLE-ONLY read
        // (readDroppedIdentitiesDurable) skips this half: the local marker IS the failed
        // append the caller must not mistake for a readable one.
        if (includeLocalPending && !pendingDroppedMarkers.isEmpty()) {
            Set<Long> scoped = new java.util.HashSet<>(ids);
            for (PendingDropMarker marker : pendingDroppedMarkers.values()) {
                if (!scoped.contains(marker.id)) {
                    continue;
                }
                markers.add(marker.key());
                if (deadlineMillis == Long.MAX_VALUE) {
                    writeDroppedMarker(marker.id, marker.bindSqlDigest, marker.planSqlHash);
                }
            }
        }
        return markers;
    }

    /** The live implementation half of BaselinePersistence.readDroppedIdentities. */
    private static Set<String> readDroppedIdentitiesFromTable(Collection<Long> ids) {
        return readDroppedIdentitiesFromTable(ids, Long.MAX_VALUE);
    }

    /** Reads each tombstone chunk using the time left in the original budget. */
    private static Set<String> readDroppedIdentitiesFromTable(Collection<Long> ids, long deadlineMillis) {
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
                    readDroppedIdentitiesChunk(chunk, markers, remainingReadTimeout(deadlineMillis));
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                readDroppedIdentitiesChunk(chunk, markers, remainingReadTimeout(deadlineMillis));
            }
        } catch (Exception e) {
            throw new RuntimeException("SPM durable drop-marker read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
        return markers;
    }

    /**
     * The DURABLE tombstones only (no process-local fallback, no retry side effect): for
     * a caller that must PROVE an append is readable - the condemnation confirmation
     * before a fresh id is allocated and the DROP confirmation before success is
     * reported. The pendingDroppedMarkers fallback of the scoped read marks the very
     * append the caller just failed to make as present, so the confirmation would pass
     * with the marker living in this process only (see confirmDroppedMarkerDurable).
     */
    private Set<String> readDroppedIdentitiesDurable(Collection<Long> ids) {
        return readDroppedIdentities(ids, false);
    }

    /** One scoped page of the tombstone read (see readDroppedIdentities). */
    private static void readDroppedIdentitiesChunk(List<Long> ids, Set<String> markers,
            int timeoutSeconds)
            throws Exception {
        StringBuilder idList = new StringBuilder();
        StringBuilder tombstoneIdList = new StringBuilder();
        for (Long id : ids) {
            if (idList.length() > 0) {
                idList.append(',');
                tombstoneIdList.append(',');
            }
            idList.append(id);
            tombstoneIdList.append(tombstoneSeqId(id));
        }
        Map<String, String> params = new HashMap<>();
        params.put("ids", idList.toString());
        params.put("tombstoneIds", tombstoneIdList.toString());
        List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                SELECT_SEQ_DROPPED_SQL, params, timeoutSeconds));
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
                identityPlanSqlHash(plan));
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
     * The IDENTITY-scoped plan-SQL hash of one baseline: the plan SQL PLUS its schema
     * fingerprint. The fingerprint is part of a baseline's INCARNATION: after an ALTER
     * TABLE the SAME SQL under the new schema is a different baseline, and every
     * identity-scoped record - the pending-create reservation / marker / retirement, the
     * DROP tombstone, the collision key - must not let one incarnation's record retire or
     * mask the other's (a stale fingerprint branch that deleted by (id, digest, planSql)
     * alone removed a NEWER incarnation's successful row, and its tombstone masked it
     * too). The hash is opaque, so folding the fingerprint into it makes every reader /
     * writer of the column agree without a schema change.
     */
    private static long identityPlanSqlHash(BaselinePlan plan) {
        // A fingerprint-less incarnation keeps the historical plan-SQL-only hash:
        // the empty string carries no incarnation information, and mixing the old
        // shape with a folded one would split the identity of rows the upgrade
        // itself wrote. A real fingerprint  is folded in so that two
        // incarnations of the same plan SQL under different schemas never collide.
        String fingerprint = plan.getSchemaFingerprint();
        if (fingerprint == null || fingerprint.isEmpty()) {
            return SPMUtils.hashOf(plan.getPlanSql() == null ? "" : plan.getPlanSql());
        }
        return identityPlanSqlHash(plan.getPlanSql(), fingerprint);
    }

    /** One identity plan-SQL hash (see identityPlanSqlHash(BaselinePlan)). */
    private static long identityPlanSqlHash(String planSql, String schemaFingerprint) {
        return SPMUtils.hashOf((planSql == null ? "" : planSql) + '\u0000'
                + (schemaFingerprint == null ? "" : schemaFingerprint));
    }

    /**
     * Whether one row's identity carries a readable DROP TOMBSTONE: the
     * user's DROP completed, so the row is not adoptable any more even while its own
     * DELETE lags its publication. A tombstone READ failure propagates (the caller fails
     * retryably instead of adopting a row it cannot clear).
     */
    private boolean isTombstonedIdentity(BaselinePlan plan) {
        return plan != null && plan.getId() > 0
                && isTombstonedRow(readDroppedIdentities(List.of(plan.getId())), plan);
    }

    /**
     * Whether one row is covered by a read tombstone set: its own identity, or the
     * ID-scoped marker of a completed DROP (see noteDroppedIdState).
     */
    private static boolean isTombstonedRow(Set<String> markers, BaselinePlan row) {
        return markers.contains(droppedIdentityKey(row))
                || markers.contains(droppedIdKey(row.getId()));
    }

    /**
     * Whether one compact identity answer already NAMES the incarnation of every row it is about
     * to filter (see withIdentityDrops).
     *
     * A compact row is the compact COPY of one state change of one incarnation, so an answer
     * whose baseline_id equals the row's own id is that row's own state: alive here means
     * the row was alive when that state was written, and a DROP of it would have appended a
     * tombstone for the SAME id (the fence republishes it until every reader sees it). An answer
     * naming a DIFFERENT (necessarily newer) id says nothing about the row's incarnation, so the
     * caller reconciles with the identity-filtered history instead.
     *
     * Parameter state: the compact (batched) answer of the identity
     * Parameter rows: the rows the caller is about to filter
     * Returns: whether the answer names every row's own incarnation
     */
    private static boolean namesEveryIncarnation(BaselinePersistence.SeqReservation state,
            Collection<BaselinePlan> rows) {
        for (BaselinePlan row : rows) {
            if (row == null || row.getId() != state.id()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Extends scoped tombstones using the latest sequence state of each bind/plan/fingerprint identity. Compact
     * states are fetched in batches. Missing, dropped or mismatched states use an exact keyed identity read.
     * The latest DROP masks its allocation and older rows of that identity. A larger id represents a later
     * creation, even if its reservation is still unreadable. Visible for package tests.
     */
    Set<String> withIdentityDrops(Set<String> tombstones, Collection<BaselinePlan> rows) {
        return withIdentityDrops(tombstones, rows, Long.MAX_VALUE);
    }

    private Set<String> withIdentityDrops(Set<String> tombstones, Collection<BaselinePlan> rows,
            long deadlineMillis) {
        Map<String, List<BaselinePlan>> byIdentity = null;
        for (BaselinePlan row : rows) {
            if (row == null || row.getId() <= 0 || row.getBindSqlDigest() == null
                    || row.getPlanSql() == null) {
                continue;
            }
            if (isTombstonedRow(tombstones, row)) {
                continue;
            }
            if (byIdentity == null) {
                byIdentity = new java.util.LinkedHashMap<>();
            }
            byIdentity.computeIfAbsent(row.getBindSqlDigest() + '\u0000'
                    + identityPlanSqlHash(row), k -> new ArrayList<>()).add(row);
        }
        if (byIdentity == null) {
            return tombstones;
        }
        Set<String> extended = new java.util.HashSet<>(tombstones);
        // Batch reads use the slot hash. A result that cannot name the row being filtered
        // requires the full identity filter to disambiguate a hash collision.
        List<Map.Entry<String, List<BaselinePlan>>> groups =
                new ArrayList<>(byIdentity.entrySet());
        for (int start = 0; start < groups.size(); start += IDENTITY_STATE_BATCH_SIZE) {
            List<Map.Entry<String, List<BaselinePlan>>> batch = groups.subList(start,
                    Math.min(groups.size(), start + IDENTITY_STATE_BATCH_SIZE));
            Set<Long> compactIds = new java.util.LinkedHashSet<>();
            for (Map.Entry<String, List<BaselinePlan>> group : batch) {
                BaselinePlan first = group.getValue().get(0);
                compactIds.add(compactSeqId(first.getBindSqlDigest(), identityPlanSqlHash(first)));
            }
            Map<Long, BaselinePersistence.SeqReservation> batched;
            try {
                batched = persistence.readLatestReservations(compactIds,
                        remainingReadTimeout(deadlineMillis));
            } catch (RuntimeException e) {
                throw new RuntimeException("SPM durable identity-state read failed (retry the"
                        + " operation): " + e.getMessage(), e);
            }
            for (Map.Entry<String, List<BaselinePlan>> group : batch) {
                BaselinePlan first = group.getValue().get(0);
                long planSqlHash = identityPlanSqlHash(first);
                BaselinePersistence.SeqReservation state = batched == null ? null
                        : batched.get(compactSeqId(first.getBindSqlDigest(), planSqlHash));
                if (state == null || state.dropped || !namesEveryIncarnation(state, group.getValue())) {
                    state = persistence.readLatestReservation(first.getBindSqlDigest(), planSqlHash, deadlineMillis);
                }
                if (state == null || !state.dropped) {
                    continue;
                }
                for (BaselinePlan row : group.getValue()) {
                    if (row.getId() <= state.id) {
                        extended.add(droppedIdentityKey(row));
                    }
                }
            }
        }
        return extended;
    }

    /**
     * Removes every row whose identity carries a DROP TOMBSTONE: the row
     * was deleted by a completed DROP and revived afterwards by a delayed write (a
     * demoted master's in-flight status INSERT committing after the delete). Matching it
     * would make the dropped baseline ACTIVE again. The rows are also repaired away
     * (best effort - a follower's load cannot write).
     *
     * Parameter snapshot: the rows read from the durable table (not mutated)
     * Returns: the rows without resurrected ones
     */
    private Map<Long, BaselinePlan> filterResurrectedRows(
            Map<Long, BaselinePlan> snapshot) {
        if (snapshot == null || snapshot.isEmpty()) {
            return snapshot == null ? Map.of() : snapshot;
        }
        // The tombstone read is scoped to the SNAPSHOT's ids - the
        // append-only sequence table keeps every historical drop marker, and an
        // unrestricted read grew with the CREATE / DROP churn until it timed out and
        // follower caches stopped applying later GLOBAL changes.
        Set<String> tombstones = withIdentityDrops(
                readDroppedIdentities(snapshot.keySet()), snapshot.values());
        if (tombstones.isEmpty()) {
            return snapshot;
        }
        Map<Long, BaselinePlan> filtered = new java.util.LinkedHashMap<>(snapshot);
        for (BaselinePlan row : snapshot.values()) {
            if (!isTombstonedRow(tombstones, row)) {
                continue;
            }
            filtered.remove(row.getId());
            repairRevivedRow(row);
        }
        return filtered;
    }

    /**
     * The RAW-row variant of filterResurrectedRows: the tombstones are matched against
     * EVERY raw row BEFORE the per-id winner is picked. A demoted leader's abandoned
     * CREATE can receive an identity-only tombstone and then publish late under the SAME
     * id as another leader's successful CREATE. When the condemned row won the timestamp
     * / digest tie, filtering the already-COLLAPSED map removed the whole id - the valid
     * row stayed hidden from refresh and SHOW until a leader repaired the condemned row
     * and another refresh ran.
     *
     * Parameter rowsById: the RAW rows read from the durable table (not mutated)
     * Returns: the surviving rows with one deterministic winner per id
     */
    private Map<Long, BaselinePlan> filterResurrectedRowsById(
            Map<Long, List<BaselinePlan>> rowsById) {
        if (rowsById == null || rowsById.isEmpty()) {
            return Map.of();
        }
        List<BaselinePlan> rawRows = new ArrayList<>();
        for (List<BaselinePlan> rows : rowsById.values()) {
            if (rows != null) {
                rawRows.addAll(rows);
            }
        }
        Set<String> tombstones = withIdentityDrops(
                readDroppedIdentities(rowsById.keySet()), rawRows);
        if (tombstones.isEmpty()) {
            return collapseSnapshotRows(rowsById);
        }
        Map<Long, List<BaselinePlan>> surviving = new java.util.LinkedHashMap<>(rowsById.size());
        for (Map.Entry<Long, List<BaselinePlan>> entry : rowsById.entrySet()) {
            List<BaselinePlan> kept = new ArrayList<>(entry.getValue().size());
            for (BaselinePlan row : entry.getValue()) {
                if (row == null) {
                    continue;
                }
                if (isTombstonedRow(tombstones, row)) {
                    repairRevivedRow(row);
                    continue;
                }
                kept.add(row);
            }
            if (!kept.isEmpty()) {
                surviving.put(entry.getKey(), kept);
            }
        }
        return collapseSnapshotRows(surviving);
    }

    /**
     * Logs and best-effort repairs one durable row revived after its DROP (see
     * filterResurrectedRows and filterResurrectedRowsById).
     */
    private void repairRevivedRow(BaselinePlan row) {
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
     * Parameter rows: the rows of one key / id read from the durable table (not mutated)
     * Returns: the rows without dropped identities
     */
    private List<BaselinePlan> filterTombstonedDurableRows(List<BaselinePlan> rows) {
        return filterTombstonedDurableRows(rows, Long.MAX_VALUE);
    }

    /**
     * Filters tombstones with the remaining matching budget. Bounded matching reads only mask dropped
     * rows; refresh and management operations perform any durable repairs.
     */
    private List<BaselinePlan> filterTombstonedDurableRows(List<BaselinePlan> rows,
            long deadlineMillis) {
        if (rows == null || rows.isEmpty()) {
            return rows == null ? new ArrayList<>() : rows;
        }
        Set<String> tombstones = withIdentityDrops(
                readDroppedIdentities(rowIds(rows), true, deadlineMillis), rows, deadlineMillis);
        if (tombstones.isEmpty()) {
            return rows;
        }
        List<BaselinePlan> surviving = new ArrayList<>(rows.size());
        for (BaselinePlan row : rows) {
            if (row == null || !isTombstonedRow(tombstones, row)) {
                surviving.add(row);
                continue;
            }
            if (deadlineMillis != Long.MAX_VALUE) {
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
     * Best-effort removal of one resolved create's unconfirmed sequence marker. Only unconfirmed rows for markerId
     * are deleted; plain reservations retain the allocation watermark. A failed cleanup can delay another create
     * of the same identity until the pending fence expires.
     */
    private void retireSeqPendingMarker(BaselinePlan plan, long markerId) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return;
        }
        if (!persistence.isDurable()) {
            return;
        }
        persistence.retirePendingSeqState(plan.getBindSqlDigest(), identityPlanSqlHash(plan),
                markerId);
    }

    /** Removes the pending marker from the identity slot, leaving the original allocation readable. */
    private static void retirePendingSeqStateFromTable(String bindSqlDigest, long planSqlHash, long markerId) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = sequenceIdentityParams(bindSqlDigest, planSqlHash, markerId);
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(DELETE_PENDING_SEQ_MARKER_SQL,
                    params, BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            LOG.warn("SPM could not retire the unconfirmed-create marker of baseline {}: {}", markerId, e.getMessage());
        }
    }

    /** The sequence-table key of one identity. The full digest and hash disambiguate key collisions. */
    private static long compactSeqId(String bindSqlDigest, long planSqlHash) {
        long mixed = SPMUtils.hashOf(bindSqlDigest + '' + planSqlHash);
        return COMPACT_SEQ_ID_BASE + (mixed & 0x3FFFFFFFFFFFFFFFL);
    }

    private static Map<String, String> sequenceIdentityParams(String bindSqlDigest, long planSqlHash, long id) {
        Map<String, String> params = new HashMap<>();
        params.put("compactId", String.valueOf(compactSeqId(bindSqlDigest, planSqlHash)));
        params.put("lastId", String.valueOf(id));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        return params;
    }

    private static void appendSequenceState(long seqId, long id, String digest, long hash, long time, boolean dropped) {
        Map<String, String> params = sequenceIdentityParams(digest, hash, id);
        params.put("seqId", String.valueOf(seqId));
        params.put("reserveTime", toTs(time));
        params.put("dropped", dropped ? "1" : "0");
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_SEQ_STATE_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
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
     * (the concurrent-write scenario: "the sequence reservation may already be visible"): a
     * fresh marker whose baseline row is still unreadable DEFERS the retry; once the row
     * publishes it is ADOPTED, and an expired marker is treated as a lost write (see
     * DURABLE_PENDING_CREATE_FENCE_MICROS).
     *
     * The PLAIN reservation row fences exactly like the marker: it is
     * appended BEFORE the baseline write, so while it is young and its id is unreadable
     * the write it describes may still be in flight - fencing on it closes the hole a
     * FAILED marker append used to leave open (a retry on another FE then allocated a
     * second id next to the possibly-committed write). It never holds back a legitimate
     * re-create: a readable row is adopted (the retry reports the SAME baseline instead
     * of duplicating it), and an unreadable one is condemned once the fence expires.
     *
     * Parameter plan: the CREATE's baseline
     * Returns: the ADOPTED id when the marker's row became readable, else null (the create
     *         proceeds: no ambiguous write of this key exists, or its fence expired)
     */
    private Long resolveDurablePendingCreate(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return null;
        }
        if (!persistence.isDurable()) {
            return null;
        }
        long planSqlHash = identityPlanSqlHash(plan);
        BaselinePersistence.SeqReservation reservation =
                persistence.readLatestReservation(plan.getBindSqlDigest(), planSqlHash);
        if (reservation == null) {
            return null;
        }
        // A DROP by id may cover an identity without knowing its digest or plan SQL.
        // Use durable, id-keyed tombstones to reconcile that separate state.
        Set<String> tombstones = readDroppedIdentitiesDurable(Set.of(reservation.id));
        if (tombstones.contains(droppedIdentityKey(reservation.id,
                plan.getBindSqlDigest(), planSqlHash))
                || tombstones.contains(droppedIdKey(reservation.id))) {
            // resolved by a completed DROP / a condemned deserted write: the key may be
            // created again under a fresh id, exactly like the compact tombstone branch
            // below
            return null;
        }
        if (reservation.dropped) {
            // The identity was RESOLVED by a tombstone (a completed DROP, or a deserted
            // write condemned after its fence expired): the key may be created again
            // under a fresh id, and no row of the old incarnation may be adopted.
            return null;
        }
        BaselinePersistence.DurablePresence presence = probeDurableRow(reservation.id,
                plan.getBindSqlDigest(), plan.getPlanSql(), null, plan.getSchemaFingerprint());
        if (presence == BaselinePersistence.DurablePresence.UNKNOWN) {
            // The probe could not READ the id's rows (a timeout / an unavailable
            // tablet): the outcome of the reserved write is UNPROVEN, and the expiry
            // handling below would durably TOMBSTONE a possibly-healthy identity - a
            // refresh would then filter / repair away the earlier successful baseline
            // even though this retry never proved it absent. Only a SUCCESSFUL absent
            // read may reach the expiry handling; UNKNOWN fails the CREATE retryably
            // and leaves the old identity untouched.
            throw new IllegalStateException("SPM cannot create baseline: the durable row of"
                    + " the previously reserved identity (id " + reservation.id
                    + ") cannot be read (probe UNKNOWN); nothing was changed, retry the"
                    + " statement");
        }
        if (presence == BaselinePersistence.DurablePresence.PRESENT) {
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
            // A plain reservation is already resolved once its row is readable. Only
            // retire an actual ambiguous-write marker; another plain compact append
            // for the same id would survive the lower-id prune on every repeated CREATE.
            if (reservation.unconfirmed) {
                retireSeqPendingMarker(plan, reservation.id);
            }
            LOG.info("SPM durable pending create of baseline {} adopted from the durable"
                    + " table", readable.getId());
            return readable.getId();
        }
        if (SPMUtils.nowMicros() - reservation.reserveTimeMicros
                <= DURABLE_PENDING_CREATE_FENCE_MICROS) {
            throw new IllegalStateException("SPM cannot create baseline: a previously COMMITTED"
                    + " write of the same baseline (id " + reservation.id + ") is still awaiting"
                    + " publication (its id is consumed); retry the statement");
        }
        if (reservation.reserveTimeMicros <= 0) {
            // UNKNOWN age (the stored instant could not be read): condemning on an unknown
            // age tombstoned a possibly FRESH identity whose write is still awaiting
            // publication, and the fresh id allocated next to it published a duplicate.
            // Defer retryably instead - the reservation becomes readable with its time.
            throw new IllegalStateException("SPM cannot create baseline: the age of the"
                    + " reservation of id " + reservation.id + " cannot be proven (its stored"
                    + " timestamp is unreadable); nothing was changed, retry the statement");
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
                        + " than {} us and the row never became readable; condemning the"
                        + " identity (any late publication is repaired away) and allocating a"
                        + " fresh id",
                reservation.id, DURABLE_PENDING_CREATE_FENCE_MICROS);
        return null;
    }

    /**
     * Condemns one abandoned create identity: the append-only tombstone of
     * a write this FE gave up on. If the underlying INSERT was COMMITTED and its row
     * publishes later, every load filters it (and repairs it away) - the alternative was
     * a second ENABLED row published next to the fresh baseline the retry allocated.
     *
     * Parameter id: the abandoned id
     * Parameter bindSqlDigest: the baseline's canonical bind digest
     * Parameter planSqlHash: the hash of the baseline's plan SQL
     */
    private void condemnAbandonedIdentity(long id, String bindSqlDigest,
            long planSqlHash) {
        writeDroppedMarker(id, bindSqlDigest, planSqlHash);
    }

    /**
     * Condemns one DESERTED in-memory pending create and proves the tombstone DURABLE
     * before the caller may retire the entry / allocate a fresh id (see
     * resolvePendingCreate / reconcilePendingCreates). noteDroppedSeqState alone is best
     * effort on the wire: an unreadable tombstone fails the CREATE retryably instead of
     * letting the retry allocate a second id while the first COMMITTED row is still
     * unreadable (both rows could publish ENABLED after a restart lost this process's
     * marker).
     */
    private void condemnPendingIdentity(BaselinePlan plan) {
        if (plan.getBindSqlDigest() == null || plan.getPlanSql() == null) {
            return; // no identity to tombstone (noteDroppedSeqState writes nothing either)
        }
        noteDroppedSeqState(plan);
        confirmDroppedMarkerDurable(plan.getId(), plan.getBindSqlDigest(),
                identityPlanSqlHash(plan));
    }

    /**
     * Confirms one condemnation tombstone is READABLE before the caller allocates a
     * fresh id (see resolveDurablePendingCreate): the marker append is best effort on
     * the wire, so it is retried bounded, and a marker that never becomes readable
     * fails the CREATE retryably - allocating a second id next to a
     * COMMITTED-but-unreadable row whose only protection lived in this process's
     * memory let both rows publish as ENABLED after a restart / handoff.
     */
    private void confirmDroppedMarkerDurable(long id, String bindSqlDigest,
            long planSqlHash) {
        if (!persistence.isDurable()) {
            return;
        }
        String key = droppedIdentityKey(id, bindSqlDigest, planSqlHash);
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            writeDroppedMarker(id, bindSqlDigest, planSqlHash);
            // DURABLE-ONLY read: the process-local fallback of the scoped read marks
            // the very append this loop just failed to make as present (the failure put
            // the entry there), so the confirmation could pass without one readable
            // marker - and the caller would allocate a fresh id next to the abandoned,
            // possibly COMMITTED write.
            if (readDroppedIdentitiesDurable(Set.of(id)).contains(key)) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        throw new IllegalStateException("SPM cannot confirm the condemnation tombstone of"
                + " the abandoned baseline id " + id + "; retry the CREATE");
    }

    /**
     * Confirms the tombstones of a COMPLETED drop are READABLE before the DROP reports
     * success (see confirmDroppedMarkerDurable): the marker append is best effort on the
     * wire (writeDroppedMarker only warns), so a dropped id whose append never landed lets
     * a delayed write of the removed row - or the unreadable loser of a handoff collision -
     * revive the dropped baseline on every FE that filtered it in memory only. Retried
     * bounded, then the DROP is surfaced as retryable: the durable row is already gone, so
     * the retry converges by re-appending the same markers.
     *
     * Parameter removed: the rows this FE removed (one identity tombstone each)
     * Parameter id: the dropped id (the id-scoped tombstone)
     */
    private void confirmDropTombstonesDurable(Collection<BaselinePlan> removed,
            long id) {
        if (!persistence.isDurable()) {
            return;
        }
        oweDropIdTombstone(id);
        for (BaselinePlan row : removed) {
            oweDropTombstone(id, row);
        }
        confirmPendingDropTombstones(id);
    }

    /**
     * Confirms every retained scope of a DROP, including identities and twins removed by an earlier partial
     * attempt. Normal completion and absent-row retry share this fence: all history markers and compact
     * identity states must be readable before the obligation is released. A timeout retains every local mask.
     */
    private void confirmPendingDropTombstones(long id) {
        if (!persistence.isDurable()) {
            pendingDropTombstoneConfirmations.remove(id);
            pendingDroppedMarkers.values().removeIf(marker -> marker.id == id);
            return;
        }
        PendingDropObligation obligation = Objects.requireNonNull(pendingDropTombstoneConfirmations.get(id));
        Set<Long> scoped = new java.util.HashSet<>();
        scoped.add(id);
        for (PendingDropMarker marker : obligation.owed.values()) {
            scoped.add(marker.id);
        }
        List<PendingDropMarker> identities = identityMarkersOf(obligation);
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            Set<String> readable = readDroppedIdentitiesDurable(scoped);
            boolean complete = readable.contains(droppedIdKey(id));
            for (String key : obligation.owed.keySet()) {
                complete &= readable.contains(key);
            }
            if (complete) {
                complete = compactTombstonesVisible(identities);
            }
            if (complete) {
                pendingDropTombstoneConfirmations.remove(id);
                for (String key : obligation.owed.keySet()) {
                    pendingDroppedMarkers.remove(key);
                }
                LOG.info("SPM confirmed the drop tombstones of baseline {}", id);
                return;
            }
            writeDroppedMarker(id, "", 0L);
            for (PendingDropMarker marker : obligation.owed.values()) {
                if (marker.id == id && marker.bindSqlDigest.isEmpty()) {
                    continue; // the id-scoped marker was just re-appended
                }
                writeDroppedMarker(marker.id, marker.bindSqlDigest, marker.planSqlHash);
            }
            sleepBeforeVisibilityRetry();
        }
        for (PendingDropMarker marker : obligation.owed.values()) {
            pendingDroppedMarkers.putIfAbsent(marker.key(), marker);
        }
        throw new IllegalStateException("SPM cannot confirm the drop tombstones of baseline id "
                + id + " (the row is gone but its filter markers are not readable); retry the DROP");
    }

    /** The retained identity markers, excluding id-scoped markers. */
    private static List<PendingDropMarker> identityMarkersOf(PendingDropObligation obligation) {
        List<PendingDropMarker> markers = new ArrayList<>(obligation.owed.size());
        for (PendingDropMarker marker : obligation.owed.values()) {
            if (!marker.bindSqlDigest.isEmpty()) {
                markers.add(marker);
            }
        }
        return markers;
    }

    /** Checks that the identity slots agree with the confirmed id-keyed tombstones. */
    private boolean compactTombstonesVisible(Collection<PendingDropMarker> identities) {
        if (identities == null || identities.isEmpty()) {
            return true;
        }
        Set<Long> slots = new java.util.LinkedHashSet<>();
        for (PendingDropMarker identity : identities) {
            slots.add(compactSeqId(identity.bindSqlDigest, identity.planSqlHash));
        }
        Map<Long, BaselinePersistence.SeqReservation> states;
        try {
            states = persistence.readLatestReservations(slots, INTERNAL_QUERY_TIMEOUT_SECONDS);
        } catch (RuntimeException e) {
            throw new RuntimeException("SPM durable identity-state read failed (retry the DROP): "
                    + e.getMessage(), e);
        }
        for (PendingDropMarker identity : identities) {
            BaselinePersistence.SeqReservation state = states == null ? null
                    : states.get(compactSeqId(identity.bindSqlDigest, identity.planSqlHash));
            if (state == null) {
                state = persistence.readLatestReservation(identity.bindSqlDigest, identity.planSqlHash);
            }
            // A readable slot must show the matching tombstone or name a newer incarnation.
            if (state != null && (state.id < identity.id || state.id == identity.id && !state.dropped)) {
                return false;
            }
        }
        return true;
    }

    /** Reads the latest reservation using the full identity and its stable sort key. */
    private static BaselinePersistence.SeqReservation readPersistedSeqReservation(String bindSqlDigest,
            long planSqlHash) {
        return readPersistedSeqReservation(bindSqlDigest, planSqlHash, Long.MAX_VALUE);
    }

    private static BaselinePersistence.SeqReservation readPersistedSeqReservation(String bindSqlDigest,
            long planSqlHash, long deadlineMillis) {
        Map<String, String> params = new HashMap<>();
        params.put("compactId", String.valueOf(compactSeqId(bindSqlDigest, planSqlHash)));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(bindSqlDigest));
        params.put("planSqlHash", String.valueOf(planSqlHash));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_COMPACT_SEQ_SQL, params, remainingReadTimeout(deadlineMillis)));
            return rows == null || rows.isEmpty() ? null : parseSeqReservation(rows.get(0));
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline id sequence identity read failed (retry the"
                    + " CREATE): " + e.getMessage(), e);
        }
    }

    /** The latest state in an identity slot. */
    private static BaselinePersistence.SeqReservation parseSeqReservation(ResultRow row) {
        return parseSeqReservation(row, 0);
    }

    /**
     * The offset form of parseSeqReservation(ResultRow) (see
     * readLatestReservationsFromTable, whose read prefixes the compact slot id).
     *
     * Parameter row: the result row
     * Parameter offset: the index of the baseline_id column
     * Returns: the parsed state, or null when the row carries no id
     */
    private static BaselinePersistence.SeqReservation parseSeqReservation(ResultRow row,
            int offset) {
        String idText = row.getWithDefault(offset, "");
        if (idText == null || idText.isEmpty()) {
            return null;
        }
        String timeText = row.getValues().size() > offset + 1
                ? row.getWithDefault(offset + 1, "") : "";
        long reserveTime = parseStoredMicros(timeText);
        return new BaselinePersistence.SeqReservation(Long.parseLong(idText.trim()), reserveTime,
                isFlagSet(row, offset + 2), isFlagSet(row, offset + 3));
    }

    /**
     * The live implementation half of BaselinePersistence.readLatestReservations: ONE
     * bounded read over the requested compact slots, keeping the newest row of each slot in the
     * same newest-first order the single-identity read takes. A slot without a readable row is
     * simply absent from the result, which makes the caller read its full identity.
     *
     * Parameter compactIds: the compact slot ids
     * Parameter timeoutSeconds: the statement timeout
     * Returns: compact slot id to its newest compact record
     */
    private static Map<Long, BaselinePersistence.SeqReservation> readLatestReservationsFromTable(
            Collection<Long> compactIds, int timeoutSeconds) {
        if (!persistenceEnabled() || compactIds == null || compactIds.isEmpty()) {
            return Map.of();
        }
        StringBuilder ids = new StringBuilder();
        for (Long compactId : compactIds) {
            if (compactId == null) {
                continue;
            }
            if (ids.length() > 0) {
                ids.append(", ");
            }
            ids.append(compactId.longValue());
        }
        if (ids.length() == 0) {
            return Map.of();
        }
        Map<String, String> params = new HashMap<>();
        params.put("compactIds", ids.toString());
        List<ResultRow> rows;
        try {
            rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_COMPACT_SEQ_BATCH_SQL, params, timeoutSeconds));
        } catch (Exception e) {
            throw new RuntimeException("SPM batched baseline identity read failed: "
                    + e.getMessage(), e);
        }
        if (rows == null || rows.isEmpty()) {
            return Map.of();
        }
        Map<Long, BaselinePersistence.SeqReservation> states = new HashMap<>();
        for (ResultRow row : rows) {
            String idText = row.getWithDefault(0, "");
            if (idText == null || idText.isEmpty()) {
                continue;
            }
            long slot = Long.parseLong(idText.trim());
            if (states.containsKey(slot)) {
                continue; // the ORDER BY put the newest row of this slot first
            }
            BaselinePersistence.SeqReservation state = parseSeqReservation(row, 1);
            if (state != null) {
                states.put(slot, state);
            }
        }
        return states;
    }

    /**
     * Builds the persisted snapshot from the internal table: one BaselinePlan per row,
     * decoded SHALLOWLY (the persisted scalars only - the transient trees are rebuilt when
     * a row enters the match cache, see accumulateSnapshotRow / deepenPersistedRow).
     * Invalid rows are skipped with a warning (the next cycle retries).
     *
     * The read is PAGINATED and ordered by id: the table has no retention cap, so one
     * SELECT * had to return every row within the fixed per-query timeout - a
     * snapshot that outgrew it failed as a whole and never converged (the follower kept
     * its old published cache, local baseline DDL waited behind the held writer lock, and
     * confirmed SHOW / post-forward refreshes failed on the same path forever). Each page
     * is bounded by SNAPSHOT_PAGE_SIZE rows and its own timeout, and the loop
     * walks the id space forward until a short page ends the snapshot.
     */
    private Map<Long, BaselinePlan> readPersistedSnapshot() throws Exception {
        return resolveRefreshSnapshot(readRawRefreshSnapshot());
    }

    /**
     * The side-effect-FREE half of one snapshot read: the rows of one stable durable state
     * with the DROP tombstones still unmade. Split out of readPersistedSnapshot so
     * the refresh can run the expensive part (one internal query per page) OUTSIDE the
     * writer lock and keep only the repair and the publication serialized with the writers
     * (see refreshFromInternalTable).
     */
    private static final class RawRefreshSnapshot {
        /** live table: the RAW rows per id, so a tombstone can be matched per row */
        private final Map<Long, List<BaselinePlan>> rowsById;
        /** substituted store: the whole snapshot (a substituted store has no page reader) */
        private final Map<Long, BaselinePlan> whole;

        private RawRefreshSnapshot(Map<Long, List<BaselinePlan>> rowsById,
                Map<Long, BaselinePlan> whole) {
            this.rowsById = rowsById;
            this.whole = whole;
        }
    }

    /**
     * Reads the side-effect-free half of a snapshot (see RawRefreshSnapshot): no tombstone
     * match and no repair delete, so it is safe to run outside the writer lock. The live
     * table's pre-attempt settlement of this FE's HELD mutation windows stays in this half
     * on purpose - it only closes windows this FE itself opened (it never writes a row) and
     * it must run before the quiet gate of every read attempt.
     */
    private RawRefreshSnapshot readRawRefreshSnapshot() throws Exception {
        if (persistence instanceof TablePersistence) {
            return new RawRefreshSnapshot(readRawSnapshotFromTable(), null);
        }
        // a substituted store hands out the whole snapshot
        return new RawRefreshSnapshot(null, persistence.readSnapshot());
    }

    /**
     * The REPAIR half of a snapshot read: the DROP tombstones are matched and a revived row
     * is repaired away (see filterResurrectedRows / filterResurrectedRowsById). Runs under
     * the writer lock - both the tombstone read and the repair delete must not interleave
     * with a concurrent DROP / revive.
     *
     * Parameter raw: the side-effect-free half (see readRawRefreshSnapshot)
     * Returns: the snapshot to publish
     */
    private Map<Long, BaselinePlan> resolveRefreshSnapshot(RawRefreshSnapshot raw) {
        if (raw.rowsById != null) {
            // the raw rows are filtered BEFORE the per-id winner is picked: collapsing
            // first would let a condemned row remove a valid twin of the same id
            return filterResurrectedRowsById(raw.rowsById);
        }
        // The live loader applies the drop-tombstone filter per page (see
        // collectFilteredSnapshot); a substituted store hands out the whole snapshot, so
        // the same filter (and its repair of a revived row) runs here. The map is copied:
        // every publish path mutates the snapshot it is handed
        return filterResurrectedRows(new java.util.LinkedHashMap<>(raw.whole));
    }

    /** The live implementation half of BaselinePersistence.readSnapshot. */
    private Map<Long, BaselinePlan> readSnapshotFromTable() throws Exception {
        return filterResurrectedRowsById(readRawSnapshotFromTable());
    }

    /**
     * The RAW-row half of readSnapshotFromTable, WITHOUT the DROP-tombstone filter: the raw rows
     * of one stable state, so a refresh can read them outside the writer lock and keep only the
     * filter (and its best-effort repair delete) serialized with the writers (see
     * readRawRefreshSnapshot). The pre-attempt settlement probes this FE's HELD mutation windows
     * and closes the ones whose outcome is confirmed - the quiet gate of the read would
     * otherwise reject their pending marker forever (see
     * settleHeldMutationWindowsBeforeSnapshot). Settlement is limited to windows this FE opened.
     */
    private Map<Long, List<BaselinePlan>> readRawSnapshotFromTable() throws Exception {
        return readStableRawSnapshot(BaselineManager::readSnapshotPage,
                this::readSnapshotFence,
                this::settleHeldMutationWindowsBeforeSnapshot);
    }

    /** One page of the whole-table snapshot, read through the internal table. */
    private static List<ResultRow> readSnapshotPage(Long pageStart, long offset) throws Exception {
        return inInternalIoMode(() -> StatisticsUtil.executeQuery(
                snapshotPageSql(pageStart, offset), Collections.emptyMap(),
                INTERNAL_QUERY_TIMEOUT_SECONDS));
    }

    /**
     * As readStableRawSnapshot(SnapshotPageReader, SnapshotFenceReader), with a
     * PRE-ATTEMPT callback: the production read settles this FE's held mutation windows
     * here (see settleHeldMutationWindowsBeforeSnapshot) - the quiet gate below rejects a
     * visible pending marker, and the post-snapshot settlement
     * (resolveHeldMutationWindows) could never run because that gate failed first, so
     * refresh / GLOBAL SHOW / promotion reload bounced until the marker aged out although
     * the committed row had long become readable. Best effort: a failing settlement must
     * not mask the read itself (the gate still retries / ages out).
     */
    private static Map<Long, List<BaselinePlan>> readStableRawSnapshot(SnapshotPageReader reader,
            SnapshotFenceReader fenceReader, Runnable settleBeforeAttempt) throws Exception {
        for (int attempt = 1; ; attempt++) {
            if (settleBeforeAttempt != null) {
                try {
                    settleBeforeAttempt.run();
                } catch (RuntimeException e) {
                    LOG.warn("SPM pre-snapshot settlement of held mutation windows failed"
                            + " (best effort; the read continues): {}", e.getMessage());
                }
            }
            SnapshotFence before = fenceReader.readFence();
            AtomicLong rowsRead = new AtomicLong();
            SnapshotCompletion completion = new SnapshotCompletion();
            Map<Long, List<BaselinePlan>> snapshot =
                    collectSnapshotRows(reader, SNAPSHOT_PAGE_SIZE, rowsRead, completion);
            SnapshotFence after = fenceReader.readFence();
            // THREE conditions are required: the fence proves no mutation COMPLETED
            // around the read, BOTH fence reads must show no OPEN mutation window (the
            // tick moves BEFORE a mutation's statement, so two same-tick reads alone could
            // certify a snapshot whose pages straddle that statement - e.g. a DROP's
            // DELETE removing a group between page 1 and the OFFSET continuation, see
            // beginRowMutation), and the END PROBE proves the loop really reached the
            // table's end (see collectSnapshotRows: a silently TRUNCATED read - an
            // internal-query row limit cancelling a page - looks exactly like a short,
            // completed page, and the probe is the bounded replacement of the former
            // table-wide row-count cross-check).
            if (before.matches(after) && before.quiet() && after.quiet()
                    && completion.endProven) {
                return snapshot;
            }
            LOG.warn("SPM baseline table changed while its paginated snapshot was read"
                            + " ({} -> {}, rows read {}, end proven {}, attempt {}/{})",
                    before, after, rowsRead.get(), completion.endProven, attempt,
                    SNAPSHOT_STABILITY_ATTEMPTS);
            if (attempt >= SNAPSHOT_STABILITY_ATTEMPTS) {
                throw new IllegalStateException("SPM baseline table kept changing while its"
                        + " paginated snapshot was read (concurrent CREATE / ALTER / DROP,"
                        + " or a truncated read; retry the operation)");
            }
        }
    }

    /** The fence of one paginated snapshot read (see readStableRawSnapshot). */
    static final class SnapshotFence {
        final long tickMax;
        final long tickCount;
        final long tickSum;
        /** The tick of an OPEN mutation window (0 = none; see beginRowMutation). */
        final long pending;

        SnapshotFence(long tickMax, long tickCount, long tickSum) {
            this(tickMax, tickCount, tickSum, 0);
        }

        SnapshotFence(long tickMax, long tickCount, long tickSum, long pending) {
            this.tickMax = tickMax;
            this.tickCount = tickCount;
            this.tickSum = tickSum;
            this.pending = pending;
        }

        boolean matches(SnapshotFence other) {
            return tickMax == other.tickMax && tickCount == other.tickCount
                    && tickSum == other.tickSum;
        }

        /**
         * Whether this fence proves no mutation window was OPEN at its read: the tick
         * tuple alone cannot (the tick advances BEFORE the mutation's statement - see
         * beginRowMutation), so a read that observed an open window must retry.
         */
        boolean quiet() {
            return pending == 0;
        }

        @Override
        public String toString() {
            return "(clock=" + tickMax + ", ticks=" + tickCount
                    + ", tickSum=" + tickSum + ", pending=" + pending + ")";
        }
    }

    /** Reads the fence token of the snapshot read (see readStableRawSnapshot). */
    @FunctionalInterface
    interface SnapshotFenceReader {
        SnapshotFence readFence() throws Exception;
    }

    /** Reads SELECT_CLOCK_SQL (one all-NULL row when the slot is empty). */
    private SnapshotFence readSnapshotFence() throws Exception {
        long[] clock = persistence.clock();
        return new SnapshotFence(clock[0], clock[1], clock[2], clock[3]);
    }

    /** The live implementation half of BaselinePersistence.clock. */
    private static long[] clockFromTable() {
        if (!persistenceEnabled()) {
            return new long[] {0L, 0L, 0L, 0L};
        }
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_CLOCK_SQL, Collections.emptyMap(), INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return new long[] {0L, 0L, 0L, 0L};
            }
            ResultRow row = rows.get(0);
            return new long[] {
                    parseWatermark(row.getWithDefault(0, "")),
                    parseWatermark(row.getWithDefault(1, "")),
                    parseWatermark(row.getValues().size() > 2 ? row.getWithDefault(2, "") : ""),
                    parseWatermark(row.getValues().size() > 3 ? row.getWithDefault(3, "") : "")};
        } catch (Exception e) {
            throw new RuntimeException("SPM mutation clock read failed (retry the"
                    + " operation): " + e.getMessage(), e);
        }
    }

    /** Whether readStableRawSnapshot's pagination loop PROVED it reached the table's end. */
    static final class SnapshotCompletion {
        boolean endProven;
    }

    /**
     * The pagination loop itself: the RAW rows of the whole table grouped per id. The
     * per-id winner is picked AFTER the tombstone filter (see
     * filterResurrectedRowsById), so a tombstoned row never hides a valid twin. Invalid
     * rows are skipped with a warning.
     *
     * When the caller passes a completion sink, every short / empty page is followed by an
     * end probe (the same page read started one position past the last consumed row): only
     * an empty probe proves the read was not silently TRUNCATED - an internal-query row
     * limit cancelling a page looks exactly like a completed one, and the probe is the
     * bounded replacement of the former table-wide row-count cross-check (see
     * readStableRawSnapshot).
     */
    private static Map<Long, List<BaselinePlan>> collectSnapshotRows(SnapshotPageReader reader,
            int pageSize, AtomicLong rowsReadSink, SnapshotCompletion completion) throws Exception {
        Map<Long, List<BaselinePlan>> rowsById = new HashMap<>();
        Long bound = null;
        long skipped = 0;
        if (completion != null) {
            completion.endProven = false;
        }
        while (true) {
            List<ResultRow> rows = reader.readPage(bound, skipped);
            if (rows == null || rows.isEmpty()) {
                if (completion != null) {
                    // a re-read of the same cursor must stay empty, or rows the first read
                    // swallowed (a cancelled query) would end the snapshot early
                    List<ResultRow> probe = reader.readPage(bound, skipped);
                    completion.endProven = probe == null || probe.isEmpty();
                }
                return rowsById;
            }
            rowsReadSink.addAndGet(rows.size());
            for (ResultRow row : rows) {
                accumulateSnapshotRow(rowsById, row);
            }
            String lastRowId = rows.get(rows.size() - 1).getWithDefault(0, "");
            if (rows.size() < pageSize || lastRowId.isEmpty()) {
                // a short page ends the snapshot - but only when nothing follows it
                if (completion != null) {
                    completion.endProven = lastRowId.isEmpty()
                            ? true // an id-less row: the cursor cannot advance
                            : probeSnapshotEnd(reader, rows, lastRowId, bound, skipped);
                }
                return rowsById;
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
     * The end probe of a short page (see collectSnapshotRows): reads the page started one
     * position past the page's last consumed row and reports whether the continuation is
     * empty, i.e. the short page really was the end of the table.
     */
    private static boolean probeSnapshotEnd(SnapshotPageReader reader, List<ResultRow> rows,
            String lastRowId, Long bound, long skipped) throws Exception {
        long pageLastId = Long.parseLong(lastRowId.trim());
        long trailing = 0;
        for (int i = rows.size() - 1; i >= 0; i--) {
            String rowId = rows.get(i).getWithDefault(0, "");
            if (rowId.isEmpty() || Long.parseLong(rowId.trim()) != pageLastId) {
                break;
            }
            trailing++;
        }
        long probeOffset = bound != null && pageLastId == bound ? skipped + trailing : trailing;
        List<ResultRow> probe = reader.readPage(pageLastId, probeOffset);
        return probe == null || probe.isEmpty();
    }

    /** Reads one page of the snapshot (see collectSnapshotRows). */
    @FunctionalInterface
    interface SnapshotPageReader {
        List<ResultRow> readPage(Long pageStart, long offset) throws Exception;
    }

    /**
     * Builds the first page or a continuation page using the shared total stored-column ordering. A continuation
     * includes ids at or above pageStart and skips offset rows in that range. The page size is SNAPSHOT_PAGE_SIZE.
     */
    private static String snapshotPageSql(Long pageStart, long offset) {
        if (pageStart == null) {
            return SELECT_ALL_ORDERED_SQL + " LIMIT " + SNAPSHOT_PAGE_SIZE;
        }
        return SELECT_PAGE_SQL.replace("${lastId}", Long.toString(pageStart))
                .replace("${pageSize}", Integer.toString(SNAPSHOT_PAGE_SIZE))
                .replace("${offset}", Long.toString(offset));
    }

    /**
     * Parses one raw snapshot row into its id's group (see collectSnapshotRows): the
     * deterministic per-id winner is picked later, after the tombstone filter.
     */
    private static void accumulateSnapshotRow(Map<Long, List<BaselinePlan>> rowsById,
            ResultRow row) {
        try {
            // The snapshot decode is SHALLOW (see shallowPersistedRow): a load / refresh
            // publishes the hash index, settles the mutation fences, filters the drop
            // tombstones, collapses the duplicate keys and advances the id watermark - all
            // of which read the persisted SCALARS only. The transient parameterized trees
            // are rebuilt LAZILY, the moment a row enters the per-hash match cache (see
            // deepenPersistedRow): a durable-mode load keeps the HASHES only, so parsing
            // every row of the table was pure overhead on the load / refresh path.
            BaselinePlan p = shallowPersistedRow(row);
            rowsById.computeIfAbsent(p.getId(), id -> new ArrayList<>(2)).add(p);
        } catch (Throwable t) {
            LOG.warn("SPM skip invalid persisted baseline row: {}", t.getMessage());
        }
    }

    /**
     * Collapses the raw per-id row groups to one DETERMINISTIC winner per id. The read
     * order must not decide: "last read wins" would make refresh / restart pick the
     * status NONDETERMINISTICALLY - a failed DISABLE could be silently re-enabled. Two
     * rows carry the same id when, e.g., an ALTER status update's compensating delete
     * failed or an out-of-contract manual write happened.
     */
    private static Map<Long, BaselinePlan> collapseSnapshotRows(
            Map<Long, List<BaselinePlan>> rowsById) {
        Map<Long, BaselinePlan> snapshot = new HashMap<>(rowsById.size());
        for (Map.Entry<Long, List<BaselinePlan>> entry : rowsById.entrySet()) {
            BaselinePlan winner = null;
            for (BaselinePlan row : entry.getValue()) {
                if (row == null) {
                    continue;
                }
                if (winner == null) {
                    winner = row;
                    continue;
                }
                // deterministic winner (see pickDurableWinner) so every FE / restart
                // converges on it
                BaselinePlan chosen = pickDurableWinner(winner, row);
                LOG.warn("SPM persisted baseline id {} appears in more than one row"
                                + " (statuses {} / {}, update times {} / {});"
                                + " deterministically keeping the {} row",
                        entry.getKey(), winner.getStatus(), row.getStatus(),
                        winner.getUpdateTime(), row.getUpdateTime(), chosen.getStatus());
                winner = chosen;
            }
            if (winner != null) {
                snapshot.put(entry.getKey(), winner);
            }
        }
        return snapshot;
    }

    /**
     * Decodes one persisted row SHALLOWLY: every persisted SCALAR is read, the transient
     * parameterized trees are NOT built (the row is marked, see BaselinePlan.isTreesPending).
     * The load / refresh snapshot exists to serve the load-time invariants - the hash
     * index, the mutation-fence settlement, the drop-tombstone filter, the duplicate-key
     * collapse and the id watermark - and none of them parses SQL: rebuilding every row's
     * trees was the load's dominant cost although a durable-mode load keeps only the HASHES
     * (the rows enter the match cache - and thereby get their trees - only when matched).
     *
     * The temporary-table guard belongs to the shallow decode as well: it is a TEXT-level
     * check, and a row it rejects must not enter the index / fence state at all.
     */
    private static BaselinePlan shallowPersistedRow(ResultRow row) throws Exception {
        BaselinePlan p = fromRow(row);
        // Fail closed on legacy temporary-table rows: before the create-time rejection,
        // a GLOBAL baseline over a temporary table froze the CREATOR session's internal
        // table name into planSql (sessionId_#TEMP._name). Replaying it from another
        // session would read the creator's (possibly still live) temporary table, so such
        // rows are never loaded (the log line names the row).
        if (referencesTemporaryTable(p.getBindSql(), p.getCreatorSqlMode())
                || referencesTemporaryTable(p.getPlanSql(), storedPlanSqlMode(p))) {
            throw new RuntimeException("SPM baseline " + p.getId()
                    + " references a temporary table (the frozen plan carries the creator"
                    + " session's internal name); skipping the row");
        }
        p.setTreesPending(true);
        return p;
    }

    /**
     * Reads one persisted row in FULL: the shallow decode plus the transient parameterized
     * trees (see deepenPersistedRowOrThrow). The by-hash / by-id / by-key reads use this
     * form - their rows enter the cache or serve as baselines right away - while the
     * snapshot read decodes shallowly and deepens LAZILY (see deepenPersistedRow).
     */
    private static BaselinePlan parsePersistedRow(ResultRow row) throws Exception {
        return deepenPersistedRowOrThrow(shallowPersistedRow(row));
    }

    /**
     * Rebuilds the transient trees of one row and clears its pending mark; THROWS when the
     * bindSql cannot be parsed (see parsePersistedRow). The PERSISTED provenance
     * (plan_frozen) is AUTHORITATIVE when the row carries it: an explicitly NOT-frozen
     * row's planSql is ordinary user SQL by construction (the raw fallback or a failed
     * decompile), and re-classifying it by RE-PARSING the text is exactly how a legal
     * unqualified UDF call named _spm_const_var(1) - the decompiler refuses such a
     * function, so CREATE keeps the raw text and records plan_frozen=false - got flipped to
     * frozen on reload: the marker-shaped parse (an unqualified single-integer-argument call
     * IS the marker syntax) hid the real UDF, and the frozen replay then replaced the call
     * with the caller's literal (a matching SELECT _spm_const_var(2) returned 2 instead of
     * evaluating the function). Only a row WITHOUT provenance (a legacy pre-column row)
     * falls back to the parse-based classifier.
     */
    private static BaselinePlan deepenPersistedRowOrThrow(BaselinePlan p) throws Exception {
        String planSql = p.getPlanSql();
        boolean frozen = SPMPlanner.isFrozenPlanSql(planSql, p.getPlanFrozen());
        // Rebuild the transient trees with ONE shared builder over both texts in
        // the CREATE order (bind first, then plan), so the placeholder ids of the
        // two trees stay aligned and a value extracted from the bind tree can
        // never be substituted into a literal slot of the other tree. Frozen
        // (placeholder-carrying) planSql is replayed as text - no plan tree. The
        // non-frozen planSql is either the SPM decompiled text (MODE_DEFAULT) or the
        // user's raw fallback text (creator mode) - plan_sql_mode carries which one.
        Pair<LogicalPlan, LogicalPlan> trees = SPMPlanner.rebuildParameterizedTrees(
                p.getBindSql(), frozen ? null : planSql, p.getCreatorSqlMode(), storedPlanSqlMode(p));
        if (trees.first == null) {
            throw new RuntimeException("SPM baseline " + p.getId()
                    + " bindSql cannot be parsed");
        }
        p.setParameterizedBindPlan(trees.first);
        if (!frozen) {
            p.setParameterizedPlanPlan(trees.second);
        }
        p.setTreesPending(false);
        return p;
    }

    /**
     * The LAZY deep parse of a (possibly shallow) row: a row the snapshot decode marked
     * gets its transient trees rebuilt here - at the moment it ENTERS the match cache (see
     * BaselineRowCache.mergeEntry / #putCompleteLocked) - while a row that already carries
     * them (a publish-path row, a by-hash read, a test-store row) is returned unchanged, so
     * this is idempotent.
     *
     * An unusable row fails CLOSED with null: it must never serve a match / rewrite (the
     * placeholder substitution would run against a null tree), and the caller skips it -
     * exactly like the former eager snapshot parse, which simply never published such a row.
     */
    private static BaselinePlan deepenPersistedRow(BaselinePlan p) {
        if (p == null || !p.isTreesPending()) {
            return p;
        }
        try {
            return deepenPersistedRowOrThrow(p);
        } catch (Throwable t) {
            LOG.warn("SPM skip the persisted baseline {} whose transient trees cannot be"
                    + " rebuilt: {}", p.getId(), t.getMessage());
            return null;
        }
    }

    private static long storedPlanSqlMode(BaselinePlan plan) {
        return plan.getPlanSqlMode() == null ? SqlModeHelper.MODE_DEFAULT : plan.getPlanSqlMode();
    }

    /**
     * Whether a stored text REFERENCES the creator session's temporary-table name. The
     * marker is looked up in the PARSED relations only - never as a raw substring: an
     * ordinary predicate / value literal (s = '_#TEMP._') or a comment carries the
     * same characters, and the old substring test rejected such a durable row on every
     * refresh - the baseline silently disappeared from every FE although CREATE had
     * accepted it (the create-time guard inspects the RESOLVED relations). Use the
     * stored SQL mode so quoted values are decoded as they were at capture.
     */
    private static boolean referencesTemporaryTable(String text, long sqlMode) {
        if (text == null || !text.contains(FeNameFormat.TEMPORARY_TABLE_SIGN)) {
            return false;
        }
        try {
            Plan parsed = SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(text, sqlMode);
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
     * CREATE dedup lookup. A newer durable watermark requires a by-key table read. Otherwise a complete cached
     * hash bucket can answer the lookup; a cold or evicted bucket still requires a durable read. Remembering an
     * id watermark does not retain its rows. Read failures propagate instead of allowing an uncertain INSERT.
     *
     * Parameter plan: the baseline being created
     * Parameter tableWatermark: durable allocation watermark confirmed by the caller
     * Returns: the rows carrying the plan's (bind_sql_digest, plan_sql) key
     */
    private List<BaselinePlan> readPersistedRowsForCreate(BaselinePlan plan, long tableWatermark) {
        if (!(persistence instanceof TablePersistence)) {
            // Injected stores use their cache or persistence API; the static SQL helper belongs to the live store.
            return readRowsByKeyFromCacheOrTable(plan.getBindSqlHash(), plan.getBindSqlDigest(),
                    plan.getPlanSql());
        }
        if (hasUnseenDurableIds(tableWatermark) || plan.getBindSqlHash() == 0) {
            // the store may miss durable rows (or cannot index this key): the scanned
            // read is the only COMPLETE answer
            return readPersistedByKey(plan.getBindSqlDigest(), plan.getPlanSql());
        }
        return readRowsByKeyFromCacheOrTable(plan.getBindSqlHash(), plan.getBindSqlDigest(), plan.getPlanSql());
    }

    /**
     * Whether the allocation watermark is above the largest id observed by a load or local publication. False
     * permits checking a complete cached bucket, but does not prove that it is cached.
     *
     * Parameter tableWatermark: durable allocation watermark (zero when none)
     */
    private boolean hasUnseenDurableIds(long tableWatermark) {
        return tableWatermark > maxPersistedIdSeen;
    }

    /**
     * Finds the exact bind digest and plan SQL in one complete cached bucket. A missing bucket is not evidence
     * of absence: the live store reads by key, and an injected store with authoritative hash reads uses that
     * persistence API. Stores without hash reads retain all rows in their in-memory cache.
     */
    private List<BaselinePlan> readRowsByKeyFromCacheOrTable(long bindSqlHash, String bindSqlDigest,
            String planSql) {
        List<BaselinePlan> result = new ArrayList<>();
        List<BaselinePlan> rows = baselineCache.getComplete(bindSqlHash);
        if (rows == null) {
            if (persistence instanceof TablePersistence) {
                return readPersistedByKey(bindSqlDigest, planSql);
            }
            rows = durableHashReadAvailable()
                    ? readPersistedRowsByHash(bindSqlHash, Long.MAX_VALUE) : List.of();
        }
        for (BaselinePlan row : rows) {
            if (Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                    && Objects.equals(row.getPlanSql(), planSql)) {
                result.add(row);
            }
        }
        return result;
    }

    /**
     * Reads every durable row with the given (bind_sql_digest, plan_sql) key. A read
     * failure is rethrown as a retryable error: createBaseline must not fall through to
     * an INSERT while the durable duplicate state is unknown.
     *
     * Parameter bindSqlDigest: the parameterized digest
     * Parameter planSql: the frozen plan SQL
     * Returns: the parsed rows (possibly empty)
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
     * Selects a durable winner independently of input order. A later epoch-microsecond update time wins; tied
     * times prefer an inactive status. Remaining ties compare every other persisted scalar and finally the id,
     * retaining the smaller content key. Rows with identical stored metadata are interchangeable.
     */
    private static BaselinePlan pickDurableWinner(BaselinePlan first, BaselinePlan second) {
        if (first.getUpdateTime() != second.getUpdateTime()) {
            return first.getUpdateTime() > second.getUpdateTime() ? first : second;
        }
        boolean firstDisabled = !first.getStatus().isActive();
        boolean secondDisabled = !second.getStatus().isActive();
        if (firstDisabled != secondDisabled) {
            return firstDisabled ? first : second;
        }
        // Fully tied (same stored instant, same status): the winner must be DETERMINISTIC
        // two masters can leave two DIFFERENT rows of one id with the
        // same stored instant (a delayed INSERT committing after a handoff collision),
        // and an arbitrary pick made refresh / restart / SHOW / the paginated read
        // disagree on which row is authoritative. The CONTENT is a total,
        // order-independent tie-breaker; fully identical rows stay interchangeable.
        return durableContentComparator.compare(first, second) <= 0 ? first : second;
    }

    /**
     * Requests an authoritative reload on master acquisition. The match index and cached buckets may predate
     * the previous master's writes; reloading rebuilds their published state from the durable table. loaded is
     * cleared
     * first, so a failed read keeps the lazy-retry state machine intact (the next access
     * retries; mutators fail visibly via ensureLoadedOrThrow until the read succeeds).
     */
    public void forceReloadFromInternalTable() {
        if (!durableWritesEnabled()) {
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
                knownHashes = new ConcurrentHashMap<>();
                knownHashSignatures = new ConcurrentHashMap<>();
                baselineCache.clear();
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
                || !Objects.equals(memory.getPlanSqlDigest(), row.getPlanSqlDigest())
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
     * Copies changed creation and update timestamps into the cached object using exact epoch-microsecond equality.
     * Timestamp-only refreshes keep the existing transient trees.
     */
    private static boolean copyPersistedTimestamps(BaselinePlan memory, BaselinePlan row) {
        boolean copied = false;
        if (!sameStoredInstant(memory.getUpdateTime(), row.getUpdateTime())) {
            memory.setUpdateTime(row.getUpdateTime());
            copied = true;
        }
        if (!sameStoredInstant(memory.getCreateTime(), row.getCreateTime())) {
            memory.setCreateTime(row.getCreateTime());
            copied = true;
        }
        return copied;
    }

    /**
     * Whether the persisted change invalidates the cached row's TRANSIENT SQL TREES (see
     * mergeEntry): any SQL text / frozen payload / identity-column change needs a rebuild,
     * while the status / timing / cost / query-id columns are plain scalars the cached
     * object adopts in place (adoptPersistedScalars).
     */
    private static boolean persistedTreesChanged(BaselinePlan memory, BaselinePlan row) {
        return !Objects.equals(memory.getBindSql(), row.getBindSql())
                || !Objects.equals(memory.getBindSqlDigest(), row.getBindSqlDigest())
                || memory.getBindSqlHash() != row.getBindSqlHash()
                || !Objects.equals(memory.getPlanSql(), row.getPlanSql())
                || memory.getSource() != row.getSource()
                || memory.getCreatorSqlMode() != row.getCreatorSqlMode()
                || !Objects.equals(memory.getPlanSqlMode(), row.getPlanSqlMode())
                || !Objects.equals(memory.getPlanFrozen(), row.getPlanFrozen())
                || !Objects.equals(memory.getSchemaFingerprint(), row.getSchemaFingerprint());
    }

    /**
     * Adopts the persisted SCALAR columns into a cached object whose SQL trees are still
     * valid (see persistedTreesChanged).
     *
     * Parameter memory: the cached object (mutated in place)
     * Parameter row: the persisted row
     * Returns: whether anything was adopted
     */
    private static boolean adoptPersistedScalars(BaselinePlan memory, BaselinePlan row) {
        boolean adopted = copyPersistedTimestamps(memory, row);
        if (memory.getStatus() != row.getStatus()) {
            memory.setStatus(row.getStatus());
            adopted = true;
        }
        if (!Objects.equals(memory.getQueryId(), row.getQueryId())) {
            memory.setQueryId(row.getQueryId());
            adopted = true;
        }
        if (Double.compare(memory.getCost(), row.getCost()) != 0) {
            memory.setCost(row.getCost());
            adopted = true;
        }
        if (memory.getQueryTimeMs() != row.getQueryTimeMs()) {
            memory.setQueryTimeMs(row.getQueryTimeMs());
            adopted = true;
        }
        if (!Objects.equals(memory.getPlanSqlDigest(), row.getPlanSqlDigest())) {
            memory.setPlanSqlDigest(row.getPlanSqlDigest());
            adopted = true;
        }
        return adopted;
    }

    /** Compares stored epoch-microsecond timestamps exactly. */
    private static boolean sameStoredInstant(long memoryMicros, long rowMicros) {
        return memoryMicros == rowMicros;
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
        p.setCreateTime(parseStoredMicros(row.get(10)));
        p.setUpdateTime(parseStoredMicros(row.get(11)));
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
     * The conditional status INSERT with the DURABLE-ORDER term (see
     * INSERT_IF_PREVIOUS_STATUS_SQL): the statement also requires that NO newer row of the
     * id exists, i.e. that the state it flips from is still the latest intent.
     *
     * A status statement that stalled past its shared mutation window (or crossed a leader
     * handoff) used to find its previous-status precondition intact and land with a stored
     * timestamp derived from the row it saw - reversing a successor's successful no-op ENABLE,
     * which wrote nothing at all. A successor now writes its own intent (see
     * persistNoOpStatusIntent) and this term voids the older statement's precondition, so the
     * late flip writes nothing and reports a retryable conflict instead.
     *
     * Parameter previousUpdateMicros: the stored INSTANT the flip is based on, in epoch
     *                             microseconds
     *                             (0 = unknown)
     * Returns: the statement text
     */
    static String conditionalStatusInsertSql(long previousUpdateMicros) {
        if (previousUpdateMicros <= 0) {
            return INSERT_IF_PREVIOUS_STATUS_SQL;
        }
        return INSERT_IF_PREVIOUS_STATUS_SQL.replace(" LIMIT 1",
                " AND NOT EXISTS (SELECT 1 FROM " + SPM_BASELINES_TABLE
                        + " WHERE `id` = ${id} AND " + storedMicros("`update_time`")
                        + " > ${previousUpdateMicros}) LIMIT 1");
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
    private boolean persistTransitionInsert(BaselinePlan p, BaselineStatus previousStatus,
            long previousUpdateMicros) {
        if (!persistence.isDurable()) {
            return true; // in-memory only (no durable status rows exist)
        }
        // open the mutation window BEFORE the status statement and close it after (see
        // beginRowMutation): a snapshot read that overlaps the flip must retry
        long mutation = beginRowMutation();
        boolean windowHeld = false;
        try {
            try {
                if (!persistence.insertIfPreviousPresent(p, previousStatus,
                        previousUpdateMicros)) {
                    return false; // nothing written: the previous-status row was gone
                }
            } catch (RuntimeException e) {
                // the store models the same ambiguous outcome as the live table: the
                // insert reported an error that may have been raised AFTER a commit
                throwAmbiguousStatusInsertUnlessDurable(p, e);
            }
            confirmInsertVisible(p);
            return true;
        } catch (AmbiguousInsertException | UnconfirmedVisibleException e) {
            // The window must stay open on BOTH unresolved outcomes: the conditional INSERT
            // either WROTE its row (the affected-row evidence) and only its publication lags
            // every probe, or it reported an ERROR that may have been raised after a commit
            // and no read can confirm it yet. Closing the window here - the normal finally -
            // reported the flip as SETTLED to the whole cluster: a successor whose fence read
            // saw the QUIET clock concluded on the OLD status row and accepted ALTER... ENABLE
            // as a no-op while this possibly-committed DISABLED row was still on its way, and
            // the row then reversed that successful ALTER. Hold the window - a SHARED
            // unresolved fence - until a snapshot read here proves the row readable (then the
            // window is closed, see resolveHeldMutationWindows) or it ages past
            // MUTATION_WINDOW_MAX_AGE_SECONDS, the same bound that heals an abandoned window.
            windowHeld = holdUnresolvedMutationWindow(mutation, p);
            throw e;
        } finally {
            if (!windowHeld) {
                endRowMutation(mutation);
            }
        }
    }

    /**
     * Whether the conditional status INSERT actually WROTE a row: a conditional
     * INSERT... SELECT... WHERE... that matches no row reports SQL OK with ZERO
     * affected rows, and that count is the only durable signal that separates "wrote
     * nothing" from "written but not yet readable" (see updateStatus). An
     * unknown count (-1) is NOT proof of a zero write, so it counts as written and is then
     * subject to the visibility confirmation.
     */
    private static boolean insertWroteRows(long affectedRows) {
        return affectedRows != 0;
    }

    /**
     * Runs the conditional status INSERT and reports whether a row was actually WRITTEN.
     *
     * INSERT... SELECT... WHERE status = previousStatus reports SQL OK with
     * ZERO affected rows when the previous-status row is gone (a concurrent DROP, or a
     * handoff flip that already moved the status away and back): the requested status was
     * NOT written and a plain visibility confirmation would misread the no-op as a
     * publication lag (the concurrent-write scenario). The affected-row count is the
     * only durable signal that separates the two, so the caller can refuse to publish an
     * unobserved status.
     *
     * Returns: true when the new-status row was written; false when the statement matched no
     *         previous-status row and wrote nothing
     */
    private boolean writeConditionalStatusInsert(BaselinePlan p,
            BaselineStatus previousStatus, long previousUpdateMicros) {
        Map<String, String> params = insertParams(p);
        params.put("previousStatus", previousStatus.name());
        params.put("previousUpdateMicros", String.valueOf(Math.max(0L, previousUpdateMicros)));
        // insertParams already bound schemaFingerprint: the WHERE predicate couples the
        // previous-status row to the CACHED row's fingerprint, so a reused id carrying a
        // DIFFERENT incarnation's (digest, planSql, fingerprint) cannot be flipped
        try {
            QueryState state = inInternalIoMode(() -> StatisticsUtil.execUpdate(
                    conditionalStatusInsertSql(previousUpdateMicros), params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
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
            // THE attempted stored timestamp is the proof it landed (the previous-status row
            // would not prove it - it is exactly the row the conditional statement must
            // not have matched; and the status ALONE could be a STALE row left by a
            // previously failed old-row delete). When no read can prove it, the error
            // must travel as an AMBIGUOUS outcome (see
            // throwAmbiguousStatusInsertUnlessDurable): the ordinary RuntimeException used
            // to bypass the unresolved-window handler of persistTransitionInsert, whose
            // finally then closed the shared window although the row may have committed
            // .
            throwAmbiguousStatusInsertUnlessDurable(p, e);
            return true; // the row IS durable (the helper only returns then)
        }
    }

    /**
     * Classifies an ERROR of the conditional status INSERT (see writeConditionalStatusInsert).
     *
     * A reported error may have been raised AFTER a commit (a statement timeout commits
     * while the error travels), so its outcome is AMBIGUOUS, never a proven failure. A
     * READABLE row proves the write landed and the caller keeps it; otherwise the error must
     * surface as AmbiguousStatusInsertException - NOT as an ordinary
     * RuntimeException - so that persistTransitionInsert keeps its SHARED mutation window
     * open (a successor must not conclude on the old-status row) while updateStatus still
     * reports the flip as UNPROVEN (it must never publish a status no read observed: the
     * committed-row evidence of the confirmed path is missing here).
     *
     * Parameter p: the row the write attempted
     * Parameter cause: the reported error
     */
    private void throwAmbiguousStatusInsertUnlessDurable(BaselinePlan p, Exception cause) {
        if (observedInsertRowIsOurs(p)) {
            LOG.warn("SPM persist (status insert) reported {} but the row is durable"
                    + " (id={}); keeping it", cause.getMessage(), p.getId());
            return;
        }
        throw new AmbiguousStatusInsertException("SPM persist (status insert) of baseline "
                + p.getId() + " reported an error and may still have COMMITTED (a committed"
                + " row is only waiting for publication): " + cause.getMessage()
                + " - the flip is not published and its mutation window stays shared;"
                + " retry the statement", cause);
    }

    /** The retryable conflict of a status flip whose conditional INSERT matched no row. */
    private static IllegalStateException statusConflict(long id, BaselineStatus previousStatus) {
        return new IllegalStateException("SPM cannot change the status of baseline " + id
                + ": its " + previousStatus + " row was gone when the conditional flip"
                + " wrote (a concurrent DROP or status flip won); retry the statement");
    }

    private void persistInsert(BaselinePlan p) {
        if (!persistence.isDurable()) {
            return; // in-memory only (no durable table exists)
        }
        // Open the mutation WINDOW before the row write and keep it open ACROSS the
        // INSERT (see beginRowMutation): the tick tuple alone only proves the bump was
        // visible, while an overlapping snapshot read must also see the statement itself
        // as either pending (window open) or complete (tick moved).
        long mutation = beginRowMutation();
        try {
            try {
                persistence.insert(p);
            } catch (RuntimeException e) {
                // An outcome that may have been raised AFTER a commit is AMBIGUOUS: the
                // attempted row is adopted instead of failing the write outright.
                throwAmbiguousInsertUnlessDurable(p, e);
            }
            // A reported SUCCESS still does not prove the row is READABLE: the default
            // insert return mode accepts SQL OK with the transaction merely COMMITTED
            // (publication timed out). Publishing the id into this FE's cache and returning
            // it would let a leadership change before publication re-allocate the same id
            // from an older MAX(id) - both rows then become visible and the winner rule
            // silently discards one version.
            confirmInsertVisible(p);
        } finally {
            endRowMutation(mutation);
        }
    }

    /** The live implementation half of BaselinePersistence.insert. */
    private void insertIntoTable(BaselinePlan p) {
        if (!durableWritesEnabled()) {
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
            // stored SECOND is the proof it landed. An outcome that cannot be PROVEN
            // counts as AMBIGUOUS, never as a genuine failure.
            throwAmbiguousInsertUnlessDurable(p, e);
        }
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
        // EVERY column of the internal tables is NOT NULL (this release publishes the
        // schema for the first time, see InternalSchema): a row whose model field is unset
        // binds the SAFE value instead of a NULL - MODE_DEFAULT for the plan's parser mode
        // and FALSE for "the stored planSql is the decompiled frozen rendering" (FALSE means
        // the text is ordinary user SQL whose parameterized tree is rebuilt, i.e. no
        // placeholder is ever substituted into a text that may not carry any).
        params.put("planSqlMode",
                String.valueOf(p.getPlanSqlMode() == null
                        ? SqlModeHelper.MODE_DEFAULT : p.getPlanSqlMode()));
        params.put("planFrozen", String.valueOf(Boolean.TRUE.equals(p.getPlanFrozen())));
        params.put("schemaFingerprint",
                StatisticsUtil.escapeSQL(p.getSchemaFingerprint() == null
                        ? "" : p.getSchemaFingerprint()));
        params.put("planSqlDigest",
                StatisticsUtil.escapeSQL(p.getPlanSqlDigest() == null
                        ? "" : p.getPlanSqlDigest()));
        return params;
    }

    /**
     * The INSERT reported an ERROR that may have been raised AFTER a commit: the row
     * MAY be durable, but no read can prove it yet. The caller must NOT publish the
     * row; it must reconcile or retry (a CREATE remembers the attempted identity so its
     * retry defers / adopts instead of allocating a second id, a status flip reconciles
     * the cache against a winner that IS readable and reports the failure).
     */
    private static class AmbiguousInsertException extends IllegalStateException {
        AmbiguousInsertException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The INSERT reported SUCCESS, but the committed row is not READABLE yet: the
     * write IS durable, only its publication lags. The caller may treat the write as
     * confirmed and publish the row it proves (a status flip publishes the new-status
     * row, whose stored instant is strictly later than every row the write met; a CREATE
     * still remembers the identity like the ambiguous form - the id must not be handed
     * out again either way).
     */
    private static final class UnconfirmedVisibleException extends IllegalStateException {
        UnconfirmedVisibleException(String message) {
            super(message);
        }
    }

    /**
     * The AMBIGUOUS outcome of the conditional status INSERT: it reported an error that may
     * have been raised AFTER a commit, and no read can prove the row yet. A specialization
     * of AmbiguousInsertException so every "error, outcome unsure" INSERT failure
     * is handled by type: updateStatus must report the flip as failed while
     * persistTransitionInsert keeps the SHARED mutation window open until a snapshot read
     * shows the requested row (or a later winner) or the window ages past its bound.
     */
    private static final class AmbiguousStatusInsertException extends AmbiguousInsertException {
        AmbiguousStatusInsertException(String message, Throwable cause) {
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
     * Parameter p: the row that was just written
     * Throws UnconfirmedVisibleException: the write reported success but no read sees it
     */
    private void confirmInsertVisible(BaselinePlan p) {
        if (!persistence.isDurable()) {
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            if (persistence.isRowReadable(p)) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        throw new UnconfirmedVisibleException("SPM persist (insert) reported success but baseline "
                + p.getId() + " is not readable yet; an id no read can see could be"
                + " re-allocated after a leadership change - retry the statement");
    }

    /**
     * Polls whether an identity deletion is visible. Returns when the row is absent, or logs and returns after the
     * probe budget expires because the statement already reported success. Probe exceptions propagate. The caller
     * handles cache removal and durable deletion fences.
     */
    private void confirmIdentityGone(BaselinePlan p) {
        if (!persistence.isDurable()) {
            return;
        }
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            if (persistence.isRowGone(p)) {
                return;
            }
            sleepBeforeVisibilityRetry();
        }
        LOG.warn("SPM persist (delete) reported success and baseline {} is still readable"
                + " after {} probes; the committed delete is the durable outcome, removing"
                + " the row from the cache", p.getId(), BASELINE_VISIBILITY_ATTEMPTS);
    }

    /**
     * Polls whether the old status rows are gone after a successful delete. Probe failures are retried; exhaustion
     * logs a warning and returns. Publishing the confirmed new row does not require every old version to have
     * disappeared.
     */
    private void confirmStatusRowGone(BaselinePlan p, BaselineStatus status) {
        if (!persistence.isDurable()) {
            return;
        }
        long id = p.getId();
        for (int attempt = 0; attempt < BASELINE_VISIBILITY_ATTEMPTS; attempt++) {
            boolean gone;
            try {
                gone = persistence.isStatusRowGone(id, p.getSchemaFingerprint(), status);
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
     * As above, with an optional SCHEMA FINGERPRINT constraint: the duplicate fast path
     * of a CREATE must not accept a REUSED id whose row carries the same key but a
     * different incarnation (it may have been recreated after the referenced schema
     * changed, which is exactly what the baseline would then reject at replay).
     */
    private BaselinePersistence.DurablePresence probeDurableRow(long id, String bindSqlDigest,
            String planSql, BaselineStatus status, String schemaFingerprint) {
        return persistence.probeRow(id, bindSqlDigest, planSql, status, schemaFingerprint, null);
    }

    private BaselinePersistence.DurablePresence probeDurableRow(long id, String bindSqlDigest,
            String planSql, BaselineStatus status, String schemaFingerprint,
            Long requiredUpdateTimeMicros) {
        return persistence.probeRow(id, bindSqlDigest, planSql, status, schemaFingerprint,
                requiredUpdateTimeMicros);
    }

    /**
     * Reads rows by id and checks the supplied bind digest, plan SQL and optional status, schema fingerprint and
     * exact epoch-microsecond update time. The update-time condition distinguishes this status write from an older
     * row of the same status. Read failures return UNKNOWN, while a successful read with no matching row returns
     * ABSENT.
     */
    private BaselinePersistence.DurablePresence probeRowFromTable(long id,
            String bindSqlDigest, String planSql, BaselineStatus status,
            String schemaFingerprint, Long requiredUpdateTimeMicros) {
        try {
            // The read is keyed BY THE KNOWN ID (the table's duplicate key): the probe
            // used to filter bind_sql_digest / plan_sql, which scans every bucket and row
            // of a table that grows without a cap, and the mandatory readback of every
            // CREATE could exhaust the fixed probe budget on a large table - reporting an
            // unconfirmed CREATE although its row had committed. The attempted identity /
            // status / stored instant are compared HERE, against the id-keyed rows.
            for (BaselinePlan row : readPersistedById(id)) {
                if (Objects.equals(row.getBindSqlDigest(), bindSqlDigest)
                        && Objects.equals(row.getPlanSql(), planSql)
                        && (status == null || row.getStatus() == status)
                        && (schemaFingerprint == null || Objects.equals(
                                row.getSchemaFingerprint(), schemaFingerprint))
                        && (requiredUpdateTimeMicros == null || sameStoredInstant(
                                row.getUpdateTime(), requiredUpdateTimeMicros))) {
                    return BaselinePersistence.DurablePresence.PRESENT;
                }
            }
            return BaselinePersistence.DurablePresence.ABSENT;
        } catch (Throwable t) {
            LOG.warn("SPM cannot reconcile the ambiguous write of baseline {}: {}",
                    id, t.getMessage());
            return BaselinePersistence.DurablePresence.UNKNOWN;
        }
    }

    /**
     * Probes for a readable row matching id, bind digest, plan SQL, status and the exact attempted update
     * timestamp. This probe does not constrain the schema fingerprint; identity-scoped deletion and duplicate
     * checks apply their own fingerprint conditions.
     */
    private boolean observedInsertRowIsOurs(BaselinePlan p) {
        // Check the attempted id, SQL identity, status and exact epoch-microsecond update time. The fingerprint
        // argument is left unconstrained in this probe.
        return probeDurableRow(p.getId(), p.getBindSqlDigest(), p.getPlanSql(),
                p.getStatus(), null, p.getUpdateTime())
                == BaselinePersistence.DurablePresence.PRESENT;
    }

    /** The live implementation half of BaselinePersistence.isRowReadable. */
    private boolean observedInsertRowIsOursFromTable(BaselinePlan p) {
        return probeRowFromTable(p.getId(), p.getBindSqlDigest(), p.getPlanSql(), p.getStatus(),
                null, p.getUpdateTime()) == BaselinePersistence.DurablePresence.PRESENT;
    }

    /**
     * Fails an INSERT whose outcome is AMBIGUOUS. A reported error may have
     * been raised AFTER a commit (a statement timeout: the row is committed and merely
     * waiting for publication), so it must travel as AmbiguousInsertException:
     * the create path then REMEMBERS the attempted identity and the retry defers /
     * adopts instead of allocating a SECOND id for the same baseline (whose unreadable
     * row the durable-key read cannot see), while the status path does NOT publish the
     * unproven flip. An ordinary exception left that handler unreachable for this path.
     * A probe that already SEES the row treats the write as landed.
     *
     * Parameter p: the row the write attempted
     * Parameter cause: the reported error
     */
    private void throwAmbiguousInsertUnlessDurable(BaselinePlan p, Exception cause) {
        if (observedInsertRowIsOurs(p)) {
            LOG.warn("SPM persist (insert) reported {} but the row is durable (id={});"
                    + " keeping it", cause.getMessage(), p.getId());
            return;
        }
        throw new AmbiguousInsertException("SPM persist (insert) of baseline " + p.getId()
                + " reported an error and may still have COMMITTED (a committed row is"
                + " only waiting for publication): " + cause.getMessage()
                + " - its id is retained until the outcome is resolved; retry the"
                + " statement", cause);
    }

    /**
     * The newest stored update_time of one id at the column's FULL precision, in epoch
     * MICROSECONDS (0 when the id has no row).
     *
     * MICROSECONDS are the SPM time unit of every stored instant (see InternalSchema
     * #SPM_TIMESTAMP_SCALE): the columns carry 6 fractional digits, now is taken from a
     * microsecond clock, and every durable-order comparison ("no newer row exists", "this
     * is the version I complete") is made against the EXACT stored value. A coarser unit
     * would be strictly EARLIER than the row it was derived from, so those comparisons
     * would see the id's own row as a newer write and void themselves.
     *
     * Parameter id: the baseline id
     * Returns: the newest stored instant in epoch microseconds
     */
    private long readNewestStoredUpdateMicros(long id) {
        return persistence.newestStoredUpdateMicros(id);
    }

    /**
     * Reads the newest stored timestamp between two shared-clock probes and rejects either probe showing an open
     * mutation window. Without a shared clock, uses the plain timestamp read. This method does not compare the
     * clock tokens for equality; the conditional status insert also checks for newer durable rows before writing.
     */
    private long readNewestStoredUpdateMicrosAroundQuietClock(long id) {
        if (!sharedMutationClockAvailable()) {
            return readNewestStoredUpdateMicros(id);
        }
        SnapshotFence before = readSnapshotFenceOrRetry(id);
        long newestMicros = readNewestStoredUpdateMicros(id);
        SnapshotFence after = readSnapshotFenceOrRetry(id);
        if (!before.quiet() || !after.quiet()) {
            throw new IllegalStateException("SPM cannot order the stored instant of baseline "
                    + id + ": a mutation of the baseline table is still in flight (" + before
                    + " -> " + after + "), so the newest durable row of the id may be"
                    + " unreadable - retry the ALTER");
        }
        return newestMicros;
    }

    /**
     * readSnapshotFence with the retryable failure the ALTER paths use: an unreadable
     * clock cannot prove that no foreign write is hidden, so the statement must not be dispatched.
     *
     * Parameter id: the baseline id (for the message)
     * Returns: the fence token
     */
    private SnapshotFence readSnapshotFenceOrRetry(long id) {
        try {
            return readSnapshotFence();
        } catch (Exception e) {
            throw new IllegalStateException("SPM cannot read the shared mutation clock of"
                    + " baseline " + id + ", so a concurrent write of the baseline table"
                    + " cannot be ruled out - retry the ALTER", e);
        }
    }

    /** The live implementation half of BaselinePersistence.newestStoredUpdateMicros. */
    private static long newestStoredUpdateMicrosFromTable(long id) {
        if (!persistenceEnabled()) {
            return 0;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(id));
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_MAX_UPDATE_TIME_SQL, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            String text = rows.get(0).getWithDefault(0, "");
            if (text == null || text.isEmpty()) {
                return 0;
            }
            return parseStoredMicros(text);
        } catch (Exception e) {
            throw new RuntimeException("SPM baseline update_time watermark read failed"
                    + " (retry the statement): " + e.getMessage(), e);
        }
    }

    private void persistDeleteByIdentity(BaselinePlan p) {
        // The repair deletes can be dispatched by a demoted master (an in-flight command
        // or a stale-cache reconciliation runs its own read first): deleting AFTER the
        // handoff could erase a row the NEW master just created once the id was reused.
        // Fence like the create path.
        assertLeaderForWrite();
        if (!persistence.isDurable()) {
            return;
        }
        // Open the mutation window BEFORE the delete and close it AFTER (see
        // beginRowMutation): a strict failure fails the DROP, so a delete that may have
        // committed is never preceded by a missing window.
        long mutation = beginRowMutation();
        boolean windowHeld = false;
        try {
            // DISPATCH-time re-checks, the same two the status DELETE runs:
            // everything above was verified BEFORE this point, and a paused caller can
            // outlive its own fences. A DEMOTED writer that was paused between the check
            // and the dispatch would otherwise land this DELETE after the handoff - and a
            // window that aged past MUTATION_WINDOW_MAX_AGE_SECONDS is ignored by every
            // successor, so the same pause let the DELETE erase a row the successor had
            // already updated. Both are re-validated immediately before the statement.
            assertLeaderForWrite();
            assertMutationWindowDispatchable(mutation);
            try {
                persistence.deleteByIdentity(p);
            } catch (Exception e) {
                // A reported error may hide a committed DELETE: the row being GONE is the
                // ONLY proof the drop landed. An unconfirmable read (UNKNOWN) must NOT be
                // treated as proof of absence - removing the cache entry then reported
                // success while the durable row stayed and a refresh / restart resurrected
                // the dropped baseline.
                BaselinePersistence.DurablePresence presence = probeDurableRow(p.getId(),
                        p.getBindSqlDigest(), p.getPlanSql(), null, p.getSchemaFingerprint(),
                        null);
                if (presence == BaselinePersistence.DurablePresence.ABSENT) {
                    LOG.warn("SPM persist (delete) reported {} but the row is gone (id={});"
                            + " treating it as deleted", e.getMessage(), p.getId());
                    return;
                }
                // The DELETE outcome is AMBIGUOUS (the probe still SEES the row, or no read
                // could answer): the delete may have been dispatched and committed, or it
                // may have failed before the commit. Hold the SHARED window open - the
                // absence fence alone is process-local, so a successor whose fence read saw
                // the QUIET clock could read the OLD ENABLED row and lose it when this
                // delayed DELETE published. The window makes every later fence read fail
                // retryably until a snapshot shows the row gone (then it is closed, see
                // resolveHeldMutationWindows) or it ages past its bound.
                windowHeld = holdUnresolvedAbsenceMutationWindow(mutation, p.getId());
                throw new RuntimeException("SPM persist (delete) failed: " + e.getMessage()
                        + (presence == BaselinePersistence.DurablePresence.UNKNOWN
                                ? " (the durable row could not be confirmed deleted)" : ""), e);
            }
            // Poll visibility after SQL success. If the probe budget expires, confirmIdentityGone logs the lag
            // and the caller treats the delete as committed while retaining its publication fence.
            confirmIdentityGone(p);
        } finally {
            if (!windowHeld) {
                endRowMutation(mutation);
            }
        }
    }

    /** The live implementation half of BaselinePersistence.deleteByIdentity. */
    private static void deleteByIdentityFromTable(BaselinePlan p) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(p.getId()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        // the SCHEMA FINGERPRINT completes the identity (see identityPlanSqlHash): without
        // it the stale-incarnation cleanup of ONE fingerprint deleted a NEWER
        // incarnation's row that shared the same id / digest / planSql
        String fingerprint = p.getSchemaFingerprint() == null ? "" : p.getSchemaFingerprint();
        params.put("schemaFingerprint", StatisticsUtil.escapeSQL(fingerprint));
        Map<String, String> nullFingerprintParams = new HashMap<>();
        nullFingerprintParams.put("id", params.get("id"));
        nullFingerprintParams.put("bindSqlDigest", params.get("bindSqlDigest"));
        nullFingerprintParams.put("planSql", params.get("planSql"));
        inInternalIoMode(() -> {
            if (fingerprint.isEmpty()) {
                // A TVF-only baseline stores an EMPTY fingerprint; IFNULL(col,'')=''
                // needs IS NULL OR = '' and DeleteFromCommand rejects it, so the row
                // could never be deleted. Two supported SIMPLE predicates cover both
                // stored shapes (NULL = pre-column rows, '' = current writes).
                StatisticsUtil.execUpdate(DELETE_BY_IDENTITY_SQL_NULL_FINGERPRINT,
                        nullFingerprintParams, BASELINE_WRITE_TIMEOUT_SECONDS);
                StatisticsUtil.execUpdate(DELETE_BY_IDENTITY_SQL_EMPTY_FINGERPRINT,
                        nullFingerprintParams, BASELINE_WRITE_TIMEOUT_SECONDS);
                return null;
            }
            StatisticsUtil.execUpdate(DELETE_BY_IDENTITY_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS);
            return null;
        });
    }

    /**
     * Removes the row(s) with the given id whose status matches the previous status AND
     * whose content matches the plan's identity (bind_sql_digest + plan_sql) AND whose
     * stored VERSION is the one this flip completes.
     *
     * The leadership fence matters here: an ALTER of the OLD master can reach THIS
     * DELETE after a handoff while the new master already completed the opposite flip
     * (both ALTERs pass their early checks). The delayed DELETE... WHERE id AND
     * status = the old status then removed the ONLY durable row the new master had just
     * written - both ALTERs reported success and the baseline was durably gone. The
     * identity key keeps the same statement from touching a REUSED id's row as well.
     *
     * Two  fences close the remaining windows of that hazard:
     * the LEADER / window age are re-checked right before the dispatch (see
     *   assertMutationWindowDispatchable): the check above ran before the window opened,
     *   and the window itself was verified only when it OPENED - a pause between the two
     *   (a stalled thread) can outlive both, so a demoted FE would delete a row the new
     *   master wrote after the handoff while its own marker is already ignored;
     * the DELETE is bound to the OLD row's stored VERSION (see deleteByIdAndStatusSql):
     *   matching (id, old status, identity) alone also matched a row a LATER flip BACK to
     *   that status had written (the new master's ENABLE after this DISABLE), so the
     *   delayed cleanup erased a row both ALTERs had reported successful.
     *
     * Parameter p: the flipped row's snapshot (new status + new stored instant)
     * Parameter status: the OLD status whose row this cleanup removes
     * Parameter oldUpdateTimeMicros: the stored VERSION of the old row (the stored instant this
     *                        flip completes, in epoch microseconds; 0 / unknown = not bound)
     */
    private void persistDeleteByIdAndStatus(BaselinePlan p, BaselineStatus status,
            long oldUpdateTimeMicros) {
        assertLeaderForWrite();
        // Open the mutation window BEFORE the status DELETE and close it after (see
        // beginRowMutation): an overlapping snapshot read must retry over the flip's
        // DELETE half as well.
        long mutation = beginRowMutation();
        try {
            // DISPATCH-time re-checks: everything above was verified BEFORE this point, and
            // a paused caller can outlive its own fences. A demoted FE must not run the
            // cleanup at all, and a window that aged past MUTATION_WINDOW_MAX_AGE_SECONDS
            // is ignored by every successor - both would let this DELETE land on a state
            // written after the window closed.
            assertLeaderForWrite();
            assertMutationWindowDispatchable(mutation);
            if (!persistence.isDurable()) {
                return;
            }
            long id = p.getId();
            try {
                persistence.deleteByIdAndStatus(p, status, oldUpdateTimeMicros);
            } catch (Exception e) {
                // Ambiguous commit: only a READABLE zero row count proves the delete landed.
                // An unconfirmable probe must surface as the original write failure.
                try {
                    if (durableRowCount(id, p.getSchemaFingerprint(), status) == 0) {
                        LOG.warn("SPM persist (delete by status) reported {} but no row carries"
                                + " ({}, {}); treating it as deleted", e.getMessage(), id, status);
                        return;
                    }
                } catch (Throwable probeFailure) {
                    throw new RuntimeException("SPM persist (delete by status) failed: "
                            + e.getMessage() + " (the durable row could not be confirmed"
                            + " deleted)", e);
                }
                throw new RuntimeException("SPM persist (delete by status) failed: "
                        + e.getMessage(), e);
            }
            // Poll old-status visibility. A lagging or unanswerable probe does not undo the confirmed new row;
            // exhaustion is logged and the caller publishes the new status with its fence.
            confirmStatusRowGone(p, status);
        } finally {
            endRowMutation(mutation);
        }
    }

    /** The live implementation half of BaselinePersistence.deleteByIdAndStatus. */
    private static void deleteByIdAndStatusFromTable(BaselinePlan p, BaselineStatus status,
            long oldUpdateTimeMicros) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(p.getId()));
        params.put("status", status.name());
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(p.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(p.getPlanSql()));
        String fingerprint = p.getSchemaFingerprint() == null ? "" : p.getSchemaFingerprint();
        params.put("schemaFingerprint", StatisticsUtil.escapeSQL(fingerprint));
        List<String> deleteSqls = deleteByIdAndStatusSql(oldUpdateTimeMicros, fingerprint);
        inInternalIoMode(() -> {
            for (String deleteSql : deleteSqls) {
                StatisticsUtil.execUpdate(deleteSql, params, BASELINE_WRITE_TIMEOUT_SECONDS);
            }
            return null;
        });
    }

    /**
     * The live implementation half of BaselinePersistence.pruneSupersededVersions
     * (see the SQL constants): removes the versions of one identity whose stored instant is
     * strictly earlier than the retained one.
     *
     * Parameter identity: the identity scoping the delete
     * Parameter keepFromMicros: the stored instant to keep from (0 = nothing to prune)
     */
    private static void pruneSupersededVersionsFromTable(BaselinePlan identity,
            long keepFromMicros) {
        if (!persistenceEnabled() || keepFromMicros <= 0) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(identity.getId()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(identity.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(identity.getPlanSql()));
        String fingerprint = identity.getSchemaFingerprint() == null
                ? "" : identity.getSchemaFingerprint();
        params.put("schemaFingerprint", StatisticsUtil.escapeSQL(fingerprint));
        params.put("keepFrom", toTs(keepFromMicros));
        List<String> deleteSqls = pruneSupersededVersionsSql(fingerprint);
        inInternalIoMode(() -> {
            for (String deleteSql : deleteSqls) {
                StatisticsUtil.execUpdate(deleteSql, params, BASELINE_WRITE_TIMEOUT_SECONDS);
            }
            return null;
        });
    }

    /**
     * The prune statements for one identity (see DELETE_SUPERSEDED_VERSIONS_SQL): ONE
     * normally, TWO for an empty fingerprint - a TVF-only baseline stores an empty
     * fingerprint and IFNULL(...) = '' is rejected by DeleteFromCommand, so the NULL and
     * empty-string shapes need separate simple predicates.
     */
    private static List<String> pruneSupersededVersionsSql(String fingerprint) {
        if (fingerprint == null || fingerprint.isEmpty()) {
            return List.of(DELETE_SUPERSEDED_VERSIONS_SQL_NULL_FINGERPRINT,
                    DELETE_SUPERSEDED_VERSIONS_SQL_EMPTY_FINGERPRINT);
        }
        return List.of(DELETE_SUPERSEDED_VERSIONS_SQL);
    }

    /**
     * The live implementation half of BaselinePersistence.countVersionsBefore: the
     * visibility probe of a prune (see confirmPruneVisible).
     *
     * Parameter identity: the identity scoping the read
     * Parameter keepFromMicros: the stored instant the prune keeps from
     * Returns: the readable count of older versions
     */
    private static long countVersionsBeforeFromTable(BaselinePlan identity,
            long keepFromMicros) {
        if (!persistenceEnabled()) {
            return 0;
        }
        Map<String, String> params = new HashMap<>();
        params.put("id", String.valueOf(identity.getId()));
        params.put("bindSqlDigest", StatisticsUtil.escapeSQL(identity.getBindSqlDigest()));
        params.put("planSql", StatisticsUtil.escapeSQL(identity.getPlanSql()));
        String fingerprint = identity.getSchemaFingerprint() == null
                ? "" : identity.getSchemaFingerprint();
        params.put("schemaFingerprint", StatisticsUtil.escapeSQL(fingerprint));
        params.put("keepFrom", toTs(keepFromMicros));
        long older = 0;
        for (String sql : countVersionsBeforeSqlTexts(fingerprint)) {
            older += countVersionsBeforeSql(sql, params);
        }
        return older;
    }

    /**
     * The count statements one visibility proof runs (see countVersionsBeforeFromTable):
     * the prune issues one DELETE per STORED SHAPE (see pruneSupersededVersionsSql) - for
     * an empty fingerprint BOTH the NULL-fingerprint and the empty-string variant, because
     * the planner rejects the IFNULL form. The proof must cover the same shapes and SUM
     * them: counting only the empty-string rows reported 0 while the NULL-fingerprint
     * DELETE was still unreadable, and confirmPruneVisible then closed the shared mutation
     * window over a mixed snapshot a paginated reader can still observe.
     */
    private static List<String> countVersionsBeforeSqlTexts(String schemaFingerprint) {
        String fingerprint = schemaFingerprint == null ? "" : schemaFingerprint;
        if (fingerprint.isEmpty()) {
            return List.of(COUNT_VERSIONS_BEFORE_SQL_NULL_FINGERPRINT,
                    COUNT_VERSIONS_BEFORE_SQL_EMPTY_FINGERPRINT);
        }
        return List.of(COUNT_VERSIONS_BEFORE_SQL);
    }

    /** One reclaimed-version count statement (see countVersionsBeforeFromTable). */
    private static long countVersionsBeforeSql(String sql, Map<String, String> params) {
        try {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    sql, params, INTERNAL_QUERY_TIMEOUT_SECONDS));
            if (rows == null || rows.isEmpty()) {
                return 0;
            }
            String count = rows.get(0).getWithDefault(0, "0");
            return count == null || count.isEmpty() ? 0L : Long.parseLong(count.trim());
        } catch (Exception e) {
            throw new RuntimeException("SPM reclaimed-version count failed: " + e.getMessage(), e);
        }
    }

    /**
     * Builds status-cleanup DELETE statements constrained by the old row's identity and, when positive, its exact
     * update time in epoch microseconds. This prevents a delayed cleanup from deleting a later row of the same
     * status. A zero time leaves the version unconstrained. Empty fingerprints require separate NULL and
     * empty-string statements.
     */
    private static List<String> deleteByIdAndStatusSql(long oldUpdateTimeMicros, String fingerprint) {
        String version = oldUpdateTimeMicros <= 0 ? "" : " AND `update_time` = '"
                + toTs(oldUpdateTimeMicros) + "'";
        if (fingerprint == null || fingerprint.isEmpty()) {
            // A TVF-only baseline stores an EMPTY fingerprint, and
            // IFNULL(schema_fingerprint,'') = '' needs IS NULL OR = '' - rejected by
            // DeleteFromCommand - so the old-status row could never be cleaned up. The
            // two SUPPORTED simple predicates cover both stored shapes.
            return List.of(DELETE_BY_ID_AND_STATUS_SQL_NULL_FINGERPRINT + version,
                    DELETE_BY_ID_AND_STATUS_SQL_EMPTY_FINGERPRINT + version);
        }
        if (oldUpdateTimeMicros <= 0) {
            return List.of(DELETE_BY_ID_AND_STATUS_SQL);
        }
        // The version predicate must keep update_time a BARE column on the left: the
        // duplicate-key delete fallback rejects any functional left side outright
        // ("Left expr of binary predicate should be column name"), and the original
        // form bound it through UNIX_TIMESTAMP(update_time) - every status flip's
        // cleanup DELETE then failed, the old-status row survived and one baseline id
        // ended up with two / three durable rows  regression run:
        // test_spm_baseline_ddl failed the "must be persisted" count with 3 rows).
        // toTs renders the SAME stored instant as an explicit-UTC-offset literal, so
        // the comparison stays instant-exact and session-zone independent.
        return List.of(DELETE_BY_ID_AND_STATUS_SQL + version);
    }

    /**
     * Epoch MICROSECONDS of ONE TIMESTAMPTZ column, read at the column's OWN precision: the
     * SELECT lists read the stored INSTANT as a MICROSECOND difference from the epoch instead
     * of the column's rendering, because the rendering of a TIMESTAMPTZ depends on the READING
     * session's time zone while the stored value does not - every downstream comparison
     * (status-flip ordering, the duplicate winner, the pending fence age) would otherwise shift
     * with the zone.
     *
     * The difference of two TIMESTAMPTZ values is ABSOLUTE: UNIX_TIMESTAMP of a
     * TIMESTAMPTZ went through the DATETIMEV2 signature, i.e. through the reading session's
     * CIVIL time, so a GLOBAL DST time_zone made the two instants of a repeated hour collapse
     * onto one second before the conversion back to the epoch (measured on the sandbox:
     * 2026-11-01 05:30Z and 06:30Z both returned 1793511000 under America/New_York) - the
     * attempted-instant confirmation and the duplicate / status ordering then compared two
     * DIFFERENT instants as equal. TIMESTAMPDIFF(MICROSECOND, epoch, col) never renders
     * a civil time, keeps all six fractional digits, and returns exactly the epoch-microsecond
     * long every reader / writer uses.
     *
     * Parameter sqlExpression: the column as it appears in the SELECT list, already quoted
     *                      (a bare column or an aggregate such as MAX(update_time)) - the
     *                      helper must not add quoting, or an aggregate would be wrapped
     *                      into MAX(update_time) and fail to parse
     * Returns: the SQL expression, in epoch MICROSECONDS
     */
    private static String storedMicros(String sqlExpression) {
        return "TIMESTAMPDIFF(MICROSECOND,"
                + " CAST('1970-01-01 00:00:00+00:00' AS TIMESTAMPTZ), " + sqlExpression + ")";
    }

    /**
     * Epoch MICROSECONDS -> internal-table TIMESTAMPTZ literal WITH an explicit UTC offset,
     * rendered at the column's MICROSECOND precision
     * ('yyyy-MM-dd HH:mm:ss.SSSSSS+00:00').
     *
     * The columns store an ABSOLUTE instant, and the offset is what makes the literal one:
     * the FEs of one cluster do not share a host zone, and the internal statements run under
     * whatever time_zone their session carries - a bare 'yyyy-MM-dd HH:mm:ss' would
     * therefore be interpreted in the WRITING session's zone (and rendered in the READING
     * one), so the duplicate-row recovery (pickDurableWinner, which keeps the row with the
     * LATER updateTime) compared instants written by different hosts as if they were one
     * clock - a UTC master's 12:00 ENABLED row outranked a UTC-8 successor's 12:01 DISABLED
     * row. The offset pins the instant regardless of any session zone.
     *
     * The value is written EXACTLY as held (see InternalSchema.SPM_TIMESTAMP_SCALE): the
     * column keeps all six fractional digits, so the instant read back is the instant that
     * was written, and two flips inside one millisecond still store DIFFERENT values.
     *
     * Parameter epochMicros: the instant in epoch microseconds
     * Returns: the literal
     */
    private static String toTs(long epochMicros) {
        return OffsetDateTime.ofInstant(
                        Instant.EPOCH.plus(epochMicros, java.time.temporal.ChronoUnit.MICROS),
                        ZoneOffset.UTC)
                .format(TS_FORMAT);
    }

    /**
     * One TIMESTAMPTZ column read through TIMESTAMPDIFF (see storedMicros): epoch
     * MICROSECONDS, never a rendering. Parsing the session-zone-dependent rendering instead
     * would make every stored-time comparison (the status-flip ordering, the duplicate
     * winner, the pending fence age) depend on the reading session's time zone.
     *
     * The read already IS the epoch-microsecond integer (the difference of two absolute
     * instants), so the decimal carries no unit to shift - scaling it again (as the former
     * second-valued UNIX_TIMESTAMP text required) would multiply every instant by a million.
     *
     * Parameter stored: the column's TIMESTAMPDIFF text (epoch MICROSECONDS; empty / null = no
     *               value)
     * Returns: the epoch microseconds, 0 when absent
     */
    private static long parseStoredMicros(String stored) {
        if (stored == null || stored.isEmpty()) {
            return 0;
        }
        return new java.math.BigDecimal(stored.trim())
                .setScale(0, java.math.RoundingMode.DOWN)
                .longValue();
    }

    // live implementations.

    /** The internal-table implementation of BaselinePersistence. */
    private static final class TablePersistence implements BaselinePersistence {

        private final BaselineManager owner;

        TablePersistence(BaselineManager owner) {
            this.owner = owner;
        }

        @Override
        public boolean isDurable() {
            return owner.durableWritesEnabled();
        }

        @Override
        public boolean reservationReadsAvailable() {
            // The live table/store pair models the sequence watermark and the identity
            // record: the create's ownership proof runs (and a lagging READBACK defers
            // the CREATE instead of skipping the proof).
            return owner.durableWritesEnabled();
        }

        @Override
        public List<BaselinePlan> readRowsById(long id, boolean rebuildTrees) {
            return readRowsByIdFromTable(id, rebuildTrees);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash, int timeoutSeconds) {
            return owner.readRowsByHashFromTable(hash, timeoutSeconds);
        }

        @Override
        public List<BaselinePlan> readRowsByHash(long hash) {
            // The live store implements the TIMED read; the plain overload defaults to an
            // empty list on the interface, so any caller of it silently saw no rows (the
            // same-key twin retirement did). Keep both spellings working.
            return readRowsByHash(hash, INTERNAL_QUERY_TIMEOUT_SECONDS);
        }

        @Override
        public Map<Long, BaselinePlan> readSnapshot() {
            try {
                return owner.readSnapshotFromTable();
            } catch (Exception e) {
                throw new RuntimeException("SPM baseline snapshot read failed: "
                        + e.getMessage(), e);
            }
        }

        @Override
        public DurablePresence probeRow(long id, String bindSqlDigest, String planSql,
                BaselineStatus status, String schemaFingerprint, Long requiredUpdateTimeMicros) {
            return owner.probeRowFromTable(id, bindSqlDigest, planSql, status, schemaFingerprint,
                    requiredUpdateTimeMicros);
        }

        @Override
        public int countByIdAndStatus(long id, String schemaFingerprint, BaselineStatus status) {
            return countByIdAndStatusFromTable(id, schemaFingerprint, status);
        }

        @Override
        public boolean isRowReadable(BaselinePlan plan) {
            return owner.observedInsertRowIsOursFromTable(plan);
        }

        @Override
        public boolean isRowGone(BaselinePlan plan) {
            return owner.probeRowFromTable(plan.getId(), plan.getBindSqlDigest(),
                    plan.getPlanSql(), null, plan.getSchemaFingerprint(), null)
                    == DurablePresence.ABSENT;
        }

        @Override
        public boolean isStatusRowGone(long id, String schemaFingerprint, BaselineStatus status) {
            return countByIdAndStatusFromTable(id, schemaFingerprint, status) == 0;
        }

        @Override
        public long newestStoredUpdateMicros(long id) {
            return newestStoredUpdateMicrosFromTable(id);
        }

        @Override
        public void insert(BaselinePlan plan) {
            owner.insertIntoTable(plan);
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus) {
            return owner.writeConditionalStatusInsert(plan, previousStatus, 0L);
        }

        @Override
        public boolean insertIfPreviousPresent(BaselinePlan plan, BaselineStatus previousStatus,
                long previousUpdateMicros) {
            return owner.writeConditionalStatusInsert(plan, previousStatus, previousUpdateMicros);
        }

        @Override
        public void deleteByIdentity(BaselinePlan plan) {
            deleteByIdentityFromTable(plan);
        }

        @Override
        public void deleteByIdAndStatus(BaselinePlan plan, BaselineStatus status,
                long oldUpdateTimeMicros) {
            deleteByIdAndStatusFromTable(plan, status, oldUpdateTimeMicros);
        }

        @Override
        public void pruneSupersededVersions(BaselinePlan identity, long keepFromMicros) {
            pruneSupersededVersionsFromTable(identity, keepFromMicros);
        }

        @Override
        public long countVersionsBefore(BaselinePlan identity, long keepFromMicros) {
            return countVersionsBeforeFromTable(identity, keepFromMicros);
        }

        @Override
        public long tableWatermark() {
            return tableWatermarkFromTable();
        }

        @Override
        public long sequenceWatermark() {
            return owner.readCompactIdWatermark();
        }

        @Override
        public void reserveId(long id, String bindSqlDigest, long planSqlHash, long reserveTimeMicros) {
            owner.reserveIdIntoTable(id, bindSqlDigest, planSqlHash, reserveTimeMicros);
        }

        @Override
        public void notePendingSeqState(String bindSqlDigest, long planSqlHash, long id,
                long atMillis) {
            notePendingSeqStateIntoTable(bindSqlDigest, planSqlHash, id);
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash) {
            return readPersistedSeqReservation(bindSqlDigest, planSqlHash);
        }

        @Override
        public SeqReservation readLatestReservation(String bindSqlDigest, long planSqlHash, long deadlineMillis) {
            return readPersistedSeqReservation(bindSqlDigest, planSqlHash, deadlineMillis);
        }

        @Override
        public Map<Long, SeqReservation> readLatestReservations(Collection<Long> compactIds,
                int timeoutSeconds) {
            return readLatestReservationsFromTable(compactIds, timeoutSeconds);
        }

        @Override
        public BaselinePersistence.ReservationView readReservationView(String bindSqlDigest,
                long planSqlHash) {
            // The two reads this store's OWN successor path performs, in that order.
            // Reuse the keyed read that confirms the identity to a successor.
            return new BaselinePersistence.ReservationView(sequenceWatermark(),
                    readPersistedSeqReservation(bindSqlDigest, planSqlHash));
        }

        @Override
        public void retirePendingSeqState(String bindSqlDigest, long planSqlHash, long markerId) {
            retirePendingSeqStateFromTable(bindSqlDigest, planSqlHash, markerId);
        }

        @Override
        public boolean appendDroppedMarker(long id, String bindSqlDigest, long planSqlHash,
                long atMillis) {
            return appendDroppedMarkerIntoTable(id, bindSqlDigest, planSqlHash);
        }

        @Override
        public Set<String> readDroppedIdentities(Collection<Long> ids) {
            return readDroppedIdentitiesFromTable(ids);
        }

        @Override
        public Set<String> readDroppedIdentitiesBefore(Collection<Long> ids, long deadlineMillis) {
            return readDroppedIdentitiesFromTable(ids, deadlineMillis);
        }

        @Override
        public long readHwmRecord() {
            return readHwmRecordFromTable();
        }

        @Override
        public long readSequenceTail(long floor) {
            return readSequenceTailFromTable(floor);
        }

        @Override
        public long readLegacySequenceWatermark() {
            return readLegacySequenceWatermarkFromTable();
        }

        @Override
        public long[] slotState() {
            return slotStateFromTable();
        }

        @Override
        public long[] clock() {
            return clockFromTable();
        }

        @Override
        public void appendHwmRecord(long lastId, long tick, long pending) {
            appendHwmRecordIntoTable(lastId, tick, pending);
        }

        @Override
        public void pruneHwmRecords(long lastId, long tick) {
            pruneHwmRecordsFromTable(lastId, tick);
        }

        @Override
        public void closeMutationWindow(long windowTick) {
            closeMutationWindowFromTable(windowTick);
        }

        @Override
        public long[] expiredMutationWindowState() {
            return readSlotStateFromTable(SELECT_EXPIRED_WINDOWS_SQL);
        }

        @Override
        public long coveringCompletedTick(long lastId) {
            List<ResultRow> rows = inInternalIoMode(() -> StatisticsUtil.executeQuery(
                    SELECT_COVERING_CLOCK_SQL, Map.of("lastId", String.valueOf(lastId)),
                    INTERNAL_QUERY_TIMEOUT_SECONDS));
            return rows.isEmpty() ? 0 : parseWatermark(rows.get(0).getWithDefault(0, ""));
        }

        @Override
        public void pruneExpiredMutationWindows(long lastId, long tick) {
            Map<String, String> params = Map.of("lastId", String.valueOf(lastId), "tick", String.valueOf(tick));
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_EXPIRED_WINDOWS_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        }

        @Override
        public long countVisiblePending(long tick) {
            return countVisiblePendingFromTable(tick);
        }
    }

    /** The live implementation of BaselineClusterGateway. */
    private static final class TableClusterGateway implements BaselineClusterGateway {

        @Override
        public boolean isLeader() {
            return FeConstants.runningUnitTest || Env.getCurrentEnv() == null
                    || Env.getCurrentEnv().isMaster();
        }

        @Override
        public void syncJournalWithMaster(ConnectContext ctx) {
            if (isLeader() || ctx == null) {
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
    }

    /** The live implementation halves of the mutation-clock writes. */
    private static void appendHwmRecordIntoTable(long id, long tick, long pending) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(id));
        params.put("tick", String.valueOf(tick));
        params.put("pending", String.valueOf(pending));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(INSERT_HWM_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void pruneHwmRecordsFromTable(long id, long tick) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastId", String.valueOf(id));
        params.put("tick", String.valueOf(tick));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_HWM_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_HWM_SAME_WATERMARK_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static void closeMutationWindowFromTable(long windowTick) {
        if (!persistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("windowTick", String.valueOf(windowTick));
        try {
            inInternalIoMode(() -> StatisticsUtil.execUpdate(PRUNE_HWM_OPEN_SQL, params,
                    BASELINE_WRITE_TIMEOUT_SECONDS));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
