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

package org.apache.doris.nereids.spm.capture;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.SPMUtils;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.plugin.audit.AuditLoader;
import org.apache.doris.plugin.audit.AuditPublicationHorizon;
import org.apache.doris.plugin.audit.AuditWriterZones;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * PlanCaptureManager - SPM auto capture scheduler (Phase 2, design doc 7.2.1 / 7.2.4).
 *
 * A Leader-FE daemon that periodically scans the audit_log internal table and
 * automatically creates baselines for high-value queries:
 *
 * - only queries executed by the Nereids planner are captured;
 * - the capture filter (PlanCaptureFilter) enforces the multi-table / table-exists /
 *   regex / performance-threshold rules;
 * - the baseline is built through the Phase 1 flow (SPMPlanner.buildBaselineFromSql:
 *   SPM-mode optimize + decompile + parameterize) with source = CAPTURE and the actual
 *   query_time filled for candidate ordering;
 * - duplicate (digest, planSql) baselines are skipped (BaselineManager dedup).
 *
 * The whole cycle is guarded by the global session variable enable_plan_capture
 * (default false, tunable via `SET GLOBAL enable_plan_capture = true`), and any
 * failure is logged and skipped so auto capture never breaks the cluster.
 */
public class PlanCaptureManager extends MasterDaemon {

    /**
     * Test seam replacing the live leadership probe of persistCheckpoint (null in
     * production). A capture cycle runs on the master, but an in-flight cycle can reach
     * its checkpoint write AFTER a handoff (the daemon checks isMaster only at the cycle
     * start), which a unit test cannot interleave otherwise.
     */
    @VisibleForTesting
    public static volatile java.util.function.BooleanSupplier checkpointLeadershipProbeForTest;

    /**
     * Statement timeout (seconds) of the checkpoint read / write. The default
     * StatisticsUtil overloads assign the ANALYZE timeout (43,200 seconds), so a stalled
     * internal-table read or write could hold the single capture cycle for hours and
     * delay every later capture / retry. Both operations are latency-sensitive: fail
     * fast, keep the cycle consistent, retry next cycle.
     */
    static final int CHECKPOINT_IO_TIMEOUT_SECONDS = 10;

    /** Bounded read-back attempts confirming the first reservation is VISIBLE. */
    private static final int CHECKPOINT_VISIBILITY_ATTEMPTS = 5;

    /** Delay between the reservation visibility reads (millis). */
    private static final long CHECKPOINT_VISIBILITY_RETRY_MILLIS = 200L;

    private static final Logger LOG = LogManager.getLogger(PlanCaptureManager.class);

    private static final PlanCaptureManager INSTANCE = new PlanCaptureManager();

    /**
     * Re-scan overlap (millis) applied to the watermark: AuditLoader buffers events
     * asynchronously and writes their original event timestamp, so a row can become
     * visible AFTER its window has passed (it would otherwise be excluded from every
     * future window forever). Re-scanning a lagged/overlapping window plus query-id
     * deduplication makes late arrivals capturable without processing an execution
     * twice.
     */
    private static final long SCAN_WINDOW_OVERLAP_MS = 300_000L;

    /** Upper bound for the processed-query-id dedup map. */
    private static final int MAX_TRACKED_QUERY_IDS = 10000;

    /**
     * Audit pages ONE wakeup may consume. The daemon interval (default 3h) bounds how
     * often the backlog is drained, so consuming a single page per wakeup left a window
     * truncated at the page limit needing one extra interval per page - a window holding
     * more than `plan_capture_max_batch_size` eligible rows per interval could never catch
     * up. The drain stays BOUNDED so one cycle cannot run unboundedly long (each page is
     * one bounded query plus its checkpoint write).
     */
    private static final int MAX_PAGES_PER_CYCLE = 50;

    /**
     * Wakeup delay used while a window is still pending after a cycle: the backlog drains
     * promptly instead of one page per `plan_capture_interval_seconds`.
     */
    private static final long PENDING_WINDOW_RESUME_INTERVAL_MS = 5_000L;

    /**
     * In-memory budget of the queued retry candidates, in statement characters (the
     * dominant part of a queued entry; the statement text is what the leader FE holds).
     * One drain of MAX_PAGES_PER_CYCLE pages can enqueue up to a page budget of
     * failures per page, so a transient external-metadata outage (every capture fails
     * with "table metadata unavailable") would otherwise retain tens of thousands of full
     * statements - hundreds of megabytes - before the first cohort reaches its third
     * attempt. When the budget is reached the DRAIN pauses: the pending window keeps its
     * bounds and its cursor (the unconsumed rows stay reachable by the keyset scan) while
     * the replay burns the queue down, and the daemon resumes promptly (see
     * pendingWindowNeedsPromptResume).
     */
    private static final long MAX_QUEUED_FAILURE_CHARS = 64L * 1024 * 1024;

    /**
     * Test seam overriding MAX_QUEUED_FAILURE_CHARS (null = the production
     * budget): a unit test cannot queue tens of megabytes of statements just to reach it.
     * Written through setQueuedFailureBudgetForTest.
     */
    private static volatile Long queuedFailureBudgetForTest;

    /**
     * Queued failures replayed in ONE cycle (see replayQueuedFailures): replanning
     * a whole outage-sized queue every wakeup would keep the FE busy for the length of the
     * outage itself.
     */
    private static final int MAX_RETRY_REPLAY_PER_CYCLE = 1000;

    /**
     * Bounded retries for a FAILED capture: the query id stays retryable for later
     * overlapping scans until it either succeeds or reaches this attempt count. Marking
     * the id before processing would make a transient failure permanent - the
     * overlapping scans would skip the row and the watermark passes it long before the
     * dedup map evicts the entry.
     */
    private static final int MAX_CAPTURE_ATTEMPTS = 3;

    /**
     * The fixed id value every checkpoint row carries: uniqueness is the
     * (leader_epoch, write_seq) token now, not the id, but the
     * predicate (WHERE id = 1) stays valid for rows written by older builds.
     */
    private static final long CHECKPOINT_ID = 1L;

    /** Upper bound for the retry entries written into the checkpoint row (row size). */
    private static final int MAX_PERSISTED_RETRIES = 64;

    /** Table of the durable capture checkpoint (see InternalSchema). */
    private static final String CHECKPOINT_TABLE =
            "`__internal_schema`.`spm_capture_checkpoint`";

    /**
     * The checkpoint columns every read expects, in the order applyCheckpointRow
     * consumes them. Shared by the TOKEN query and the pending-window query so the two
     * can never drift apart.
     */
    private static final String CHECKPOINT_COLUMN_LIST =
            "`last_scan_timestamp`, `pending_window_start`, `pending_window_end`,"
                    + " `cursor_query_time`, `cursor_time`, `cursor_query_id`,"
                    + " `failed_attempts`, `retry_queue`, `cursor_tail`,"
                    + " `min_query_time_ms`, `min_scan_rows`, `include_pattern`,"
                    + " `exclude_pattern`, `scan_zone`, `leader_epoch`, `write_seq`";

    /**
     * The read takes the lexicographically GREATEST row of the append-only table
     * (leader_epoch, write_seq) is the row's write token. The
     * previous ORDER BY update_time LIMIT 1 was ambiguous between the newest row
     * and whatever a delayed stale write (a demoted FE whose INSERT committed late) had
     * put next to it. update_time only breaks an EXACT token tie (two same-epoch writers
     * resumed from the same row), where the more recently committed row is the better
     * guess.
     */
    private static final String CHECKPOINT_SELECT_SQL =
            "SELECT " + CHECKPOINT_COLUMN_LIST + " FROM "
                    + CHECKPOINT_TABLE
                    + " WHERE `id` = " + CHECKPOINT_ID
                    + " ORDER BY `leader_epoch` DESC, `write_seq` DESC, `update_time` DESC"
                    + " LIMIT 1";

    /**
     * The companion read of CHECKPOINT_SELECT_SQL: the
     * MOST-BEHIND PENDING window, i.e. the reservation whose unconsumed prefix reaches
     * furthest into the past. The token ordering alone HIDES such a row forever when its
     * INSERT commits only after a later leader read the table as empty: leader A reserves
     * [09:00,12:00), its row is still unreadable when B promotes and reads nothing, B
     * appends [09:10,12:10) with a higher token - and A's row, once it DOES publish, is
     * never the token-greatest one again. The 09:05 row inside A's unconsumed prefix then
     * falls outside B's window and every later overlap: skipped permanently. A pending
     * row (start > 0, start < end) is a window its writer has NOT fully consumed,
     * so it is read here regardless of its token; the same-start tie prefers the writer
     * that progressed furthest (higher token, later commit).
     */
    private static final String CHECKPOINT_SELECT_PENDING_SQL =
            "SELECT " + CHECKPOINT_COLUMN_LIST + " FROM "
                    + CHECKPOINT_TABLE
                    + " WHERE `id` = " + CHECKPOINT_ID
                    + " AND `pending_window_start` > 0"
                    + " AND `pending_window_start` < `pending_window_end`"
                    + " ORDER BY `pending_window_start` ASC, `leader_epoch` DESC,"
                    + " `write_seq` DESC, `update_time` DESC LIMIT 1";

    /**
     * One APPEND of the checkpoint: a plain INSERT whose row carries the
     * writer's next (leader_epoch, write_seq) token. The previous statement was a
     * conditional UPSERT against UNIQUE-key(id) + merge-on-write, which made a demoted
     * leader's late (forwarded) write REPLACE the new master's row - the newer checkpoint
     * (including a queued retry for a late audit row) was destroyed, and the epoch
     * condition that was supposed to refuse it could not fire reliably (the condition is
     * evaluated at execution time, not commit time, and its refusal then aborted the
     * new leader's own drain). Append-only makes the same write harmless: it adds an
     * OLDER row that the reader's ORDER BY never picks. The row's leader_epoch is
     * the FE's max journal id (see currentLeaderEpoch); write_seq orders
     * the writes of one epoch (seeded from the loaded row, see applyCheckpointRow).
     *
     * The target columns are listed EXPLICITLY: the PHYSICAL order of an upgraded table
     * can differ from a freshly created one, and a positional INSERT then shifts every
     * value behind the first out-of-position column - the tail JSON was written into
     * failed_attempts, the retry JSON into update_time and NOW() into cursor_tail - and
     * the checkpoint write failed / persisted garbage. Address the columns by NAME, like
     * the SELECT list above.
     */
    private static final String CHECKPOINT_INSERT_SQL =
            "INSERT INTO " + CHECKPOINT_TABLE
                    + " (`leader_epoch`, `write_seq`, `id`, `last_scan_timestamp`,"
                    + " `pending_window_start`, `pending_window_end`, `cursor_query_time`,"
                    + " `cursor_time`, `cursor_query_id`, `cursor_tail`, `failed_attempts`,"
                    + " `retry_queue`, `min_query_time_ms`, `min_scan_rows`,"
                    + " `include_pattern`, `exclude_pattern`, `scan_zone`, `update_time`)"
                    + " VALUES (${epoch}, ${seq}, " + CHECKPOINT_ID + ", ${lastScan},"
                    + " ${pendingStart}, ${pendingEnd}, ${cursorQueryTime}, '${cursorTime}',"
                    + " '${cursorQueryId}', '${cursorTail}', '${failedAttempts}',"
                    + " '${retryQueue}', ${minQueryTimeMs}, ${minScanRows},"
                    + " '${includePattern}', '${excludePattern}', '${scanZone}', NOW())";

    /**
     * Best-effort garbage collection of the append-only checkpoint: removes
     * the rows the just-written one supersedes - strictly older epochs, plus same-epoch
     * rows with a lower write_seq. Correctness never depends on it (the reader's ORDER BY
     * ignores stale rows), so failures are swallowed by the caller.
     */
    private static final String CHECKPOINT_PRUNE_SQL =
            "DELETE FROM " + CHECKPOINT_TABLE
                    + " WHERE `leader_epoch` < ${epoch}"
                    + " OR (`leader_epoch` = ${epoch} AND `write_seq` < ${seq})";

    private AuditLogScanner scanner = new AuditLogScanner();

    /** Capture filter, refreshed from the global session variables each cycle. */
    private PlanCaptureFilter filter;

    /** Last scan window start (epoch millis); 0 means "first run, scan one interval". */
    private long lastScanTimestamp = 0;

    /**
     * The write token of the append-only checkpoint rows: this process's
     * last write_seq, seeded by applyCheckpointRow from the row it
     * loaded and advanced by every CONFIRMED write. See the seeding comment there for
     * why the loaded row must raise it.
     */
    private long checkpointWriteSeq = 0;

    /**
     * The session time_zone (zone ID) the most recent scan PASS rendered its window bounds
     * in - the zone the audited rows' time columns are stored in. Empty = never
     * scanned (a fresh process follows the global zone). audit_log keeps the WRITER's
     * local rendering, so a global time_zone change makes the already published rows
     * invisible to bounds rendered in the new zone: while this differs from the current
     * global zone, the next pass re-renders the window in this zone first and only then in
     * the new one (see resolveScanPassZone and the exhaustion branch of
     * runCaptureCycle). Persisted with the checkpoint so a takeover resumes the
     * same rendering.
     */
    private String lastScanZone = "";

    /**
     * Pending scan window of a TRUNCATED cycle: the (start, end) pair the resume cursor
     * below belongs to. While set, every cycle keeps scanning the SAME window - the end
     * must stay fixed until the window is fully consumed, because the next
     * interval-derived window would start around this window's end and leave every row the
     * cursor has not reached yet permanently out of scope.
     */
    private long pendingWindowStart = 0;
    private long pendingWindowEnd = 0;

    /**
     * The FILTER SNAPSHOT the pending window was opened with (null while none is pending).
     * The audit SQL and the in-memory PlanCaptureFilter#shouldCapture stage must
     * judge one window's rows by the SAME thresholds: a `SET GLOBAL
     * plan_capture_min_query_time_ms` between two pages of the same window otherwise made
     * the SQL return rows the stale filter rejected terminally (they were consumed,
     * never captured) or pushed already-passed rows behind the cursor where a LOWERED
     * threshold could no longer reach them. The whole filter is pinned, so a pattern
     * change applies from the next window on.
     */
    private PlanCaptureFilter pendingWindowFilter;

    /**
     * Set when a cycle could not finish its pending window (page budget / failed
     * checkpoint write): the daemon then reschedules the next cycle promptly instead of
     * waiting the full `plan_capture_interval_seconds` (default 3h), which would grow the
     * backlog by one interval's worth of eligible rows per consumed page.
     */
    private volatile boolean pendingWindowNeedsPromptResume;

    /** Query ids already handled in earlier (overlapping) windows. */
    private final Map<String, Boolean> processedQueryIds = new LinkedHashMap<>();

    /**
     * Failure attempts per query id (bounded retry, see MAX_CAPTURE_ATTEMPTS). An id is
     * removed here when it succeeds or is given up on; the map is capped like the
     * processed-id map so a long-running failure burst cannot grow unbounded.
     */
    private final Map<String, Integer> failedCaptureAttempts = new LinkedHashMap<>();

    /**
     * Candidates whose capture failed and that still have retry budget: keyset pagination
     * advances the scan cursor past their audit rows and the window overlap only re-reads
     * recent rows, so they are REPLAYED one attempt per cycle from here. Bounded like the
     * other query-id maps; entries leave on success, on give-up, or when the id turns
     * terminal elsewhere.
     */
    private final Map<String, CapturedQuery> failedCaptureQueue = new LinkedHashMap<>();

    /**
     * Statement characters currently retained by failedCaptureQueue (see
     * MAX_QUEUED_FAILURE_CHARS). Guarded by the capture daemon thread (all
     * mutations happen inside a cycle) plus the test seams.
     */
    private long queuedFailureChars = 0;

    /**
     * Pre-page checkpoint state of the page a queued retry was FIRST seen on: the durable
     * checkpoint must never move past an entry that the persisted JSON drops
     * (MAX_PERSISTED_RETRIES) - keyset pagination has already moved beyond its
     * audit row, so only a cursor BEFORE that row can reach it after a restart / handoff.
     * First-wins (pages only move forward) and evicted together with the queue.
     */
    private final Map<String, RetryAnchor> failedCaptureAnchors = new LinkedHashMap<>();

    /** One pre-page checkpoint state (see failedCaptureAnchors). */
    private static final class RetryAnchor {
        final long lastScanTimestamp;
        final long windowStart;
        final long windowEnd;
        final long cursorQueryTime;
        final String cursorTime;
        final String cursorQueryId;
        final String cursorTail;

        /**
         * The zone THIS page's bounds / cursor were RENDERED in. The anchor describes a
         * position of the audit stream, and that position is only reachable when it is
         * rendered in the same zone again (see resolveScanPassZone): persisting
         * the CURRENT cycle's zone beside an earlier window's bounds made a takeover
         * scan those bounds in a zone the rows were never written under, and the omitted
         * retries (the ones the truncated queue could not carry) stayed unreachable.
         */
        final String scanZone;

        /** The filter snapshot the page was judged by (see pageStartFilter). */
        final PlanCaptureFilter filter;

        RetryAnchor(long lastScanTimestamp, long windowStart, long windowEnd,
                long cursorQueryTime, String cursorTime, String cursorQueryId,
                String cursorTail, String scanZone, PlanCaptureFilter filter) {
            this.lastScanTimestamp = lastScanTimestamp;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.cursorQueryTime = cursorQueryTime;
            this.cursorTime = cursorTime;
            this.cursorQueryId = cursorQueryId;
            this.cursorTail = cursorTail;
            this.scanZone = scanZone;
            this.filter = filter;
        }
    }

    /**
     * Resume cursor of a TRUNCATED scan window: the FULL ORDER BY key tuple of the last
     * consumed row -- (time, query_time, query_id) plus the encoded tail (client_ip,
     * sql_hash, scan_rows, return_rows, statement hash) that uniquely separates audit
     * rows sharing the first three keys. CURSOR_ABSENT while no partial window is
     * pending - a short batch advances the watermark instead. Zero and NULL query_time
     * are VALID cursors (see AuditLogScanner.CURSOR_QUERY_TIME_NULL).
     */
    private long cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
    private String cursorTime = "";
    private String cursorQueryId = "";
    private String cursorTail = "";

    /**
     * The pre-page state (watermark + window bounds + cursor) the CURRENT cycle's scan
     * started from. It is the durable fallback persisted when the retry state is
     * truncated by MAX_PERSISTED_RETRIES (see persistCheckpoint): the durable
     * cursor must never move past retries the checkpoint can no longer carry, otherwise
     * a restart / leader handoff neither replays them from the queue nor re-reads their
     * audit rows (the keyset cursor is beyond them and they can age outside the
     * five-minute overlap), silently losing those captures.
     */
    private long pageStartLastScanTimestamp = 0;
    private long pageStartWindowStart = 0;
    private long pageStartWindowEnd = 0;
    private long pageStartCursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
    private String pageStartCursorTime = "";
    private String pageStartCursorQueryId = "";
    private String pageStartCursorTail = "";

    /**
     * The zone the CURRENT page's bounds / cursor were rendered in (the pass zone of the
     * cycle that opened the page, see resolveScanPassZone). It travels with
     * currentPageAnchor() and is persisted whenever the retry state rewinds the
     * durable cursor to a page anchor: a takeover must re-render those bounds in the
     * SAME zone, otherwise the audit rows written under the anchor's rendering are
     * invisible to the re-scan (see RetryAnchor#scanZone).
     */
    private String pageStartZoneId = "";

    /**
     * The FILTER SNAPSHOT the CURRENT page was scanned with (the same value handed to
     * AuditLogScanner#scan). It travels with currentPageAnchor() and is
     * persisted whenever the retry state rewinds the durable cursor to an anchor: the
     * rows of that page were admitted (or filtered) by THESE thresholds / patterns, so a
     * takeover must re-scan the rewound range with the same eligibility - judging the
     * re-scan by a configuration that changed in between could terminally filter the
     * omitted oldest failure before its retry is even reachable.
     */
    private PlanCaptureFilter pageStartFilter;

    /** Whether the durable checkpoint was already consulted in this process. */
    private boolean checkpointLoaded = false;

    /**
     * Whether THIS process has ever seen a durable checkpoint row - read it from the store
     * or written by this process. While it is false, the window a cycle consumes exists
     * only in memory: runCaptureCycle records that window BEFORE scanning (see the
     * initial reservation there), so a takeover can still resume it.
     */
    private boolean durableCheckpointObserved = false;

    /**
     * The EARLIEST window start this process has begun scanning (0 = nothing is covered
     * yet). The windows of one process form a CONTIGUOUS range - the first is derived
     * around the loaded watermark, and every later one starts one overlap behind its
     * predecessor's end - so from this instant on, the chain has scanned or will scan
     * everything. That is exactly the knowledge the checkpoint read needs
     * to tell a STALE pending row of a window this chain already consumed (the
     * append-only table keeps the reservation row of every window forever) from an
     * EARLIER leader's window whose unconsumed prefix was never scanned - the latter must
     * be adopted, the former must not be re-scanned forever. In-memory only: a fresh
     * process starts over and re-evaluates the table on its own.
     */
    private long scannedFromMillis = 0;

    /**
     * Checkpoint read / write seams. Production talks to the internal table through
     * StatisticsUtil; tests replace them to simulate a failing first read and to observe
     * the exact statements a persist issues. The production READ resolves to the earlier
     * pending window when one is still un-accounted-for (see readCheckpointRow).
     */
    private Supplier<List<ResultRow>> checkpointReader = this::readCheckpointRow;

    /** One checkpoint write statement. */
    @VisibleForTesting
    public interface CheckpointWriter {
        void write(String sql, Map<String, String> params) throws Exception;
    }

    /**
     * Raised when the conditional UPSERT REFUSED the write: the durable row carries a
     * newer leader_epoch than the writer's. The affected-row count of the
     * real statement is the only signal, so the production writer translates it here and
     * persistCheckpoint() treats it as a failed write.
     */
    @VisibleForTesting
    static final class CheckpointWriteRefusedException extends RuntimeException {
        CheckpointWriteRefusedException() {
            super("the conditional checkpoint UPSERT wrote no row: the durable row belongs"
                    + " to a newer leader epoch");
        }
    }

    private CheckpointWriter checkpointWriter = (sql, params) -> {
        QueryState state = StatisticsUtil.execUpdate(sql, params, CHECKPOINT_IO_TIMEOUT_SECONDS);
        if (state != null && !checkpointWriteAccepted(state.getAffectedRows())) {
            throw new CheckpointWriteRefusedException();
        }
    };

    /**
     * Whether a scripted checkpoint read / write seam is installed (tests only). The
     * leadership fence of persistCheckpoint guards LIVE writes: a scripted store
     * stands in for the internal table, exactly like the simulator stores in
     * BaselineManager.assertLeaderForWrite.
     */
    private boolean checkpointSeamsForTest = false;

    /**
     * Test seam for the prune's visibility gate (tests only): null means the scripted
     * store's just-written row is READABLE (see writtenCheckpointVisible), so tests
     * that do not care about the gate keep the previous prune behavior.
     */
    private java.util.function.BooleanSupplier checkpointWrittenVisibleForTest = null;

    /**
     * Whether the cloud-mode warning was already logged (the gate fires every cycle).
     */
    private boolean cloudModeWarned = false;

    /**
     * The start of the window the FIRST cycle would have consumed when its checkpoint read
     * FAILED (0 = none). A failed read records no window and the daemon retries promptly;
     * without this floor the first cycle that succeeds on an EMPTY store would derive its
     * own [now-interval, now) and permanently skip the rows of the first attempted window
     * (every later window starts even later). Cleared as soon as a durable row is read or
     * the reserved window becomes durable.
     */
    private long firstAttemptedWindowStart = 0;

    /**
     * The CLUSTER-WIDE audit publication horizon: the start time (epoch millis) of the
     * oldest audit event ANY FE has accepted but not published yet (0 = nothing
     * outstanding), i.e. one this FE's loader owes, one still held / queued before the
     * loader of any FE, or a follower's batch whose load reported Publish Timeout. The
     * capture runs on the leader alone, so only the shared view can fence a follower's
     * backlog. Production reads the live shared table; tests replace it.
     */
    private LongSupplier auditQueueHorizon = AuditPublicationHorizon::clusterHorizon;

    /**
     * The zones the CLUSTER's audit writers have rendered rows in: a window
     * only completes after a pass in EVERY one of them (plus the current global zone),
     * because rows stored under a zone that is no longer current are invisible to bounds
     * rendered in the current zone. Production reads the shared table (+ this FE's live
     * writer history); tests replace it.
     */
    private Supplier<Set<String>> auditWriterZones = AuditPublicationHorizon::clusterWriterZones;

    /**
     * The zones this pending window already COMPLETED a pass in. Seeded
     * with the pass zone the durable checkpoint describes, and reset whenever the window
     * is completed or abandoned. In-memory only: a takeover that restores the pending
     * window re-scans zone passes it cannot remember - duplicates are filtered by the
     * processed-query-id dedup, so the cost is a re-scan, never a correctness gap.
     */
    private final Set<String> scannedZonesInWindow = new LinkedHashSet<>();

    /**
     * The fencing token of the checkpoint UPSERT: the max journal id of
     * this FE - a cluster-wide monotonic clock - so a demoted leader's write (which
     * FORWARDS and executes on the new master) is refused by the STATEMENT itself when
     * the stored row carries a newer epoch. Production reads the live environment; tests
     * replace it.
     */
    private LongSupplier checkpointEpoch = PlanCaptureManager::currentLeaderEpoch;

    // capture statistics (design doc 7.2.1 / 7.2.6)
    private final AtomicLong successCount = new AtomicLong(0);
    private final AtomicLong skipDuplicateCount = new AtomicLong(0);
    private final AtomicLong skipSingleTableCount = new AtomicLong(0);
    private final AtomicLong skipFilterCount = new AtomicLong(0);
    private final AtomicLong failCount = new AtomicLong(0);

    private PlanCaptureManager() {
        super("PlanCaptureManager",
                Math.max(1, VariableMgr.getDefaultSessionVariable().getPlanCaptureIntervalSeconds())
                        * 1000L);
        this.filter = buildFilterFromGlobal();
    }

    /** The pre-page state of the CURRENT page (anchors entries queued by this page). */
    private RetryAnchor currentPageAnchor() {
        return new RetryAnchor(pageStartLastScanTimestamp, pageStartWindowStart,
                pageStartWindowEnd, pageStartCursorQueryTime, pageStartCursorTime,
                pageStartCursorQueryId, pageStartCursorTail, pageStartZoneId, pageStartFilter);
    }

    public static PlanCaptureManager getInstance() {
        return INSTANCE;
    }

    /**
     * Builds a capture filter from the global session variables (so `SET GLOBAL`
     * changes to the thresholds / table regex take effect on the next cycle).
     *
     * @return a new filter
     */
    private static PlanCaptureFilter buildFilterFromGlobal() {
        try {
            SessionVariable global = VariableMgr.getDefaultSessionVariable();
            return new PlanCaptureFilter(global.getPlanCaptureIncludePattern(),
                    global.getPlanCaptureExcludePattern(),
                    global.getPlanCaptureMinQueryTimeMs(),
                    global.getPlanCaptureMinScanRows());
        } catch (RuntimeException e) {
            // e.g. a legacy invalid regex in the global variable: never let it escape the
            // singleton constructor / the daemon cycle (leader startup calls getInstance()
            // before enable_plan_capture is even checked, and a PatternSyntaxException
            // there would terminate the FE transition)
            LOG.error("SPM plan capture disabled: invalid capture filter configuration", e);
            return null;
        }
    }

    @Override
    protected void runAfterCatalogReady() {
        SessionVariable global = VariableMgr.getDefaultSessionVariable();
        // Reschedule from the cycle itself: MasterDaemon sleeps its stored intervalMs, so
        // only setInterval() here makes a `SET GLOBAL plan_capture_interval_seconds`
        // change affect future wakeups (rereading the variable in the cycle would only
        // change the scan window). Clamp to >= 1s so a misconfiguration cannot spin.
        setInterval(Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L);
        // SPM baseline management (CREATE / ALTER / DROP / SHOW) explicitly rejects cloud
        // mode; until the full lifecycle is supported the capture daemon must not create
        // (or keep retrying to create) global baselines a cloud deployment cannot show,
        // disable or drop.
        if (Config.isCloudMode()) {
            if (!cloudModeWarned) {
                cloudModeWarned = true;
                LOG.warn("SPM plan capture is not supported in cloud mode, skipping");
            }
            return;
        }
        if (!global.isEnablePlanCapture()) {
            return;
        }
        if (!Env.getCurrentEnv().isMaster()) {
            // auto capture runs on the Leader FE only
            return;
        }
        if (Env.isCheckpointThread()) {
            return;
        }
        PlanCaptureFilter newFilter = buildFilterFromGlobal();
        if (newFilter == null) {
            LOG.error("Plan capture filter unavailable (invalid capture regex?),"
                    + " skipping this capture cycle");
            return;
        }
        runCaptureCycle(global, newFilter);
        if (pendingWindowNeedsPromptResume) {
            // A window this cycle could not finish (page budget reached or its progress
            // not durable) must NOT wait another full interval: the next wakeup continues
            // exactly where this one stopped. The configured interval would add one
            // interval's worth of eligible rows per consumed page, so a window holding
            // more than one page per interval would never drain.
            setInterval(Math.min(
                    Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L,
                    PENDING_WINDOW_RESUME_INTERVAL_MS));
        }
    }

    /**
     * One capture cycle body: everything after the runtime guards (cloud mode, enable
     * flag, leader / checkpoint-thread checks, filter refresh). Split out so unit tests
     * can drive a FULL cycle - checkpoint read through window derivation, scan, state
     * advance and persist - without the process-global guards (master / cloud / enable)
     * a test environment cannot satisfy.
     *
     * @param global the global session variables of this cycle
     * @param newFilter the filter refreshed for this cycle
     */
    @VisibleForTesting
    public void runCaptureCycle(SessionVariable global, PlanCaptureFilter newFilter) {
        try {
            // A restarted / newly promoted leader must NOT start from a fresh
            // interval-derived window: a truncated window from the previous leader is
            // checkpointed here, and skipping it would permanently exclude its unconsumed
            // tail (the overlap only reaches rows younger than the NEW watermark).
            // A FAILED read returns false and the cycle aborts BEFORE deriving or
            // persisting anything: writing a freshly derived window while the previous
            // leader's unconsumed tail is still unreadable would overwrite its only
            // record (the write path shares the same internal table the read failed on).
            // Whether THIS cycle is the one that resolved the durable checkpoint: the load
            // read applies the SAME choice the re-read below makes, so re-reading it
            // immediately would only duplicate the query.
            boolean resolvedCheckpointThisCycle = !checkpointLoaded;
            if (!loadCheckpointIfNeeded()) {
                // The read failed and recorded nothing, so this process still has no window.
                // Remember the window this cycle WOULD have consumed and retry promptly: the
                // internal-schema initializer is asynchronous, and a later cycle deriving its
                // OWN [now-interval, now) would permanently skip every eligible short row of
                // this first attempted window (no later overlap reaches behind a NEW window's
                // start). Nothing is written here - an unreadable checkpoint must never be
                // replaced by a freshly derived one.
                long attemptedStart = System.currentTimeMillis()
                        - Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L;
                if (firstAttemptedWindowStart == 0 || attemptedStart < firstAttemptedWindowStart) {
                    firstAttemptedWindowStart = attemptedStart;
                }
                pendingWindowNeedsPromptResume = true;
                LOG.warn("Plan capture cycle skipped: durable checkpoint not confirmed");
                return;
            }

            // A window that is still PENDING keeps the filter snapshot it was opened with:
            // its rows behind the cursor were already judged by those thresholds, and the
            // audit SQL must use exactly the same values (see AuditLogScanner#scan). The
            // snapshot is chosen AFTER the checkpoint load, so a TAKEOVER continues the
            // restored window with the restored thresholds in its very first cycle. A NEW
            // window follows the filter refreshed for this cycle, so `SET GLOBAL
            // plan_capture_min_query_time_ms` takes effect from the next window on.
            PlanCaptureFilter cycleFilter = pendingWindowFilter != null
                    ? pendingWindowFilter : newFilter;
            this.filter = cycleFilter;
            pendingWindowNeedsPromptResume = false;

            long currentTime = System.currentTimeMillis();
            // The CLUSTER-WIDE publication fence: the oldest audit event ANY FE has
            // accepted but not published yet (the local loader queue alone
            // cannot see a follower's backlog - the capture runs on the leader, and the
            // follower's row would land behind the advanced watermark). An unreadable
            // shared table means the fence is INCOMPLETE, so the cycle is skipped and
            // retried promptly instead of advancing blind.
            long publicationHorizon;
            Set<String> clusterWriterZones;
            try {
                publicationHorizon = auditQueueHorizon.getAsLong();
                // the writer zones travel with the SAME fail-closed read:
                // an unreadable zone set must not complete a window any more than an
                // unreadable horizon may advance past it
                Set<String> zones = auditWriterZones.get();
                clusterWriterZones = zones == null ? Collections.emptySet() : zones;
            } catch (RuntimeException e) {
                // The read failed and recorded nothing, so remember the window this cycle
                // WOULD have consumed: an empty checkpoint can load at
                // 09:00, repeated horizon-read failures return here, and a 10:00 audit
                // row may publish normally during the outage. When the reads recover at
                // 15:00, a first window derived from [now-interval, now) alone would
                // permanently skip it - exactly like the failed-checkpoint-read path
                // above, record the earliest attempted start before returning.
                long attemptedStart = System.currentTimeMillis()
                        - Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L;
                if (firstAttemptedWindowStart == 0 || attemptedStart < firstAttemptedWindowStart) {
                    firstAttemptedWindowStart = attemptedStart;
                }
                // The attempted start above lives only in THIS process: a restart or leader
                // change before the reads recover makes the successor derive its OWN later
                // window and permanently skip the eligible rows of this one. Persist the
                // window as a durable reservation when the store holds no progress row yet
                // (nothing was scanned, so bounds-only is the complete state).
                reserveFirstAttemptedWindowDurably(cycleFilter);
                pendingWindowNeedsPromptResume = true;
                LOG.warn("Plan capture cycle skipped: the cluster audit publication horizon"
                        + " could not be read", e);
                return;
            }
            // a non-positive interval / batch size can never be written through SQL SET
            // (see SessionVariable), but clamp defensively: an interval of 0 would make
            // every window empty and a batch size of 0 would return LIMIT 0, mark the
            // window exhausted and advance the watermark over every eligible row
            long intervalMs = Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L;
            int batchSize = Math.max(1, global.getPlanCaptureMaxBatchSize());
            // overlap the window so audit rows loaded late (published after their event
            // time has passed) are still scanned; the overlap follows the audit loader's
            // configured batch interval so rows written at the tail of a loader batch -
            // whose event time predates the new watermark - are not lost. Duplicates are
            // filtered by query id below.
            long[] window = resolveScanWindow(lastScanTimestamp, pendingWindowStart, pendingWindowEnd,
                    currentTime, intervalMs,
                    scanWindowOverlapMs(GlobalVariable.auditPluginMaxBatchInternalSec,
                            publicationHorizon, currentTime),
                    firstAttemptedWindowStart);
            long scanStart = window[0];
            long scanEnd = window[1];
            if (scanStart >= scanEnd) {
                return;
            }

            // The zone this cycle's scan RENDERS its bounds in (see resolveScanPassZone):
            // remember it as the zone of the record this cycle persists, so a takeover
            // resumes the same rendering and the next window can detect a change.
            String currentZoneId = AuditLogScanner.auditWriteZone().getId();
            String passZoneId = resolveScanPassZone(currentZoneId);
            lastScanZone = passZoneId;
            // The zone passes this window still OWES: the pass about to run
            // counts as done here (it either completes this cycle or continues the same
            // window next cycle with the same rendering), and the checkpoint's pass zone
            // was seeded the same way on a takeover - so a restored pending window never
            // re-scans a zone the previous leader already covered.
            if (!passZoneId.isEmpty()) {
                scannedZonesInWindow.add(passZoneId);
            }

            if (!durableCheckpointObserved) {
                // FIRST cycle after a successful-but-EMPTY read: the store holds NO row
                // describing the window this process is about to consume, so its bounds and
                // page-top cursor exist only in memory. A restart / leader handoff between
                // the scan and the final persist would leave the takeover with nothing to
                // resume - it would derive a NEW window and permanently skip this page's
                // unconsumed tail (the later overlap only reaches rows younger than the new
                // watermark). Record the window TO CONSUME before consuming it: pending =
                // this window, cursor = its top, watermark = the pre-page one. A failed
                // write ABORTS the cycle: scanning on would advance progress no durable
                // state could ever resume.
                pendingWindowStart = scanStart;
                pendingWindowEnd = scanEnd;
                pendingWindowFilter = cycleFilter;
                if (!persistCheckpointAndConfirm()) {
                    // either this reservation could not be confirmed readable (retried next
                    // cycle, idempotent UPSERT) or an earlier leader's window surfaced and
                    // was adopted instead (resumed next cycle) - both abort WITHOUT scanning
                    // a single audit row while the window waits. Resume PROMPTLY: leaving the
                    // daemon at the configured interval (three hours by default) delayed the
                    // retry of a window nothing was consumed from, exactly like the failed
                    // first READ above (the adoption path already set the flag,
                    // the write / visibility failure did not).
                    pendingWindowNeedsPromptResume = true;
                    LOG.warn("Plan capture cycle skipped: the initial checkpoint row could not"
                            + " be confirmed VISIBLE / was superseded by an earlier window");
                    return;
                }
                // the reservation is durable and readable: the remembered first attempted
                // window is now covered by a durable record and must not widen anything
                firstAttemptedWindowStart = 0;
            }

            // An EARLIER leader's reservation can publish only now - after
            // this process's load already read the table as empty (or read a row this
            // process has since superseded). The production read surfaces it; adopting it
            // (instead of consuming this process's derived window over it) keeps its
            // unconsumed prefix reachable. Without this step the prefix - the rows between
            // the earlier window's start and this process's window start - was skipped
            // permanently, because later reads only ever saw the token-greatest row.
            if (!resolvedCheckpointThisCycle && adoptEarlierPendingCheckpoint()) {
                LOG.warn("Plan capture cycle skipped: an earlier pending checkpoint window"
                        + " surfaced and was adopted");
                return;
            }

            // The earliest window start THIS process actually scanned marks
            // the beginning of the range its chain covers (see scannedFromMillis).
            // Recorded when the pass BEGINS: from the reservation above on, the window is
            // durable (or was loaded from the store), so a crash leaves the chain resumable
            // exactly here, and every window the chain derives next stays behind it.
            if (scannedFromMillis == 0 || scanStart < scannedFromMillis) {
                scannedFromMillis = scanStart;
            }

            // Drain this window with a BOUNDED number of pages: one page per wakeup would
            // make a window holding more than one page wait one interval per page, so a
            // capture rate above `plan_capture_max_batch_size` per interval could never
            // catch up with the audit stream.
            Set<String> scannedQueryIds = new HashSet<>();
            AuditLogScanner.ScanBatch batch = null;
            int pages = 0;
            for (int page = 0; page < MAX_PAGES_PER_CYCLE; page++) {
                if (queuedFailureChars > queuedFailureBudget()) {
                    // The retry queue holds more un-replayed failures than the leader FE
                    // should retain: PAUSE the drain (no page is consumed, so the window
                    // stays pending with its cursor and every unconsumed row remains
                    // reachable) and let the bounded replay below burn the queue down.
                    pendingWindowNeedsPromptResume = true;
                    break;
                }
                // Snapshot the PRE-PAGE state: when this page ends up with more retries
                // than the durable checkpoint can carry, persistCheckpoint falls back to
                // THIS state so the next leader re-scans the page instead of stepping
                // over the omitted retries. Refreshed per page - a retry queued by page N
                // must stay reachable from page N's top, not from the cycle's.
                pageStartLastScanTimestamp = lastScanTimestamp;
                pageStartWindowStart = scanStart;
                pageStartWindowEnd = scanEnd;
                pageStartCursorQueryTime = cursorQueryTime;
                pageStartCursorTime = cursorTime;
                pageStartCursorQueryId = cursorQueryId;
                pageStartCursorTail = cursorTail;
                pageStartZoneId = passZoneId;
                pageStartFilter = cycleFilter;

                batch = scanner.scan(scanStart, scanEnd, batchSize, cycleFilter,
                        cursorQueryTime, cursorTime, cursorQueryId, cursorTail, passZoneId);
                pages++;
                for (CapturedQuery candidate : batch.getCandidates()) {
                    scannedQueryIds.add(retryKeyOf(candidate));
                    handleCandidate(candidate);
                }
                if (batch.isWindowExhausted()) {
                    break;
                }
                // The batch limit truncated the window: KEEP the window BOUNDS and remember
                // the full total-order cursor of the last consumed row, so the next page
                // (and a later leader) resumes inside the same window. Advancing to the
                // window end here would permanently skip every eligible row beyond the
                // LIMIT; letting the next cycle derive a new interval window would skip
                // everything the cursor has not reached yet as well. The cursor TAIL is
                // what keeps rows sharing (time, query_time, query_id) - e.g. a whole page
                // of NULL query ids - from looping or being skipped (see
                // AuditLogScanner#ORDER_BY).
                pendingWindowStart = scanStart;
                pendingWindowEnd = scanEnd;
                pendingWindowFilter = cycleFilter;
                cursorQueryTime = batch.getCursorQueryTime();
                cursorTime = batch.getCursorTime();
                cursorQueryId = batch.getCursorQueryId();
                cursorTail = batch.getCursorTail();
                // Make this page's progress durable BEFORE consuming the next one: a page
                // nothing durable describes would be re-derived as a NEW window by a
                // takeover (see the reservation above). A failed write STOPS the drain -
                // consuming further pages while the store is unavailable is exactly what
                // the reservation exists to prevent - and the cycle resumes promptly.
                if (!persistCheckpoint()) {
                    break;
                }
            }
            // Rows whose capture failed stay queued: keyset pagination moved the cursor
            // past their raw rows and the five-minute overlap only re-reads recent ones,
            // so without this replay attempts 2..N would be unreachable for older
            // failures. Replayed ONCE per cycle with the UNION of every page's keys: an id
            // that appeared in ANY page of this cycle was already retried there, and
            // replaying per page would burn one attempt per page for a failure the page
            // loop kept failing.
            replayQueuedFailures(scannedQueryIds);
            if (batch == null) {
                // The queue budget paused the drain before a single page: the window (a
                // resumed one, or the one just reserved above) stays pending with the
                // cursor it has, and the next cycle - scheduled promptly - retries.
                pendingWindowNeedsPromptResume = true;
            } else if (batch.isWindowExhausted()) {
                // The pass THIS cycle rendered completed: record its zone.
                scannedZonesInWindow.add(passZoneId);
                // Re-read the global writer zone AT EXHAUSTION: the value
                // sampled before the page loop is stale when `SET GLOBAL time_zone` landed
                // while the pages were scanned - a short row loaded under the NEW zone
                // (e.g. a 12:30 UTC event stored as 20:30 under +08:00) is invisible to
                // the UTC pages, and comparing the stale sample skipped the re-scan that
                // would have seen it, so the window was checkpointed at 13:00 and the next
                // +08:00 window started around 20:55 - past the row forever.
                String exhaustZoneId = AuditLogScanner.auditWriteZone().getId();
                // EVERY zone that can own rows of this window must get a completed pass
                // before the watermark advances: the union of the writer
                // zones the cluster reported (plus this FE's live writer history), the
                // zone this pass rendered in and the CURRENT global zone. A window drained
                // in UTC and +08:00 while an intermediate -05:00 epoch rendered rows is
                // otherwise advanced past events neither rendering can see.
                Set<String> requiredZones = new LinkedHashSet<>(clusterWriterZones);
                requiredZones.add(passZoneId);
                requiredZones.add(exhaustZoneId);
                String missingZone = firstNotScanned(requiredZones, scannedZonesInWindow);
                if (missingZone != null) {
                    // Re-scan the SAME window from its top in the missing zone instead of
                    // advancing the watermark - rows rendered under that zone are
                    // invisible to bounds rendered in the others (the reviewer's 12:00Z
                    // event stored as 07:00 under -05:00 falls outside both the UTC and
                    // the +08:00 renderings). `lastScanZone` follows the re-scan, so the
                    // following pass itself advances normally once every required zone was
                    // covered (several changes chain one pass each).
                    LOG.info("Plan capture: the window [{}, {}) was not yet scanned in zone {}"
                                    + " (completed passes: {}); re-scanning it there before"
                                    + " advancing the watermark",
                            scanStart, scanEnd, missingZone, scannedZonesInWindow);
                    pendingWindowStart = scanStart;
                    pendingWindowEnd = scanEnd;
                    pendingWindowFilter = cycleFilter;
                    cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
                    cursorTime = "";
                    cursorQueryId = "";
                    cursorTail = "";
                    lastScanZone = missingZone;
                    pendingWindowNeedsPromptResume = true;
                } else if (blocksWindowCompletion(publicationHorizon, scanEnd)) {
                    // An audit event older than the window END is STILL unpublished
                    // it may expose its row anywhere this
                    // window scans - including BELOW the position the descending keyset
                    // pagination has already walked past (a 10:30 row publishing between
                    // page one and page two sorts before the 10:00 cursor and is skipped).
                    // The value is the MINIMUM over all outstanding events, so even a
                    // horizon BELOW the window does not prove the window is clear: an
                    // older event can mask a newer one inside it. Completing the window
                    // is only safe once nothing below scanEnd is outstanding: re-scan
                    // the widened window from its TOP and resume promptly. When the
                    // event publishes (or its 30-minute fence expires) the re-scan sees
                    // the row; with nothing outstanding the completion proceeds normally.
                    LOG.info("Plan capture: window [{}, {}) stays pending: an audit event at"
                                    + " {} is still unpublished and its row may appear inside"
                                    + " the window after its page was walked; re-scanning from"
                                    + " the top", scanStart, scanEnd, publicationHorizon);
                    pendingWindowStart = scanStart;
                    pendingWindowEnd = scanEnd;
                    pendingWindowFilter = cycleFilter;
                    cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
                    cursorTime = "";
                    cursorQueryId = "";
                    cursorTail = "";
                    lastScanZone = exhaustZoneId;
                    pendingWindowNeedsPromptResume = true;
                } else {
                    // The whole window was scanned: advance the watermark to the CONSUMED
                    // window end (not to `now` - rows that arrived between a resumed pending
                    // window's end and now would be skipped), keep the overlap so
                    // late-arriving audit rows stay capturable, and drop the resume state.
                    lastScanTimestamp = nextScanTimestamp(lastScanTimestamp, scanEnd, true);
                    clearPendingWindow();
                    cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
                    cursorTime = "";
                    cursorQueryId = "";
                    cursorTail = "";
                    lastScanZone = exhaustZoneId;
                    if (!failedCaptureQueue.isEmpty()) {
                        // The window is consumed but retries outlived it: this cycle replayed at
                        // most MAX_RETRY_REPLAY_PER_CYCLE of them, so without a prompt resume the
                        // remaining batches would each wait a FULL capture interval - a 25,000
                        // entry outage queue would need days to burn down at the default three
                        // hours per 1,000 entries. Keep the per-cycle work cap and only shorten
                        // the WAKEUP: resume queued retries at the pending-window cadence until
                        // the queue is drained.
                        pendingWindowNeedsPromptResume = true;
                    }
                }
            } else {
                // The window is still not consumed (page budget reached, the last
                // checkpoint write failed, or the queue budget paused the drain): keep the
                // bounds, the cursor and the pinned filter, and resume promptly instead of
                // after a full interval.
                pendingWindowStart = scanStart;
                pendingWindowEnd = scanEnd;
                pendingWindowFilter = cycleFilter;
                if (batch != null) {
                    cursorQueryTime = batch.getCursorQueryTime();
                    cursorTime = batch.getCursorTime();
                    cursorQueryId = batch.getCursorQueryId();
                    cursorTail = batch.getCursorTail();
                }
                pendingWindowNeedsPromptResume = true;
            }
            // Make the progress durable for the NEXT process (leader handoff / restart).
            persistCheckpoint();

            LOG.info("PlanCapture cycle finished: pages={}, captured={}, dup={},"
                            + " singleTable={}, filtered={}, fail={}",
                    pages, successCount.get(), skipDuplicateCount.get(), skipSingleTableCount.get(),
                    skipFilterCount.get(), failCount.get());
        } catch (Exception e) {
            // A failed cycle (e.g. scanner.scan timing out) must NOT leave the next wakeup
            // at the default capture interval: the cycle already cleared
            // pendingWindowNeedsPromptResume and the window reservation - when one was made
            // - is durable, so this is exactly the state the prompt resume exists for
            // . The next cycle resumes the pending window (or re-derives when
            // nothing is pending, which a prompt cycle merely does earlier).
            pendingWindowNeedsPromptResume = true;
            LOG.warn("Plan capture cycle failed; the pending window is retained for a prompt resume", e);
        }
    }

    /**
     * Handles one candidate with query-id tracking (see MAX_CAPTURE_ATTEMPTS): the id is
     * marked as consumed only when the candidate reached a TERMINAL state - filtered out,
     * deduplicated, persisted, or given up on after bounded failures. A transient failure
     * therefore stays retryable: it enters the failed-candidate queue and is replayed one
     * attempt per cycle (the page cursor has already moved past its row).
     *
     * @param candidate the audit candidate
     */
    @VisibleForTesting
    void handleCandidate(CapturedQuery candidate) {
        handleCandidate(candidate, false);
    }

    /**
     * Handles one candidate with query-id tracking (see MAX_CAPTURE_ATTEMPTS).
     *
     * @param candidate              the audit candidate
     * @param eligibilityAlreadyDecided whether the capture FILTER already admitted this
     *                                 candidate when it was first handled (the replay of
     *                                 a queued failure): the retry must complete the
     *                                 WINDOW's decision, not re-judge the row against a
     *                                 configuration that changed in between - a candidate
     *                                 that became ineligible (a RAISED threshold, a new
     *                                 table-name pattern) would otherwise be marked
     *                                 TERMINAL, and its audit row is behind the cursor
     *                                 (possibly older than the next window's overlap), so
     *                                 nothing would ever capture it.
     */
    @VisibleForTesting
    void handleCandidate(CapturedQuery candidate, boolean eligibilityAlreadyDecided) {
        String retryKey = retryKeyOf(candidate);
        if (processedQueryIds.containsKey(retryKey)) {
            return; // already handled in an earlier overlapping window
        }
        boolean terminal = processCandidate(candidate, eligibilityAlreadyDecided);
        if (terminal) {
            failedCaptureAttempts.remove(retryKey);
            removeQueuedFailure(retryKey);
            failedCaptureAnchors.remove(retryKey);
            markQueryIdProcessed(retryKey);
            return;
        }
        int attempts = failedCaptureAttempts.merge(retryKey, 1, Integer::sum);
        if (attempts >= MAX_CAPTURE_ATTEMPTS) {
            // bounded retry: a permanently broken row must not burn every cycle
            LOG.warn("Plan capture gave up on query key {} after {} failed attempts",
                    retryKey, attempts);
            failedCaptureAttempts.remove(retryKey);
            removeQueuedFailure(retryKey);
            failedCaptureAnchors.remove(retryKey);
            markQueryIdProcessed(retryKey);
        } else {
            LOG.info("Plan capture failed for query key {} (attempt {}/{}), queued for retry",
                    retryKey, attempts, MAX_CAPTURE_ATTEMPTS);
            queueFailure(retryKey, candidate);
            // first-wins: the entry must stay reachable from the page it was FIRST
            // queued on even after later pages advance the scan cursor past its row
            failedCaptureAnchors.putIfAbsent(retryKey, currentPageAnchor());
            // NO queue / attempt eviction: dropping the oldest failures before their
            // bounded retries are spent made them unreachable while the leader kept
            // running - their audit rows are behind the live cursor (and may be older
            // than the overlap window), so nothing would ever retry them even without
            // handoff / checkpoint truncation. Retention is bounded by construction:
            // every entry leaves after MAX_CAPTURE_ATTEMPTS attempts or on a terminal
            // result, and the DRAIN pauses (see MAX_QUEUED_FAILURE_CHARS) before the
            // queue can grow past a per-cycle page budget, so both maps stay bounded by
            // the queue budget instead of by the cycle count.
        }
    }

    /**
     * The in-memory budget of the retry queue (see MAX_QUEUED_FAILURE_CHARS).
     *
     * @return the budget in statement characters
     */
    private static long queuedFailureBudget() {
        return queuedFailureBudgetForTest == null
                ? MAX_QUEUED_FAILURE_CHARS : queuedFailureBudgetForTest;
    }

    /**
     * Queues a failed capture and accounts its statement size against the in-memory
     * budget (see MAX_QUEUED_FAILURE_CHARS).
     *
     * @param retryKey  the tracking key
     * @param candidate the failed candidate
     */
    private void queueFailure(String retryKey, CapturedQuery candidate) {
        if (!failedCaptureQueue.containsKey(retryKey)) {
            queuedFailureChars += candidateChars(candidate);
        }
        failedCaptureQueue.put(retryKey, candidate);
    }

    /**
     * Removes a queued failure and gives its statement size back to the budget.
     *
     * @param retryKey the tracking key
     */
    private void removeQueuedFailure(String retryKey) {
        CapturedQuery queued = failedCaptureQueue.remove(retryKey);
        if (queued != null) {
            queuedFailureChars -= candidateChars(queued);
        }
    }

    /** Approximate in-memory size of one queued statement (chars, see MAX_QUEUED_FAILURE_CHARS). */
    private static long candidateChars(CapturedQuery candidate) {
        String stmt = candidate.getStmt();
        return stmt == null ? 0L : stmt.length();
    }

    /**
     * Tracking key of a candidate: its audit query id when usable, otherwise a
     * SYNTHETIC key derived from the row identity. The audit plugin stores an empty or
     * literal "NaN" query id for some execution paths; treating those rows as
     * untrackable made a transient capture failure permanent (the keyset cursor had
     * already moved past the row and nothing ever replayed it). The synthetic key is
     * stable for the same audit row - statement + metrics + namespace, hashed - so an
     * overlapping re-read maps onto the same retry entry; the "spm-retry:" prefix keeps
     * it from colliding with a real query id.
     *
     * @param candidate the audit candidate
     * @return the stable tracking key
     */
    @VisibleForTesting
    public static String retryKeyOf(CapturedQuery candidate) {
        String queryId = candidate.getQueryId();
        if (queryId != null && !queryId.isEmpty() && !"NaN".equals(queryId)) {
            return queryId;
        }
        String identity = candidate.getStmt() + '\u0001' + candidate.getQueryTimeMs()
                + '\u0001' + candidate.getScanRows() + '\u0001' + candidate.getReturnRows()
                + '\u0001' + candidate.getSqlHash() + '\u0001' + candidate.getDb()
                + '\u0001' + candidate.getCatalog()
                // the ORIGINATING parser mode takes part: AuditLogScanner.toBatch keeps
                // same-text default / PIPES_AS_CONCAT executions separate (a || b has
                // different semantics), so the retry key must separate them as well -
                // otherwise the first capture consumes the key and the second row is
                // skipped as "already processed", or two failures overwrite each other
                + '\u0001' + candidate.getSqlMode()
                // plus the raw audit digest where available (the structural
                // discriminator the scanner used)
                + '\u0001' + (candidate.getSqlDigest() == null ? "" : candidate.getSqlDigest());
        return "spm-retry:" + Long.toHexString(SPMUtils.hashOf(identity));
    }

    /**
     * Replays the queued transient failures, one attempt each per cycle. A queued key
     * that ALSO appeared in this cycle's page was already retried by the page loop (and
     * stays queued when it failed again); every other queued key is retried here, so a
     * failure stays reachable regardless of where the keyset cursor has moved.
     *
     * The WORK is bounded by MAX_RETRY_REPLAY_PER_CYCLE: a metadata outage can
     * queue a whole drain's worth of failures (see MAX_QUEUED_FAILURE_CHARS), and walking
     * - let alone replanning - every one of them in each wakeup would turn one outage into
     * a sustained planning load. A queue larger than the budget is walked over the
     * following cycles; every entry leaves after MAX_CAPTURE_ATTEMPTS attempts, so the
     * queue's head keeps draining and the tail is reached within a bounded number of
     * cycles.
     *
     * @param scannedQueryIds the tracking keys (retryKeyOf) this cycle's page
     *                        already processed
     */
    @VisibleForTesting
    void replayQueuedFailures(Set<String> scannedQueryIds) {
        if (failedCaptureQueue.isEmpty()) {
            return;
        }
        int replayed = 0;
        for (Map.Entry<String, CapturedQuery> entry
                : new ArrayList<>(failedCaptureQueue.entrySet())) {
            String retryKey = entry.getKey();
            if (scannedQueryIds.contains(retryKey)) {
                continue; // already retried by this cycle's page
            }
            if (replayed >= MAX_RETRY_REPLAY_PER_CYCLE) {
                break; // the remainder waits for the next cycle (see the javadoc)
            }
            replayed++;
            // NO remove-before-retry: LinkedHashMap#put on an EXISTING key keeps its
            // original position, while remove+re-add moved the retried entry BEHIND
            // entries queued by newer pages. persistCheckpoint assumes the FIRST queue
            // entry carries the EARLIEST anchor, so reordering made a later page's
            // pre-page cursor get persisted while encodeRetryQueue dropped the older
            // entries that anchor belonged to - unrecoverable on handoff.
            // The ELIGIBILITY of the queued row was already decided by the page that
            // queued it: re-judging it against the CURRENT thresholds / table patterns
            // made a configuration change turn the retry into a terminal rejection.
            handleCandidate(entry.getValue(), true);
            if (processedQueryIds.containsKey(retryKey)) {
                // consumed elsewhere (e.g. by the page): never replay it again
                removeQueuedFailure(retryKey);
                failedCaptureAttempts.remove(retryKey);
                failedCaptureAnchors.remove(retryKey);
            }
        }
    }

    /** Marks a query id as consumed and keeps the dedup map bounded. */
    private void markQueryIdProcessed(String queryId) {
        processedQueryIds.put(queryId, Boolean.TRUE);
        if (processedQueryIds.size() > MAX_TRACKED_QUERY_IDS) {
            evictOldest(processedQueryIds, MAX_TRACKED_QUERY_IDS / 10);
        }
    }

    /** Evicts the count oldest entries of an insertion-ordered map. */
    private static void evictOldest(Map<String, ?> map, int count) {
        java.util.Iterator<String> it = map.keySet().iterator();
        int drop = count;
        while (it.hasNext() && drop-- > 0) {
            it.next();
            it.remove();
        }
    }

    /**
     * Filters and captures a single candidate query.
     *
     * @param candidate the audit candidate
     * @return true when the candidate reached a terminal state (no retry needed),
     *         false when the capture FAILED and should be retried
     */
    private boolean processCandidate(CapturedQuery candidate) {
        return processCandidate(candidate, false);
    }

    /**
     * One capture attempt.
     *
     * @param candidate              the audit candidate
     * @param eligibilityAlreadyDecided skip the FILTER stage: the candidate was already
     *                                 admitted when it was queued (see
     *                                 handleCandidate(CapturedQuery, boolean)),
     *                                 so a configuration change in between must not turn
     *                                 the retry into a terminal rejection. The
     *                                 catalog-existence stage below still runs: it is not
     *                                 a configuration question, and a definitive MISSING
     *                                 stays terminal.
     * @return whether the candidate is TERMINAL (filtered out, persisted, deduplicated)
     */
    private boolean processCandidate(CapturedQuery candidate, boolean eligibilityAlreadyDecided) {
        try {
            // Level 3/5 filter: multi-table + table-name regex (pure logic). The
            // extraction PARSES the statement, so it must run under the audit row's
            // ORIGINATING parser mode just like the later build - the daemon thread's
            // global mode can differ (e.g. NO_BACKSLASH_ESCAPES globally while the
            // audited session used the default), and a parse failure here silently
            // drops the row as terminal while the cursor advances past it.
            List<String> tables = SqlModeHelper.withSqlMode(candidate.getSqlMode(),
                    () -> PlanCaptureFilter.extractTableNames(candidate.getStmt(),
                            candidate.getCatalog(), candidate.getDb()));
            if (!eligibilityAlreadyDecided
                    && !filter.shouldCapture(candidate.toAuditEvent(), tables)) {
                if (tables.size() < 2) {
                    skipSingleTableCount.incrementAndGet();
                } else {
                    skipFilterCount.incrementAndGet();
                }
                return true;
            }
            // Level 4 filter: tables must still exist in the CAPTURED namespace (external
            // tables resolve through their own catalog, not InternalCatalog). A definitive
            // MISSING is terminal; UNAVAILABLE (catalog still initializing / metadata
            // outage) stays RETRYABLE - a terminal decision would mark the audit row
            // processed and the keyset cursor has already advanced past it, permanently
            // losing an otherwise eligible external query during a transient outage.
            PlanCaptureFilter.TableLookup lookup =
                    filter.checkAllTablesExist(tables, candidate.getCatalog(), candidate.getDb());
            if (lookup == PlanCaptureFilter.TableLookup.MISSING) {
                skipFilterCount.incrementAndGet();
                return true;
            }
            if (lookup == PlanCaptureFilter.TableLookup.UNAVAILABLE) {
                LOG.info("Plan capture deferred for query {}: table metadata is unavailable"
                        + " (attempt {})", candidate.getQueryId(),
                        failedCaptureAttempts.getOrDefault(retryKeyOf(candidate), 0) + 1);
                return false;
            }

            // Build the baseline through the Phase 1 flow (bindSql = planSql = stmt,
            // SPM-mode optimize + decompile + parameterize)
            BaselinePlan baseline;
            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                // Resolve names with the CAPTURED namespace instead of the internal-schema
                // default: StatisticsUtil.buildConnectContext(false) points at
                // __internal_schema, so an unqualified join from the audited database
                // could not resolve its tables at all.
                if (candidate.getCatalog() != null && !candidate.getCatalog().isEmpty()) {
                    // changeDefaultCatalog clears the database, so the catalog must be
                    // switched BEFORE the database is set
                    ctx.connectContext.changeDefaultCatalog(candidate.getCatalog());
                }
                if (candidate.getDb() != null && !candidate.getDb().isEmpty()) {
                    ctx.connectContext.setDatabase(candidate.getDb());
                }
                baseline = buildBaselineUnderCapturedMode(ctx.connectContext, candidate);
            }
            baseline.setSource(BaselineSource.CAPTURE);
            baseline.setQueryTimeMs(candidate.getQueryTimeMs());
            // audit correlation: the bindSql above is exactly the audit row's stmt text
            // (candidate.getStmt() read from audit_log verbatim), and the queryId lets the
            // audit record of the captured execution be located later through
            // `WHERE query_id = ...` (SHOW BASELINE PLANS exposes it as query_id)
            baseline.setQueryId(candidate.getQueryId());

            // Persist; createBaseline dedups identical (digest, planSql). The manager
            // stores the exact object reference, so we can tell "created" from
            // "duplicate" by reference identity.
            BaselineManager manager = BaselineManager.getInstance();
            long id = manager.createBaseline(baseline);
            if (manager.getBaseline(id) == baseline) {
                successCount.incrementAndGet();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Captured baseline {} for query: {}", id, candidate.getStmt());
                }
            } else {
                skipDuplicateCount.incrementAndGet();
            }
            return true;
        } catch (Exception e) {
            failCount.incrementAndGet();
            LOG.warn("Failed to capture baseline for query: {}", candidate.getStmt(), e);
            // NOT terminal: the query id stays retryable (bounded by MAX_CAPTURE_ATTEMPTS)
            return false;
        }
    }

    /**
     * Builds the baseline with the candidate's ORIGINATING parser mode in force for the
     * whole build: the parser reads it (PIPES_AS_CONCAT decides whether "a || b" is a
     * CONCAT or a boolean OR, NO_BACKSLASH_ESCAPES how a literal decodes) and SPMPlanner
     * records it as the baseline's creatorSqlMode, which the reload path re-parses the
     * stored bindSql with. The audit_log row carries the mode of the session that ran
     * the captured statement; without re-applying it the build silently ran under the
     * internal default, so a CONCAT-mode statement was captured as an OR (or failed)
     * and could never produce a usable baseline for later CONCAT-mode executions.
     */
    private static BaselinePlan buildBaselineUnderCapturedMode(ConnectContext ctx,
            CapturedQuery candidate) {
        BaselinePlan[] holder = new BaselinePlan[1];
        SqlModeHelper.withSqlMode(candidate.getSqlMode(), () -> {
            try {
                holder[0] = new SPMPlanner().buildBaselineFromSql(ctx, candidate.getStmt(),
                        candidate.getStmt());
            } catch (UserException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
        return holder[0];
    }

    // ==================== durable checkpoint (design doc 7.2.4) ====================

    /** Whether the durable checkpoint store can be used (internal schema db enabled). */
    static boolean checkpointPersistenceEnabled() {
        return FeConstants.enableInternalSchemaDb;
    }

    /**
     * Reads the durable checkpoint ONCE per process, before the first window is derived.
     * The checkpoint fields are process-local otherwise: a truncated [T-3h, T) window
     * advances one page per daemon cycle, so a leader handoff / FE restart near T+3h would
     * start at [T, T+3h) and permanently exclude the unconsumed tail - the later overlap is
     * relative to the NEW watermark and cannot recover it.
     *
     * @return true when the checkpoint state is CONFIRMED for this cycle (read succeeded,
     *         nothing to load, progress already exists, or persistence is disabled);
     *         false when the read FAILED - the caller must abort the cycle because
     *         persisting a freshly derived window would overwrite the previous leader's
     *         unconsumed tail before it could be read
     */
    private boolean loadCheckpointIfNeeded() {
        if (checkpointLoaded || !checkpointPersistenceEnabled()) {
            return true;
        }
        if (lastScanTimestamp != 0 || pendingWindowEnd > 0
                || cursorQueryTime != AuditLogScanner.CURSOR_ABSENT) {
            checkpointLoaded = true; // progress already exists (e.g. a unit test): never override it
            return true;
        }
        try {
            List<ResultRow> rows = checkpointReader.get();
            if (rows == null || rows.isEmpty()) {
                checkpointLoaded = true; // a successful read with no row yet
                return true;
            }
            applyCheckpointRow(rows.get(0));
            checkpointLoaded = true; // only a SUCCESSFUL read consumes the checkpoint
            if (lastScanTimestamp != 0 || pendingWindowEnd > 0
                    || cursorQueryTime != AuditLogScanner.CURSOR_ABSENT) {
                LOG.info("SPM capture resumed from the durable checkpoint: lastScan={},"
                                + " pending=[{}, {}), cursorQueryTime={}",
                        lastScanTimestamp, pendingWindowStart, pendingWindowEnd, cursorQueryTime);
            }
            return true;
        } catch (Exception e) {
            // Keep checkpointLoaded FALSE: the internal-schema initializer is asynchronous
            // (and a BE / tablet may not be ready yet), so this read can fail before the
            // table exists. Marking the checkpoint consumed on failure made the process
            // start from the default window and later OVERWRITE the only record of the
            // previous leader's unconsumed tail. The next cycle retries.
            LOG.warn("SPM capture checkpoint read failed (will retry next cycle): {}",
                    e.getMessage());
            return false;
        }
    }

    /** Applies one checkpoint row (column order = CHECKPOINT_SELECT_SQL). */
    @VisibleForTesting
    public void applyCheckpointRow(ResultRow row) {
        lastScanTimestamp = parseLongValue(row.get(0));
        pendingWindowStart = parseLongValue(row.get(1));
        pendingWindowEnd = parseLongValue(row.get(2));
        cursorQueryTime = parseLongValue(row.get(3));
        cursorTime = row.get(4) == null ? "" : row.get(4);
        cursorQueryId = row.get(5) == null ? "" : row.get(5);
        // the tail column is APPENDED to the SELECT list: rows written by an older FE
        // (or fabricated by tests) carry fewer values
        cursorTail = row.getValues().size() > 8 && row.get(8) != null ? row.get(8) : "";
        // the scan zone column is APPENDED after the pattern columns: rows written by an
        // older FE (or fabricated by tests) carry fewer values. Empty = unknown rendering,
        // which simply follows the current global zone.
        lastScanZone = row.getValues().size() > 13 && row.get(13) != null ? row.get(13) : "";
        if (cursorQueryTime != AuditLogScanner.CURSOR_ABSENT && cursorTail.isEmpty()) {
            // Legacy row: a cursor exists but its tail does not (the column predates it).
            // The (time, query_time, query_id) prefix alone cannot always make progress
            // - a page whose rows share a NULL query_id loops forever / skips the rest -
            // so the partial cursor is NOT trusted: reset it and re-scan the pending
            // window from its top. Captures are idempotent (query-id dedup + baseline
            // dedup by digest / planSql), so the re-read cannot double-capture.
            LOG.warn("SPM capture checkpoint cursor has no tail (legacy row);"
                    + " re-scanning the pending window from the top");
            cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
            cursorTime = "";
            cursorQueryId = "";
        }
        failedCaptureAttempts.clear();
        failedCaptureAttempts.putAll(decodeFailedAttempts(row.get(6)));
        failedCaptureQueue.clear();
        failedCaptureQueue.putAll(decodeRetryQueue(row.get(7)));
        failedCaptureAnchors.clear();
        queuedFailureChars = 0;
        for (CapturedQuery queued : failedCaptureQueue.values()) {
            queuedFailureChars += candidateChars(queued);
        }
        // The threshold / pattern columns are APPENDED as well (same reason as the cursor
        // tail). They describe the PENDING window: the scan of this window continues with
        // the values its already-consumed rows were judged by, not with the takeover's
        // current globals - a row behind the cursor cannot be re-judged, so a changed
        // threshold or table-name pattern must not re-filter the window halfway. An
        // absent / NULL triple (a row written before the columns existed, or fabricated
        // by tests) leaves the window unpinned: the cycle then uses the filter it
        // refreshed from the globals.
        pendingWindowFilter = null;
        if (pendingWindowStart < pendingWindowEnd && row.getValues().size() > 10
                && row.get(9) != null && row.get(10) != null) {
            long restoredMinQueryTimeMs = parseLongValue(row.get(9));
            long restoredMinScanRows = parseLongValue(row.get(10));
            if (restoredMinQueryTimeMs >= 0 && restoredMinScanRows >= 0) {
                pendingWindowFilter = new PlanCaptureFilter(
                        row.getValues().size() > 11 && row.get(11) != null ? row.get(11) : "",
                        row.getValues().size() > 12 && row.get(12) != null ? row.get(12) : "",
                        restoredMinQueryTimeMs, restoredMinScanRows);
            }
        }
        // The append-only write token of the row just read (the two columns
        // APPENDED to the SELECT list): a process that RESUMES this checkpoint must write
        // strictly ABOVE it. The leader epoch is the max journal id, which does NOT change
        // across an FE restart, so without the seeding a same-epoch restarted leader
        // would store (epoch, 1) next to the durable (epoch, N) - and the reader, ranking
        // by write_seq, would keep preferring the row the restart meant to supersede.
        if (row.getValues().size() > 15 && row.get(15) != null) {
            long loadedSeq = parseLongValue(row.get(15));
            if (loadedSeq > checkpointWriteSeq) {
                checkpointWriteSeq = loadedSeq;
            }
        }
        // a row was READ: this process now knows a durable record exists, so the initial
        // reservation in runCaptureCycle never overwrites / takes over its role
        durableCheckpointObserved = true;
        // the store HAS a row: the "first attempted window" floor of a failed fresh read is
        // moot - the durable record defines the window to consume
        firstAttemptedWindowStart = 0;
        // Restored retries take the RESTORED cursor as their anchor. That is now always a
        // position BEFORE every persisted retry row: persistCheckpoint rewinds to the
        // oldest queued entry's PRE-PAGE anchor whenever the queue is non-empty (not only
        // when the JSON truncates), so a truncated 65th entry can still re-scan the row
        // whose retry the JSON dropped. The anchor carries the RESTORED filter snapshot:
        // the rewound range was judged by those thresholds / patterns when it was first
        // scanned, and persisting it again (before anything re-scans it) keeps the
        // eligibility stable across any number of handoffs.
        RetryAnchor restoredAnchor = new RetryAnchor(lastScanTimestamp, pendingWindowStart,
                pendingWindowEnd, cursorQueryTime, cursorTime, cursorQueryId, cursorTail,
                lastScanZone, pendingWindowFilter);
        for (String retryKey : failedCaptureQueue.keySet()) {
            failedCaptureAnchors.put(retryKey, restoredAnchor);
        }
    }

    /**
     * Persists the current capture progress (window bounds + total-order cursor + retry
     * state) for the NEXT process. A failed write is logged and skipped - the checkpoint is
     * a best-effort resume aid and must never break the cycle - but the RESULT is reported
     * so the initial reservation can refuse to consume a window nothing durable describes.
     *
     * @return true when the progress is durable afterwards (or the store is disabled);
     *         false when the write failed
     */
    private boolean persistCheckpoint() {
        if (!checkpointPersistenceEnabled()) {
            return true;
        }
        if (!isLeaderForCheckpointWrite()) {
            // A demoted FE's in-memory progress is OBSOLETE: the new master may have
            // advanced (or REWOUND) the durable checkpoint meanwhile - it can have queued
            // a retry for a late audit row the old cursor had not reached yet - and this
            // forwarded UPSERT would replace that queue and cursor with ours. A row behind
            // the revived cursor is then neither replayed from the queue nor reachable by
            // keyset pagination, so it is never retried. Skipping the write is the fence;
            // Env.transferToMaster drops the local progress when this FE is promoted
            // again (see reloadCheckpointOnPromotion).
            LOG.warn("SPM capture checkpoint NOT persisted: this FE is no longer the master"
                    + " (the new leader owns the checkpoint)");
            return false;
        }
        // Truncation guard: the two JSON maps below keep only the most recent
        // MAX_PERSISTED_RETRIES entries, so persisting the CURRENT cursor / watermark while
        // entries were omitted would step over exactly those omitted retries - after a
        // restart or leader handoff they are neither replayed from the queue (dropped) nor
        // reachable by keyset pagination (the cursor is past them; the five-minute overlap
        // only reaches recent rows).
        //
        // The durable cursor must sit before the EARLIEST queued entry, not merely before
        // the CURRENT page: page 1 may queue 100 failures (its checkpoint rewinds before
        // page 1), and a LATER page - already past page 1 - still sees the same 100.
        // Rewinding only to that later page's start would leave the 36 oldest entries
        // neither queued (truncated JSON) nor re-readable (cursor past them).
        // Every queued retry therefore carries the PRE-PAGE anchor of the page it was
        // FIRST seen on (failedCaptureAnchors), and the durable cursor uses the OLDEST
        // queued entry's anchor whenever ANY retry is queued. That is required even when
        // the JSON fits the budget: on RESTORE every queued entry takes the CHECKPOINT
        // cursor as its anchor, so a checkpoint written with the LIVE cursor (past the
        // rows of entries that fit) would later rewind a truncated 65th entry only to a
        // position AFTER the omitted row - which the next leader can neither replay from
        // the queue nor re-scan, losing its remaining attempt.
        // The condition is self-healing: retries leave the queue on success or after
        // MAX_CAPTURE_ATTEMPTS, and the cursor advances again once the queue is empty.
        boolean queueTruncated = failedCaptureQueue.size() > MAX_PERSISTED_RETRIES;
        boolean attemptsTruncated = failedCaptureAttempts.size() > MAX_PERSISTED_RETRIES;
        boolean retriesTruncated = queueTruncated || attemptsTruncated;
        RetryAnchor durableAnchor = null;
        if (!failedCaptureQueue.isEmpty()) {
            // insertion order = page order: the FIRST (oldest) queued retry precedes every
            // other queued retry, and its anchor precedes its own audit row
            durableAnchor = failedCaptureAnchors.get(
                    failedCaptureQueue.keySet().iterator().next());
        }
        long durableLastScan;
        long durablePendingStart;
        long durablePendingEnd;
        long durableCursorQueryTime;
        String durableCursorTime;
        String durableCursorQueryId;
        String durableCursorTail;
        String durableScanZone;
        if (durableAnchor != null) {
            durableLastScan = durableAnchor.lastScanTimestamp;
            durablePendingStart = durableAnchor.windowStart;
            durablePendingEnd = durableAnchor.windowEnd;
            durableCursorQueryTime = durableAnchor.cursorQueryTime;
            durableCursorTime = durableAnchor.cursorTime;
            durableCursorQueryId = durableAnchor.cursorQueryId;
            durableCursorTail = durableAnchor.cursorTail;
            // the anchor's OWN zone, not this cycle's: the anchor describes an earlier
            // page, and its rows are only reachable when re-rendered in that zone (the
            // reviewer's W1-in-UTC example: a +08 takeover could not see the UTC-stored
            // rows of the window this anchor rewinds to)
            durableScanZone = durableAnchor.scanZone;
        } else if (retriesTruncated || !failedCaptureQueue.isEmpty()) {
            durableLastScan = pageStartLastScanTimestamp;
            durablePendingStart = pageStartWindowStart;
            durablePendingEnd = pageStartWindowEnd;
            durableCursorQueryTime = pageStartCursorQueryTime;
            durableCursorTime = pageStartCursorTime;
            durableCursorQueryId = pageStartCursorQueryId;
            durableCursorTail = pageStartCursorTail;
            durableScanZone = pageStartZoneId;
        } else {
            durableLastScan = lastScanTimestamp;
            durablePendingStart = pendingWindowStart;
            durablePendingEnd = pendingWindowEnd;
            durableCursorQueryTime = cursorQueryTime;
            durableCursorTime = cursorTime;
            durableCursorQueryId = cursorQueryId;
            durableCursorTail = cursorTail;
            durableScanZone = lastScanZone;
        }
        if (retriesTruncated) {
            LOG.warn("SPM capture retry state (retry queue {}, failed attempts {}) exceeds the"
                            + " durable checkpoint budget ({} entries); persisting a cursor"
                            + " before the oldest omitted retry (anchor={})",
                    failedCaptureQueue.size(), failedCaptureAttempts.size(),
                    MAX_PERSISTED_RETRIES, durableAnchor != null);
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastScan", String.valueOf(durableLastScan));
        params.put("pendingStart", String.valueOf(durablePendingStart));
        params.put("pendingEnd", String.valueOf(durablePendingEnd));
        params.put("cursorQueryTime", String.valueOf(durableCursorQueryTime));
        params.put("cursorTime",
                StatisticsUtil.escapeSQL(durableCursorTime == null ? "" : durableCursorTime));
        params.put("cursorQueryId",
                StatisticsUtil.escapeSQL(durableCursorQueryId == null ? "" : durableCursorQueryId));
        params.put("cursorTail",
                StatisticsUtil.escapeSQL(durableCursorTail == null ? "" : durableCursorTail));
        params.put("failedAttempts",
                StatisticsUtil.escapeSQL(encodeFailedAttempts(failedCaptureAttempts)));
        params.put("retryQueue",
                StatisticsUtil.escapeSQL(encodeRetryQueue(failedCaptureQueue)));
        // The thresholds AND the table-name patterns of the PENDING window (its filter
        // snapshot), or the -1 / empty sentinels when none is pending: the takeover must
        // continue the window exactly as it was opened (see applyCheckpointRow). When the
        // cursor was REWOUND to the oldest queued retry's pre-page anchor, the persisted
        // snapshot is THAT page's filter instead: the re-scan of the rewound range must
        // judge its rows by the eligibility they were first admitted with, not by a
        // configuration that changed after W1 was exhausted (a tightened threshold /
        // pattern would otherwise terminally filter the omitted oldest failure before the
        // restored retry queue can retry it).
        PlanCaptureFilter durableFilter = durableAnchor != null && durableAnchor.filter != null
                ? durableAnchor.filter : pendingWindowFilter;
        params.put("minQueryTimeMs", String.valueOf(
                durableFilter == null ? -1L : durableFilter.getMinQueryTimeMs()));
        params.put("minScanRows", String.valueOf(
                durableFilter == null ? -1L : durableFilter.getMinScanRows()));
        params.put("includePattern", StatisticsUtil.escapeSQL(durableFilter == null
                ? "" : durableFilter.getIncludePatternText()));
        params.put("excludePattern", StatisticsUtil.escapeSQL(durableFilter == null
                ? "" : durableFilter.getExcludePatternText()));
        params.put("scanZone", StatisticsUtil.escapeSQL(durableScanZone == null
                ? "" : durableScanZone));
        // the append-only write token (see CHECKPOINT_INSERT_SQL): the
        // writer's leader epoch plus this process's next write_seq. The
        // counter commits only with a CONFIRMED write, so a failed write reuses its
        // token - harmless: equal tokens are equivalent rows, and a retry would write
        // the same state anyway.
        final long writeEpoch = checkpointEpoch.getAsLong();
        params.put("epoch", String.valueOf(writeEpoch));
        final long writeSeq = checkpointWriteSeq + 1;
        params.put("seq", String.valueOf(writeSeq));
        try {
            // One plain APPEND: the new row is durable before anything reads it, and the
            // row it supersedes stays intact whatever happens to THIS statement - the old
            // single-row UPSERT could leave the store without a checkpoint when a
            // demoted FE's forwarded write replaced the new master's row / the commit
            // was deferred past the writer's timeout.
            // Pin the DEFAULT parser mode for the write: escapeSQL doubles backslashes,
            // which only decode back in that mode - under a global NO_BACKSLASH_ESCAPES
            // the stored JSON / SQL text would keep the doubled bytes (and a doubled
            // quote can break the Gson round-trip of the retry queue).
            SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
                try {
                    checkpointWriter.write(CHECKPOINT_INSERT_SQL, params);
                } catch (Exception writeFailure) {
                    throw new RuntimeException(writeFailure);
                }
                return null;
            });
        } catch (CheckpointWriteRefusedException refused) {
            // The statement reported NO written row ( defensive path): the
            // append-only INSERT has no refusing WHERE clause any more, so
            // this only fires on a store oddity or a test seam. Treat it as a failed
            // write: the drain stops, the window stays pending with its cursor, and the
            // next cycle retries.
            LOG.warn("SPM capture checkpoint NOT persisted: the statement reported no"
                    + " written row");
            return false;
        } catch (Exception e) {
            LOG.warn("SPM capture checkpoint write failed (will retry next cycle): {}",
                    e.getMessage());
            return false;
        }
        checkpointWriteSeq = writeSeq;
        durableCheckpointObserved = true;
        pruneStaleCheckpointRows(params, writeEpoch, writeSeq);
        return true;
    }

    /**
     * Best-effort prune of the append-only checkpoint rows the just-written row SUPERSEDES
     * strictly older epochs, plus same-epoch rows with a lower write_seq.
     * The row just written is never its own victim (seq is its token), and the
     * reader's ORDER BY ignores whatever a failed prune leaves behind - so this runs
     * OUTSIDE the checkpoint write's try/catch: the row IS durable, and a GC error must
     * not be reported as a failed checkpoint (which would stop the drain).
     *
     * The prune itself is gated on the just-written row being READABLE: a plain
     * append can return SQL OK while its row is still unreadable, and deleting the rows
     * it supersedes in that state removes the last READABLE pending-window reservation.
     * A new leader then loads an empty store and derives a later window; worse, if the
     * delayed row publishes after that leader's adoption read, the next prune of the new
     * leader deletes the older-epoch row before any cycle can adopt it - the first
     * window's unconsumed tail is lost permanently. The superseded rows are themselves
     * harmless while unreadable (the reader orders by token), so they stay until this
     * append is confirmed.
     */
    private void pruneStaleCheckpointRows(Map<String, String> params, long epoch, long writeSeq) {
        if (!writtenCheckpointVisible(epoch, writeSeq)) {
            LOG.info("SPM capture checkpoint prune deferred: the row {}:{} is not readable"
                    + " yet; the superseded rows stay until it is", epoch, writeSeq);
            return;
        }
        try {
            SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT, () -> {
                try {
                    checkpointWriter.write(CHECKPOINT_PRUNE_SQL, params);
                } catch (Exception pruneFailure) {
                    throw new RuntimeException(pruneFailure);
                }
                return null;
            });
        } catch (Exception pruneFailure) {
            // includes the production seam's CheckpointWriteRefusedException: a DELETE
            // that matched 0 rows is not an error
            LOG.info("SPM capture checkpoint prune skipped: {}", pruneFailure.getMessage());
        }
    }

    /**
     * Whether the checkpoint row this process just wrote is READABLE (the token query
     * resolves to it): the precondition of the prune above. A bounded read-back, like the
     * reservation's visibility probe - internal publication can lag the successful
     * statement, and the confirmation must name THIS row ({@code epoch}:{@code writeSeq}),
     * not merely any readable row.
     */
    private boolean writtenCheckpointVisible(long epoch, long writeSeq) {
        java.util.function.BooleanSupplier seam = checkpointWrittenVisibleForTest;
        if (seam != null) {
            return seam.getAsBoolean();
        }
        if (checkpointSeamsForTest) {
            // scripted reader / writer tests own the store: there is no publication lag
            // to observe, so the confirmation degrades to the previous best-effort prune
            return true;
        }
        for (int attempt = 0; attempt < CHECKPOINT_VISIBILITY_ATTEMPTS; attempt++) {
            try {
                ResultRow newest = firstCheckpointRow(CHECKPOINT_SELECT_SQL);
                if (newest != null
                        && parseLongValue(newest.get(14)) == epoch
                        && parseLongValue(newest.get(15)) == writeSeq) {
                    return true;
                }
            } catch (Exception e) {
                LOG.debug("SPM capture checkpoint visibility probe failed: {}", e.getMessage());
            }
            try {
                Thread.sleep(CHECKPOINT_VISIBILITY_RETRY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * Durably records the window a cycle WOULD have consumed when the store holds no row
     * describing this process's progress yet (see the horizon-read failure in
     * runCaptureCycle): the earliest attempted start lives only in memory otherwise, and
     * a restart / leader handoff before the reads recover makes the successor derive its
     * OWN later window - permanently skipping the eligible rows of the first attempted
     * window (no later overlap reaches behind a new window's start).
     *
     * The reservation carries BOUNDS only (no scan ever ran): cursor, counters and zone
     * list stay at their fresh values, exactly like the window a recovery cycle then
     * scans. A failed write leaves the window pending in memory as well, so the retry -
     * which the caller has already made prompt - resumes the very same bounds.
     *
     * @param cycleFilter the filter this cycle's window was opened with
     */
    private void reserveFirstAttemptedWindowDurably(PlanCaptureFilter cycleFilter) {
        if (durableCheckpointObserved || pendingWindowStart != 0
                || firstAttemptedWindowStart == 0) {
            return;
        }
        pendingWindowStart = firstAttemptedWindowStart;
        pendingWindowEnd = System.currentTimeMillis();
        pendingWindowFilter = cycleFilter;
        if (persistCheckpointAndConfirm()) {
            // the durable record now covers the attempted window: it must not widen a
            // later derivation any more
            firstAttemptedWindowStart = 0;
        }
    }

    /**
     * First-cycle reservation with VISIBILITY confirmation. An internal INSERT can return
     * SQL OK with transaction status COMMITTED although the publication timed out (the
     * default return mode is committed), so a successful write does not prove the row is
     * READABLE: consuming the window then relies on a reservation no takeover can read -
     * a leadership change before publication makes the next FE derive a later window and
     * permanently skip the unconsumed tail of a truncated page (the failed-UPSERT abort
     * does not cover this OK/COMMITTED path). The write is followed by a bounded read-back
     * through the same reader the load path uses; until the row is visible the cycle is
     * skipped and durableCheckpointObserved stays false, so the next cycle
     * re-persists (idempotent UPSERT) and re-confirms.
     *
     * @return true when the reservation is durable AND readable (or persistence is off)
     */
    private boolean persistCheckpointAndConfirm() {
        if (!checkpointPersistenceEnabled()) {
            return true;
        }
        // the reservation's identity: the window bounds written by persistCheckpoint for
        // THIS cycle (they are not touched by the write itself)
        final long reservationStart = pendingWindowStart;
        final long reservationEnd = pendingWindowEnd;
        if (reconcileReadableCheckpoint(reservationStart, reservationEnd)) {
            return false;
        }
        if (!persistCheckpoint()) {
            return false;
        }
        for (int attempt = 0; attempt < CHECKPOINT_VISIBILITY_ATTEMPTS; attempt++) {
            try {
                List<ResultRow> rows = checkpointReader.get();
                if (rows != null && !rows.isEmpty()) {
                    if (isOurReservationRow(rows.get(0), reservationStart, reservationEnd)) {
                        return true;
                    }
                    // A readable FOREIGN row (an earlier leader's reservation that only now
                    // surfaced): it describes a window this process never consumed. Adopting
                    // it cannot lose anything, while consuming OUR window over it would
                    // (= the single-row UPSERT has already replaced it) permanently skip the
                    // foreign window's tail - the exact loss this reservation exists to
                    // prevent.
                    if (reconcileReadableCheckpoint(reservationStart, reservationEnd)) {
                        return false;
                    }
                }
            } catch (Exception e) {
                LOG.debug("SPM capture checkpoint visibility probe failed: {}", e.getMessage());
            }
            try {
                Thread.sleep(CHECKPOINT_VISIBILITY_RETRY_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        durableCheckpointObserved = false;
        LOG.warn("SPM capture: OUR checkpoint reservation is not readable yet (any OTHER"
                + " row is not proof the write became visible); the cycle will retry");
        return false;
    }

    /**
     * Reconciles the FIRST-cycle reservation with the store: an earlier leader's row (its
     * own reservation, a demoted master's, or this process's earlier attempt) can become
     * READABLE only after this process's empty read was cached and its window derived -
     * the reviewer's takeover: leader A commits [09:00,12:00) but its row is still
     * unreadable when B promotes at 12:10 and derives [09:10,12:10); the single-row
     * UNIQUE-key UPSERT would then replace A's record and an eligible 09:05 audit row
     * would fall outside B's window and every later overlap. When such a row surfaces,
     * its window - not this process's freshly derived one - is the window to consume:
     * adopt it (bounds, cursor, retry state), put the adopted state back immediately (so
     * the store converges to it even when this process's own reservation is already
     * queued behind the foreign row), and abort the cycle. The next cycle resumes
     * PROMPTLY from the earlier window, so nothing behind it is skipped.
     *
     * @return true when a foreign row was adopted (the caller must abort the cycle)
     */
    private boolean reconcileReadableCheckpoint(long reservationStart, long reservationEnd) {
        List<ResultRow> rows;
        try {
            rows = checkpointReader.get();
        } catch (Exception e) {
            // the read fails like the cycle-initial one: nothing to reconcile
            return false;
        }
        if (rows == null || rows.isEmpty()) {
            return false;
        }
        ResultRow row = rows.get(0);
        if (isOurReservationRow(row, reservationStart, reservationEnd)) {
            return false;
        }
        LOG.warn("SPM capture: a checkpoint row of an earlier leader is readable (window"
                + " [{}, {})); adopting it instead of replacing it with the derived"
                + " window [{}, {})",
                parseLongValue(row.get(1)), parseLongValue(row.get(2)),
                reservationStart, reservationEnd);
        adoptCheckpointRow(row);
        return true;
    }

    /**
     * Adopts one foreign checkpoint row as THIS process's progress
     * apply the state, write it back (this process's own reservation may
     * already be queued behind the foreign row, and until the adopted state is written
     * back a later takeover could read the superseded window), and resume PROMPTLY. The
     * caller aborts the cycle: consuming this process's derived window over the adopted
     * one would permanently skip the adopted window's unconsumed prefix.
     *
     * Every caller rejected the SAME-state cases first, so the adopted
     * window is always a DIFFERENT one - and the zone-pass credit belongs to the window
     * (see clearPendingWindow). Keeping this process's passes falsely credits
     * zones for the adopted window: a leader that staged its own derived window in UTC
     * (the pass marks its rendering zone before scanning) and then adopted an earlier
     * reservation with scan_zone = -05:00 resumed the adopted window in -05:00
     * only, while UTC stayed marked as covered - a 09:05 row rendered in UTC was never
     * scanned, the completeness check passed, and the watermark advanced over it. The
     * set is therefore reset to the adopted window's baseline: the resumed pass re-seeds
     * its OWN zone (lastScanZone → pass zone, see
     * resolveScanPassZone) and every other required zone must earn a real pass.
     */
    private void adoptCheckpointRow(ResultRow row) {
        scannedZonesInWindow.clear();
        applyCheckpointRow(row);
        persistCheckpoint();
        pendingWindowNeedsPromptResume = true;
    }

    /**
     * Adopts an EARLIER pending window that surfaced AFTER the load. The
     * load consults the checkpoint once; without this step a reservation whose INSERT
     * commits later (a demoted master's forwarded write, an internal-schema publication
     * that timed out) stays invisible to every later read of the token-greatest row, and
     * the unconsumed prefix of its window - the rows before this process's derived window
     * start - is skipped permanently. The production reader surfaces such a row (see
     * readCheckpointRow); this method turns it into the same adoption the
     * reservation phase performs: apply the earlier state, write it back so the store
     * converges to it, and let the caller abort the cycle and resume PROMPTLY.
     *
     * @return true when the caller must abort the cycle (adopted, or the store could not
     *         be re-read: without the read no earlier window can be ruled out)
     */
    private boolean adoptEarlierPendingCheckpoint() {
        if (!checkpointPersistenceEnabled()) {
            return false;
        }
        List<ResultRow> rows;
        try {
            rows = checkpointReader.get();
        } catch (Exception e) {
            // The cycle aborts FAIL CLOSED (an unreadable store must not advance the
            // watermark) and must resume PROMPTLY like every other aborted cycle: at the
            // configured interval (three hours by default) the window this very cycle
            // would have consumed stays unconsumed for that long.
            pendingWindowNeedsPromptResume = true;
            LOG.warn("SPM capture: the checkpoint re-read before this cycle's window failed"
                    + " (retrying promptly): {}", e.getMessage());
            return true; // fail closed: an unreadable store must not advance the watermark
        }
        if (rows == null || rows.isEmpty()) {
            return false;
        }
        ResultRow row = rows.get(0);
        if (!isPendingWindowRow(row)) {
            return false; // the resolved row is plain progress, not a pending window
        }
        long start = parseLongValue(row.get(1));
        long end = parseLongValue(row.get(2));
        if (start == pendingWindowStart && end == pendingWindowEnd) {
            return false; // the window this process is already consuming
        }
        if (scannedFromMillis > 0 && start >= scannedFromMillis) {
            // Inside what THIS process has already scanned or will scan (see
            // scannedFromMillis): the stale reservation row of a CONSUMED window stays
            // readable in the append-only table, and adopting it again would re-scan the
            // same span on every wakeup. Only a row naming an EARLIER, unscanned prefix
            // is new information.
            return false;
        }
        LOG.warn("SPM capture: a pending checkpoint window of an earlier leader is readable"
                + " NOW (window [{}, {})); adopting it before advancing this process's"
                + " progress", start, end);
        adoptCheckpointRow(row);
        return true;
    }

    /**
     * Whether one resolved checkpoint row IS a pending window: a window its writer never
     * fully consumed. Size-safe: rows fabricated by tests / written by an
     * older build can carry fewer than the three leading columns.
     */
    private static boolean isPendingWindowRow(ResultRow row) {
        if (row == null || row.getValues().size() < 3) {
            return false;
        }
        long start = parseLongValue(row.get(1));
        long end = parseLongValue(row.get(2));
        return start > 0 && start < end;
    }

    /**
     * The production checkpoint read: the row a read must resolve to is NOT
     * always the token-greatest one. The append-only table keeps an EARLIER leader's
     * pending reservation even when its INSERT commits only after this process's first
     * read, and the token ordering then hides it forever - see
     * CHECKPOINT_SELECT_PENDING_SQL. The read therefore also asks for the
     * most-behind pending window and prefers it over the newest progress unless
     * chooseCheckpointRow decides otherwise.
     *
     * @return the resolved row as a one-element list (empty when the store has no row)
     */
    private List<ResultRow> readCheckpointRow() {
        ResultRow newest = firstCheckpointRow(CHECKPOINT_SELECT_SQL);
        ResultRow behind = firstCheckpointRow(CHECKPOINT_SELECT_PENDING_SQL);
        ResultRow chosen = chooseCheckpointRow(newest, behind, pendingWindowStart,
                pendingWindowEnd, scannedFromMillis);
        if (chosen != null && chosen != newest && LOG.isDebugEnabled()) {
            LOG.debug("SPM capture checkpoint read: preferring the earlier pending window"
                    + " [{}, {}) over the token-greatest progress",
                    parseLongValue(chosen.get(1)), parseLongValue(chosen.get(2)));
        }
        return chosen == null ? Collections.emptyList() : Collections.singletonList(chosen);
    }

    /** The first row of one checkpoint query; null when the store has none. */
    private static ResultRow firstCheckpointRow(String sql) {
        List<ResultRow> rows = StatisticsUtil.executeQuery(sql, Collections.emptyMap(),
                CHECKPOINT_IO_TIMEOUT_SECONDS);
        return rows == null || rows.isEmpty() ? null : rows.get(0);
    }

    /**
     * Which of the two candidate rows a read resolves to (pure so the
     * priority is testable without a store).
     *
     * The most-behind pending row WINS - it names a window whose unconsumed prefix no
     * token-greater row accounts for - UNLESS
     *   it is not a pending window (start <= 0 or start >= end): the
     *       token row is the state to resume;
     *   it carries the SAME window as the newest row: both describe one state, and
     *       the higher token is at least as new;
     *   it IS the window this process already consumes (the current
     *       pendingWindow bounds): there is nothing to adopt;
     *   it lies inside [scannedFrom, +infinity): this process's own window
     *       chain (contiguous from the first window it scanned, see
     *       scannedFromMillis) has scanned it or will scan it - adopting a
     *       consumed window again would re-scan the same span on every wakeup, because
     *       the append-only table keeps its reservation row.
     *
     * @param newest the token-greatest row (null = none)
     * @param behind the most-behind pending row (null = none)
     * @param currentPendingStart this process's current pending window start
     * @param currentPendingEnd this process's current pending window end
     * @param scannedFrom the earliest window start THIS process began scanning (0 = none)
     * @return the row the read resolves to
     */
    @VisibleForTesting
    public static ResultRow chooseCheckpointRow(ResultRow newest, ResultRow behind,
            long currentPendingStart, long currentPendingEnd, long scannedFrom) {
        if (behind == null || !isPendingWindowRow(behind)) {
            return newest; // nothing behind, or the behind row is not a pending window
        }
        if (newest == null) {
            return behind;
        }
        long behindStart = parseLongValue(behind.get(1));
        long behindEnd = parseLongValue(behind.get(2));
        if (newest.getValues().size() > 2
                && behindStart == parseLongValue(newest.get(1))
                && behindEnd == parseLongValue(newest.get(2))) {
            return newest; // the same window: the higher token is at least as new
        }
        if (behindStart == currentPendingStart && behindEnd == currentPendingEnd) {
            return newest; // the window this process already consumes
        }
        if (scannedFrom > 0 && behindStart >= scannedFrom) {
            return newest; // already scanned / will be scanned by this process's chain
        }
        return behind;
    }

    /**
     * Whether the read-back row IS the reservation just written. A non-empty read is NOT
     * proof: an old master demoted mid-cycle can publish its OWN reservation after this
     * cycle's successful-but-not-yet-visible write, and treating that foreign row as
     * confirmation would consume this window WHILE the final UPSERT replaces the old
     * master's still-unconsumed pending window. In the single-row store the pending
     * bounds identify the reservation: any other window is not this write, the SAME
     * window describes the very page this cycle is about to consume.
     */
    private static boolean isOurReservationRow(ResultRow row, long reservationStart,
            long reservationEnd) {
        return parseLongValue(row.get(1)) == reservationStart
                && parseLongValue(row.get(2)) == reservationEnd;
    }

    private static long parseLongValue(String text) {
        if (text == null || text.isEmpty()) {
            return 0L;
        }
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * JSON of the failed-attempt counters, bounded to the most recent entries so the
     * checkpoint row stays small. Package-visible for tests.
     */
    @VisibleForTesting
    public static String encodeFailedAttempts(Map<String, Integer> attempts) {
        return new Gson().toJson(boundedTail(attempts, MAX_PERSISTED_RETRIES));
    }

    /** Decodes encodeFailedAttempts; blank / broken input decodes to empty. */
    @VisibleForTesting
    public static Map<String, Integer> decodeFailedAttempts(String text) {
        return decodeJsonMap(text, new TypeToken<Map<String, Integer>>() { });
    }

    /**
     * JSON of the queued retry candidates, bounded to the most recent entries. The whole
     * candidate is persisted so a resumed process can retry it without re-reading the
     * audit row (the keyset cursor has already moved past it).
     */
    @VisibleForTesting
    public static String encodeRetryQueue(Map<String, CapturedQuery> queue) {
        List<Map<String, String>> encoded = new ArrayList<>();
        int skip = Math.max(0, queue.size() - MAX_PERSISTED_RETRIES);
        int index = 0;
        for (Map.Entry<String, CapturedQuery> entry : queue.entrySet()) {
            if (index++ < skip) {
                continue;
            }
            CapturedQuery candidate = entry.getValue();
            Map<String, String> row = new HashMap<>();
            row.put("queryId", entry.getKey());
            row.put("stmt", candidate.getStmt() == null ? "" : candidate.getStmt());
            row.put("queryTimeMs", String.valueOf(candidate.getQueryTimeMs()));
            row.put("scanRows", String.valueOf(candidate.getScanRows()));
            row.put("returnRows", String.valueOf(candidate.getReturnRows()));
            row.put("sqlDigest", candidate.getSqlDigest() == null ? "" : candidate.getSqlDigest());
            row.put("sqlHash", candidate.getSqlHash() == null ? "" : candidate.getSqlHash());
            row.put("db", candidate.getDb() == null ? "" : candidate.getDb());
            row.put("catalog", candidate.getCatalog() == null ? "" : candidate.getCatalog());
            row.put("sqlMode", String.valueOf(candidate.getSqlMode()));
            encoded.add(row);
        }
        return new Gson().toJson(encoded);
    }

    /** Decodes encodeRetryQueue; blank / broken input decodes to empty. */
    @VisibleForTesting
    public static Map<String, CapturedQuery> decodeRetryQueue(String text) {
        Map<String, CapturedQuery> queue = new LinkedHashMap<>();
        if (text == null || text.trim().isEmpty()) {
            return queue;
        }
        try {
            List<Map<String, String>> decoded = new Gson().fromJson(text,
                    new TypeToken<List<Map<String, String>>>() { }.getType());
            if (decoded == null) {
                return queue;
            }
            for (Map<String, String> row : decoded) {
                String queryId = row.get("queryId");
                CapturedQuery candidate = new CapturedQuery(
                        row.getOrDefault("stmt", ""),
                        parseLongValue(row.get("queryTimeMs")),
                        parseLongValue(row.get("scanRows")),
                        parseLongValue(row.get("returnRows")),
                        row.getOrDefault("sqlDigest", ""),
                        row.getOrDefault("sqlHash", ""),
                        row.getOrDefault("db", ""),
                        row.getOrDefault("catalog", ""),
                        queryId == null ? "" : queryId,
                        false,
                        AuditLogScanner.decodeAuditSqlMode(row.get("sqlMode")));
                queue.put(queryId == null ? "" : queryId, candidate);
            }
        } catch (RuntimeException e) {
            LOG.warn("SPM capture retry-queue decode failed: {}", e.getMessage());
        }
        return queue;
    }

    private static Map<String, Integer> decodeJsonMap(String text, TypeToken<Map<String, Integer>> token) {
        if (text == null || text.trim().isEmpty()) {
            return new LinkedHashMap<>();
        }
        try {
            Map<String, Integer> decoded = new Gson().fromJson(text, token.getType());
            return decoded == null ? new LinkedHashMap<>() : decoded;
        } catch (RuntimeException e) {
            LOG.warn("SPM capture checkpoint JSON decode failed: {}", e.getMessage());
            return new LinkedHashMap<>();
        }
    }

    /** The most recent limit entries of an insertion-ordered map. */
    private static <V> Map<String, V> boundedTail(Map<String, V> source, int limit) {
        if (source.size() <= limit) {
            return source;
        }
        Map<String, V> tail = new LinkedHashMap<>();
        int skip = source.size() - limit;
        int index = 0;
        for (Map.Entry<String, V> entry : source.entrySet()) {
            if (index++ < skip) {
                continue;
            }
            tail.put(entry.getKey(), entry.getValue());
        }
        return tail;
    }

    // ==================== statistics (design doc 7.2.6) ====================

    /**
     * Snapshot of the capture counters.
     */
    public static class CaptureStats {
        public final long success;
        public final long duplicate;
        public final long singleTable;
        public final long filtered;
        public final long failed;

        CaptureStats(long success, long duplicate, long singleTable, long filtered, long failed) {
            this.success = success;
            this.duplicate = duplicate;
            this.singleTable = singleTable;
            this.filtered = filtered;
            this.failed = failed;
        }

        @Override
        public String toString() {
            return "CaptureStats{captured=" + success
                    + ", dup=" + duplicate
                    + ", singleTable=" + singleTable
                    + ", filtered=" + filtered
                    + ", fail=" + failed + "}";
        }
    }

    /**
     * Returns the current capture statistics.
     *
     * @return a snapshot of the capture counters
     */
    public CaptureStats getStats() {
        return new CaptureStats(successCount.get(), skipDuplicateCount.get(),
                skipSingleTableCount.get(), skipFilterCount.get(), failCount.get());
    }

    /**
     * Minimum scan-window overlap: the greater of the base five minutes and TWO of the
     * whole upstream publication delay - the audit loader batch interval
     * (audit_plugin_max_batch_interval_sec) PLUS the delay before an event even
     * reaches the loader: WorkloadRuntimeStatusMgr holds a finished query's audit event
     * until query_audit_log_timeout_ms (or, while external DML statistics are
     * still awaited, up to be_report_query_statistics_timeout_ms) has passed, and
     * the loader then only polls its queue every AuditLoader#QUEUE_POLL_INTERVAL_MILLIS.
     *
     * A row is reachable ONLY while its event time is still inside a window's range or
     * inside the overlap of a later one (pagination walks the event time DESC, so a late
     * row above the page cursor is never reached by the pending pages). With only the
     * batch interval covered, a row released later than that - a large
     * query_audit_log_timeout_ms / be_report_query_statistics_timeout_ms, a slow loader
     * queue - fell outside every later overlap and was silently never captured; the
     * horizon must therefore follow the WHOLE upstream delay, not just the batch
     * interval. The completion predicate (time + query_time >= start) stays as is: it
     * covers the query DURATION, this overlap covers the publication delay.
     *
     * @param auditBatchIntervalSec the configured audit loader batch interval (seconds)
     * @return the overlap in milliseconds (never less than SCAN_WINDOW_OVERLAP_MS)
     */
    static long scanWindowOverlapMs(long auditBatchIntervalSec) {
        long batchMs = Math.max(0L, auditBatchIntervalSec) * 1000L;
        long upstreamDelayMs = Math.max(
                Math.max(Config.query_audit_log_timeout_ms,
                        Config.be_report_query_statistics_timeout_ms),
                AuditLoader.QUEUE_POLL_INTERVAL_MILLIS);
        return Math.max(SCAN_WINDOW_OVERLAP_MS, 2 * (batchMs + upstreamDelayMs));
    }

    /**
     * Overlap including the LOCAL audit loader's outstanding queue horizon. The delay a
     * batch interval plus the configured holds describe is a BOUNDED wait; it says nothing
     * about how long the loader itself needs to PUBLISH the events it already accepted - a
     * slow / stalled stream load leaves them in the AuditLoader queue long after
     * that bound, and no fixed overlap can cover an unbounded backlog (the reviewer's
     * example: a short 11:50 query missing from the 12:00 scan but loaded successfully at
     * 12:02; a five minute overlap never re-reads it again). The window is therefore never
     * allowed to start after the oldest event the loader still owes: while such an event
     * exists, its instant fenced the window start, so every row from that instant on is
     * re-scanned until the backlog is published.
     *
     * @param auditBatchIntervalSec the configured audit loader batch interval (seconds)
     * @param oldestUnpublishedEventMs AuditLoader#oldestUnpublishedEventTime()
     *        (0 when the loader is not running or has nothing outstanding)
     * @param currentTimeMs the cycle's clock reading
     * @return the overlap in milliseconds (never less than scanWindowOverlapMs(long))
     */
    static long scanWindowOverlapMs(long auditBatchIntervalSec, long oldestUnpublishedEventMs,
            long currentTimeMs) {
        long baseMs = scanWindowOverlapMs(auditBatchIntervalSec);
        if (oldestUnpublishedEventMs <= 0 || currentTimeMs <= oldestUnpublishedEventMs) {
            return baseMs;
        }
        return Math.max(baseMs, currentTimeMs - oldestUnpublishedEventMs);
    }

    /**
     * Resolves the (start, end) window the next capture cycle scans.
     *
     * A truncated cycle leaves pendingStart/pendingEnd set: the SAME window is
     * scanned again (from the stored cursor) until it is exhausted, because a newly
     * derived interval window would start around the pending window's end and leave every
     * row the cursor has not reached yet permanently out of scope. Without a pending
     * window the bounds are derived from the watermark (with the late-arrival overlap).
     *
     * @return [windowStart, windowEnd]
     */
    static long[] resolveScanWindow(long lastScanTimestamp, long pendingStart, long pendingEnd,
            long currentTime, long intervalMs, long overlapMs) {
        return resolveScanWindow(lastScanTimestamp, pendingStart, pendingEnd, currentTime,
                intervalMs, overlapMs, 0);
    }

    /**
     * Same as the six-argument overload, with the floor of a FAILED first checkpoint read
     * (firstAttemptedWindowStart): a fresh process whose first read failed must not
     * skip the window that cycle would have consumed, so the first derived window starts
     * there when that is EARLIER than the interval-derived start.
     *
     * The FIRST window (no watermark yet) is widened by the overlap as well
     * #8): the overlap already folds the cluster publication horizon (see
     * scanWindowOverlapMs(long, long, long)), and a first pass delayed by leader
     * readiness must start NO LATER than the oldest event any FE still owes. A 13:00 cycle
     * with a three-hour interval and a valid 09:00 follower horizon otherwise started at
     * 10:00, and a short 09:00 row publishing before the scan fell outside both the window
     * and the next overlap (which only reaches behind a watermark that is never moved back).
     *
     * @return [windowStart, windowEnd]
     */
    static long[] resolveScanWindow(long lastScanTimestamp, long pendingStart, long pendingEnd,
            long currentTime, long intervalMs, long overlapMs, long firstAttemptedStart) {
        if (pendingEnd > 0) {
            return new long[] {pendingStart, pendingEnd};
        }
        long start = (lastScanTimestamp == 0)
                ? Math.max(0L, currentTime - Math.max(intervalMs, overlapMs))
                : Math.max(0L, lastScanTimestamp - overlapMs);
        if (lastScanTimestamp == 0 && firstAttemptedStart > 0 && firstAttemptedStart < start) {
            start = firstAttemptedStart;
        }
        return new long[] {start, currentTime};
    }

    /**
     * The zone THIS cycle's scan pass renders its window bounds in.
     *
     * A window that is mid-drain keeps the rendering frozen in its cursor (the tail
     * records the zone its pages were rendered in, so a global time_zone change cannot mix
     * two renderings inside one keyset walk). A window whose pass has NOT started - a new
     * window, or one whose cursor was just reset for the zone-change re-scan - renders in
     * the zone the PREVIOUS pass used while that differs from the current global zone: the
     * rows published before `SET GLOBAL time_zone` are stored in the old rendering and are
     * invisible to bounds rendered in the new zone, which would silently exhaust the window
     * and advance the watermark past them (see AuditLogScanner#scan). The following pass
     * revisits the same window in the new zone for the rows published after the change.
     *
     * @param currentZoneId the current global session time_zone's ID
     * @return the zone ID to render this pass in
     */
    private String resolveScanPassZone(String currentZoneId) {
        String frozenZone = AuditLogScanner.zoneIdOfTail(cursorTail);
        if (frozenZone != null) {
            return frozenZone;
        }
        return lastScanZone == null || lastScanZone.isEmpty() ? currentZoneId : lastScanZone;
    }

    /** The first zone of required without a completed pass yet, or null (none). */
    private static String firstNotScanned(Set<String> required, Set<String> scanned) {
        for (String zone : required) {
            if (zone != null && !zone.isEmpty() && !scanned.contains(zone)) {
                return zone;
            }
        }
        return null;
    }

    /**
     * Whether an outstanding publication fences the window's completion: an
     * event that is still unpublished with an instant INSIDE [scanStart, scanEnd)
     * may expose its row anywhere in the window when it publishes - including BELOW the
     * position the descending keyset pagination already walked past - so the drained
     * pages cannot be trusted to have seen it.
     *
     * A positive horizon BELOW the window does NOT establish that the
     * window is clear either. The horizon is the MINIMUM over all outstanding events, so
     * an older unpublished event (a long 08:30 query) MASKS a newer one inside the window
     * (an unpublished 10:15 row): the min drops below scanStart while the 10:15
     * row is still in flight, the window would be treated as complete, and the next fixed
     * overlap can start after that row. The only sound reading of the value is "some
     * event below scanEnd is outstanding": it may own a row this window scans (rows are
     * admitted by membership OR by completion reaching the window start), so every
     * positive horizon below scanEnd retains the window. A horizon at or after
     * scanEnd belongs to later windows only.
     *
     * @param publicationHorizon the cluster horizon observed at cycle start
     * @param scanEnd the window end (epoch millis)
     * @return whether the window must stay pending
     */
    private static boolean blocksWindowCompletion(long publicationHorizon, long scanEnd) {
        return publicationHorizon > 0 && publicationHorizon < scanEnd;
    }

    private void clearPendingWindow() {
        pendingWindowStart = 0;
        pendingWindowEnd = 0;
        // the pinned threshold snapshot belongs to the window: a later window must follow
        // the globals again
        pendingWindowFilter = null;
        // the per-window zone-pass record belongs to the window as well
        scannedZonesInWindow.clear();
    }

    /**
     * For tests: resets the counters, the scan window and the resume cursor.
     */
    public void resetForTest() {
        clearProgressState();
        // restore the filter from the CURRENT globals: a test that installed its own filter
        // (setFilterForTest) must not leak it into the next test's candidates
        this.filter = buildFilterFromGlobal();
        // restore the production read / write seams (tests replace them)
        checkpointReader = this::readCheckpointRow;
        checkpointWriter = (sql, params) -> {
            QueryState state = StatisticsUtil.execUpdate(sql, params, CHECKPOINT_IO_TIMEOUT_SECONDS);
            if (state != null && !checkpointWriteAccepted(state.getAffectedRows())) {
                throw new CheckpointWriteRefusedException();
            }
        };
        checkpointSeamsForTest = false;
        checkpointWrittenVisibleForTest = null;
        auditQueueHorizon = AuditPublicationHorizon::clusterHorizon;
        auditWriterZones = AuditPublicationHorizon::clusterWriterZones;
        checkpointEpoch = PlanCaptureManager::currentLeaderEpoch;
        // the append-only write counter is process state, like the epoch supplier
        checkpointWriteSeq = 0;
        // the writer-zone registry is process-wide (written by the audit loader): a test
        // must not inherit another test's zones through the production supplier
        AuditWriterZones.resetForTest();
        successCount.set(0);
        skipDuplicateCount.set(0);
        skipSingleTableCount.set(0);
        skipFilterCount.set(0);
        failCount.set(0);
    }

    /** Drops every in-memory PROGRESS field (window / cursor / caches), as a fresh process. */
    private void clearProgressState() {
        lastScanTimestamp = 0;
        clearPendingWindow();
        firstAttemptedWindowStart = 0;
        lastScanZone = "";
        cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
        cursorTime = "";
        cursorQueryId = "";
        cursorTail = "";
        pageStartLastScanTimestamp = 0;
        pageStartWindowStart = 0;
        pageStartWindowEnd = 0;
        pageStartCursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
        pageStartCursorTime = "";
        pageStartCursorQueryId = "";
        pageStartCursorTail = "";
        pageStartZoneId = "";
        pageStartFilter = null;
        processedQueryIds.clear();
        failedCaptureAttempts.clear();
        failedCaptureQueue.clear();
        failedCaptureAnchors.clear();
        queuedFailureChars = 0;
        checkpointLoaded = false;
        durableCheckpointObserved = false;
        scannedFromMillis = 0;
    }

    /**
     * MASTER PROMOTION hook (called from Env.transferToMaster): the in-memory progress may
     * be STALE - this FE ran the daemon under an earlier mastership, or lost a cycle after
     * a demotion - while the interim master advanced (or rewound) the durable checkpoint.
     * Continuing from the stale cursor / queue would either skip rows the interim master
     * had not consumed yet, or clobber its retry queue on the next persist (a queued row
     * behind the revived cursor is never retried). Drop the local progress and force the
     * next cycle to RELOAD the durable checkpoint, exactly like a freshly started process
     * (loadCheckpointIfNeeded).
     *
     * The initial-reservation confirmation of a cycle does not cover this: it protects
     * the window THIS process is about to consume, not progress adopted from an earlier
     * mastership.
     */
    public void reloadCheckpointOnPromotion() {
        clearProgressState();
        LOG.info("SPM capture checkpoint state dropped on master promotion; the next cycle"
                + " reloads the durable checkpoint");
    }

    /** The live leadership probe of the checkpoint write (see persistCheckpoint). */
    private boolean isLeaderForCheckpointWrite() {
        if (checkpointLeadershipProbeForTest != null) {
            return checkpointLeadershipProbeForTest.getAsBoolean();
        }
        if (FeConstants.runningUnitTest || Env.getCurrentEnv() == null
                || checkpointSeamsForTest) {
            // unit tests / an uninitialized process / a scripted store have no live
            // master to fence (same convention as BaselineManager.assertLeaderForWrite)
            return true;
        }
        return Env.getCurrentEnv().isMaster();
    }

    /**
     * The checkpoint UPSERT's fencing token: the FE's current max journal
     * id. Journal ids are a cluster-wide MONOTONIC clock that grows across leadership
     * changes (a new master journals before it can service writes), so a demoted
     * leader's statement - which forwards to the new master and executes THERE - carries
     * an epoch the stored row already exceeds, and the conditional statement refuses to
     * overwrite. When the journal is unavailable (partial startup) the value falls back
     * to 0: still refused behind any leader that ever wrote a positive epoch, and the
     * first write of a fresh cluster (no row yet) always passes (the COALESCE floor).
     *
     * @return the fencing token
     */
    static long currentLeaderEpoch() {
        try {
            Env env = Env.getCurrentEnv();
            if (env == null) {
                return 0L;
            }
            Long journalId = env.getMaxJournalId();
            return journalId == null ? 0L : journalId;
        } catch (Throwable t) {
            return 0L;
        }
    }

    /**
     * For tests: replaces the audit scanner (e.g. with a scripted subclass).
     *
     * @param testScanner the scanner to use
     */
    @VisibleForTesting
    void setScannerForTest(AuditLogScanner testScanner) {
        this.scanner = testScanner;
    }

    /**
     * Next scan watermark: only a FULLY consumed window may advance to its end. A window
     * truncated by the batch limit keeps its watermark and resumes from the batch cursor
     * instead (see runAfterCatalogReady / AuditLogScanner).
     *
     * @param lastScanTimestamp the current watermark
     * @param currentTime       the window end just scanned
     * @param windowExhausted   whether the batch consumed the whole window
     * @return the next watermark
     */
    @VisibleForTesting
    static long nextScanTimestamp(long lastScanTimestamp, long currentTime, boolean windowExhausted) {
        return windowExhausted ? currentTime : lastScanTimestamp;
    }

    /**
     * For tests: processes a single candidate without touching the scanner.
     *
     * @param candidate the candidate query
     * @return true when the candidate reached a terminal state (see processCandidate)
     */
    public boolean processCandidateForTest(CapturedQuery candidate) {
        return processCandidate(candidate);
    }

    /**
     * For tests: runs one candidate through the query-id tracking AND the capture
     * pipeline, exactly like one cycle's loop body does.
     *
     * @param candidate the candidate query
     */
    @VisibleForTesting
    public void handleCandidateForTest(CapturedQuery candidate) {
        handleCandidate(candidate);
    }

    /**
     * For tests: the live checkpoint fields (lastScan, pendingStart, pendingEnd,
     * cursorQueryTime, cursorTime, cursorQueryId, cursorTail, minQueryTimeMs,
     * minScanRows, includePattern, excludePattern).
     */
    @VisibleForTesting
    public Object[] checkpointFieldsForTest() {
        return new Object[] {lastScanTimestamp, pendingWindowStart, pendingWindowEnd,
                cursorQueryTime, cursorTime, cursorQueryId, cursorTail,
                pendingWindowFilter == null ? -1L : pendingWindowFilter.getMinQueryTimeMs(),
                pendingWindowFilter == null ? -1L : pendingWindowFilter.getMinScanRows(),
                pendingWindowFilter == null ? "" : pendingWindowFilter.getIncludePatternText(),
                pendingWindowFilter == null ? "" : pendingWindowFilter.getExcludePatternText()};
    }

    /**
     * For tests: whether the last cycle left a window that must resume PROMPTLY (the
     * daemon then sleeps PENDING_WINDOW_RESUME_INTERVAL_MS instead of the
     * configured interval).
     *
     * @return true when a pending window could not be finished by the last cycle
     */
    @VisibleForTesting
    public boolean isPendingWindowResumePromptForTest() {
        return pendingWindowNeedsPromptResume;
    }

    /**
     * For tests: the zone ID the last scan pass rendered its bounds in (see
     * lastScanZone) - empty when nothing was scanned yet.
     *
     * @return the zone ID
     */
    @VisibleForTesting
    public String lastScanZoneForTest() {
        return lastScanZone;
    }

    /**
     * For tests: the zone ID the NEXT cycle's scan pass will render its bounds in, given
     * the current global zone (see resolveScanPassZone).
     *
     * @param currentZoneId the current global session time_zone id
     * @return the zone ID to render in
     */
    @VisibleForTesting
    public String resolveScanPassZoneForTest(String currentZoneId) {
        return resolveScanPassZone(currentZoneId);
    }

    /**
     * For tests: the page budget of ONE cycle (see MAX_PAGES_PER_CYCLE).
     *
     * @return how many audit pages one wakeup may consume
     */
    @VisibleForTesting
    public static int maxPagesPerCycleForTest() {
        return MAX_PAGES_PER_CYCLE;
    }

    /**
     * For tests: overrides the queued-failure budget (see MAX_QUEUED_FAILURE_CHARS).
     *
     * @param budget statement characters the queue may retain, null for the production value
     */
    @VisibleForTesting
    public static void setQueuedFailureBudgetForTest(Long budget) {
        queuedFailureBudgetForTest = budget;
    }

    /**
     * For tests: replays the queued failures exactly like one capture cycle does.
     *
     * @param scannedQueryIds the query ids this cycle's page already processed
     */
    @VisibleForTesting
    public void replayQueuedFailuresForTest(Set<String> scannedQueryIds) {
        replayQueuedFailures(scannedQueryIds);
    }

    /**
     * For tests: whether the candidate is queued for a later retry attempt.
     *
     * @param queryId the audit query id
     * @return true when the id is queued
     */
    @VisibleForTesting
    public boolean isQueuedForTest(String queryId) {
        return failedCaptureQueue.containsKey(queryId);
    }

    /**
     * For tests: whether the query id is tracked as consumed (the next overlapping scan
     * would skip it).
     *
     * @param queryId the audit query id
     * @return true when the id is terminal
     */
    @VisibleForTesting
    public boolean isQueryIdTrackedForTest(String queryId) {
        return processedQueryIds.containsKey(queryId);
    }

    /**
     * For tests: the failed-attempt count of a query id.
     *
     * @param queryId the audit query id
     * @return the number of failed attempts so far
     */
    @VisibleForTesting
    public int failedAttemptsForTest(String queryId) {
        return failedCaptureAttempts.getOrDefault(queryId, 0);
    }

    /**
     * For tests: returns the internal filter.
     */
    public PlanCaptureFilter getFilter() {
        return filter;
    }

    /**
     * For tests: installs the filter the next candidate handling uses (a capture cycle
     * refreshes it from the globals itself).
     *
     * @param testFilter the filter to use
     */
    @VisibleForTesting
    public void setFilterForTest(PlanCaptureFilter testFilter) {
        this.filter = testFilter;
    }

    @VisibleForTesting
    public boolean isCheckpointLoadedForTest() {
        return checkpointLoaded;
    }

    @VisibleForTesting
    public boolean isDurableCheckpointObservedForTest() {
        return durableCheckpointObserved;
    }

    @VisibleForTesting
    public void setCheckpointReaderForTest(Supplier<List<ResultRow>> reader) {
        this.checkpointReader = reader;
        this.checkpointSeamsForTest = true;
    }

    @VisibleForTesting
    public void setCheckpointWriterForTest(CheckpointWriter writer) {
        this.checkpointWriter = writer;
        this.checkpointSeamsForTest = true;
    }

    /**
     * For tests: decides whether the checkpoint row just written counts as READABLE
     * (the gate of the prune, see writtenCheckpointVisible). Null means "probe the
     * store" - i.e. "a scripted test's row is always considered visible".
     */
    @VisibleForTesting
    public void setCheckpointWrittenVisibleForTest(
            java.util.function.BooleanSupplier visible) {
        this.checkpointWrittenVisibleForTest = visible;
    }

    /** For tests: installs a scripted local audit loader publication horizon. */
    @VisibleForTesting
    public void setAuditQueueHorizonForTest(LongSupplier horizon) {
        this.auditQueueHorizon = horizon;
    }

    /** For tests: installs a scripted cluster audit WRITER zone set. */
    @VisibleForTesting
    public void setAuditWriterZonesForTest(Supplier<Set<String>> zones) {
        this.auditWriterZones = zones;
    }

    /** For tests: installs a scripted checkpoint UPSERT epoch. */
    @VisibleForTesting
    public void setCheckpointEpochForTest(LongSupplier epoch) {
        this.checkpointEpoch = epoch;
    }

    @VisibleForTesting
    public void loadCheckpointForTest() {
        loadCheckpointIfNeeded();
    }

    @VisibleForTesting
    public void persistCheckpointForTest() {
        persistCheckpoint();
    }

    /**
     * For tests: the append-only checkpoint INSERT (see CHECKPOINT_INSERT_SQL).
     * The column NAMES must stay one-to-one with
     * org.apache.doris.catalog.InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA - the
     * VALUES bind positionally, but against that NAMED column list - so an upgraded table
     * with a different PHYSICAL order cannot shift the values.
     */
    @VisibleForTesting
    public static String checkpointInsertSqlForTest() {
        return CHECKPOINT_INSERT_SQL;
    }

    /**
     * Whether the conditional UPSERT's affected-row count PROVES it wrote the row
     * . An unknown count (-1) is NOT proof of a refusal, so it counts as
     * written (the same convention as BaselineManager#insertWroteRows).
     *
     * @param affectedRows the statement's affected-row count
     * @return whether the write may be treated as landed
     */
    @VisibleForTesting
    static boolean checkpointWriteAccepted(long affectedRows) {
        return affectedRows != 0;
    }

    /**
     * For tests: rewrites the counters used by the truncation guard in
     * persistCheckpoint() (failedCaptureAttempts is a map and cannot be
     * sized through the public seams), and the retry queue, plus the PRE-PAGE state the
     * durable fallback is taken from.
     */
    @VisibleForTesting
    public void seedCheckpointStateForTest(long windowStart, long windowEnd,
            long pageStartCursorQueryTime, String pageStartCursorTime,
            String pageStartCursorQueryId, int failedQueueEntries, long lastScanTimestamp,
            long cursorQueryTime, String cursorTime, String cursorQueryId) {
        seedCheckpointStateForTest(windowStart, windowEnd, pageStartCursorQueryTime,
                pageStartCursorTime, pageStartCursorQueryId, failedQueueEntries,
                lastScanTimestamp, cursorQueryTime, cursorTime, cursorQueryId, "", "");
    }

    /**
     * For tests: same as the ten-argument overload, with explicit cursor tails (the
     * durable tail must fall back to the PRE-PAGE tail exactly like the other cursor
     * fields when the retry state was truncated).
     */
    @VisibleForTesting
    public void seedCheckpointStateForTest(long windowStart, long windowEnd,
            long pageStartCursorQueryTime, String pageStartCursorTime,
            String pageStartCursorQueryId, int failedQueueEntries, long lastScanTimestamp,
            long cursorQueryTime, String cursorTime, String cursorQueryId,
            String pageStartCursorTail, String cursorTail) {
        for (int i = 0; i < failedQueueEntries; i++) {
            failedCaptureAttempts.put("seed-failed-" + i, 1);
            failedCaptureQueue.put("seed-failed-" + i,
                    new CapturedQuery("select " + i, 1, 1, 1, "d", "h", "db", "cat",
                            "q:" + i, false, SqlModeHelper.MODE_DEFAULT));
            // entries queued by the CURRENT page carry the pre-page anchor of this page
            // (first-wins, mirroring handleCandidate): the truncation guard persists the
            // OLDEST queued entry's anchor instead of blindly the current page start
            failedCaptureAnchors.putIfAbsent("seed-failed-" + i, new RetryAnchor(
                    lastScanTimestamp - 1, windowStart, windowEnd, pageStartCursorQueryTime,
                    pageStartCursorTime, pageStartCursorQueryId, pageStartCursorTail,
                    pageStartZoneId, pageStartFilter));
        }
        this.pageStartLastScanTimestamp = lastScanTimestamp - 1;
        this.pageStartWindowStart = windowStart;
        this.pageStartWindowEnd = windowEnd;
        this.pageStartCursorQueryTime = pageStartCursorQueryTime;
        this.pageStartCursorTime = pageStartCursorTime;
        this.pageStartCursorQueryId = pageStartCursorQueryId;
        this.pageStartCursorTail = pageStartCursorTail;
        // the seeded page is the CURRENT one: it renders in the zone this cycle would
        // use (the last one observed), exactly like a live page start
        this.pageStartZoneId = this.lastScanZone;
        this.pendingWindowStart = windowStart;
        this.pendingWindowEnd = windowEnd;
        this.lastScanTimestamp = lastScanTimestamp;
        this.cursorQueryTime = cursorQueryTime;
        this.cursorTime = cursorTime;
        this.cursorQueryId = cursorQueryId;
        this.cursorTail = cursorTail;
    }
}
