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
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;
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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
     * Test seam replacing the live leadership probe of {@link #persistCheckpoint} (null in
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
     * Bounded retries for a FAILED capture: the query id stays retryable for later
     * overlapping scans until it either succeeds or reaches this attempt count. Marking
     * the id before processing would make a transient failure permanent - the
     * overlapping scans would skip the row and the watermark passes it long before the
     * dedup map evicts the entry.
     */
    private static final int MAX_CAPTURE_ATTEMPTS = 3;

    /** Durable checkpoint key: the internal table holds exactly one row. */
    private static final long CHECKPOINT_ID = 1L;

    /** Upper bound for the retry entries written into the checkpoint row (row size). */
    private static final int MAX_PERSISTED_RETRIES = 64;

    /** Table of the durable capture checkpoint (see InternalSchema). */
    private static final String CHECKPOINT_TABLE =
            "`__internal_schema`.`spm_capture_checkpoint`";

    private static final String CHECKPOINT_SELECT_SQL =
            "SELECT `last_scan_timestamp`, `pending_window_start`, `pending_window_end`,"
                    + " `cursor_query_time`, `cursor_time`, `cursor_query_id`,"
                    + " `failed_attempts`, `retry_queue`, `cursor_tail` FROM " + CHECKPOINT_TABLE
                    + " WHERE `id` = " + CHECKPOINT_ID + " ORDER BY `update_time` DESC LIMIT 1";

    /**
     * One UPSERT statement: the table is UNIQUE-key(id) with merge-on-write, so inserting
     * the row again REPLACES it atomically. The previous delete-then-insert pair was two
     * separately committed statements: a crash / leadership loss / timeout / failed
     * INSERT after the DELETE left NO row for the next leader, which then derived a fresh
     * window and permanently skipped the deleted pending window's unconsumed tail.
     *
     * The target columns are listed EXPLICITLY. The VALUES order below follows
     * {@link org.apache.doris.catalog.InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA}, but
     * the PHYSICAL order of an upgraded table can differ: the upgrade of a pre-existing
     * table APPENDS the columns it adds ({@code InternalSchemaInitializer#
     * upgradeSpmCaptureCheckpointSchema}), which used to place cursor_tail after
     * update_time. A positional INSERT then shifts every value behind the first
     * out-of-position column - the tail JSON was written into failed_attempts, the retry
     * JSON into update_time and NOW() into cursor_tail - and the checkpoint write failed
     * / persisted garbage. Address the columns by NAME instead: the write must stay
     * correct on every physical layout, exactly like the (by-name) CHECKPOINT_SELECT_SQL
     * read.
     */
    private static final String CHECKPOINT_INSERT_SQL =
            "INSERT INTO " + CHECKPOINT_TABLE
                    + " (`id`, `last_scan_timestamp`, `pending_window_start`, `pending_window_end`,"
                    + " `cursor_query_time`, `cursor_time`, `cursor_query_id`, `cursor_tail`,"
                    + " `failed_attempts`, `retry_queue`, `update_time`)"
                    + " VALUES (" + CHECKPOINT_ID + ", ${lastScan}, ${pendingStart}, ${pendingEnd},"
                    + " ${cursorQueryTime}, '${cursorTime}', '${cursorQueryId}', '${cursorTail}',"
                    + " '${failedAttempts}', '${retryQueue}', NOW())";

    private AuditLogScanner scanner = new AuditLogScanner();

    /** Capture filter, refreshed from the global session variables each cycle. */
    private PlanCaptureFilter filter;

    /** Last scan window start (epoch millis); 0 means "first run, scan one interval". */
    private long lastScanTimestamp = 0;

    /**
     * Pending scan window of a TRUNCATED cycle: the (start, end) pair the resume cursor
     * below belongs to. While set, every cycle keeps scanning the SAME window - the end
     * must stay fixed until the window is fully consumed, because the next
     * interval-derived window would start around this window's end and leave every row the
     * cursor has not reached yet permanently out of scope.
     */
    private long pendingWindowStart = 0;
    private long pendingWindowEnd = 0;

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
     * Pre-page checkpoint state of the page a queued retry was FIRST seen on: the durable
     * checkpoint must never move past an entry that the persisted JSON drops
     * ({@link #MAX_PERSISTED_RETRIES}) - keyset pagination has already moved beyond its
     * audit row, so only a cursor BEFORE that row can reach it after a restart / handoff.
     * First-wins (pages only move forward) and evicted together with the queue.
     */
    private final Map<String, RetryAnchor> failedCaptureAnchors = new LinkedHashMap<>();

    /** One pre-page checkpoint state (see {@link #failedCaptureAnchors}). */
    private static final class RetryAnchor {
        final long lastScanTimestamp;
        final long windowStart;
        final long windowEnd;
        final long cursorQueryTime;
        final String cursorTime;
        final String cursorQueryId;
        final String cursorTail;

        RetryAnchor(long lastScanTimestamp, long windowStart, long windowEnd,
                long cursorQueryTime, String cursorTime, String cursorQueryId,
                String cursorTail) {
            this.lastScanTimestamp = lastScanTimestamp;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.cursorQueryTime = cursorQueryTime;
            this.cursorTime = cursorTime;
            this.cursorQueryId = cursorQueryId;
            this.cursorTail = cursorTail;
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
     * truncated by {@link #MAX_PERSISTED_RETRIES} (see persistCheckpoint): the durable
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
     * Checkpoint read / write seams. Production talks to the internal table through
     * StatisticsUtil; tests replace them to simulate a failing first read and to observe
     * the exact statements a persist issues.
     */
    private Supplier<List<ResultRow>> checkpointReader = () -> StatisticsUtil.executeQuery(
            CHECKPOINT_SELECT_SQL, Collections.emptyMap(), CHECKPOINT_IO_TIMEOUT_SECONDS);

    /** One checkpoint write statement. */
    @VisibleForTesting
    public interface CheckpointWriter {
        void write(String sql, Map<String, String> params) throws Exception;
    }

    private CheckpointWriter checkpointWriter = (sql, params) -> StatisticsUtil.execUpdate(
            sql, params, CHECKPOINT_IO_TIMEOUT_SECONDS);

    /**
     * Whether a scripted checkpoint read / write seam is installed (tests only). The
     * leadership fence of {@link #persistCheckpoint} guards LIVE writes: a scripted store
     * stands in for the internal table, exactly like the simulator stores in
     * BaselineManager.assertLeaderForWrite.
     */
    private boolean checkpointSeamsForTest = false;

    /** Whether the cloud-mode warning was already logged (the gate fires every cycle). */
    private boolean cloudModeWarned = false;

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
                pageStartCursorQueryId, pageStartCursorTail);
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
            // refresh the filter so SET GLOBAL changes take effect this cycle
            this.filter = newFilter;

            // A restarted / newly promoted leader must NOT start from a fresh
            // interval-derived window: a truncated window from the previous leader is
            // checkpointed here, and skipping it would permanently exclude its unconsumed
            // tail (the overlap only reaches rows younger than the NEW watermark).
            // A FAILED read returns false and the cycle aborts BEFORE deriving or
            // persisting anything: writing a freshly derived window while the previous
            // leader's unconsumed tail is still unreadable would overwrite its only
            // record (the write path shares the same internal table the read failed on).
            if (!loadCheckpointIfNeeded()) {
                LOG.warn("Plan capture cycle skipped: durable checkpoint not confirmed");
                return;
            }

            long currentTime = System.currentTimeMillis();
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
                    scanWindowOverlapMs(GlobalVariable.auditPluginMaxBatchInternalSec));
            long scanStart = window[0];
            long scanEnd = window[1];
            if (scanStart >= scanEnd) {
                return;
            }

            // Snapshot the pre-page state: when this page ends up with more retries than
            // the durable checkpoint can carry, persistCheckpoint falls back to THIS
            // state so the next leader re-scans the page instead of stepping over the
            // omitted retries.
            pageStartLastScanTimestamp = lastScanTimestamp;
            pageStartWindowStart = scanStart;
            pageStartWindowEnd = scanEnd;
            pageStartCursorQueryTime = cursorQueryTime;
            pageStartCursorTime = cursorTime;
            pageStartCursorQueryId = cursorQueryId;
            pageStartCursorTail = cursorTail;

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
                if (!persistCheckpointAndConfirm()) {
                    LOG.warn("Plan capture cycle skipped: the initial checkpoint row could not"
                            + " be confirmed VISIBLE (a reservation nothing can read cannot"
                            + " protect the window)");
                    return;
                }
            }
            AuditLogScanner.ScanBatch batch = scanner.scan(scanStart, scanEnd,
                    batchSize, cursorQueryTime, cursorTime, cursorQueryId, cursorTail);
            Set<String> scannedQueryIds = new HashSet<>();
            for (CapturedQuery candidate : batch.getCandidates()) {
                scannedQueryIds.add(retryKeyOf(candidate));
                handleCandidate(candidate);
            }
            // Rows whose capture failed stay queued: keyset pagination moved the cursor
            // past their raw rows and the five-minute overlap only re-reads recent ones,
            // so without this replay attempts 2..N would be unreachable for older
            // failures. An id that ALSO appeared in this page was already retried above.
            replayQueuedFailures(scannedQueryIds);
            if (batch.isWindowExhausted()) {
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
            } else {
                // The batch limit truncated the window: KEEP the window BOUNDS and remember
                // the full total-order cursor of the last consumed row, so the next cycle
                // resumes inside the same window. Advancing to the window end here would
                // permanently skip every eligible row beyond the LIMIT; letting the next
                // cycle derive a new interval window would skip everything the cursor has
                // not reached yet as well. The cursor TAIL is what keeps rows sharing
                // (time, query_time, query_id) - e.g. a whole page of NULL query ids -
                // from looping or being skipped (see AuditLogScanner#ORDER_BY).
                pendingWindowStart = scanStart;
                pendingWindowEnd = scanEnd;
                cursorQueryTime = batch.getCursorQueryTime();
                cursorTime = batch.getCursorTime();
                cursorQueryId = batch.getCursorQueryId();
                cursorTail = batch.getCursorTail();
            }
            // Make the progress durable for the NEXT process (leader handoff / restart).
            persistCheckpoint();

            LOG.info("PlanCapture cycle finished: captured={}, dup={}, singleTable={}, filtered={}, fail={}",
                    successCount.get(), skipDuplicateCount.get(), skipSingleTableCount.get(),
                    skipFilterCount.get(), failCount.get());
        } catch (Exception e) {
            LOG.warn("Plan capture cycle failed", e);
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
        String retryKey = retryKeyOf(candidate);
        if (processedQueryIds.containsKey(retryKey)) {
            return; // already handled in an earlier overlapping window
        }
        boolean terminal = processCandidate(candidate);
        if (terminal) {
            failedCaptureAttempts.remove(retryKey);
            failedCaptureQueue.remove(retryKey);
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
            failedCaptureQueue.remove(retryKey);
            failedCaptureAnchors.remove(retryKey);
            markQueryIdProcessed(retryKey);
        } else {
            LOG.info("Plan capture failed for query key {} (attempt {}/{}), queued for retry",
                    retryKey, attempts, MAX_CAPTURE_ATTEMPTS);
            failedCaptureQueue.put(retryKey, candidate);
            // first-wins: the entry must stay reachable from the page it was FIRST
            // queued on even after later pages advance the scan cursor past its row
            failedCaptureAnchors.putIfAbsent(retryKey, currentPageAnchor());
            // NO queue / attempt eviction: dropping the oldest failures before their
            // bounded retries are spent made them unreachable while the leader kept
            // running - their audit rows are behind the live cursor (and may be older
            // than the overlap window), so nothing would ever retry them even without
            // handoff / checkpoint truncation. Retention is bounded by construction:
            // every entry leaves after MAX_CAPTURE_ATTEMPTS attempts or on a terminal
            // result, and one cycle can add at most one page, so both maps never exceed
            // ~MAX_CAPTURE_ATTEMPTS pages and their entries are removed together.
        }
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
     * @param scannedQueryIds the tracking keys ({@link #retryKeyOf}) this cycle's page
     *                        already processed
     */
    @VisibleForTesting
    void replayQueuedFailures(Set<String> scannedQueryIds) {
        if (failedCaptureQueue.isEmpty()) {
            return;
        }
        for (Map.Entry<String, CapturedQuery> entry
                : new ArrayList<>(failedCaptureQueue.entrySet())) {
            String retryKey = entry.getKey();
            if (scannedQueryIds.contains(retryKey)) {
                continue; // already retried by this cycle's page
            }
            // NO remove-before-retry: LinkedHashMap#put on an EXISTING key keeps its
            // original position, while remove+re-add moved the retried entry BEHIND
            // entries queued by newer pages. persistCheckpoint assumes the FIRST queue
            // entry carries the EARLIEST anchor, so reordering made a later page's
            // pre-page cursor get persisted while encodeRetryQueue dropped the older
            // entries that anchor belonged to - unrecoverable on handoff.
            handleCandidate(entry.getValue());
            if (processedQueryIds.containsKey(retryKey)) {
                // consumed elsewhere (e.g. by the page): never replay it again
                failedCaptureQueue.remove(retryKey);
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

    /** Evicts the {@code count} oldest entries of an insertion-ordered map. */
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
        try {
            // Level 3/5 filter: multi-table + table-name regex (pure logic). The
            // extraction PARSES the statement, so it must run under the audit row's
            // ORIGINATING parser mode just like the later build - the daemon thread's
            // global mode can differ (e.g. NO_BACKSLASH_ESCAPES globally while the
            // audited session used the default), and a parse failure here silently
            // drops the row as terminal while the cursor advances past it.
            List<String> tables = SqlModeHelper.withSqlMode(candidate.getSqlMode(),
                    () -> PlanCaptureFilter.extractTableNames(candidate.getStmt()));
            if (!filter.shouldCapture(candidate.toAuditEvent(), tables)) {
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
        // a row was READ: this process now knows a durable record exists, so the initial
        // reservation in runCaptureCycle never overwrites / takes over its role
        durableCheckpointObserved = true;
        // Restored retries take the RESTORED cursor as their anchor. That is now always a
        // position BEFORE every persisted retry row: persistCheckpoint rewinds to the
        // oldest queued entry's PRE-PAGE anchor whenever the queue is non-empty (not only
        // when the JSON truncates), so a truncated 65th entry can still re-scan the row
        // whose retry the JSON dropped.
        RetryAnchor restoredAnchor = new RetryAnchor(lastScanTimestamp, pendingWindowStart,
                pendingWindowEnd, cursorQueryTime, cursorTime, cursorQueryId, cursorTail);
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
        if (durableAnchor != null) {
            durableLastScan = durableAnchor.lastScanTimestamp;
            durablePendingStart = durableAnchor.windowStart;
            durablePendingEnd = durableAnchor.windowEnd;
            durableCursorQueryTime = durableAnchor.cursorQueryTime;
            durableCursorTime = durableAnchor.cursorTime;
            durableCursorQueryId = durableAnchor.cursorQueryId;
            durableCursorTail = durableAnchor.cursorTail;
        } else if (retriesTruncated || !failedCaptureQueue.isEmpty()) {
            durableLastScan = pageStartLastScanTimestamp;
            durablePendingStart = pageStartWindowStart;
            durablePendingEnd = pageStartWindowEnd;
            durableCursorQueryTime = pageStartCursorQueryTime;
            durableCursorTime = pageStartCursorTime;
            durableCursorQueryId = pageStartCursorQueryId;
            durableCursorTail = pageStartCursorTail;
        } else {
            durableLastScan = lastScanTimestamp;
            durablePendingStart = pendingWindowStart;
            durablePendingEnd = pendingWindowEnd;
            durableCursorQueryTime = cursorQueryTime;
            durableCursorTime = cursorTime;
            durableCursorQueryId = cursorQueryId;
            durableCursorTail = cursorTail;
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
        try {
            // Single UPSERT: the new row is durable BEFORE the old one stops being read
            // (the table is UNIQUE-key(id) + merge-on-write), so no crash / timeout can
            // leave the shared store without a checkpoint row.
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
        } catch (Exception e) {
            LOG.warn("SPM capture checkpoint write failed (will retry next cycle): {}",
                    e.getMessage());
            return false;
        }
        durableCheckpointObserved = true;
        return true;
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
     * skipped and {@code durableCheckpointObserved} stays false, so the next cycle
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
        if (!persistCheckpoint()) {
            return false;
        }
        for (int attempt = 0; attempt < CHECKPOINT_VISIBILITY_ATTEMPTS; attempt++) {
            try {
                List<ResultRow> rows = checkpointReader.get();
                if (rows != null && !rows.isEmpty()
                        && isOurReservationRow(rows.get(0), reservationStart, reservationEnd)) {
                    return true;
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

    /** Decodes {@link #encodeFailedAttempts}; blank / broken input decodes to empty. */
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

    /** Decodes {@link #encodeRetryQueue}; blank / broken input decodes to empty. */
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

    /** The most recent {@code limit} entries of an insertion-ordered map. */
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
     * ({@code audit_plugin_max_batch_interval_sec}) PLUS the delay before an event even
     * reaches the loader: WorkloadRuntimeStatusMgr holds a finished query's audit event
     * until {@code query_audit_log_timeout_ms} (or, while external DML statistics are
     * still awaited, up to {@code be_report_query_statistics_timeout_ms}) has passed, and
     * the loader then only polls its queue every {@link AuditLoader#QUEUE_POLL_INTERVAL_MILLIS}.
     *
     * <p>A row is reachable ONLY while its event time is still inside a window's range or
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
     * @return the overlap in milliseconds (never less than {@link #SCAN_WINDOW_OVERLAP_MS})
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
     * Resolves the (start, end) window the next capture cycle scans.
     *
     * A truncated cycle leaves {@code pendingStart/pendingEnd} set: the SAME window is
     * scanned again (from the stored cursor) until it is exhausted, because a newly
     * derived interval window would start around the pending window's end and leave every
     * row the cursor has not reached yet permanently out of scope. Without a pending
     * window the bounds are derived from the watermark (with the late-arrival overlap).
     *
     * @return [windowStart, windowEnd]
     */
    static long[] resolveScanWindow(long lastScanTimestamp, long pendingStart, long pendingEnd,
            long currentTime, long intervalMs, long overlapMs) {
        if (pendingEnd > 0) {
            return new long[] {pendingStart, pendingEnd};
        }
        long start = (lastScanTimestamp == 0)
                ? currentTime - intervalMs
                : Math.max(0L, lastScanTimestamp - overlapMs);
        return new long[] {start, currentTime};
    }

    private void clearPendingWindow() {
        pendingWindowStart = 0;
        pendingWindowEnd = 0;
    }

    /**
     * For tests: resets the counters, the scan window and the resume cursor.
     */
    public void resetForTest() {
        clearProgressState();
        // restore the production read / write seams (tests replace them)
        checkpointReader = () -> StatisticsUtil.executeQuery(
                CHECKPOINT_SELECT_SQL, Collections.emptyMap(), CHECKPOINT_IO_TIMEOUT_SECONDS);
        checkpointWriter = (sql, params) -> StatisticsUtil.execUpdate(
                sql, params, CHECKPOINT_IO_TIMEOUT_SECONDS);
        checkpointSeamsForTest = false;
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
        processedQueryIds.clear();
        failedCaptureAttempts.clear();
        failedCaptureQueue.clear();
        failedCaptureAnchors.clear();
        checkpointLoaded = false;
        durableCheckpointObserved = false;
    }

    /**
     * MASTER PROMOTION hook (called from Env.transferToMaster): the in-memory progress may
     * be STALE - this FE ran the daemon under an earlier mastership, or lost a cycle after
     * a demotion - while the interim master advanced (or rewound) the durable checkpoint.
     * Continuing from the stale cursor / queue would either skip rows the interim master
     * had not consumed yet, or clobber its retry queue on the next persist (a queued row
     * behind the revived cursor is never retried). Drop the local progress and force the
     * next cycle to RELOAD the durable checkpoint, exactly like a freshly started process
     * ({@link #loadCheckpointIfNeeded}).
     *
     * <p>The initial-reservation confirmation of a cycle does not cover this: it protects
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
     * cursorQueryTime, cursorTime, cursorQueryId, cursorTail).
     */
    @VisibleForTesting
    public Object[] checkpointFieldsForTest() {
        return new Object[] {lastScanTimestamp, pendingWindowStart, pendingWindowEnd,
                cursorQueryTime, cursorTime, cursorQueryId, cursorTail};
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

    @VisibleForTesting
    public void loadCheckpointForTest() {
        loadCheckpointIfNeeded();
    }

    @VisibleForTesting
    public void persistCheckpointForTest() {
        persistCheckpoint();
    }

    /**
     * For tests: the durable UPSERT statement (see {@link #CHECKPOINT_INSERT_SQL}). The
     * column list must stay one-to-one with
     * {@link org.apache.doris.catalog.InternalSchema#SPM_CAPTURE_CHECKPOINT_SCHEMA} (and
     * must be a column LIST, not a positional VALUES) so an upgraded table with a
     * different PHYSICAL order cannot shift the values.
     */
    @VisibleForTesting
    public static String checkpointInsertSqlForTest() {
        return CHECKPOINT_INSERT_SQL;
    }

    /**
     * For tests: rewrites the counters used by the truncation guard in
     * {@link #persistCheckpoint()} ({@code failedCaptureAttempts} is a map and cannot be
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
                    pageStartCursorTime, pageStartCursorQueryId, pageStartCursorTail));
        }
        this.pageStartLastScanTimestamp = lastScanTimestamp - 1;
        this.pageStartWindowStart = windowStart;
        this.pageStartWindowEnd = windowEnd;
        this.pageStartCursorQueryTime = pageStartCursorQueryTime;
        this.pageStartCursorTime = pageStartCursorTime;
        this.pageStartCursorQueryId = pageStartCursorQueryId;
        this.pageStartCursorTail = pageStartCursorTail;
        this.pendingWindowStart = windowStart;
        this.pendingWindowEnd = windowEnd;
        this.lastScanTimestamp = lastScanTimestamp;
        this.cursorQueryTime = cursorQueryTime;
        this.cursorTime = cursorTime;
        this.cursorQueryId = cursorQueryId;
        this.cursorTail = cursorTail;
    }
}
