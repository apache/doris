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
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.nereids.spm.BaselinePlan;
import org.apache.doris.nereids.spm.BaselineSource;
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.SessionVariable;
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
                    + " `failed_attempts`, `retry_queue` FROM " + CHECKPOINT_TABLE
                    + " WHERE `id` = " + CHECKPOINT_ID + " ORDER BY `update_time` DESC LIMIT 1";

    /**
     * One UPSERT statement: the table is UNIQUE-key(id) with merge-on-write, so inserting
     * the row again REPLACES it atomically. The previous delete-then-insert pair was two
     * separately committed statements: a crash / leadership loss / timeout / failed
     * INSERT after the DELETE left NO row for the next leader, which then derived a fresh
     * window and permanently skipped the deleted pending window's unconsumed tail.
     */
    private static final String CHECKPOINT_INSERT_SQL =
            "INSERT INTO " + CHECKPOINT_TABLE
                    + " VALUES (" + CHECKPOINT_ID + ", ${lastScan}, ${pendingStart}, ${pendingEnd},"
                    + " ${cursorQueryTime}, '${cursorTime}', '${cursorQueryId}',"
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
     * Resume cursor of a TRUNCATED scan window: (query_time, time, query_id) of the last
     * consumed row. CURSOR_ABSENT while no partial window is pending - a short batch
     * advances the watermark instead. Zero and NULL query_time are VALID cursors (see
     * AuditLogScanner.CURSOR_QUERY_TIME_NULL).
     */
    private long cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
    private String cursorTime = "";
    private String cursorQueryId = "";

    /** Whether the durable checkpoint was already consulted in this process. */
    private boolean checkpointLoaded = false;

    /**
     * Checkpoint read / write seams. Production talks to the internal table through
     * StatisticsUtil; tests replace them to simulate a failing first read and to observe
     * the exact statements a persist issues.
     */
    private Supplier<List<ResultRow>> checkpointReader =
            () -> StatisticsUtil.executeQuery(CHECKPOINT_SELECT_SQL, Collections.emptyMap());

    /** One checkpoint write statement. */
    @VisibleForTesting
    public interface CheckpointWriter {
        void write(String sql, Map<String, String> params) throws Exception;
    }

    private CheckpointWriter checkpointWriter = StatisticsUtil::execUpdate;

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
        try {
            // refresh the filter so SET GLOBAL changes take effect this cycle
            this.filter = newFilter;

            // A restarted / newly promoted leader must NOT start from a fresh
            // interval-derived window: a truncated window from the previous leader is
            // checkpointed here, and skipping it would permanently exclude its unconsumed
            // tail (the overlap only reaches rows younger than the NEW watermark).
            loadCheckpointIfNeeded();

            long currentTime = System.currentTimeMillis();
            // a non-positive interval / batch size can never be written through SQL SET
            // (see SessionVariable), but clamp defensively: an interval of 0 would make
            // every window empty and a batch size of 0 would return LIMIT 0, mark the
            // window exhausted and advance the watermark over every eligible row
            long intervalMs = Math.max(1L, global.getPlanCaptureIntervalSeconds()) * 1000L;
            int batchSize = Math.max(1, global.getPlanCaptureMaxBatchSize());
            // overlap the window so audit rows loaded late (whose event time is older
            // than the last watermark) are still scanned; duplicates are filtered by
            // query id below
            long[] window = resolveScanWindow(lastScanTimestamp, pendingWindowStart, pendingWindowEnd,
                    currentTime, intervalMs, SCAN_WINDOW_OVERLAP_MS);
            long scanStart = window[0];
            long scanEnd = window[1];
            if (scanStart >= scanEnd) {
                return;
            }

            AuditLogScanner.ScanBatch batch = scanner.scan(scanStart, scanEnd,
                    batchSize, cursorQueryTime, cursorTime, cursorQueryId);
            Set<String> scannedQueryIds = new HashSet<>();
            for (CapturedQuery candidate : batch.getCandidates()) {
                scannedQueryIds.add(candidate.getQueryId());
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
            } else {
                // The batch limit truncated the window: KEEP the window BOUNDS and remember
                // the (query_time, time, query_id) cursor of the last consumed row, so the
                // next cycle resumes inside the same window. Advancing to the window end
                // here would permanently skip every eligible row beyond the LIMIT; letting
                // the next cycle derive a new interval window would skip everything the
                // cursor has not reached yet as well.
                pendingWindowStart = scanStart;
                pendingWindowEnd = scanEnd;
                cursorQueryTime = batch.getCursorQueryTime();
                cursorTime = batch.getCursorTime();
                cursorQueryId = batch.getCursorQueryId();
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
        String queryId = candidate.getQueryId();
        boolean trackId = queryId != null && !queryId.isEmpty() && !"NaN".equals(queryId);
        if (trackId && processedQueryIds.containsKey(queryId)) {
            return; // already handled in an earlier overlapping window
        }
        boolean terminal = processCandidate(candidate);
        if (!trackId) {
            return;
        }
        if (terminal) {
            failedCaptureAttempts.remove(queryId);
            failedCaptureQueue.remove(queryId);
            markQueryIdProcessed(queryId);
            return;
        }
        int attempts = failedCaptureAttempts.merge(queryId, 1, Integer::sum);
        if (attempts >= MAX_CAPTURE_ATTEMPTS) {
            // bounded retry: a permanently broken row must not burn every cycle
            LOG.warn("Plan capture gave up on query id {} after {} failed attempts",
                    queryId, attempts);
            failedCaptureAttempts.remove(queryId);
            failedCaptureQueue.remove(queryId);
            markQueryIdProcessed(queryId);
        } else {
            LOG.info("Plan capture failed for query id {} (attempt {}/{}), queued for retry",
                    queryId, attempts, MAX_CAPTURE_ATTEMPTS);
            failedCaptureQueue.put(queryId, candidate);
            if (failedCaptureAttempts.size() > MAX_TRACKED_QUERY_IDS) {
                evictOldest(failedCaptureAttempts, MAX_TRACKED_QUERY_IDS / 10);
            }
            if (failedCaptureQueue.size() > MAX_TRACKED_QUERY_IDS) {
                evictOldest(failedCaptureQueue, MAX_TRACKED_QUERY_IDS / 10);
            }
        }
    }

    /**
     * Replays the queued transient failures, one attempt each per cycle. A queued id that
     * ALSO appeared in this cycle's page was already retried by the page loop (and stays
     * queued when it failed again); every other queued id is retried here, so a failure
     * stays reachable regardless of where the keyset cursor has moved.
     *
     * @param scannedQueryIds the query ids this cycle's page already processed
     */
    @VisibleForTesting
    void replayQueuedFailures(Set<String> scannedQueryIds) {
        if (failedCaptureQueue.isEmpty()) {
            return;
        }
        for (Map.Entry<String, CapturedQuery> entry
                : new ArrayList<>(failedCaptureQueue.entrySet())) {
            String queryId = entry.getKey();
            if (scannedQueryIds.contains(queryId)) {
                continue; // already retried by this cycle's page
            }
            failedCaptureQueue.remove(queryId);
            handleCandidate(entry.getValue());
            if (processedQueryIds.containsKey(queryId)) {
                // consumed elsewhere (e.g. by the page): never replay it again
                failedCaptureQueue.remove(queryId);
                failedCaptureAttempts.remove(queryId);
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
            // Level 3/5 filter: multi-table + table-name regex (pure logic)
            List<String> tables = PlanCaptureFilter.extractTableNames(candidate.getStmt());
            if (!filter.shouldCapture(candidate.toAuditEvent(), tables)) {
                if (tables.size() < 2) {
                    skipSingleTableCount.incrementAndGet();
                } else {
                    skipFilterCount.incrementAndGet();
                }
                return true;
            }
            // Level 4 filter: tables must still exist in the CAPTURED namespace (external
            // tables resolve through their own catalog, not InternalCatalog)
            if (!filter.allTablesExist(tables, candidate.getCatalog(), candidate.getDb())) {
                skipFilterCount.incrementAndGet();
                return true;
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
                baseline = new SPMPlanner().buildBaselineFromSql(
                        ctx.connectContext, candidate.getStmt(), candidate.getStmt());
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
     */
    private void loadCheckpointIfNeeded() {
        if (checkpointLoaded || !checkpointPersistenceEnabled()) {
            return;
        }
        if (lastScanTimestamp != 0 || pendingWindowEnd > 0
                || cursorQueryTime != AuditLogScanner.CURSOR_ABSENT) {
            checkpointLoaded = true; // progress already exists (e.g. a unit test): never override it
            return;
        }
        try {
            List<ResultRow> rows = checkpointReader.get();
            if (rows == null || rows.isEmpty()) {
                checkpointLoaded = true; // a successful read with no row yet
                return;
            }
            applyCheckpointRow(rows.get(0));
            checkpointLoaded = true; // only a SUCCESSFUL read consumes the checkpoint
            if (lastScanTimestamp != 0 || pendingWindowEnd > 0
                    || cursorQueryTime != AuditLogScanner.CURSOR_ABSENT) {
                LOG.info("SPM capture resumed from the durable checkpoint: lastScan={},"
                                + " pending=[{}, {}), cursorQueryTime={}",
                        lastScanTimestamp, pendingWindowStart, pendingWindowEnd, cursorQueryTime);
            }
        } catch (Exception e) {
            // Keep checkpointLoaded FALSE: the internal-schema initializer is asynchronous
            // (and a BE / tablet may not be ready yet), so this read can fail before the
            // table exists. Marking the checkpoint consumed on failure made the process
            // start from the default window and later OVERWRITE the only record of the
            // previous leader's unconsumed tail. The next cycle retries.
            LOG.warn("SPM capture checkpoint read failed (will retry next cycle): {}",
                    e.getMessage());
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
        failedCaptureAttempts.clear();
        failedCaptureAttempts.putAll(decodeFailedAttempts(row.get(6)));
        failedCaptureQueue.clear();
        failedCaptureQueue.putAll(decodeRetryQueue(row.get(7)));
    }

    /**
     * Persists the current capture progress (window bounds + total-order cursor + retry
     * state) for the NEXT process. A failed write is logged and skipped - the checkpoint is
     * a best-effort resume aid and must never break the cycle.
     */
    private void persistCheckpoint() {
        if (!checkpointPersistenceEnabled()) {
            return;
        }
        Map<String, String> params = new HashMap<>();
        params.put("lastScan", String.valueOf(lastScanTimestamp));
        params.put("pendingStart", String.valueOf(pendingWindowStart));
        params.put("pendingEnd", String.valueOf(pendingWindowEnd));
        params.put("cursorQueryTime", String.valueOf(cursorQueryTime));
        params.put("cursorTime", StatisticsUtil.escapeSQL(cursorTime == null ? "" : cursorTime));
        params.put("cursorQueryId",
                StatisticsUtil.escapeSQL(cursorQueryId == null ? "" : cursorQueryId));
        params.put("failedAttempts",
                StatisticsUtil.escapeSQL(encodeFailedAttempts(failedCaptureAttempts)));
        params.put("retryQueue",
                StatisticsUtil.escapeSQL(encodeRetryQueue(failedCaptureQueue)));
        try {
            // Single UPSERT: the new row is durable BEFORE the old one stops being read
            // (the table is UNIQUE-key(id) + merge-on-write), so no crash / timeout can
            // leave the shared store without a checkpoint row.
            checkpointWriter.write(CHECKPOINT_INSERT_SQL, params);
        } catch (Exception e) {
            LOG.warn("SPM capture checkpoint write failed (will retry next cycle): {}",
                    e.getMessage());
        }
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
                        queryId == null ? "" : queryId);
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
        lastScanTimestamp = 0;
        clearPendingWindow();
        cursorQueryTime = AuditLogScanner.CURSOR_ABSENT;
        cursorTime = "";
        cursorQueryId = "";
        processedQueryIds.clear();
        failedCaptureAttempts.clear();
        failedCaptureQueue.clear();
        checkpointLoaded = false;
        // restore the production read / write seams (tests replace them)
        checkpointReader = () -> StatisticsUtil.executeQuery(
                CHECKPOINT_SELECT_SQL, Collections.emptyMap());
        checkpointWriter = StatisticsUtil::execUpdate;
        successCount.set(0);
        skipDuplicateCount.set(0);
        skipSingleTableCount.set(0);
        skipFilterCount.set(0);
        failCount.set(0);
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
     * For tests: the live checkpoint fields
     * (lastScan, pendingStart, pendingEnd, cursorQueryTime, cursorTime, cursorQueryId).
     */
    @VisibleForTesting
    public Object[] checkpointFieldsForTest() {
        return new Object[] {lastScanTimestamp, pendingWindowStart, pendingWindowEnd,
                cursorQueryTime, cursorTime, cursorQueryId};
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
    public void setCheckpointReaderForTest(Supplier<List<ResultRow>> reader) {
        this.checkpointReader = reader;
    }

    @VisibleForTesting
    public void setCheckpointWriterForTest(CheckpointWriter writer) {
        this.checkpointWriter = writer;
    }

    @VisibleForTesting
    public void loadCheckpointForTest() {
        loadCheckpointIfNeeded();
    }

    @VisibleForTesting
    public void persistCheckpointForTest() {
        persistCheckpoint();
    }
}
