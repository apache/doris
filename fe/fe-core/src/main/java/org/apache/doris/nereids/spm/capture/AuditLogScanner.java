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

import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * AuditLogScanner - audit log query wrapper (Phase 2, design doc 7.2.3).
 *
 * Reads the __internal_schema.audit_log internal table through the internal query
 * mechanism and returns the high-value query candidates for SPM auto capture.
 *
 * Within a capture cycle the results are deduplicated by (catalog, db, sql_digest): the
 * record with the largest query_time wins, so the same query SHAPE is only processed once
 * per cycle - but only within one namespace. Identical unqualified SQL executed in two
 * databases is a DIFFERENT query for SPM (its eventual match key is namespace-qualified),
 * so the database / catalog must take part in the dedup key.
 *
 * Pagination: the batch LIMIT is applied with a stable total-order cursor (see
 * ORDER_BY); the caller resumes from the returned cursor until a batch comes
 * back shorter than the limit (window exhausted); advancing the window past a truncated
 * batch would permanently skip every eligible row beyond the LIMIT.
 */
public class AuditLogScanner {

    /**
     * Cursor sentinel: no resume cursor is pending. A valid audit query_time is
     * non-negative, so the sentinel lies outside the valid domain (a zero query_time is
     * a perfectly valid cursor and must not be mistaken for "no cursor").
     */
    public static final long CURSOR_ABSENT = Long.MIN_VALUE;

    /**
     * Cursor sentinel: the cursor row's query_time is NULL. query_time is nullable and
     * eligibility also accepts large scan_rows alone, so a full page can legitimately end
     * with a NULL query_time; NULL must stay distinguishable from a zero query_time so
     * the resume predicate can compare it three-valued (IS NULL).
     */
    public static final long CURSOR_QUERY_TIME_NULL = Long.MIN_VALUE + 1;

    /**
     * Statement timeout (seconds) of the synchronous audit read. The default
     * StatisticsUtil overload assigns the ANALYZE timeout (43,200 seconds), so a stalled
     * internal-table read could hold the single capture cycle for half a day and delay
     * every later capture / retry. The read is latency-sensitive: fail fast and let the
     * next cycle retry.
     */
    static final int AUDIT_SCAN_TIMEOUT_SECONDS = 30;

    private static final Logger LOG = LogManager.getLogger(AuditLogScanner.class);

    /**
     * Wall-clock format of the scan bounds. MILLISECOND precision:
     * audit_log.time is DATETIMEV2(3), but a second-precision rendering of the
     * EXCLUSIVE upper bound dropped the fraction - a row published at 07:00:00.500 with
     * a window end rendered as 07:00:00.900 was tested with time < '07:00:00' and
     * excluded although it belongs to the window. If its writer zone then changed (the
     * old zone retired after its checkpoint passed), no later window ever recovered the
     * row.
     */
    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Parses a scan bound: the string-form builders may hand in a second-precision
     * spelling (2026-01-01 11:55:00) while the millisecond rendering is the
     * published form, so the fraction is optional HERE only - rendering
     * always spells it out.
     */
    private static final DateTimeFormatter DATETIME_PARSE_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]");

    /**
     * Lookback floor of the completion-aware scan lower bound (see buildScanSql): a query
     * that started earlier than this before the window cannot be admitted even when its
     * completion reaches into the window - the trade-off that keeps the range-partitioned
     * audit table prunable instead of rescanning every retained partition per page.
     */
    private static final long LATE_COMPLETION_LOOKBACK_MILLIS = java.util.concurrent.TimeUnit.DAYS
            .toMillis(1);

    /** audit_log SELECT columns (order must match rowToCapturedQuery / toBatch). */
    private static final String SELECT_COLUMNS =
            "`stmt`, `query_time`, `scan_rows`, `return_rows`, `sql_digest`, `sql_hash`, `db`, `catalog`,"
                    + " `query_id`, `is_internal`, `time`, `sql_mode`, `client_ip`, md5(`stmt`)";

    /**
     * Canonical name of the row-content hash pseudo column (the last ORDER BY / cursor
     * tie breaker). Auditing rows that agree on EVERY ordered key are content-duplicates
     * (same statement, same client, same metrics), so skipping extra copies of such a
     * content class is safe: capture dedupes by (catalog, db, digest) anyway.
     */
    private static final String STMT_HASH_EXPR = "md5(`stmt`)";

    /**
     * Total order of the scan / cursor. The row-EVENT time is the FIRST key (with
     * query_id / client_ip / metrics / statement hash as durable tie breakers): the
     * audit loader writes rows asynchronously with the ORIGINAL event time, so a row
     * published after page 1 can carry an event time OLDER than the current cursor -
     * under the previous query_time-first order it sorted BEFORE the cursor and every
     * resumed page skipped it forever. With the event time leading, an older-event-time
     * row sorts AFTER the cursor and the resumed pages reach it; a row whose event time
     * is newer than the cursor is picked up by the window overlap (see
     * PlanCaptureManager#scanWindowOverlapMs, which follows the loader's configured
     * batch interval).
     */
    private static final String ORDER_BY =
            " ORDER BY `time` DESC, `query_time` DESC, `query_id` DESC, `client_ip` DESC,"
                    + " `sql_hash` DESC, `scan_rows` DESC, `return_rows` DESC, " + STMT_HASH_EXPR
                    + " DESC, `catalog` DESC, `db` DESC, `sql_mode` DESC ";

    /**
     * Tail of the pagination cursor AFTER (query_time, time, query_id): client_ip,
     * sql_hash, scan_rows, return_rows and the statement hash. The audit table is a
     * DUPLICATE KEY table whose key omits client_ip, and the raw ORDER BY has NO
     * genuinely unique column: without the tail, rows sharing the first three keys made
     * the resume predicate either re-select the whole (NULL query_id) group forever or
     * skip the remaining duplicates after the first LIMIT. The tail is persisted with
     * the checkpoint (see PlanCaptureManager#cursorTail) so a restarted / handed-off
     * leader resumes exactly after the last consumed row.
     */
    public static final class CursorTail {
        private final String clientIp;
        private final String sqlHash;
        private final String scanRows;
        private final String returnRows;
        private final String stmtHash;
        /** The row's namespace + parser mode: two NaN-id rows otherwise identical in
         * client / metrics can still be SEPARATE capture identities (toBatch dedupes by
         * catalog+db+sql_mode+identity), so the ordered cursor must reach them too -
         * without these keys the strict after-cursor chain excluded the second row on
         * every later page. */
        private final String catalog;
        private final String db;
        private final String sqlMode;
        /** Whether catalog / db / sql_mode are PART of this tail. A legacy
         * five-element tail (written by a pre-upgrade leader) has no namespace keys:
         * treating its absent keys as NULL values would extend the resume chain with
         * three NULL comparisons and then terminate it - skipping the group's
         * remaining rows, where the old chain still reached them. */
        private final boolean hasNamespaceKeys;
        /**
         * The session time zone the cursor's timestamp strings were RENDERED in (the
         * audit writer's zone, see auditWriteZone()); null in a tail written
         * before the element existed. A PENDING window keeps scanning in this zone so a
         * global time_zone change never mixes two renderings inside one window (the
         * bounds are epoch millis re-formatted every cycle, while the cursor is the
         * persisted string).
         */
        private final String zoneId;

        CursorTail(String clientIp, String sqlHash, String scanRows, String returnRows,
                String stmtHash) {
            this(clientIp, sqlHash, scanRows, returnRows, stmtHash, null, null, null, false, null);
        }

        CursorTail(String clientIp, String sqlHash, String scanRows, String returnRows,
                String stmtHash, String catalog, String db, String sqlMode) {
            this(clientIp, sqlHash, scanRows, returnRows, stmtHash, catalog, db, sqlMode, true,
                    null);
        }

        CursorTail(String clientIp, String sqlHash, String scanRows, String returnRows,
                String stmtHash, String catalog, String db, String sqlMode, String zoneId) {
            this(clientIp, sqlHash, scanRows, returnRows, stmtHash, catalog, db, sqlMode, true,
                    zoneId);
        }

        private CursorTail(String clientIp, String sqlHash, String scanRows, String returnRows,
                String stmtHash, String catalog, String db, String sqlMode,
                boolean hasNamespaceKeys, String zoneId) {
            this.clientIp = clientIp;
            this.sqlHash = sqlHash;
            this.scanRows = scanRows;
            this.returnRows = returnRows;
            this.stmtHash = stmtHash;
            this.catalog = catalog;
            this.db = db;
            this.sqlMode = sqlMode;
            this.hasNamespaceKeys = hasNamespaceKeys;
            this.zoneId = zoneId;
        }

        String getClientIp() {
            return clientIp;
        }

        String getSqlHash() {
            return sqlHash;
        }

        String getScanRows() {
            return scanRows;
        }

        String getReturnRows() {
            return returnRows;
        }

        String getStmtHash() {
            return stmtHash;
        }

        String getCatalog() {
            return catalog;
        }

        String getDb() {
            return db;
        }

        String getSqlMode() {
            return sqlMode;
        }

        /** Whether the namespace / mode keys are part of this tail (see the field). */
        boolean hasNamespaceKeys() {
            return hasNamespaceKeys;
        }

        /** The zone the timestamp strings were rendered in (null = not recorded). */
        String getZoneId() {
            return zoneId;
        }
    }

    /** Encodes a cursor tail as a compact JSON list (null-safe; empty text = absent). */
    public static String encodeCursorTail(CursorTail tail) {
        if (tail == null
                || (tail.getClientIp() == null && tail.getSqlHash() == null
                && tail.getScanRows() == null && tail.getReturnRows() == null
                && tail.getStmtHash() == null && tail.getCatalog() == null
                && tail.getDb() == null && tail.getSqlMode() == null)) {
            // no tail information at all (a row without the appended columns - e.g. a
            // pre-column audit row or a fabricated test row): treat it as a PREFIX-only
            // cursor; a JSON array of nulls would otherwise extend the resume chain with
            // all-NULL keys and terminate it immediately.
            return "";
        }
        if (!tail.hasNamespaceKeys()) {
            // a legacy tail round-trips as five elements so the decoder marks it legacy
            // again (the namespace keys were never observed)
            return new Gson().toJson(Arrays.asList(tail.getClientIp(), tail.getSqlHash(),
                    tail.getScanRows(), tail.getReturnRows(), tail.getStmtHash()));
        }
        if (tail.getZoneId() == null) {
            // a tail written before the zone element existed (or by a fixture): keep the
            // eight-element form so it decodes without a zone again
            return new Gson().toJson(Arrays.asList(tail.getClientIp(), tail.getSqlHash(),
                    tail.getScanRows(), tail.getReturnRows(), tail.getStmtHash(),
                    tail.getCatalog(), tail.getDb(), tail.getSqlMode()));
        }
        return new Gson().toJson(Arrays.asList(tail.getClientIp(), tail.getSqlHash(),
                tail.getScanRows(), tail.getReturnRows(), tail.getStmtHash(),
                tail.getCatalog(), tail.getDb(), tail.getSqlMode(), tail.getZoneId()));
    }

    /** Decodes a cursor tail; blank / broken / all-null text decodes to null (legacy cursor). */
    static CursorTail decodeCursorTail(String text) {
        if (text == null || text.trim().isEmpty()) {
            return null;
        }
        try {
            List<String> values = new Gson().fromJson(text,
                    new TypeToken<List<String>>() { }.getType());
            if (values == null || values.size() < 5) {
                return null;
            }
            boolean hasAnyValue = false;
            for (String value : values) {
                if (value != null) {
                    hasAnyValue = true;
                    break;
                }
            }
            if (!hasAnyValue) {
                return null;
            }
            if (values.size() < 8) {
                // five (or partially extended) element tail written before the
                // namespace / mode keys existed: it carries NO information about them,
                // so the resume chain keeps the legacy prefix comparison
                return new CursorTail(values.get(0), values.get(1), values.get(2),
                        values.get(3), values.get(4));
            }
            if (values.size() < 9) {
                // namespace-aware tail without the zone element (pre-zone writer)
                return new CursorTail(values.get(0), values.get(1), values.get(2),
                        values.get(3), values.get(4), values.get(5), values.get(6),
                        values.get(7));
            }
            return new CursorTail(values.get(0), values.get(1), values.get(2),
                    values.get(3), values.get(4), values.get(5), values.get(6),
                    values.get(7), values.get(8));
        } catch (RuntimeException e) {
            return null;
        }
    }

    /** Value of a column that may be missing (pre-column rows); null when out of range. */
    private static String valueAt(ResultRow row, int index) {
        List<String> values = row.getValues();
        return index < values.size() ? values.get(index) : null;
    }

    /**
     * Result of one audit scan: the namespace-deduplicated candidates plus the resume
     * cursor (the full ORDER BY key tuple of the last RAW row read).
     */
    public static class ScanBatch {
        private final List<CapturedQuery> candidates;
        private final boolean windowExhausted;
        private final long cursorQueryTime;
        private final String cursorTime;
        private final String cursorQueryId;
        private final String cursorTail;

        ScanBatch(List<CapturedQuery> candidates, boolean windowExhausted,
                long cursorQueryTime, String cursorTime, String cursorQueryId,
                String cursorTail) {
            this.candidates = candidates;
            this.windowExhausted = windowExhausted;
            this.cursorQueryTime = cursorQueryTime;
            this.cursorTime = cursorTime == null ? "" : cursorTime;
            this.cursorQueryId = cursorQueryId == null ? "" : cursorQueryId;
            this.cursorTail = cursorTail == null ? "" : cursorTail;
        }

        ScanBatch(List<CapturedQuery> candidates, boolean windowExhausted,
                long cursorQueryTime, String cursorTime, String cursorQueryId) {
            this(candidates, windowExhausted, cursorQueryTime, cursorTime, cursorQueryId, "");
        }

        public List<CapturedQuery> getCandidates() {
            return candidates;
        }

        /** Whether the batch returned fewer RAW rows than the limit (whole window read). */
        public boolean isWindowExhausted() {
            return windowExhausted;
        }

        public long getCursorQueryTime() {
            return cursorQueryTime;
        }

        public String getCursorTime() {
            return cursorTime;
        }

        public String getCursorQueryId() {
            return cursorQueryId;
        }

        /** Encoded tail of the cursor (see CursorTail); empty = absent. */
        public String getCursorTail() {
            return cursorTail;
        }
    }

    /**
     * Scans the audit_log table within the given time window (first page).
     *
     * @param startTimeMs  window start (epoch millis, inclusive)
     * @param endTimeMs    window end (epoch millis, exclusive)
     * @param maxBatchSize max number of raw rows to scan (prevents OOM)
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize) {
        return scan(startTimeMs, endTimeMs, maxBatchSize, CURSOR_ABSENT, "", "", "");
    }

    /**
     * Scans the audit_log table within the given time window, resuming after the cursor
     * returned by the previous batch.
     *
     * @param startTimeMs    window start (epoch millis, inclusive)
     * @param endTimeMs      window end (epoch millis, exclusive)
     * @param maxBatchSize   max number of raw rows per batch
     * @param cursorQueryTime query_time of the last consumed row (CURSOR_ABSENT = start
     *                        from the top; CURSOR_QUERY_TIME_NULL = that row's value was
     *                        NULL; any other value - including 0 - is a real cursor)
     * @param cursorTime     event time of the last consumed row; empty = SQL NULL
     * @param cursorQueryId  query_id of the last consumed row; empty = SQL NULL
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
            long cursorQueryTime, String cursorTime, String cursorQueryId) {
        return scan(startTimeMs, endTimeMs, maxBatchSize, cursorQueryTime, cursorTime,
                cursorQueryId, "");
    }

    /**
     * Scans the audit_log table within the given time window, resuming after the FULL
     * cursor tuple (see CursorTail).
     *
     * The thresholds are read from the CURRENT global session variables. Only callers
     * outside the capture cycle (tests, tooling) may use this overload: the cycle owns a
     * pinned snapshot of them and must pass it via
     * scan(long, long, int, PlanCaptureFilter, long, String, String, String), so
     * that the SQL stage and the in-memory
     * PlanCaptureFilter#shouldCapture stage never compare against two different
     * threshold sets.
     *
     * @param startTimeMs    window start (epoch millis, inclusive)
     * @param endTimeMs      window end (epoch millis, exclusive)
     * @param maxBatchSize   max number of raw rows per batch
     * @param cursorQueryTime query_time of the last consumed row (CURSOR_ABSENT = start
     *                        from the top; CURSOR_QUERY_TIME_NULL = that row's value was
     *                        NULL; any other value - including 0 - is a real cursor)
     * @param cursorTime     event time of the last consumed row; empty = SQL NULL
     * @param cursorQueryId  query_id of the last consumed row; empty = SQL NULL
     * @param cursorTail     encoded tail of the last consumed row (empty = legacy cursor
     *                       without a tail: the resume predicate falls back to the
     *                       (time, query_time, query_id) prefix)
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
            long cursorQueryTime, String cursorTime, String cursorQueryId, String cursorTail) {
        // no pattern is used here, so only the thresholds matter: the constructor reads
        // them from the current globals
        return scan(startTimeMs, endTimeMs, maxBatchSize, new PlanCaptureFilter(null, null),
                cursorQueryTime, cursorTime, cursorQueryId, cursorTail);
    }

    /**
     * Scans the audit_log table within the given time window, resuming after the FULL
     * cursor tuple (see CursorTail), with the thresholds of the given filter.
     *
     * Deriving the SQL thresholds from the SAME filter instance that later decides
     * PlanCaptureFilter#shouldCapture is what keeps the two stages consistent: the
     * SQL returns a row exactly when the filter would accept it, so no row the filter
     * rejects is ever consumed (marked processed) and no row the filter accepts is
     * unreachable behind the cursor. Reading the globals here instead made a
     * `SET GLOBAL plan_capture_min_query_time_ms` between the cycle's filter construction
     * and this statement return rows the stale in-memory filter then failed TERMINALLY,
     * and a LOWERED threshold made already-passed rows unreachable below the cursor.
     *
     * @param startTimeMs    window start (epoch millis, inclusive)
     * @param endTimeMs      window end (epoch millis, exclusive)
     * @param maxBatchSize   max number of raw rows per batch
     * @param filter         the threshold snapshot of this window (the caller's filter)
     * @param cursorQueryTime query_time of the last consumed row (CURSOR_ABSENT = start
     *                        from the top; CURSOR_QUERY_TIME_NULL = that row's value was
     *                        NULL; any other value - including 0 - is a real cursor)
     * @param cursorTime     event time of the last consumed row; empty = SQL NULL
     * @param cursorQueryId  query_id of the last consumed row; empty = SQL NULL
     * @param cursorTail     encoded tail of the last consumed row (empty = legacy cursor
     *                       without a tail: the resume predicate falls back to the
     *                       (time, query_time, query_id) prefix)
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
            PlanCaptureFilter filter, long cursorQueryTime, String cursorTime,
            String cursorQueryId, String cursorTail) {
        return scan(startTimeMs, endTimeMs, maxBatchSize, filter, cursorQueryTime,
                cursorTime, cursorQueryId, cursorTail, null);
    }

    /**
     * As the eight-argument overload, with the zone the window must be rendered in when
     * its cursor does not carry one (a window whose FIRST pass runs now).
     *
     * audit_log.time is the audit WRITER's local rendering and the writer follows the
     * global session time_zone, so after SET GLOBAL time_zone the rows published
     * BEFORE the change are stored in the OLD rendering and are invisible to bounds
     * rendered in the new zone - the reviewer's example: a 10:00 UTC row stored as
     * "10:00" is searched as [17:00, 20:00) after the zone becomes +08, the (empty) page
     * looks exhausted and the watermark moves past the row forever. The window is
     * therefore opened in the zone the PREVIOUS scan used while that differs from the
     * global zone: the old rendering's rows are found first, and the following pass (see
     * PlanCaptureManager's exhaustion branch) revisits the SAME window in the new zone
     * for the rows published after the change. Each pass is a single rendering, so the
     * keyset pagination keeps walking one consistent total order.
     *
     * @param firstPassZoneId zone ID of the previous scan pass (empty / null = follow the
     *                        current global time_zone)
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
            PlanCaptureFilter filter, long cursorQueryTime, String cursorTime,
            String cursorQueryId, String cursorTail, String firstPassZoneId) {
        // The bounds are rendered in the zone the AUDIT WRITER used (the global session
        // time_zone, see auditWriteZone) - not the FE host zone - because
        // __internal_schema.audit_log.time stores the writer's rendering. A PENDING
        // window keeps the zone recorded in its cursor: the window's epoch bounds are
        // re-rendered every cycle while the cursor is the persisted string, so a global
        // time_zone change mid-window would otherwise compare two different renderings
        // and skip the whole unconsumed range. Only a NEW window follows a changed
        // global zone.
        ZoneId auditZone = scanZoneFor(cursorTail);
        if (zoneOfTail(cursorTail) == null && firstPassZoneId != null && !firstPassZoneId.isEmpty()) {
            ZoneId firstPassZone = parseZone(firstPassZoneId);
            if (firstPassZone != null && !firstPassZone.equals(auditZone)) {
                LOG.info("SPM audit scan opens the window in zone {} (the global time_zone is"
                        + " now {}): its already published rows were rendered under the"
                        + " previous zone", firstPassZone, auditZone);
                auditZone = firstPassZone;
            }
        }
        // window bounds as MONOTONE wall-clock ranges: a UTC window crossing a DST
        // transition renders as several local ranges (see localTimeRanges), never as one
        // inverted range that matches nothing
        List<String[]> windowRanges = localTimeRanges(startTimeMs, endTimeMs, auditZone);
        // defense in depth: a non-positive batch size can no longer be written through
        // SQL SET (see SessionVariable), but LIMIT 0 here would mark the window
        // exhausted on an empty page and advance the watermark over every eligible row
        int limit = Math.max(1, maxBatchSize);

        long minQueryTimeMs = filter.getMinQueryTimeMs();
        long minScanRows = filter.getMinScanRows();
        String sql = buildScanSql(windowRanges, limit, minQueryTimeMs, minScanRows,
                cursorPredicate(cursorQueryTime, cursorTime, cursorQueryId, cursorTail),
                zoneOffsetSwingSeconds(auditZone), lateCompletionFloor(startTimeMs, auditZone));

        // bounded statement timeout: see AUDIT_SCAN_TIMEOUT_SECONDS (the no-timeout
        // overload would inherit the 12h analyze timeout)
        List<ResultRow> rows = StatisticsUtil.execStatisticQuery(sql, false,
                AUDIT_SCAN_TIMEOUT_SECONDS);
        return toBatch(rows, limit, auditZone, startTimeMs, endTimeMs);
    }

    /**
     * The zone bounds and the resume cursor are rendered in for the given cursor: a
     * PENDING window keeps the zone recorded in its cursor (its epoch bounds are
     * re-rendered every cycle while the cursor is the persisted string - a global
     * time_zone change would otherwise mix two renderings inside one window), and a NEW
     * window follows the current global zone (the audit writer's own zone).
     */
    static ZoneId scanZoneFor(String cursorTail) {
        ZoneId pendingZone = zoneOfTail(cursorTail);
        if (pendingZone == null) {
            return auditWriteZone();
        }
        if (!pendingZone.equals(auditWriteZone())) {
            LOG.info("SPM audit scan continues the pending window in zone {} (the global"
                    + " time_zone is now {}); the next window follows the new zone",
                    pendingZone, auditWriteZone());
        }
        return pendingZone;
    }

    /**
     * The zone the audit WRITER rendered its timestamps in. AuditLoader formats the
     * event time with TimeUtils, which on its own (context-less) worker thread
     * falls back to the GLOBAL session variable time_zone; the scan bounds must use
     * exactly the same zone, otherwise a non-UTC host zone makes every stored row fall
     * outside the windows (or renders window bounds that match nothing).
     */
    static ZoneId auditWriteZone() {
        return TimeUtils.getOrSystemTimeZone(
                VariableMgr.getDefaultSessionVariable().getTimeZone()).toZoneId();
    }

    /** The zone recorded in a cursor tail, or null when absent / unparsable. */
    private static ZoneId zoneOfTail(String cursorTail) {
        CursorTail tail = decodeCursorTail(cursorTail);
        if (tail == null || tail.getZoneId() == null || tail.getZoneId().isEmpty()) {
            return null;
        }
        return parseZone(tail.getZoneId());
    }

    /**
     * The zone ID recorded in a cursor tail, or null when the tail is absent / carries no
     * (parsable) zone - a caller deciding whether the window still owes a pass in another
     * zone needs exactly this distinction (see PlanCaptureManager's scan-zone handoff).
     */
    static String zoneIdOfTail(String cursorTail) {
        ZoneId zone = zoneOfTail(cursorTail);
        return zone == null ? null : zone.getId();
    }

    /** Parses a zone ID (aliases allowed); an unusable value is null, never an error. */
    private static ZoneId parseZone(String zoneId) {
        try {
            return ZoneId.of(zoneId, TimeUtils.timeZoneAliasMap);
        } catch (RuntimeException e) {
            LOG.warn("SPM audit scan ignores an unparsable zone '{}': {}", zoneId, e.getMessage());
            return null;
        }
    }

    /**
     * Turns one page of raw audit rows into a batch: namespace-aware dedup plus the
     * resume cursor. Package-visible for tests (the SQL / pagination contract is tested
     * against fabricated rows).
     *
     * @param rows         the raw rows of one page
     * @param maxBatchSize the batch limit (a shorter page exhausts the window)
     * @return the scan batch
     */
    static ScanBatch toBatch(List<ResultRow> rows, int maxBatchSize) {
        return toBatch(rows, maxBatchSize, auditWriteZone());
    }

    /**
     * Turns one page of raw audit rows into a batch with an explicit timestamp zone (the
     * zone the bounds were rendered in; it travels with the cursor so a pending window
     * keeps its rendering, see CursorTail#getZoneId()).
     *
     * @param rows         the raw rows of one page
     * @param maxBatchSize the batch limit (a shorter page exhausts the window)
     * @param auditZone    the zone the window bounds were rendered in
     * @return the scan batch
     */
    static ScanBatch toBatch(List<ResultRow> rows, int maxBatchSize, ZoneId auditZone) {
        // Without explicit instant bounds the repeated-hour guard is inactive (see the
        // 5-argument overload): the string-only callers keep the previous behavior.
        return toBatch(rows, maxBatchSize, auditZone, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    /**
     * As the 3-argument toBatch with the window's own INSTANT bounds: a row whose civil
     * timestamp falls in a REPEATED hour (a fall-back transition makes the same wall
     * clock occur twice) may belong to EITHER occurrence, and the SQL range predicate
     * deliberately renders monotone wall-clock ranges and cannot tell them apart. Such a
     * row is kept as a candidate only while the LATER of its two possible instants is
     * still inside [startTimeMs, endTimeMs); otherwise it cannot provably belong to this
     * window, and consuming it here would either capture it under the wrong (older,
     * pinned) window filter or - worse - terminally reject it as filtered, after which
     * the window truly containing the later instant skips the query id forever although
     * its own filter would admit it. Skipping the candidate (the cursor above has
     * already moved past the raw row) leaves the row to that later window, which renders
     * the same civil hour again and owns the row exactly once.
     *
     * @param rows         the raw rows of one page
     * @param maxBatchSize the batch limit
     * @param auditZone    the zone the window bounds were rendered in
     * @param startTimeMs  the window start (epoch millis, inclusive)
     * @param endTimeMs    the window end (epoch millis, exclusive)
     * @return the scan batch
     */
    static ScanBatch toBatch(List<ResultRow> rows, int maxBatchSize, ZoneId auditZone,
            long startTimeMs, long endTimeMs) {
        if (rows == null || rows.isEmpty()) {
            return new ScanBatch(List.of(), true, CURSOR_ABSENT, "", "");
        }
        Map<String, CapturedQuery> deduped = new LinkedHashMap<>();
        long lastQueryTime = CURSOR_ABSENT;
        String lastTime = "";
        String lastQueryId = "";
        CursorTail lastTail = null;
        for (ResultRow row : rows) {
            // the cursor always moves to the last RAW row read, even when that row is
            // unusable / filtered later: it has been consumed and must not be scanned
            // again by the next page. query_time is nullable: keep NULL distinguishable
            // from 0 (both are valid cursors, but the resume predicate must compare a
            // NULL three-valued through IS NULL).
            String rawQueryTime = row.get(1);
            lastQueryTime = rawQueryTime == null
                    ? CURSOR_QUERY_TIME_NULL : parseLong(rawQueryTime);
            lastTime = row.getWithDefault(10, "");
            lastQueryId = row.getWithDefault(8, "");
            // the full ORDER BY key tuple: without the tail a group of rows sharing
            // (time, query_time, query_id) either repeated forever (NULL query_id group)
            // or was skipped after the first LIMIT (duplicate non-NULL tuples)
            lastTail = new CursorTail(valueAt(row, 12), valueAt(row, 5), valueAt(row, 2),
                    valueAt(row, 3), valueAt(row, 13), valueAt(row, 7), valueAt(row, 6),
                    valueAt(row, 11), auditZone == null ? null : auditZone.getId());
            if (rowFallsOutsideAllOccurrences(row.getWithDefault(10, ""), auditZone,
                    startTimeMs, endTimeMs)) {
                continue;
            }
            CapturedQuery candidate = rowToCapturedQuery(row);
            if (candidate == null || candidate.getStmt() == null || candidate.getStmt().isEmpty()) {
                continue;
            }
            String digest = candidate.getSqlDigest();
            if (digest == null || digest.isEmpty()) {
                digest = candidate.getStmt();
            }
            // namespace-aware key + SPM-match identity: the database / catalog take part
            // (SPM namespace-qualifies its match key), the ORIGINATING parser mode takes
            // part (a || b parses differently under PIPES_AS_CONCAT), and the digest is
            // refined with the CONCRETE generator arguments SPM keeps unparameterized
            // (the digest masks literals, so explode(split(s,',')) and explode(split(s,';'))
            // would otherwise collapse although they are different baselines).
            String key = candidate.getCatalog() + '\u0001' + candidate.getDb() + '\u0001'
                    + candidate.getSqlMode() + '\u0001'
                    + dedupIdentity(candidate.getStmt(), digest, candidate.getSqlMode());
            deduped.merge(key, candidate, (a, b) -> b.getQueryTimeMs() >= a.getQueryTimeMs() ? b : a);
        }
        return new ScanBatch(new ArrayList<>(deduped.values()), rows.size() < maxBatchSize,
                lastQueryTime, lastTime, lastQueryId, encodeCursorTail(lastTail));
    }

    /**
     * Whether a raw audit row's wall-clock timestamp can PROVABLY not belong to the
     * window [startTimeMs, endTimeMs) under either occurrence of a repeated hour (see
     * toBatch): when the zone repeats that wall clock (a fall-back transition), the LATER
     * of the two possible instants is the only one that can still make the row a member;
     * if even that instant is outside the window, the row belongs to a LATER window.
     * An unambiguous wall clock keeps the exact SQL-range membership (false).
     */
    private static boolean rowFallsOutsideAllOccurrences(String rawTime, ZoneId zone,
            long startTimeMs, long endTimeMs) {
        if (rawTime == null || rawTime.isEmpty() || zone == null) {
            return false;
        }
        LocalDateTime civil;
        try {
            civil = LocalDateTime.parse(rawTime, DATETIME_PARSE_FORMAT);
        } catch (RuntimeException e) {
            return false;
        }
        java.util.List<ZoneOffset> offsets = zone.getRules().getValidOffsets(civil);
        if (offsets == null || offsets.size() < 2) {
            return false;
        }
        long firstInstant = civil.toInstant(offsets.get(0)).toEpochMilli();
        long secondInstant = civil.toInstant(offsets.get(1)).toEpochMilli();
        long laterInstant = Math.max(firstInstant, secondInstant);
        return laterInstant < startTimeMs || laterInstant >= endTimeMs;
    }

    /**
     * SPM-match identity of one audit row used for the dedup: the audit digest when
     * present (it already covers the whole logical shape), refined with the CONCRETE
     * generator arguments for statements that mention a generator - SPM deliberately
     * keeps LATERAL VIEW / UNNEST arguments concrete and compares them exactly, so two
     * same-digest statements with different arguments are different baselines. A row
     * without an audit digest falls back to its text (never coarser than SPM).
     *
     * The generator arguments are parsed under the row's ORIGINATING mode: the capture
     * daemon's ambient mode can differ (a NO_BACKSLASH_ESCAPES session's split '\a'
     * means backslash + a, while the default mode reads '\a' as 'a'), and parsing both
     * rows in the daemon's mode made their fingerprints equal although SPM compares
     * them concretely - one eligible row was discarded as a duplicate.
     *
     * Package-private for tests: the identity is the only observable of the gate.
     *
     * @param stmt the audit statement text
     * @param digest the audit digest (null / empty falls back to the statement)
     * @param sqlMode the ORIGINATING parser mode of the row
     * @return the dedup identity
     */
    @VisibleForTesting
    static String dedupIdentity(String stmt, String digest, long sqlMode) {
        if (digest == null || digest.isEmpty()) {
            return stmt;
        }
        // The digest renders every literal as "?" and every scan selector as
        // PARTITION(?) / TABLET(?): two statements that differ ONLY in a concrete selector
        // (PARTITION(p1) vs PARTITION(p2)) are different baselines - matching compares the
        // selectors in sameScanIdentity - so the selector fingerprint joins the identity
        // for statements that mention one. Generator arguments join for the same reason
        // (SPM keeps LATERAL VIEW / UNNEST arguments concrete).
        String generators = mentionsGenerator(stmt) ? generatorFingerprint(stmt, sqlMode) : "";
        String selectors = mentionsScanSelector(stmt)
                ? scanSelectorFingerprint(stmt, sqlMode) : "";
        // The digest also masks the CONTENTS of an inline VALUES relation and the
        // property map of a table-valued function, while SPM compares both
        // concretely (row boundaries, arity, cell order, property payloads): two
        // same-digest rows that differ only there are different baselines, and
        // without this token toBatch dropped one of them.
        String payloads = mentionsConcreteRelationPayload(stmt)
                ? concreteRelationFingerprint(stmt, sqlMode) : "";
        if (generators.isEmpty() && selectors.isEmpty() && payloads.isEmpty()) {
            return digest;
        }
        return digest + '\u0001' + generators + '\u0001' + selectors + '\u0001' + payloads;
    }

    /**
     * Whether the statement can carry a concrete scan selector. The tokens are the ones
     * the grammar actually spells out; each one is a selector the audit digest MASKS
     * (PARTITION(p1) and PARTITION(p2) both render as PARTITION(?)) while SPM compares it
     * concretely (sameScanIdentity / sameScanParams), so the fingerprint must join the
     * dedup identity for exactly these statements:
     *   PARTITION / TABLET / TABLESAMPLE / INDEX: specifiedPartition, tabletList,
     *       sample and index selectors;
     *   "FOR VERSION AS OF" / "FOR TIME AS OF": tableSnapshot. The formerly checked
     *       "FOR TIMESTAMP" is not a form the grammar accepts, so a statement using
     *       time travel never got a fingerprint and two same-digest variants (only the
     *       version / time differs) collapsed into one identity - the capture then kept
     *       one of them and dropped the other;
     *   '@': optScanParams, the relation-level scan parameters that SPM keeps
     *       concrete (sameScanParams compares type + payloads), i.e. the @branch
     *       / @incr / @tag / @options forms.
     * A statement mentioning none of them keeps the plain digest: the gate only has to be
     * a cheap pre-filter, over-matching costs one parse, under-matching loses identity.
     *
     * Package-private for tests.
     *
     * @param stmt the audit statement text
     * @return whether the statement needs its concrete selectors in the dedup identity
     */
    @VisibleForTesting
    static boolean mentionsScanSelector(String stmt) {
        if (stmt == null) {
            return false;
        }
        String upper = upperWithCollapsedWhitespace(stmt);
        return upper.contains("PARTITION") || upper.contains("TABLET")
                || upper.contains("TABLESAMPLE") || upper.contains("INDEX")
                || upper.contains("FOR VERSION AS OF") || upper.contains("FOR TIME AS OF")
                || upper.indexOf('@') >= 0;
    }

    /** The concrete scan selectors of the statement (the full text when unparsable). */
    private static String scanSelectorFingerprint(String stmt, long sqlMode) {
        try {
            return org.apache.doris.qe.SqlModeHelper.withSqlMode(sqlMode, () -> {
                org.apache.doris.nereids.trees.plans.Plan parsed =
                        new org.apache.doris.nereids.parser.NereidsParser().parseSingle(stmt);
                return org.apache.doris.nereids.spm.SPMPlanTreeSupport
                        .scanSelectorFingerprint(parsed);
            });
        } catch (Throwable t) {
            // unparsable: keep the full-text identity, never a coarser one
            return stmt;
        }
    }

    /**
     * Whether the statement can carry a concrete relation payload that the audit digest
     * masks while SPM compares it structurally: the rows of an inline VALUES relation
     * and the property map of a table-valued function. Over-matching only costs one
     * parse; under-matching drops the payload fingerprint from the dedup identity and
     * toBatch then keeps one of two genuinely different baselines.
     *
     * Package-private for tests.
     *
     * @param stmt the audit statement text
     * @return whether the statement needs its concrete relation payloads in the dedup
     *         identity
     */
    @VisibleForTesting
    static boolean mentionsConcreteRelationPayload(String stmt) {
        if (stmt == null) {
            return false;
        }
        String upper = upperWithCollapsedWhitespace(stmt);
        if (upper.contains("VALUES")) {
            return true;
        }
        // a table-valued function's property map is spelled ("name"= (the name is a
        // quoted identifier); the gate only has to over-match
        return mentionsTvfPropertyMap(stmt);
    }

    /**
     * Whether the raw text opens a table-valued function's property map: the
     * ("name"= spelling, matched loosely - the pre-filter only has to over-match, and a
     * miss would keep two different properties on one dedup identity.
     *
     * @param stmt the audit statement text
     * @return whether the statement may carry a property map
     */
    private static boolean mentionsTvfPropertyMap(String stmt) {
        int index = stmt.indexOf('(');
        while (index >= 0) {
            int nameStart = index + 1;
            while (nameStart < stmt.length() && Character.isWhitespace(stmt.charAt(nameStart))) {
                nameStart++;
            }
            if (nameStart < stmt.length() && stmt.charAt(nameStart) == '"') {
                int nameEnd = stmt.indexOf('"', nameStart + 1);
                if (nameEnd > nameStart + 1) {
                    int equalsAt = nameEnd + 1;
                    while (equalsAt < stmt.length()
                            && Character.isWhitespace(stmt.charAt(equalsAt))) {
                        equalsAt++;
                    }
                    if (equalsAt < stmt.length() && stmt.charAt(equalsAt) == '=') {
                        return true;
                    }
                }
            }
            index = stmt.indexOf('(', index + 1);
        }
        return false;
    }

    /**
     * The concrete relation payloads of the statement (the full text when unparsable).
     * A PARSED statement without any concrete payload (the gate over-matched: an ordinary
     * table named order_values carries the VALUES token, a plain call carries a
     * parenthesis) contributes NOTHING: the raw-text fallback is reserved for PARSE
     * FAILURES. Returning the full statement here put the unmasked texts of
     * WHERE o.k = 1 / WHERE o.k = 2 on the SAME audit digest, so capture treated every
     * literal variant as a distinct query and replanned / probed each of them instead of
     * deduplicating the page.
     */
    private static String concreteRelationFingerprint(String stmt, long sqlMode) {
        try {
            return org.apache.doris.qe.SqlModeHelper.withSqlMode(sqlMode, () -> {
                org.apache.doris.nereids.trees.plans.Plan parsed =
                        new org.apache.doris.nereids.parser.NereidsParser().parseSingle(stmt);
                if (!(parsed instanceof org.apache.doris.nereids.trees.plans.logical
                        .LogicalPlan)) {
                    return stmt;
                }
                return org.apache.doris.nereids.spm.SPMPlanTreeSupport
                        .concreteRelationPayloadFingerprint(
                                (org.apache.doris.nereids.trees.plans.logical.LogicalPlan)
                                        parsed);
            });
        } catch (Throwable t) {
            // unparsable: keep the full-text identity, never a coarser one
            return stmt;
        }
    }

    /**
     * Whether the statement can carry concrete generator arguments (LATERAL VIEW /
     * UNNEST). Over-matching only costs one parse; under-matching drops the argument
     * fingerprint from the dedup identity (the multi-token marker must be
     * matched across whitespace runs - see upperWithCollapsedWhitespace).
     *
     * Package-private for tests.
     *
     * @param stmt the audit statement text
     * @return whether the statement needs its concrete generator arguments in the
     *         dedup identity
     */
    @VisibleForTesting
    static boolean mentionsGenerator(String stmt) {
        if (stmt == null) {
            return false;
        }
        String upper = upperWithCollapsedWhitespace(stmt);
        return upper.contains("LATERAL VIEW") || upper.contains("UNNEST");
    }

    /**
     * Upper-cased statement with every whitespace run collapsed to one space: the
     * multi-token gates above (LATERAL VIEW, FOR TIME AS OF) must not miss
     * a line-break separated spelling. The audit DIGEST masks the concrete
     * generator / snapshot arguments, so a statement the gate misses keeps the plain
     * digest as its dedup identity - two slow queries differing only in a split delimiter
     * or a time-travel snapshot then share that identity and toBatch drops one, although
     * SPM compares those arguments concretely. Over-matching only costs one parse.
     */
    private static String upperWithCollapsedWhitespace(String stmt) {
        return stmt.toUpperCase(java.util.Locale.ROOT).replaceAll("\\s+", " ");
    }

    /** The concrete generator arguments of the statement (the full text when unparsable). */
    private static String generatorFingerprint(String stmt, long sqlMode) {
        try {
            String fingerprint = org.apache.doris.qe.SqlModeHelper.withSqlMode(sqlMode, () -> {
                org.apache.doris.nereids.trees.plans.Plan parsed =
                        new org.apache.doris.nereids.parser.NereidsParser().parseSingle(stmt);
                StringBuilder sb = new StringBuilder();
                org.apache.doris.nereids.spm.SPMPlanTreeSupport.<RuntimeException>walkPlans(
                        parsed, node -> {
                            if (node instanceof org.apache.doris.nereids.trees.plans.logical
                                    .LogicalGenerate) {
                                org.apache.doris.nereids.trees.plans.logical.LogicalGenerate<?> generate =
                                        (org.apache.doris.nereids.trees.plans.logical.LogicalGenerate<?>)
                                                node;
                                for (org.apache.doris.nereids.trees.expressions.Expression generator
                                        : generate.getGenerators()) {
                                    sb.append(generator.toSql()).append('|');
                                }
                            }
                        });
                return sb.toString();
            });
            return fingerprint;
        } catch (Throwable t) {
            // unparsable: keep the full-text identity, never a coarser one
            return stmt;
        }
    }

    /**
     * Builds the audit_log scan SQL. Public for tests: the pushed-down predicate shape is
     * part of the capture contract - eligibility is query time OR scanned rows (the same
     * rule as PlanCaptureFilter), and internal maintenance queries are filtered in SQL
     * instead of relying on a hardcoded event flag.
     *
     * @param start          window start timestamp (formatted)
     * @param end            window end timestamp (formatted)
     * @param maxBatchSize   LIMIT for the scan
     * @param minQueryTimeMs query-time threshold
     * @param minScanRows    scan-rows threshold
     * @return the scan SQL
     */
    public static String buildScanSql(String start, String end, int maxBatchSize,
            long minQueryTimeMs, long minScanRows) {
        return buildScanSql(start, end, maxBatchSize, minQueryTimeMs, minScanRows, "");
    }

    /**
     * Builds the audit_log scan SQL with an optional resume-cursor predicate. The ORDER
     * BY defines the stable total order the cursor walks (see ORDER_BY): the
     * row EVENT time first, then every remaining identity / metric key as a durable tie
     * breaker.
     *
     * @param start           window start timestamp (formatted)
     * @param end             window end timestamp (formatted)
     * @param maxBatchSize    LIMIT for the scan
     * @param minQueryTimeMs  query-time threshold
     * @param minScanRows     scan-rows threshold
     * @param cursorPredicate resume-cursor predicate (empty when starting at the top)
     * @return the scan SQL
     */
    public static String buildScanSql(String start, String end, int maxBatchSize,
            long minQueryTimeMs, long minScanRows, String cursorPredicate) {
        return buildScanSql(List.<String[]>of(new String[] {start, end}), maxBatchSize,
                minQueryTimeMs, minScanRows, cursorPredicate, 0L);
    }

    /**
     * As buildScanSql(String, String, int, long, long, String) with an explicit
     * offset swing for the completion-aware lower bound (see
     * buildScanSql(List, int, long, long, String, long)): the single-range form a
     * fixed-offset zone produces.
     *
     * @param start               window start timestamp (formatted)
     * @param end                 window end timestamp (formatted)
     * @param maxBatchSize        LIMIT for the scan
     * @param minQueryTimeMs      query-time threshold
     * @param minScanRows         scan-rows threshold
     * @param cursorPredicate     resume-cursor predicate (empty when starting at the top)
     * @param offsetSwingSeconds  max offset swing of the window's zone (0 = none)
     * @return the scan SQL
     */
    static String buildScanSql(String start, String end, int maxBatchSize,
            long minQueryTimeMs, long minScanRows, String cursorPredicate,
            long offsetSwingSeconds) {
        return buildScanSql(List.<String[]>of(new String[] {start, end}), maxBatchSize,
                minQueryTimeMs, minScanRows, cursorPredicate, offsetSwingSeconds);
    }

    /**
     * Builds the audit_log scan SQL for a window rendered as one or MORE monotone
     * wall-clock ranges (see localTimeRanges) and with the zone's offset swing
     * applied to the completion-aware lower bound (see
     * zoneOffsetSwingSeconds).
     *
     * @param windowRanges        (start, end) wall-clock pairs of the window
     * @param maxBatchSize        LIMIT for the scan
     * @param minQueryTimeMs      query-time threshold
     * @param minScanRows         scan-rows threshold
     * @param cursorPredicate     resume-cursor predicate (empty when starting at the top)
     * @param offsetSwingSeconds  max offset swing of the window's zone (0 = none)
     * @return the scan SQL
     */
    static String buildScanSql(List<String[]> windowRanges, int maxBatchSize,
            long minQueryTimeMs, long minScanRows, String cursorPredicate,
            long offsetSwingSeconds) {
        return buildScanSql(windowRanges, maxBatchSize, minQueryTimeMs, minScanRows,
                cursorPredicate, offsetSwingSeconds,
                completeWindowFloor(windowRanges.get(0)[0]));
    }

    /**
     * As the six-argument overload with an EXPLICIT completion floor (the top-level
     * partition-pruning lower bound), rendered by the caller from the window-start
     * INSTANT (see lateCompletionFloor): the string form is civil arithmetic and
     * therefore wrong across a DST transition (see
     * completeWindowFloor(String)).
     *
     * @param floor the already rendered completion floor (see lateCompletionFloor)
     * @return the scan SQL
     */
    static String buildScanSql(List<String[]> windowRanges, int maxBatchSize,
            long minQueryTimeMs, long minScanRows, String cursorPredicate,
            long offsetSwingSeconds, String floor) {
        // The window lower bound is COMPLETION-aware: audit_log.time is the query's START
        // time, but its row is published only when the query FINISHES. A long-running
        // query started at 11:50 is absent from the 12:00 scan; without the
        // completion predicate the next (default three-hour) window starts at
        // 12:00 - overlap, so its 11:50 row - now visible - would be excluded
        // FOREVER. Rows are therefore also eligible while their completion
        // (time + query_time) reaches into the window.
        //
        // The window predicate may therefore only bound the START time from ABOVE and
        // split the ranges for the LOWER bound: conjoining the start-time membership
        // (`time >= window start`, which windowPredicate implies for the first range)
        // nullified the completion branch entirely - the earlier-start row the branch
        // exists for failed the conjunct on every later scan. Only the upper bound and
        // the partitionable floor are top-level conjuncts; the lower bound is the OR of
        // (start-time membership in one of the ranges, completion reaching the window
        // start).
        // The completion branch is BOUNDED by a floor: an unbounded
        // "time >= start OR completion >= start" cannot prune ANY old partition of the
        // range-partitioned audit table (query_time is only known per row), so every
        // keyset page would rescan retained history under the short timeout. The floor
        // (start - LATE_COMPLETION_LOOKBACK_MILLIS) keeps the pruning intact for a
        // three-hour window while still admitting every query whose completion reaches
        // into it; a query LONGER than the lookback is the documented miss.
        //
        // The completion is civil arithmetic on the writer's LOCAL rendering, so it must
        // be widened by the zone's offset swing: a query started 01:30 PST (09:30Z) that
        // finishes 03:10:01 PDT computes as 02:10:01 without the swing, and a window
        // starting 03:05 would exclude the row on EVERY later scan (no overlap reaches it
        // again). Adding the swing seconds makes the bound conservative in the admitting
        // direction, which is the safe side for a late-completion lookback.
        //
        // The duration is added at MILLISECOND precision: both `time` and
        // `query_time` are millisecond values, and the previous CAST(query_time / 1000)
        // truncated the duration to whole seconds - a row at 11:50:00.900 lasting 299100
        // ms truly completes at 11:55:00.000 (the next overlap start) but computed as
        // 11:54:59.900 and was excluded on every later scan. Microsecond arithmetic on
        // the exact millisecond duration is exact in both directions.
        String start = windowRanges.get(0)[0];
        // The upper bound is the GREATEST rendered end, not the last range's
        // #5): at a fall-back transition the ranges render as [01:45, 02:00) then
        // [01:00, 01:15), and bounding the whole scan by the LAST end (01:15) discarded
        // every row of the first range (a published 01:50 row) before the OR / completion
        // predicate could admit it - the capture then checkpointed past it. The maximum
        // keeps the partition-pruning conjunct while covering every range; the wall-clock
        // strings share one format, so the comparison is exact.
        String lastEnd = windowRanges.get(0)[1];
        for (int i = 1; i < windowRanges.size(); i++) {
            if (windowRanges.get(i)[1].compareTo(lastEnd) > 0) {
                lastEnd = windowRanges.get(i)[1];
            }
        }
        String completionBound = "timestampadd(MICROSECOND, CAST(`query_time` AS BIGINT) * 1000"
                + (offsetSwingSeconds == 0 ? "" : " + " + offsetSwingSeconds * 1_000_000L)
                + ", `time`)";
        return "SELECT " + SELECT_COLUMNS + " FROM __internal_schema.audit_log "
                + "WHERE `time` >= '" + floor + "' "
                + "AND `time` < '" + lastEnd + "' "
                + "AND (" + windowPredicate(windowRanges)
                + " OR " + completionBound + " >= '" + start + "') "
                + "AND `is_query` = true "
                + "AND `is_nereids` = true "
                + "AND (`query_time` >= " + minQueryTimeMs
                + " OR `scan_rows` >= " + minScanRows + ") "
                + "AND `is_internal` = false "
                + (cursorPredicate == null ? "" : cursorPredicate)
                + ORDER_BY
                + "LIMIT " + maxBatchSize;
    }

    /**
     * The completion floor computed from the window-start INSTANT in the scan zone: the
     * window start minus LATE_COMPLETION_LOOKBACK_MILLIS, then minus the zone's full
     * offset swing, rendered in the zone the bounds are rendered in.
     *
     * The civil rendering of the lookback instant alone is NOT a safe lower bound: at a
     * fall-back the civil clock moves BACKWARD although instants only move forward, so
     * the rendered floor (2026-11-01 01:30 PDT for a window starting 24h later) can sit
     * AFTER a legitimately admittable row's civil time (a query at 09:05Z = 01:05 PST,
     * the second occurrence of the repeated hour, whose 23h30m completion reaches the
     * window). The zone-less `time >= floor` conjunct then discarded that row before the
     * completion branch could admit it, and later floors only move forward - the row was
     * never revisited. Subtracting the swing makes the bound hold for BOTH occurrences:
     * every instant >= start - lookback renders at or above it (civil(t) >=
     * civil(lookback instant) - swing, see zoneOffsetSwingSeconds), so no admittable row
     * is pruned away. The wider floor only scans extra rows; membership is still decided
     * by the window / completion predicate, so the widening can never admit a wrong row.
     *
     * @param startTimeMs window start (epoch millis)
     * @param zone        the zone the bounds are rendered in
     * @return the rendered floor timestamp
     */
    static String lateCompletionFloor(long startTimeMs, ZoneId zone) {
        LocalDateTime civilFloor = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(startTimeMs - LATE_COMPLETION_LOOKBACK_MILLIS), zone)
                .minusSeconds(zoneOffsetSwingSeconds(zone));
        return civilFloor.format(DATETIME_FORMAT);
    }

    /**
     * The window membership predicate: one range as-is, several ranges OR'd (the order
     * matters only for readability - the union is what the scan stores).
     */
    private static String windowPredicate(List<String[]> windowRanges) {
        if (windowRanges.size() == 1) {
            String[] range = windowRanges.get(0);
            return "`time` >= '" + range[0] + "' AND `time` < '" + range[1] + "'";
        }
        StringBuilder predicate = new StringBuilder("(");
        for (int i = 0; i < windowRanges.size(); i++) {
            String[] range = windowRanges.get(i);
            if (i > 0) {
                predicate.append(" OR ");
            }
            predicate.append("(`time` >= '").append(range[0])
                    .append("' AND `time` < '").append(range[1]).append("')");
        }
        return predicate.append(')').toString();
    }

    /**
     * The partitionable floor of the completion-aware lower bound derived from the rendered
     * window start WITHOUT a zone: civil arithmetic on the local timestamp. It is only
     * exact for a fixed-offset rendering - across a DST transition the subtraction lands
     * an hour off the intended instant - so the production scan computes the floor from
     * the window-start INSTANT instead (see lateCompletionFloor); this form is
     * kept for the string-only builders (tests, legacy single-range callers). An unparsable
     * timestamp keeps the start itself (never a LESS bounded range).
     */
    private static String completeWindowFloor(String start) {
        try {
            return java.time.LocalDateTime.parse(start, DATETIME_PARSE_FORMAT)
                    .minus(java.time.Duration.ofMillis(LATE_COMPLETION_LOOKBACK_MILLIS))
                    .format(DATETIME_FORMAT);
        } catch (RuntimeException e) {
            return start;
        }
    }

    /**
     * Resume-cursor predicate of the scan total order (see ORDER_BY): strictly
     * "after" the last consumed row in EVERY ordered key, so a truncated batch continues
     * exactly where it stopped. Presence is decided SOLELY by the CURSOR_ABSENT sentinel:
     * the key columns are nullable and an empty value means SQL NULL (NOT "no cursor").
     *
     * NULL means "largest value" under DESC (NULLS LAST), so entering a NULL group is
     * expressed through IS NULL and the chain continues with the next key. When the LAST
     * key is NULL the remaining rows of the group agree on every ordered column - they
     * are content duplicates of the cursor row (same event time, same query id, same
     * client, same statement hash, ...), and capture dedupes by (catalog, db, digest), so
     * the predicate stops there ("1 = 0") instead of re-selecting the group forever
     * (which is what the old query_id-only predicate did for NULL query ids).
     */
    static String cursorPredicate(long cursorQueryTime, String cursorTime, String cursorQueryId) {
        return cursorPredicate(cursorQueryTime, cursorTime, cursorQueryId, "");
    }

    /**
     * Resume-cursor predicate with the full cursor tail (see CursorTail); a blank
     * tail (legacy cursor) falls back to the (time, query_time, query_id) prefix.
     */
    static String cursorPredicate(long cursorQueryTime, String cursorTime, String cursorQueryId,
            String cursorTail) {
        if (cursorQueryTime == CURSOR_ABSENT) {
            return "";
        }
        List<CursorKey> keys = new ArrayList<>();
        keys.add(new CursorKey("`time`", emptyToNull(cursorTime), false));
        keys.add(new CursorKey("`query_time`",
                cursorQueryTime == CURSOR_QUERY_TIME_NULL ? null : String.valueOf(cursorQueryTime),
                true));
        keys.add(new CursorKey("`query_id`", emptyToNull(cursorQueryId), false));
        CursorTail tail = decodeCursorTail(cursorTail);
        if (tail != null) {
            keys.add(new CursorKey("`client_ip`", emptyToNull(tail.getClientIp()), false));
            keys.add(new CursorKey("`sql_hash`", emptyToNull(tail.getSqlHash()), false));
            keys.add(new CursorKey("`scan_rows`", emptyToNull(tail.getScanRows()), true));
            keys.add(new CursorKey("`return_rows`", emptyToNull(tail.getReturnRows()), true));
            keys.add(new CursorKey(STMT_HASH_EXPR, emptyToNull(tail.getStmtHash()), false));
            if (tail.hasNamespaceKeys()) {
                // namespace + parser mode: two NaN-id rows otherwise equal on every ordered
                // key can still be SEPARATE capture identities (see toBatch's dedup key);
                // without these keys the strict chain excluded the second row on every page.
                // A legacy tail never observed them, so its chain keeps the old prefix - an
                // appended NULL comparison would skip the group's remaining rows.
                keys.add(new CursorKey("`catalog`", emptyToNull(tail.getCatalog()), false));
                keys.add(new CursorKey("`db`", emptyToNull(tail.getDb()), false));
                // audit_log.sql_mode is a STRING column carrying the mode NAME (e.g.
                // PIPES_AS_CONCAT; numeric modes are stored as their decimal text). The
                // ORDER BY compares it as a string, so the cursor must too: rendering it
                // numerically emitted "sql_mode < PIPES_AS_CONCAT" (an identifier, not a
                // literal) and EVERY page after a named-mode row failed with "unknown
                // column", pinning the capture window forever.
                keys.add(new CursorKey("`sql_mode`", emptyToNull(tail.getSqlMode()), false));
            }
        }
        return " AND (" + renderAfter(keys, 0) + ") ";
    }

    /** One ordered key of the cursor: rendered expression, raw value (null = SQL NULL). */
    private static final class CursorKey {
        private final String expr;
        private final String value;
        private final boolean numeric;

        private CursorKey(String expr, String value, boolean numeric) {
            this.expr = expr;
            this.value = value;
            this.numeric = numeric;
        }
    }

    /** Renders the strictly-after chain for keys[i..]; see cursorPredicate. */
    private static String renderAfter(List<CursorKey> keys, int index) {
        if (index >= keys.size()) {
            return "1 = 1";
        }
        CursorKey key = keys.get(index);
        if (key.value == null) {
            if (index == keys.size() - 1) {
                // every ordered column agrees with the cursor row: content duplicate
                return "1 = 0";
            }
            return "(" + key.expr + " IS NULL AND " + renderAfter(keys, index + 1) + ")";
        }
        String value = key.numeric ? key.value : "'" + escapeSQLString(key.value) + "'";
        if (index == keys.size() - 1) {
            return "(" + key.expr + " < " + value + " OR " + key.expr + " IS NULL)";
        }
        return "(" + key.expr + " < " + value + " OR " + key.expr + " IS NULL"
                + " OR (" + key.expr + " = " + value + " AND " + renderAfter(keys, index + 1) + "))";
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }

    private static String escapeSQLString(String value) {
        return value.replace("'", "''");
    }

    /** Formats a scan bound in the given zone (see auditWriteZone()). */
    static String formatTimestamp(long epochMillis, ZoneId zone) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zone);
        return time.format(DATETIME_FORMAT);
    }

    /** Formats an instant with an EXPLICIT offset (see localTimeRanges). */
    private static String formatTimestampWithOffset(long epochMillis, ZoneOffset offset) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), offset);
        return time.format(DATETIME_FORMAT);
    }

    /**
     * Renders a UTC window as the list of (start, end) wall-clock ranges the stored
     * audit timestamps are compared against, splitting at every zone offset transition
     * inside the window.
     *
     * audit_log.time is the audit WRITER's local rendering, so the scan compares
     * strings - which is only sound while the offset stays constant inside the window.
     * With time_zone = America/Los_Angeles the window [2026-11-01 08:45Z, 09:15Z)
     * renders as [01:45, 01:15): start > end, so the SQL matched NO row, the empty page
     * looked exhausted and the capture advanced its watermark past rows written in the
     * repeated hour (a row at 09:05Z / 01:05 PST stayed invisible even when its
     * five-minute overlap window was scanned later). Each segment below is rendered with
     * the offset in effect INSIDE it - the upper bound with the offset just BEFORE the
     * segment end, which is what keeps a fall-back segment [01:45, 02:00) monotone
     * instead of ending at the repeated 01:00 - so every pair is a valid range and the
     * union covers the whole window.
     *
     * @param startMs window start (epoch millis, inclusive)
     * @param endMs   window end (epoch millis, exclusive)
     * @param zone    the zone the stored timestamps were rendered in
     * @return one or more (start, end) wall-clock ranges, in window order
     */
    static List<String[]> localTimeRanges(long startMs, long endMs, ZoneId zone) {
        List<String[]> ranges = new ArrayList<>();
        if (endMs <= startMs) {
            ranges.add(new String[] {formatTimestamp(startMs, zone), formatTimestamp(endMs, zone)});
            return ranges;
        }
        long segmentStart = startMs;
        while (segmentStart < endMs) {
            long segmentEnd = nextOffsetTransition(segmentStart, endMs, zone);
            ranges.add(new String[] {
                    formatTimestamp(segmentStart, zone),
                    // the offset BEFORE the segment end: at a fall-back transition the
                    // instant itself already renders with the NEW offset, which would
                    // invert the range
                    formatTimestampWithOffset(segmentEnd,
                            zone.getRules().getOffset(Instant.ofEpochMilli(segmentEnd - 1)))});
            segmentStart = segmentEnd;
        }
        return ranges;
    }

    /**
     * The instant of the next zone offset transition strictly after fromMs, or
     * endMs when none falls inside the window.
     */
    private static long nextOffsetTransition(long fromMs, long endMs, ZoneId zone) {
        java.time.zone.ZoneOffsetTransition transition =
                zone.getRules().nextTransition(Instant.ofEpochMilli(fromMs));
        if (transition == null) {
            return endMs;
        }
        long transitionMs = transition.getInstant().toEpochMilli();
        return transitionMs > fromMs && transitionMs < endMs ? transitionMs : endMs;
    }

    /**
     * The maximum offset swing of a zone (max offset - min offset over its whole history),
     * in seconds; 0 for a fixed-offset zone.
     *
     * Bounds |offset(completion) - offset(start)| for any two instants of the
     * zone, which is exactly the error of the civil-time completion arithmetic below (the
     * stored start is a local rendering, adding the ELAPSED seconds to it ignores a DST
     * transition in between). The scan uses it to widen the completion-aware lower bound,
     * so a query spanning a transition can no longer be excluded from every later window.
     *
     * @param zone the zone the stored timestamps were rendered in
     * @return the swing in seconds
     */
    static long zoneOffsetSwingSeconds(ZoneId zone) {
        java.time.zone.ZoneRules rules = zone.getRules();
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (java.time.zone.ZoneOffsetTransition transition : rules.getTransitions()) {
            max = Math.max(max, Math.max(transition.getOffsetBefore().getTotalSeconds(),
                    transition.getOffsetAfter().getTotalSeconds()));
            min = Math.min(min, Math.min(transition.getOffsetBefore().getTotalSeconds(),
                    transition.getOffsetAfter().getTotalSeconds()));
        }
        return max == Integer.MIN_VALUE || min == Integer.MAX_VALUE ? 0L : max - min;
    }

    /**
     * Maps one audit_log row to a CapturedQuery.
     *
     * @param row the result row (column order matches SELECT_COLUMNS)
     * @return the candidate, or null when the row is unusable
     */
    private static CapturedQuery rowToCapturedQuery(ResultRow row) {
        if (row == null) {
            return null;
        }
        try {
            String stmt = row.getWithDefault(0, "");
            long queryTime = parseLong(row.getWithDefault(1, "0"));
            long scanRows = parseLong(row.getWithDefault(2, "0"));
            long returnRows = parseLong(row.getWithDefault(3, "0"));
            String sqlDigest = row.getWithDefault(4, "");
            String sqlHash = row.getWithDefault(5, "");
            String db = row.getWithDefault(6, "");
            String catalog = row.getWithDefault(7, "");
            String queryId = row.getWithDefault(8, "");
            boolean isInternal = Boolean.parseBoolean(row.getWithDefault(9, "false"));
            // the ORIGINATING parser mode of the captured statement: the build must run
            // under it and the baseline persists it as creatorSqlMode (a reload re-parses
            // the stored bindSql the same way). Rows read before the column existed (and
            // fabricated test rows) carry fewer values - they mean the default mode.
            long sqlMode = row.getValues().size() > 11
                    ? decodeAuditSqlMode(row.get(11)) : SqlModeHelper.MODE_DEFAULT;
            return new CapturedQuery(stmt, queryTime, scanRows, returnRows, sqlDigest, sqlHash, db, catalog,
                    queryId, isInternal, sqlMode);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * Decodes the audit_log `sql_mode` text (the decoded names string the audit plugin
     * captured from the originating session, e.g. "PIPES_AS_CONCAT"; a numeric mode is
     * accepted too) back into the long parser mode the capture must build the baseline
     * under. Empty / broken / zero text means the default mode - a literal "a || b" in
     * a PIPES_AS_CONCAT statement must not be captured as a boolean OR. Public for tests
     * (the decode is part of the capture contract).
     *
     * @param text the audit_log sql_mode text (may be null / empty)
     * @return the decoded parser mode (never 0)
     */
    public static long decodeAuditSqlMode(String text) {
        if (text == null || text.trim().isEmpty()) {
            return SqlModeHelper.MODE_DEFAULT;
        }
        try {
            long mode = SqlModeHelper.encode(text.trim());
            return mode == 0 ? SqlModeHelper.MODE_DEFAULT : mode;
        } catch (Exception e) {
            return SqlModeHelper.MODE_DEFAULT;
        }
    }

    private static long parseLong(String text) {
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }
}
