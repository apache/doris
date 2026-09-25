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

import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
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
 * Pagination: the batch LIMIT is applied with a stable (query_time, time, query_id)
 * cursor. The caller resumes from the returned cursor until a batch comes back shorter
 * than the limit (window exhausted); advancing the window past a truncated batch would
 * permanently skip every eligible row beyond the LIMIT.
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

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** audit_log SELECT columns (order must match rowToCapturedQuery / toBatch). */
    private static final String SELECT_COLUMNS =
            "`stmt`, `query_time`, `scan_rows`, `return_rows`, `sql_digest`, `sql_hash`, `db`, `catalog`,"
                    + " `query_id`, `is_internal`, `time`";

    /**
     * Result of one audit scan: the namespace-deduplicated candidates plus the resume
     * cursor ((query_time, time, query_id) of the last RAW row read).
     */
    public static class ScanBatch {
        private final List<CapturedQuery> candidates;
        private final boolean windowExhausted;
        private final long cursorQueryTime;
        private final String cursorTime;
        private final String cursorQueryId;

        ScanBatch(List<CapturedQuery> candidates, boolean windowExhausted,
                long cursorQueryTime, String cursorTime, String cursorQueryId) {
            this.candidates = candidates;
            this.windowExhausted = windowExhausted;
            this.cursorQueryTime = cursorQueryTime;
            this.cursorTime = cursorTime == null ? "" : cursorTime;
            this.cursorQueryId = cursorQueryId == null ? "" : cursorQueryId;
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
        return scan(startTimeMs, endTimeMs, maxBatchSize, CURSOR_ABSENT, "", "");
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
     * @param cursorTime     time (event time) of the last consumed row
     * @param cursorQueryId  query_id of the last consumed row
     * @return the scan batch (candidates + resume cursor)
     */
    public ScanBatch scan(long startTimeMs, long endTimeMs, int maxBatchSize,
            long cursorQueryTime, String cursorTime, String cursorQueryId) {
        String start = formatTimestamp(startTimeMs);
        String end = formatTimestamp(endTimeMs);
        // defense in depth: a non-positive batch size can no longer be written through
        // SQL SET (see SessionVariable), but LIMIT 0 here would mark the window exhausted
        // on an empty page and advance the watermark over every eligible row
        int limit = Math.max(1, maxBatchSize);

        SessionVariable global = VariableMgr.getDefaultSessionVariable();
        long minQueryTimeMs = global.getPlanCaptureMinQueryTimeMs();
        long minScanRows = global.getPlanCaptureMinScanRows();
        String sql = buildScanSql(start, end, limit, minQueryTimeMs, minScanRows,
                cursorPredicate(cursorQueryTime, cursorTime, cursorQueryId));

        List<ResultRow> rows = StatisticsUtil.execStatisticQuery(sql);
        return toBatch(rows, limit);
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
        if (rows == null || rows.isEmpty()) {
            return new ScanBatch(List.of(), true, CURSOR_ABSENT, "", "");
        }
        Map<String, CapturedQuery> deduped = new LinkedHashMap<>();
        long lastQueryTime = CURSOR_ABSENT;
        String lastTime = "";
        String lastQueryId = "";
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
            CapturedQuery candidate = rowToCapturedQuery(row);
            if (candidate == null || candidate.getStmt() == null || candidate.getStmt().isEmpty()) {
                continue;
            }
            String digest = candidate.getSqlDigest();
            if (digest == null || digest.isEmpty()) {
                digest = candidate.getStmt();
            }
            // namespace-aware key: the database / catalog take part, otherwise identical
            // unqualified SQL from two namespaces collapses to one candidate and the
            // other namespace never gets a baseline (SPM namespace-qualifies its match
            // key, so the two executions really are different queries)
            String key = candidate.getCatalog() + '\u0001' + candidate.getDb() + '\u0001' + digest;
            deduped.merge(key, candidate, (a, b) -> b.getQueryTimeMs() >= a.getQueryTimeMs() ? b : a);
        }
        return new ScanBatch(new ArrayList<>(deduped.values()), rows.size() < maxBatchSize,
                lastQueryTime, lastTime, lastQueryId);
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
     * BY defines the stable total order the cursor walks:
     * (query_time DESC, time DESC, query_id DESC).
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
        return "SELECT " + SELECT_COLUMNS + " FROM __internal_schema.audit_log "
                + "WHERE `time` >= '" + start + "' AND `time` < '" + end + "' "
                + "AND `is_query` = true "
                + "AND `is_nereids` = true "
                + "AND (`query_time` >= " + minQueryTimeMs
                + " OR `scan_rows` >= " + minScanRows + ") "
                + "AND `is_internal` = false "
                + (cursorPredicate == null ? "" : cursorPredicate)
                + " ORDER BY `query_time` DESC, `time` DESC, `query_id` DESC "
                + "LIMIT " + maxBatchSize;
    }

    /**
     * Resume-cursor predicate of the (query_time, time, query_id) total order: strictly
     * "after" the last consumed row, so a truncated batch continues exactly where it
     * stopped without re-reading or skipping rows.
     *
     * Zero and NULL query_time rows are legitimate cursors: presence is decided by the
     * CURSOR_ABSENT sentinel (a zero query_time is NOT "no cursor"), and a NULL cursor
     * compares through IS NULL (Doris orders NULLs after every value under DESC, which
     * the raw ORDER BY of the scan relies on).
     */
    static String cursorPredicate(long cursorQueryTime, String cursorTime, String cursorQueryId) {
        if (cursorQueryTime == CURSOR_ABSENT
                || cursorTime == null || cursorTime.isEmpty()
                || cursorQueryId == null || cursorQueryId.isEmpty()) {
            return "";
        }
        String time = escapeSQLString(cursorTime);
        String queryId = escapeSQLString(cursorQueryId);
        String strictlyAfter = "(`time` < '" + time
                + "' OR (`time` = '" + time + "' AND `query_id` < '" + queryId + "'))";
        if (cursorQueryTime == CURSOR_QUERY_TIME_NULL) {
            return " AND (`query_time` IS NULL AND " + strictlyAfter + ") ";
        }
        return " AND (`query_time` < " + cursorQueryTime
                + " OR `query_time` IS NULL"
                + " OR (`query_time` = " + cursorQueryTime
                + " AND " + strictlyAfter + ")) ";
    }

    private static String escapeSQLString(String value) {
        return value.replace("'", "''");
    }

    private static String formatTimestamp(long epochMillis) {
        LocalDateTime time = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        return time.format(DATETIME_FORMAT);
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
            return new CapturedQuery(stmt, queryTime, scanRows, returnRows, sqlDigest, sqlHash, db, catalog,
                    queryId, isInternal);
        } catch (RuntimeException e) {
            return null;
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
