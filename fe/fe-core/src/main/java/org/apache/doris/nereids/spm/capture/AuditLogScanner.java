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
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
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
 * {@link #ORDER_BY}); the caller resumes from the returned cursor until a batch comes
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

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
                    + " DESC ";

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

        CursorTail(String clientIp, String sqlHash, String scanRows, String returnRows,
                String stmtHash) {
            this.clientIp = clientIp;
            this.sqlHash = sqlHash;
            this.scanRows = scanRows;
            this.returnRows = returnRows;
            this.stmtHash = stmtHash;
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
    }

    /** Encodes a cursor tail as a compact JSON list (null-safe; empty text = absent). */
    public static String encodeCursorTail(CursorTail tail) {
        if (tail == null
                || (tail.getClientIp() == null && tail.getSqlHash() == null
                && tail.getScanRows() == null && tail.getReturnRows() == null
                && tail.getStmtHash() == null)) {
            // no tail information at all (a row without the appended columns - e.g. a
            // pre-column audit row or a fabricated test row): treat it as a PREFIX-only
            // cursor; a JSON array of nulls would otherwise extend the resume chain with
            // all-NULL keys and terminate it immediately.
            return "";
        }
        return new Gson().toJson(Arrays.asList(tail.getClientIp(), tail.getSqlHash(),
                tail.getScanRows(), tail.getReturnRows(), tail.getStmtHash()));
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
            return new CursorTail(values.get(0), values.get(1), values.get(2),
                    values.get(3), values.get(4));
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

        /** Encoded tail of the cursor (see {@link CursorTail}); empty = absent. */
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
     * cursor tuple (see {@link CursorTail}).
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
                cursorPredicate(cursorQueryTime, cursorTime, cursorQueryId, cursorTail));

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
                    valueAt(row, 3), valueAt(row, 13));
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
                lastQueryTime, lastTime, lastQueryId, encodeCursorTail(lastTail));
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
     * BY defines the stable total order the cursor walks (see {@link #ORDER_BY}): the
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
        return "SELECT " + SELECT_COLUMNS + " FROM __internal_schema.audit_log "
                + "WHERE `time` >= '" + start + "' AND `time` < '" + end + "' "
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
     * Resume-cursor predicate of the scan total order (see {@link #ORDER_BY}): strictly
     * "after" the last consumed row in EVERY ordered key, so a truncated batch continues
     * exactly where it stopped. Presence is decided SOLELY by the CURSOR_ABSENT sentinel:
     * the key columns are nullable and an empty value means SQL NULL (NOT "no cursor").
     *
     * <p>NULL means "largest value" under DESC (NULLS LAST), so entering a NULL group is
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
     * Resume-cursor predicate with the full cursor tail (see {@link CursorTail}); a blank
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

    /** Renders the strictly-after chain for keys[i..]; see {@link #cursorPredicate}. */
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
