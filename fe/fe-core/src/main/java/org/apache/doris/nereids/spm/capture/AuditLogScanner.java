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
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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

    /**
     * Statement timeout (seconds) of the synchronous audit read. The default
     * StatisticsUtil overload assigns the ANALYZE timeout (43,200 seconds), so a stalled
     * internal-table read could hold the single capture cycle for half a day and delay
     * every later capture / retry. The read is latency-sensitive: fail fast and let the
     * next cycle retry.
     */
    static final int AUDIT_SCAN_TIMEOUT_SECONDS = 30;

    private static final Logger LOG = LogManager.getLogger(AuditLogScanner.class);

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

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
         * audit writer's zone, see {@link #auditWriteZone()}); null in a tail written
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
        // The bounds are rendered in the zone the AUDIT WRITER used (the global session
        // time_zone, see auditWriteZone) - not the FE host zone - because
        // __internal_schema.audit_log.time stores the writer's rendering. A PENDING
        // window keeps the zone recorded in its cursor: the window's epoch bounds are
        // re-rendered every cycle while the cursor is the persisted string, so a global
        // time_zone change mid-window would otherwise compare two different renderings
        // and skip the whole unconsumed range. Only a NEW window follows a changed
        // global zone.
        ZoneId auditZone = scanZoneFor(cursorTail);
        String start = formatTimestamp(startTimeMs, auditZone);
        String end = formatTimestamp(endTimeMs, auditZone);
        // defense in depth: a non-positive batch size can no longer be written through
        // SQL SET (see SessionVariable), but LIMIT 0 here would mark the window exhausted
        // on an empty page and advance the watermark over every eligible row
        int limit = Math.max(1, maxBatchSize);

        SessionVariable global = VariableMgr.getDefaultSessionVariable();
        long minQueryTimeMs = global.getPlanCaptureMinQueryTimeMs();
        long minScanRows = global.getPlanCaptureMinScanRows();
        String sql = buildScanSql(start, end, limit, minQueryTimeMs, minScanRows,
                cursorPredicate(cursorQueryTime, cursorTime, cursorQueryId, cursorTail));

        // bounded statement timeout: see AUDIT_SCAN_TIMEOUT_SECONDS (the no-timeout
        // overload would inherit the 12h analyze timeout)
        List<ResultRow> rows = StatisticsUtil.execStatisticQuery(sql, false,
                AUDIT_SCAN_TIMEOUT_SECONDS);
        return toBatch(rows, limit, auditZone);
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
     * event time with {@link TimeUtils}, which on its own (context-less) worker thread
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
        try {
            return ZoneId.of(tail.getZoneId(), TimeUtils.timeZoneAliasMap);
        } catch (RuntimeException e) {
            LOG.warn("SPM audit scan ignores an unparsable cursor zone '{}': {}",
                    tail.getZoneId(), e.getMessage());
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
     * keeps its rendering, see {@link CursorTail#getZoneId()}).
     *
     * @param rows         the raw rows of one page
     * @param maxBatchSize the batch limit (a shorter page exhausts the window)
     * @param auditZone    the zone the window bounds were rendered in
     * @return the scan batch
     */
    static ScanBatch toBatch(List<ResultRow> rows, int maxBatchSize, ZoneId auditZone) {
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
     */
    private static String dedupIdentity(String stmt, String digest, long sqlMode) {
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
        if (generators.isEmpty() && selectors.isEmpty()) {
            return digest;
        }
        return digest + '\u0001' + generators + '\u0001' + selectors;
    }

    /** Whether the statement can carry a concrete scan selector (partition / tablet / ...). */
    private static boolean mentionsScanSelector(String stmt) {
        if (stmt == null) {
            return false;
        }
        String upper = stmt.toUpperCase(java.util.Locale.ROOT);
        return upper.contains("PARTITION") || upper.contains("TABLET")
                || upper.contains("TABLESAMPLE") || upper.contains("INDEX")
                || upper.contains("FOR TIMESTAMP");
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

    private static boolean mentionsGenerator(String stmt) {
        if (stmt == null) {
            return false;
        }
        String upper = stmt.toUpperCase(java.util.Locale.ROOT);
        return upper.contains("LATERAL VIEW") || upper.contains("UNNEST");
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
        // The window lower bound is COMPLETION-aware: audit_log.time is the query's START
        // time, but its row is published only when the query FINISHES. A long-running
        // query started at 11:50 is absent from the 12:00 scan; without the
        // completion predicate the next (default three-hour) window starts at
        // 12:00 - overlap, so its 11:50 row - now visible - would be excluded
        // FOREVER. Rows are therefore also eligible while their completion
        // (time + query_time) reaches into the window.
        // The completion branch is BOUNDED by a floor: an unbounded
        // "time >= start OR completion >= start" cannot prune ANY old partition of the
        // range-partitioned audit table (query_time is only known per row), so every
        // keyset page would rescan retained history under the short timeout. The floor
        // (start - LATE_COMPLETION_LOOKBACK_MILLIS) keeps the pruning intact for a
        // three-hour window while still admitting every query whose completion reaches
        // into it; a query LONGER than the lookback is the documented miss.
        String floor = completeWindowFloor(start);
        return "SELECT " + SELECT_COLUMNS + " FROM __internal_schema.audit_log "
                + "WHERE `time` >= '" + floor + "' "
                + "AND (`time` >= '" + start + "'"
                + " OR timestampadd(SECOND, CAST(`query_time` / 1000 AS BIGINT), `time`)"
                + " >= '" + start + "') AND `time` < '" + end + "' "
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
     * The partitionable floor of the completion-aware lower bound: the window start minus
     * {@link #LATE_COMPLETION_LOOKBACK_MILLIS}, rendered like the window bounds. An
     * unparsable timestamp keeps the start itself (never a LESS bounded range).
     */
    private static String completeWindowFloor(String start) {
        try {
            return java.time.LocalDateTime.parse(start, DATETIME_FORMAT)
                    .minus(java.time.Duration.ofMillis(LATE_COMPLETION_LOOKBACK_MILLIS))
                    .format(DATETIME_FORMAT);
        } catch (RuntimeException e) {
            return start;
        }
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

    /** Formats a scan bound in the given zone (see {@link #auditWriteZone()}). */
    static String formatTimestamp(long epochMillis, ZoneId zone) {
        LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), zone);
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
