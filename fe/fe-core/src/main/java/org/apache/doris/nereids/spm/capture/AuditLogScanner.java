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
 * Within a capture cycle the results are deduplicated by sql_digest (design doc 7.2.5):
 * the record with the largest query_time wins, so the same query shape is only
 * processed once per cycle.
 */
public class AuditLogScanner {

    private static final DateTimeFormatter DATETIME_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** audit_log SELECT columns (order must match rowToCapturedQuery). */
    private static final String SELECT_COLUMNS =
            "`stmt`, `query_time`, `scan_rows`, `return_rows`, `sql_digest`, `sql_hash`, `db`, `catalog`,"
                    + " `query_id`";

    /**
     * Scans the audit_log table within the given time window.
     *
     * @param startTimeMs  window start (epoch millis, inclusive)
     * @param endTimeMs    window end (epoch millis, exclusive)
     * @param maxBatchSize max number of raw rows to scan (prevents OOM)
     * @return deduplicated candidates sorted by query_time descending
     */
    public List<CapturedQuery> scan(long startTimeMs, long endTimeMs, int maxBatchSize) {
        String start = formatTimestamp(startTimeMs);
        String end = formatTimestamp(endTimeMs);

        long minQueryTimeMs = VariableMgr.getDefaultSessionVariable()
                .getPlanCaptureMinQueryTimeMs();
        String sql = "SELECT " + SELECT_COLUMNS + " FROM __internal_schema.audit_log "
                + "WHERE `time` >= '" + start + "' AND `time` < '" + end + "' "
                + "AND `is_query` = true "
                + "AND `is_nereids` = true "
                + "AND `query_time` >= " + minQueryTimeMs + " "
                + "ORDER BY `query_time` DESC "
                + "LIMIT " + maxBatchSize;

        List<ResultRow> rows = StatisticsUtil.execStatisticQuery(sql);
        if (rows == null || rows.isEmpty()) {
            return List.of();
        }

        // dedup by sql_digest within the batch, keeping the fastest (largest query_time)
        // representative, while preserving the query_time-descending order
        Map<String, CapturedQuery> deduped = new LinkedHashMap<>();
        for (ResultRow row : rows) {
            CapturedQuery candidate = rowToCapturedQuery(row);
            if (candidate == null || candidate.getStmt() == null || candidate.getStmt().isEmpty()) {
                continue;
            }
            String digest = candidate.getSqlDigest();
            if (digest == null || digest.isEmpty()) {
                digest = candidate.getStmt();
            }
            deduped.merge(digest, candidate, (a, b) -> b.getQueryTimeMs() >= a.getQueryTimeMs() ? b : a);
        }
        return new ArrayList<>(deduped.values());
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
    private CapturedQuery rowToCapturedQuery(ResultRow row) {
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
            return new CapturedQuery(stmt, queryTime, scanRows, returnRows, sqlDigest, sqlHash, db, catalog,
                    queryId);
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
