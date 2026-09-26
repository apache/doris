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

import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.statistics.repository.ResultRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Audit scan pagination / dedup contract tests.
 *
 * - Dedup is namespace-aware: (catalog, db, digest), because SPM's eventual match key is
 *   namespace-qualified - identical unqualified SQL in two databases is two queries, and
 *   collapsing them would starve the other namespace forever.
 * - Pagination uses a stable (query_time, time, query_id) cursor: the batch LIMIT must
 *   never advance the window past unscanned rows (the old behavior advanced the watermark
 *   to the window end and permanently skipped every row beyond the LIMIT).
 */
public class AuditLogScannerCursorTest {

    /** One raw audit_log row in the SELECT-column order of AuditLogScanner. */
    private static ResultRow row(String stmt, long queryTime, String digest,
            String db, String catalog, String queryId, String time) {
        return rowRaw(stmt, String.valueOf(queryTime), digest, db, catalog, queryId, time);
    }

    /** One raw audit_log row with a RAW query_time value (may be null). */
    private static ResultRow rowRaw(String stmt, String queryTimeRaw, String digest,
            String db, String catalog, String queryId, String time) {
        List<String> values = new ArrayList<>();
        values.add(stmt);                        // 0 stmt
        values.add(queryTimeRaw);                // 1 query_time
        values.add("100");                       // 2 scan_rows
        values.add("10");                        // 3 return_rows
        values.add(digest);                      // 4 sql_digest
        values.add("hash");                      // 5 sql_hash
        values.add(db);                          // 6 db
        values.add(catalog);                     // 7 catalog
        values.add(queryId);                     // 8 query_id
        values.add("false");                     // 9 is_internal
        values.add(time);                        // 10 time
        return new ResultRow(values);
    }

    @Test
    public void testDedupKeepsNamespacesApart() {
        // same stmt / digest, two databases: both must survive as separate candidates,
        // each represented by its longest-running execution
        List<ResultRow> rows = List.of(
                row("select * from t", 1000, "d1", "db1", "internal", "q1", "2026-01-01 00:00:00"),
                row("select * from t", 1000, "d1", "db2", "internal", "q2", "2026-01-01 00:00:01"),
                row("select * from t", 5000, "d1", "db1", "internal", "q3", "2026-01-01 00:00:02"));

        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(rows, 10);
        List<CapturedQuery> candidates = batch.getCandidates();
        Assertions.assertEquals(2, candidates.size(),
                "identical SQL in two databases is two queries: " + candidates);
        CapturedQuery db1 = candidates.stream().filter(c -> "db1".equals(c.getDb()))
                .findFirst().orElseThrow(AssertionError::new);
        CapturedQuery db2 = candidates.stream().filter(c -> "db2".equals(c.getDb()))
                .findFirst().orElseThrow(AssertionError::new);
        Assertions.assertEquals(5000L, db1.getQueryTimeMs(),
                "the namespace keeps its longest-running row");
        Assertions.assertEquals(1000L, db2.getQueryTimeMs());
        Assertions.assertEquals("q3", db1.getQueryId());
    }

    @Test
    public void testDedupKeepsCatalogsApart() {
        List<ResultRow> rows = List.of(
                row("select * from t", 1000, "d1", "db1", "cat1", "q1", "2026-01-01 00:00:00"),
                row("select * from t", 2000, "d1", "db1", "cat2", "q2", "2026-01-01 00:00:01"));
        Assertions.assertEquals(2, AuditLogScanner.toBatch(rows, 10).getCandidates().size(),
                "the catalog takes part in the dedup key as well");
    }

    @Test
    public void testCursorIsLastRawRowEvenWhenUnusable() {
        // the trailing row has an empty statement (dropped as a candidate) but has been
        // CONSUMED: the next page must resume after it, never rescan it
        List<ResultRow> rows = List.of(
                row("select * from t", 2000, "d1", "db1", "internal", "qA", "2026-01-01 00:00:00"),
                row("", 100, "", "db1", "internal", "qB", "2026-01-01 00:00:01"));

        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(rows, 10);
        Assertions.assertEquals(1, batch.getCandidates().size(),
                "an empty statement is not a candidate");
        Assertions.assertEquals(100L, batch.getCursorQueryTime());
        Assertions.assertEquals("2026-01-01 00:00:01", batch.getCursorTime());
        Assertions.assertEquals("qB", batch.getCursorQueryId());
    }

    @Test
    public void testWindowExhaustionSignal() {
        List<ResultRow> twoRows = List.of(
                row("select * from t", 2000, "d1", "db1", "internal", "qA", "2026-01-01 00:00:00"),
                row("select * from t", 1000, "d2", "db1", "internal", "qB", "2026-01-01 00:00:01"));

        Assertions.assertTrue(AuditLogScanner.toBatch(List.of(), 10).isWindowExhausted(),
                "an empty page exhausts the window");
        Assertions.assertTrue(AuditLogScanner.toBatch(twoRows, 10).isWindowExhausted(),
                "a page shorter than the limit exhausts the window");
        Assertions.assertFalse(AuditLogScanner.toBatch(twoRows, 2).isWindowExhausted(),
                "a full page is TRUNCATED: the window must be kept and resumed by cursor");
    }

    @Test
    public void testCursorPredicateIsStrictlyAfterAndEscaped() {
        Assertions.assertEquals("", AuditLogScanner.cursorPredicate(
                AuditLogScanner.CURSOR_ABSENT, "", ""),
                "only the absent sentinel means 'no cursor': start at the top of the window");
        // an empty time / query_id means SQL NULL, NOT "no cursor": clearing the
        // predicate here restarted a truncated window at its first page on every cycle
        String nullTime = AuditLogScanner.cursorPredicate(100, "", "q");
        Assertions.assertNotEquals("", nullTime,
                "an empty time is a NULL tie-breaker, not a missing cursor");
        Assertions.assertTrue(nullTime.contains("`time` IS NULL"), nullTime);
        Assertions.assertTrue(nullTime.contains("`query_id` < 'q'"), nullTime);
        String nullQueryId = AuditLogScanner.cursorPredicate(100, "t", "");
        Assertions.assertTrue(nullQueryId.contains("`time` = 't'")
                && nullQueryId.contains("`query_id` IS NULL"), nullQueryId);

        String predicate = AuditLogScanner.cursorPredicate(
                123, "2026-01-01 00:00:00", "q'1");
        Assertions.assertTrue(predicate.contains("`query_time` < 123"), predicate);
        Assertions.assertTrue(predicate.contains("`query_time` = 123"), predicate);
        Assertions.assertTrue(predicate.contains("`time` = '2026-01-01 00:00:00'"), predicate);
        Assertions.assertTrue(predicate.contains("`query_id` < 'q''1'"),
                "a quote inside the query id must be escaped: " + predicate);
        Assertions.assertTrue(predicate.startsWith(" AND "), predicate);
    }

    /**
     * A FULL page whose last raw row carries a NULL time (or a NULL query_id) must still
     * resume: the NULL is encoded in the total-order predicate, otherwise the fixed
     * pending window restarts at its first page every cycle and the later eligible rows
     * are never reached.
     */
    @Test
    public void testNullTieBreakersResumePastFirstPage() {
        List<ResultRow> nullTimePage = List.of(
                rowRaw("select * from t", "100", "d1", "db1", "internal",
                        "q1", "2026-01-01 00:00:02"),
                rowRaw("select * from t", "100", "d2", "db1", "internal",
                        "q2", null));
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(nullTimePage, 2);
        Assertions.assertFalse(batch.isWindowExhausted(), "a full page is truncated");
        Assertions.assertEquals("", batch.getCursorTime(),
                "a NULL time must stay distinguishable (empty means NULL)");
        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId());
        Assertions.assertNotEquals("", predicate,
                "a NULL time must not clear the resume predicate (window restart)");
        Assertions.assertTrue(predicate.contains("`time` IS NULL"), predicate);
        Assertions.assertTrue(predicate.contains("`query_id` < 'q2'"), predicate);

        List<ResultRow> nullQueryIdPage = List.of(
                rowRaw("select * from t", "100", "d3", "db1", "internal",
                        "q3", "2026-01-01 00:00:03"),
                rowRaw("select * from t", "100", "d4", "db1", "internal",
                        null, "2026-01-01 00:00:03"));
        batch = AuditLogScanner.toBatch(nullQueryIdPage, 2);
        predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId());
        Assertions.assertNotEquals("", predicate,
                "a NULL query_id must not clear the resume predicate (window restart)");
        Assertions.assertTrue(predicate.contains("`query_id` IS NULL"), predicate);

        String resumed = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 03:00:00", 500, 1000, 100000, predicate);
        Assertions.assertTrue(resumed.contains("`query_id` IS NULL"),
                "the resumed page carries the NULL tie-breaker: " + resumed);
    }

    @Test
    public void testScanSqlCarriesTotalOrderAndCursor() {
        String sql = AuditLogScanner.buildScanSql(
                "2026-01-01 00:00:00", "2026-01-01 03:00:00", 500, 1000, 100000);
        Assertions.assertTrue(
                sql.contains("ORDER BY `query_time` DESC, `time` DESC, `query_id` DESC"),
                "the cursor walks a stable total order: " + sql);
        Assertions.assertTrue(sql.contains("LIMIT 500"), sql);

        String resumed = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 03:00:00", 500, 1000, 100000,
                AuditLogScanner.cursorPredicate(9, "2026-01-01 01:00:00", "q"));
        Assertions.assertTrue(resumed.contains("`query_time` < 9"),
                "the resumed page continues exactly after the cursor: " + resumed);
        Assertions.assertTrue(
                resumed.contains("ORDER BY `query_time` DESC, `time` DESC, `query_id` DESC"),
                resumed);
    }

    @Test
    public void testWatermarkMovesOnlyForExhaustedWindows() {
        Assertions.assertEquals(100L,
                PlanCaptureManager.nextScanTimestamp(100L, 500L, false),
                "a truncated window keeps its watermark: the cursor resumes inside it");
        Assertions.assertEquals(500L,
                PlanCaptureManager.nextScanTimestamp(100L, 500L, true),
                "an exhausted window advances the watermark to the window end");
    }

    // ==================== truncated windows keep their bounds ====================

    /**
     * A truncated window must be paged to its end: the next cycle has to keep BOTH bounds
     * of the pending window. Deriving a fresh interval window would start around the
     * previous window's end (currentTime - interval) and permanently skip every row the
     * cursor has not reached yet.
     */
    @Test
    public void testTruncatedWindowBoundsStayFixed() {
        long[] pending = PlanCaptureManager.resolveScanWindow(
                0L, 1000L, 2000L, 99_000L, 60_000L, 5_000L);
        Assertions.assertArrayEquals(new long[] {1000L, 2000L}, pending,
                "a pending window keeps its start AND end across cycles");

        long[] first = PlanCaptureManager.resolveScanWindow(
                0L, 0L, 0L, 99_000L, 60_000L, 5_000L);
        Assertions.assertArrayEquals(new long[] {39_000L, 99_000L}, first,
                "without a pending window the first cycle scans one interval");

        long[] resumed = PlanCaptureManager.resolveScanWindow(
                50_000L, 0L, 0L, 99_000L, 60_000L, 5_000L);
        Assertions.assertArrayEquals(new long[] {45_000L, 99_000L}, resumed,
                "without a pending window the watermark (with overlap) starts the window");
    }

    // ==================== zero / NULL query_time cursors ====================

    /**
     * query_time is nullable and eligibility also accepts large scan_rows alone, so a
     * full page can legitimately end with a row whose query_time is 0. The old guard
     * (cursorQueryTime <= 0) cleared the resume predicate, restarted at the first page and
     * every row beyond the LIMIT stayed unreachable.
     */
    @Test
    public void testZeroQueryTimeCursorIsPresent() {
        ResultRow zeroRow = rowRaw("select * from t", "0", "d1", "db1", "internal",
                "qZero", "2026-01-01 00:00:00");
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(List.of(zeroRow), 10);
        Assertions.assertEquals(0L, batch.getCursorQueryTime(),
                "a zero query_time is a VALID cursor value");

        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId());
        Assertions.assertNotEquals("", predicate,
                "a zero query_time must not clear the resume predicate");
        Assertions.assertTrue(predicate.contains("`query_time` = 0"), predicate);
        String resumed = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 03:00:00", 500, 1000, 10000, predicate);
        Assertions.assertTrue(resumed.contains("`query_time` = 0"),
                "the resumed page continues after the zero-query_time cursor: " + resumed);
    }

    /**
     * A NULL query_time must stay distinguishable from a zero one: the resume predicate
     * compares it three-valued through IS NULL, so the rows after a NULL cursor are
     * neither skipped nor re-scanned.
     */
    @Test
    public void testNullQueryTimeCursorIsPresent() {
        ResultRow nullRow = rowRaw("select * from t", null, "d1", "db1", "internal",
                "qNull", "2026-01-01 00:00:01");
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(List.of(nullRow), 10);
        Assertions.assertEquals(AuditLogScanner.CURSOR_QUERY_TIME_NULL,
                batch.getCursorQueryTime(),
                "a NULL query_time must stay distinguishable from a zero value");

        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId());
        Assertions.assertNotEquals("", predicate, "a NULL cursor is a real cursor");
        Assertions.assertTrue(predicate.contains("`query_time` IS NULL"), predicate);
        Assertions.assertTrue(predicate.contains("`time` < '2026-01-01 00:00:01'"), predicate);
        Assertions.assertFalse(predicate.contains("`query_time` < "),
                "the NULL branch must not compare numerically: " + predicate);

        // a numeric cursor also covers the NULL rows: NULLs sort after every non-null
        // value under the scan's raw `query_time` DESC order
        Assertions.assertTrue(AuditLogScanner.cursorPredicate(100, "t", "q")
                        .contains("`query_time` IS NULL"),
                "rows with NULL query_time must be reachable behind a numeric cursor");
    }

    /**
     * Presence is tracked by the CURSOR_ABSENT sentinel, NOT by the numeric value: only
     * the sentinel means "no cursor".
     */
    @Test
    public void testAbsentCursorSentinel() {
        Assertions.assertEquals("", AuditLogScanner.cursorPredicate(
                AuditLogScanner.CURSOR_ABSENT, "2026-01-01 00:00:00", "q"),
                "the sentinel means 'no cursor': start at the top of the window");
        Assertions.assertEquals(AuditLogScanner.CURSOR_ABSENT,
                AuditLogScanner.toBatch(List.of(), 10).getCursorQueryTime(),
                "an empty page reports the absent sentinel");
    }

    /**
     * The audit_log sql_mode must be projected, decoded and carried on the candidate:
     * the capture builds the baseline under the ORIGINATING mode (a literal "a || b" is
     * CONCAT under PIPES_AS_CONCAT, a boolean OR otherwise).
     */
    @Test
    public void testSqlModeIsSelectedDecodedAndCarried() {
        String sql = AuditLogScanner.buildScanSql(
                "2026-01-01 00:00:00", "2026-01-01 01:00:00", 100, 0, 0);
        Assertions.assertTrue(sql.contains("`sql_mode`"),
                "the scan must project the originating parser mode: " + sql);

        List<String> values = new ArrayList<>();
        values.add("select a || b from t1"); // 0 stmt
        values.add("1000");                  // 1 query_time
        values.add("100");                   // 2 scan_rows
        values.add("10");                    // 3 return_rows
        values.add("d1");                    // 4 sql_digest
        values.add("hash");                  // 5 sql_hash
        values.add("db1");                   // 6 db
        values.add("internal");              // 7 catalog
        values.add("q1");                    // 8 query_id
        values.add("false");                 // 9 is_internal
        values.add("2026-01-01 00:00:00");   // 10 time
        values.add("PIPES_AS_CONCAT");       // 11 sql_mode
        CapturedQuery candidate = AuditLogScanner.toBatch(
                List.of(new ResultRow(values)), 10).getCandidates().get(0);
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT, candidate.getSqlMode(),
                "the decoded mode must ride on the candidate");

        Assertions.assertEquals(SqlModeHelper.MODE_DEFAULT,
                AuditLogScanner.decodeAuditSqlMode(""),
                "an empty mode means the default (pre-mode audit rows)");
        Assertions.assertEquals(SqlModeHelper.MODE_DEFAULT,
                AuditLogScanner.decodeAuditSqlMode("NO_SUCH_MODE"),
                "an unsupported mode text falls back to the default");
        Assertions.assertEquals(SqlModeHelper.MODE_PIPES_AS_CONCAT,
                AuditLogScanner.decodeAuditSqlMode(
                        String.valueOf(SqlModeHelper.MODE_PIPES_AS_CONCAT)),
                "a numeric mode is accepted too (persisted retry-queue entries)");
    }
}
