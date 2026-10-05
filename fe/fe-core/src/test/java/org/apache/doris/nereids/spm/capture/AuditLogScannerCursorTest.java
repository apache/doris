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

import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Audit scan pagination / dedup contract tests.
 *
 * - Dedup is namespace-aware: (catalog, db, digest), because SPM's eventual match key is
 *   namespace-qualified - identical unqualified SQL in two databases is two queries, and
 *   collapsing them would starve the other namespace forever.
 * - Pagination uses a stable total-order cursor: the row EVENT time first, then
 *   query_time / query_id plus a durable tie-breaker tail (client_ip, sql_hash, metrics,
 *   statement hash) so rows sharing (time, query_time, query_id) neither loop nor are
 *   skipped. The batch LIMIT must never advance the window past unscanned rows (the old
 *   behavior advanced the watermark to the window end and permanently skipped every row
 *   beyond the LIMIT).
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

    /** One raw audit_log row WITH the appended cursor-tail columns (client_ip + md5). */
    private static ResultRow rowFull(String stmt, String queryTimeRaw, String digest,
            String db, String catalog, String queryId, String time, String clientIp,
            String scanRows, String returnRows, String stmtHash) {
        List<String> values = new ArrayList<>();
        values.add(stmt);                        // 0 stmt
        values.add(queryTimeRaw);                // 1 query_time
        values.add(scanRows);                    // 2 scan_rows
        values.add(returnRows);                  // 3 return_rows
        values.add(digest);                      // 4 sql_digest
        values.add("hash");                      // 5 sql_hash
        values.add(db);                          // 6 db
        values.add(catalog);                     // 7 catalog
        values.add(queryId);                     // 8 query_id
        values.add("false");                     // 9 is_internal
        values.add(time);                        // 10 time
        // 0 = MODE_DEFAULT: a real audit row ALWAYS carries sql_mode, and a NULL last
        // tail key would make the resume chain terminate with 1 = 0
        values.add("0");                         // 11 sql_mode
        values.add(clientIp);                    // 12 client_ip
        values.add(stmtHash);                    // 13 md5(stmt)
        return new ResultRow(values);
    }

    private static AuditLogScanner.CursorTail newTail(String clientIp, String sqlHash,
            String scanRows, String returnRows, String stmtHash) {
        return new AuditLogScanner.CursorTail(clientIp, sqlHash, scanRows, returnRows, stmtHash);
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

    /**
     * round-23 #1: the digest renders every scan selector as PARTITION(?): two slow
     * same-digest statements differing only in the concrete partition must NOT collapse
     * into one candidate - the survivor's baseline could never serve the discarded one
     * (sameScanIdentity requires the partition names to match).
     */
    @Test
    public void testDedupKeepsPartitionVariantsApart() {
        List<ResultRow> rows = List.of(
                row("select * from t partition(p1) join u on t.k = u.k", 5000, "d9", "db1",
                        "internal", "q1", "2026-01-01 00:00:00"),
                row("select * from t partition(p2) join u on t.k = u.k", 5000, "d9", "db1",
                        "internal", "q2", "2026-01-01 00:00:01"));
        Assertions.assertEquals(2, AuditLogScanner.toBatch(rows, 10).getCandidates().size(),
                "two PARTITION variants of one digest are two baselines: "
                        + AuditLogScanner.toBatch(rows, 10).getCandidates());

        // control: the SAME statement (same selector) still dedups to the longest run
        List<ResultRow> same = List.of(
                row("select * from t partition(p1) join u on t.k = u.k", 5000, "d9", "db1",
                        "internal", "q1", "2026-01-01 00:00:00"),
                row("select * from t partition(p1) join u on t.k = u.k", 7000, "d9", "db1",
                        "internal", "q2", "2026-01-01 00:00:01"));
        Assertions.assertEquals(1, AuditLogScanner.toBatch(same, 10).getCandidates().size(),
                "identical statements still collapse");
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
        // a NULL at the LAST key of a PREFIX-ONLY cursor terminates the chain: every
        // ordered column agrees with the cursor row, the group is content-duplicate and
        // re-selecting it (the old behavior) looped forever
        Assertions.assertTrue(nullQueryId.contains("`time` = 't'")
                && nullQueryId.contains("1 = 0"), nullQueryId);

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
        // PREFIX-ONLY cursor (a pre-column row): the NULL query_id is the last key and
        // every ordered column agrees with the cursor row - the rest of the group is
        // content-duplicate, so the chain terminates explicitly instead of looping
        Assertions.assertTrue(predicate.contains("1 = 0"), predicate);

        String resumed = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 03:00:00", 500, 1000, 100000, predicate);
        Assertions.assertTrue(resumed.contains("1 = 0"),
                "the resumed page skips the content-duplicate group explicitly: " + resumed);
    }

    /**
     * The window lower bound must be COMPLETION-aware: audit_log.time is the query's
     * START time, so a long-running query started before the window is published only
     * after it finishes - without the completion predicate the next window's start-time
     * lower bound excludes its row FOREVER.
     */
    @Test
    public void testScanLowerBoundIsCompletionAware() {
        String sql = AuditLogScanner.buildScanSql(
                "2026-01-01 11:55:00", "2026-01-01 15:00:00", 500, 1000, 100000);
        Assertions.assertTrue(sql.contains("timestampadd(MICROSECOND"),
                "the lower bound must also admit rows whose START predates the window but"
                        + " whose COMPLETION reaches into it: " + sql);
        // these bounds arrive as GIVEN strings (the string-form builder); the DERIVED
        // floor is what gets rendered - round-41 #12: at millisecond precision
        Assertions.assertTrue(sql.contains("`time` >= '2026-01-01 11:55:00'"),
                "the start-time bound stays: " + sql);
        Assertions.assertTrue(sql.contains("`time` < '2026-01-01 15:00:00'"),
                "the upper bound stays start-time based: " + sql);
        // round-23 #2: the completion branch is FLOORED, otherwise the OR admits every
        // old query-time partition and the range-partitioned audit table can never prune
        Assertions.assertTrue(sql.contains("`time` >= '2025-12-31 11:55:00.000'"),
                "the completion branch must be bounded so old partitions still prune: " + sql);
    }

    @Test
    public void testScanSqlCarriesTotalOrderAndCursor() {
        String sql = AuditLogScanner.buildScanSql(
                "2026-01-01 00:00:00", "2026-01-01 03:00:00", 500, 1000, 100000);
        Assertions.assertTrue(sql.contains("ORDER BY `time` DESC, `query_time` DESC,"
                        + " `query_id` DESC, `client_ip` DESC, `sql_hash` DESC, `scan_rows` DESC,"
                        + " `return_rows` DESC, md5(`stmt`) DESC"),
                "the cursor walks a genuinely unique total order (event time first,"
                        + " statement hash last): " + sql);
        Assertions.assertTrue(sql.contains("LIMIT 500"), sql);

        String resumed = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 03:00:00", 500, 1000, 100000,
                AuditLogScanner.cursorPredicate(9, "2026-01-01 01:00:00", "q"));
        Assertions.assertTrue(resumed.contains("`query_time` < 9"),
                "the resumed page continues exactly after the cursor: " + resumed);
        Assertions.assertTrue(
                resumed.contains("ORDER BY `time` DESC, `query_time` DESC,"
                        + " `query_id` DESC, `client_ip` DESC, `sql_hash` DESC, `scan_rows` DESC,"
                        + " `return_rows` DESC, md5(`stmt`) DESC"),
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
     * The dedup identity must be at least as fine as the SPM match identity: the
     * ORIGINATING parser mode separates same-text default / PIPES_AS_CONCAT executions,
     * and the CONCRETE generator arguments (which SPM compares exactly) separate two
     * same-digest LATERAL VIEW statements with different split delimiters.
     */
    @Test
    public void testDedupUsesSpmMatchIdentity() {
        String withComma =
                "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a"
                        + " LATERAL VIEW explode(split(t2.s, ',')) e AS c";
        String withSemicolon =
                "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a"
                        + " LATERAL VIEW explode(split(t2.s, ';')) e AS c";
        List<ResultRow> rows = List.of(
                modeRow(withComma, "d1", ""),
                modeRow(withSemicolon, "d1", ""),
                modeRow(withComma, "", "PIPES_AS_CONCAT"));
        Assertions.assertEquals(3,
                AuditLogScanner.toBatch(rows, 10).getCandidates().size(),
                "different parser modes / generator arguments must stay separate candidates");
        // control: identical rows still dedup to one candidate
        Assertions.assertEquals(1,
                AuditLogScanner.toBatch(List.of(modeRow(withComma, "d1", ""),
                        modeRow(withComma, "d1", "")), 10).getCandidates().size());
    }

    /**
     * Two NaN-id executions can differ ONLY by namespace or parser mode within one
     * DATETIMEV2(3) tick; toBatch treats them as separate capture identities, so the
     * ordered cursor must distinguish them too - otherwise the strict after-cursor
     * chain excluded the second row on every later page (and the advanced window lost
     * its baseline permanently).
     */
    @Test
    public void testCursorOrderDistinguishesNamespaceAndModeRows() {
        String sql = AuditLogScanner.buildScanSql("2026-01-01 00:00:00",
                "2026-01-01 01:00:00", 500, 1000, 10000);
        Assertions.assertTrue(sql.contains("`catalog` DESC, `db` DESC, `sql_mode` DESC"),
                "the ordered cursor must distinguish namespace / mode rows too: " + sql);

        String tail = AuditLogScanner.encodeCursorTail(new AuditLogScanner.CursorTail(
                "10.0.0.1", "h1", "100", "10", "m1", "internal", "b", "0"));
        String predicate = AuditLogScanner.cursorPredicate(5L, "2026-01-01 00:00:00",
                "qid", tail);
        Assertions.assertTrue(predicate.contains("`db` < 'b'"),
                "the resume chain must reach a row that differs only by db: " + predicate);
        Assertions.assertTrue(predicate.contains("`sql_mode` < '0'"),
                "... and one that differs only by parser mode: " + predicate);

        // legacy five-element tails keep the prefix-only comparison
        String legacy = "[\"10.0.0.1\",\"h1\",\"100\",\"10\",\"m1\"]";
        Assertions.assertFalse(AuditLogScanner.cursorPredicate(5L, "2026-01-01 00:00:00",
                "qid", legacy).contains("`db`"),
                "a legacy tail has no namespace keys to compare");
    }

    /**
     * The generator fingerprint must parse under the audit row's SQL mode: with
     * NO_BACKSLASH_ESCAPES split '\\a' is backslash + a whereas the default mode reads
     * it as 'a', so parsing both rows in the daemon's ambient mode made their
     * fingerprints equal and one eligible row was discarded - although SPM compares
     * these generator arguments concretely and the two baselines differ.
     */
    @Test
    public void testGeneratorFingerprintParsesUnderTheAuditRowMode() {
        String literal = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a"
                + " LATERAL VIEW explode(split(t2.s, '\\a')) e AS c";
        String plain = "SELECT * FROM t1 JOIN t2 ON t1.a = t2.a"
                + " LATERAL VIEW explode(split(t2.s, 'a')) e AS c";
        List<ResultRow> rows = List.of(
                modeRow(literal, "d-gen", "NO_BACKSLASH_ESCAPES"),
                modeRow(plain, "d-gen", "NO_BACKSLASH_ESCAPES"));
        Assertions.assertEquals(2, AuditLogScanner.toBatch(rows, 10).getCandidates().size(),
                "the two generators are concrete SPM arguments: both rows are eligible");
    }

    /**
     * audit_log.sql_mode is a STRING column: a page ending on a NAMED mode row
     * (PIPES_AS_CONCAT) must carry that TEXT into the cursor, and the resume predicate
     * must compare it as a quoted string literal - the numeric rendering emitted
     * "sql_mode < PIPES_AS_CONCAT" (an identifier, not a literal), so every later page
     * failed with an unknown column and the capture window stayed pinned forever.
     */
    @Test
    public void testNamedModeRowKeepsStringCursor() {
        List<ResultRow> page = List.of(
                modeRow("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a", "d1", "PIPES_AS_CONCAT"),
                modeRow("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a WHERE t2.b = 1", "d2",
                        "PIPES_AS_CONCAT"));
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(page, 1);
        Assertions.assertFalse(batch.isWindowExhausted(), "a full page truncates the window");
        String tail = batch.getCursorTail();
        Assertions.assertNotEquals("", tail, "the named mode travels in the cursor tail");

        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId(), tail);
        Assertions.assertTrue(predicate.contains("`sql_mode` < 'PIPES_AS_CONCAT'"),
                "the named mode must be compared as an escaped string literal: " + predicate);
        Assertions.assertFalse(predicate.contains("`sql_mode` < PIPES_AS_CONCAT"),
                "an unquoted mode name parses as an identifier (unknown column): " + predicate);
    }

    /** One raw audit_log row (12 columns) with statement / digest / sql_mode overridden. */
    private static ResultRow modeRow(String stmt, String digest, String sqlMode) {
        List<String> values = new ArrayList<>();
        values.add(stmt);                    // 0 stmt
        values.add("1000");                  // 1 query_time
        values.add("100");                   // 2 scan_rows
        values.add("10");                    // 3 return_rows
        values.add(digest);                  // 4 sql_digest
        values.add("hash");                  // 5 sql_hash
        values.add("db1");                   // 6 db
        values.add("internal");              // 7 catalog
        values.add("q1");                    // 8 query_id
        values.add("false");                 // 9 is_internal
        values.add("2026-01-01 00:00:00");   // 10 time
        values.add(sqlMode);                 // 11 sql_mode
        return new ResultRow(values);
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

    // ==================== durable cursor tail / late rows / loader lag ====================

    /** A cursor tail must round-trip through its text form (checkpoint persistence). */
    @Test
    public void testCursorTailRoundTrip() {
        String text = AuditLogScanner.encodeCursorTail(
                newTail("10.0.0.1", "h1", "100", "10", "m1"));
        AuditLogScanner.CursorTail decoded = AuditLogScanner.decodeCursorTail(text);
        Assertions.assertNotNull(decoded, text);
        Assertions.assertEquals("10.0.0.1", decoded.getClientIp());
        Assertions.assertEquals("h1", decoded.getSqlHash());
        Assertions.assertEquals("100", decoded.getScanRows());
        Assertions.assertEquals("10", decoded.getReturnRows());
        Assertions.assertEquals("m1", decoded.getStmtHash());

        Assertions.assertEquals("", AuditLogScanner.encodeCursorTail(null),
                "an absent tail encodes as empty text");
        Assertions.assertEquals("", AuditLogScanner.encodeCursorTail(
                newTail(null, null, null, null, null)),
                "an all-NULL tail means 'no tail information' (pre-column row)");
        Assertions.assertNull(AuditLogScanner.decodeCursorTail(""), "empty text = no tail");
        Assertions.assertNull(AuditLogScanner.decodeCursorTail("not-json"),
                "broken text must degrade to a prefix cursor, never throw");
    }

    /**
     * Rows that agree on (time, query_time, query_id) must neither loop nor be skipped:
     * the tail keys continue the comparison INSIDE the group (the old predicate either
     * re-selected the whole NULL-query-id group forever or skipped the duplicates left
     * after the first LIMIT).
     */
    @Test
    public void testIdenticalTuplesContinueWithTail() {
        List<ResultRow> page = List.of(
                rowFull("select * from t", "100", "d1", "db1", "internal", "q1",
                        "2026-01-01 00:00:00", "10.0.0.9", "100", "10", "m3"),
                rowFull("select * from t", "100", "d1", "db1", "internal", "q1",
                        "2026-01-01 00:00:00", "10.0.0.9", "100", "10", "m2"));
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(page, 2);
        Assertions.assertFalse(batch.isWindowExhausted(), "a full page is truncated");
        String tail = batch.getCursorTail();
        Assertions.assertNotEquals("", tail,
                "the cursor carries the full tail of the last raw row: " + tail);

        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId(), tail);
        // the comparison descends through every ordered key and terminates on the
        // statement hash - NOT with the old 'query_id < q1' that skipped the duplicates
        Assertions.assertTrue(predicate.contains("`client_ip` = '10.0.0.9'"), predicate);
        Assertions.assertTrue(predicate.contains("`sql_hash`"), predicate);
        Assertions.assertTrue(predicate.contains("`scan_rows`"), predicate);
        Assertions.assertTrue(predicate.contains("`return_rows`"), predicate);
        Assertions.assertTrue(predicate.contains("md5(`stmt`)"), predicate);
        Assertions.assertTrue(predicate.contains("'m2'"), predicate);
        Assertions.assertFalse(predicate.contains("1 = 0"),
                "the group's remaining rows stay reachable: " + predicate);
    }

    /**
     * Pagination robustness: a UNIQUE row published AFTER page 1 - into the fixed
     * pending window, carrying an event time OLDER than the cursor - must still be
     * reached. The scan sorts by the row EVENT time first, so such a row sorts AFTER the
     * cursor; under the old query_time-first order it sorted BEFORE the cursor and every
     * resumed page excluded it forever.
     */
    @Test
    public void testLateRowSortsAfterCursor() {
        List<ResultRow> page = List.of(
                rowFull("select * from t", "500", "d2", "db1", "internal", "qFirst",
                        "2026-01-01 00:00:02", "10.0.0.2", "100", "10", "mFirst"));
        AuditLogScanner.ScanBatch batch = AuditLogScanner.toBatch(page, 10);
        String predicate = AuditLogScanner.cursorPredicate(batch.getCursorQueryTime(),
                batch.getCursorTime(), batch.getCursorQueryId(), batch.getCursorTail());
        // an older-event-time row (e.g. query_time 900, time 00:01) satisfies the FIRST
        // branch of the chain ... (the cursor time is the raw row string)
        Assertions.assertTrue(predicate.contains("`time` < '2026-01-01 00:00:02'"),
                "an older-event-time row sorts AFTER the cursor: " + predicate);
        // ... and the event time leads the chain (the query_time comparison only applies
        // INSIDE the equal-time group, it can no longer exclude the late row globally)
        Assertions.assertTrue(predicate.indexOf("`time` <") >= 0
                && predicate.indexOf("`time` <") < predicate.indexOf("`query_time` <"),
                "the event time leads the resume chain: " + predicate);
    }

    /**
     * The window overlap must follow the WHOLE upstream publication delay, not just the
     * loader's batch interval: WorkloadRuntimeStatusMgr holds a finished query's audit
     * event until query_audit_log_timeout_ms (or, for external DML, up to
     * be_report_query_statistics_timeout_ms) has passed and the loader polls its queue
     * every QUEUE_POLL_INTERVAL_MILLIS, so a row can become visible that long after its
     * event time. With only the batch interval covered, such a row falls outside every
     * later overlap and is silently never captured (pagination walks the event time
     * DESC, so the pending pages never reach a row above their cursor either).
     */
    @Test
    public void testOverlapFollowsTheUpstreamPublicationDelay() {
        Assertions.assertEquals(300_000L, PlanCaptureManager.scanWindowOverlapMs(30),
                "the base five-minute overlap dominates the default configuration");
        Assertions.assertEquals(300_000L, PlanCaptureManager.scanWindowOverlapMs(0),
                "a misconfigured (zero) interval falls back to the base overlap");

        int originalAuditTimeout = org.apache.doris.common.Config.query_audit_log_timeout_ms;
        int originalStatsTimeout = org.apache.doris.common.Config.be_report_query_statistics_timeout_ms;
        try {
            // a slow event pipeline alone must already widen the horizon
            org.apache.doris.common.Config.query_audit_log_timeout_ms = 600_000;
            Assertions.assertEquals(1_260_000L, PlanCaptureManager.scanWindowOverlapMs(30),
                    "the hold before the loader (query_audit_log_timeout_ms) is covered");
            org.apache.doris.common.Config.query_audit_log_timeout_ms = originalAuditTimeout;
            org.apache.doris.common.Config.be_report_query_statistics_timeout_ms = 900_000;
            Assertions.assertEquals(1_860_000L, PlanCaptureManager.scanWindowOverlapMs(30),
                    "the external-DML statistics wait is covered as well");
            // ... and it still scales with a slow loader batch interval
            Assertions.assertEquals(2_400_000L, PlanCaptureManager.scanWindowOverlapMs(300),
                    "batch interval and upstream hold add up");
        } finally {
            org.apache.doris.common.Config.query_audit_log_timeout_ms = originalAuditTimeout;
            org.apache.doris.common.Config.be_report_query_statistics_timeout_ms = originalStatsTimeout;
        }
    }

    /**
     * Scan bounds are rendered in the zone the AUDIT WRITER used - the global session
     * time_zone (TimeUtils falls back to it on the loader's context-less worker) - not
     * the FE host zone. On a UTC host with time_zone '-08:00' every stored row is 8h
     * off, so host-zone bounds would exclude every row from every window.
     */
    @Test
    public void testScanBoundsUseTheAuditWriterZone() {
        org.apache.doris.qe.SessionVariable global =
                org.apache.doris.qe.VariableMgr.getDefaultSessionVariable();
        String originalZone = global.getTimeZone();
        try {
            global.setTimeZone("+08:00");
            Assertions.assertEquals(java.time.ZoneOffset.ofHours(8),
                    AuditLogScanner.auditWriteZone().getRules()
                            .getOffset(java.time.Instant.EPOCH),
                    "the audit writer's zone must be the global session time_zone");
            // round-41 #12: rendered bounds carry MILLISECOND precision
            Assertions.assertEquals("1970-01-01 08:00:00.000",
                    AuditLogScanner.formatTimestamp(0L, AuditLogScanner.auditWriteZone()));
            global.setTimeZone("UTC");
            Assertions.assertEquals("1970-01-01 00:00:00.000",
                    AuditLogScanner.formatTimestamp(0L, AuditLogScanner.auditWriteZone()));
        } finally {
            global.setTimeZone(originalZone);
        }
    }

    /**
     * A PENDING window keeps the zone its timestamps were rendered in (recorded in the
     * cursor tail): its epoch bounds are re-formatted every cycle while the resume cursor
     * is the persisted string, so a global time_zone change mid-window would otherwise
     * compare two renderings and skip the whole unconsumed range. Only a NEW window (no
     * cursor) follows the changed zone. Legacy tails carry no zone and stay supported.
     */
    @Test
    public void testCursorTailKeepsTheWindowZone() {
        String withZone = AuditLogScanner.encodeCursorTail(new AuditLogScanner.CursorTail(
                "10.0.0.1", "h", "1", "1", "m", "ctl", "db", "0", "+08:00"));
        AuditLogScanner.CursorTail decoded = AuditLogScanner.decodeCursorTail(withZone);
        Assertions.assertEquals("+08:00", decoded.getZoneId(),
                "the zone round-trips through the tail: " + withZone);
        Assertions.assertEquals(java.time.ZoneId.of("+08:00"), AuditLogScanner.scanZoneFor(withZone));

        org.apache.doris.qe.SessionVariable global =
                org.apache.doris.qe.VariableMgr.getDefaultSessionVariable();
        String originalZone = global.getTimeZone();
        try {
            // the global zone changes while the window is pending: the window keeps its
            // own rendering, a NEW window follows the new zone
            global.setTimeZone("UTC");
            Assertions.assertEquals(java.time.ZoneId.of("+08:00"),
                    AuditLogScanner.scanZoneFor(withZone));
            Assertions.assertEquals(java.time.ZoneId.of("UTC"), AuditLogScanner.scanZoneFor(""));
            Assertions.assertEquals(java.time.ZoneId.of("UTC"),
                    AuditLogScanner.scanZoneFor(null));
        } finally {
            global.setTimeZone(originalZone);
        }

        // a legacy tail (no zone element) and an eight-element tail decode without a zone
        String legacy = new com.google.gson.Gson().toJson(
                java.util.Arrays.asList("10.0.0.1", "h", "1", "1", "m"));
        Assertions.assertNull(AuditLogScanner.decodeCursorTail(legacy).getZoneId());
        Assertions.assertEquals(AuditLogScanner.auditWriteZone(),
                AuditLogScanner.scanZoneFor(legacy),
                "a zone-less cursor uses the current audit zone");
    }

    /**
     * A legacy (prefix-only) cursor whose last key inside the equal-prefix group is NULL
     * terminates the chain explicitly; WITH a tail the same group continues on the tail
     * keys instead.
     */
    @Test
    public void testLegacyPrefixCursorVersusTail() {
        String legacy = AuditLogScanner.cursorPredicate(100, "2026-01-01 00:00:03", "");
        Assertions.assertTrue(legacy.contains("1 = 0"), legacy);
        String withTail = AuditLogScanner.cursorPredicate(100, "2026-01-01 00:00:03", "",
                AuditLogScanner.encodeCursorTail(newTail("10.0.0.1", "h", "1", "1", "m")));
        Assertions.assertFalse(withTail.contains("1 = 0"),
                "with a tail the NULL query_id group continues on the tail keys: " + withTail);
        Assertions.assertTrue(withTail.contains("`query_id` IS NULL AND"), withTail);
    }

    // ==================== round-28 #4: DST-safe window bounds ====================

    /**
     * With a fall-back zone a UTC window renders as an INVERTED local range
     * ({@code [01:45, 01:15)} in Los Angeles): the scan matched no row at all, the empty
     * page looked exhausted and the capture advanced its watermark past rows written in
     * the repeated hour. The window is split at the transition into monotone segments, so
     * a row written at 09:05Z (01:05 PST, the SECOND 01:05 of that day) is still inside
     * one of them.
     */
    @Test
    public void testFallBackWindowSplitsIntoMonotoneLocalRanges() {
        ZoneId losAngeles = ZoneId.of("America/Los_Angeles");
        List<String[]> ranges = AuditLogScanner.localTimeRanges(
                Instant.parse("2026-11-01T08:45:00Z").toEpochMilli(),
                Instant.parse("2026-11-01T09:15:00Z").toEpochMilli(), losAngeles);
        Assertions.assertEquals(2, ranges.size(), describe(ranges));
        assertRange(ranges.get(0), "2026-11-01 01:45:00.000", "2026-11-01 02:00:00.000");
        assertRange(ranges.get(1), "2026-11-01 01:00:00.000", "2026-11-01 01:15:00.000");
        for (String[] range : ranges) {
            Assertions.assertTrue(range[0].compareTo(range[1]) < 0,
                    "every segment must be a MONOTONE range: " + describe(ranges));
        }

        // the row at 09:05Z renders as 01:05 (PST) and falls into the second segment
        String rowRendering = AuditLogScanner.formatTimestamp(
                Instant.parse("2026-11-01T09:05:00Z").toEpochMilli(), losAngeles);
        Assertions.assertEquals("2026-11-01 01:05:00.000", rowRendering);
        Assertions.assertTrue(rowRendering.compareTo(ranges.get(1)[0]) >= 0
                        && rowRendering.compareTo(ranges.get(1)[1]) < 0,
                "the late row stays inside a segment: " + describe(ranges));
    }

    /** A spring-forward window splits the same way and simply SKIPS the missing hour. */
    @Test
    public void testSpringForwardWindowSplitsAtTheTransition() {
        List<String[]> ranges = AuditLogScanner.localTimeRanges(
                Instant.parse("2026-03-08T09:45:00Z").toEpochMilli(),
                Instant.parse("2026-03-08T10:15:00Z").toEpochMilli(),
                ZoneId.of("America/Los_Angeles"));
        Assertions.assertEquals(2, ranges.size(), describe(ranges));
        assertRange(ranges.get(0), "2026-03-08 01:45:00.000", "2026-03-08 02:00:00.000");
        assertRange(ranges.get(1), "2026-03-08 03:00:00.000", "2026-03-08 03:15:00.000");
    }

    /** A fixed-offset zone keeps the single range - the pre-existing SQL shape. */
    @Test
    public void testFixedOffsetZoneKeepsASingleRange() {
        List<String[]> ranges = AuditLogScanner.localTimeRanges(
                Instant.parse("2026-11-01T08:45:00Z").toEpochMilli(),
                Instant.parse("2026-11-01T09:15:00Z").toEpochMilli(), ZoneId.of("UTC"));
        Assertions.assertEquals(1, ranges.size(), describe(ranges));
        assertRange(ranges.get(0), "2026-11-01 08:45:00.000", "2026-11-01 09:15:00.000");
    }

    /** The scan SQL carries every segment (OR'd) instead of one inverted range. */
    @Test
    public void testScanSqlCarriesAllWindowSegments() {
        List<String[]> ranges = AuditLogScanner.localTimeRanges(
                Instant.parse("2026-11-01T08:45:00Z").toEpochMilli(),
                Instant.parse("2026-11-01T09:15:00Z").toEpochMilli(),
                ZoneId.of("America/Los_Angeles"));
        String sql = AuditLogScanner.buildScanSql(ranges, 500, 1000, 100000, "", 3600);
        Assertions.assertTrue(
                sql.contains("(`time` >= '2026-11-01 01:45:00.000'"
                        + " AND `time` < '2026-11-01 02:00:00.000')"),
                "the first segment must be a valid range: " + sql);
        Assertions.assertTrue(
                sql.contains(" OR (`time` >= '2026-11-01 01:00:00.000'"
                        + " AND `time` < '2026-11-01 01:15:00.000')"),
                "the repeated-hour segment must be scanned as well: " + sql);
        Assertions.assertFalse(sql.contains("`time` >= '2026-11-01 01:45:00.000' AND `time` <"
                        + " '2026-11-01 01:15:00.000'"),
                "the inverted range must be gone: " + sql);
        // round-41 #5: the top-level partition bound is the GREATEST rendered end (02:00),
        // not the last range's (01:15): the last-end bound discarded every row of the
        // first segment - including an 01:50 row published after the rollback - before
        // the OR could admit it, and the capture then checkpointed past it
        Assertions.assertTrue(sql.contains("`time` < '2026-11-01 02:00:00.000' AND ("),
                "the scan bound must be the greatest segment end: " + sql);
    }

    // ==================== round-28 #7: absolute completion across a transition ====================

    /**
     * The completion-aware lower bound adds the row's ELAPSED seconds to its LOCAL start
     * rendering, which ignores a DST transition in between: a query started 01:30 PST
     * (09:30Z) that finishes 03:10:01 PDT computes as 02:10:01, and a window starting
     * 03:05 excluded it on every later scan. The bound is widened by the zone's maximum
     * offset swing, so the civil arithmetic error can no longer hide an eligible row.
     */
    @Test
    public void testCompletionBoundIsWidenedByTheZoneOffsetSwing() {
        Assertions.assertEquals(0L,
                AuditLogScanner.zoneOffsetSwingSeconds(ZoneId.of("UTC")),
                "a fixed-offset zone needs no widening");
        Assertions.assertEquals(3600L,
                AuditLogScanner.zoneOffsetSwingSeconds(ZoneId.of("America/Los_Angeles")),
                "Los Angeles swings one hour");

        String sql = AuditLogScanner.buildScanSql("2026-03-08 03:05:00",
                "2026-03-08 06:00:00", 500, 1000, 100000, "", 3600L);
        Assertions.assertTrue(sql.contains("CAST(`query_time` AS BIGINT) * 1000 + 3600000000"),
                "the completion must carry the swing (one hour = 3600000000 micros): " + sql);
        String withoutSwing = AuditLogScanner.buildScanSql("2026-03-08 03:05:00",
                "2026-03-08 06:00:00", 500, 1000, 100000, "", 0L);
        Assertions.assertFalse(withoutSwing.contains("+ 3600000000"),
                "a zone without transitions keeps the established SQL: " + withoutSwing);
    }

    private static void assertRange(String[] range, String start, String end) {
        Assertions.assertEquals(start, range[0]);
        Assertions.assertEquals(end, range[1]);
    }

    private static String describe(List<String[]> ranges) {
        StringBuilder text = new StringBuilder();
        for (String[] range : ranges) {
            text.append('[').append(range[0]).append(' ').append(range[1]).append(']');
        }
        return text.toString();
    }
}
