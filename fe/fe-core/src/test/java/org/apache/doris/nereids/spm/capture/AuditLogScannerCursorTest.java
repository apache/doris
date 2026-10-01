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
        Assertions.assertTrue(sql.contains("timestampadd(SECOND"),
                "the lower bound must also admit rows whose START predates the window but"
                        + " whose COMPLETION reaches into it: " + sql);
        Assertions.assertTrue(sql.contains("`time` >= '2026-01-01 11:55:00'"),
                "the start-time bound stays: " + sql);
        Assertions.assertTrue(sql.contains("`time` < '2026-01-01 15:00:00'"),
                "the upper bound stays start-time based: " + sql);
        // round-23 #2: the completion branch is FLOORED, otherwise the OR admits every
        // old query-time partition and the range-partitioned audit table can never prune
        Assertions.assertTrue(sql.contains("`time` >= '2025-12-31 11:55:00'"),
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
        // branch of the chain ...
        Assertions.assertTrue(predicate.contains("`time` < '2026-01-01 00:00:02'"),
                "an older-event-time row sorts AFTER the cursor: " + predicate);
        // ... and the event time leads the chain (the query_time comparison only applies
        // INSIDE the equal-time group, it can no longer exclude the late row globally)
        Assertions.assertTrue(predicate.indexOf("`time` <") >= 0
                && predicate.indexOf("`time` <") < predicate.indexOf("`query_time` <"),
                "the event time leads the resume chain: " + predicate);
    }

    /**
     * The window overlap must follow the audit loader's publication batch interval: a
     * row becomes visible up to one (worst case two) loader intervals after its event
     * time, and the fixed five-minute overlap does not cover a loader configured beyond
     * it (audit_plugin_max_batch_interval_sec is settable).
     */
    @Test
    public void testOverlapFollowsAuditLoaderInterval() {
        Assertions.assertEquals(300_000L, PlanCaptureManager.scanWindowOverlapMs(30),
                "the base five-minute overlap dominates a fast loader");
        Assertions.assertEquals(600_000L, PlanCaptureManager.scanWindowOverlapMs(300),
                "two five-minute loader intervals are covered");
        Assertions.assertEquals(300_000L, PlanCaptureManager.scanWindowOverlapMs(0),
                "a misconfigured (zero) interval falls back to the base overlap");
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
}
