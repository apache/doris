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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * Round-30 #1: the completion-aware window lower bound must actually ADMIT the row it
 * exists for.
 *
 * audit_log.time is the query's START, so a query started 11:50 that only finishes after
 * the 12:00 scan is published into a window that already passed. The scan predicate
 * therefore ORs a completion branch (`time + query_time >= window start`, bounded by a
 * partition-pruning floor) into the lower bound - but the window membership conjunct
 * (`time >= start AND time < end`) was ANDed on top of it, which made the earlier-start
 * branch UNREACHABLE: the next scan starts at 11:55 after the overlap and rejected the
 * 11:50 row on every later scan as well.
 *
 * These tests evaluate the COMPLETE generated Boolean predicate for one audit row (the
 * leaves that are not about the window time - metrics, cursor - are treated as true, a
 * pure read is not the question), so the row example from the review is decided by the
 * same condition text the FE sends to the BE.
 */
public class AuditScanPredicateTest {

    private static final DateTimeFormatter TS =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final String WINDOW_START = "2026-01-01 11:55:00";
    private static final String WINDOW_END = "2026-01-01 15:00:00";

    private static String windowSql() {
        return AuditLogScanner.buildScanSql(WINDOW_START, WINDOW_END, 500, 1000, 100000);
    }

    // ==================== the reviewer's row ====================

    /**
     * Started 11:50, ran 20 minutes, its row published only after the 12:00 scan: the next
     * scan begins at 11:55 (overlap) and must still admit it, otherwise no later window
     * (whose overlap only reaches the newest rows) ever sees it again.
     */
    @Test
    public void testLateCompletionRowStartedBeforeTheWindowIsAdmitted() {
        String sql = windowSql();
        Assertions.assertTrue(matches(sql, "2026-01-01 11:50:00", 20 * 60 * 1000L),
                "a row whose completion reaches into the window must be admitted although"
                        + " its start predates it: " + sql);
    }

    /** A row that ended before the window started is not resurrected by the branch. */
    @Test
    public void testShortQueryBeforeTheWindowStaysExcluded() {
        Assertions.assertFalse(matches(windowSql(), "2026-01-01 11:00:00", 60_000L),
                "the completion branch only admits completions that REACH the window");
    }

    /** The floor keeps the partition pruning intact: rows beyond it are excluded. */
    @Test
    public void testRowsBeforeTheCompletionFloorStayExcluded() {
        Assertions.assertFalse(matches(windowSql(), "2025-12-30 11:50:00", 20 * 60 * 1000L),
                "the completion lookback is bounded, or every old partition would be"
                        + " rescanned");
    }

    /** The upper bound applies to the START time, so later windows keep their rows. */
    @Test
    public void testRowsBeyondTheWindowEndStayExcluded() {
        Assertions.assertFalse(matches(windowSql(), "2026-01-01 16:00:00", 60_000L),
                "a row started after the window belongs to the next window");
    }

    /** Ordinary rows inside the window keep working. */
    @Test
    public void testRowsInsideTheWindowAreAdmitted() {
        Assertions.assertTrue(matches(windowSql(), "2026-01-01 12:30:00", 1000L));
        Assertions.assertTrue(matches(windowSql(), WINDOW_START, 500L),
                "the window start itself is inside");
    }

    /** A row that starts exactly at the window end is the next window's row. */
    @Test
    public void testRowAtTheWindowEndIsExcluded() {
        Assertions.assertFalse(matches(windowSql(), WINDOW_END, 1000L));
    }

    /**
     * A DST-split window is several wall-clock ranges: the completion branch must survive
     * there as well (the same AND nullified it for every range).
     */
    @Test
    public void testSegmentedWindowAdmitsLateCompletionsToo() {
        List<String[]> ranges = AuditLogScanner.localTimeRanges(
                Instant.parse("2026-03-08T09:45:00Z").toEpochMilli(),
                Instant.parse("2026-03-08T10:15:00Z").toEpochMilli(),
                ZoneId.of("America/Los_Angeles"));
        String sql = AuditLogScanner.buildScanSql(ranges, 500, 1000, 100000, "", 0);
        // first segment [2026-03-08 01:45:00, 02:00:00): a query started 01:40 that ran
        // 10 minutes completes at 01:50 and must be admitted
        Assertions.assertTrue(matches(sql, "2026-03-08 01:40:00", 10 * 60 * 1000L),
                "the completion branch must survive the segmented window: " + sql);
        // a row inside the repeated-hour segment is admitted by the start-time membership
        Assertions.assertTrue(matches(sql, "2026-03-08 03:05:00", 1000L), sql);
        // a row before the first segment with a short completion is not admitted
        Assertions.assertFalse(matches(sql, "2026-03-08 01:20:00", 60_000L), sql);
    }

    /**
     * round-32 #13: the completion floor is derived from the window-start INSTANT in the
     * scan zone - subtracting from the civil {@code LocalDateTime} landed an hour off
     * after a spring-forward transition, and the too-late floor rejected a row inside the
     * promised 24 hour lookback on EVERY later scan (the reviewer's ~23h40m query).
     */
    @Test
    public void testCompletionFloorSubtractsFromTheWindowStartInstant() {
        ZoneId zone = ZoneId.of("America/Los_Angeles");
        // 2026-03-08 03:05 PDT (the spring-forward transition was that morning)
        long startMs = Instant.parse("2026-03-08T10:05:00Z").toEpochMilli();
        long endMs = startMs + 30 * 60_000L;
        Assertions.assertEquals("2026-03-07 02:05:00",
                AuditLogScanner.lateCompletionFloor(startMs, zone),
                "24 hours before 03:05 PDT is 02:05 PST, not 03:05 PST");
        List<String[]> ranges = AuditLogScanner.localTimeRanges(startMs, endMs, zone);
        long swing = AuditLogScanner.zoneOffsetSwingSeconds(zone);
        String sql = AuditLogScanner.buildScanSql(ranges, 500, 1000, 100000, "",
                swing, AuditLogScanner.lateCompletionFloor(startMs, zone));
        // started 02:30 PST (10:30Z), ran ~23h40m, completes 03:10 PDT: inside the window
        // and inside the lookback
        long queryTimeMs = 23 * 3600_000L + 40 * 60_000L;
        Assertions.assertTrue(matches(sql, "2026-03-07 02:30:00", queryTimeMs),
                "a row inside the lookback whose completion reaches the window must be"
                        + " admitted: " + sql);
        // the old CIVIL floor (LocalDateTime.minus(1 day)) landed at 03:05 PST and
        // rejected exactly that row - the bug this fixes
        String stale = AuditLogScanner.buildScanSql(ranges, 500, 1000, 100000, "", swing,
                "2026-03-07 03:05:00.000");
        Assertions.assertFalse(matches(stale, "2026-03-07 02:30:00", queryTimeMs),
                "the civil floor excluded the row permanently");
    }

    // ==================== a minimal evaluator of the generated predicate ====================

    /**
     * Evaluates the WHERE clause of the generated scan SQL for one audit row
     *
     * @param sql          the generated statement
     * @param time         the row's `time` (the writer's local rendering)
     * @param queryTimeMs  the row's `query_time` in millis
     * @return whether the row passes the window-time conditions
     */
    private static boolean matches(String sql, String time, long queryTimeMs) {
        int whereAt = sql.indexOf(" WHERE ");
        int orderAt = sql.indexOf(" ORDER BY ");
        Assertions.assertTrue(whereAt > 0 && orderAt > whereAt,
                "the scan SQL must carry a WHERE clause: " + sql);
        return evaluate(sql.substring(whereAt + " WHERE ".length(), orderAt), time,
                queryTimeMs);
    }

    private static boolean evaluate(String condition, String time, long queryTimeMs) {
        String text = stripOuterParentheses(condition.trim());
        List<String> orParts = splitTopLevel(text, " OR ");
        if (orParts.size() > 1) {
            for (String part : orParts) {
                if (evaluate(part, time, queryTimeMs)) {
                    return true;
                }
            }
            return false;
        }
        List<String> andParts = splitTopLevel(text, " AND ");
        if (andParts.size() > 1) {
            for (String part : andParts) {
                if (!evaluate(part, time, queryTimeMs)) {
                    return false;
                }
            }
            return true;
        }
        return evaluateLeaf(text, time, queryTimeMs);
    }

    /** One leaf: the window-time conditions decide, every other column is "true". */
    private static boolean evaluateLeaf(String leaf, String time, long queryTimeMs) {
        if (leaf.contains("timestampadd")) {
            int at = leaf.indexOf(">= '");
            Assertions.assertTrue(at > 0, "unexpected completion leaf: " + leaf);
            String bound = leaf.substring(at + 4, leaf.indexOf('\'', at + 4));
            long swingSeconds = swingSecondsOf(leaf);
            String completion = LocalDateTime.parse(time, TS)
                    .plusSeconds(queryTimeMs / 1000 + swingSeconds)
                    .format(TS);
            return completion.compareTo(bound) >= 0;
        }
        if (leaf.contains("`time`")) {
            int ge = leaf.indexOf(">= '");
            if (ge > 0) {
                String bound = leaf.substring(ge + 4, leaf.indexOf('\'', ge + 4));
                return time.compareTo(bound) >= 0;
            }
            int lt = leaf.indexOf("< '");
            Assertions.assertTrue(lt > 0, "unexpected time leaf: " + leaf);
            String bound = leaf.substring(lt + 3, leaf.indexOf('\'', lt + 3));
            return time.compareTo(bound) < 0;
        }
        // metrics / flags / the resume cursor: not part of the window membership question
        return true;
    }

    /** The extra seconds the completion expression was widened by (0 when absent). */
    private static long swingSecondsOf(String leaf) {
        int at = leaf.indexOf("AS BIGINT");
        if (at < 0) {
            return 0;
        }
        int plus = leaf.indexOf("+ ", at);
        if (plus < 0) {
            return 0;
        }
        int end = plus + 2;
        while (end < leaf.length() && Character.isDigit(leaf.charAt(end))) {
            end++;
        }
        return end == plus + 2 ? 0 : Long.parseLong(leaf.substring(plus + 2, end).trim());
    }

    private static String stripOuterParentheses(String text) {
        while (text.startsWith("(") && text.endsWith(")") && wrapsWholeExpression(text)) {
            text = text.substring(1, text.length() - 1).trim();
        }
        return text;
    }

    private static boolean wrapsWholeExpression(String text) {
        int depth = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '(') {
                depth++;
            } else if (c == ')') {
                depth--;
                if (depth == 0) {
                    return i == text.length() - 1;
                }
            }
        }
        return false;
    }

    /** Splits on a top-level {@code separator} (no enclosing parentheses, no literals). */
    private static List<String> splitTopLevel(String text, String separator) {
        List<String> parts = new ArrayList<>();
        int depth = 0;
        boolean inLiteral = false;
        int from = 0;
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\'') {
                inLiteral = !inLiteral;
            } else if (!inLiteral && c == '(') {
                depth++;
            } else if (!inLiteral && c == ')') {
                depth--;
            } else if (!inLiteral && depth == 0 && c == separator.charAt(0)
                    && text.startsWith(separator, i)) {
                parts.add(text.substring(from, i));
                from = i + separator.length();
                i = from - 1;
            }
        }
        parts.add(text.substring(from));
        return parts;
    }
}
