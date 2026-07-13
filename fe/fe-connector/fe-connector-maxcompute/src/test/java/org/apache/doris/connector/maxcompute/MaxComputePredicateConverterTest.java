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

package org.apache.doris.connector.maxcompute;

import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.pushdown.ConnectorAnd;
import org.apache.doris.connector.api.pushdown.ConnectorColumnRef;
import org.apache.doris.connector.api.pushdown.ConnectorComparison;
import org.apache.doris.connector.api.pushdown.ConnectorExpression;
import org.apache.doris.connector.api.pushdown.ConnectorIn;
import org.apache.doris.connector.api.pushdown.ConnectorLiteral;
import org.apache.doris.connector.api.pushdown.ConnectorOr;

import com.aliyun.odps.OdpsType;
import com.aliyun.odps.table.optimizer.predicate.Predicate;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Guards {@link MaxComputePredicateConverter}'s DATETIME / TIMESTAMP / TIMESTAMP_NTZ predicate
 * push-down formatting (FIX-DATETIME-PUSHDOWN-FORMAT, GAP0/1). The connector module has no
 * fe-core / Mockito, so the converter is exercised directly with hand-built
 * {@link ConnectorExpression}s — no network or live ODPS.
 *
 * <p><b>Why this matters.</b> The literal value for a datetime column arrives as a
 * {@link LocalDateTime} (from fe-core's {@code ExprToConnectorExpressionConverter.convertDateLiteral}).
 * It must be pushed to ODPS as a space-separated, fixed-precision string in UTC, converted from the
 * <em>session</em> time zone — exactly as legacy {@code MaxComputeScanNode.convertLiteralToOdpsValues}
 * did. Two regressions are pinned here:</p>
 * <ul>
 *   <li><b>delta-1 (format):</b> the previous {@code String.valueOf(value)} emitted
 *       {@link LocalDateTime#toString()}'s 'T'-separated, variable-precision form
 *       ({@code "2023-02-02T00:00"}), which the space-separated formatter could not parse — so the
 *       whole conjunct tree silently degraded to {@link Predicate#NO_PREDICATE} (predicate never
 *       pushed = full scan) on a non-UTC session, or pushed a malformed literal on a UTC session.</li>
 *   <li><b>delta-2 (timezone):</b> the source time zone must be the session TZ
 *       ({@code ConnectorSession.getTimeZone()}), not the project-region TZ; using the wrong base
 *       shifts the pushed UTC literal and silently loses rows.</li>
 * </ul>
 */
public class MaxComputePredicateConverterTest {

    private static final String UTC = "UTC";
    private static final String SHANGHAI = "Asia/Shanghai"; // fixed +08:00, no DST
    // Doris accepts SET time_zone='CST' and stores it verbatim (mapping it to +08:00 via its own
    // alias map), but java.time.ZoneId.of("CST") throws ZoneRulesException.
    private static final String CST = "CST";

    private static Map<String, OdpsType> typeMap() {
        Map<String, OdpsType> m = new HashMap<>();
        m.put("dt", OdpsType.DATETIME);
        m.put("ts", OdpsType.TIMESTAMP);
        m.put("ntz", OdpsType.TIMESTAMP_NTZ);
        m.put("id", OdpsType.INT);
        m.put("amount", OdpsType.INT);
        return m;
    }

    private static MaxComputePredicateConverter converter(boolean pushDown, String sourceTzId) {
        return new MaxComputePredicateConverter(typeMap(), pushDown, sourceTzId);
    }

    private static ConnectorComparison eq(String colName, ConnectorLiteral value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(colName, ConnectorType.of("DATETIME")), value);
    }

    // ---- delta-1: format the LocalDateTime directly (space-separated, fixed precision) ----

    @Test
    public void testDatetimeFormatsWithSpaceSeparatorAndMillis() {
        Predicate p = converter(true, UTC)
                .convert(eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 00:00:00.000\""),
                "DATETIME must push a space-separated, 3-digit-fraction literal; got: " + p);
    }

    @Test
    public void testDatetimeFractionTruncatedToMillis() {
        // nanos = 123456000 (.123456); DATETIME scale 3 truncates to .123, matching legacy
        // getStringValue(DatetimeV2Type(3)) = microsecond / 1000.
        Predicate p = converter(true, UTC).convert(
                eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0, 123456000))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 00:00:00.123\""),
                "DATETIME fraction must truncate to 3 digits; got: " + p);
    }

    @Test
    public void testTimestampFormatsWithMicros() {
        Predicate p = converter(true, UTC).convert(
                eq("ts", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0, 123456000))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 00:00:00.123456\""),
                "TIMESTAMP must push a 6-digit fraction; got: " + p);
    }

    // ---- delta-1: a non-UTC session must NOT drop the predicate (perf-regression repro) ----

    @Test
    public void testNonUtcDatetimeDoesNotDropPredicate() {
        // Before the fix: String.valueOf(LocalDateTime) = "2023-02-02T08:00" -> parse with the
        // space-separated formatter throws -> the whole tree degraded to NO_PREDICATE.
        Predicate p = converter(true, SHANGHAI)
                .convert(eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 8, 0, 0))));
        Assertions.assertNotSame(Predicate.NO_PREDICATE, p,
                "a non-UTC DATETIME predicate must still be pushed down, not dropped");
    }

    // ---- delta-2: the source TZ is the session TZ (DATETIME/TIMESTAMP convert to UTC) ----

    @Test
    public void testDatetimeConvertsSessionTzToUtc() {
        // Shanghai 08:00 -> UTC 00:00. Using the wrong source TZ would shift the literal and lose rows.
        Predicate p = converter(true, SHANGHAI)
                .convert(eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 8, 0, 0))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 00:00:00.000\""),
                "session TZ (Shanghai) 08:00 must convert to UTC 00:00; got: " + p);
    }

    @Test
    public void testTimestampNtzDoesNotConvertTz() {
        // TIMESTAMP_NTZ has no timezone: legacy does NOT convert. Shanghai session, local 08:00
        // must stay 08:00 (only formatted), unlike DATETIME / TIMESTAMP.
        Predicate p = converter(true, SHANGHAI)
                .convert(eq("ntz", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 8, 0, 0))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 08:00:00.000000\""),
                "TIMESTAMP_NTZ must not apply TZ conversion; got: " + p);
    }

    // ---- a datetime leaf must not collapse the whole tree ----

    @Test
    public void testMixedAndTreeNotDropped() {
        ConnectorComparison idEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5));
        // Shanghai 08:00 -> UTC 00:00 (same kept-conjunct check as the dedicated delta-2 test).
        ConnectorAnd and = new ConnectorAnd(Arrays.<ConnectorExpression>asList(idEq,
                eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 8, 0, 0)))));
        Predicate p = converter(true, SHANGHAI).convert(and);
        Assertions.assertNotSame(Predicate.NO_PREDICATE, p);
        Assertions.assertTrue(p.toString().contains("2023-02-02 00:00:00.000"),
                "the AND tree must keep the converted datetime conjunct; got: " + p);
    }

    // ---- IN-list datetime goes through the same formatting path ----

    @Test
    public void testDatetimeInListFormatsEachValue() {
        // convertIn -> formatLiteralValue: each datetime element must be space-separated formatted.
        ConnectorIn in = new ConnectorIn(
                new ConnectorColumnRef("dt", ConnectorType.of("DATETIME")),
                Arrays.<ConnectorExpression>asList(
                        ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0)),
                        ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 3, 3, 0, 0, 0))),
                false);
        String s = converter(true, UTC).convert(in).toString();
        Assertions.assertTrue(
                s.contains("\"2023-02-02 00:00:00.000\"") && s.contains("\"2023-03-03 00:00:00.000\""),
                "each IN-list datetime element must be space-separated formatted; got: " + s);
    }

    // ---- F1: a Doris-valid-but-ZoneId-invalid session zone (e.g. CST) must degrade the datetime
    //      predicate, NOT throw out of planning, and must NOT block non-datetime pushdown ----

    @Test
    public void testUnparseableSessionZoneDegradesDatetimePredicate() {
        // SET time_zone='CST' is accepted by Doris and stored verbatim, but ZoneId.of("CST") throws.
        // Lazy parse inside convert()'s catch -> the datetime predicate degrades to NO_PREDICATE
        // (BE re-filters) instead of failing the whole query (legacy MaxComputeScanNode parity).
        Predicate p = converter(true, CST)
                .convert(eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0))));
        Assertions.assertSame(Predicate.NO_PREDICATE, p);
    }

    @Test
    public void testUnparseableSessionZoneStillPushesNonDatetimePredicate() {
        // A non-datetime predicate never resolves the zone, so it must still push down under a CST
        // session (legacy resolves the zone only inside convertDateTimezone, for datetime literals).
        ConnectorComparison idEq = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5));
        Predicate p = converter(true, CST).convert(idEq);
        Assertions.assertNotSame(Predicate.NO_PREDICATE, p);
        Assertions.assertTrue(p.toString().contains("id"),
                "non-datetime predicate must push under a CST session; got: " + p);
    }

    @Test
    public void testTimestampNtzPushesUnderUnparseableZone() {
        // TIMESTAMP_NTZ does no TZ conversion -> never parses the zone -> pushes even under CST.
        Predicate p = converter(true, CST)
                .convert(eq("ntz", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 8, 0, 0))));
        Assertions.assertTrue(p.toString().contains("\"2023-02-02 08:00:00.000000\""),
                "TIMESTAMP_NTZ must push (no zone parse) even under a CST session; got: " + p);
    }

    // ---- guards ----

    @Test
    public void testNonLocalDateTimeValueDropsPredicate() {
        // Defensive: a non-LocalDateTime value for a datetime column -> throw -> caught -> dropped
        // (mirrors legacy throwing for a non-DateLiteral, which drops the predicate).
        Predicate p = converter(true, UTC).convert(eq("dt", ConnectorLiteral.ofString("2023-02-02 00:00:00")));
        Assertions.assertSame(Predicate.NO_PREDICATE, p);
    }

    @Test
    public void testPushDownDisabledDropsDatetimePredicate() {
        // dateTimePushDown = false -> DATETIME branch falls through -> throw -> dropped (BE filters).
        Predicate p = converter(false, UTC)
                .convert(eq("dt", ConnectorLiteral.ofDatetime(LocalDateTime.of(2023, 2, 2, 0, 0, 0))));
        Assertions.assertSame(Predicate.NO_PREDICATE, p);
    }

    // ---- G2 (FIX-PREDICATE-COLGUARD): a predicate on a column absent from the table schema must
    //      degrade to NO_PREDICATE (legacy MaxComputeScanNode containsKey-guard parity), NOT push a
    //      malformed predicate to ODPS. "ghost" is not in typeMap(). ----

    @Test
    public void testUnknownColumnComparisonDropsPredicate() {
        // Before the fix, formatLiteralValue quoted the value and pushed `ghost == "5"`; now it
        // throws -> convert()'s catch -> NO_PREDICATE (BE re-filters), so no malformed pushdown.
        ConnectorComparison cmp = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("ghost", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5));
        Predicate p = converter(true, UTC).convert(cmp);
        Assertions.assertSame(Predicate.NO_PREDICATE, p,
                "a predicate on an unknown column must be dropped, not pushed malformed");
    }

    @Test
    public void testUnknownColumnInListDropsPredicate() {
        ConnectorIn in = new ConnectorIn(
                new ConnectorColumnRef("ghost", ConnectorType.of("INT")),
                Arrays.<ConnectorExpression>asList(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofLong(2)),
                false);
        Predicate p = converter(true, UTC).convert(in);
        Assertions.assertSame(Predicate.NO_PREDICATE, p,
                "an IN predicate on an unknown column must be dropped, not pushed malformed");
    }

    @Test
    public void testKnownColumnComparisonStillPushed() {
        // Regression guard: the get()!=null path is unaffected — a known column still pushes down.
        ConnectorComparison cmp = new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5));
        Predicate p = converter(true, UTC).convert(cmp);
        Assertions.assertNotSame(Predicate.NO_PREDICATE, p);
        Assertions.assertTrue(p.toString().contains("id"),
                "a known-column predicate must still push down; got: " + p);
    }

    // ---- L9 (FIX-L9): a top-level AND must push its convertible conjuncts even when one conjunct is
    //      unconvertible, instead of dropping the whole filter (perf; BE re-filters). Only the ROOT AND
    //      gets per-conjunct tolerance — OR and nested AND stay all-or-nothing so no rows are lost.
    //      "ghost"/"ghost2" are not in typeMap(), so they are unconvertible. ----

    private static ConnectorComparison intEq(String col, long value) {
        return new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef(col, ConnectorType.of("INT")), ConnectorLiteral.ofLong(value));
    }

    @Test
    public void topLevelAndKeepsConvertibleWhenOneConjunctFails() {
        // Before the fix: id=5 AND ghost=3 degraded the whole tree to NO_PREDICATE (full scan);
        // now id=5 still pushes down and only ghost falls back to BE.
        ConnectorAnd and = new ConnectorAnd(Arrays.<ConnectorExpression>asList(
                intEq("id", 5), intEq("ghost", 3)));
        Predicate p = converter(true, UTC).convert(and);
        Assertions.assertNotSame(Predicate.NO_PREDICATE, p,
                "a convertible conjunct must survive an unconvertible sibling");
        String s = p.toString();
        Assertions.assertTrue(s.contains("id"), "the id conjunct must be pushed; got: " + s);
        Assertions.assertFalse(s.contains("ghost"), "the unconvertible conjunct must be dropped; got: " + s);
    }

    @Test
    public void topLevelAndAllConjunctsFailDropsToNoPredicate() {
        ConnectorAnd and = new ConnectorAnd(Arrays.<ConnectorExpression>asList(
                intEq("ghost", 3), intEq("ghost2", 4)));
        Assertions.assertSame(Predicate.NO_PREDICATE, converter(true, UTC).convert(and),
                "when every conjunct is unconvertible the filter degrades to NO_PREDICATE");
    }

    @Test
    public void topLevelAndSingleSurvivorReturnedWithoutWrappingAnd() {
        // One survivor -> return it directly (not wrapped in CompoundPredicate(AND, [x])).
        Predicate single = converter(true, UTC).convert(new ConnectorAnd(
                Arrays.<ConnectorExpression>asList(intEq("id", 5), intEq("ghost", 3))));
        Predicate bare = converter(true, UTC).convert(intEq("id", 5));
        Assertions.assertEquals(bare.toString(), single.toString(),
                "a single surviving conjunct must equal the bare predicate; got: " + single);
    }

    @Test
    public void nestedAndStaysAllOrNothing() {
        // id=5 AND (amount=6 AND ghost=3): the nested AND is converted whole, so its failure drops the
        // whole nested conjunct -- amount is lost too. Only top-level conjuncts get per-conjunct tolerance.
        ConnectorAnd nested = new ConnectorAnd(Arrays.<ConnectorExpression>asList(
                intEq("amount", 6), intEq("ghost", 3)));
        ConnectorAnd and = new ConnectorAnd(Arrays.<ConnectorExpression>asList(intEq("id", 5), nested));
        String s = converter(true, UTC).convert(and).toString();
        Assertions.assertTrue(s.contains("id"), "top-level id conjunct must push; got: " + s);
        Assertions.assertFalse(s.contains("amount"),
                "a nested AND is all-or-nothing: its convertible sibling is dropped with it; got: " + s);
    }

    @Test
    public void topLevelOrIsNotTolerated() {
        // id=5 OR ghost=3: dropping a disjunct would make the predicate a subset and lose rows, so OR is
        // all-or-nothing -- one unconvertible disjunct drops the whole OR.
        ConnectorOr or = new ConnectorOr(Arrays.<ConnectorExpression>asList(
                intEq("id", 5), intEq("ghost", 3)));
        Assertions.assertSame(Predicate.NO_PREDICATE, converter(true, UTC).convert(or),
                "an unconvertible disjunct must drop the whole OR (no row loss)");
    }

    // ---- L20 (FIX-L20): the comparison operator symbol must come from the ODPS SDK's own
    //      BinaryPredicate.Operator description, not a hand-written table. EQ must push a single "=",
    //      never Java's "==" (which MaxCompute, like SQL, does not accept -> pushdown lost -> full
    //      scan). Legacy MaxComputeScanNode used odpsOp.getDescription(); the migration hand-wrote the
    //      symbols and EQ drifted to "==". "id" is INT in typeMap(), so numeric formatting applies. ----

    private static String pushedComparison(ConnectorComparison.Operator op) {
        ConnectorComparison cmp = new ConnectorComparison(op,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5));
        // RawPredicate.toString() returns the raw pushed string; normalize whitespace (formatLiteralValue
        // pads values with surrounding spaces) so the assertion pins the operator, not the spacing.
        return converter(true, UTC).convert(cmp).toString().trim().replaceAll("\\s+", " ");
    }

    @Test
    public void testEqualsEmitsSingleEqualsNotDoubleEquals() {
        // RED on the pre-fix code, which emitted "id == 5". The ODPS SDK's EQUALS description is "=".
        String raw = converter(true, UTC).convert(new ConnectorComparison(ConnectorComparison.Operator.EQ,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5))).toString();
        Assertions.assertFalse(raw.contains("=="), "EQ must push a single '=', not Java's '=='; got: " + raw);
        Assertions.assertEquals("id = 5", raw.trim().replaceAll("\\s+", " "),
                "EQ must push 'id = 5'; got: " + raw);
    }

    @Test
    public void testAllComparisonOperatorsEmitSdkSymbols() {
        // Pin the whole operator set to the SDK BinaryPredicate.Operator descriptions so a future
        // hand-edit cannot silently drift any symbol again.
        Assertions.assertEquals("id = 5", pushedComparison(ConnectorComparison.Operator.EQ));
        Assertions.assertEquals("id != 5", pushedComparison(ConnectorComparison.Operator.NE));
        Assertions.assertEquals("id < 5", pushedComparison(ConnectorComparison.Operator.LT));
        Assertions.assertEquals("id <= 5", pushedComparison(ConnectorComparison.Operator.LE));
        Assertions.assertEquals("id > 5", pushedComparison(ConnectorComparison.Operator.GT));
        Assertions.assertEquals("id >= 5", pushedComparison(ConnectorComparison.Operator.GE));
    }

    @Test
    public void testEqForNullIsNotPushedDown() {
        // EQ_FOR_NULL ("<=>") has no ODPS BinaryPredicate equivalent: it must degrade to NO_PREDICATE
        // (default -> throw -> caught), never be pushed as a malformed "<=>" RawPredicate. BE re-filters.
        Predicate p = converter(true, UTC).convert(new ConnectorComparison(
                ConnectorComparison.Operator.EQ_FOR_NULL,
                new ConnectorColumnRef("id", ConnectorType.of("INT")), ConnectorLiteral.ofLong(5)));
        Assertions.assertSame(Predicate.NO_PREDICATE, p,
                "EQ_FOR_NULL has no ODPS equivalent and must not be pushed down");
    }

    // ---- P4-3-IN direction regression: the IN-polarity fix pushes `col IN (values)` (column first),
    //      not the reversed form. This had no dedicated test; pin both IN and NOT IN direction. ----

    @Test
    public void testInListEmitsColumnThenValues() {
        ConnectorIn in = new ConnectorIn(
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                Arrays.<ConnectorExpression>asList(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofLong(2)),
                false);
        String s = converter(true, UTC).convert(in).toString().trim().replaceAll("\\s+", " ");
        Assertions.assertEquals("id IN ( 1 , 2 )", s, "IN must push 'col IN (values)'; got: " + s);
    }

    @Test
    public void testNotInListEmitsColumnThenNotIn() {
        ConnectorIn in = new ConnectorIn(
                new ConnectorColumnRef("id", ConnectorType.of("INT")),
                Arrays.<ConnectorExpression>asList(ConnectorLiteral.ofLong(1), ConnectorLiteral.ofLong(2)),
                true);
        String s = converter(true, UTC).convert(in).toString().trim().replaceAll("\\s+", " ");
        Assertions.assertEquals("id NOT IN ( 1 , 2 )", s, "NOT IN must push 'col NOT IN (values)'; got: " + s);
    }
}
