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

package org.apache.doris.nereids.spm;

import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderSnapshot;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.spm.placeholder.TestSPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Abs;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Concat;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Round-91 review fixes.
 *
 * <ul>
 *   <li>#1 {@code isAllNullMap} did not follow {@code map_filter} - a type-preserving MAP wrapper -
 *       so an all-NULL-valued MAP under it stayed a NullType producer and the Boolean consumer
 *       erased the whole {@code element_at} operand together with its markers.</li>
 *   <li>#4 {@code utc_time()} / {@code utc_date()} were missing from the replay-time context
 *       guard, so the optimizer folded the CREATE clock into the frozen planSql.</li>
 *   <li>#5 a session-time-zone dependent CAST CHAIN over literals slipped past the name-based
 *       guard, so an inline VALUES cell / generator argument / star REPLACE payload froze the
 *       creator's zone value.</li>
 * </ul>
 */
public class SPMRound91GuardTest {

    @Test
    public void testImplicitAiResourceInAsofUsingConditionRequiresRawReplay() {
        String query = "SELECT l.k, r.ts FROM l ASOF LEFT JOIN r "
                + "MATCH_CONDITION(CAST(%s AS DATETIME) >= r.ts) USING(k)";
        Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse(String.format(query, "ai_generate(l.prompt)"))));
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse(String.format(query, "ai_generate('explicit_resource', l.prompt)"))));
    }

    /**
     * The MAP wrapper path: every NULL in the {@code map(...)} VALUE positions keeps a marker, and
     * the NULL producer remains untyped and the Boolean consumer binds its own cast.
     */
    @Test
    public void testMapFilterWrapperKeepsItsValueMarkers() {
        String sql = "SELECT k, k > 0 OR element_at(map_filter(map('x', NULL),"
                + " (m_k, m_v) -> TRUE), 'x') AS c FROM t";
        List<SpmConstVar> markers = markers(sql);
        Assertions.assertFalse(markers.isEmpty(), "the capture must keep markers: " + sql);
        Assertions.assertTrue(markers.stream().anyMatch(marker -> marker.getDataType().isNullType()), sql);
    }

    @Test
    public void testConditionalContainersAndNthValueKeepResultMarkers() {
        for (String operand : List.of(
                "element_at(if(k > 0, array(NULL), array(NULL)), 1)",
                "element_at(case when k > 0 then [NULL] else [NULL] end, 1)",
                "element_at(coalesce(map('x', NULL), map('y', NULL)), 'x')",
                "nth_value(NULL, 1) OVER (ORDER BY k)")) {
            String query = "SELECT k, k > 0 OR " + operand + " AS c FROM t";
            List<SpmConstVar> values = markers(query);
            Assertions.assertTrue(values.stream().anyMatch(marker -> marker.getDataType().isNullType()
                    || marker.getDataType().isArrayType()), query);
        }
    }

    /** {@code utc_time()} and {@code utc_date()} read the statement clock like {@code now()}. */
    @Test
    public void testUtcClockFunctionsAreReplayContextExpressions() {
        Assertions.assertTrue(SPMPlanTreeSupport.containsCreatorContextExpression(
                firstProjection("SELECT utc_time() AS t")),
                "utc_time() is the statement clock evaluated in the UTC zone");
        Assertions.assertTrue(SPMPlanTreeSupport.containsCreatorContextExpression(
                firstProjection("SELECT utc_date() AS d")),
                "utc_date() is the statement clock evaluated in the UTC zone");
        // control: a pure function of its argument stays freezable
        Assertions.assertFalse(SPMPlanTreeSupport.containsCreatorContextExpression(
                firstProjection("SELECT unix_timestamp('2020-01-01 00:00:00') AS u")),
                "unix_timestamp(arg) is a pure function of the argument");
    }

    /**
     * The CAST-only chain: a TIMESTAMPTZ / zone-less transition over literals is folded in the
     * CREATOR's session zone, so an inline VALUES cell carrying it must not be frozen.
     */
    @Test
    public void testZoneSensitiveCastChainInValuesCellIsDetected() {
        String sql = "SELECT * REPLACE (CAST(CAST(CAST('2020-01-01 00:00:00+00:00'"
                + " AS TIMESTAMPTZ) AS DATETIME(6)) AS STRING) AS a) FROM t";
        Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(parse(sql)),
                "a TIMESTAMPTZ cast chain over literals freezes the creator's session zone");
    }

    /** Control: a CAST chain that stays inside zone-less types remains freezable. */
    @Test
    public void testZoneLessCastChainStaysFreezable() {
        String sql = "SELECT * REPLACE (CAST(CAST(1 AS BIGINT) AS STRING) AS a) FROM t";
        Assertions.assertFalse(
                SPMPlanTreeSupport.requiresSessionSensitiveReplay(parse(sql)),
                "a zone-less cast chain is session independent");
    }

    @Test
    public void testConcreteCastPreflightOnUnanalyzedChildren() {
        for (String child : List.of("k", "abs(k)")) {
            LogicalPlan query = parse("SELECT * REPLACE(CAST(" + child + " AS BIGINT) AS k) FROM t");
            Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(query));
        }
        for (String child : List.of("'abc'", "abs(1)")) {
            LogicalPlan query = parse("SELECT * REPLACE(CAST(" + child + " AS INT) AS k) FROM t");
            Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(query));
            Assertions.assertTrue(SPMPlanTreeSupport.containsFoldedStrictCastPayload(query));
        }
    }

    @Test
    public void testConcreteSessionCastCheckLeavesPlaceholderConversionsFreezable() {
        LogicalPlan query = parse("SELECT CAST('abc' AS DOUBLE)");
        Assertions.assertTrue(SPMPlanTreeSupport.containsConcreteSessionSensitiveExpression(query));
        LogicalPlan parameterized = SPMPlanTreeSupport.transform(query, new TestSPMPlaceholderBuilder());
        Assertions.assertFalse(SPMPlanTreeSupport.containsConcreteSessionSensitiveExpression(parameterized));
        Assertions.assertFalse(SPMPlanTreeSupport.containsConcreteSessionSensitiveExpression(
                parse("SELECT CAST(1 AS BIGINT)")));
    }

    @Test
    public void testAnalyzedZoneSensitiveCastUsesBoundTypes() {
        Expression formatted = new Concat(new StringLiteral("2020-01-01"),
                new StringLiteral(" 00:00:00+00:00"));
        // Strict casts cannot rely on the independent non-strict-cast guard.
        Cast timestamp = new Cast(formatted, TimeStampTzType.SYSTEM_DEFAULT, true, true);
        Cast datetime = new Cast(timestamp, DateTimeV2Type.SYSTEM_DEFAULT, true, true);
        Assertions.assertTrue(containsAnalyzedSensitiveExpression(timestamp));
        Assertions.assertTrue(containsAnalyzedSensitiveExpression(datetime));

        Cast number = new Cast(new Abs(new IntegerLiteral(-7)), BigIntType.INSTANCE, true, true);
        Assertions.assertFalse(containsAnalyzedSensitiveExpression(number));
        Cast column = new Cast(new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT),
                TimeStampTzType.SYSTEM_DEFAULT, true, true);
        Assertions.assertFalse(containsAnalyzedSensitiveExpression(column));
        Cast marker = new Cast(SpmConstVar.of(1L, new StringLiteral("2020-01-01 00:00:00")),
                TimeStampTzType.SYSTEM_DEFAULT, true, true);
        Assertions.assertFalse(containsAnalyzedSensitiveExpression(marker));
    }

    private static boolean containsAnalyzedSensitiveExpression(Expression expression) {
        return SPMPlanTreeSupport.containsConcreteSessionSensitiveExpression(
                new LogicalOneRowRelation(new RelationId(1), List.of(new Alias(new ExprId(1), expression, "v"))));
    }

    @Test
    public void testTemporalBuiltinPayloadsRespectCallerSettings() {
        String uuid = "to_uuid_or_zero('00000000-0000-7000-8000-000000000000')";
        for (String value : List.of("from_second(0)", "from_millisecond(1)", "from_microsecond(1)",
                "hour_from_unixtime(0)", "uuid_v7_to_datetime(" + uuid + ")", "UUIDv7ToDateTime(" + uuid + ")",
                "monthname(DATE '2024-01-01')", "dayname(DATE '2024-01-01')")) {
            Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT * REPLACE(" + value + " AS k) FROM t")), value);
        }
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT * REPLACE(uuid_v7_to_datetime(" + uuid + ", 'UTC') AS k) FROM t")));
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT * REPLACE(monthname(k) AS k) FROM t")));
    }

    @Test
    public void testImplicitEncryptionModeRequiresRawReplay() {
        for (String function : List.of("aes_encrypt", "aes_decrypt", "sm4_encrypt", "sm4_decrypt")) {
            Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT hex(" + function + "(s, 'fixture-key')) FROM t")), function);
            Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT * REPLACE(" + function + "(s, 'fixture-key', 'fixture-iv') AS s) FROM t")),
                    function);
            Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT " + function + "(s, 'fixture-key', '', 'AES_128_ECB') FROM t")), function);
        }
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT db1.aes_encrypt(s, 'fixture-key') FROM t")));
    }

    @Test
    public void testOmittedAiResourceRequiresRawReplay() {
        for (String name : List.of("ai_filter", "ai_fixgrammar", "ai_generate", "ai_sentiment",
                "ai_summarize", "embed")) {
            assertAiResourceGuard(name, "s");
        }
        for (String name : List.of("ai_agg", "ai_classify", "ai_extract", "ai_mask", "ai_similarity", "ai_translate")) {
            assertAiResourceGuard(name, "s, other");
        }
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT db1.ai_sentiment(s) FROM t")));
        Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT * REPLACE(ai_sentiment(s) AS s) FROM t")));
    }

    private static void assertAiResourceGuard(String name, String inputs) {
        Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT " + name + "(" + inputs + ") FROM t")), name);
        Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                parse("SELECT " + name + "('resource', " + inputs + ") FROM t")), name);
    }

    @Test
    public void testConcreteDecimalArrayPayloadRequiresRawReplay() {
        for (String function : List.of("array_avg", "array_sum", "array_product", "array_cum_sum")) {
            Assertions.assertTrue(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT * REPLACE(" + function + "([1.2]) AS k) FROM t")), function);
            Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT * REPLACE(" + function + "([1]) AS k) FROM t")), function);
            Assertions.assertFalse(SPMPlanTreeSupport.requiresSessionSensitiveReplay(
                    parse("SELECT * REPLACE(" + function + "(a) AS k) FROM t")), function);
        }
    }

    /** Every {@code SpmConstVar} marker the parameterization produced for one query. */
    private static List<SpmConstVar> markers(String sql) {
        TestSPMPlaceholderBuilder builder = new TestSPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(parse(sql), builder);
        List<SpmConstVar> markers = new ArrayList<>();
        for (PlaceholderSnapshot marker : builder.getPlaceholderExprs()) {
            if (marker.placeholderExpr() instanceof SpmConstVar) {
                markers.add((SpmConstVar) marker.placeholderExpr());
            }
        }
        return markers;
    }

    private static Expression firstProjection(String select) {
        Expression found = firstNodeExpression(parse(select));
        if (found == null) {
            throw new AssertionError("no expression in " + select);
        }
        return found;
    }

    private static Expression firstNodeExpression(Plan plan) {
        for (Expression expression : plan.getExpressions()) {
            return expression;
        }
        for (Plan child : plan.children()) {
            Expression found = firstNodeExpression(child);
            if (found != null) {
                return found;
            }
        }
        for (Plan extra : plan.extraPlans()) {
            Expression found = firstNodeExpression(extra);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static LogicalPlan parse(String sql) {
        context();
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
