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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.analysis.SessionVarGuardRewriter;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteTestHelper;
import org.apache.doris.nereids.rules.expression.MergeGuardExpr;
import org.apache.doris.nereids.trees.expressions.functions.generator.Explode;
import org.apache.doris.nereids.trees.expressions.functions.scalar.DateTrunc;
import org.apache.doris.nereids.trees.expressions.functions.scalar.If;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class VariablePersistTest extends ExpressionRewriteTestHelper {
    private static final NereidsParser PARSER = new NereidsParser();

    @BeforeEach
    public void setUp() {
        cascadesContext = MemoTestUtils.createCascadesContext(
                new UnboundRelation(new RelationId(1), ImmutableList.of("tbl")));
        context = new ExpressionRewriteContext(cascadesContext);
    }

    // Test point 2: When persisted sessionVars in view differ from current sessionVar, only the expressions
    // whose value depends on the changed variable family are guarded (per-variable dependency scoping): a
    // time-zone sensitive expression is guarded when the creation time zone differs, a NeedSessionVarGuard
    // expression is guarded when another affectQueryResult variable differs, and an expression with no
    // dependency on the changed variables is left untouched.
    @Test
    public void testAddGuardWhenSessionVarsDifferent() {
        Map<String, String> persistSessionVars = ImmutableMap.of(
                "time_zone", "+00:00", "enable_decimal256", "true");
        Map<String, String> currentSessionVars = ImmutableMap.of(
                "time_zone", "+08:00", "enable_decimal256", "false");
        boolean matches = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, persistSessionVars);
        Assertions.assertFalse(matches, "Should return false when sessionVars differ");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars, currentSessionVars);

        // a time-zone sensitive expression is guarded because the creation time zone differs
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        Expression rewrittenTz = dateTruncOnTz.accept(rewriter, Boolean.FALSE);
        Assertions.assertTrue(rewrittenTz instanceof SessionVarGuardExpr,
                "time-zone sensitive expression should be guarded when time_zone differs");
        Assertions.assertEquals(dateTruncOnTz, ((SessionVarGuardExpr) rewrittenTz).child());

        // a NeedSessionVarGuard expression is guarded because enable_decimal256 differs
        Expression ifExpr = new If(new SlotReference("cond", BooleanType.INSTANCE),
                new SlotReference("a", BooleanType.INSTANCE), new SlotReference("b", BooleanType.INSTANCE));
        Expression rewrittenIf = ifExpr.accept(rewriter, Boolean.FALSE);
        Assertions.assertTrue(rewrittenIf instanceof SessionVarGuardExpr,
                "NeedSessionVarGuard expression should be guarded when a non-time_zone variable differs");

        // an expression with no dependency on the changed variables must NOT be guarded:
        // a boolean conjunction has no decimal256 / time-zone dependency
        Expression andExpr = new And(new SlotReference("a", BooleanType.INSTANCE),
                new SlotReference("b", BooleanType.INSTANCE));
        Expression rewritten = andExpr.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "expression with no dependency on the changed variables should not be guarded");
    }

    /**
     * The guard family mask computed from the actual session difference: a time-zone-only difference must
     * not activate the "other" family (so integer SUM keeps rewriting across zones) and vice versa.
     */
    @Test
    public void testComputeGuardMask() {
        Map<String, String> persist = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "1");
        // matching session -> no guard
        Map<String, String> currentSame = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "1");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(currentSame, persist));
        // time zone differs only -> time-zone guard only
        Map<String, String> currentTzDiff = ImmutableMap.of("time_zone", "+08:00", "sql_mode", "1");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeGuardMask(currentTzDiff, persist));
        // other variable differs only -> other guard only
        Map<String, String> currentOtherDiff = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "2");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(currentOtherDiff, persist));
        // both differ -> both guards
        Map<String, String> currentBothDiff = ImmutableMap.of("time_zone", "+08:00", "sql_mode", "2");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE | SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(currentBothDiff, persist));
        // equivalent time-zone spellings do not count as a difference
        Map<String, String> currentTzEquivalent = ImmutableMap.of("time_zone", "UTC", "sql_mode", "1");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(currentTzEquivalent, persist));
        // pre-time_zone metadata (no time_zone key) -> time-zone guard fence
        Map<String, String> legacyPersist = ImmutableMap.of("sql_mode", "1");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeGuardMask(currentSame, legacyPersist));
        // empty persist -> no guard
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(currentSame, new HashMap<>()));
    }

    /**
     * GUARD_OTHER must be derived from the union of both key sets. A variable that exists only in the
     * current session (e.g. enable_decimal256 was added to the persisted plan-variable set after this
     * view/MV was created) means the object was materialized with the other (default) value, so the
     * current setting must not be eligible for rewrite; the old whole-map comparison caught this
     * one-sided key set, the persist-only loop did not.
     */
    @Test
    public void testComputeGuardMaskCurrentOnlyVariable() {
        // the creation map predates enable_decimal256 being part of the plan-variable set
        Map<String, String> persist = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "1");
        // current session enables the newer variable -> must get GUARD_OTHER
        Map<String, String> currentNewVarOn = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "1",
                "enable_decimal256", "true");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(currentNewVarOn, persist));
        // even the default value of the newer variable is a mismatch (conservative compatibility fence:
        // the object was materialized before the variable existed, so its materialized semantics are unknown)
        Map<String, String> currentNewVarDefault = ImmutableMap.of("time_zone", "+00:00", "sql_mode", "1",
                "enable_decimal256", "false");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(currentNewVarDefault, persist));
        // a persist-only key missing from the current map is a mismatch too
        Map<String, String> currentMissingPersisted = ImmutableMap.of("time_zone", "+00:00");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(currentMissingPersisted, persist));
        // a full match still yields no guard
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(persist, persist));
    }

    /**
     * The mask-driven rewriter (used for the shared MTMV rewrite caches) guards the selected families
     * unconditionally, independent of the session it is constructed in.
     */
    @Test
    public void testMaskDrivenRewriterGuardsSelectedFamilies() {
        Map<String, String> persist = ImmutableMap.of("time_zone", "+00:00", "enable_decimal256", "true");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter timeZoneOnly =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persist,
                        SessionVarGuardRewriter.GUARD_TIME_ZONE);
        SessionVarGuardRewriter.AddSessionVarGuardRewriter otherOnly =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persist,
                        SessionVarGuardRewriter.GUARD_OTHER);
        SessionVarGuardRewriter.AddSessionVarGuardRewriter both =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persist,
                        SessionVarGuardRewriter.GUARD_TIME_ZONE | SessionVarGuardRewriter.GUARD_OTHER);

        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        Expression ifExpr = new If(new SlotReference("cond", BooleanType.INSTANCE),
                new SlotReference("a", BooleanType.INSTANCE), new SlotReference("b", BooleanType.INSTANCE));

        // time-zone mask guards the time-zone sensitive expression only
        Assertions.assertTrue(dateTruncOnTz.accept(timeZoneOnly, Boolean.FALSE) instanceof SessionVarGuardExpr);
        Assertions.assertFalse(ifExpr.accept(timeZoneOnly, Boolean.FALSE) instanceof SessionVarGuardExpr);
        // other mask guards the NeedSessionVarGuard expression only
        Assertions.assertFalse(dateTruncOnTz.accept(otherOnly, Boolean.FALSE) instanceof SessionVarGuardExpr);
        Assertions.assertTrue(ifExpr.accept(otherOnly, Boolean.FALSE) instanceof SessionVarGuardExpr);
        // both masks guard both families
        Assertions.assertTrue(dateTruncOnTz.accept(both, Boolean.FALSE) instanceof SessionVarGuardExpr);
        Assertions.assertTrue(ifExpr.accept(both, Boolean.FALSE) instanceof SessionVarGuardExpr);
    }

    // Test point 3: When persisted sessionVars in view match current sessionVar, GuardExpr should not be added
    @Test
    public void testNoGuardWhenSessionVarsSame() {
        Map<String, String> persistSessionVars = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> currentSessionVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, persistSessionVars);
        Assertions.assertTrue(matches, "Should return true when sessionVars match");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(null);
        Expression expr = PARSER.parseExpression("a * b");
        expr = ExpressionRewriteTestHelper.replaceUnboundSlot(expr, new HashMap<>());
        expr = ExpressionRewriteTestHelper.typeCoercion(expr);
        Expression rewritten = expr.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "Should not add GuardExpr when sessionVar is null");
        Assertions.assertEquals(expr, rewritten);
    }

    // Test when persisted sessionVars are empty or null
    @Test
    public void testCheckSessionVariablesMatchWithEmptyPersistVars() {
        Map<String, String> currentSessionVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches1 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, null);
        Assertions.assertTrue(matches1, "Should return true when persistSessionVars is null");

        Map<String, String> emptyPersistVars = new HashMap<>();
        boolean matches2 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                currentSessionVars, emptyPersistVars);
        Assertions.assertTrue(matches2, "Should return true when persistSessionVars is empty");
    }

    @Test
    public void testMultipleSessionVars() {
        Map<String, String> persistSessionVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "10"
        );
        Map<String, String> partialMatchVars = ImmutableMap.of("enable_decimal256", "true");
        boolean matches1 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                partialMatchVars, persistSessionVars);
        Assertions.assertFalse(matches1, "Should return false when only partial variables match");

        Map<String, String> fullMatchVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "10"
        );
        boolean matches2 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                fullMatchVars, persistSessionVars);
        Assertions.assertTrue(matches2, "Should return true when all variables match");

        Map<String, String> valueMismatchVars = ImmutableMap.of(
                "enable_decimal256", "true",
                "decimal_overflow_scale", "8"
        );
        boolean matches3 = SessionVarGuardRewriter.checkSessionVariablesMatch(
                valueMismatchVars, persistSessionVars);
        Assertions.assertFalse(matches3, "Should return false when variable values do not match");
    }

    // guard(guard(child)) -> guard(child)
    @Test
    public void testMergeGuardExprRemoveSame() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        Expression child = PARSER.parseExpression("a + b");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        Expression rewritten = outerGuard.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr);
        SessionVarGuardExpr resultGuard = (SessionVarGuardExpr) rewritten;
        Assertions.assertEquals(child, resultGuard.child());
        Assertions.assertEquals(sessionVars1, resultGuard.getSessionVars());
    }

    // guard(guard(child)) -> child, slot need not be guarded
    @Test
    public void testMergeGuardExprRemoveUseless() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        Expression child = new SlotReference("a", BooleanType.INSTANCE);
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        Expression rewritten = outerGuard.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertEquals(child, rewritten);
    }

    // guard(guard(and(guard(guard(child1), child2)))) -> and(child1,child2)
    @Test
    public void testMergeGuardExprComplicated() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "true");
        SlotReference slotA = new SlotReference("a", BooleanType.INSTANCE);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(slotA, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        SlotReference slotB = new SlotReference("b", BooleanType.INSTANCE);
        And and = new And(outerGuard, slotB);
        SessionVarGuardExpr innerGuard2 = new SessionVarGuardExpr(and, sessionVars2);
        SessionVarGuardExpr outerGuard2 = new SessionVarGuardExpr(innerGuard2, sessionVars1);
        Expression rewritten = outerGuard2.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertEquals(rewritten, new And(slotA, slotB));
    }


    // guard1(guard2(child)) -> exception
    @Test
    public void testMergeGuardExprException() {
        Map<String, String> sessionVars1 = ImmutableMap.of("enable_decimal256", "true");
        Map<String, String> sessionVars2 = ImmutableMap.of("enable_decimal256", "false");
        Expression child = PARSER.parseExpression("a");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr innerGuard = new SessionVarGuardExpr(child, sessionVars2);
        SessionVarGuardExpr outerGuard = new SessionVarGuardExpr(innerGuard, sessionVars1);
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class, () ->
                outerGuard.accept(MergeGuardExpr.INSTANCE, null));
        Assertions.assertTrue(exception.getMessage().contains("Conflicting session variable guards"));
    }

    // Test deep nesting (3 layers) - should not cause infinite loop
    @Test
    public void testMergeGuardExprDeepNesting() {
        Map<String, String> sessionVars = ImmutableMap.of("enable_decimal256", "true");
        Expression child = PARSER.parseExpression("a + b");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);
        SessionVarGuardExpr guard1 = new SessionVarGuardExpr(child, sessionVars);
        SessionVarGuardExpr guard2 = new SessionVarGuardExpr(guard1, sessionVars);
        SessionVarGuardExpr guard3 = new SessionVarGuardExpr(guard2, sessionVars);
        Expression rewritten = guard3.accept(MergeGuardExpr.INSTANCE, null);

        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr);
        SessionVarGuardExpr resultGuard = (SessionVarGuardExpr) rewritten;
        Assertions.assertEquals(child, resultGuard.child());
        Assertions.assertEquals(sessionVars, resultGuard.getSessionVars());
    }

    /**
     * A time-zone sensitive expression (an expression that operates on a TIMESTAMPTZ value, such as
     * date_trunc on a timestamptz column) must be guarded when the persisted session variables differ
     * from the current session. Otherwise a materialized view built in one time zone could be rewritten
     * in a session with a different time zone and return stale materialized values.
     */
    @Test
    public void testTimeZoneSensitiveExprGetsGuard() {
        Map<String, String> persistSessionVars = ImmutableMap.of("time_zone", "+00:00");
        Map<String, String> currentSessionVars = ImmutableMap.of("time_zone", "+08:00");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars, currentSessionVars);
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        Expression rewritten = dateTruncOnTz.accept(rewriter, Boolean.FALSE);
        Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr,
                "date_trunc on TIMESTAMPTZ should be guarded when session vars differ");
        Assertions.assertEquals(dateTruncOnTz, ((SessionVarGuardExpr) rewritten).child());

        // cast of a timestamptz value is also time-zone sensitive
        Expression castOnTz = new Cast(tzSlot, VarcharType.SYSTEM_DEFAULT);
        Expression rewrittenCast = castOnTz.accept(rewriter, Boolean.FALSE);
        Assertions.assertTrue(rewrittenCast instanceof SessionVarGuardExpr,
                "cast of TIMESTAMPTZ should be guarded when session vars differ");
    }

    /**
     * Equivalent time-zone spellings (UTC / Etc/UTC / GMT / +00:00) denote the same instant zone and must
     * not disable a rewrite: the time-zone sensitive expression below must NOT be guarded when the creation
     * zone and the current zone are merely spelled differently.
     */
    @Test
    public void testEquivalentTimeZoneSpellingsNotGuarded() {
        Map<String, String> persistSessionVars = ImmutableMap.of("time_zone", "+00:00");
        Map<String, String> currentSessionVars = ImmutableMap.of("time_zone", "UTC");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars, currentSessionVars);
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        Expression rewritten = dateTruncOnTz.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "equivalent time-zone spellings should not cause a guard");
        Assertions.assertEquals(dateTruncOnTz, rewritten);
    }

    /**
     * The same function applied to a plain DATETIME/DATE column is NOT time-zone sensitive, so it must not
     * be guarded even when the creation time zone differs; guarding it would unnecessarily disable
     * materialized view rewrite for a value that does not depend on the session time zone.
     */
    @Test
    public void testDateTimeExprNotGuarded() {
        Map<String, String> persistSessionVars = ImmutableMap.of("time_zone", "+00:00");
        Map<String, String> currentSessionVars = ImmutableMap.of("time_zone", "+08:00");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars, currentSessionVars);
        SlotReference dtSlot = new SlotReference("dt", DateTimeV2Type.SYSTEM_DEFAULT);
        Expression dateTruncOnDt = new DateTrunc(dtSlot, new VarcharLiteral("day"));
        Expression rewritten = dateTruncOnDt.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "date_trunc on DATETIME should not be guarded");
        Assertions.assertEquals(dateTruncOnDt, rewritten);
    }

    /**
     * D2: a whole-plan rewrite must not replace subtype-constrained structural expressions with a guard.
     * row_number() OVER (ORDER BY ts) where ts is a TIMESTAMPTZ must not wrap the OrderExpression (its
     * owner WindowExpression.withChildren casts order keys back to OrderExpression and would throw a
     * ClassCastException). The structural root is preserved; only genuinely value-producing children are
     * guarded.
     */
    @Test
    public void testWindowOrderByTimestamptzNotReplaced() {
        Map<String, String> var = ImmutableMap.of("time_zone", "+00:00");
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        SlotReference ts = new SlotReference("ts", TimeStampTzType.of(6));
        WindowExpression windowExpr = new WindowExpression(new RowNumber(), ImmutableList.of(),
                ImmutableList.of(new OrderExpression(new OrderKey(ts, true, true))));
        LogicalWindow<LogicalPlan> window = new LogicalWindow<>(
                ImmutableList.of(new Alias(windowExpr, "rn")), scan);
        SessionVarGuardRewriter rewriter = new SessionVarGuardRewriter(var,
                SessionVarGuardRewriter.GUARD_TIME_ZONE, cascadesContext);
        // must not throw ClassCastException
        Plan rewritten = SessionVarGuardRewriter.rewritePlanTree(rewriter, window);
        Assertions.assertNotNull(rewritten);
        Assertions.assertTrue(rewritten instanceof LogicalWindow);
        // row_number() OVER (ORDER BY ts) is zone invariant: the structural OrderExpression must survive
        // and nothing must be wrapped in a guard
        LogicalWindow<?> rewrittenWindow = (LogicalWindow<?>) rewritten;
        Assertions.assertTrue(rewrittenWindow.getWindowExpressions().stream()
                        .noneMatch(expr -> expr.containsType(SessionVarGuardExpr.class)),
                "zone-invariant window expression must not be guarded");
    }

    /**
     * D2: a whole-plan rewrite must not replace a table-generating function root with a guard.
     * explode(ARRAY&lt;TIMESTAMPTZ&gt;) must not wrap the Explode root (GenerateExpressionRewrite casts the
     * generator back to Function and would throw a ClassCastException). The generator root is preserved.
     */
    @Test
    public void testGenerateExplodeTimestamptzNotReplaced() {
        Map<String, String> var = ImmutableMap.of("time_zone", "+00:00");
        LogicalOlapScan scan = PlanConstructor.newLogicalOlapScan(0, "t", 0);
        SlotReference arr = new SlotReference("arr", ArrayType.of(TimeStampTzType.of(6)));
        Explode explode = new Explode(arr);
        LogicalGenerate<LogicalPlan> generate = new LogicalGenerate<>(
                ImmutableList.of(explode),
                ImmutableList.of(new SlotReference("e", TimeStampTzType.of(6))), scan);
        SessionVarGuardRewriter rewriter = new SessionVarGuardRewriter(var,
                SessionVarGuardRewriter.GUARD_TIME_ZONE, cascadesContext);
        // must not throw ClassCastException
        Plan rewritten = SessionVarGuardRewriter.rewritePlanTree(rewriter, generate);
        Assertions.assertNotNull(rewritten);
        Assertions.assertTrue(rewritten instanceof LogicalGenerate);
        LogicalGenerate<?> rewrittenGenerate = (LogicalGenerate<?>) rewritten;
        Assertions.assertEquals(1, rewrittenGenerate.getGenerators().size());
        Assertions.assertTrue(rewrittenGenerate.getGenerators().get(0) instanceof Explode);
    }

    /**
     * D5: the single-argument rewriter guards BOTH dependency families unconditionally, independent of the
     * current thread-local session. Its only production caller builds it inside an AutoCloseSessionVariable
     * scope (where the current session already equals var), while the wrapped expression is translated /
     * executed later in a different (load) session.
     */
    @Test
    public void testSingleArgRewriterGuardsUnconditionally() {
        Map<String, String> persist = ImmutableMap.of("time_zone", "+00:00", "enable_decimal256", "true");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persist);
        // time-zone sensitive expression is guarded
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        Assertions.assertTrue(dateTruncOnTz.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "single-arg rewriter must guard time-zone sensitive expressions unconditionally");
        // NeedSessionVarGuard expression is guarded
        Expression ifExpr = new If(new SlotReference("cond", BooleanType.INSTANCE),
                new SlotReference("a", BooleanType.INSTANCE), new SlotReference("b", BooleanType.INSTANCE));
        Assertions.assertTrue(ifExpr.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "single-arg rewriter must guard NeedSessionVarGuard expressions unconditionally");
        // a zone-invariant boolean conjunction is not guarded
        Expression andExpr = new And(new SlotReference("a", BooleanType.INSTANCE),
                new SlotReference("b", BooleanType.INSTANCE));
        Assertions.assertFalse(andExpr.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "unrelated expressions must not be guarded");
    }

    /**
     * The cache-mismatch guard added to an MTMV rewrite cache must stay structurally distinct from the
     * nested persisted-object guard BindRelation adds while expanding a view into the query, even when both
     * wrap the same child with the same session variables. Materialized view matching compares expressions
     * by equals, so without this distinction a FORCE_IN_RBO pre-rewrite could substitute an MTMV
     * materialized in another zone for a query that would evaluate the expression in its own zone after
     * dropping the guard.
     */
    @Test
    public void testCacheGuardStructurallyDistinctFromNestedObjectGuard() {
        Map<String, String> sessionVars = ImmutableMap.of("time_zone", "+00:00");
        Expression child = PARSER.parseExpression("date_trunc(ts, 'day')");
        child = ExpressionRewriteTestHelper.replaceUnboundSlot(child, new HashMap<>());
        child = ExpressionRewriteTestHelper.typeCoercion(child);

        // the same child and the same session vars: only the cache-guard distinction may tell them apart
        SessionVarGuardExpr nestedObjectGuard = new SessionVarGuardExpr(child, sessionVars);
        SessionVarGuardExpr cacheGuard = new SessionVarGuardExpr(child, sessionVars, true);

        Assertions.assertFalse(nestedObjectGuard.isCacheGuard());
        Assertions.assertTrue(cacheGuard.isCacheGuard());
        Assertions.assertNotEquals(nestedObjectGuard, cacheGuard,
                "a query-side nested-object guard must never equal a cache-mismatch guard");
        Assertions.assertNotEquals(nestedObjectGuard.hashCode(), cacheGuard.hashCode());
        // rebuilding must preserve the kind, otherwise downstream rewrites could erase the distinction
        Assertions.assertTrue(((SessionVarGuardExpr) cacheGuard.withChildren(ImmutableList.of(child)))
                .isCacheGuard());
    }
}
