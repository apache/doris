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
import org.apache.doris.nereids.trees.expressions.functions.agg.AvgMap;
import org.apache.doris.nereids.trees.expressions.functions.agg.SumMap;
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
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TimeStampTzType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;

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
     * A cache-building rewriter (the one MTMVCache.from uses for a nonzero guard mask) must retain a
     * cache-mismatch marker around an existing non-cache guard: an MTMV over a view carries the view's
     * query-side guard (cacheGuard=false) in its definition plan, and without re-marking it the guarded
     * cache for a cross-zone query would have no cache guard, so the isCacheGuard() rejection in
     * AbstractMaterializedViewRule never fires and a value materialized in the MV's refresh session could
     * be substituted into a query evaluated in a different zone. The marker must be structurally distinct
     * from the query-side guard (same shape but cacheGuard=true).
     */
    @Test
    public void testCacheGuardPreservedAroundExistingViewGuard() {
        Map<String, String> viewVars = ImmutableMap.of("time_zone", "+08:00");
        Map<String, String> mvVars = ImmutableMap.of("time_zone", "+00:00");
        // the view expansion already wraps the zone-sensitive expression in a query-side (non-cache) guard
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));
        SessionVarGuardExpr viewGuard = new SessionVarGuardExpr(dateTruncOnTz, viewVars);
        Assertions.assertFalse(viewGuard.isCacheGuard());

        // the query-side rewriter leaves the existing view guard untouched
        SessionVarGuardRewriter.AddSessionVarGuardRewriter querySideRewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(mvVars, viewVars);
        Expression querySide = viewGuard.accept(querySideRewriter, Boolean.FALSE);
        Assertions.assertSame(viewGuard, querySide);

        // the cache-building rewriter wraps the existing view guard in a cache-mismatch marker
        SessionVarGuardRewriter.AddSessionVarGuardRewriter cacheRewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(mvVars,
                        SessionVarGuardRewriter.GUARD_TIME_ZONE);
        Expression cacheSide = viewGuard.accept(cacheRewriter, Boolean.FALSE);
        Assertions.assertTrue(cacheSide instanceof SessionVarGuardExpr);
        SessionVarGuardExpr cacheGuard = (SessionVarGuardExpr) cacheSide;
        Assertions.assertTrue(cacheGuard.isCacheGuard());
        // the marker wraps the existing view guard (keeping its own session vars inside)
        Assertions.assertEquals(viewGuard, cacheGuard.child());
        // the cache guard must stay structurally distinct from the query-side guard
        Assertions.assertNotEquals(querySide, cacheSide);
    }

    /**
     * The cache-mismatch marker around an existing non-cache guard is scoped to the guard family the cache
     * is built for (computeGuardMask separates time-zone and other-variable dependencies): a time-zone-only
     * cache must NOT wrap an existing "other"-family guard (e.g. a decimal multiply guarded by
     * enable_decimal256), and an other-family-only cache must NOT wrap an existing time-zone guard -
     * otherwise a safe cross-zone nested-view rewrite whose semantics agree through the view guard would be
     * rejected by the isCacheGuard() gates. The outer marker is added only when the existing guard's family
     * intersects the cache mask.
     */
    @Test
    public void testCacheGuardPreservedOnlyForIntersectingFamily() {
        Map<String, String> viewVars = ImmutableMap.of("time_zone", "+08:00", "enable_decimal256", "true");
        Map<String, String> mvVars = ImmutableMap.of("time_zone", "+00:00", "enable_decimal256", "false");

        // an "other"-family view guard: decimal multiplication depends on enable_decimal256
        SlotReference a = new SlotReference("a", DecimalV3Type.createDecimalV3Type(27, 9));
        SlotReference b = new SlotReference("b", DecimalV3Type.createDecimalV3Type(27, 9));
        SessionVarGuardExpr otherViewGuard = new SessionVarGuardExpr(new Multiply(a, b), viewVars);

        // a time-zone view guard: date_trunc on a TIMESTAMPTZ
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        SessionVarGuardExpr tzViewGuard =
                new SessionVarGuardExpr(new DateTrunc(tzSlot, new VarcharLiteral("day")), viewVars);

        // a time-zone-only cache mask wraps the time-zone guard but leaves the "other"-family guard as-is
        SessionVarGuardRewriter.AddSessionVarGuardRewriter tzCache = new SessionVarGuardRewriter
                .AddSessionVarGuardRewriter(mvVars, SessionVarGuardRewriter.GUARD_TIME_ZONE);
        Expression tzWrapped = tzViewGuard.accept(tzCache, Boolean.FALSE);
        Assertions.assertTrue(tzWrapped instanceof SessionVarGuardExpr
                && ((SessionVarGuardExpr) tzWrapped).isCacheGuard(),
                "time-zone guard must get the marker under a time-zone cache mask");
        Expression otherKept = otherViewGuard.accept(tzCache, Boolean.FALSE);
        Assertions.assertSame(otherViewGuard, otherKept,
                "other-family guard must not get the marker under a time-zone-only cache mask");

        // an other-family-only cache mask wraps the other-family guard but leaves the time-zone guard as-is
        SessionVarGuardRewriter.AddSessionVarGuardRewriter otherCache = new SessionVarGuardRewriter
                .AddSessionVarGuardRewriter(mvVars, SessionVarGuardRewriter.GUARD_OTHER);
        Expression otherWrapped = otherViewGuard.accept(otherCache, Boolean.FALSE);
        Assertions.assertTrue(otherWrapped instanceof SessionVarGuardExpr
                && ((SessionVarGuardExpr) otherWrapped).isCacheGuard(),
                "other-family guard must get the marker under an other-family cache mask");
        Expression tzKept = tzViewGuard.accept(otherCache, Boolean.FALSE);
        Assertions.assertSame(tzViewGuard, tzKept,
                "time-zone guard must not get the marker under an other-family-only cache mask");
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
        // empty or missing metadata -> the creation zone is unknown, so the time-zone fence applies
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeGuardMask(currentSame, new HashMap<>()));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeGuardMask(currentSame, null));
        Assertions.assertTrue(SessionVarGuardRewriter.needsSessionVarGuard(currentSame, new HashMap<>()),
                "unknown creation metadata must enable the time-zone guard fence");
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

    @Test
    public void testMapAggregateFunctionsNeedGuard() {
        Map<String, String> persistSessionVars = ImmutableMap.of("enable_decimal256", "true");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars);
        SlotReference map = new SlotReference("m",
                MapType.of(StringType.INSTANCE, DecimalV3Type.createDecimalV3Type(38, 10)));
        for (Expression expr : ImmutableList.of(new SumMap(map), new AvgMap(map))) {
            Expression rewritten = expr.accept(rewriter, Boolean.FALSE);
            Assertions.assertTrue(rewritten instanceof SessionVarGuardExpr, expr.toSql());
            Assertions.assertEquals(persistSessionVars, ((SessionVarGuardExpr) rewritten).getSessionVars());
            Assertions.assertEquals(expr, rewritten.child(0));
        }
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

    private static Map<String, String> syncMvSessionVars(long sqlMode, boolean enableDecimal256) {
        return ImmutableMap.of(
                SessionVariable.SQL_MODE, Long.toString(sqlMode),
                "enable_decimal256", Boolean.toString(enableDecimal256));
    }

    @Test
    public void testSyncMvMatchingSqlModes() {
        long mode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        Map<String, String> current = syncMvSessionVars(mode, false);
        Map<String, String> persisted = syncMvSessionVars(mode, false);

        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, persisted));
    }

    @Test
    public void testSyncMvStrictTransTablesDifferenceInBothDirections() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictMode = baseMode | SqlModeHelper.MODE_STRICT_TRANS_TABLES;
        Map<String, String> withoutStrict = syncMvSessionVars(baseMode, false);
        Map<String, String> withStrict = syncMvSessionVars(strictMode, false);

        // Rollup created without strict mode, queried with strict mode.
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(withStrict, withoutStrict));

        // Rollup created with strict mode, queried without strict mode.
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(withoutStrict, withStrict));

        // Identical strict-mode configurations must also match.
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(withStrict, withStrict));

        // Compatibility also applies when SQL_MODE is the only persisted variable.
        Map<String, String> persistedSqlModeOnly =
                ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(baseMode));
        Map<String, String> currentSqlModeOnly =
                ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(strictMode));
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                        currentSqlModeOnly, persistedSqlModeOnly));
    }

    @Test
    public void testSyncMvOtherSqlModeDifferencesRemainIncompatible() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        Map<String, String> persisted = syncMvSessionVars(baseMode, false);
        long[] otherModes = {
                SqlModeHelper.MODE_PIPES_AS_CONCAT,
                SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES
        };

        for (long otherMode : otherModes) {
            Map<String, String> current = syncMvSessionVars(baseMode | otherMode, false);
            Assertions.assertFalse(
                    SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, persisted),
                    "Unexpectedly accepted SQL-mode bit: " + otherMode);
        }

        // The exception must not allow other mode changes with SQL_MODE-only state, either.
        Map<String, String> persistedSqlModeOnly =
                ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(baseMode));
        Map<String, String> currentSqlModeOnly = ImmutableMap.of(
                SessionVariable.SQL_MODE,
                Long.toString(baseMode | SqlModeHelper.MODE_PIPES_AS_CONCAT));
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                        currentSqlModeOnly, persistedSqlModeOnly));
    }

    @Test
    public void testSyncMvStrictTableModesDoNotHideOtherModeDifferences() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictModes = SqlModeHelper.MODE_STRICT_TRANS_TABLES
                | SqlModeHelper.MODE_STRICT_ALL_TABLES;

        Map<String, String> persisted = syncMvSessionVars(baseMode, false);

        long[] incompatibleModes = {
            SqlModeHelper.MODE_PIPES_AS_CONCAT,
            SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES
        };

        for (long incompatibleMode : incompatibleModes) {
            Map<String, String> current = syncMvSessionVars(
                    baseMode | strictModes | incompatibleMode, false);

            Assertions.assertFalse(
                    SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                    current, persisted),
                    "Strict-table exemption must not hide other SQL-mode differences: "
                    + incompatibleMode);
        }
    }

    @Test
    public void testSyncMvOtherSessionVariablesStillRequireExactMatch() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictMode = baseMode
                | SqlModeHelper.MODE_STRICT_TRANS_TABLES
                | SqlModeHelper.MODE_STRICT_ALL_TABLES;
        Map<String, String> persisted = syncMvSessionVars(baseMode, false);

        // A strict-mode difference is allowed, but an unrelated variable mismatch is not.
        Map<String, String> current = syncMvSessionVars(strictMode, true);
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, persisted));
    }

    @Test
    public void testSyncMvMissingSessionVariablesRemainIncompatible() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        Map<String, String> persisted = syncMvSessionVars(baseMode, false);
        Map<String, String> missingSqlMode = ImmutableMap.of("enable_decimal256", "false");
        Map<String, String> missingDecimal256 =
                ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(baseMode));

        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(missingSqlMode, persisted));
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(missingDecimal256, persisted));
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(null, persisted));
    }

    @Test
    public void testSyncMvMalformedSqlModeDoesNotBypassGuard() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        Map<String, String> persisted = syncMvSessionVars(baseMode, false);
        Map<String, String> malformed = ImmutableMap.of(
                SessionVariable.SQL_MODE, "INVALID_SQL_MODE",
                "enable_decimal256", "false");

        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(malformed, persisted));
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(persisted, malformed));
    }

    @Test
    public void testSyncMvPreservesEmptyPersistedVariableBehavior() {
        Map<String, String> current = syncMvSessionVars(
                SqlModeHelper.MODE_ONLY_FULL_GROUP_BY, false);

        // Preserve the existing behavior for older MVs without saved variables.
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, null));
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, ImmutableMap.of()));
    }

    @Test
    public void testGeneralSessionVariableComparisonRemainsStrict() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictMode = baseMode
                | SqlModeHelper.MODE_STRICT_TRANS_TABLES
                | SqlModeHelper.MODE_STRICT_ALL_TABLES;
        Map<String, String> persisted = syncMvSessionVars(baseMode, false);
        Map<String, String> current = syncMvSessionVars(strictMode, false);

        // The Sync MV exception must not weaken the general-purpose comparator.
        Assertions.assertFalse(
                SessionVarGuardRewriter.checkSessionVariablesMatch(current, persisted));
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(current, persisted));
    }

    @Test
    public void testSyncMvStrictTableModeCombinations() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictTrans = SqlModeHelper.MODE_STRICT_TRANS_TABLES;
        long strictAll = SqlModeHelper.MODE_STRICT_ALL_TABLES;

        long[] modes = {
            baseMode,
            baseMode | strictTrans,
            baseMode | strictAll,
            baseMode | strictTrans | strictAll
        };

        // All four configurations should be mutually compatible for sync MVs.
        for (long persistedMode : modes) {
            for (long currentMode : modes) {
                Assertions.assertTrue(
                        SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                        syncMvSessionVars(currentMode, false),
                        syncMvSessionVars(persistedMode, false)),
                        "Strict-table SQL-mode flags should not block sync MV rewriting: "
                        + "current=" + currentMode + ", persisted=" + persistedMode);
            }
        }

        // The same exemption applies when SQL_MODE is the only saved variable.
        Map<String, String> baseSqlModeOnly = ImmutableMap.of(
                SessionVariable.SQL_MODE, Long.toString(baseMode));
        Map<String, String> bothStrictSqlModeOnly = ImmutableMap.of(
                SessionVariable.SQL_MODE, Long.toString(baseMode | strictTrans | strictAll));

        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                bothStrictSqlModeOnly, baseSqlModeOnly));
        Assertions.assertTrue(
                SessionVarGuardRewriter.checkSessionVariablesMatchForSyncMv(
                baseSqlModeOnly, bothStrictSqlModeOnly));
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
     * Comparisons and boolean combinations over TIMESTAMPTZ instants are zone invariant: whether two
     * instants are equal or ordered does not depend on the session time zone, so guarding them would
     * disable safe cross-zone rewrites (e.g. a sync MV whose WHERE is `ts1 = ts2`). A comparison whose
     * operand is itself zone dependent (a cast of an offset-free string into TIMESTAMPTZ) is still
     * guarded: the child visit wraps the operand, so the predicate keeps a guard in its subtree.
     */
    @Test
    public void testZoneInvariantTimestampTzComparisonsNotGuarded() {
        Map<String, String> persistSessionVars = ImmutableMap.of("time_zone", "+00:00");
        Map<String, String> currentSessionVars = ImmutableMap.of("time_zone", "+08:00");
        SessionVarGuardRewriter.AddSessionVarGuardRewriter rewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(persistSessionVars, currentSessionVars);
        SlotReference ts1 = new SlotReference("ts1", TimeStampTzType.of(6));
        SlotReference ts2 = new SlotReference("ts2", TimeStampTzType.of(6));

        // equality / ordering / BETWEEN / IN over instants are zone invariant
        Expression eq = new EqualTo(ts1, ts2);
        Assertions.assertFalse(eq.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "equality of two TIMESTAMPTZ instants is zone invariant");
        Expression gt = new GreaterThan(ts1, ts2);
        Assertions.assertFalse(gt.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "ordering of two TIMESTAMPTZ instants is zone invariant");
        Expression between = new Between(ts1, ts2, ts2);
        Assertions.assertFalse(between.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "BETWEEN over TIMESTAMPTZ instants is zone invariant");
        Expression in = new InPredicate(ts1, ImmutableList.of(ts2, ts1));
        Assertions.assertFalse(in.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "IN over TIMESTAMPTZ instants is zone invariant");

        // boolean combinations of zone-invariant predicates are zone invariant as well (any zone dependent
        // operand is guarded on its own by the child visit)
        Expression conjunction = new And(eq, gt);
        Assertions.assertFalse(conjunction.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "AND of zone-invariant predicates is zone invariant");
        Expression negation = new Not(eq);
        Assertions.assertFalse(negation.accept(rewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "NOT of a zone-invariant predicate is zone invariant");

        // a cast of an offset-free string into TIMESTAMPTZ is zone dependent, so the operand is guarded
        // (the comparison itself stays unguarded, but the guard survives in the predicate subtree)
        Expression castInto = new Cast(new VarcharLiteral("2024-01-01 00:00:00"), TimeStampTzType.of(6));
        Expression sensitiveComparison = new EqualTo(castInto, ts2);
        Expression rewritten = sensitiveComparison.accept(rewriter, Boolean.FALSE);
        Assertions.assertFalse(rewritten instanceof SessionVarGuardExpr,
                "the comparison operator itself is zone invariant");
        Assertions.assertTrue(rewritten.anyMatch(SessionVarGuardExpr.class::isInstance),
                "a zone-dependent operand must stay guarded inside the comparison");
    }

    /**
     * Old-metadata compatibility fence: an object created before time_zone was persisted (empty map, or a
     * map that carries other plan variables but no time_zone key) has an unknown creation zone, so its
     * time-zone sensitive expressions must always be guarded. Without the fence a new FE would evaluate
     * the object against the current session zone while the materialized value may have been produced in
     * a different (unknown) zone. The reverse direction (old FE reading new metadata) cannot be repaired
     * from the new code and requires rebuilding the object there.
     */
    @Test
    public void testLegacyMetadataFenceGuardsTimeZoneExpressions() {
        Map<String, String> currentSessionVars = ImmutableMap.of("time_zone", "+08:00", "sql_mode", "0");
        SlotReference tzSlot = new SlotReference("ts", TimeStampTzType.of(6));
        Expression dateTruncOnTz = new DateTrunc(tzSlot, new VarcharLiteral("day"));

        // no persisted variables at all (metadata written before session-variable persistence)
        Assertions.assertTrue(SessionVarGuardRewriter.needsSessionVarGuard(currentSessionVars, new HashMap<>()));
        SessionVarGuardRewriter.AddSessionVarGuardRewriter emptyMapRewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(new HashMap<>(), currentSessionVars);
        Assertions.assertTrue(dateTruncOnTz.accept(emptyMapRewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "unknown creation zone must guard time-zone sensitive expressions");

        // a legacy map that carries other plan variables but no time_zone key
        Map<String, String> legacyPersist = ImmutableMap.of("sql_mode", "0");
        Assertions.assertTrue(SessionVarGuardRewriter.needsSessionVarGuard(currentSessionVars, legacyPersist));
        SessionVarGuardRewriter.AddSessionVarGuardRewriter legacyRewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(legacyPersist, currentSessionVars);
        Assertions.assertTrue(dateTruncOnTz.accept(legacyRewriter, Boolean.FALSE) instanceof SessionVarGuardExpr,
                "missing time_zone in legacy metadata must guard time-zone sensitive expressions");
        // the current metadata with a matching zone never guards
        SessionVarGuardRewriter.AddSessionVarGuardRewriter currentRewriter =
                new SessionVarGuardRewriter.AddSessionVarGuardRewriter(currentSessionVars, currentSessionVars);
        Assertions.assertFalse(dateTruncOnTz.accept(currentRewriter, Boolean.FALSE) instanceof SessionVarGuardExpr);
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

    /**
     * Only a persisted map that carries time_zone identifies the creation zone. Everything else is the
     * old-metadata upgrade direction and must stay behind the metadata compatibility fence: it is never an
     * unconditional match, so it cannot be rewritten across zones until it is recreated.
     */
    @Test
    public void testCreationSessionKnown() {
        Assertions.assertFalse(SessionVarGuardRewriter.isCreationSessionKnown(null));
        Assertions.assertFalse(SessionVarGuardRewriter.isCreationSessionKnown(new HashMap<>()));
        Assertions.assertFalse(SessionVarGuardRewriter.isCreationSessionKnown(
                ImmutableMap.of(SessionVariable.SQL_MODE, "0")));
        Assertions.assertTrue(SessionVarGuardRewriter.isCreationSessionKnown(
                ImmutableMap.of(SessionVariable.TIME_ZONE, "+00:00")));
        Assertions.assertTrue(SessionVarGuardRewriter.isCreationSessionKnown(
                ImmutableMap.of(SessionVariable.TIME_ZONE, "+00:00", SessionVariable.SQL_MODE, "0")));
    }

    /**
     * The synchronous MV cache must not treat a legacy (empty or time_zone-less) persisted map as a match.
     * The narrow strict-table sql_mode exception is still honored, but only for the other guard family.
     */
    @Test
    public void testSyncMvGuardMaskFencesUnknownCreationZone() {
        long baseMode = SqlModeHelper.MODE_ONLY_FULL_GROUP_BY;
        long strictMode = baseMode
                | SqlModeHelper.MODE_STRICT_TRANS_TABLES
                | SqlModeHelper.MODE_STRICT_ALL_TABLES;
        Map<String, String> current = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+08:00",
                SessionVariable.SQL_MODE, Long.toString(strictMode));
        Map<String, String> persisted = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+08:00",
                SessionVariable.SQL_MODE, Long.toString(baseMode));

        // matching creation session -> no guard
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current, current));
        // strict-table sql_mode-only difference keeps the historical exemption
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current, persisted));
        // an empty or time_zone-less map is an unknown creation zone, never an unconditional match
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current, new HashMap<>()));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current, null));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current,
                        ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(strictMode))));
        // the strict-table exemption must not rescue an unknown creation zone: the zone family is still
        // guarded, and the legacy sql_mode difference only adds the (otherwise exempted) other family
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE | SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current,
                        ImmutableMap.of(SessionVariable.SQL_MODE, Long.toString(baseMode))));
        // a zone difference is a mismatch even without any other difference
        Map<String, String> otherZone = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+00:00",
                SessionVariable.SQL_MODE, Long.toString(strictMode));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeSyncMvGuardMask(current, otherZone));
        // the sql_mode exemption must not hide a non-sql_mode variable difference
        Map<String, String> otherVarDiff = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+08:00",
                SessionVariable.SQL_MODE, Long.toString(baseMode),
                "enable_decimal256", "true");
        Map<String, String> currentNoDecimal = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+08:00",
                SessionVariable.SQL_MODE, Long.toString(strictMode));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeSyncMvGuardMask(currentNoDecimal, otherVarDiff));
    }

    /**
     * New-metadata -> old-FE direction of the compatibility fence. The new FE persists time_zone, which an
     * old FE does not register: its whole-map comparison still sees a mismatch (so it never uses the
     * unconditional-match cache), but its guarded cache does not classify TIMESTAMPTZ expressions, so the
     * object must be rebuilt/recreated before an old read-serving FE is used. This test only pins the
     * observable part of that contract: the persisted map is a whole-map mismatch for a session that does
     * not know the key, and the same map reports a known creation session on the new FE.
     */
    @Test
    public void testNewMetadataMismatchesWholeMapComparison() {
        Map<String, String> newMetadata = ImmutableMap.of(
                SessionVariable.TIME_ZONE, "+00:00",
                SessionVariable.SQL_MODE, "0");
        Map<String, String> oldFeSession = ImmutableMap.of(SessionVariable.SQL_MODE, "0");

        Assertions.assertNotEquals(oldFeSession, newMetadata,
                "an old FE's whole-map comparison must not treat new metadata as identical");
        Assertions.assertTrue(SessionVarGuardRewriter.isCreationSessionKnown(newMetadata));
        // the old FE's own (time_zone-less) map stays fenced on the new FE
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE,
                SessionVarGuardRewriter.computeGuardMask(newMetadata, oldFeSession));
    }

    /**
     * The persisted plan-variable maps carry a fence version stamp, which is what makes the version fence a
     * read fence instead of an incidental map difference: metadata stamped by a newer FE (the downgrade
     * direction) cannot be interpreted by this FE, so it is never reused for rewrite; a map without the
     * stamp predates the fence and keeps its time-zone guard; and the stamp itself never counts as a
     * session-variable difference between two maps produced by this FE.
     */
    @Test
    public void testPlanSessionVarFenceVersion() {
        Assertions.assertEquals(0, SessionVarGuardRewriter.planSessionVarFenceVersion(null));
        Assertions.assertEquals(0, SessionVarGuardRewriter.planSessionVarFenceVersion(new HashMap<>()));
        Assertions.assertEquals(0, SessionVarGuardRewriter.planSessionVarFenceVersion(
                ImmutableMap.of(SessionVariable.TIME_ZONE, "+00:00")));

        // every map this FE produces is stamped with the current version
        SessionVariable sessionVariable = new SessionVariable();
        Map<String, String> stamped = sessionVariable.getAffectQueryResultInPlanVariables();
        Assertions.assertEquals(String.valueOf(SessionVariable.CURRENT_PLAN_SESSION_VAR_FENCE_VERSION),
                stamped.get(SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION));
        Assertions.assertEquals(SessionVariable.CURRENT_PLAN_SESSION_VAR_FENCE_VERSION,
                SessionVarGuardRewriter.planSessionVarFenceVersion(stamped));
        // ... and rewriting is not disabled by the stamp itself
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(stamped, stamped));
        // a map written before the stamp existed (same plan variables, no stamp) is not a difference either
        Map<String, String> beforeStamp = new HashMap<>(stamped);
        beforeStamp.remove(SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION);
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_NONE,
                SessionVarGuardRewriter.computeGuardMask(stamped, beforeStamp));

        // metadata written by a newer FE must not be reused: both guard families are selected (read fence)
        Map<String, String> newerStamp = ImmutableMap.of(
                SessionVariable.TIME_ZONE, sessionVariable.getTimeZone(),
                SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION,
                String.valueOf(SessionVariable.CURRENT_PLAN_SESSION_VAR_FENCE_VERSION + 1));
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE | SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(stamped, newerStamp));
        // an unparsable stamp fails closed as well
        Map<String, String> malformedStamp = ImmutableMap.of(
                SessionVariable.TIME_ZONE, sessionVariable.getTimeZone(),
                SessionVariable.PLAN_SESSION_VAR_FENCE_VERSION, "not-a-version");
        Assertions.assertEquals(SessionVarGuardRewriter.GUARD_TIME_ZONE | SessionVarGuardRewriter.GUARD_OTHER,
                SessionVarGuardRewriter.computeGuardMask(stamped, malformedStamp));
        // pre-fence metadata (no stamp at all, written before time_zone was persisted either) keeps the
        // time-zone guard
        int preFenceMask = SessionVarGuardRewriter.computeGuardMask(stamped,
                ImmutableMap.of(SessionVariable.SQL_MODE, "0"));
        Assertions.assertTrue((preFenceMask & SessionVarGuardRewriter.GUARD_TIME_ZONE) != 0,
                "pre-fence metadata must stay behind the time-zone guard, got " + preFenceMask);
    }
}
