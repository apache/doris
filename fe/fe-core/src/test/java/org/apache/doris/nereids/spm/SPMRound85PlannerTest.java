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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

/**
 * Round-85 review fixes on the capture / admission side.
 *
 * <ul>
 *   <li>#1 a REPEAT (GROUPING SETS) is a consumer of the block's output aliases exactly like a
 *       GROUP BY, so an alias permutation that changes what a subtotal groups has to be
 *       rejected;</li>
 *   <li>#2 a MATCH's ANALYZER decides how the pattern is tokenized, so it belongs to the
 *       resolved-consumer identity;</li>
 *   <li>#3 an ALIAS UDF body is inlined during analysis - a replay-time context expression
 *       inside it (directly or through another alias) has to be rejected before the capture
 *       folds it into the frozen SQL;</li>
 *   <li>#4 an all-NULL / empty container behind a TYPE-PRESERVING wrapper, and a bare NULL
 *       container under the MAP overload, keep their markers.</li>
 * </ul>
 */
public class SPMRound85PlannerTest {

    // ==================== #1: GROUPING SETS ====================

    /**
     * Bind {@code SELECT k AS x, v AS y, count(*) AS n FROM t GROUP BY GROUPING SETS((x,y),(x))}
     * with a plan that swaps the labels. Every output expression is positionally equal and both
     * texts spell the same grouping sets, but a set member that names no base column binds the
     * block's OUTPUT: the bind subtotals k, the plan subtotals v.
     */
    @Test
    public void testGroupingSetAliasPermutationIsRejected() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((x, y), (x))";
        String plan = "SELECT k AS y, v AS x, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((x, y), (x))";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "a GROUPING SETS member that is not a base column groups the block's OUTPUT"
                        + " item, so the permutation subtotals another expression");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalGroupingSetsAreAccepted() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((x, y), (x))";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** Grouping sets over BASE columns keep their meaning under a label swap. */
    @Test
    public void testGroupingSetOverBaseColumnsIsAccepted() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((k, v), (k))";
        String plan = "SELECT k AS y, v AS x, count(*) AS n FROM t"
                + " GROUP BY GROUPING SETS((k, v), (k))";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "both sides subtotal the base column k");
    }

    // ==================== #2: the MATCH analyzer ====================

    /**
     * {@code s MATCH_ANY 'hello' USING ANALYZER standard} and {@code ... ANALYZER none} over the
     * same column and pattern are different predicates: standard matches 'hello world' as well,
     * none only 'hello'. The two output labels are swapped while the ORDER BY keeps naming x, so
     * the sort resolves to the standard match on the bind side and to the none match on the plan
     * side - the frozen replay then keeps the other row.
     */
    @Test
    public void testMatchAnalyzerIsPartOfTheResolvedSortKey() {
        String bind = "SELECT id, s MATCH_ANY 'hello' USING ANALYZER standard AS x,"
                + " s MATCH_ANY 'hello' USING ANALYZER none AS y FROM t ORDER BY x";
        String plan = "SELECT id, s MATCH_ANY 'hello' USING ANALYZER standard AS y,"
                + " s MATCH_ANY 'hello' USING ANALYZER none AS x FROM t ORDER BY x";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "two matches over the same column and pattern with different analyzers are"
                        + " different expressions");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalMatchAnalyzerIsAccepted() {
        String bind = "SELECT id, s MATCH_ANY 'hello' USING ANALYZER standard AS x,"
                + " s MATCH_ANY 'hello' USING ANALYZER none AS y FROM t ORDER BY x";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    // ==================== #3: the resolved ALIAS body ====================

    /**
     * {@code CREATE ALIAS FUNCTION f() WITH PARAMETER() AS current_user()} is an ordinary call in
     * the parsed bind text; the body only enters the plan when analysis INLINES it, and the
     * creator-context folding then persists the CREATOR's identity into the frozen SQL.
     */
    @Test
    public void testAliasBodyContextExpressionIsDetected() {
        try {
            ConnectContext ctx = context("db1");
            FunctionRegistry registry = aliasRegistry("f", "current_user()");
            Assertions.assertTrue(inAliasBodies(ctx, "SELECT f() AS c FROM t", registry),
                    "the alias body is inlined during analysis: its current_user() has to be"
                            + " rejected before the capture folds it");
        } finally {
            ConnectContext.remove();
        }
    }

    /** A nested alias body ({@code g() -> f() -> current_user()}) is reached transitively. */
    @Test
    public void testNestedAliasBodyContextExpressionIsDetected() {
        try {
            ConnectContext ctx = context("db1");
            FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
            Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.eq("g")))
                    .thenReturn(List.of(alias("g", "f()")));
            Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.eq("f")))
                    .thenReturn(List.of(alias("f", "current_user()")));
            Assertions.assertTrue(inAliasBodies(ctx, "SELECT g() AS c FROM t", registry),
                    "an alias calling another alias must be expanded transitively");
        } finally {
            ConnectContext.remove();
        }
    }

    /** An alias body without a context expression stays accepted. */
    @Test
    public void testAliasBodyWithoutContextExpressionIsAccepted() {
        try {
            ConnectContext ctx = context("db1");
            FunctionRegistry registry = aliasRegistry("f", "1 + 1");
            Assertions.assertFalse(inAliasBodies(ctx, "SELECT f() AS c FROM t", registry),
                    "a pure alias body is freezable like any other expression");
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #4: the container payloads ====================

    /**
     * An EMPTY array literal is ARRAY&lt;NULL_TYPE&gt; as well: {@code element_at([], 1)} is a
     * NullType producer, so the Boolean consumer replaced the call and its marker and the
     * {@code element_at([TRUE], 1)} caller had nothing to substitute.
     */
    @Test
    public void testEmptyArrayContainerKeepsItsMarkers() {
        Assertions.assertEquals(1, booleanArrayMarkers(
                "SELECT k FROM t WHERE k > 0 OR element_at([], 1)"),
                "the empty array container is retyped ARRAY<BOOLEAN>");
    }

    /**
     * The all-NULL array under a TYPE-PRESERVING wrapper: {@code array_sort} declares
     * {@code retArgType(0)}, so {@code element_at(array_sort([NULL]), 1)} is NullType exactly like
     * the direct container - the wrapper used to hide the payload from the repair.
     */
    @Test
    public void testAllNullArrayUnderArraySortKeepsItsMarkers() {
        Assertions.assertEquals(1, booleanArrayMarkers(
                "SELECT k FROM t WHERE k > 0 OR element_at(array_sort([NULL]), 1)"),
                "the all-NULL array inside array_sort is retyped ARRAY<BOOLEAN>");
    }

    /**
     * The all-NULL-valued map under {@code map_values}: the wrapper turns the map's VALUE type into
     * the result's element type, so its payload has to be retyped as well.
     */
    @Test
    public void testAllNullMapUnderMapValuesKeepsItsMarkers() {
        int maps = 0;
        for (SpmConstVar var : markers(
                "SELECT k FROM t WHERE k > 0 OR element_at(map_values({'x': NULL}), 1)")) {
            if (var.getDataType() instanceof MapType
                    && ((MapType) var.getDataType()).getValueType() instanceof BooleanType) {
                maps++;
            }
        }
        Assertions.assertEquals(1, maps,
                "the all-NULL map inside map_values is retyped MAP<STRING, BOOLEAN>");
    }

    /**
     * A BARE NULL container under the MAP overload: {@code element_at(NULL, 'x')} resolved through
     * the NullType-input signature and stayed NullType, so its marker (and the index marker) were
     * erased - the {@code element_at({'x': TRUE}, 'x')} caller then had nothing to substitute.
     */
    @Test
    public void testBareNullMapContainerKeepsItsMarkers() {
        int maps = 0;
        for (SpmConstVar var : markers(
                "SELECT k FROM t WHERE k > 0 OR element_at(NULL, 'x')")) {
            if (var.getDataType() instanceof MapType
                    && ((MapType) var.getDataType()).getValueType() instanceof BooleanType) {
                maps++;
            }
        }
        Assertions.assertEquals(1, maps,
                "the bare NULL container of the MAP overload is typed MAP<STRING, BOOLEAN>");
    }

    // ==================== helpers ====================

    private static int booleanArrayMarkers(String sql) {
        int arrays = 0;
        for (SpmConstVar var : markers(sql)) {
            if (var.getDataType() instanceof ArrayType
                    && ((ArrayType) var.getDataType()).getItemType() instanceof BooleanType) {
                arrays++;
            }
        }
        return arrays;
    }

    /** The scalar placeholder markers of one parameterized statement. */
    private static List<SpmConstVar> markers(String sql) {
        LogicalPlan plan = parse(sql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        List<SpmConstVar> vars = new java.util.ArrayList<>();
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar) {
                vars.add((SpmConstVar) marker.getPlaceholderExpr());
            }
        }
        return vars;
    }

    private static boolean inAliasBodies(ConnectContext ctx, String sql, FunctionRegistry registry) {
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getFunctionRegistry()).thenReturn(registry);
            return SPMPlanTreeSupport.containsReplayContextInAliasBodies(ctx, parse(sql));
        }
    }

    /** A registry answering every name-level lookup with the SAME alias body. */
    private static FunctionRegistry aliasRegistry(String name, String bodySql) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of(alias(name, bodySql)));
        return registry;
    }

    private static AliasUdfBuilder alias(String name, String bodySql) {
        AliasUdf alias = new AliasUdf(name, List.of(),
                new NereidsParser().parseExpression(bodySql), List.of(), Map.of());
        return new AliasUdfBuilder(alias);
    }

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static ConnectContext context(String database) {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase(database);
        ctx.setThreadLocalInfo();
        return ctx;
    }
}
