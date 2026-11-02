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
import org.apache.doris.catalog.Function.BinaryType;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Round-84 review fixes on the capture side.
 *
 * <ul>
 *   <li>#1 an all-NULL-valued MAP LITERAL is a NullType producer too, and its whole-leaf marker
 *       has to be retyped to {@code MAP<key, BOOLEAN>} so the Boolean consumer cannot erase
 *       {@code element_at({'x': NULL}, 'x')} together with its markers;</li>
 *   <li>#2 a WINDOW's order direction / null placement / frame is part of the resolved-consumer
 *       identity: {@code OVER(ORDER BY k ASC)} and {@code OVER(ORDER BY k DESC)} live outside the
 *       expression children;</li>
 *   <li>#3 a function's WRITTEN database qualifier is part of that identity as well;</li>
 *   <li>#5 the UDF dependency entry carries the EFFECTIVE resolved scope, so an unqualified
 *       {@code f(k)} under {@code USE db1} and under {@code USE db2} are different entries;</li>
 *   <li>#6 a GROUP BY key that names no base column resolves through the block's OUTPUT, so the
 *       two statements' group keys have to resolve to the same expressions.</li>
 * </ul>
 */
public class SPMRound84PlannerTest {

    // ==================== #1: the all-NULL MAP literal container ====================

    /**
     * {@code k > 0 OR element_at({'x': NULL}, 'x')}: the map LITERAL is one leaf marker, and the
     * MAP overload of element_at reads its VALUE type - NullType here. Without the repair the
     * Boolean consumer replaced the whole call and both markers, and the {@code {'x': TRUE}}
     * variant had no id left to substitute.
     */
    @Test
    public void testAllNullMapLiteralContainerKeepsItsMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at({'x': NULL}, 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanMaps = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getDataType() instanceof MapType
                    && ((MapType) var.getDataType()).getValueType() instanceof BooleanType) {
                booleanMaps++;
            }
        }
        Assertions.assertEquals(1, booleanMaps,
                "the all-NULL map literal is retyped MAP<STRING, BOOLEAN>: "
                        + builder.getPlaceholderExprs());
    }

    /** A map literal with a REAL value keeps its own value type. */
    @Test
    public void testTypedMapLiteralKeepsItsValueType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at({'x': 1}, 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            Assertions.assertFalse(var.getDataType() instanceof MapType
                            && ((MapType) var.getDataType()).getValueType() instanceof BooleanType,
                    "only an all-NULL map is retyped: " + builder.getPlaceholderExprs());
        }
    }

    // ==================== #2: the window specification ====================

    /**
     * Bind {@code SELECT k, row_number() OVER(ORDER BY k ASC) AS x, row_number() OVER(ORDER BY k
     * DESC) AS y FROM t QUALIFY x = 1} with a plan that swaps the two labels. Both QUALIFY texts
     * are `x = 1` and both output expressions are positionally equal, but the bind keeps the row
     * whose ASC row number is 1 (k = 1) while the plan keeps the DESC one (k = 2) - the window
     * SPEC lives outside the expression children.
     */
    @Test
    public void testWindowOrderDirectionIsPartOfTheResolvedConsumer() {
        String bind = "SELECT k, row_number() OVER(ORDER BY k ASC) AS x,"
                + " row_number() OVER(ORDER BY k DESC) AS y FROM t QUALIFY x = 1";
        String plan = "SELECT k, row_number() OVER(ORDER BY k ASC) AS y,"
                + " row_number() OVER(ORDER BY k DESC) AS x FROM t QUALIFY x = 1";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "two windows over the same key with opposite directions are different"
                        + " expressions");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalWindowAliasesAreAccepted() {
        String bind = "SELECT k, row_number() OVER(ORDER BY k ASC) AS x,"
                + " row_number() OVER(ORDER BY k DESC) AS y FROM t QUALIFY x = 1";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    // ==================== #3: the function's written scope ====================

    /**
     * Bind {@code SELECT db1.f(k) AS x, db2.f(k) AS y FROM t HAVING x > 0} with a plan that swaps
     * the labels: db1.f and db2.f are two different functions with one SQL signature (a Java and a
     * Python scalar), so the bind filters db1.f(k) and the plan filters db2.f(k).
     */
    @Test
    public void testQualifiedFunctionScopeIsPartOfTheResolvedConsumer() {
        String bind = "SELECT db1.f(k) AS x, db2.f(k) AS y FROM t HAVING x > 0";
        String plan = "SELECT db1.f(k) AS y, db2.f(k) AS x FROM t HAVING x > 0";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "the two qualified calls read different functions, so the permutation filters"
                        + " another value");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalQualifiedFunctionAliasesAreAccepted() {
        String bind = "SELECT db1.f(k) AS x, db2.f(k) AS y FROM t HAVING x > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    // ==================== #5: the resolved UDF scope in the fingerprint ====================

    /**
     * The SAME unqualified {@code f(k)} resolves in the session's CURRENT database: under
     * {@code USE db1} it is db1.f (returns 1), under {@code USE db2} it is db2.f (returns -1). The
     * two declarations here are IDENTICAL (same class, signature, nullability, volatility), so
     * only the resolved scope separates their dependency entries - without it the frozen db1 plan
     * replayed under a db2 session and returned 1 instead of -1.
     */
    @Test
    public void testUnqualifiedUdfCarriesItsResolvedScope() {
        try {
            String underDb1 = fingerprint(context("db1"), "SELECT f(k) AS c FROM shared.t",
                    udfRegistry("f"));
            String underDb2 = fingerprint(context("db2"), "SELECT f(k) AS c FROM shared.t",
                    udfRegistry("f"));
            Assertions.assertNotEquals(underDb1, underDb2,
                    "an unqualified UDF call means the CURRENT database's function: " + underDb2);
        } finally {
            ConnectContext.remove();
        }
    }

    /** A plain BUILTIN keeps the scope-free entry: its definition cannot depend on the database. */
    @Test
    public void testBuiltinCallStaysScopeFree() {
        try {
            String underDb1 = fingerprint(context("db1"), "SELECT lower(s) AS c FROM shared.t",
                    builtinRegistry("lower"));
            String underDb2 = fingerprint(context("db2"), "SELECT lower(s) AS c FROM shared.t",
                    builtinRegistry("lower"));
            Assertions.assertEquals(underDb1, underDb2,
                    "a builtin resolves identically in every database");
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * SPM's OWN placeholders resolve in NO scope: {@code _spm_const_var} exists only inside the
     * frozen plan text this module renders, so the session's current database must not enter the
     * entry. Attaching it made the identity depend on WHICH session computed the fingerprint - a
     * CREATE without a current database wrote {@code fn:_spm_const_var|none|u=-} while the
     * replaying session (current database db1) computed
     * {@code fn:db1._spm_const_var|none|u=-}, and the post-plan revalidation then refused a
     * healthy placeholder-only baseline (`test_spm_review_round9`: {@code SELECT 1 AS x LIMIT 0}
     * got "replay metadata changed between validation and planning" for a baseline whose whole
     * fingerprint is that one entry).
     */
    @Test
    public void testInternalPlaceholderStaysScopeFree() {
        try {
            String underDb1 = fingerprint(context("db1"),
                    "SELECT _spm_const_var(1, 1) AS c FROM shared.t", builtinRegistry("lower"));
            String underDb2 = fingerprint(context("db2"),
                    "SELECT _spm_const_var(1, 1) AS c FROM shared.t", builtinRegistry("lower"));
            Assertions.assertTrue(underDb1.contains("fn:_spm_const_var|none|u=-"), underDb1);
            Assertions.assertEquals(underDb1, underDb2,
                    "an SPM placeholder resolves in no database scope: " + underDb2);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #6: the GROUP BY consumer ====================

    /**
     * Bind {@code SELECT k AS x, v AS y, count(*) AS n FROM t GROUP BY x} with a plan that swaps
     * the labels: {@code x} names no base column, so BindExpression resolves it through the block's
     * OUTPUT - the bind groups k (one group of two rows) while the plan groups v (two groups of
     * one). Every set-wise contract passes because only the labels differ.
     */
    @Test
    public void testGroupByAliasPermutationIsRejected() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t GROUP BY x";
        String plan = "SELECT k AS y, v AS x, count(*) AS n FROM t GROUP BY x";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "a GROUP BY name that is not a base column groups the block's OUTPUT item, so"
                        + " the permutation groups another expression");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalGroupByAliasesAreAccepted() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t GROUP BY x";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** A GROUP BY over BASE columns keeps its meaning under a label swap. */
    @Test
    public void testGroupByBaseColumnIsAccepted() {
        String bind = "SELECT k AS x, v AS y, count(*) AS n FROM t GROUP BY k";
        String plan = "SELECT k AS y, v AS x, count(*) AS n FROM t GROUP BY k";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "both sides group the base column k");
    }

    // ==================== helpers ====================

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

    /** A registry answering the name-level lookup with ONE ordinary UDF, whatever the scope. */
    private static FunctionRegistry udfRegistry(String name) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of(javaUdf(name)));
        return registry;
    }

    /** A registry in which the name is a BUILTIN (no UDF of that name exists). */
    private static FunctionRegistry builtinRegistry(String name) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Map<String, List<FunctionBuilder>> builtins = new HashMap<>();
        builtins.put(name, List.of(javaUdf(name)));
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(builtins);
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of());
        return registry;
    }

    private static FunctionBuilder javaUdf(String name) {
        FunctionSignature signature =
                FunctionSignature.of(IntegerType.INSTANCE, List.of(StringType.INSTANCE));
        JavaUdf udf = new JavaUdf(name, 1L, "spm_db", BinaryType.JAVA_UDF, signature,
                NullableMode.ALWAYS_NOT_NULLABLE, FunctionVolatility.IMMUTABLE,
                VolatileIdentity.NON_VOLATILE, "/tmp/udf.jar", "sym", "", "", "checksum",
                true, -1L);
        return new JavaUdfBuilder(udf);
    }

    private static String fingerprint(ConnectContext ctx, String sql, FunctionRegistry registry) {
        try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
            Env env = Mockito.mock(Env.class);
            envStatic.when(Env::getCurrentEnv).thenReturn(env);
            Mockito.when(env.getFunctionRegistry()).thenReturn(registry);
            return SPMPlanTreeSupport.schemaFingerprint(ctx, parse(sql));
        }
    }
}
