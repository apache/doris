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
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Round-82 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#1 the resolved-consumer comparison includes SEMANTIC node attributes: a CAST's TARGET
 *       TYPE, so swapping {@code CAST(k AS INT)} and {@code CAST(k AS DOUBLE)} between two
 *       output aliases no longer passes as "the same predicate";</li>
 *   <li>#2 an ordinary UDF's VOLATILITY is part of its definition identity: re-creating the same
 *       function as VOLATILE makes two calls of it distinct, while the frozen plan still folds
 *       {@code f(k) = f(k)} to TRUE;</li>
 *   <li>#3 an ALIAS function's ORDERED FORMAL-PARAMETER list is part of its identity:
 *       {@code WITH PARAMETER(y,x)} maps the arguments the other way round and expands to
 *       {@code concat(b,a)} over the same body text;</li>
 *   <li>#4 the capture-time registry EPOCH fences a query that only calls BUILTINS as well: a
 *       UDF created and dropped inside the capture window leaves both entry sets saying
 *       "builtin" while the frozen plan inlined the alias body;</li>
 *   <li>#5 a LATERAL VIEW generator's TABLE ALIAS is a relation qualifier of its block, so a
 *       directly selected {@code lv.x} keeps the header {@code x};</li>
 *   <li>#6 the array reducers {@code array_min} / {@code array_max} return their input's element
 *       type, so an all-NULL array makes them NullType producers and their markers must be
 *       retyped for the Boolean consumer.</li>
 * </ul>
 */
public class SPMRound82PlannerTest {

    // ==================== #1: the CAST's target type ====================

    /**
     * Bind {@code SELECT CAST(k AS INT) AS x, CAST(k AS DOUBLE) AS y FROM t HAVING x > 0} with a
     * plan that swaps the two labels: the positional output expressions agree, both predicates
     * spell `x > 0`, and the resolved nodes share class and child - only the TARGET TYPE tells
     * them apart. The bind filters {@code CAST(0.5 AS INT) = 0} (row dropped) while the replay
     * filters the DOUBLE value (row kept).
     */
    @Test
    public void testCastTargetTypeIsPartOfTheResolvedConsumer() {
        String bind = "SELECT CAST(k AS INT) AS x, CAST(k AS DOUBLE) AS y FROM t HAVING x > 0";
        String plan = "SELECT CAST(k AS INT) AS y, CAST(k AS DOUBLE) AS x FROM t HAVING x > 0";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "two casts over the same column are different expressions when their TARGET"
                        + " types differ");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalCastAliasesAreAccepted() {
        String bind = "SELECT CAST(k AS INT) AS x, CAST(k AS DOUBLE) AS y FROM t HAVING x > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** A swap between two casts of the SAME target type still carries the column choice. */
    @Test
    public void testCastSwapOverDifferentColumnsIsStillRejected() {
        String bind = "SELECT CAST(k AS INT) AS x, CAST(v AS INT) AS y FROM t HAVING x > 0";
        String plan = "SELECT CAST(k AS INT) AS y, CAST(v AS INT) AS x FROM t HAVING x > 0";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "the two casts read DIFFERENT columns, so the permutation still filters another"
                        + " value");
        String sameColumn = "SELECT CAST(k AS INT) AS x, CAST(k AS INT) AS y FROM t HAVING x > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(
                        parse(sameColumn), parse(sameColumn), sameColumn));
    }

    // ==================== #2: the UDF's volatility ====================

    /**
     * Re-creating the SAME function with another VOLATILITY keeps class, signature, return type
     * and nullability identical, but the optimizer treats the calls differently: a VOLATILE UDF
     * gets a fresh volatile identity per call, so {@code f(k) = f(k)} stays a real comparison
     * (it may return FALSE) while the frozen baseline was simplified to TRUE.
     */
    @Test
    public void testUdfVolatilityIsPartOfTheFingerprint() {
        ConnectContext ctx = context();
        try {
            String immutable = fingerprint(ctx, "SELECT my_udf(k) = my_udf(k) AS c FROM t",
                    udfRegistry(FunctionVolatility.IMMUTABLE));
            String sameAgain = fingerprint(ctx, "SELECT my_udf(k) = my_udf(k) AS c FROM t",
                    udfRegistry(FunctionVolatility.IMMUTABLE));
            Assertions.assertEquals(immutable, sameAgain,
                    "the same definition keeps one identity");

            String volatileDefinition = fingerprint(ctx,
                    "SELECT my_udf(k) = my_udf(k) AS c FROM t",
                    udfRegistry(FunctionVolatility.VOLATILE));
            Assertions.assertNotEquals(immutable, volatileDefinition,
                    "volatility decides whether two calls of the function may collapse, so it"
                            + " must change the dependency identity: " + volatileDefinition);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #3: the alias function's parameter ORDER ====================

    /**
     * {@code CREATE ALIAS FUNCTION f(STRING,STRING) WITH PARAMETER(x,y) AS concat(x,y)} and the
     * same declaration with {@code WITH PARAMETER(y,x)} share the body text, the signature, the
     * saved variables and the recursive function entries - only the ORDERED parameter list
     * differs, and it maps the arguments the other way round ({@code f(a,b)} expands to
     * {@code concat(b,a)}).
     */
    @Test
    public void testAliasParameterOrderIsPartOfTheFingerprint() {
        ConnectContext ctx = context();
        try {
            String xy = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t", aliasRegistry("x", "y"));
            String sameAgain = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t",
                    aliasRegistry("x", "y"));
            Assertions.assertEquals(xy, sameAgain, "the same declaration keeps one identity");

            String yx = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t", aliasRegistry("y", "x"));
            Assertions.assertNotEquals(xy, yx,
                    "the parameter list is the argument MAPPING, so its order must change the"
                            + " identity: " + yx);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #4: the registry epoch covers builtin-only captures ====================

    /**
     * With {@code prefer_udf_over_builtin=true} a new alias UDF SHADOWS a builtin of the same
     * name. A CREATE followed by a DROP inside the capture window leaves both bind-side entry
     * sets saying "builtin lower" although the frozen plan inlined the alias body - only the
     * registry EPOCH records that anything happened, so the fence must not require the endpoint
     * to resolve to a UDF.
     */
    @Test
    public void testBuiltinOnlyCaptureIsFencedByTheRegistryEpoch() {
        ConnectContext ctx = context();
        try {
            LogicalPlan bind = parse("SELECT lower(s) AS x FROM t");
            FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
            Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
            Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                    .thenReturn(List.of());
            Mockito.when(registry.getUdfModifyVersion()).thenReturn(9L);
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                Env env = Mockito.mock(Env.class);
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getFunctionRegistry()).thenReturn(registry);

                // the endpoint sets AGREE: nothing but the epoch remembers the shadowing alias
                Set<String> snapshot = SPMPlanTreeSupport.bindFunctionDependencyEntries(ctx, bind);
                Assertions.assertFalse(snapshot.isEmpty(), "the bind text calls a function");
                Assertions.assertThrows(IllegalStateException.class,
                        () -> SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries(
                                ctx, bind, "SELECT upper(s) AS x FROM t", Map.of(),
                                snapshot, 8L),
                        "a registry that moved inside the capture window must fail the CREATE"
                                + " even when both endpoints resolve to the same builtin");
            }
        } finally {
            ConnectContext.remove();
        }
    }

    /** A query that calls NO function keeps skipping the epoch check (nothing can shadow it). */
    @Test
    public void testFunctionlessCaptureIsNotFencedByTheRegistryEpoch() {
        ConnectContext ctx = context();
        try {
            LogicalPlan bind = parse("SELECT k AS x FROM t");
            FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
            Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
            Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                    .thenReturn(List.of());
            Mockito.when(registry.getUdfModifyVersion()).thenReturn(9L);
            try (MockedStatic<Env> envStatic = Mockito.mockStatic(Env.class)) {
                Env env = Mockito.mock(Env.class);
                envStatic.when(Env::getCurrentEnv).thenReturn(env);
                Mockito.when(env.getFunctionRegistry()).thenReturn(registry);
                Assertions.assertDoesNotThrow(
                        () -> SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries(
                                ctx, bind, "SELECT k AS x FROM t", Map.of(),
                                SPMPlanTreeSupport.bindFunctionDependencyEntries(ctx, bind), 8L),
                        "no function call means no definition can have been inlined");
            }
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #5: the generator's table alias ====================

    /**
     * {@code SELECT lv.x, k + 1 AS z FROM t LATERAL VIEW explode_numbers(t.n) lv AS x}: the
     * generated column binds under the generator's TABLE ALIAS, so `lv.x` is a
     * relation-qualified column (header `x`) and NOT a struct-field path. Leaving the qualifier
     * out of the block namespace renamed the CORRECT frozen header to `lv.x` even for the
     * identical query.
     */
    @Test
    public void testLateralViewQualifierIsPartOfTheBlockNamespace() {
        LogicalPlan user = parse("SELECT lv.x, k + 1 AS z FROM t"
                + " LATERAL VIEW explode_numbers(t.n) lv AS x");
        LogicalPlan frozenReplay = parse("SELECT x, k + 1 AS z FROM t");

        Assertions.assertEquals(List.of("x", "z"), projectLabels(frozenReplay),
                "precondition: the frozen tree reports the caller-visible headers");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, user);
        Assertions.assertEquals(List.of("x", "z"), projectLabels(aligned),
                "a generated column selected THROUGH its generator alias keeps its own header");
    }

    /** A struct-field path over a real column still keeps its full name. */
    @Test
    public void testStructFieldPathStillKeepsItsFullName() {
        LogicalPlan user = parse("SELECT s.a, k + 1 AS z FROM t");
        LogicalPlan frozenReplay = parse("SELECT s.a, k + 1 AS z FROM t");
        LogicalPlan aligned = SPMPlanTreeSupport.alignRootOutputLabels(frozenReplay, user);
        // the ITEM TEXT is what keeps the path: alignment must NOT rewrite `s.a` to `a`, because
        // `s` names no relation of the block (the 1-arg label rule the assertion helper uses
        // keeps the last component, so only the tree itself can tell the two apart)
        Assertions.assertTrue(projectItems(aligned).get(0).contains("s.a"),
                "`s` names no relation of the block, so `s.a` is a FIELD PATH and keeps its"
                        + " full name: " + projectItems(aligned));
    }

    // ==================== #6: the array reducers ====================

    /**
     * {@code k > 0 OR array_min(array(NULL))}: ARRAY_MIN returns its input's element type, so the
     * all-NULL array makes the whole call NullType. The unrecognized reducer left the NULL marker
     * typed ARRAY&lt;NULL_TYPE&gt;, the Boolean consumer replaced the call (markers included) and
     * the {@code array_min(array(TRUE))} caller replayed NULL for k = -1.
     */
    @Test
    public void testArrayMinOverAllNullArrayKeepsBooleanMarkers() {
        assertBooleanNullMarkers("SELECT k FROM t WHERE k > 0 OR array_min(array(NULL))");
    }

    /** ARRAY_MAX has the same element-typed signature (see the ARRAY_MIN case). */
    @Test
    public void testArrayMaxOverAllNullArrayKeepsBooleanMarkers() {
        assertBooleanNullMarkers("SELECT k FROM t WHERE k > 0 OR array_max(array(NULL))");
    }

    /** A reducer over a TYPED array keeps its own element type: only NullType is retyped. */
    @Test
    public void testArrayMinOverATypedArrayKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR array_min(array(NULL, 1))");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getValue().isNullLiteral() && var.getDataType() instanceof BooleanType) {
                Assertions.fail("only an all-NULL array is retyped: "
                        + builder.getPlaceholderExprs());
            }
        }
    }

    // ==================== helpers ====================

    /** One all-NULL array inside a Boolean consumer must leave BOOLEAN-typed NULL markers. */
    private static void assertBooleanNullMarkers(String sql) {
        LogicalPlan plan = parse(sql);
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        int booleanNullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getValue().isNullLiteral() && var.getDataType() instanceof BooleanType) {
                booleanNullMarkers++;
            }
        }
        Assertions.assertTrue(booleanNullMarkers > 0,
                "the all-NULL array makes the reducer a NullType producer, so its markers have"
                        + " to be BOOLEAN already: " + builder.getPlaceholderExprs());
    }

    /** The root projection's labels, read with the SPM label rule (unbound items included). */
    private static List<String> projectLabels(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> labels = new ArrayList<>();
        for (NamedExpression item : ((LogicalProject<?>) node).getProjects()) {
            labels.add(SpmPlanTreeSupportTestAccess.outputLabel(item));
        }
        return labels;
    }

    /** The root projection's item TEXTS (an unbound item has no renderable label). */
    private static List<String> projectItems(LogicalPlan root) {
        Plan node = root;
        while (!(node instanceof LogicalProject)) {
            node = node.child(0);
        }
        List<String> items = new ArrayList<>();
        for (NamedExpression item : ((LogicalProject<?>) node).getProjects()) {
            items.add(item.toString());
        }
        return items;
    }

    private static LogicalPlan parse(String sql) {
        Plan parsed = new NereidsParser().parseSingle(sql);
        Assertions.assertTrue(parsed instanceof LogicalPlan, "not a plan: " + sql);
        return (LogicalPlan) parsed;
    }

    private static ConnectContext context() {
        ConnectContext ctx = new ConnectContext();
        ctx.setStatementContext(new StatementContext());
        ctx.setSessionVariable(new SessionVariable());
        ctx.setDatabase("spm_db");
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** A registry exposing ONE same-name Java UDF with the given VOLATILITY. */
    private static FunctionRegistry udfRegistry(FunctionVolatility volatility) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of(javaUdf(volatility)));
        return registry;
    }

    /** A registry exposing the ALIAS db1.f whose formal parameters are given IN ORDER. */
    private static FunctionRegistry aliasRegistry(String first, String second) {
        AliasUdf alias = new AliasUdf("f", List.of(IntegerType.INSTANCE),
                new NereidsParser().parseExpression("concat(x, y)"),
                List.of(first, second), Map.of());
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of(new AliasUdfBuilder(alias)));
        return registry;
    }

    private static FunctionBuilder javaUdf(FunctionVolatility volatility) {
        FunctionSignature signature =
                FunctionSignature.of(IntegerType.INSTANCE, List.of(IntegerType.INSTANCE));
        JavaUdf udf = new JavaUdf("my_udf", 1L, "spm_db", BinaryType.JAVA_UDF, signature,
                NullableMode.ALWAYS_NOT_NULLABLE, volatility, VolatileIdentity.NON_VOLATILE,
                "/tmp/udf.jar", "sym", "", "", "checksum", true, -1L);
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
