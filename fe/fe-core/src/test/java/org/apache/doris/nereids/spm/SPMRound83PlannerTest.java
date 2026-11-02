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
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
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
 * Round-83 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#1 an aggregate's DISTINCT flag joins the resolved-consumer identity: {@code sum(k)} and
 *       {@code sum(DISTINCT k)} are the same class, name and child, so an alias permutation that
 *       swapped them filtered different aggregates;</li>
 *   <li>#2 a BARE NULL container of {@code element_at} (whose NullType-input signature returns
 *       NullType) is retyped to a one-element ARRAY&lt;BOOLEAN&gt;, so the Boolean consumer no longer
 *       erases the call with both markers;</li>
 *   <li>#3 {@code map_values} follows its map's VALUE type into an array of it, so it is an
 *       all-NULL array when the map is all-NULL-valued;</li>
 *   <li>#5 an alias body's identity carries the qualified inner calls at their STRUCTURAL
 *       POSITIONS, not as an unordered entry set.</li>
 * </ul>
 */
public class SPMRound83PlannerTest {

    // ==================== #1: the aggregate's DISTINCT flag ====================

    /**
     * Bind {@code SELECT sum(k) AS x, sum(DISTINCT k) AS y FROM t HAVING x > 1} with a plan that
     * swaps the two labels: the positional outputs agree, both predicates spell `x > 1`, and the
     * resolved aggregates share class, name and child - only DISTINCT tells them apart. With
     * t(k) = {1,1} the bind filters 2 > 1 (keeps the row) while the replay filters 1 > 1.
     */
    @Test
    public void testAggregateDistinctIsPartOfTheResolvedConsumer() {
        String bind = "SELECT sum(k) AS x, sum(DISTINCT k) AS y FROM t HAVING x > 1";
        String plan = "SELECT sum(k) AS y, sum(DISTINCT k) AS x FROM t HAVING x > 1";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "sum(k) and sum(DISTINCT k) are different expressions even though class, name"
                        + " and child agree");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalDistinctAggregatesAreAccepted() {
        String bind = "SELECT sum(k) AS x, sum(DISTINCT k) AS y FROM t HAVING x > 1";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** Two aggregates over the same child and the same DISTINCT flag stay interchangeable. */
    @Test
    public void testSwappedIdenticalAggregatesAreRejectedOnTheColumn() {
        String bind = "SELECT sum(k) AS x, sum(v) AS y FROM t HAVING x > 1";
        String plan = "SELECT sum(k) AS y, sum(v) AS x FROM t HAVING x > 1";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "the two aggregates read different columns, so the permutation still changes"
                        + " the filtered value");
    }

    // ==================== #2: the bare NULL container ====================

    /**
     * {@code k > 0 OR element_at(NULL, 1)}: the container is a BARE NULL, and ElementAt's
     * dedicated NullType-input signature makes the whole call NullType - the Boolean consumer then
     * replaced it (with both markers) by a typed constant, so the
     * {@code element_at([TRUE], 1)} variant had no id to substitute and the replay returned NULL
     * for k = -1. The container marker is retyped to a one-element ARRAY&lt;BOOLEAN&gt; because the
     * INTEGER index selects the ARRAY overload.
     */
    @Test
    public void testBareNullContainerOfElementAtKeepsItsMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(NULL, 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanArrays = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getDataType() instanceof ArrayType
                    && ((ArrayType) var.getDataType()).getItemType() instanceof BooleanType) {
                booleanArrays++;
            }
        }
        Assertions.assertEquals(1, booleanArrays,
                "the bare NULL container is retyped ARRAY<BOOLEAN>: "
                        + builder.getPlaceholderExprs());
    }

    /**
     * A STRING index selects the MAP overload, whose container must stay a map: the bare NULL is
     * left alone (typing it as an array would be a type error on replay).
     */
    @Test
    public void testBareNullContainerWithAStringIndexIsLeftAlone() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(NULL, 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            Assertions.assertFalse(var.getDataType() instanceof ArrayType,
                    "a MAP lookup's container must not be turned into an array: "
                            + builder.getPlaceholderExprs());
        }
    }

    /** A bare NULL container under an array reducer has no map overload and is retyped as well. */
    @Test
    public void testBareNullContainerOfAnArrayReducerIsRetyped() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR array_min(NULL)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        boolean retyped = false;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getDataType() instanceof ArrayType
                    && ((ArrayType) var.getDataType()).getItemType() instanceof BooleanType) {
                retyped = true;
            }
        }
        Assertions.assertTrue(retyped,
                "array_min(NULL) is NullType, so its container marker has to be an array of"
                        + " BOOLEAN already: " + builder.getPlaceholderExprs());
    }

    /** A BRACKET all-NULL array under an array reducer is retyped like an element_at container. */
    @Test
    public void testBracketAllNullArrayUnderAnArrayReducerIsRetyped() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR array_max([NULL])");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        int booleanArrays = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getDataType() instanceof ArrayType
                    && ((ArrayType) var.getDataType()).getItemType() instanceof BooleanType) {
                booleanArrays++;
            }
        }
        Assertions.assertEquals(1, booleanArrays,
                "the bracket literal is ONE marker, so the reducer's container repair has to"
                        + " retype it: " + builder.getPlaceholderExprs());
    }

    // ==================== #3: map_values ====================

    /**
     * {@code k > 0 OR element_at(map_values(map('x', NULL)), 1)}: MAP_VALUES turns the map's
     * VALUE type into an ARRAY of it, so an all-NULL-valued map makes the array
     * ARRAY&lt;NULL_TYPE&gt; and element_at NullType - the unrecognized wrapper left the value
     * marker NullType and {@code visitOr} erased the whole call.
     */
    @Test
    public void testMapValuesOverAnAllNullMapKeepsBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0"
                + " OR element_at(map_values(map('x', NULL)), 1)");
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
                "the all-NULL map value makes the extracted array NullType, so its markers have"
                        + " to be BOOLEAN already: " + builder.getPlaceholderExprs());
    }

    /** A map with a real value keeps its own value type. */
    @Test
    public void testMapValuesOverATypedMapKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0"
                + " OR element_at(map_values(map('x', 1)), 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (var.getValue().isNullLiteral() && var.getDataType() instanceof BooleanType) {
                Assertions.fail("only an all-NULL map is retyped: "
                        + builder.getPlaceholderExprs());
            }
        }
    }

    // ==================== #5: qualified inner calls at their positions ====================

    /**
     * {@code ALIAS f(a,b) AS concat(db1.g(a), db2.g(b))} and the same declaration with the two
     * qualified calls SWAPPED: the body text is identical because UnboundFunction#toSql drops the
     * database, and the resolved entry SET is identical too ({concat, db1.g, db2.g}) - only the
     * structural POSITION of each qualified call separates them, and it decides which argument
     * each function consumes.
     */
    @Test
    public void testAliasBodyKeepsQualifiedCallsAtTheirPositions() {
        ConnectContext ctx = context();
        try {
            String ab = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t",
                    aliasOverQualifiedCalls(false));
            String sameAgain = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t",
                    aliasOverQualifiedCalls(false));
            Assertions.assertEquals(ab, sameAgain, "the same declaration keeps one identity");

            String ba = fingerprint(ctx, "SELECT db1.f(a, b) AS c FROM t",
                    aliasOverQualifiedCalls(true));
            Assertions.assertNotEquals(ab, ba,
                    "the qualified calls occupy different positions, so the expansion maps the"
                            + " arguments the other way round: " + ba);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== helpers ====================

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

    /** A registry in which db1.f is an ALIAS over db1.g / db2.g (swapped when requested). */
    private static FunctionRegistry aliasOverQualifiedCalls(boolean swapped) {
        String body = swapped ? "concat(db2.g(a), db1.g(b))" : "concat(db1.g(a), db2.g(b))";
        AliasUdf alias = new AliasUdf("f", List.of(StringType.INSTANCE, StringType.INSTANCE),
                new NereidsParser().parseExpression(body), List.of("a", "b"), Map.of());
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of());
        Mockito.when(registry.findUdfBuilder(Mockito.eq("db1"), Mockito.eq("f")))
                .thenReturn(List.of(new AliasUdfBuilder(alias)));
        Mockito.when(registry.findUdfBuilder(Mockito.eq("db1"), Mockito.eq("g")))
                .thenReturn(List.of(identityUdf("g")));
        Mockito.when(registry.findUdfBuilder(Mockito.eq("db2"), Mockito.eq("g")))
                .thenReturn(List.of(identityUdf("g")));
        return registry;
    }

    private static FunctionBuilder identityUdf(String name) {
        FunctionSignature signature =
                FunctionSignature.of(StringType.INSTANCE, List.of(StringType.INSTANCE));
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
