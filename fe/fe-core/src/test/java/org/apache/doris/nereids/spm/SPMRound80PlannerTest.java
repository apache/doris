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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
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
 * Round-80 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#1 the MAP overload of `element_at` takes its type from the map's VALUE type: an
 *       all-NULL-valued {@code map('x', NULL)} makes the call a NullType producer exactly
 *       like an all-NULL array, so the value marker has to be BOOLEAN;</li>
 *   <li>#2 an output label a top-level ORDER BY key CONSUMES is semantic, not cosmetic: the
 *       sort resolves the block's output name before the base columns, so renaming the item
 *       changes what the sort reads;</li>
 *   <li>#4 a bracket ARRAY literal cannot hold element markers (it is ONE literal), so its
 *       marker payload has to carry the array type the consumer needs instead;</li>
 *   <li>#5 an ordinary (Java / Python) UDF's nullability is an INSTANCE property, and the
 *       dependency fingerprint has to include it.</li>
 * </ul>
 */
public class SPMRound80PlannerTest {

    // ==================== #1: MAP element producers ====================

    /**
     * `k > 0 OR element_at(map('x', NULL), 'x')`: CreateMap derives MAP&lt;STRING, NULL_TYPE&gt;
     * and element_at's MAP signature returns that value type, so the WHOLE call is NullType and
     * the Boolean consumer replaces it with a typed constant NULL - unless the value's marker is
     * BOOLEAN already, the `map('x', TRUE)` variant has nothing to substitute.
     */
    @Test
    public void testAllNullMapValueKeepsBooleanMarkers() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(map('x', NULL), 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanValueMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            Expression placeholder = marker.getPlaceholderExpr();
            if (!(placeholder instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar marker2 = (SpmConstVar) placeholder;
            if (!marker2.getValue().isNullLiteral()) {
                continue;
            }
            Assertions.assertTrue(marker2.getDataType() instanceof BooleanType,
                    "the map VALUE marker is consumed by a Boolean consumer, got: "
                            + marker2.getDataType());
            booleanValueMarkers++;
        }
        Assertions.assertEquals(1, booleanValueMarkers,
                "only the map's VALUE is retyped: " + builder.getPlaceholderExprs());
    }

    /** A MAP's KEY keeps its own type - a BOOLEAN key would change the map's signature. */
    @Test
    public void testAllNullMapKeyKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(map(NULL, NULL), 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int nullMarkers = 0;
        int booleanNullMarkers = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (!(marker.getPlaceholderExpr() instanceof SpmConstVar)) {
                continue;
            }
            SpmConstVar var = (SpmConstVar) marker.getPlaceholderExpr();
            if (!var.getValue().isNullLiteral()) {
                continue;
            }
            nullMarkers++;
            if (var.getDataType() instanceof BooleanType) {
                booleanNullMarkers++;
            }
        }
        Assertions.assertEquals(2, nullMarkers,
                "both map operands are parameterized: " + builder.getPlaceholderExprs());
        Assertions.assertEquals(1, booleanNullMarkers,
                "only the map's VALUE is retyped BOOLEAN: " + builder.getPlaceholderExprs());
    }

    /** A map with a real value keeps its own value type (only the NullType producer is retyped). */
    @Test
    public void testNonNullMapValueKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at(map('x', 1), 'x')");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getValue().isNullLiteral()) {
                Assertions.fail("no NULL marker may be typed: " + builder.getPlaceholderExprs());
            }
        }
    }

    // ==================== #4: bracket ARRAY containers ====================

    /**
     * `k > 0 OR element_at([NULL], 1)`: the bracket array is ONE literal marker, so its own type
     * is what `element_at` reads. A marker still typed ARRAY&lt;NULL_TYPE&gt; left the call
     * NullType, the Boolean consumer replaced the whole call and the marker disappeared from the
     * frozen text - the `element_at([TRUE], 1)` variant then had no id to substitute.
     */
    @Test
    public void testBracketAllNullArrayMarkerKeepsItsMarker() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at([NULL], 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);

        int booleanArrays = 0;
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getDataType() instanceof ArrayType) {
                ArrayType type = (ArrayType) ((SpmConstVar) marker.getPlaceholderExpr()).getDataType();
                Assertions.assertTrue(type.getItemType() instanceof BooleanType,
                        "the all-NULL array container is retyped BOOLEAN, got: " + type);
                booleanArrays++;
            }
        }
        Assertions.assertEquals(1, booleanArrays,
                "exactly the array literal is retyped: " + builder.getPlaceholderExprs());
    }

    /** A non-NULL bracket array keeps its element type. */
    @Test
    public void testBracketNonNullArrayMarkerKeepsItsElementType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0 OR element_at([1], 1)");
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        SPMPlanTreeSupport.transform(plan, builder);
        for (PlaceholderExpr marker : builder.getPlaceholderExprs()) {
            if (marker.getPlaceholderExpr() instanceof SpmConstVar
                    && ((SpmConstVar) marker.getPlaceholderExpr()).getDataType() instanceof ArrayType) {
                ArrayType type = (ArrayType) ((SpmConstVar) marker.getPlaceholderExpr()).getDataType();
                Assertions.assertFalse(type.getItemType() instanceof BooleanType,
                        "only an all-NULL array is retyped, got: " + type);
            }
        }
    }

    // ==================== #2: ORDER BY consumes output aliases ====================

    /**
     * Bind {@code SELECT k AS x FROM t ORDER BY abs(x)} with plan {@code k AS y}: both sort
     * TEXTS are `abs(x)`, but the bind sorts the ALIASED value while the plan falls through to
     * the base column t.x - the replay returns the other row, with no error.
     */
    @Test
    public void testOrderByAliasRenameIsRejected() {
        String bind = "SELECT k AS x FROM t ORDER BY abs(x) LIMIT 1";
        String plan = "SELECT k AS y FROM t ORDER BY abs(x) LIMIT 1";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "an ORDER BY name that addresses the block's output alias cannot be renamed");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalOrderByAliasIsAccepted() {
        String bind = "SELECT k AS x FROM t ORDER BY abs(x) LIMIT 1";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    /** A sort over a BASE column keeps its meaning under a label change. */
    @Test
    public void testOrderByOnABaseColumnIsAccepted() {
        String bind = "SELECT k AS x FROM t ORDER BY abs(k) LIMIT 1";
        String plan = "SELECT k AS y FROM t ORDER BY abs(k) LIMIT 1";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "`k` addresses the base column on both sides");
    }

    /** Without an ORDER BY consumer the label really is only a label. */
    @Test
    public void testLabelRenameWithoutAnOrderByConsumerIsAccepted() {
        String bind = "SELECT k AS x FROM t";
        String plan = "SELECT k AS y FROM t";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "a pure output label may differ between bind and plan");
    }

    // ==================== #5: instance nullability of an ordinary UDF ====================

    /**
     * Re-creating the SAME Java UDF with another nullability keeps the class name and the
     * signature identical, but the optimizer treats it differently (an ALWAYS_NOT_NULLABLE UDF
     * removes a null-rejecting filter, an ALWAYS_NULLABLE one keeps an outer join alive). The
     * dependency fingerprint must separate the two definitions.
     */
    @Test
    public void testUdfInstanceNullableModeIsPartOfTheFingerprint() {
        ConnectContext ctx = context();
        try {
            FunctionRegistry registry = udfRegistry("my_udf", NullableMode.ALWAYS_NULLABLE);
            String nullable = fingerprint(ctx, "SELECT my_udf(1)", registry);
            FunctionRegistry sameAgain = udfRegistry("my_udf", NullableMode.ALWAYS_NULLABLE);
            Assertions.assertEquals(nullable, fingerprint(ctx, "SELECT my_udf(1)", sameAgain),
                    "the same definition keeps one identity");

            FunctionRegistry recreated = udfRegistry("my_udf", NullableMode.ALWAYS_NOT_NULLABLE);
            String notNullable = fingerprint(ctx, "SELECT my_udf(1)", recreated);
            Assertions.assertNotEquals(nullable, notNullable,
                    "the instance's nullability changes what the optimizer may do with the"
                            + " call, so it must change the dependency identity: " + notNullable);
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

    /** A registry exposing ONE same-name Java UDF with the given instance nullability. */
    private static FunctionRegistry udfRegistry(String name, NullableMode mode) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Map<String, List<FunctionBuilder>> builtins = new HashMap<>();
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(builtins);
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of(javaUdf(name, mode)));
        return registry;
    }

    private static FunctionBuilder javaUdf(String name, NullableMode mode) {
        FunctionSignature signature =
                FunctionSignature.of(IntegerType.INSTANCE, List.of(IntegerType.INSTANCE));
        JavaUdf udf = new JavaUdf(name, 1L, "spm_db", BinaryType.JAVA_UDF, signature, mode,
                FunctionVolatility.IMMUTABLE, VolatileIdentity.NON_VOLATILE,
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
