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
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;

/**
 * Round-86 review fixes on the capture / admission side.
 *
 * <ul>
 *   <li>#1 an INTERVAL's unit is part of the resolved-consumer identity (DAY vs MONTH over the
 *       same value 1);</li>
 *   <li>#2 a LAMBDA's ordered parameter names are part of it as well ({@code (p,q)} vs
 *       {@code (q,p)} over the same body);</li>
 *   <li>#3 the executable definition of an ordinary UDF (id / checksum / location) belongs to its
 *       dependency entry, because the capture may fold a constant call into a literal;</li>
 *   <li>#5 a DERIVED OUTPUT carrying an all-NULL container keeps its markers when an upper block
 *       consumes it as a Boolean operand.</li>
 * </ul>
 */
public class SPMRound86PlannerTest {

    // ==================== #1: the interval unit ====================

    /**
     * {@code d + INTERVAL 1 DAY} and {@code d + INTERVAL 1 MONTH} are the same class over the same
     * child (1); only the UNIT differs. An alias permutation between them made the bind HAVING
     * filter days_add's value while the frozen replay filtered months_add's.
     */
    @Test
    public void testIntervalUnitIsPartOfTheResolvedConsumer() {
        String bind = "SELECT d + INTERVAL 1 DAY AS x, d + INTERVAL 1 MONTH AS y FROM t"
                + " HAVING x < '2020-01-15'";
        String plan = "SELECT d + INTERVAL 1 DAY AS y, d + INTERVAL 1 MONTH AS x FROM t"
                + " HAVING x < '2020-01-15'";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "two intervals over the same value with different units are different"
                        + " expressions");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalIntervalUnitsAreAccepted() {
        String bind = "SELECT d + INTERVAL 1 DAY AS x, d + INTERVAL 1 MONTH AS y FROM t"
                + " HAVING x < '2020-01-15'";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    // ==================== #2: the lambda parameter mapping ====================

    /**
     * {@code array_map((p,q)->p-q, a, b)} and {@code array_map((q,p)->p-q, a, b)} have the same
     * parsed children (the body p-q); the names bind to the inputs BY POSITION, so the first
     * subtracts a-b and the second b-a.
     */
    @Test
    public void testLambdaParameterOrderIsPartOfTheResolvedConsumer() {
        String bind = "SELECT array_map((p, q) -> p - q, a, b) AS x,"
                + " array_map((q, p) -> p - q, a, b) AS y FROM t HAVING element_at(x, 1) > 0";
        String plan = "SELECT array_map((p, q) -> p - q, a, b) AS y,"
                + " array_map((q, p) -> p - q, a, b) AS x FROM t HAVING element_at(x, 1) > 0";
        Assertions.assertThrows(AnalysisException.class,
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(plan), bind),
                "the same lambda body over swapped parameter names means another expression");
    }

    /** The same statement on both sides stays accepted. */
    @Test
    public void testIdenticalLambdaParameterOrderIsAccepted() {
        String bind = "SELECT array_map((p, q) -> p - q, a, b) AS x,"
                + " array_map((q, p) -> p - q, a, b) AS y FROM t HAVING element_at(x, 1) > 0";
        Assertions.assertDoesNotThrow(
                () -> SPMPlanTreeSupport.rejectManualPlanDivergence(parse(bind), parse(bind), bind));
    }

    // ==================== #3: the UDF executable definition ====================

    /**
     * The capture may fold an IMMUTABLE constant call into a literal (BE folding accepts
     * {@code isConstant()}, and {@code Udf.foldable} only stops the FE folder), so the frozen SQL
     * of {@code SELECT * REPLACE(g(1) AS x) FROM t} contains no call at all. Re-creating g with
     * another implementation must therefore change its dependency entry.
     */
    @Test
    public void testUdfImplementationChecksumIsPartOfTheDependencyEntry() {
        try {
            String first = fingerprint(context(), "SELECT g(1) AS c FROM shared.t",
                    udfRegistry("g", "checksum-a", "sym-a"));
            String second = fingerprint(context(), "SELECT g(1) AS c FROM shared.t",
                    udfRegistry("g", "checksum-b", "sym-a"));
            Assertions.assertNotEquals(first, second,
                    "another implementation must invalidate the frozen constant: " + second);
        } finally {
            ConnectContext.remove();
        }
    }

    /** The very same definition keeps the entry stable. */
    @Test
    public void testIdenticalUdfImplementationKeepsTheDependencyEntry() {
        try {
            String first = fingerprint(context(), "SELECT g(1) AS c FROM shared.t",
                    udfRegistry("g", "checksum-a", "sym-a"));
            String second = fingerprint(context(), "SELECT g(1) AS c FROM shared.t",
                    udfRegistry("g", "checksum-a", "sym-a"));
            Assertions.assertEquals(first, second,
                    "an unchanged definition must keep its entry: " + second);
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #5: the derived container output ====================

    /**
     * {@code SELECT k, [NULL] AS a FROM t} exposes an ARRAY&lt;NULL_TYPE&gt; output, and the upper
     * block's {@code element_at(a, 1)} is NullType: the Boolean consumer then erased the whole
     * call together with the markers, so the {@code [TRUE]} variant replayed NULL. The payload is
     * retyped even though the projection is not itself a Boolean context.
     */
    @Test
    public void testDerivedArrayOutputKeepsItsMarkers() {
        LogicalPlan plan = parse("SELECT k, k > 0 OR element_at(a, 1) AS c"
                + " FROM (SELECT k, [NULL] AS a FROM t) x");
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
                "the derived all-NULL array output is retyped ARRAY<BOOLEAN>: "
                        + builder.getPlaceholderExprs());
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
        ctx.setDatabase("db1");
        ctx.setThreadLocalInfo();
        return ctx;
    }

    /** A registry answering the name-level lookup with ONE ordinary Java UDF. */
    private static FunctionRegistry udfRegistry(String name, String checksum, String symbol) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.<FunctionBuilder>of(javaUdf(name, checksum, symbol)));
        return registry;
    }

    private static JavaUdfBuilder javaUdf(String name, String checksum, String symbol) {
        FunctionSignature signature =
                FunctionSignature.of(IntegerType.INSTANCE, List.of(StringType.INSTANCE));
        JavaUdf udf = new JavaUdf(name, 1L, "spm_db", BinaryType.JAVA_UDF, signature,
                NullableMode.ALWAYS_NOT_NULLABLE, FunctionVolatility.IMMUTABLE,
                VolatileIdentity.NON_VOLATILE, "/tmp/udf.jar", symbol, "", "", checksum,
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
