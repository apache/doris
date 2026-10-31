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
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.placeholder.PlaceholderExpr;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.VolatileIdentity;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.AliasUdfBuilder;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdfBuilder;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
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
 * Round-81 review fixes on the capture / replay side.
 *
 * <ul>
 *   <li>#1 a TYPE-PRESERVING array transformation ({@code array_sort} / {@code array_distinct},
 *       both {@code retArgType(0)}) over an all-NULL array is an all-NULL array as well, so the
 *       markers inside it must be retyped for the Boolean consumer instead of being erased
 *       together with the {@code element_at} call;</li>
 *   <li>#2 the ALIAS-body recursion identity is the RESOLVED SCOPE + name, not the bare name:
 *       {@code ALIAS db1.f(x) = db2.f(x)} used to be read as a self-cycle, so db2.f's definition
 *       (its declared RETURN TYPE in particular) never entered the fingerprint;</li>
 *   <li>#3 the GROUP BY mirror key includes the qualified function / column identities, so a
 *       GROUP BY list that groups by {@code db1.f(k,1)} AND {@code db2.f(k,1)} no longer maps
 *       both onto one key and rewrites the {@code db1.f} select item with the {@code db2.f}
 *       group key.</li>
 * </ul>
 */
public class SPMRound81PlannerTest {

    // ==================== #1: type-preserving array transformations ====================

    /**
     * {@code k > 0 OR element_at(array_sort(array(NULL)), 1)}: ARRAY_SORT declares
     * {@code retArgType(0)}, so the wrapper keeps ARRAY&lt;NULL_TYPE&gt; and element_at returns
     * NullType. The unrecognized wrapper left the NULL marker typed ARRAY&lt;NULL_TYPE&gt;, the
     * Boolean consumer replaced the whole element_at call (markers included) with a typed
     * constant, and the {@code array_sort(array(TRUE))} variant had nothing left to substitute -
     * the frozen replay returned NULL for k = -1 where the caller returns TRUE.
     */
    @Test
    public void testArraySortOverAllNullArrayKeepsBooleanMarkers() {
        assertBooleanNullMarkers("SELECT k FROM t WHERE k > 0"
                + " OR element_at(array_sort(array(NULL)), 1)");
    }

    /** ARRAY_DISTINCT has the same type-preserving signature (see the ARRAY_SORT case). */
    @Test
    public void testArrayDistinctOverAllNullArrayKeepsBooleanMarkers() {
        assertBooleanNullMarkers("SELECT k FROM t WHERE k > 0"
                + " OR element_at(array_distinct(array(NULL)), 1)");
    }

    /** A transformation over a TYPED array keeps its own element type: only NullType is retyped. */
    @Test
    public void testArraySortWithATypedPartKeepsItsMarkerType() {
        LogicalPlan plan = parse("SELECT k FROM t WHERE k > 0"
                + " OR element_at(array_sort(array(NULL, 1)), 1)");
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

    // ==================== #2: the alias body's function SCOPE ====================

    /**
     * {@code ALIAS db1.f(x) = db2.f(x)}: the body calls an ORDINARY UDF that merely shares the
     * alias's NAME. Name-keyed recursion declared that body a self-cycle, returned
     * {@code fn:f|cycle} and never inspected db2.f, so re-creating db2.f with another DECLARED
     * return type kept the fingerprint identical - the frozen plan keeps the constant the old
     * type folded, exactly the hole the return-type identity closed for direct calls.
     */
    @Test
    public void testAliasBodyResolvesTheQualifiedUdfIdentity() {
        ConnectContext ctx = context();
        try {
            String tinyint = fingerprint(ctx, "SELECT db1.f(k) FROM t",
                    aliasOverDb2(org.apache.doris.nereids.types.TinyIntType.INSTANCE));
            String sameAgain = fingerprint(ctx, "SELECT db1.f(k) FROM t",
                    aliasOverDb2(org.apache.doris.nereids.types.TinyIntType.INSTANCE));
            Assertions.assertEquals(tinyint, sameAgain,
                    "the same definitions keep one identity");

            String asInt = fingerprint(ctx, "SELECT db1.f(k) FROM t",
                    aliasOverDb2(IntegerType.INSTANCE));
            Assertions.assertNotEquals(tinyint, asInt,
                    "db2.f is expanded through its own scope, so its declared return type is"
                            + " part of the alias's identity: " + asInt);
        } finally {
            ConnectContext.remove();
        }
    }

    /**
     * A body that really references ITSELF still stops at the cycle: the unqualified call
     * resolves in the current database, which is the scope the alias was found in. The
     * fingerprint stays stable (and the walk terminates).
     */
    @Test
    public void testSelfReferencingAliasBodyStaysACycle() {
        ConnectContext ctx = context();
        try {
            // the body's UNQUALIFIED call resolves in the CURRENT database, i.e. exactly the
            // scope the alias itself was found in
            ctx.setDatabase("db1");
            AliasUdf alias = new AliasUdf("f", List.of(IntegerType.INSTANCE),
                    new NereidsParser().parseExpression("f(x)"), List.of("x"), Map.of());
            FunctionRegistry registry = scopedRegistry("db1",
                    new AliasUdfBuilder(alias), null, null);
            Assertions.assertEquals(fingerprint(ctx, "SELECT f(k) FROM t", registry),
                    fingerprint(ctx, "SELECT f(k) FROM t", registry),
                    "a self-referencing body is recorded as a cycle and terminates");
        } finally {
            ConnectContext.remove();
        }
    }

    // ==================== #3: qualified identities in the GROUP BY mirror ====================

    /**
     * {@code SELECT db1.f(k, 1) AS x, count(*) AS n FROM t GROUP BY db1.f(k, 1), db2.f(k, 1)}:
     * UnboundFunction#toString drops the database, so both group keys rendered the same text and
     * the reverse lookup replaced the {@code db1.f} output with the LAST recorded key
     * ({@code db2.f}). Both keys exist and the copy parameterizes identically, so CREATE stored
     * the baseline - but the caller's tree carries db1.f, which the AST check rejects against the
     * stored db2.f, so the baseline could not match even its own identical query.
     */
    @Test
    public void testGroupByMirrorKeyKeepsTheQualifiedFunction() {
        LogicalPlan plan = parse("SELECT db1.f(k, 1) AS x, count(*) AS n FROM t"
                + " GROUP BY db1.f(k, 1), db2.f(k, 1)");
        LogicalPlan transformed = SPMPlanTreeSupport.transform(plan, new SPMPlaceholderBuilder());

        LogicalAggregate<?> aggregate = findAggregate(transformed);
        // the unbound plan has no renderable text (UnboundException), so the messages stay
        // structural
        Assertions.assertNotNull(aggregate, "the plan keeps its aggregate");
        NamedExpression item = null;
        for (NamedExpression output : aggregate.getOutputExpressions()) {
            // NamedExpression#getName is UNBOUND on the parse tree (UnboundException), so the
            // alias is read from UnboundAlias#getAlias
            if (output instanceof UnboundAlias
                    && ((UnboundAlias) output).getAlias().filter("x"::equals).isPresent()) {
                item = output;
            }
        }
        Assertions.assertNotNull(item, "the SELECT item survives");
        Expression value = item.child(0);
        Assertions.assertTrue(value instanceof UnboundFunction,
                "the mirrored output is the parameterized GROUP BY key: " + value.getClass());
        Assertions.assertEquals("db1",
                ((UnboundFunction) value).getDbName(),
                "the db1.f group key must mirror the db1.f select item, not the identically"
                        + " rendered db2.f key: " + value);
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
                "the all-NULL array makes element_at a NullType producer, so its markers have"
                        + " to be BOOLEAN already: " + builder.getPlaceholderExprs());
    }

    private static LogicalAggregate<?> findAggregate(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            return (LogicalAggregate<?>) plan;
        }
        for (Plan child : plan.children()) {
            LogicalAggregate<?> found = findAggregate(child);
            if (found != null) {
                return found;
            }
        }
        return null;
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

    /** A registry in which db1.f is an ALIAS whose body calls the ordinary db2.f. */
    private static FunctionRegistry aliasOverDb2(org.apache.doris.nereids.types.DataType returnType) {
        AliasUdf alias = new AliasUdf("f", List.of(IntegerType.INSTANCE),
                new NereidsParser().parseExpression("db2.f(x)"), List.of("x"), Map.of());
        return scopedRegistry("db1", new AliasUdfBuilder(alias), "db2",
                javaUdf("f", NullableMode.ALWAYS_NOT_NULLABLE, returnType));
    }

    /**
     * A registry answering the NAME-level UDF lookup per database: {@code aliasDb} resolves the
     * alias, {@code udfDb} the ordinary function, every other scope is empty.
     */
    private static FunctionRegistry scopedRegistry(String aliasDb, FunctionBuilder aliasBuilder,
            String udfDb, FunctionBuilder udfBuilder) {
        FunctionRegistry registry = Mockito.mock(FunctionRegistry.class);
        Mockito.when(registry.getName2BuiltinBuilders()).thenReturn(new HashMap<>());
        Mockito.when(registry.findUdfBuilder(Mockito.any(), Mockito.any()))
                .thenReturn(List.of());
        if (aliasDb != null) {
            Mockito.when(registry.findUdfBuilder(Mockito.eq(aliasDb), Mockito.any()))
                    .thenReturn(List.of(aliasBuilder));
        }
        if (udfDb != null) {
            Mockito.when(registry.findUdfBuilder(Mockito.eq(udfDb), Mockito.any()))
                    .thenReturn(List.of(udfBuilder));
        }
        return registry;
    }

    private static FunctionBuilder javaUdf(String name, NullableMode mode,
            org.apache.doris.nereids.types.DataType returnType) {
        FunctionSignature signature =
                FunctionSignature.of(returnType, List.of(IntegerType.INSTANCE));
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
