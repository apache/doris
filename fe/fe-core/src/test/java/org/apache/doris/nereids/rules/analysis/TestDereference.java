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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.common.FeConstants;
import org.apache.doris.datasource.test.TestExternalCatalog.TestCatalogProvider;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference.ArrayItemSlot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ElementAt;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TestDereference extends TestWithFeService {

    private static final Map<String, Map<String, List<Column>>> CATALOG_META = ImmutableMap.of(
            "t", ImmutableMap.of(
                    "t", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("t", new VariantType())
                    ),
                    "outer_table", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("value", PrimitiveType.INT),
                            new Column("@event_name", PrimitiveType.VARCHAR),
                            new Column("payload", new StructType(new StructField("k", Type.INT))),
                            new Column("items", new ArrayType(
                                    new StructType(new StructField("value", Type.INT))))
                    ),
                    "inner_table", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("t1", PrimitiveType.INT),
                            new Column("t", new StructType(new StructField("value", Type.INT)))
                    ),
                    "inner_variant_table", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("outer_alias", new VariantType())
                    ),
                    "shadow_table", ImmutableList.of(
                            new Column("id", PrimitiveType.INT),
                            new Column("v", PrimitiveType.INT),
                            new Column("s", new StructType(new StructField("v", Type.INT))),
                            new Column("arr", new ArrayType(Type.INT))
                    ),
                    "plain_table", ImmutableList.of(
                            new Column("id", PrimitiveType.INT)
                    )
            )
    );

    @Override
    protected void runBeforeAll() throws Exception {
        FeConstants.runningUnitTest = true;
        createCatalog("create catalog t properties("
                + " \"type\"=\"test\","
                + " \"catalog_provider.class\"=\"org.apache.doris.nereids.rules.analysis.TestDereference$CustomCatalogProvider\""
                + ")");
        connectContext.changeDefaultCatalog("t");
        useDatabase("t");
    }

    @Test
    public void testBindPriority() {
        // column
        testBind("select t from t");
        // table.column
        testBind("select t.t from t");
        // db.table.column
        testBind("select t.t.t from t");
        // catalog.db.table.column
        testBind("select t.t.t.t from t");
        // catalog.db.table.column.subColumn
        testBind("select t.t.t.t.t from t");
        // catalog.db.table.column.subColumn.subColumn2
        testBind("select t.t.t.t.t.t from t");
    }

    @Test
    public void testCorrelatedSubqueryPrefersOuterTableAlias() {
        testBind("select t1.`@event_name` from outer_table t1 where exists ("
                + "select 1 from inner_table inner_alias where t1.`@event_name` = 'click')");
    }

    @Test
    public void testOuterTableAliasTakesPriorityOverInnerVariantColumn() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select outer_alias.id from outer_table outer_alias where exists ("
                        + "select 1 from inner_variant_table inner_alias where outer_alias.value = 1)")
                .getPlan();

        LogicalApply<?, ?> apply = getOnlyApply(plan);
        Assertions.assertEquals(1, apply.getCorrelationSlot().size());
        Assertions.assertEquals("value", apply.getCorrelationSlot().get(0).getName());
        List<String> qualifier = apply.getCorrelationSlot().get(0).getQualifier();
        Assertions.assertEquals("outer_alias", qualifier.get(qualifier.size() - 1));
    }

    @Test
    public void testInnerAliasShadowsOuterAliasInFilter() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select t.id from outer_table t where exists ("
                        + "select 1 from inner_table t where t.id = 1)")
                .getPlan();

        Assertions.assertTrue(getOnlyApply(plan).getCorrelationSlot().isEmpty());
    }

    @Test
    public void testInnerAliasKeepsNestedFieldFallback() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select t.id from outer_table t where exists ("
                        + "select 1 from inner_table t where t.value = 1)")
                .getPlan();

        Assertions.assertTrue(getOnlyApply(plan).getCorrelationSlot().isEmpty());
    }

    @Test
    public void testInnerAliasKeepsScalarFieldError() {
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("select t1.id from outer_table t1 where exists ("
                                + "select 1 from inner_table t1 where t1.`@event_name` = 'click')"));
        Assertions.assertTrue(exception.getMessage().contains("No such field '@event_name' in 't1'"));
    }

    @Test
    public void testLambdaArgumentTakesPriorityOverOuterTableAlias() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select array_map(x -> x.value, x.items) from outer_table x")
                .getPlan();

        List<Lambda> lambdas = new ArrayList<>();
        for (Plan node : plan.<Plan>collectToList(ignored -> true)) {
            node.getExpressions().forEach(expression ->
                    lambdas.addAll(expression.collectToList(Lambda.class::isInstance)));
        }
        Assertions.assertEquals(1, lambdas.size());
        Assertions.assertTrue(lambdas.get(0).getLambdaFunction().containsType(ElementAt.class));
        Assertions.assertTrue(lambdas.get(0).getLambdaFunction().anyMatch(ArrayItemSlot.class::isInstance));
    }

    @Test
    public void testOuterNestedFieldRegistersCorrelationSlot() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select outer_alias.id from outer_table outer_alias where exists ("
                        + "select 1 from inner_variant_table inner_alias where outer_alias.payload.k = 1)")
                .getPlan();

        LogicalApply<?, ?> apply = getOnlyApply(plan);
        Assertions.assertEquals(1, apply.getCorrelationSlot().size());
        Assertions.assertEquals("payload", apply.getCorrelationSlot().get(0).getName());
        List<String> qualifier = apply.getCorrelationSlot().get(0).getQualifier();
        Assertions.assertEquals("outer_alias", qualifier.get(qualifier.size() - 1));
    }

    @Test
    public void testInnerHavingAliasShadowsOuterAlias() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select t.id from outer_table t where exists ("
                        + "select 1 from inner_table t having max(t.id) > 0)")
                .getPlan();

        Assertions.assertTrue(getOnlyApply(plan).getCorrelationSlot().isEmpty());
    }

    @Test
    public void testInnerQualifyAliasShadowsOuterAlias() {
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select t.id from outer_table t where exists ("
                        + "select 1 from inner_table t group by t.id "
                        + "qualify row_number() over (order by id) = t.id)")
                .getPlan();

        Assertions.assertTrue(getOnlyApply(plan).getCorrelationSlot().isEmpty());
    }

    @Test
    public void testOutputAliasDoesNotShadowRelationQualifier() {
        // the select output is a nearer scope than the relation for ORDER BY, HAVING and QUALIFY,
        // q.v should still be the column v of relation q rather than a field of the scalar output alias q
        List<String> sqls = ImmutableList.of(
                "select q.v as q from (select 7 as v) q order by q.v",
                "select q.v as q from shadow_table q order by q.v",
                "select distinct q.v as q from shadow_table q order by q.v",
                "select q.v as p, p.id as q from shadow_table q join plain_table p on q.id = p.id order by q.v, p.id",
                "select * from plain_table o where o.id in (select q.v as q from shadow_table q order by q.v limit 1)",
                // db.table.column, the alias has the same name as the database
                "select q.v as t from shadow_table q order by t.q.v",
                // aggregate
                "select q.id as q from shadow_table q group by q.id order by max(q.v)",
                "select max(q.v) as q from shadow_table q group by q.id order by q.id",
                "select q.id + 1 as q from shadow_table q group by q.id + 1 order by q.id + 1",
                "select q.v as q from shadow_table q having q.v > 0",
                "select q.id + 1 as q from shadow_table q group by q.id + 1 having q.id + 1 > 0",
                "select max(q.id) as q from shadow_table q group by q.v having q.v > 0",
                "select q.v as q from shadow_table q qualify row_number() over (order by q.id) = 1 and q.v > 0",
                "select q.id + 1 as q from shadow_table q group by q.id + 1 "
                        + "qualify row_number() over (order by q.id + 1) = 1"
        );
        for (String sql : sqls) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(sql), sql);
        }
    }

    @Test
    public void testRelationQualifierTakesPriorityOverComplexOutputAlias() {
        // the output alias q is a struct with field v, q.v should not silently become element_at(q, 'v')
        assertBoundToColumnV("select q.s as q from shadow_table q order by q.v");
        assertBoundToColumnV("select q.s as q from shadow_table q having q.v > 0");
        assertBoundToColumnV("select q.s as q from shadow_table q qualify row_number() over (order by q.v) = 1");
    }

    @Test
    public void testOutputAliasWithoutRelationQualifierKeepsScalarFieldError() {
        // q is only an output alias here, the relation is p, so q.v is a field of the scalar alias q
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("select p.v as q from shadow_table p order by q.v"));
        Assertions.assertTrue(exception.getMessage().contains("No such field 'v' in 'q'"),
                exception.getMessage());
    }

    @Test
    public void testRelationQualifierOccupiesNestedOutputAliasPath() {
        // q.v is the scalar column v of relation q, so q.v.b is not the path v.b of the struct alias q
        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("select named_struct('v', named_struct('b', 1)) as q "
                                + "from shadow_table q order by q.v.b"));
        Assertions.assertTrue(exception.getMessage().contains("No such field 'b' in 'v'"),
                exception.getMessage());
    }

    @Test
    public void testOutputAliasKeepsNestedFieldFallback() {
        // no relation-qualified column matches, so the first part falls back to the output alias
        assertBoundToNestedField("select q.s as a from shadow_table q order by a.v");
        assertBoundToNestedField("select p.s as q from shadow_table p join plain_table q on p.id = q.id order by q.v");
        assertBoundToNestedField("select p.s as q from shadow_table p join plain_table q on p.id = q.id "
                + "having q.v > 0");
    }

    @Test
    public void testLambdaBodyBindsByEnclosingClauseScopes() {
        // a name that is not a lambda argument is resolved the same way as outside the lambda,
        // ORDER BY, HAVING and QUALIFY can see the child output behind the select output
        List<String> sqls = ImmutableList.of(
                "select id from shadow_table order by array_sum(array_map(x -> x + v, arr))",
                "select id from shadow_table q order by array_sum(array_map(x -> x + q.v, q.arr))",
                "select q.v as q from shadow_table q order by array_sum(array_map(x -> x + q.v, q.arr))",
                "select q.v as q from shadow_table q having array_sum(array_map(x -> x + q.v, q.arr)) > 0",
                "select q.id as q from shadow_table q group by q.id "
                        + "having sum(array_sum(array_map(x -> x + q.v, q.arr))) > 0",
                "select q.v as q from shadow_table q "
                        + "qualify row_number() over (order by array_sum(array_map(x -> x + q.v, q.arr))) = 1"
        );
        for (String sql : sqls) {
            Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(sql), sql);
        }

        // a nested lambda resolves through the lambda around it
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                "select id from shadow_table q order by "
                        + "array_sum(array_map(x -> array_sum(array_map(y -> y + x + q.v, q.arr)), q.arr))"));
        // the enclosing clause is a join condition, both sides are its own scope rather than an outer scope
        Assertions.assertDoesNotThrow(() -> PlanChecker.from(connectContext).analyze(
                "select q.id from shadow_table q join plain_table p "
                        + "on array_sum(array_map(x -> x + p.id, q.arr)) > 0"));

        AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                () -> PlanChecker.from(connectContext)
                        .analyze("select id from shadow_table order by array_map(x -> x + unknown_column, arr)"));
        Assertions.assertTrue(exception.getMessage().contains("Unknown column 'unknown_column'"),
                exception.getMessage());
    }

    @Test
    public void testLambdaBodyFollowsAmbiguityOfEnclosingClause() {
        // id is both the output alias of q.id and the output slot p.id, HAVING does not pick the exact match
        String having = "select q.id as id, p.id from shadow_table q join plain_table p on q.id = p.id having ";
        for (String predicate : ImmutableList.of("id > 0", "array_sum(array_map(x -> x + id, q.arr)) > 0")) {
            AnalysisException exception = Assertions.assertThrows(AnalysisException.class,
                    () -> PlanChecker.from(connectContext).analyze(having + predicate), predicate);
            Assertions.assertTrue(exception.getMessage().contains("id is ambiguous"), exception.getMessage());
        }
    }

    @Test
    public void testLambdaBodyRegistersCorrelationSlot() {
        // the enclosing analyzer of the subquery filter sees the outer scope, so does the lambda body
        Plan plan = PlanChecker.from(connectContext)
                .analyze("select o.id from plain_table o where exists ("
                        + "select 1 from shadow_table q where array_sum(array_map(x -> x + o.id, q.arr)) > 0)")
                .getPlan();

        LogicalApply<?, ?> apply = getOnlyApply(plan);
        Assertions.assertEquals(1, apply.getCorrelationSlot().size());
        Assertions.assertEquals("id", apply.getCorrelationSlot().get(0).getName());
        List<String> qualifier = apply.getCorrelationSlot().get(0).getQualifier();
        Assertions.assertEquals("o", qualifier.get(qualifier.size() - 1));
    }

    private void assertBoundToColumnV(String sql) {
        Plan plan = PlanChecker.from(connectContext).analyze(sql).getPlan();
        Assertions.assertFalse(containsElementAt(plan), sql);
    }

    private void assertBoundToNestedField(String sql) {
        Plan plan = PlanChecker.from(connectContext).analyze(sql).getPlan();
        Assertions.assertTrue(containsElementAt(plan), sql);
    }

    private boolean containsElementAt(Plan plan) {
        return plan.anyMatch(node -> ((Plan) node).getExpressions().stream()
                .anyMatch(expression -> expression.containsType(ElementAt.class)));
    }

    private LogicalApply<?, ?> getOnlyApply(Plan plan) {
        List<LogicalApply<?, ?>> applies = plan.collectToList(LogicalApply.class::isInstance);
        Assertions.assertEquals(1, applies.size());
        return applies.get(0);
    }

    private void testBind(String sql) {
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite();
    }

    public static class CustomCatalogProvider implements TestCatalogProvider {

        @Override
        public Map<String, Map<String, List<Column>>> getMetadata() {
            return CATALOG_META;
        }
    }
}
