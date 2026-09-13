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
