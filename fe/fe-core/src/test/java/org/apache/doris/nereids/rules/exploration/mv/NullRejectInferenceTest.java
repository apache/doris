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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.Predicates.SplitPredicate;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class NullRejectInferenceTest extends SqlTestBase {

    private static final TestMaterializedViewRule TEST_RULE = new TestMaterializedViewRule();

    @Test
    void testTwoHopNullRejectFromInnerJoinConditions() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_orderkey, supplier.s_name, nation.n_name from lineitem "
                        + "inner join supplier on lineitem.l_suppkey = supplier.s_suppkey "
                        + "inner join nation on supplier.s_nationkey = nation.n_nationkey "
                        + "where nation.n_name = 'CHINA'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_orderkey, supplier.s_name, nation.n_name from lineitem "
                        + "left outer join supplier on lineitem.l_suppkey = supplier.s_suppkey "
                        + "left outer join nation on supplier.s_nationkey = nation.n_nationkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "s_name")));
    }

    @Test
    void testNullRejectCompensationForInnerJoinFullJoinRewrite() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "inner join orders on lineitem.l_orderkey = orders.o_orderkey "
                        + "where orders.o_orderdate = '2023-10-17'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "full outer join orders on lineitem.l_orderkey = orders.o_orderkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "l_shipdate")));
    }

    @Test
    void testNullRejectCompensationForInnerJoinFullJoinRewriteOnRightSide() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext queryContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "inner join orders on lineitem.l_orderkey = orders.o_orderkey "
                        + "where lineitem.l_shipdate = '2023-10-17'",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select lineitem.l_shipdate, orders.o_orderdate from lineitem "
                        + "full outer join orders on lineitem.l_orderkey = orders.o_orderkey",
                connectContext
        );
        Plan viewPlan = PlanChecker.from(viewContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        StructInfo queryStructInfo = StructInfo.of(queryPlan, queryPlan, queryContext);
        StructInfo viewStructInfo = StructInfo.of(viewPlan, viewPlan, viewContext);
        RelationMapping relationMapping = RelationMapping.generate(
                queryStructInfo.getRelations(), viewStructInfo.getRelations(), 8).get(0);
        SlotMapping queryToView = SlotMapping.generate(relationMapping);
        SlotMapping viewToQuery = queryToView.inverse();
        LogicalCompatibilityContext compatibilityContext = LogicalCompatibilityContext.from(
                relationMapping, viewToQuery, queryStructInfo, viewStructInfo);
        ComparisonResult comparisonResult = StructInfo.isGraphLogicalEquals(
                queryStructInfo, viewStructInfo, compatibilityContext);

        Assertions.assertFalse(comparisonResult.isInvalid());
        Assertions.assertFalse(comparisonResult.getViewNoNullableSlot().isEmpty());

        SplitPredicate compensatePredicates = TEST_RULE.predicatesCompensateForTest(
                queryStructInfo, viewStructInfo, viewToQuery, comparisonResult, queryContext);
        Assertions.assertFalse(compensatePredicates.isInvalid());
        Assertions.assertTrue(compensatePredicates.toList().stream()
                .anyMatch(expression -> isNotNullOnSlot(expression, "o_orderdate")));
    }

    private static boolean isNotNullOnSlot(Expression expression, String slotName) {
        if (!(expression instanceof Not) || ((Not) expression).isGeneratedIsNotNull()
                || !(((Not) expression).child() instanceof IsNull)) {
            return false;
        }
        Expression slot = ((IsNull) ((Not) expression).child()).child();
        return slot instanceof SlotReference && slotName.equals(((SlotReference) slot).getName());
    }

    private static class TestMaterializedViewRule extends AbstractMaterializedViewRule {
        @Override
        public List<Rule> buildRules() {
            return ImmutableList.of();
        }

        private SplitPredicate predicatesCompensateForTest(StructInfo queryStructInfo,
                StructInfo viewStructInfo, SlotMapping viewToQuerySlotMapping,
                ComparisonResult comparisonResult, CascadesContext cascadesContext) {
            return predicatesCompensate(queryStructInfo, viewStructInfo, viewToQuerySlotMapping,
                    comparisonResult, cascadesContext);
        }
    }
}
