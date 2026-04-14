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
                "select T1.id from T1 inner join T2 on T1.id = T2.id "
                        + "inner join T3 on T2.id = T3.id where T3.score = 1",
                connectContext
        );
        Plan queryPlan = PlanChecker.from(queryContext)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);

        CascadesContext viewContext = createCascadesContext(
                "select T1.id from T1 left outer join T2 on T1.id = T2.id "
                        + "left outer join T3 on T2.id = T3.id",
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
