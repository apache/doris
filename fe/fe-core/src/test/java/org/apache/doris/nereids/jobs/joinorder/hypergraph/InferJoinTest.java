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

package org.apache.doris.nereids.jobs.joinorder.hypergraph;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.RuleSet;
import org.apache.doris.nereids.rules.exploration.mv.AbstractMaterializedViewRule;
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.rules.exploration.mv.LogicalCompatibilityContext;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.exploration.mv.mapping.RelationMapping;
import org.apache.doris.nereids.rules.exploration.mv.mapping.SlotMapping;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

class InferJoinTest extends SqlTestBase {
    @Test
    void testInnerInferLeft() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T2 on T1.id = T2.id where T1.id = 0",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left join T2 on T1.id = T2.id where T1.id = 0",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(1, res.getViewNoNullableSlot().size());
        Assertions.assertEquals("[id, score]",
                res.getViewNoNullableSlot().iterator().next().stream().map(ExpressionTrait::toSql).collect(Collectors.toList()).toString());
    }

    @Test
    void testInnerInferLeftWithFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join T2 on T1.id = T2.id where T1.id = 0",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left join (select * from T2 where id = 0) T2 on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(1, res.getViewNoNullableSlot().size());
        Assertions.assertEquals("[id, score]",
                res.getViewNoNullableSlot().iterator().next().stream().map(ExpressionTrait::toSql).collect(Collectors.toList()).toString());
        Assertions.assertEquals("(id = 0)", res.getViewExpressions().get(0).toSql());
        Assertions.assertEquals("(id = 0)", res.getQueryExpressions().get(0).toSql());
    }

    // should consider rebuilding the hypergraph
    @Disabled
    @Test
    void testInnerInferLeftWithJoinCond() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 inner join "
                        + "(select T2.id from T2 inner join T3 on T2.id = T3.id) T2 "
                        + "on T1.id = T2.id",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left join "
                        + "(select T2.id from T2 inner join T3 on T2.id = T3.id and T2.score = T3.score) T2 "
                        + "on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertFalse(res.isInvalid());
        Assertions.assertEquals(1, res.getViewNoNullableSlot().size());
        Assertions.assertEquals("[id, score]",
                res.getViewNoNullableSlot().iterator().next().stream().map(ExpressionTrait::toSql).collect(Collectors.toList()).toString());
        Assertions.assertEquals("score = score", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testLeftOuterJoinWithRightFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 left outer join ( select * from T2 where T2.id = 0) T2 on T1.id = T2.id",
                connectContext
        );
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        CascadesContext c2 = createCascadesContext(
                "select * from T1 left outer join T2 on T1.id = T2.id",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2));
        Assertions.assertTrue(res.isInvalid());
    }

    LogicalCompatibilityContext constructContext(Plan p1, Plan p2) {
        StructInfo st1 = AbstractMaterializedViewRule.extractStructInfo(p1,
                null).get(0);
        StructInfo st2 = AbstractMaterializedViewRule.extractStructInfo(p2,
                null).get(0);
        RelationMapping rm = RelationMapping.generate(st1.getRelations(), st2.getRelations()).get(0);
        SlotMapping sm = SlotMapping.generate(rm);
        return LogicalCompatibilityContext.from(rm, sm, st1, st2);
    }
}
