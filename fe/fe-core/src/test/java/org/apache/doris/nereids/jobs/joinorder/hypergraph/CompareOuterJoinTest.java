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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CompareOuterJoinTest extends SqlTestBase {
    @Test
    void testStarGraphWithInnerJoin() {
        //      t2
        //      |
        //t3-- t1 -- t4
        //      |
        //     t5
        CascadesContext c1 = createCascadesContext(
                "select * from T1, T2, T3, T4 where "
                        + "T1.id = T2.id "
                        + "and T1.id = T3.id "
                        + "and T1.id = T4.id ",
                connectContext
        );
        Plan p1 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .getPlan().child(0);
        Plan p2 = PlanChecker.from(c1)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        Assertions.assertFalse(
                HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2)).isInvalid());
    }

    @Test
    void testRandomQuery() {
        Plan p1 = new HyperGraphBuilder().randomBuildPlanWith(3, 3);
        p1 = PlanChecker.from(connectContext, p1)
                .analyze()
                .rewrite()
                .getPlan();
        Plan p2 = PlanChecker.from(connectContext, p1)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        Assertions.assertFalse(
                HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2)).isInvalid());
    }

    @Test
    void testInnerJoinWithFilter() {
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
                "select * from T1 inner join T2 on T1.id = T2.id",
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
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(id = 0)", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testInnerJoinWithFilter2() {
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
                "select * from T1 inner join T2 on T1.id = T2.id where T1.id = 0",
                connectContext
        );
        Plan p2 = PlanChecker.from(c2)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0).child(0);
        HyperGraph h1 = HyperGraph.toStructInfo(p1).get(0);
        HyperGraph h2 = HyperGraph.toStructInfo(p2).get(0);
        List<Expression> exprList = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2)).getQueryExpressions();
        Assertions.assertEquals(0, exprList.size());
    }

    @Test
    void testLeftOuterJoinWithLeftFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES");
        CascadesContext c1 = createCascadesContext(
                "select * from ( select * from T1 where T1.id = 0) T1 left outer join T2 on T1.id = T2.id",
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
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(id = 0)", res.getQueryExpressions().get(0).toSql());
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
