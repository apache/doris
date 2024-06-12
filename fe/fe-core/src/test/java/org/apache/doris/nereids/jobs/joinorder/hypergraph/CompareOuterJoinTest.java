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
import org.apache.doris.nereids.rules.exploration.mv.ComparisonResult;
import org.apache.doris.nereids.rules.exploration.mv.HyperGraphComparator;
import org.apache.doris.nereids.sqltest.SqlTestBase;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

class CompareOuterJoinTest extends SqlTestBase {
    @Test
    void testStarGraphWithInnerJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
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
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        Assertions.assertFalse(
                HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1)).isInvalid());
    }

    @Test
    void testRandomQuery() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        connectContext.getSessionVariable().setEnableMaterializedViewRewrite(false);
        Plan p1 = new HyperGraphBuilder(Sets.newHashSet(JoinType.INNER_JOIN))
                .randomBuildPlanWith(3, 3);
        PlanChecker planChecker = PlanChecker.from(connectContext, p1)
                .analyze()
                .rewrite();
        p1 = planChecker
                .getPlan();
        Plan p2 = PlanChecker.from(connectContext, p1)
                .analyze()
                .rewrite()
                .applyExploration(RuleSet.BUSHY_TREE_JOIN_REORDER)
                .getAllPlan().get(0);
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        Assertions.assertFalse(
                HyperGraphComparator.isLogicCompatible(h1, h2,
                        constructContext(p1, p2, planChecker.getCascadesContext())).isInvalid());
    }

    @Test
    void testInnerJoinWithFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
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
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(id = 0)", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testInnerJoinWithFilter2() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
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
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        List<Expression> exprList = HyperGraphComparator.isLogicCompatible(h1, h2,
                constructContext(p1, p2, c1)).getQueryExpressions();
        Assertions.assertEquals(0, exprList.size());
    }

    @Test
    void testLeftOuterJoinWithLeftFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from ( select * from T1 where T1.id = 0) T1 left outer join T2 on T1.id = T2.id",
                connectContext
        );
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
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertEquals(1, res.getQueryExpressions().size());
        Assertions.assertEquals("(id = 0)", res.getQueryExpressions().get(0).toSql());
    }

    @Test
    void testLeftOuterJoinWithRightFilter() {
        connectContext.getSessionVariable().setDisableNereidsRules("INFER_PREDICATES,PRUNE_EMPTY_PARTITION");
        CascadesContext c1 = createCascadesContext(
                "select * from T1 left outer join ( select * from T2 where T2.id = 0) T2 on T1.id = T2.id",
                connectContext
        );
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
        HyperGraph h1 = HyperGraph.builderForMv(p1).build();
        HyperGraph h2 = HyperGraph.builderForMv(p2).build();
        ComparisonResult res = HyperGraphComparator.isLogicCompatible(h1, h2, constructContext(p1, p2, c1));
        Assertions.assertTrue(res.isInvalid());
    }
}
