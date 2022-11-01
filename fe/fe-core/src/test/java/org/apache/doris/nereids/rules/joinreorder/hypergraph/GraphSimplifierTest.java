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

package org.apache.doris.nereids.rules.joinreorder.hypergraph;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.jobs.cascades.DeriveStatsJob;
import org.apache.doris.nereids.pattern.GroupExpressionMatching;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.joinreorder.HyperGraphJoinReorderGroupPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanConstructor;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class GraphSimplifierTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(2, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(3, "t4", 0);
    private final LogicalOlapScan scan5 = PlanConstructor.newLogicalOlapScan(4, "t5", 0);

    @Test
    void testGraphSimplifier() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(scan2, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan3, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan4, JoinType.INNER_JOIN, Pair.of(0, 0))
                .hashJoinUsing(scan5, JoinType.INNER_JOIN, Pair.of(0, 0))
                .build();

        Plan joinCluster = extractJoinCluster(plan);
        HyperGraph hyperGraph = HyperGraph.fromPlan(joinCluster);
        GraphSimplifier graphSimplifier = new GraphSimplifier(hyperGraph);
        graphSimplifier.initFirstStep();
        while (graphSimplifier.applySimplificationStep()) {
        }

        String target = "digraph G {  # 2 edges\n"
                + "  LOGICAL_OLAP_SCAN0 [label=\"LOGICAL_OLAP_SCAN0 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN1 [label=\"LOGICAL_OLAP_SCAN1 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN2 [label=\"LOGICAL_OLAP_SCAN2 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN3 [label=\"LOGICAL_OLAP_SCAN3 \n"
                + " rowCount=0.00\"];\n"
                + "  LOGICAL_OLAP_SCAN4 [label=\"LOGICAL_OLAP_SCAN4 \n"
                + " rowCount=0.00\"];\n"
                + "e0 [shape=circle, width=.001, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> e0 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN4 -> e0 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN1 -> e0 [arrowhead=none, label=\"\"]\n"
                + "e2 [shape=circle, width=.001, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN0 -> e2 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN1 -> e2 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN2 -> e2 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN4 -> e2 [arrowhead=none, label=\"\"]\n"
                + "LOGICAL_OLAP_SCAN3 -> e2 [arrowhead=none, label=\"\"]\n"
                + "}\n";
        String dottyGraph = hyperGraph.toDottyHyperGraph();
        assert dottyGraph.equals(target) : dottyGraph;
    }

    Plan extractJoinCluster(Plan plan) {
        Rule rule = new HyperGraphJoinReorderGroupPlan().build();
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(MemoTestUtils.createConnectContext(),
                plan);
        deriveStats(cascadesContext);
        GroupExpressionMatching groupExpressionMatching
                = new GroupExpressionMatching(rule.getPattern(),
                cascadesContext.getMemo().getRoot().getLogicalExpression());
        List<Plan> planList = new ArrayList<>();
        for (Plan matchingPlan : groupExpressionMatching) {
            planList.add(matchingPlan);
        }
        assert planList.size() == 1 : "Now we only support one join cluster";
        return planList.get(0);
    }

    private void deriveStats(CascadesContext cascadesContext) {
        cascadesContext.pushJob(
                new DeriveStatsJob(cascadesContext.getMemo().getRoot().getLogicalExpression(),
                        cascadesContext.getCurrentJobContext()));
        cascadesContext.getJobScheduler().executeJobPool(cascadesContext);
    }
}
