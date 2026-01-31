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

package org.apache.doris.nereids.jobs.joinorder.hypergraphv2;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class OtherJoinTest extends TPCHTestBase {
    @Test
    public void test() {
        for (int t = 40; t < 64; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                for (int i = 0; i < 1; i++) {
                    System.out.println(String.valueOf(t) + " " + e + ": " + i);
                    randomTest(t, e);
                }
            }
        }
        System.out.println("aaa");
    }

    private void randomTest(int tableNum, int edgeNum) {
        connectContext.setStatementContext(new StatementContext());
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        Plan plan = hyperGraphBuilder
                .randomBuildPlanWith(tableNum, edgeNum);
        plan = new LogicalProject(plan.getOutput(), plan);
        final String originalPlanStr = plan.treeString();
        Set<List<String>> res1 = hyperGraphBuilder.evaluate(plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext.getStatementContext(),
                plan);
        hyperGraphBuilder.initStats("tpch", cascadesContext);
        try {
            Plan optimizedPlan = PlanChecker.from(cascadesContext)
                    .dpHypOptimize()
                    .getBestPlanTree();

            Set<List<String>> res2 = hyperGraphBuilder.evaluate(optimizedPlan);

            if (!res1.equals(res2)) {
                res1 = hyperGraphBuilder.evaluate(plan);
                res2 = hyperGraphBuilder.evaluate(optimizedPlan);
                System.out.println("==== ORIGINAL PLAN ====");
                System.out.println(originalPlanStr);
                System.out.println("==== OPTIMIZED PLAN ====");
                System.out.println(optimizedPlan.treeString());
                cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
                PlanChecker.from(cascadesContext).dpHypOptimize().getBestPlanTree();
                System.out.println("==== EXPECTED RESULTS ====");
                System.out.println(res1);
                System.out.println("==== ACTUAL RESULTS ====");
                System.out.println(res2);
            }
            Assertions.assertTrue(res1.equals(res2));
        } catch (Exception ex) {
            System.out.println("Exception during optimization/evaluation:");
            ex.printStackTrace(System.out);
            System.out.println("==== ORIGINAL PLAN (on exception) ====");
            System.out.println(originalPlanStr);
        }

    }

    @Test
    void testOneSideJoin() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        Plan plan = hyperGraphBuilder.init(1, 2, 3)
                .addEdge(JoinType.LEFT_OUTER_JOIN, 0, 1)
                .addEdge(JoinType.LEFT_OUTER_JOIN, 0, 2)
                .buildPlan();
        Expression oneSideCond = new EqualTo(plan.child(0).getOutput().get(0), plan.child(0).getOutput().get(2));
        plan = ((LogicalJoin<?, ?>) plan).withJoinConjuncts(ImmutableList.of(), ImmutableList.of(oneSideCond),
                ((LogicalJoin<?, ?>) plan).getJoinReorderContext());
        Set<List<String>> res = hyperGraphBuilder.evaluate(plan);
        Assertions.assertEquals(4, res.size());
    }
}
