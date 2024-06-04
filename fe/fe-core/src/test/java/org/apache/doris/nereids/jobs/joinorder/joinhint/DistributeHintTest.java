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

package org.apache.doris.nereids.jobs.joinorder.joinhint;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintLeading;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import com.google.common.collect.Maps;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DistributeHintTest extends TPCHTestBase {

    private int used = 0;
    private int unused = 0;

    private int syntaxError = 0;

    private int successCases = 0;

    private int unsuccessCases = 0;

    private List<String> failCases = new ArrayList<>();

    @Test
    public void testLeading() {
        for (int t = 3; t < 10; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                for (int i = 0; i < 10; i++) {
                    System.out.println("TableNumber: " + String.valueOf(t) + " EdgeNumber: " + e + " Iteration: " + i);
                    randomTest(t, e, false, true);
                }
            }
        }
        int totalCases = successCases + unsuccessCases;
        System.out.println("TotalCases: " + totalCases + "\tSuccessCases: " + successCases + unsuccessCases + "\tUnSuccessCases: " + 0);
        for (String treePlan : failCases) {
            System.out.println(treePlan);
        }
    }

    @Test
    public void testHintJoin() {
        for (int t = 3; t < 10; t++) {
            for (int e = t - 1; e <= (t * (t - 1)) / 2; e++) {
                for (int i = 0; i < 10; i++) {
                    System.out.println("TableNumber: " + String.valueOf(t) + " EdgeNumber: " + e + " Iteration: " + i);
                    randomTest(t, e, true, false);
                }
            }
        }
        int totalCases = successCases + unsuccessCases;
        System.out.println("TotalCases: " + totalCases + "\tSuccessCases: " + successCases + "\tUnSuccessCases: " + unsuccessCases);
        for (String treePlan : failCases) {
            System.out.println(treePlan);
        }
    }

    private Plan generateLeadingHintPlan(int tableNum, Plan childPlan) {
        Map<String, SelectHint> hints = Maps.newLinkedHashMap();
        List<String> leadingParameters = new ArrayList<String>();
        for (int i = 0; i < tableNum; i++) {
            leadingParameters.add(String.valueOf(i));
        }
        Collections.shuffle(leadingParameters);
        System.out.println("LeadingHint: " + leadingParameters.toString());
        hints.put("leading", new SelectHintLeading("leading", leadingParameters));
        return new LogicalSelectHint<>(hints, childPlan);
    }

    private void randomTest(int tableNum, int edgeNum, boolean withJoinHint, boolean withLeading) {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        Plan plan = withJoinHint ? hyperGraphBuilder.buildJoinPlanWithJoinHint(tableNum, edgeNum) :
                hyperGraphBuilder.randomBuildPlanWith(tableNum, edgeNum);
        plan = new LogicalProject(plan.getOutput(), plan);
        Set<List<String>> res1 = hyperGraphBuilder.evaluate(plan);
        if (!withLeading) {
            CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
            hyperGraphBuilder.initStats("tpch", cascadesContext);
            Plan optimizedPlan = PlanChecker.from(cascadesContext)
                    .analyze()
                    .optimize()
                    .getBestPlanTree();

            Set<List<String>> res2 = hyperGraphBuilder.evaluate(optimizedPlan);
            if (!res1.equals(res2)) {
                System.out.println(plan.treeString());
                System.out.println(optimizedPlan.treeString());
                cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
                PlanChecker.from(cascadesContext).dpHypOptimize().getBestPlanTree();
                System.out.println(res1);
                System.out.println(res2);
                unsuccessCases++;
                failCases.add(plan.treeString());
                failCases.add(optimizedPlan.treeString());
            }
            successCases++;
        } else {
            // generate select hint
            for (int i = 0; i < (tableNum * tableNum - 1); i++) {
                Plan leadingPlan = generateLeadingHintPlan(tableNum, plan);
                CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, leadingPlan);
                hyperGraphBuilder.initStats("tpch", cascadesContext);
                Plan optimizedPlan = PlanChecker.from(cascadesContext)
                        .analyze()
                        .optimize()
                        .getBestPlanTree();

                Set<List<String>> res2 = hyperGraphBuilder.evaluate(optimizedPlan);
                if (!res1.equals(res2)) {
                    System.out.println(leadingPlan.treeString());
                    System.out.println(optimizedPlan.treeString());
                    cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
                    PlanChecker.from(cascadesContext).dpHypOptimize().getBestPlanTree();
                    System.out.println(res1);
                    System.out.println(res2);
                    unsuccessCases++;
                    failCases.add(leadingPlan.treeString());
                    failCases.add(optimizedPlan.treeString());
                }
                successCases++;
            }
        }
    }
}
