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
import org.apache.doris.nereids.datasets.tpch.TPCHTestBase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

public class OtherJoinTest extends TPCHTestBase {
    @Test
    public void randomTest() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder();
        Plan plan = hyperGraphBuilder
                .randomBuildPlanWith(10, 20);
        Set<List<Integer>> res1 = hyperGraphBuilder.evaluate(plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        hyperGraphBuilder.initStats(cascadesContext);
        Plan optimizedPlan = PlanChecker.from(cascadesContext)
                        .dpHypOptimize()
                        .getBestPlanTree();

        Set<List<Integer>> res2 = hyperGraphBuilder.evaluate(optimizedPlan);
        if (!res1.equals(res2)) {
            System.out.println(res1);
            System.out.println(res2);
            System.out.println(plan.treeString());
            System.out.println(optimizedPlan.treeString());
        }
        Assertions.assertTrue(res1.equals(res2));

    }
}
