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

package org.apache.doris.nereids.memo;

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.HyperGraphBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.utframe.TestWithFeService;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

class RankTest extends TestWithFeService {
    @Test
    void test() {
        HyperGraphBuilder hyperGraphBuilder = new HyperGraphBuilder(Sets.newHashSet(JoinType.INNER_JOIN));
        hyperGraphBuilder.init(0, 1, 2);
        Plan plan = hyperGraphBuilder
                .addEdge(JoinType.INNER_JOIN, 0, 1)
                .addEdge(JoinType.INNER_JOIN, 1, 2)
                .buildPlan();
        plan = new LogicalProject(plan.getOutput(), plan);
        CascadesContext cascadesContext = MemoTestUtils.createCascadesContext(connectContext, plan);
        hyperGraphBuilder.initStats(cascadesContext);
        PhysicalPlan bestPlan = PlanChecker.from(cascadesContext)
                .optimize()
                .getBestPlanTree();
        Memo memo = cascadesContext.getMemo();
        Set<String> shape = new HashSet<>();
        for (int i = 0; i < memo.getRankSize(); i++) {
            shape.add(memo.unrank(memo.rank(i + 1).first).shape(""));
        }
        System.out.println(shape);
        Assertions.assertEquals(4, shape.size());
        Assertions.assertEquals(bestPlan.shape(""), memo.unrank(memo.rank(1).first).shape(""));
    }
}
