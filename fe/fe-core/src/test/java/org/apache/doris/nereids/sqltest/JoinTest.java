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

package org.apache.doris.nereids.sqltest;

import org.apache.doris.nereids.properties.DistributionSpecHash;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.rewrite.ReorderJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class JoinTest extends SqlTestBase {
    @Test
    void testJoinUsing() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "SELECT * FROM T1 JOIN T2 using (id)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .applyBottomUp(new ReorderJoin())
                .matches(
                        innerLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testColocatedJoin() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T2 join T2 b on T2.id = b.id and T2.id = b.id;";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        System.out.println(plan.treeString());
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof PhysicalDistribute
                && ((PhysicalDistribute) p).getDistributionSpec() instanceof DistributionSpecHash));
        sql = "select * from T1 join T0 on T1.score = T0.score and T1.id = T0.id;";
        plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        Assertions.assertFalse(plan.anyMatch(p -> p instanceof PhysicalDistribute
                && ((PhysicalDistribute) p).getDistributionSpec() instanceof DistributionSpecHash));
    }

    @Test
    void testDedupConjuncts() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from T1 join T2 on T1.id = T2.id and T1.id = T2.id;";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matches(
                        innerLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );

        String sql1 = "select * from T1 left join T2 on T1.id = T2.id and T1.id = T2.id;";
        PlanChecker.from(connectContext)
                .analyze(sql1)
                .rewrite()
                .matches(
                        leftOuterLogicalJoin().when(j -> j.getHashJoinConjuncts().size() == 1)
                );
    }

    @Test
    void testBucketJoinWithAgg() {
        connectContext.getSessionVariable().setDisableNereidsRules("PRUNE_EMPTY_PARTITION");
        String sql = "select * from "
                + "(select distinct id as cnt from T2) T1 inner join"
                + "(select distinct id as cnt from T2) T2 "
                + "on T1.cnt = T2.cnt";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .optimize()
                .getBestPlanTree(PhysicalProperties.ANY);
        Assertions.assertEquals(
                ShuffleType.NATURAL,
                ((DistributionSpecHash) ((PhysicalPlan) (plan.child(0).child(0)))
                        .getPhysicalProperties().getDistributionSpec()).getShuffleType()
        );
    }
}
