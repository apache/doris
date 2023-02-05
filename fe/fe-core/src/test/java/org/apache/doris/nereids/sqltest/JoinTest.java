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

import org.apache.doris.nereids.rules.rewrite.logical.ReorderJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.util.PlanChecker;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JoinTest extends SqlTestBase {
    @Test
    void testJoinUsing() {
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
        String sql = "select * from T2 join T2 b on T2.id = b.id and T2.id = b.id;";
        PhysicalPlan plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        Assertions.assertFalse(plan.anyMatch(PhysicalDistribute.class::isInstance));
        sql = "select * from T1 join T0 on T1.score = T0.score and T1.id = T0.id;";
        plan = PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .deriveStats()
                .optimize()
                .getBestPlanTree();
        // generate colocate join plan without physicalDistribute
        Assertions.assertFalse(plan.anyMatch(PhysicalDistribute.class::isInstance));
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter1() {
        String sql
                = "select * from T1 left outer join T2 on T1.id = T2.id where T2.score > 0";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        innerLogicalJoin(
                                logicalOlapScan(),
                                logicalFilter().when(f -> f.getPredicate().toString().equals("(score#3 > 0)"))
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter2() {
        String sql
                = "select * from T1 right outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        innerLogicalJoin(
                                logicalFilter().when(f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))")),
                                logicalOlapScan()
                        )
                );
    }

    @Test
    void testInferNotNullFromFilterAndEliminateOuter3() {
        String sql
                = "select * from T1 full outer join T2 on T1.id = T2.id where T1.id = 4 OR (T1.id > 4 AND T2.score IS NULL)";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .rewrite()
                .matchesFromRoot(
                        logicalFilter(
                                leftOuterLogicalJoin(
                                        logicalFilter().when(f -> f.getPredicate().toString().equals("((id#0 = 4) OR (id#0 > 4))")),
                                        logicalOlapScan()
                                )
                        ).when(f -> f.getPredicate().toString().equals("((id#0 = 4) OR ((id#0 > 4) AND score IS NULL))"))
                );
    }

    @Test
    @Disabled
    void testInferNotNullFromJoinAndEliminateOuter() {
        String sql
                = "select * from (select T1.id from T1 left outer join T2 on T1.id = T2.id) T1 left semi join T3 on T1.id = T3.id";
        PlanChecker.from(connectContext)
                .analyze(sql)
                .printlnTree()
                .rewrite()
                .printlnTree();
    }
}
