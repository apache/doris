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

package org.apache.doris.nereids.jobs.joinorder;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class JoinOrderJobTest {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(1, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(2, "t2", 0);
    private final LogicalOlapScan scan3 = PlanConstructor.newLogicalOlapScan(3, "t3", 0);
    private final LogicalOlapScan scan4 = PlanConstructor.newLogicalOlapScan(4, "t4", 0);
    private final LogicalOlapScan scan5 = PlanConstructor.newLogicalOlapScan(5, "t5", 0);

    @Test
    void testJoinOrderJob() {
        LogicalPlan plan = new LogicalPlanBuilder(scan1)
                .hashJoinUsing(
                        new LogicalPlanBuilder(scan2)
                                .hashJoinUsing(scan3, JoinType.INNER_JOIN, Pair.of(0, 1))
                                .hashJoinUsing(scan4, JoinType.INNER_JOIN, Pair.of(0, 1))
                                .hashJoinUsing(scan5, JoinType.INNER_JOIN, Pair.of(0, 1))
                                .build(),
                        JoinType.INNER_JOIN, Pair.of(0, 1)
                )
                .project(Lists.newArrayList(1))
                .build();
        System.out.println(plan.treeString());
        PlanChecker.from(MemoTestUtils.createConnectContext(), plan)
                .deriveStats()
                .orderJoin()
                .printlnTree();
    }
}
