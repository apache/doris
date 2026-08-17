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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.nereids.trees.expressions.Add;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.util.LogicalPlanBuilder;
import org.apache.doris.nereids.util.MemoPatternMatchSupported;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Test;

public class ProjectOtherJoinConditionForNestedLoopJoinTest implements MemoPatternMatchSupported {
    private final LogicalOlapScan scan1 = PlanConstructor.newLogicalOlapScan(0, "t1", 0);
    private final LogicalOlapScan scan2 = PlanConstructor.newLogicalOlapScan(1, "t2", 0);

    @Test
    public void testNestedLoopJoin() {
        Slot a = scan1.getOutput().get(1);
        Slot b = scan2.getOutput().get(0);
        Expression otherCondition = new LessThan(a, new Add(b, b));

        LogicalPlan join = new LogicalPlanBuilder(scan1).join(scan2, JoinType.CROSS_JOIN,
                Lists.newArrayList(), Lists.newArrayList(otherCondition)).build();
        ConnectContext connectContext = MemoTestUtils.createConnectContext();
        PlanChecker.from(connectContext, join)
                .applyTopDown(new ProjectOtherJoinConditionForNestedLoopJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalOlapScan(),
                                // proj list: id#10002, name#10003, (id#10002 + id#10002) AS `(id + id)`#0
                                logicalProject().when(proj -> proj.getProjects().size() == 3)
                                )
                ).printlnTree();
    }

    @Test
    public void testHashJoin() {
        Slot id1 = scan1.getOutput().get(0);
        Slot name1 = scan1.getOutput().get(1);
        Slot id2 = scan2.getOutput().get(0);
        Slot name2 = scan2.getOutput().get(1);
        Expression eq = new EqualTo(name1, name2);
        Expression otherCondition = new LessThan(id1, new Add(id2, id2));

        LogicalPlan join = new LogicalPlanBuilder(scan1).join(scan2, JoinType.CROSS_JOIN,
                Lists.newArrayList(eq), Lists.newArrayList(otherCondition)).build();
        PlanChecker.from(MemoTestUtils.createConnectContext(), join)
                .applyTopDown(new ProjectOtherJoinConditionForNestedLoopJoin())
                .matchesFromRoot(
                        logicalJoin(
                                logicalOlapScan(),
                                logicalOlapScan()
                        )
                ).printlnTree();
    }
}
