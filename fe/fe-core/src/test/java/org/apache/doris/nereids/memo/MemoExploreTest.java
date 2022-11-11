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

import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.FakePlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanChecker;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

class MemoExploreTest implements PatternMatchSupported {

    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();

    private final LogicalJoin<LogicalOlapScan, LogicalOlapScan> logicalJoinAB = new LogicalJoin<>(JoinType.INNER_JOIN,
            PlanConstructor.newLogicalOlapScan(0, "A", 0),
            PlanConstructor.newLogicalOlapScan(1, "B", 0));

    /*
     * ┌─────────────────────────┐     ┌───────────┐
     * │  ┌─────┐       ┌─────┐  │     │  ┌─────┐  │
     * │  │0┌─┐ │       │1┌─┐ │  │     │  │1┌─┐ │  │
     * │  │ └┼┘ │       │ └┼┘ │  │     │  │ └┼┘ │  │
     * │  └──┼──┘       └──┼──┘  │     │  └──┼──┘  │
     * │Memo │             │     ├────►│Memo │     │
     * │  ┌──▼──┐       ┌──▼──┐  │     │  ┌──▼──┐  │
     * │  │ src │       │ dst │  │     │  │ dst │  │
     * │  │2    │       │3    │  │     │  │3    │  │
     * │  └─────┘       └─────┘  │     │  └─────┘  │
     * └─────────────────────────┘     └───────────┘
     */
    @Test
    void test() {
        Group srcGroup = new Group(new GroupId(2), new GroupExpression(new FakePlan()),
                new LogicalProperties(ArrayList::new));
        Group dstGroup = new Group(new GroupId(3), new GroupExpression(new FakePlan()),
                new LogicalProperties(ArrayList::new));

        FakePlan fakePlan = new FakePlan();
        GroupExpression srcParentExpression = new GroupExpression(fakePlan, Lists.newArrayList(srcGroup));
        Group srcParentGroup = new Group(new GroupId(0), srcParentExpression, new LogicalProperties(ArrayList::new));
        srcParentGroup.setBestPlan(srcParentExpression, Double.MIN_VALUE, PhysicalProperties.ANY);
        GroupExpression dstParentExpression = new GroupExpression(fakePlan, Lists.newArrayList(dstGroup));
        Group dstParentGroup = new Group(new GroupId(1), dstParentExpression, new LogicalProperties(ArrayList::new));

        Memo memo = new Memo();
        Map<GroupId, Group> groups = Deencapsulation.getField(memo, "groups");
        groups.put(srcGroup.getGroupId(), srcGroup);
        groups.put(dstGroup.getGroupId(), dstGroup);
        groups.put(srcParentGroup.getGroupId(), srcParentGroup);
        groups.put(dstParentGroup.getGroupId(), dstParentGroup);
        Map<GroupExpression, GroupExpression> groupExpressions =
                Deencapsulation.getField(memo, "groupExpressions");
        groupExpressions.put(srcParentExpression, srcParentExpression);
        groupExpressions.put(dstParentExpression, dstParentExpression);

        memo.mergeGroup(srcGroup, dstGroup);

        // check
        // Assertions.assertEquals(0, srcGroup.getParentGroupExpressions().size());
        Assertions.assertEquals(0, srcGroup.getPhysicalExpressions().size());
        Assertions.assertEquals(0, srcGroup.getLogicalExpressions().size());

        Assertions.assertEquals(0, srcParentGroup.getParentGroupExpressions().size());
        Assertions.assertEquals(0, srcParentGroup.getPhysicalExpressions().size());
        Assertions.assertEquals(0, srcParentGroup.getLogicalExpressions().size());

        // TODO: add root test.
        // Assertions.assertEquals(memo.getRoot(), dstParentGroup);

        Assertions.assertEquals(2, dstGroup.getPhysicalExpressions().size());
        Assertions.assertEquals(1, dstParentGroup.getPhysicalExpressions().size());

        Assertions.assertEquals(dstParentGroup, srcParentExpression.getOwnerGroup());
        Assertions.assertEquals(dstGroup, srcParentExpression.child(0));

        Assertions.assertNull(srcParentGroup.getBestPlan(PhysicalProperties.ANY));
    }

    /**
     * Original:
     * Group 0: LogicalOlapScan A
     * Group 1: LogicalOlapScan B
     * Group 2: Join(Group 0, Group 1)
     * <p>
     * Then:
     * Copy In Join(Group 1, Group 0) into Group 3
     * <p>
     * Expected:
     * Group 0: LogicalOlapScan A
     * Group 1: LogicalOlapScan B
     * Group 2: Join(Group 0, Group 1), Project(Group3)
     * Group 3: Join(Group 1, Group 0)
     */
    @Test
    public void testInsertSameGroup() {
        PlanChecker.from(connectContext, logicalJoinAB)
                .transform(
                        // swap join's children
                        logicalJoin(logicalOlapScan(), logicalOlapScan()).then(joinBA ->
                                new LogicalProject<>(Lists.newArrayList(joinBA.getOutput()),
                                        new LogicalJoin<>(JoinType.INNER_JOIN, joinBA.right(), joinBA.left()))
                        ))
                .checkGroupNum(4)
                .checkGroupExpressionNum(5)
                .checkMemo(memo -> {
                    Group root = memo.getRoot();
                    Assertions.assertEquals(2, root.getLogicalExpressions().size());
                    GroupExpression joinAB = root.getLogicalExpressions().get(0);
                    GroupExpression project = root.getLogicalExpressions().get(1);
                    GroupExpression joinBA = project.child(0).getLogicalExpression();
                    Assertions.assertTrue(joinAB.getPlan() instanceof LogicalJoin);
                    Assertions.assertTrue(joinBA.getPlan() instanceof LogicalJoin);
                    Assertions.assertEquals(joinAB.child(0), joinBA.child(1));
                    Assertions.assertEquals(joinAB.child(1), joinBA.child(0));
                });
    }
}
