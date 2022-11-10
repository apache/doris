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
import org.apache.doris.nereids.util.MemoTestUtils;
import org.apache.doris.nereids.util.PatternMatchSupported;
import org.apache.doris.nereids.util.PlanConstructor;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Map;

class MemoMergeGroupTest implements PatternMatchSupported {

    private final ConnectContext connectContext = MemoTestUtils.createConnectContext();


    /**
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
}
