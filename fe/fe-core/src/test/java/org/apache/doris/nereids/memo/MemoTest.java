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
import org.apache.doris.nereids.trees.plans.DummyPlan;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

class MemoTest {

    @Test
    void mergeGroup() throws Exception {
        Memo memo = new Memo();
        GroupId gid2 = new GroupId(2);
        Group srcGroup = new Group(gid2, new GroupExpression(new DummyPlan()), new LogicalProperties(ArrayList::new));
        GroupId gid3 = new GroupId(3);
        Group dstGroup = new Group(gid3, new GroupExpression(new DummyPlan()), new LogicalProperties(ArrayList::new));
        DummyPlan d = new DummyPlan();
        GroupExpression ge1 = new GroupExpression(d, Arrays.asList(srcGroup));
        GroupId gid0 = new GroupId(0);
        Group g1 = new Group(gid0, ge1, new LogicalProperties(ArrayList::new));
        g1.setBestPlan(ge1, Double.MIN_VALUE, PhysicalProperties.ANY);
        GroupExpression ge2 = new GroupExpression(d, Arrays.asList(dstGroup));
        GroupId gid1 = new GroupId(1);
        Group g2 = new Group(gid1, ge2, new LogicalProperties(ArrayList::new));
        Map<GroupId, Group> groups = (Map<GroupId, Group>) Deencapsulation.getField(memo, "groups");
        groups.put(gid2, srcGroup);
        groups.put(gid3, dstGroup);
        groups.put(gid0, g1);
        groups.put(gid1, g2);
        Map<GroupExpression, GroupExpression> groupExpressions =
                (Map<GroupExpression, GroupExpression>) Deencapsulation.getField(memo, "groupExpressions");
        groupExpressions.put(ge1, ge1);
        groupExpressions.put(ge2, ge2);
        memo.mergeGroup(srcGroup, dstGroup);
        Assertions.assertNull(g1.getBestPlan(PhysicalProperties.ANY));
        Assertions.assertEquals(ge1.getOwnerGroup(), g2);
    }
}
