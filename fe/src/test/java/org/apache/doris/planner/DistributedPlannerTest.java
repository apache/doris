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

package org.apache.doris.planner;

import org.apache.doris.analysis.TupleId;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.doris.common.jmockit.Deencapsulation;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

public class DistributedPlannerTest {

    @Mocked
    PlannerContext plannerContext;

    @Test
    public void testAssertFragmentWithDistributedInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment,
                                                       @Injectable PlanNodeId planNodeId,
                                                       @Injectable PlanFragmentId planFragmentId,
                                                       @Injectable PlanNode inputPlanRoot,
                                                       @Injectable TupleId tupleId) {
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        List<TupleId> tupleIdList = Lists.newArrayList(tupleId);
        Set<TupleId> tupleIdSet = Sets.newHashSet(tupleId);
        Deencapsulation.setField(inputPlanRoot, "tupleIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "tblRefIds", tupleIdList);
        Deencapsulation.setField(inputPlanRoot, "nullableTupleIds", Sets.newHashSet(tupleId));
        Deencapsulation.setField(inputPlanRoot, "conjuncts", Lists.newArrayList());
        new Expectations() {
            {
                inputFragment.isPartitioned();
                result = true;
                plannerContext.getNextNodeId();
                result = planNodeId;
                plannerContext.getNextFragmentId();
                result = planFragmentId;
                inputFragment.getPlanRoot();
                result = inputPlanRoot;
                inputPlanRoot.getTupleIds();
                result = tupleIdList;
                inputPlanRoot.getTblRefIds();
                result = tupleIdList;
                inputPlanRoot.getNullableTupleIds();
                result = tupleIdSet;
                assertNumRowsNode.getChildren();
                result = inputPlanRoot;
            }
        };

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                                                             assertNumRowsNode, inputFragment);
        Assert.assertFalse(assertFragment.isPartitioned());
        Assert.assertSame(assertNumRowsNode, assertFragment.getPlanRoot());
    }

    @Test
    public void testAssertFragmentWithUnpartitionInput(@Injectable AssertNumRowsNode assertNumRowsNode,
                                                       @Injectable PlanFragment inputFragment){
        DistributedPlanner distributedPlanner = new DistributedPlanner(plannerContext);

        PlanFragment assertFragment = Deencapsulation.invoke(distributedPlanner, "createAssertFragment",
                                                             assertNumRowsNode, inputFragment);
        Assert.assertSame(assertFragment, inputFragment);
        Assert.assertTrue(assertFragment.getPlanRoot() instanceof AssertNumRowsNode);
    }

}
