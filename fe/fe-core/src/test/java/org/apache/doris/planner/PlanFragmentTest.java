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

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.jmockit.Deencapsulation;

import com.google.common.collect.Lists;

import java.util.List;

import mockit.Expectations;
import mockit.Injectable;
import org.junit.Assert;
import org.junit.Test;

public class PlanFragmentTest {

    @Test
    public void testGetInputDataPartition(@Injectable PlanNode planNode,
                                          @Injectable SlotRef slotRef,
                                          @Injectable PlanNode childPlanNode,
                                          @Injectable DataPartition childDataPartition,
                                          @Injectable DataPartition childOutputPartition) {
        List<Expr> partitionExprs = Lists.newArrayList();
        partitionExprs.add(slotRef);
        DataPartition dataPartition = DataPartition.hashPartitioned(partitionExprs);
        IdGenerator<PlanFragmentId> idGenerator = PlanFragmentId.createGenerator();
        new Expectations() {
            {
                planNode.getChildren();
                result = Lists.newArrayList();
                childPlanNode.getChildren();
                result = Lists.newArrayList();
            }
        };
        PlanFragment planFragment = new PlanFragment(idGenerator.getNextId(), planNode, dataPartition);
        PlanFragment childPlanFragment = new PlanFragment(idGenerator.getNextId(), childPlanNode, childDataPartition);
        Deencapsulation.setField(planFragment, "children", Lists.newArrayList(childPlanFragment));
        Deencapsulation.setField(childPlanFragment, "outputPartition", childOutputPartition);

        List<DataPartition> inputDataPartition = planFragment.getInputDataPartition();
        Assert.assertEquals(2, inputDataPartition.size());
    }
}
