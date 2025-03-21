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

import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.FakePlan;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class GroupExpressionTest {

    @Test
    public void testMergeToNotOwnerRemoveWhenTargetWithLowerCost() {
        GroupExpression source = new GroupExpression(new FakePlan());
        source.updateLowestCostTable(PhysicalProperties.GATHER, Lists.newArrayList(), Cost.infinite(new SessionVariable()));
        source.putOutputPropertiesMap(PhysicalProperties.GATHER, PhysicalProperties.ANY);

        GroupExpression target = new GroupExpression(new FakePlan());
        target.updateLowestCostTable(PhysicalProperties.ANY, Lists.newArrayList(), Cost.zero(new SessionVariable()));
        target.putOutputPropertiesMap(PhysicalProperties.ANY, PhysicalProperties.ANY);

        source.mergeToNotOwnerRemove(target);
        Assertions.assertTrue(target.getLowestCostTable().containsKey(PhysicalProperties.ANY));
        Assertions.assertTrue(target.getLowestCostTable().containsKey(PhysicalProperties.GATHER));
        Assertions.assertEquals(PhysicalProperties.ANY, target.getOutputProperties(PhysicalProperties.ANY));
    }

    @Test
    public void testMergeToNotOwnerRemoveWhenSourceWithLowerCost() {
        GroupExpression source = new GroupExpression(new FakePlan());
        source.updateLowestCostTable(PhysicalProperties.GATHER, Lists.newArrayList(), Cost.zero(new SessionVariable()));
        source.putOutputPropertiesMap(PhysicalProperties.GATHER, PhysicalProperties.ANY);

        GroupExpression target = new GroupExpression(new FakePlan());
        target.updateLowestCostTable(PhysicalProperties.ANY, Lists.newArrayList(), Cost.infinite(new SessionVariable()));
        target.putOutputPropertiesMap(PhysicalProperties.ANY, PhysicalProperties.ANY);

        source.mergeToNotOwnerRemove(target);
        Assertions.assertTrue(target.getLowestCostTable().containsKey(PhysicalProperties.ANY));
        Assertions.assertTrue(target.getLowestCostTable().containsKey(PhysicalProperties.GATHER));
        Assertions.assertEquals(PhysicalProperties.GATHER, target.getOutputProperties(PhysicalProperties.ANY));
    }
}
