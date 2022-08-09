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

package org.apache.doris.nereids.properties;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.PlanContext;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Used for parent property drive.
 */
public class RequestPropertyDeriver extends PlanVisitor<Void, PlanContext> {
    /*
     * requestPropertyFromParent
     *             │
     *             ▼
     *          curNode (current plan node in current CostAndEnforcerJob)
     *             │
     *             ▼
     * requestPropertyToChildren
     */
    private PhysicalProperties requestPropertyFromParent;
    private List<List<PhysicalProperties>> requestPropertyToChildren;

    public RequestPropertyDeriver(JobContext context) {
        this.requestPropertyFromParent = context.getRequiredProperties();
    }

    public List<List<PhysicalProperties>> getRequiredPropertyListList(GroupExpression groupExpression) {
        requestPropertyToChildren = Lists.newArrayList();
        groupExpression.getPlan().accept(this, new PlanContext(groupExpression));
        return requestPropertyToChildren;
    }

    @Override
    public Void visit(Plan plan, PlanContext context) {
        List<PhysicalProperties> requiredPropertyList = Lists.newArrayList();
        for (int i = 0; i < context.getGroupExpression().arity(); i++) {
            requiredPropertyList.add(new PhysicalProperties());
        }
        requestPropertyToChildren.add(requiredPropertyList);
        return null;
    }

    @Override
    public Void visitPhysicalHashJoin(PhysicalHashJoin<Plan, Plan> hashJoin, PlanContext context) {
        // for broadcast join
        List<PhysicalProperties> propertiesForBroadcast = Lists.newArrayList(
                new PhysicalProperties(),
                new PhysicalProperties(new DistributionSpecReplicated())
        );
        // for shuffle join
        Pair<List<SlotReference>, List<SlotReference>> onClauseUsedSlots = JoinUtils.getOnClauseUsedSlots(hashJoin);
        List<PhysicalProperties> propertiesForShuffle = Lists.newArrayList(
                new PhysicalProperties(new DistributionSpecHash(onClauseUsedSlots.first, ShuffleType.JOIN)),
                new PhysicalProperties(new DistributionSpecHash(onClauseUsedSlots.second, ShuffleType.JOIN)));

        if (!JoinUtils.onlyBroadcast(hashJoin)) {
            requestPropertyToChildren.add(propertiesForShuffle);
        }
        if (!JoinUtils.onlyShuffle(hashJoin)) {
            requestPropertyToChildren.add(propertiesForBroadcast);
        }

        return null;
    }

    @Override
    public Void visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<Plan, Plan> nestedLoopJoin, PlanContext context) {
        // TODO: copy from physicalHashJoin, should update according to physical nested loop join properties.
        // for broadcast join
        List<PhysicalProperties> propertiesForBroadcast = Lists.newArrayList(
                new PhysicalProperties(),
                new PhysicalProperties(new DistributionSpecReplicated())
        );
        // for shuffle join
        Pair<List<SlotReference>, List<SlotReference>> onClauseUsedSlots
                = JoinUtils.getOnClauseUsedSlots(nestedLoopJoin);
        List<PhysicalProperties> propertiesForShuffle = Lists.newArrayList(
                new PhysicalProperties(new DistributionSpecHash(onClauseUsedSlots.first, ShuffleType.JOIN)),
                new PhysicalProperties(new DistributionSpecHash(onClauseUsedSlots.second, ShuffleType.JOIN)));

        if (!JoinUtils.onlyBroadcast(nestedLoopJoin)) {
            requestPropertyToChildren.add(propertiesForShuffle);
        }
        if (!JoinUtils.onlyShuffle(nestedLoopJoin)) {
            requestPropertyToChildren.add(propertiesForBroadcast);
        }

        return null;
    }

    protected static List<PhysicalProperties> computeShuffleJoinRequiredProperties(
            PhysicalProperties requestedProperty, List<SlotReference> leftShuffleColumns,
            List<SlotReference> rightShuffleColumns) {

        // requestedProperty type isn't SHUFFLE_JOIN,
        if (!(requestedProperty.getDistributionSpec() instanceof DistributionSpecHash
                && ((DistributionSpecHash) requestedProperty.getDistributionSpec()).getShuffleType()
                == ShuffleType.JOIN)) {
            return Lists.newArrayList(
                    new PhysicalProperties(new DistributionSpecHash(leftShuffleColumns, ShuffleType.JOIN)),
                    new PhysicalProperties(new DistributionSpecHash(rightShuffleColumns, ShuffleType.JOIN)));
        }

        // adjust the required property shuffle columns based on the column order required by parent
        DistributionSpecHash distributionSpec = (DistributionSpecHash) requestedProperty.getDistributionSpec();
        List<SlotReference> requestedColumns = distributionSpec.getShuffledColumns();

        boolean adjustBasedOnLeft = Utils.equalsIgnoreOrder(leftShuffleColumns, requestedColumns);
        boolean adjustBasedOnRight = Utils.equalsIgnoreOrder(rightShuffleColumns, requestedColumns);
        if (!adjustBasedOnLeft && !adjustBasedOnRight) {
            return Lists.newArrayList(
                    new PhysicalProperties(new DistributionSpecHash(leftShuffleColumns, ShuffleType.JOIN)),
                    new PhysicalProperties(new DistributionSpecHash(rightShuffleColumns, ShuffleType.JOIN)));
        }

        return Lists.newArrayList(
                new PhysicalProperties(new DistributionSpecHash(leftShuffleColumns, ShuffleType.JOIN)),
                new PhysicalProperties(new DistributionSpecHash(rightShuffleColumns, ShuffleType.JOIN)));

    }

}

