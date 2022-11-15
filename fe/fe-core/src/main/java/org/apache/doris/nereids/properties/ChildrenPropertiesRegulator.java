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
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * ensure child add enough distribute.
 */
public class ChildrenPropertiesRegulator extends PlanVisitor<Double, Void> {

    private final GroupExpression parent;
    private final List<GroupExpression> children;
    private final List<PhysicalProperties> childrenProperties;
    private final List<PhysicalProperties> requiredProperties;
    private final JobContext jobContext;
    private double enforceCost = 0.0;

    public ChildrenPropertiesRegulator(GroupExpression parent, List<GroupExpression> children,
            List<PhysicalProperties> childrenProperties, List<PhysicalProperties> requiredProperties,
            JobContext jobContext) {
        this.parent = parent;
        this.children = children;
        this.childrenProperties = childrenProperties;
        this.requiredProperties = requiredProperties;
        this.jobContext = jobContext;
    }

    /**
     * adjust children properties
     * @return enforce cost.
     */
    public double adjustChildrenProperties() {
        return parent.getPlan().accept(this, null);
    }

    @Override
    public Double visit(Plan plan, Void context) {
        return enforceCost;
    }

    @Override
    public Double visitPhysicalAggregate(PhysicalAggregate<? extends Plan> agg, Void context) {
        if (agg.isFinalPhase()
                && agg.getAggPhase() == AggPhase.LOCAL
                && children.get(0).getPlan() instanceof PhysicalDistribute) {
            return -1.0;
        }
        return 0.0;
    }

    @Override
    public Double visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            Void context) {
        Preconditions.checkArgument(children.size() == 2, String.format("children.size() is %d", children.size()));
        Preconditions.checkArgument(childrenProperties.size() == 2);
        Preconditions.checkArgument(requiredProperties.size() == 2);
        DistributionSpec leftDistributionSpec = childrenProperties.get(0).getDistributionSpec();
        DistributionSpec rightDistributionSpec = childrenProperties.get(1).getDistributionSpec();

        // broadcast do not need regular
        if (rightDistributionSpec instanceof DistributionSpecReplicated) {
            return enforceCost;
        }

        // shuffle
        if (!(leftDistributionSpec instanceof DistributionSpecHash)
                || !(rightDistributionSpec instanceof DistributionSpecHash)) {
            throw new RuntimeException("should not come here");
        }

        DistributionSpecHash leftHashSpec = (DistributionSpecHash) leftDistributionSpec;
        DistributionSpecHash rightHashSpec = (DistributionSpecHash) rightDistributionSpec;

        GroupExpression leftChild = children.get(0);
        final Pair<Double, List<PhysicalProperties>> leftLowest
                = leftChild.getLowestCostTable().get(requiredProperties.get(0));
        PhysicalProperties leftOutput = leftChild.getOutputProperties(requiredProperties.get(0));

        GroupExpression rightChild = children.get(1);
        final Pair<Double, List<PhysicalProperties>> rightLowest
                = rightChild.getLowestCostTable().get(requiredProperties.get(1));
        PhysicalProperties rightOutput = rightChild.getOutputProperties(requiredProperties.get(1));

        // check colocate join
        if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                && rightHashSpec.getShuffleType() == ShuffleType.NATURAL) {
            if (JoinUtils.couldColocateJoin(leftHashSpec, rightHashSpec)) {
                return enforceCost;
            }
        }

        // check right hand must distribute
        if (rightHashSpec.getShuffleType() != ShuffleType.ENFORCED) {
            enforceCost += updateChildEnforceAndCost(rightChild, rightOutput,
                    rightHashSpec, rightLowest.first);
            childrenProperties.set(1, new PhysicalProperties(
                    rightHashSpec.withShuffleType(ShuffleType.ENFORCED),
                    childrenProperties.get(1).getOrderSpec()));
        }

        // check bucket shuffle join
        if (leftHashSpec.getShuffleType() != ShuffleType.ENFORCED) {
            if (ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()) {
                return enforceCost;
            }
            enforceCost += updateChildEnforceAndCost(leftChild, leftOutput,
                    leftHashSpec, leftLowest.first);
            childrenProperties.set(0, new PhysicalProperties(
                    leftHashSpec.withShuffleType(ShuffleType.ENFORCED),
                    childrenProperties.get(0).getOrderSpec()));
        }
        return enforceCost;
    }

    private double updateChildEnforceAndCost(GroupExpression child, PhysicalProperties childOutput,
            DistributionSpecHash required, double currentCost) {
        DistributionSpec outputDistributionSpec;
        outputDistributionSpec = required.withShuffleType(ShuffleType.ENFORCED);

        PhysicalProperties newOutputProperty = new PhysicalProperties(outputDistributionSpec);
        GroupExpression enforcer = outputDistributionSpec.addEnforcer(child.getOwnerGroup());
        jobContext.getCascadesContext().getMemo().addEnforcerPlan(enforcer, child.getOwnerGroup());
        double enforceCost = CostCalculator.calculateCost(enforcer);

        if (enforcer.updateLowestCostTable(newOutputProperty,
                Lists.newArrayList(childOutput), enforceCost + currentCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        child.getOwnerGroup().setBestPlan(enforcer, enforceCost + currentCost, newOutputProperty);
        return enforceCost;
    }
}
