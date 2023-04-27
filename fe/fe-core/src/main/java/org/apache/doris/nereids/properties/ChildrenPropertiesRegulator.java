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
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.cost.CostCalculator;
import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DistributionSpecHash.ShuffleType;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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
     *
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
    public Double visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, Void context) {
        if (agg.getAggMode() == AggMode.INPUT_TO_RESULT
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
        final Pair<Cost, List<PhysicalProperties>> leftLowest
                = leftChild.getLowestCostTable().get(childrenProperties.get(0));
        PhysicalProperties leftOutput = leftChild.getOutputProperties(childrenProperties.get(0));

        GroupExpression rightChild = children.get(1);
        Pair<Cost, List<PhysicalProperties>> rightLowest
                = rightChild.getLowestCostTable().get(childrenProperties.get(1));
        PhysicalProperties rightOutput = rightChild.getOutputProperties(childrenProperties.get(1));

        // check colocate join
        if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                && rightHashSpec.getShuffleType() == ShuffleType.NATURAL) {
            if (JoinUtils.couldColocateJoin(leftHashSpec, rightHashSpec)) {
                return enforceCost;
            }
        }

        // check bucket shuffle join
        if (leftHashSpec.getShuffleType() != ShuffleType.ENFORCED) {
            if (ConnectContext.get().getSessionVariable().isEnableBucketShuffleJoin()) {
                // We need to recalculate the required property of right child,
                // to make right child compatible with left child.
                PhysicalProperties rightRequireProperties = calRightRequiredOfBucketShuffleJoin(
                        leftHashSpec, rightHashSpec);
                if (!rightOutput.equals(rightRequireProperties)) {
                    updateChildEnforceAndCost(rightChild, rightOutput,
                            (DistributionSpecHash) rightRequireProperties.getDistributionSpec(), rightLowest.first);
                }
                childrenProperties.set(1, rightRequireProperties);
                return enforceCost;
            }
            updateChildEnforceAndCost(leftChild, leftOutput,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(), leftLowest.first);
            childrenProperties.set(0, requiredProperties.get(0));
        }

        // check right hand must distribute.
        if (rightHashSpec.getShuffleType() != ShuffleType.ENFORCED) {
            updateChildEnforceAndCost(rightChild, rightOutput,
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(), rightLowest.first);
            childrenProperties.set(1, requiredProperties.get(1));
        }

        return enforceCost;
    }

    private PhysicalProperties calRightRequiredOfBucketShuffleJoin(DistributionSpecHash leftHashSpec,
            DistributionSpecHash rightHashSpec) {
        Preconditions.checkArgument(leftHashSpec.getShuffleType() != ShuffleType.ENFORCED);
        DistributionSpecHash leftRequireSpec = (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec();
        DistributionSpecHash rightRequireSpec = (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec();
        List<ExprId> rightShuffleIds = new ArrayList<>();
        for (ExprId scanId : leftHashSpec.getOrderedShuffledColumns()) {
            int index = leftRequireSpec.getOrderedShuffledColumns().indexOf(scanId);
            if (index == -1) {
                // when there is no exprId in leftHashSpec, we need to check EquivalenceExprIds
                Set<ExprId> equivalentExprIds = leftHashSpec.getEquivalenceExprIdsOf(scanId);
                for (ExprId alternativeExpr : equivalentExprIds) {
                    index = leftRequireSpec.getOrderedShuffledColumns().indexOf(alternativeExpr);
                    if (index != -1) {
                        break;
                    }
                }
            }
            Preconditions.checkArgument(index != -1);
            rightShuffleIds.add(rightRequireSpec.getOrderedShuffledColumns().get(index));
        }
        return new PhysicalProperties(new DistributionSpecHash(rightShuffleIds, ShuffleType.ENFORCED,
                rightHashSpec.getTableId(), rightHashSpec.getSelectedIndexId(), rightHashSpec.getPartitionIds()));
    }

    private double updateChildEnforceAndCost(GroupExpression child, PhysicalProperties childOutput,
            DistributionSpecHash required, Cost currentCost) {
        if (child.getPlan() instanceof PhysicalDistribute) {
            //To avoid continuous distribute operator, we just enforce the child's child
            childOutput = child.getInputPropertiesList(childOutput).get(0);
            Pair<Cost, GroupExpression> newChildAndCost
                    = child.getOwnerGroup().getLowestCostPlan(childOutput).get();
            child = newChildAndCost.second;
            currentCost = newChildAndCost.first;
        }

        DistributionSpec outputDistributionSpec;
        outputDistributionSpec = required.withShuffleType(ShuffleType.ENFORCED);

        PhysicalProperties newOutputProperty = new PhysicalProperties(outputDistributionSpec);
        GroupExpression enforcer = outputDistributionSpec.addEnforcer(child.getOwnerGroup());
        jobContext.getCascadesContext().getMemo().addEnforcerPlan(enforcer, child.getOwnerGroup());
        Cost totalCost = CostCalculator.addChildCost(enforcer.getPlan(),
                CostCalculator.calculateCost(enforcer, Lists.newArrayList(childOutput)),
                currentCost,
                0);

        if (enforcer.updateLowestCostTable(newOutputProperty,
                Lists.newArrayList(childOutput), totalCost)) {
            enforcer.putOutputPropertiesMap(newOutputProperty, newOutputProperty);
        }
        child.getOwnerGroup().setBestPlan(enforcer, totalCost, newOutputProperty);
        return enforceCost;
    }
}
