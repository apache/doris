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
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * ensure child add enough distribute. update children properties if we do regular
 */
public class ChildrenPropertiesRegulator extends PlanVisitor<Boolean, Void> {

    private final GroupExpression parent;
    private final List<GroupExpression> children;
    private final List<PhysicalProperties> childrenProperties;
    private final List<PhysicalProperties> requiredProperties;
    private final JobContext jobContext;

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
    public boolean adjustChildrenProperties() {
        return parent.getPlan().accept(this, null);
    }

    @Override
    public Boolean visit(Plan plan, Void context) {
        return true;
    }

    @Override
    public Boolean visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, Void context) {
        if (agg.getAggMode() == AggMode.INPUT_TO_RESULT
                && children.get(0).getPlan() instanceof PhysicalDistribute) {
            // this means one stage gather agg, usually bad pattern
            return false;
        }
        return true;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            Void context) {
        Preconditions.checkArgument(children.size() == 2, "children.size() != 2");
        Preconditions.checkArgument(childrenProperties.size() == 2);
        Preconditions.checkArgument(requiredProperties.size() == 2);
        DistributionSpec leftDistributionSpec = childrenProperties.get(0).getDistributionSpec();
        DistributionSpec rightDistributionSpec = childrenProperties.get(1).getDistributionSpec();

        // broadcast do not need regular
        if (rightDistributionSpec instanceof DistributionSpecReplicated) {
            return true;
        }

        // shuffle
        if (!(leftDistributionSpec instanceof DistributionSpecHash)
                || !(rightDistributionSpec instanceof DistributionSpecHash)) {
            throw new RuntimeException("should not come here, two children of shuffle join should all be shuffle");
        }

        DistributionSpecHash leftHashSpec = (DistributionSpecHash) leftDistributionSpec;
        DistributionSpecHash rightHashSpec = (DistributionSpecHash) rightDistributionSpec;

        Optional<PhysicalProperties> updatedForLeft = Optional.empty();
        Optional<PhysicalProperties> updatedForRight = Optional.empty();

        if ((leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                && rightHashSpec.getShuffleType() == ShuffleType.NATURAL)) {
            // check colocate join with scan
            if (JoinUtils.couldColocateJoin(leftHashSpec, rightHashSpec)) {
                return true;
            }
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                && rightHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED) {
            // must add enforce because shuffle algorithm is not same between NATURAL and BUCKETED
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if (leftHashSpec.getShuffleType() == ShuffleType.NATURAL
                && rightHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED) {
            if (bothSideShuffleKeysAreSameOrder(leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec())) {
                return true;
            }
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if (leftHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.NATURAL) {
            // TODO: we must do shuffle on right because coordinator could not do right be selection in this case,
            //  since it always to check the left most node whether olap scan node.
            //  after we fix coordinator problem, we could do right to left bucket shuffle
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.EXECUTION_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if (leftHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED) {
            if (bothSideShuffleKeysAreSameOrder(rightHashSpec, leftHashSpec,
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec())) {
                return true;
            }
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.EXECUTION_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if ((leftHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED)) {
            if (children.get(0).getPlan() instanceof PhysicalDistribute) {
                updatedForLeft = Optional.of(calAnotherSideRequired(
                        ShuffleType.STORAGE_BUCKETED, rightHashSpec, leftHashSpec,
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec()));
            } else {
                updatedForRight = Optional.of(calAnotherSideRequired(
                        ShuffleType.EXECUTION_BUCKETED, leftHashSpec, rightHashSpec,
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
            }
        } else if ((leftHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.NATURAL)) {
            // TODO: we must do shuffle on right because coordinator could not do right be selection in this case,
            //  since it always to check the left most node whether olap scan node.
            //  after we fix coordinator problem, we could do right to left bucket shuffle
            updatedForRight = Optional.of(calAnotherSideRequired(
                    ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
        } else if ((leftHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.EXECUTION_BUCKETED)) {
            if (children.get(0).getPlan() instanceof PhysicalDistribute) {
                updatedForLeft = Optional.of(calAnotherSideRequired(
                        ShuffleType.EXECUTION_BUCKETED, rightHashSpec, leftHashSpec,
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec()));
            } else {
                updatedForRight = Optional.of(calAnotherSideRequired(
                        ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
            }

        } else if ((leftHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED
                && rightHashSpec.getShuffleType() == ShuffleType.STORAGE_BUCKETED)) {
            if (bothSideShuffleKeysAreSameOrder(rightHashSpec, leftHashSpec,
                    (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(),
                    (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec())) {
                return true;
            }
            if (children.get(0).getPlan() instanceof PhysicalDistribute) {
                updatedForLeft = Optional.of(calAnotherSideRequired(
                        ShuffleType.STORAGE_BUCKETED, rightHashSpec, leftHashSpec,
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec()));
            } else {
                updatedForRight = Optional.of(calAnotherSideRequired(
                        ShuffleType.STORAGE_BUCKETED, leftHashSpec, rightHashSpec,
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(1).getDistributionSpec()));
            }
        }

        updatedForLeft.ifPresent(physicalProperties -> updateChildEnforceAndCost(0, physicalProperties));
        updatedForRight.ifPresent(physicalProperties -> updateChildEnforceAndCost(1, physicalProperties));

        return true;
    }

    @Override
    public Boolean visitPhysicalNestedLoopJoin(PhysicalNestedLoopJoin<? extends Plan, ? extends Plan> nestedLoopJoin,
            Void context) {
        Preconditions.checkArgument(children.size() == 2, String.format("children.size() is %d", children.size()));
        Preconditions.checkArgument(childrenProperties.size() == 2);
        Preconditions.checkArgument(requiredProperties.size() == 2);
        DistributionSpec rightDistributionSpec = childrenProperties.get(1).getDistributionSpec();
        if (rightDistributionSpec instanceof DistributionSpecStorageGather) {
            updateChildEnforceAndCost(1, PhysicalProperties.GATHER);
        }
        return true;
    }

    @Override
    public Boolean visitPhysicalSetOperation(PhysicalSetOperation setOperation, Void context) {
        if (children.isEmpty()) {
            return true;
        }

        PhysicalProperties requiredProperty = requiredProperties.get(0);
        DistributionSpec requiredDistributionSpec = requiredProperty.getDistributionSpec();
        if (requiredDistributionSpec instanceof DistributionSpecGather) {
            for (int i = 0; i < childrenProperties.size(); i++) {
                if (childrenProperties.get(i).getDistributionSpec() instanceof DistributionSpecStorageGather) {
                    updateChildEnforceAndCost(i, PhysicalProperties.GATHER);
                }
            }
        } else if (requiredDistributionSpec instanceof DistributionSpecAny) {
            for (int i = 0; i < childrenProperties.size(); i++) {
                if (childrenProperties.get(i).getDistributionSpec() instanceof DistributionSpecStorageAny
                        || childrenProperties.get(i).getDistributionSpec() instanceof DistributionSpecStorageGather
                        || childrenProperties.get(i).getDistributionSpec() instanceof DistributionSpecGather
                        || (childrenProperties.get(i).getDistributionSpec() instanceof DistributionSpecHash
                        && ((DistributionSpecHash) childrenProperties.get(i).getDistributionSpec())
                        .getShuffleType() == ShuffleType.NATURAL)) {
                    updateChildEnforceAndCost(i, PhysicalProperties.EXECUTION_ANY);
                }
            }
        } else if (requiredDistributionSpec instanceof DistributionSpecHash) {
            // TODO: should use the most common hash spec as basic
            DistributionSpecHash basic = (DistributionSpecHash) requiredDistributionSpec;
            for (int i = 0; i < childrenProperties.size(); i++) {
                DistributionSpecHash current = (DistributionSpecHash) childrenProperties.get(i).getDistributionSpec();
                if (current.getShuffleType() != ShuffleType.EXECUTION_BUCKETED
                        || !bothSideShuffleKeysAreSameOrder(basic, current,
                        (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                        (DistributionSpecHash) requiredProperties.get(i).getDistributionSpec())) {
                    PhysicalProperties target = calAnotherSideRequired(
                            ShuffleType.EXECUTION_BUCKETED, basic, current,
                            (DistributionSpecHash) requiredProperties.get(0).getDistributionSpec(),
                            (DistributionSpecHash) requiredProperties.get(i).getDistributionSpec());
                    updateChildEnforceAndCost(i, target);
                }
            }
        }
        return true;
    }

    private boolean bothSideShuffleKeysAreSameOrder(
            DistributionSpecHash notShuffleSideOutput, DistributionSpecHash shuffleSideOutput,
            DistributionSpecHash notShuffleSideRequired, DistributionSpecHash shuffleSideRequired) {
        return shuffleSideOutput.getOrderedShuffledColumns().equals(
                calAnotherSideRequiredShuffleIds(notShuffleSideOutput, notShuffleSideRequired, shuffleSideRequired));
    }

    private List<ExprId> calAnotherSideRequiredShuffleIds(DistributionSpecHash notShuffleSideOutput,
            DistributionSpecHash notShuffleSideRequired, DistributionSpecHash shuffleSideRequired) {
        List<ExprId> rightShuffleIds = new ArrayList<>();
        for (ExprId scanId : notShuffleSideOutput.getOrderedShuffledColumns()) {
            int index = notShuffleSideRequired.getOrderedShuffledColumns().indexOf(scanId);
            if (index == -1) {
                // when there is no exprId in notShuffleSideOutput, we need to check EquivalenceExprIds
                Set<ExprId> equivalentExprIds = notShuffleSideOutput.getEquivalenceExprIdsOf(scanId);
                for (ExprId alternativeExpr : equivalentExprIds) {
                    index = notShuffleSideRequired.getOrderedShuffledColumns().indexOf(alternativeExpr);
                    if (index != -1) {
                        break;
                    }
                }
            }
            Preconditions.checkArgument(index != -1);
            rightShuffleIds.add(shuffleSideRequired.getOrderedShuffledColumns().get(index));
        }
        return rightShuffleIds;
    }

    private PhysicalProperties calAnotherSideRequired(ShuffleType shuffleType,
            DistributionSpecHash notShuffleSideOutput, DistributionSpecHash shuffleSideOutput,
            DistributionSpecHash notShuffleSideRequired, DistributionSpecHash shuffleSideRequired) {
        List<ExprId> shuffleSideIds = calAnotherSideRequiredShuffleIds(notShuffleSideOutput,
                notShuffleSideRequired, shuffleSideRequired);
        return new PhysicalProperties(new DistributionSpecHash(shuffleSideIds, shuffleType,
                shuffleSideOutput.getTableId(), shuffleSideOutput.getSelectedIndexId(),
                shuffleSideOutput.getPartitionIds()));
    }

    private void updateChildEnforceAndCost(int index, PhysicalProperties targetProperties) {
        GroupExpression child = children.get(index);
        Pair<Cost, List<PhysicalProperties>> lowest = child.getLowestCostTable().get(childrenProperties.get(index));
        PhysicalProperties output = child.getOutputProperties(childrenProperties.get(index));
        DistributionSpec target = targetProperties.getDistributionSpec();
        updateChildEnforceAndCost(child, output, target, lowest.first);
        childrenProperties.set(index, targetProperties);
    }

    // TODO: why add enforcer according to target and target is from requiredProperties not regular
    private void updateChildEnforceAndCost(GroupExpression child, PhysicalProperties childOutput,
            DistributionSpec target, Cost currentCost) {
        if (child.getPlan() instanceof PhysicalDistribute) {
            //To avoid continuous distribute operator, we just enforce the child's child
            childOutput = child.getInputPropertiesList(childOutput).get(0);
            Pair<Cost, GroupExpression> newChildAndCost = child.getOwnerGroup().getLowestCostPlan(childOutput).get();
            child = newChildAndCost.second;
            currentCost = newChildAndCost.first;
        }

        PhysicalProperties newOutputProperty = new PhysicalProperties(target);
        GroupExpression enforcer = target.addEnforcer(child.getOwnerGroup());
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
    }
}
