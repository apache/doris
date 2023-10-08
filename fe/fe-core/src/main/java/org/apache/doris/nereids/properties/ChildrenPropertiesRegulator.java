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
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinction;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.SortPhase;
import org.apache.doris.nereids.trees.plans.physical.AbstractPhysicalSort;
import org.apache.doris.nereids.trees.plans.physical.PhysicalDistribute;
import org.apache.doris.nereids.trees.plans.physical.PhysicalFilter;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalNestedLoopJoin;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.trees.plans.physical.PhysicalSetOperation;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.JoinUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * ensure child add enough distribute. update children properties if we do regular.
 * NOTICE: all visitor should call visit(plan, context) at proper place
 * to process must shuffle except project and filter
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
        // process must shuffle
        for (int i = 0; i < children.size(); i++) {
            DistributionSpec distributionSpec = childrenProperties.get(i).getDistributionSpec();
            if (distributionSpec instanceof DistributionSpecMustShuffle) {
                updateChildEnforceAndCost(i, PhysicalProperties.EXECUTION_ANY);
            }
        }
        return true;
    }

    @Override
    public Boolean visitPhysicalHashAggregate(PhysicalHashAggregate<? extends Plan> agg, Void context) {
        if (!agg.getAggregateParam().canBeBanned) {
            return true;
        }
        // forbid one phase agg on distribute and three or four stage distinct agg inter by distribute
        if ((agg.getAggMode() == AggMode.INPUT_TO_RESULT || agg.getAggMode() == AggMode.BUFFER_TO_BUFFER)
                && children.get(0).getPlan() instanceof PhysicalDistribute) {
            // this means one stage gather agg, usually bad pattern
            return false;
        }
        // forbid TWO_PHASE_AGGREGATE_WITH_DISTINCT after shuffle
        // TODO: this is forbid good plan after cte reuse by mistake
        if (agg.getAggMode() == AggMode.INPUT_TO_BUFFER
                && requiredProperties.get(0).getDistributionSpec() instanceof DistributionSpecHash
                && children.get(0).getPlan() instanceof PhysicalDistribute) {
            return false;
        }
        // forbid multi distinct opt that bad than multi-stage version when multi-stage can be executed in one fragment
        if (agg.getAggMode() == AggMode.INPUT_TO_BUFFER || agg.getAggMode() == AggMode.INPUT_TO_RESULT) {
            List<MultiDistinction> multiDistinctions = agg.getOutputExpressions().stream()
                    .filter(Alias.class::isInstance)
                    .map(a -> ((Alias) a).child())
                    .filter(AggregateExpression.class::isInstance)
                    .map(a -> ((AggregateExpression) a).getFunction())
                    .filter(MultiDistinction.class::isInstance)
                    .map(MultiDistinction.class::cast)
                    .collect(Collectors.toList());
            if (multiDistinctions.size() == 1) {
                Expression distinctChild = multiDistinctions.get(0).child(0);
                DistributionSpec childDistribution = childrenProperties.get(0).getDistributionSpec();
                if (distinctChild instanceof SlotReference && childDistribution instanceof DistributionSpecHash) {
                    SlotReference slotReference = (SlotReference) distinctChild;
                    DistributionSpecHash distributionSpecHash = (DistributionSpecHash) childDistribution;
                    List<ExprId> groupByColumns = agg.getGroupByExpressions().stream()
                            .map(SlotReference.class::cast)
                            .map(SlotReference::getExprId)
                            .collect(Collectors.toList());
                    DistributionSpecHash groupByRequire = new DistributionSpecHash(
                            groupByColumns, ShuffleType.REQUIRE);
                    List<ExprId> distinctChildColumns = Lists.newArrayList(slotReference.getExprId());
                    distinctChildColumns.add(slotReference.getExprId());
                    DistributionSpecHash distinctChildRequire = new DistributionSpecHash(
                            distinctChildColumns, ShuffleType.REQUIRE);
                    if ((!groupByColumns.isEmpty() && distributionSpecHash.satisfy(groupByRequire))
                            || (groupByColumns.isEmpty() && distributionSpecHash.satisfy(distinctChildRequire))) {
                        return false;
                    }
                }
            }
        }
        // process must shuffle
        visit(agg, context);
        // process agg
        return true;
    }

    @Override
    public Boolean visitPhysicalPartitionTopN(PhysicalPartitionTopN<? extends Plan> partitionTopN, Void context) {
        if (partitionTopN.getPhase().isOnePhaseGlobal() && children.get(0).getPlan() instanceof PhysicalDistribute) {
            // one phase partition topn, if the child is an enforced distribution, discard this
            // and use two phase candidate.
            return false;
        } else if (partitionTopN.getPhase().isTwoPhaseGlobal()
                && !(children.get(0).getPlan() instanceof PhysicalDistribute)) {
            // two phase partition topn, if global's child is not distribution, which means
            // the local distribution has met final requirement, discard this candidate.
            return false;
        } else {
            visit(partitionTopN, context);
            return true;
        }
    }

    @Override
    public Boolean visitPhysicalFilter(PhysicalFilter<? extends Plan> filter, Void context) {
        // do not process must shuffle
        return true;
    }

    @Override
    public Boolean visitPhysicalHashJoin(PhysicalHashJoin<? extends Plan, ? extends Plan> hashJoin,
            Void context) {
        Preconditions.checkArgument(children.size() == 2, "children.size() != 2");
        Preconditions.checkArgument(childrenProperties.size() == 2);
        Preconditions.checkArgument(requiredProperties.size() == 2);
        // process must shuffle
        visit(hashJoin, context);
        // process hash join
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
        // process must shuffle
        visit(nestedLoopJoin, context);
        // process nlj
        DistributionSpec rightDistributionSpec = childrenProperties.get(1).getDistributionSpec();
        if (rightDistributionSpec instanceof DistributionSpecStorageGather) {
            updateChildEnforceAndCost(1, PhysicalProperties.GATHER);
        }
        return true;
    }

    @Override
    public Boolean visitPhysicalProject(PhysicalProject<? extends Plan> project, Void context) {
        // do not process must shuffle
        return true;
    }

    @Override
    public Boolean visitPhysicalSetOperation(PhysicalSetOperation setOperation, Void context) {
        // process must shuffle
        visit(setOperation, context);
        // union with only constant exprs list
        if (children.isEmpty()) {
            return true;
        }
        // process set operation
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

    @Override
    public Boolean visitAbstractPhysicalSort(AbstractPhysicalSort<? extends Plan> sort, Void context) {
        // process must shuffle
        visit(sort, context);
        if (sort.getSortPhase() == SortPhase.GATHER_SORT && sort.child() instanceof PhysicalDistribute) {
            // forbid gather sort need explicit shuffle
            return false;
        }
        return true;
    }

    /**
     * check both side real output hash key order are same or not.
     *
     * @param notShuffleSideOutput not shuffle side real output used hash spec
     * @param shuffleSideOutput  shuffle side real output used hash spec
     * @param notShuffleSideRequired not shuffle side required used hash spec
     * @param shuffleSideRequired shuffle side required hash spec
     * @return true if same
     */
    private boolean bothSideShuffleKeysAreSameOrder(
            DistributionSpecHash notShuffleSideOutput, DistributionSpecHash shuffleSideOutput,
            DistributionSpecHash notShuffleSideRequired, DistributionSpecHash shuffleSideRequired) {
        return shuffleSideOutput.getOrderedShuffledColumns().equals(
                calAnotherSideRequiredShuffleIds(notShuffleSideOutput, notShuffleSideRequired, shuffleSideRequired));
    }

    /**
     * calculate the shuffle side hash key right orders.
     * For example,
     * if not shuffle side real hash key is 1 2 3.
     * the requirement of hash key of not shuffle side is 3 2 1.
     * the requirement of hash key of shuffle side is 6 5 4.
     * then we should let the shuffle side real output hash key order as 4 5 6
     *
     * @param notShuffleSideOutput not shuffle side real output used hash spec
     * @param notShuffleSideRequired not shuffle side required used hash spec
     * @param shuffleSideRequired shuffle side required hash spec
     * @return shuffle side real output used hash key order
     */
    private List<ExprId> calAnotherSideRequiredShuffleIds(DistributionSpecHash notShuffleSideOutput,
            DistributionSpecHash notShuffleSideRequired, DistributionSpecHash shuffleSideRequired) {
        ImmutableList.Builder<ExprId> rightShuffleIds = ImmutableList.builder();
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
            Preconditions.checkState(index != -1, "index could not be -1");
            rightShuffleIds.add(shuffleSideRequired.getOrderedShuffledColumns().get(index));
        }
        return rightShuffleIds.build();
    }

    /**
     * generate shuffle side real output should follow PhysicalProperties. More info could see
     * calAnotherSideRequiredShuffleIds's comment.
     *
     * @param shuffleType real output shuffle type
     * @param notShuffleSideOutput not shuffle side real output used hash spec
     * @param shuffleSideOutput  shuffle side real output used hash spec
     * @param notShuffleSideRequired not shuffle side required used hash spec
     * @param shuffleSideRequired shuffle side required hash spec
     * @return shuffle side new required hash spec
     */
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
        child.getOwnerGroup().addEnforcer(enforcer);
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
