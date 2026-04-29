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

package org.apache.doris.nereids.trees.plans.physical;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.hint.DistributeHint;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.DataTrait;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.AbstractPlan;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Physical group join plan.
 *
 * <p>GroupJoin is a fused operator that combines HashJoin and HashAggregate.
 * The build side aggregates rows by join key during hash table construction,
 * and the probe side directly outputs aggregated results without materializing
 * intermediate join results.</p>
 */
public class PhysicalGroupJoin<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends AbstractPhysicalJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    private final List<Expression> groupByExpressions;
    private final List<NamedExpression> outputExpressions;
    private final List<Expression> partitionExpressions;

    /**
     * Constructor of PhysicalGroupJoin.
     */
    public PhysicalGroupJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            DistributeHint hint,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<Expression> partitionExpressions,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, hint, groupByExpressions,
                outputExpressions, partitionExpressions,
                Optional.empty(), logicalProperties, null, null, leftChild, rightChild);
    }

    private PhysicalGroupJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            DistributeHint hint,
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<Expression> partitionExpressions,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_GROUP_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                ExpressionUtils.EMPTY_CONDITION, hint, Optional.empty(), groupExpression,
                logicalProperties, physicalProperties, statistics, leftChild, rightChild);
        this.groupByExpressions = groupByExpressions;
        this.outputExpressions = outputExpressions;
        this.partitionExpressions = partitionExpressions;
    }

    public List<Expression> getGroupByExpressions() {
        return groupByExpressions;
    }

    public List<NamedExpression> getOutputExpressions() {
        return outputExpressions;
    }

    public List<Expression> getPartitionExpressions() {
        return partitionExpressions;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalGroupJoin(this, context);
    }

    @Override
    public PhysicalGroupJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        PhysicalGroupJoin<Plan, Plan> newGroupJoin = AbstractPlan.copyWithSameId(this,
                () -> new PhysicalGroupJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                        hint, groupByExpressions, outputExpressions, partitionExpressions,
                        Optional.empty(), getLogicalProperties(), physicalProperties, statistics,
                        children.get(0), children.get(1)));
        if (groupExpression.isPresent()) {
            newGroupJoin.setMutableState(MutableState.KEY_GROUP,
                    groupExpression.get().getOwnerGroup().getGroupId().asInt());
        }
        return newGroupJoin;
    }

    @Override
    public PhysicalGroupJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalGroupJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                        hint, groupByExpressions, outputExpressions, partitionExpressions,
                        groupExpression, getLogicalProperties(), null, null, left(), right()));
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalGroupJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                        hint, groupByExpressions, outputExpressions, partitionExpressions,
                        groupExpression, logicalProperties.get(), null, null,
                        children.get(0), children.get(1)));
    }

    @Override
    public PhysicalGroupJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return AbstractPlan.copyWithSameId(this,
                () -> new PhysicalGroupJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                        hint, groupByExpressions, outputExpressions, partitionExpressions,
                        groupExpression, getLogicalProperties(), physicalProperties, statistics,
                        left(), right()));
    }

    @Override
    public PhysicalGroupJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalGroupJoin<>(joinType, hashJoinConjuncts, otherJoinConjuncts,
                hint, groupByExpressions, outputExpressions, partitionExpressions,
                groupExpression, null, physicalProperties, statistics, left(), right());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder();
        builder.append("groupJoin[").append(joinType).append("]");
        builder.append(hashJoinConjuncts.stream().map(conjunct -> conjunct.shapeInfo())
                .sorted().collect(Collectors.joining(" and ", " hashCondition=(", ")")));
        if (!otherJoinConjuncts.isEmpty()) {
            builder.append(otherJoinConjuncts.stream().map(cond -> cond.shapeInfo())
                    .sorted().collect(Collectors.joining(" and ", " otherCondition=(", ")")));
        }
        if (!runtimeFilters.isEmpty()) {
            builder.append(" build RFs:").append(runtimeFilters.stream()
                    .map(rf -> rf.shapeInfo()).collect(Collectors.joining(";")));
        }
        return builder.toString();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("PhysicalGroupJoin",
                "joinType", joinType,
                "hashJoinConjuncts", hashJoinConjuncts,
                "groupByExpressions", groupByExpressions,
                "outputExpressions", outputExpressions,
                "partitionExpressions", partitionExpressions);
    }

    @Override
    public List<Expression> getExpressions() {
        return new com.google.common.collect.ImmutableList.Builder<Expression>()
                .addAll(hashJoinConjuncts)
                .addAll(groupByExpressions)
                .addAll(outputExpressions)
                .build();
    }

    @Override
    public List<Slot> computeOutput() {
        return outputExpressions.stream()
                .map(NamedExpression::toSlot)
                .collect(Collectors.toList());
    }

    private @Nullable Pair<Set<Slot>, Set<Slot>> extractNullRejectHashKeys() {
        Set<Slot> leftKeys = new HashSet<>();
        Set<Slot> rightKeys = new HashSet<>();
        for (Expression expression : hashJoinConjuncts) {
            if (!(expression instanceof EqualTo
                    && ((EqualTo) expression).left() instanceof Slot
                    && ((EqualTo) expression).right() instanceof Slot)) {
                return null;
            }
            Slot leftKey = (Slot) ((EqualTo) expression).left();
            Slot rightKey = (Slot) ((EqualTo) expression).right();
            if (left().getOutputSet().contains(leftKey)) {
                leftKeys.add(leftKey);
                rightKeys.add(rightKey);
            } else {
                leftKeys.add(rightKey);
                rightKeys.add(leftKey);
            }
        }
        return Pair.of(leftKeys, rightKeys);
    }

    @Override
    public void computeUnique(DataTrait.Builder builder) {
        if (isMarkJoin()) {
            return;
        }
        if (joinType.isLeftSemiOrAntiJoin()) {
            builder.addUniqueSlot(left().getLogicalProperties().getTrait());
        } else if (joinType.isRightSemiOrAntiJoin()) {
            builder.addUniqueSlot(right().getLogicalProperties().getTrait());
        }
        if (hashJoinConjuncts.isEmpty()) {
            return;
        }
        Pair<Set<Slot>, Set<Slot>> keys = extractNullRejectHashKeys();
        if (keys == null) {
            return;
        }
        boolean isLeftUnique = left().getLogicalProperties().getTrait().isUnique(keys.first);
        boolean isRightUnique = right().getLogicalProperties().getTrait().isUnique(keys.second);
        if ((joinType.isLeftOuterJoin() || joinType.isAsofLeftOuterJoin()) && isRightUnique) {
            builder.addUniqueSlot(left().getLogicalProperties().getTrait());
        } else if ((joinType.isRightOuterJoin() || joinType.isAsofRightOuterJoin()) && isLeftUnique) {
            builder.addUniqueSlot(right().getLogicalProperties().getTrait());
        } else if ((joinType.isInnerJoin() || joinType.isAsofInnerJoin()) && isLeftUnique && isRightUnique) {
            builder.addDataTrait(left().getLogicalProperties().getTrait());
            builder.addDataTrait(right().getLogicalProperties().getTrait());
        }
    }

    @Override
    public void computeUniform(DataTrait.Builder builder) {
        if (isMarkJoin()) {
            return;
        }
        switch (joinType) {
            case INNER_JOIN:
            case CROSS_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                break;
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
            case NULL_AWARE_LEFT_ANTI_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                break;
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                break;
            case LEFT_OUTER_JOIN:
                builder.addUniformSlot(left().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(right().getLogicalProperties().getTrait());
                break;
            case RIGHT_OUTER_JOIN:
                builder.addUniformSlot(right().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(left().getLogicalProperties().getTrait());
                break;
            case FULL_OUTER_JOIN:
                builder.addUniformSlotForOuterJoinNullableSide(left().getLogicalProperties().getTrait());
                builder.addUniformSlotForOuterJoinNullableSide(right().getLogicalProperties().getTrait());
                break;
            default:
                break;
        }
    }

    @Override
    public void computeEqualSet(DataTrait.Builder builder) {
        if (!joinType.isLeftSemiOrAntiJoin()) {
            builder.addEqualSet(right().getLogicalProperties().getTrait());
        }
        if (!joinType.isRightSemiOrAntiJoin()) {
            builder.addEqualSet(left().getLogicalProperties().getTrait());
        }
        if (joinType.isInnerJoin() || joinType.isAsofInnerJoin()) {
            for (Expression expression : getHashJoinConjuncts()) {
                Optional<Pair<Slot, Slot>> equalSlot = ExpressionUtils.extractEqualSlot(expression);
                equalSlot.ifPresent(slotSlotPair -> builder.addEqualPair(slotSlotPair.first, slotSlotPair.second));
            }
        }
    }

    @Override
    public void computeFd(DataTrait.Builder builder) {
        if (!joinType.isLeftSemiOrAntiJoin()) {
            builder.addFuncDepsDG(right().getLogicalProperties().getTrait());
        }
        if (!joinType.isRightSemiOrAntiJoin()) {
            builder.addFuncDepsDG(left().getLogicalProperties().getTrait());
        }
    }
}
