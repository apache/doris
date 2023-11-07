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

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinHint;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.statistics.Statistics;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Use nested loop algorithm to do join.
 */
public class PhysicalNestedLoopJoin<
        LEFT_CHILD_TYPE extends Plan,
        RIGHT_CHILD_TYPE extends Plan>
        extends AbstractPhysicalJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    /*
    bitmap_contains(...) or Not(bitmap_contains(...)) can be used as bitmap runtime filter condition
    bitmapRF is different from other RF in that scan node must wait for it.
    if a condition is used in rf, it can be removed from join conditions. we collect these conditions here.
     */
    private final Set<Expression> bitMapRuntimeFilterConditions = Sets.newHashSet();

    public PhysicalNestedLoopJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference,
                Optional.empty(), logicalProperties, leftChild, rightChild);
    }

    /**
     * Constructor of PhysicalNestedLoopJoin.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param hashJoinConjuncts conjunct list could use for build hash table in hash join
     */
    public PhysicalNestedLoopJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_NESTED_LOOP_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                // nested loop join ignores join hints.
                JoinHint.NONE, markJoinSlotReference,
                groupExpression, logicalProperties, leftChild, rightChild);
    }

    /**
     * Constructor of PhysicalNestedLoopJoin.
     *
     * @param joinType Which join type, left semi join, inner join...
     * @param hashJoinConjuncts conjunct list could use for build hash table in hash join
     */
    public PhysicalNestedLoopJoin(
            JoinType joinType,
            List<Expression> hashJoinConjuncts,
            List<Expression> otherJoinConjuncts,
            Optional<MarkJoinSlotReference> markJoinSlotReference,
            Optional<GroupExpression> groupExpression,
            LogicalProperties logicalProperties,
            PhysicalProperties physicalProperties,
            Statistics statistics,
            LEFT_CHILD_TYPE leftChild,
            RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.PHYSICAL_NESTED_LOOP_JOIN, joinType, hashJoinConjuncts, otherJoinConjuncts,
                // nested loop join ignores join hints.
                JoinHint.NONE, markJoinSlotReference,
                groupExpression, logicalProperties, physicalProperties, statistics, leftChild, rightChild);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitPhysicalNestedLoopJoin(this, context);
    }

    // @Override
    // public String toString() {
    //     // TODO: Maybe we could pull up this to the abstract class in the future.
    //     return Utils.toSqlString("PhysicalNestedLoopJoin[" + id.asInt() + "]" + getGroupIdWithPrefix(),
    //             "type", joinType,
    //             "otherJoinCondition", otherJoinConjuncts,
    //             "isMarkJoin", markJoinSlotReference.isPresent(),
    //             "markJoinSlotReference", markJoinSlotReference.isPresent() ? markJoinSlotReference.get() : "empty",
    //             "stats", statistics
    //     );
    // }

    @Override
    public PhysicalNestedLoopJoin<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        PhysicalNestedLoopJoin newJoin = new PhysicalNestedLoopJoin<>(joinType,
                hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference, Optional.empty(),
                getLogicalProperties(), physicalProperties, statistics, children.get(0), children.get(1));
        if (groupExpression.isPresent()) {
            newJoin.setMutableState(MutableState.KEY_GROUP, groupExpression.get().getOwnerGroup().getGroupId().asInt());
        }
        return newJoin;
    }

    @Override
    public PhysicalNestedLoopJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withGroupExpression(
            Optional<GroupExpression> groupExpression) {
        return new PhysicalNestedLoopJoin<>(joinType,
                hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference,
                groupExpression, getLogicalProperties(), left(), right());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new PhysicalNestedLoopJoin<>(joinType,
                hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference, groupExpression,
                logicalProperties.get(), children.get(0), children.get(1));
    }

    @Override
    public PhysicalNestedLoopJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> withPhysicalPropertiesAndStats(
            PhysicalProperties physicalProperties, Statistics statistics) {
        return new PhysicalNestedLoopJoin<>(joinType,
                hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference, groupExpression,
                getLogicalProperties(), physicalProperties, statistics, left(), right());
    }

    public void addBitmapRuntimeFilterCondition(Expression expr) {
        bitMapRuntimeFilterConditions.add(expr);
    }

    public boolean isBitmapRuntimeFilterCondition(Expression expr) {
        return bitMapRuntimeFilterConditions.contains(expr);
    }

    public boolean isBitMapRuntimeFilterConditionsEmpty() {
        return bitMapRuntimeFilterConditions.isEmpty();
    }

    public Set<Slot> getConditionSlot() {
        return Stream.concat(hashJoinConjuncts.stream(), otherJoinConjuncts.stream())
                .flatMap(expr -> expr.getInputSlots().stream()).collect(ImmutableSet.toImmutableSet());
    }

    @Override
    public String shapeInfo() {
        StringBuilder builder = new StringBuilder("NestedLoopJoin");
        builder.append("[").append(joinType).append("]");
        otherJoinConjuncts.forEach(expr -> builder.append(expr.shapeInfo()));
        return builder.toString();
    }

    @Override
    public PhysicalNestedLoopJoin<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> resetLogicalProperties() {
        return new PhysicalNestedLoopJoin<>(joinType,
                hashJoinConjuncts, otherJoinConjuncts, markJoinSlotReference, groupExpression,
                null, physicalProperties, statistics, left(), right());
    }
}
