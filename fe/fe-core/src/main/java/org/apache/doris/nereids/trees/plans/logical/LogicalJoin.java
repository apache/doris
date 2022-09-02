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

package org.apache.doris.nereids.trees.plans.logical;

import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.rules.exploration.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Join;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Logical join plan.
 */
public class LogicalJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> implements Join {

    private final JoinType joinType;
    private final Optional<Expression> otherJoinCondition;
    private final List<Expression> hashJoinConjuncts;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     */
    public LogicalJoin(JoinType joinType, LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, ImmutableList.of(),
                Optional.empty(), Optional.empty(),
                Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, Optional<Expression> condition,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        this(joinType, hashJoinConjuncts,
                condition, Optional.empty(), Optional.empty(), leftChild, rightChild);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, Optional<Expression> condition,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, JoinReorderContext joinReorderContext) {
        this(joinType, hashJoinConjuncts, condition,
                Optional.empty(), Optional.empty(), leftChild, rightChild);
        this.joinReorderContext.copyFrom(joinReorderContext);
    }

    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, Optional<Expression> condition,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, JoinReorderContext joinReorderContext) {
        this(joinType, hashJoinConjuncts, condition, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinReorderContext.copyFrom(joinReorderContext);
    }

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     * @param condition on clause for join node
     */
    public LogicalJoin(JoinType joinType, List<Expression> hashJoinConjuncts, Optional<Expression> condition,
            Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild) {
        super(PlanType.LOGICAL_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.hashJoinConjuncts = hashJoinConjuncts;
        this.otherJoinCondition = Objects.requireNonNull(condition, "condition can not be null");
    }

    /**
     * get combination of hashJoinConjuncts and condition
     *
     * @return combine hashJoinConjuncts and condition by AND
     */
    public Optional<Expression> getOtherJoinCondition() {
        return otherJoinCondition;
    }

    @Override
    public List<Expression> getHashJoinConjuncts() {
        return hashJoinConjuncts;
    }

    /**
     * hashJoinConjuncts and otherJoinCondition
     *
     * @return the combination of hashJoinConjuncts and otherJoinCondition
     */
    public Optional<Expression> getOnClauseCondition() {
        if (hashJoinConjuncts.isEmpty()) {
            return otherJoinCondition;
        }

        Expression onClauseCondition = ExpressionUtils.and(hashJoinConjuncts);
        if (otherJoinCondition.isPresent()) {
            onClauseCondition = ExpressionUtils.and(onClauseCondition, otherJoinCondition.get());
        }
        return Optional.of(onClauseCondition);
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public List<Slot> computeOutput() {

        List<Slot> newLeftOutput = left().getOutput().stream().map(o -> o.withNullable(true))
                .collect(Collectors.toList());

        List<Slot> newRightOutput = right().getOutput().stream().map(o -> o.withNullable(true))
                .collect(Collectors.toList());

        switch (joinType) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
                return ImmutableList.copyOf(left().getOutput());
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return ImmutableList.copyOf(right().getOutput());
            case LEFT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(left().getOutput())
                        .addAll(newRightOutput)
                        .build();
            case RIGHT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(right().getOutput())
                        .build();
            case FULL_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(newRightOutput)
                        .build();
            default:
                return ImmutableList.<Slot>builder()
                        .addAll(left().getOutput())
                        .addAll(right().getOutput())
                        .build();
        }
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalJoin",
                "type", joinType,
                "hashJoinCondition", hashJoinConjuncts,
                "otherJoinCondition", otherJoinCondition
        );
    }

    // TODO:
    // 1. consider the order of conjucts in otherJoinCondition and hashJoinConditions
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalJoin that = (LogicalJoin) o;

        return joinType == that.joinType
                // TODO: why use containsAll?
                && that.getHashJoinConjuncts().containsAll(hashJoinConjuncts)
                && hashJoinConjuncts.containsAll(that.getHashJoinConjuncts())
                && Objects.equals(otherJoinCondition, that.otherJoinCondition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(joinType, otherJoinCondition);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalJoin((LogicalJoin<Plan, Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return otherJoinCondition.<List<Expression>>map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }

    @Override
    public LogicalBinary<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinCondition, children.get(0), children.get(1),
                joinReorderContext);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinCondition, groupExpression,
                Optional.of(logicalProperties), left(), right(), joinReorderContext);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalJoin<>(joinType, hashJoinConjuncts, otherJoinCondition,
                Optional.empty(), logicalProperties, left(), right(), joinReorderContext);
    }

    @Override
    public LEFT_CHILD_TYPE left() {
        return (LEFT_CHILD_TYPE) child(0);
    }

    @Override
    public RIGHT_CHILD_TYPE right() {
        return (RIGHT_CHILD_TYPE) child(1);
    }
}
