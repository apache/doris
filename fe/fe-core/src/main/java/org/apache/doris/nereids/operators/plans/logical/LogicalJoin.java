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

package org.apache.doris.nereids.operators.plans.logical;

import org.apache.doris.nereids.operators.OperatorType;
import org.apache.doris.nereids.operators.plans.JoinType;
import org.apache.doris.nereids.rules.exploration.JoinReorderContext;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Plan;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Logical join plan operator.
 */
public class LogicalJoin extends LogicalBinaryOperator {

    private final JoinType joinType;
    private final Optional<Expression> condition;

    // Use for top-to-down join reorder
    private final JoinReorderContext joinReorderContext = new JoinReorderContext();

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     */
    public LogicalJoin(JoinType joinType) {
        this(joinType, Optional.empty());
    }

    /**
     * Constructor for LogicalJoinPlan.
     *
     * @param joinType logical type for join
     * @param condition on clause for join node
     */
    public LogicalJoin(JoinType joinType, Optional<Expression> condition) {
        super(OperatorType.LOGICAL_JOIN);
        this.joinType = Objects.requireNonNull(joinType, "joinType can not be null");
        this.condition = Objects.requireNonNull(condition, "condition can not be null");
    }

    public Optional<Expression> getCondition() {
        return condition;
    }

    public JoinType getJoinType() {
        return joinType;
    }

    @Override
    public List<Slot> computeOutput(Plan leftInput, Plan rightInput) {

        List<Slot> newLeftOutput = leftInput.getOutput().stream().map(o -> o.withNullable(true))
                .collect(Collectors.toList());

        List<Slot> newRightOutput = rightInput.getOutput().stream().map(o -> o.withNullable(true))
                .collect(Collectors.toList());

        switch (joinType) {
            case LEFT_SEMI_JOIN:
            case LEFT_ANTI_JOIN:
                return ImmutableList.copyOf(leftInput.getOutput());
            case RIGHT_SEMI_JOIN:
            case RIGHT_ANTI_JOIN:
                return ImmutableList.copyOf(rightInput.getOutput());
            case LEFT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(leftInput.getOutput())
                        .addAll(newRightOutput)
                        .build();
            case RIGHT_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(rightInput.getOutput())
                        .build();
            case FULL_OUTER_JOIN:
                return ImmutableList.<Slot>builder()
                        .addAll(newLeftOutput)
                        .addAll(newRightOutput)
                        .build();
            default:
                return ImmutableList.<Slot>builder()
                        .addAll(leftInput.getOutput())
                        .addAll(rightInput.getOutput())
                        .build();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogicalJoin (").append(joinType);
        condition.ifPresent(expression -> sb.append(", ").append(expression));
        return sb.append(")").toString();
    }

    @Override
    public List<Expression> getExpressions() {
        return condition.<List<Expression>>map(ImmutableList::of).orElseGet(ImmutableList::of);
    }

    public JoinReorderContext getJoinReorderContext() {
        return joinReorderContext;
    }
}
