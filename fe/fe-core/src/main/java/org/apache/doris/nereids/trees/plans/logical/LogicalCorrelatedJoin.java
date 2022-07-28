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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A relational algebra node with join type converted from apply node to subquery.
 * @param <LEFT_CHILD_TYPE> input.
 * @param <RIGHT_CHILD_TYPE> subquery.
 */
public class LogicalCorrelatedJoin<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {

    /**
     * Coorelated Join Type.
     */
    public enum Type {
        INNER(JoinType.INNER_JOIN),
        LEFT(JoinType.LEFT_OUTER_JOIN),
        RIGHT(JoinType.RIGHT_OUTER_JOIN),
        FULL(JoinType.FULL_OUTER_JOIN);

        private final JoinType joinType;

        Type(JoinType joinType) {
            this.joinType = joinType;
        }

        public JoinType toLogicalJoinType() {
            return joinType;
        }

        /**
         * Convert the type of ordinary join to the type of correlatedJoin.
         * @param joinType ordinary type.
         * @return correlatedJoin type.
         */
        public static Type typeConvert(JoinType joinType) {
            switch (joinType) {
                case CROSS_JOIN:
                case INNER_JOIN:
                    return Type.INNER;
                case LEFT_OUTER_JOIN:
                    return Type.LEFT;
                case RIGHT_OUTER_JOIN:
                    return Type.RIGHT;
                case FULL_OUTER_JOIN:
                    return Type.FULL;
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + joinType);
            }
        }
    }

    private final List<Expression> correlation;
    private final Type type;
    private final Optional<Expression> filter;

    public LogicalCorrelatedJoin(LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery, List<Expression> correlation,
            Type type, Optional<Expression> filter) {
        this(Optional.empty(), Optional.empty(), input, subquery,
                correlation, type, filter);
    }

    public LogicalCorrelatedJoin(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, List<Expression> correlation,
            Type type, Optional<Expression> filter) {
        super(PlanType.LOGICAL_CORRELATED_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.correlation = ImmutableList.copyOf(correlation);
        this.type = Objects.requireNonNull(type, "type can not be null");
        this.filter = Objects.requireNonNull(filter, "filter can not be null");
    }

    public List<Expression> getCorrelation() {
        return correlation;
    }

    public Type getJoinType() {
        return type;
    }

    public Optional<Expression> getFilter() {
        return filter;
    }

    @Override
    public List<Slot> computeOutput(Plan left, Plan right) {
        return ImmutableList.<Slot>builder()
                .addAll(left.getOutput())
                .addAll(right.getOutput())
                .build();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("LogicalCorrelated ((correlation: ").append(type);
        correlation.stream().map(c -> sb.append(", ").append(c));
        sb.append("), (filter: ");
        filter.ifPresent(expression -> sb.append(", ").append(expression));
        return sb.append("))").toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalCorrelatedJoin that = (LogicalCorrelatedJoin) o;
        return Objects.equals(correlation, that.getCorrelation())
                && Objects.equals(type, that.getType())
                && Objects.equals(filter, that.getFilter());
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlation, type, filter);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalCorrelated((LogicalCorrelatedJoin<Plan, Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return new ImmutableList.Builder<Expression>()
                .addAll(correlation)
                .add(filter.get())
                .build();
    }

    @Override
    public LogicalBinary<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalCorrelatedJoin<>(children.get(0), children.get(1),
                correlation, type, filter);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCorrelatedJoin<>(groupExpression, Optional.of(logicalProperties),
                left(), right(), correlation, type, filter);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCorrelatedJoin<>(Optional.empty(), logicalProperties,
                left(), right(), correlation, type, filter);
    }
}
