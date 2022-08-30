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

    private final List<Expression> correlation;
    private final Optional<Expression> filter;

    public LogicalCorrelatedJoin(LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery, List<Expression> correlation,
            Optional<Expression> filter) {
        this(Optional.empty(), Optional.empty(), input, subquery,
                correlation, filter);
    }

    public LogicalCorrelatedJoin(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, List<Expression> correlation,
            Optional<Expression> filter) {
        super(PlanType.LOGICAL_CORRELATED_JOIN, groupExpression, logicalProperties, leftChild, rightChild);
        this.correlation = ImmutableList.copyOf(correlation);
        this.filter = Objects.requireNonNull(filter, "filter can not be null");
    }

    public List<Expression> getCorrelation() {
        return correlation;
    }

    public Optional<Expression> getFilter() {
        return filter;
    }

    @Override
    public List<Slot> computeOutput() {
        return ImmutableList.<Slot>builder()
                .addAll(left().getOutput())
                .addAll(right().getOutput())
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
                && Objects.equals(filter, that.getFilter());
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlation, filter);
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
                correlation, filter);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalCorrelatedJoin<>(groupExpression, Optional.of(logicalProperties),
                left(), right(), correlation, filter);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalCorrelatedJoin<>(Optional.empty(), logicalProperties,
                left(), right(), correlation, filter);
    }
}
