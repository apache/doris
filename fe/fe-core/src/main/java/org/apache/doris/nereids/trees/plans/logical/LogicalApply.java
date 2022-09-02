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
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Apply Node for subquery.
 * Use this node to display the subquery in the relational algebra tree.
 * refer to "Orthogonal Optimization of Subqueries and Aggregation"
 *
 * @param <LEFT_CHILD_TYPE> input
 * @param <RIGHT_CHILD_TYPE> subquery
 */
public class LogicalApply<LEFT_CHILD_TYPE extends Plan, RIGHT_CHILD_TYPE extends Plan>
        extends LogicalBinary<LEFT_CHILD_TYPE, RIGHT_CHILD_TYPE> {
    // correlation : subqueries and outer associated columns
    private final List<Expression> correlation;

    public LogicalApply(LEFT_CHILD_TYPE input, RIGHT_CHILD_TYPE subquery, List<Expression> correlation) {
        this(Optional.empty(), Optional.empty(), input, subquery, correlation);
    }

    public LogicalApply(Optional<GroupExpression> groupExpression, Optional<LogicalProperties> logicalProperties,
            LEFT_CHILD_TYPE leftChild, RIGHT_CHILD_TYPE rightChild, List<Expression> correlation) {
        super(PlanType.LOGICAL_APPLY, groupExpression, logicalProperties, leftChild, rightChild);
        this.correlation = ImmutableList.copyOf(correlation);
    }

    public List<Expression> getCorrelation() {
        return correlation;
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
        return Utils.toSqlString("LogicalApply");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalApply that = (LogicalApply) o;
        return Objects.equals(correlation, that.getCorrelation());
    }

    @Override
    public int hashCode() {
        return Objects.hash(correlation);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalApply((LogicalApply<Plan, Plan>) this, context);
    }

    @Override
    public List<Expression> getExpressions() {
        return correlation;
    }

    @Override
    public LogicalBinary<Plan, Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 2);
        return new LogicalApply<>(children.get(0), children.get(1), correlation);
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalApply<>(groupExpression, Optional.of(logicalProperties), left(), right(), correlation);
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalApply<>(Optional.empty(), logicalProperties, left(), right(), correlation);
    }
}
