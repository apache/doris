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
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Logical filter plan.
 */
public class LogicalFilter<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {
    private final Expression predicates;

    private final boolean singleTableExpressionExtracted;

    public LogicalFilter(Expression predicates, CHILD_TYPE child) {
        this(predicates, Optional.empty(), Optional.empty(), child);
    }

    public LogicalFilter(Expression predicates,
            boolean singleTableExpressionExtracted,
            CHILD_TYPE child) {
        this(predicates, Optional.empty(), singleTableExpressionExtracted,
                Optional.empty(), child);
    }

    public LogicalFilter(Expression predicates, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        this(predicates, groupExpression, false, logicalProperties, child);
    }

    public LogicalFilter(Expression predicates, Optional<GroupExpression> groupExpression,
            boolean singleTableExpressionExtracted,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_FILTER, groupExpression, logicalProperties, child);
        this.predicates = Objects.requireNonNull(predicates, "predicates can not be null");
        this.singleTableExpressionExtracted = singleTableExpressionExtracted;
    }

    public Expression getPredicates() {
        return predicates;
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalFilter",
                "predicates", predicates
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalFilter that = (LogicalFilter) o;
        return predicates.equals(that.predicates)
                && singleTableExpressionExtracted == that.singleTableExpressionExtracted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicates, singleTableExpressionExtracted);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return ImmutableList.of(predicates);
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(predicates, singleTableExpressionExtracted, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFilter<>(predicates, groupExpression, singleTableExpressionExtracted,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalFilter<>(predicates, Optional.empty(),
                singleTableExpressionExtracted,
                logicalProperties, child());
    }

    public boolean isSingleTableExpressionExtracted() {
        return singleTableExpressionExtracted;
    }

}
