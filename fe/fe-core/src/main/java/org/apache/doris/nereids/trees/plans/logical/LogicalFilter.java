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
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Logical filter plan.
 */
public class LogicalFilter<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {
    private final List<Expression> conjuncts;

    private final boolean singleTableExpressionExtracted;

    public LogicalFilter(List<Expression> conjuncts, CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), Optional.empty(), child);
    }

    public LogicalFilter(List<Expression> conjuncts, boolean singleTableExpressionExtracted,
            CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), singleTableExpressionExtracted,
                Optional.empty(), child);
    }

    public LogicalFilter(List<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        this(conjuncts, groupExpression, false, logicalProperties, child);
    }

    public LogicalFilter(List<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            boolean singleTableExpressionExtracted,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_FILTER, groupExpression, logicalProperties, child);
        this.conjuncts = Objects.requireNonNull(conjuncts, "conjuncts can not be null");
        this.singleTableExpressionExtracted = singleTableExpressionExtracted;
    }

    @Override
    public List<Expression> getConjuncts() {
        return conjuncts;
    }

    @Override
    public List<Plan> extraPlans() {
        if (predicates != null) {
            return predicates.children().stream()
                .flatMap(m -> {
                    if (m instanceof SubqueryExpr) {
                        return Stream.of(
                                new LogicalSubQueryAlias<>(m.toSql(), ((SubqueryExpr) m).getQueryPlan()));
                    } else {
                        return new LogicalFilter<Plan>(m, child()).extraPlans().stream();
                    }
                }).collect(Collectors.toList());
        }
        return ImmutableList.of();
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalFilter",
                "conjuncts", conjuncts
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
        return conjuncts.equals(that.conjuncts)
                && singleTableExpressionExtracted == that.singleTableExpressionExtracted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(conjuncts, singleTableExpressionExtracted);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    @Override
    public List<? extends Expression> getExpressions() {
        return conjuncts;
    }

    @Override
    public LogicalUnary<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(conjuncts, singleTableExpressionExtracted, children.get(0));
    }

    @Override
    public Plan withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFilter<>(conjuncts, groupExpression, singleTableExpressionExtracted,
                Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withLogicalProperties(Optional<LogicalProperties> logicalProperties) {
        return new LogicalFilter<>(conjuncts, Optional.empty(),
                singleTableExpressionExtracted,
                logicalProperties, child());
    }

    public boolean isSingleTableExpressionExtracted() {
        return singleTableExpressionExtracted;
    }

}
