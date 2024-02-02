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
import org.apache.doris.nereids.properties.FdItem;
import org.apache.doris.nereids.properties.FunctionalDependencies;
import org.apache.doris.nereids.properties.FunctionalDependencies.Builder;
import org.apache.doris.nereids.properties.LogicalProperties;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.Filter;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Logical filter plan.
 */
public class LogicalFilter<CHILD_TYPE extends Plan> extends LogicalUnary<CHILD_TYPE> implements Filter {

    private final Set<Expression> conjuncts;

    public LogicalFilter(Set<Expression> conjuncts, CHILD_TYPE child) {
        this(conjuncts, Optional.empty(), Optional.empty(), child);
    }

    private LogicalFilter(Set<Expression> conjuncts, Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, CHILD_TYPE child) {
        super(PlanType.LOGICAL_FILTER, groupExpression, logicalProperties, child);
        this.conjuncts = ImmutableSet.copyOf(Objects.requireNonNull(conjuncts, "conjuncts can not be null"));
    }

    @Override
    public Set<Expression> getConjuncts() {
        return conjuncts;
    }

    public List<Expression> getExpressions() {
        return ImmutableList.copyOf(conjuncts);
    }

    @Override
    public List<? extends Plan> extraPlans() {
        return conjuncts.stream().map(Expression::children).flatMap(Collection::stream).flatMap(m -> {
            if (m instanceof SubqueryExpr) {
                return Stream.of(new LogicalSubQueryAlias<>(m.toSql(), ((SubqueryExpr) m).getQueryPlan()));
            } else {
                return new LogicalFilter<Plan>(ImmutableSet.of(m), child()).extraPlans().stream();
            }
        }).collect(Collectors.toList());
    }

    @Override
    public List<Slot> computeOutput() {
        return child().getOutput();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("LogicalFilter[" + id.asInt() + "]",
                "predicates", getPredicate()
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
        return conjuncts.equals(that.conjuncts);
    }

    @Override
    public int hashCode() {
        return Objects.hash(conjuncts);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLogicalFilter(this, context);
    }

    public LogicalFilter<Plan> withConjuncts(Set<Expression> conjuncts) {
        return new LogicalFilter<>(conjuncts, Optional.empty(), Optional.of(getLogicalProperties()), child());
    }

    @Override
    public LogicalFilter<Plan> withChildren(List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(conjuncts, children.get(0));
    }

    @Override
    public LogicalFilter<Plan> withGroupExpression(Optional<GroupExpression> groupExpression) {
        return new LogicalFilter<>(conjuncts, groupExpression, Optional.of(getLogicalProperties()), child());
    }

    @Override
    public Plan withGroupExprLogicalPropChildren(Optional<GroupExpression> groupExpression,
            Optional<LogicalProperties> logicalProperties, List<Plan> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new LogicalFilter<>(conjuncts, groupExpression, logicalProperties, children.get(0));
    }

    public LogicalFilter<Plan> withConjunctsAndChild(Set<Expression> conjuncts, Plan child) {
        return new LogicalFilter<>(conjuncts, child);
    }

    @Override
    public ImmutableSet<FdItem> computeFdItems(Supplier<List<Slot>> outputSupplier) {
        ImmutableSet.Builder<FdItem> builder = ImmutableSet.builder();

        ImmutableSet<FdItem> childItems = child().getLogicalProperties().getFdItems();
        builder.addAll(childItems);

        return builder.build();
    }
}
