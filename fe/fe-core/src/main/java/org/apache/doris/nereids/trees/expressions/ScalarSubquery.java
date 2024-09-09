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

package org.apache.doris.nereids.trees.expressions;

import org.apache.doris.nereids.exceptions.UnboundException;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A subquery that will return only one row and one column.
 */
public class ScalarSubquery extends SubqueryExpr {

    private final boolean hasTopLevelScalarAgg;

    public ScalarSubquery(LogicalPlan subquery) {
        this(subquery, ImmutableList.of());
    }

    public ScalarSubquery(LogicalPlan subquery, List<Slot> correlateSlots) {
        this(subquery, correlateSlots, Optional.empty());
    }

    public ScalarSubquery(LogicalPlan subquery, List<Slot> correlateSlots, Optional<Expression> typeCoercionExpr) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"),
                Objects.requireNonNull(correlateSlots, "correlateSlots can not be null"),
                typeCoercionExpr);
        hasTopLevelScalarAgg = findTopLevelScalarAgg(subquery, ImmutableSet.copyOf(correlateSlots)) != null;
    }

    public boolean hasTopLevelScalarAgg() {
        return hasTopLevelScalarAgg;
    }

    /**
    * getTopLevelScalarAggFunction
    */
    public Optional<NamedExpression> getTopLevelScalarAggFunction() {
        Plan plan = findTopLevelScalarAgg(queryPlan, ImmutableSet.copyOf(correlateSlots));
        if (plan != null) {
            LogicalAggregate aggregate = (LogicalAggregate) plan;
            Preconditions.checkState(aggregate.getAggregateFunctions().size() == 1,
                    "in scalar subquery, should only return 1 column 1 row, "
                            + "but we found multiple columns ", aggregate.getOutputExpressions());
            return Optional.of((NamedExpression) aggregate.getOutputExpressions().get(0));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public DataType getDataType() throws UnboundException {
        Preconditions.checkArgument(queryPlan.getOutput().size() == 1);
        return typeCoercionExpr.orElse(queryPlan.getOutput().get(0)).getDataType();
    }

    @Override
    public String toSql() {
        return " (SCALARSUBQUERY) " + super.toSql();
    }

    @Override
    public String toString() {
        return " (SCALARSUBQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitScalarSubquery(this, context);
    }

    @Override
    public Expression withTypeCoercion(DataType dataType) {
        return new ScalarSubquery(queryPlan, correlateSlots,
                dataType == queryPlan.getOutput().get(0).getDataType()
                    ? Optional.of(queryPlan.getOutput().get(0))
                    : Optional.of(new Cast(queryPlan.getOutput().get(0), dataType)));
    }

    @Override
    public ScalarSubquery withSubquery(LogicalPlan subquery) {
        return new ScalarSubquery(subquery, correlateSlots, typeCoercionExpr);
    }

    /**
     * for correlated subquery, we define top level scalar agg as if it meets the both 2 conditions:
     * 1. The agg or its child contains correlated slots
     * 2. only project, sort and subquery alias node can be agg's parent
     */
    public static Plan findTopLevelScalarAgg(Plan plan, ImmutableSet<Slot> slots) {
        if (plan instanceof LogicalAggregate) {
            if (((LogicalAggregate<?>) plan).getGroupByExpressions().isEmpty() && plan.containsSlots(slots)) {
                return plan;
            } else {
                return null;
            }
        } else if (plan instanceof LogicalProject || plan instanceof LogicalSubQueryAlias
                || plan instanceof LogicalSort) {
            for (Plan child : plan.children()) {
                Plan result = findTopLevelScalarAgg(child, slots);
                if (result != null) {
                    return result;
                }
            }
            return null;
        } else {
            return null;
        }
    }
}
