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
import org.apache.doris.nereids.trees.expressions.functions.agg.NotNullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * A subquery that will return only one row and one column.
 */
public class ScalarSubquery extends SubqueryExpr implements LeafExpression {

    // indicate if the subquery's root plan node is LogicalAggregate
    private final boolean hasTopLevelScalarAgg;

    // indicate if the subquery has limit 1 clause but it's been eliminated in previous process step
    private final boolean limitOneIsEliminated;

    private final Supplier<Slot> queryPlanSlot = Suppliers.memoize(
            () -> getScalarQueryOutputAdjustNullable(queryPlan, correlateSlots));

    public ScalarSubquery(LogicalPlan subquery) {
        this(subquery, ImmutableList.of(), false);
    }

    public ScalarSubquery(LogicalPlan subquery, List<Slot> correlateSlots, boolean limitOneIsEliminated) {
        this(subquery, correlateSlots, Optional.empty(), limitOneIsEliminated);
    }

    public ScalarSubquery(LogicalPlan subquery, List<Slot> correlateSlots, Optional<Expression> typeCoercionExpr,
                          boolean limitOneIsEliminated) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"),
                Objects.requireNonNull(correlateSlots, "correlateSlots can not be null"),
                typeCoercionExpr);
        hasTopLevelScalarAgg = findTopLevelScalarAgg(subquery, ImmutableSet.copyOf(correlateSlots)) != null;
        this.limitOneIsEliminated = limitOneIsEliminated;
    }

    public boolean hasTopLevelScalarAgg() {
        return hasTopLevelScalarAgg;
    }

    public boolean limitOneIsEliminated() {
        return limitOneIsEliminated;
    }

    /**
     * get Top Level ScalarAgg Functions
     */
    public static List<NamedExpression> getTopLevelScalarAggFunctions(Plan queryPlan,
            List<Slot> correlateSlots) {
        LogicalAggregate<?> aggregate = findTopLevelScalarAgg(queryPlan, ImmutableSet.copyOf(correlateSlots));
        if (aggregate != null) {
            return aggregate.getOutputExpressions();
        } else {
            return new ArrayList<>();
        }
    }

    @Override
    public Expression getSubqueryOutput() {
        return typeCoercionExpr.orElseGet(this::getOutputSlotAdjustNullable);
    }

    @Override
    public DataType getDataType() throws UnboundException {
        Preconditions.checkArgument(queryPlan.getOutput().size() == 1);
        return getSubqueryOutput().getDataType();
    }

    @Override
    public String computeToSql() {
        return " (SCALARSUBQUERY) " + super.computeToSql();
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
        Optional<Expression> newTypeCoercionExpr = typeCoercionExpr;
        if (!getDataType().equals(dataType)) {
            newTypeCoercionExpr = Optional.of(new Cast(getSubqueryOutput(), dataType));
        }
        return new ScalarSubquery(queryPlan, correlateSlots, newTypeCoercionExpr, limitOneIsEliminated);
    }

    @Override
    public ScalarSubquery withSubquery(LogicalPlan subquery) {
        return new ScalarSubquery(subquery, correlateSlots, typeCoercionExpr, limitOneIsEliminated);
    }

    public Slot getOutputSlotAdjustNullable() {
        return queryPlanSlot.get();
    }

    /**
     *  get query plan output slot, adjust it to
     *  1. true(no adjust), when it has top agg, and the agg function is NotNullableAggregateFunction
     *     for example: `t1.a = (select count(t2.x) from t2)`,  count(t2.x) is always not null, even if t2 contain 0 row
     *  2. false, otherwise.
     *     for example: `t1.a = (select t2.y from t2 limit 1)`, even if t2.y is not nullable, but if t2 contain 0 row,
     *     the sub query t2 output is still null.
     */
    public static Slot getScalarQueryOutputAdjustNullable(Plan queryPlan, List<Slot> correlateSlots) {
        Slot output = queryPlan.getOutput().get(0);
        boolean nullable = true;
        List<NamedExpression> aggOpts = getTopLevelScalarAggFunctions(queryPlan, correlateSlots);
        if (aggOpts.size() == 1) {
            NamedExpression agg = aggOpts.get(0);
            if (agg.getExprId().equals(output.getExprId())
                    && agg instanceof Alias
                    && ((Alias) agg).child() instanceof NotNullableAggregateFunction) {
                nullable = false;
            }
        }

        return output.withNullable(nullable);
    }

    /**
     * for subquery, we define top level scalar agg as if it meets the both 2 conditions:
     * 1. The agg or its child contains correlated slots(un-correlated sub query's correlated slot is empty)
     * 2. only project, sort and subquery alias node can be agg's parent
     */
    public static LogicalAggregate<?> findTopLevelScalarAgg(Plan plan, ImmutableSet<Slot> slots) {
        if (plan instanceof LogicalAggregate) {
            LogicalAggregate<?> agg = (LogicalAggregate<?>) plan;
            if (agg.getGroupByExpressions().isEmpty()
                    && (plan.containsSlots(slots) || slots.isEmpty())) {
                return agg;
            } else {
                return null;
            }
        } else if (plan instanceof LogicalProject || plan instanceof LogicalSubQueryAlias
                || plan instanceof LogicalSort) {
            for (Plan child : plan.children()) {
                LogicalAggregate<?> result = findTopLevelScalarAgg(child, slots);
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
