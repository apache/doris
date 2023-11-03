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
import org.apache.doris.nereids.trees.expressions.shape.LeafExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Subquery Expression.
 */
public abstract class SubqueryExpr extends Expression implements LeafExpression {

    protected final LogicalPlan queryPlan;
    protected final List<Slot> correlateSlots;
    protected final Optional<Expression> typeCoercionExpr;

    protected SubqueryExpr(LogicalPlan subquery) {
        super(ImmutableList.of());
        this.queryPlan = Objects.requireNonNull(subquery, "subquery can not be null");
        this.correlateSlots = ImmutableList.of();
        this.typeCoercionExpr = Optional.empty();
    }

    protected SubqueryExpr(LogicalPlan subquery, List<Slot> correlateSlots, Optional<Expression> typeCoercionExpr) {
        super(ImmutableList.of());
        this.queryPlan = Objects.requireNonNull(subquery, "subquery can not be null");
        this.correlateSlots = ImmutableList.copyOf(correlateSlots);
        this.typeCoercionExpr = typeCoercionExpr;
    }

    public List<Slot> getCorrelateSlots() {
        return correlateSlots;
    }

    public Optional<Expression> getTypeCoercionExpr() {
        return typeCoercionExpr;
    }

    public Expression getSubqueryOutput() {
        return typeCoercionExpr.orElseGet(() -> queryPlan.getOutput().get(0));
    }

    public Expression getSubqueryOutput(LogicalPlan queryPlan) {
        return typeCoercionExpr.orElseGet(() -> queryPlan.getOutput().get(0));
    }

    @Override
    public DataType getDataType() throws UnboundException {
        throw new UnboundException("getDataType");
    }

    @Override
    public boolean nullable() throws UnboundException {
        return true;
    }

    @Override
    public String toSql() {
        return "(" + queryPlan + ")";
    }

    @Override
    protected String getExpressionName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of("subquery");
        }
        return this.exprName.get();
    }

    @Override
    public String toString() {
        return Utils.toSqlString("SubqueryExpr",
                "QueryPlan", queryPlan,
                "CorrelatedSlots", correlateSlots,
                "typeCoercionExpr", typeCoercionExpr.isPresent() ? typeCoercionExpr.get() : "null");
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitSubqueryExpr(this, context);
    }

    public LogicalPlan getQueryPlan() {
        return queryPlan;
    }

    @Override
    public boolean hasUnbound() {
        return super.hasUnbound() || !queryPlan.bound();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubqueryExpr other = (SubqueryExpr) o;
        return Objects.equals(correlateSlots, other.correlateSlots)
                && queryPlan.deepEquals(other.queryPlan)
                && Objects.equals(typeCoercionExpr, other.typeCoercionExpr);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queryPlan, correlateSlots, typeCoercionExpr);
    }

    public List<Slot> getOutput() {
        return queryPlan.getOutput();
    }

    public abstract Expression withTypeCoercion(DataType dataType);
}
