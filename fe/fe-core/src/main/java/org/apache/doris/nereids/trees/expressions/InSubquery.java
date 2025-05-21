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
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * In predicate expression.
 */
public class InSubquery extends SubqueryExpr implements UnaryExpression {

    private final boolean isNot;

    public InSubquery(Expression compareExpression, LogicalPlan listQuery, boolean isNot) {
        this(compareExpression, listQuery, ImmutableList.of(), Optional.empty(), isNot);
    }

    public InSubquery(Expression compareExpr, LogicalPlan listQuery, List<Slot> correlateSlots, boolean isNot) {
        this(compareExpr, listQuery, correlateSlots, Optional.empty(), isNot);
    }

    /**
     * InSubquery Constructor.
     */
    public InSubquery(Expression compareExpr,
            LogicalPlan listQuery,
            List<Slot> correlateSlots,
            Optional<Expression> typeCoercionExpr,
            boolean isNot) {
        super(listQuery, correlateSlots, typeCoercionExpr, compareExpr);
        this.isNot = isNot;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return super.nullable() || this.child().nullable();
    }

    @Override
    public String computeToSql() {
        return this.child().toSql() + " IN (" + super.computeToSql() + ")";
    }

    @Override
    public String toString() {
        return this.child() + " IN (INSUBQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInSubquery(this, context);
    }

    public Expression getCompareExpr() {
        return this.child();
    }

    public boolean isNot() {
        return isNot;
    }

    @Override
    public InSubquery withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new InSubquery(children.get(0), queryPlan, correlateSlots, typeCoercionExpr, isNot);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        InSubquery other = (InSubquery) o;
        return super.equals(other)
                && Objects.equals(this.child(), other.getCompareExpr())
                && this.isNot == other.isNot;
    }

    @Override
    public int computeHashCode() {
        return Objects.hash(super.computeHashCode(), this.child(), this.isNot);
    }

    @Override
    public Expression withTypeCoercion(DataType dataType) {
        return new InSubquery(child(), queryPlan, correlateSlots,
            dataType.equals(queryPlan.getOutput().get(0).getDataType())
                ? Optional.of(queryPlan.getOutput().get(0))
                : Optional.of(new Cast(queryPlan.getOutput().get(0), dataType)),
            isNot);
    }

    @Override
    public InSubquery withSubquery(LogicalPlan subquery) {
        return new InSubquery(child(), subquery, correlateSlots, typeCoercionExpr, isNot);
    }
}
