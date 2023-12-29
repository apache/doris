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
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Exists subquery expression.
 */
public class Exists extends SubqueryExpr {

    private final boolean isNot;

    public Exists(LogicalPlan subquery, boolean isNot) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"));
        this.isNot = isNot;
    }

    public Exists(LogicalPlan subquery, List<Slot> correlateSlots, boolean isNot) {
        this(Objects.requireNonNull(subquery, "subquery can not be null"),
                Objects.requireNonNull(correlateSlots, "subquery can not be null"),
                Optional.empty(), isNot);
    }

    public Exists(LogicalPlan subquery, List<Slot> correlateSlots,
                  Optional<Expression> typeCoercionExpr, boolean isNot) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"),
                Objects.requireNonNull(correlateSlots, "subquery can not be null"),
                typeCoercionExpr);
        this.isNot = isNot;
    }

    public boolean isNot() {
        return isNot;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public String toSql() {
        return "EXISTS (SUBQUERY) " + super.toSql();
    }

    @Override
    public String toString() {
        return "EXISTS (SUBQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitExistsSubquery(this, context);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 1);
        return new Exists(((Exists) children.get(0)).getQueryPlan(), isNot);
    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) {
            return false;
        }
        Exists other = (Exists) o;
        return this.isNot == other.isNot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.queryPlan, this.isNot);
    }

    @Override
    public Expression withTypeCoercion(DataType dataType) {
        return this;
    }

    @Override
    public Exists withSubquery(LogicalPlan subquery) {
        return new Exists(subquery, correlateSlots, typeCoercionExpr, isNot);
    }
}
