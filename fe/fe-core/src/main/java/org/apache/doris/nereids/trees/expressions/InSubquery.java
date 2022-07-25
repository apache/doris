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

/**
 * In predicate expression.
 */
public class InSubquery extends SubqueryExpr implements BinaryExpression {
    private Expression compareExpr;

    public InSubquery(Expression compareExpression, LogicalPlan subquery) {
        super(Objects.requireNonNull(subquery, "subquery can not be null"));
        this.compareExpr = compareExpression;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return super.nullable() || this.compareExpr.nullable();
    }

    @Override
    public String toSql() {
        return this.compareExpr.toSql() + "IN (SUBQUERY) " + super.toSql();
    }

    @Override
    public String toString() {
        return this.compareExpr + "IN (SUBQUERY) " + super.toString();
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitInSubquery(this, context);
    }

    public Expression getCompareExpr() {
        return this.compareExpr;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 2);
        Preconditions.checkArgument(children.get(0) instanceof Expression);
        Preconditions.checkArgument(children.get(1) instanceof SubqueryExpr);
        return new InSubquery(children.get(0), ((SubqueryExpr) children.get(1)).getQueryPlan());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InSubquery inSubquery = (InSubquery) o;
        return Objects.equals(this.compareExpr, inSubquery.getCompareExpr())
                && Objects.equals(this.queryPlan, inSubquery.getQueryPlan());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.compareExpr, this.queryPlan);
    }
}
