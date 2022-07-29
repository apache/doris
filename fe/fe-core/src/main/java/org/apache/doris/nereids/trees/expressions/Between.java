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
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.base.Preconditions;

import java.util.List;
import java.util.Objects;

/**
 * Between predicate expression.
 */
public class Between extends Expression implements TernaryExpression {

    private Expression compareExpr;
    private Expression lowerBound;
    private Expression upperBound;
    /**
     * Constructor of ComparisonPredicate.
     *
     * @param compareExpr    compare of expression
     * @param lowerBound     left child of between predicate
     * @param upperBound     right child of between predicate
     */

    public Between(Expression compareExpr, Expression lowerBound,
                   Expression upperBound) {
        super(compareExpr, lowerBound, upperBound);
        this.compareExpr = compareExpr;
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public boolean nullable() throws UnboundException {
        return lowerBound.nullable() || upperBound.nullable();
    }

    @Override
    public String toSql() {
        return compareExpr.toSql() + " BETWEEN " + lowerBound.toSql() + " AND " + upperBound.toSql();
    }

    @Override
    public String toString() {
        return compareExpr + " BETWEEN " + lowerBound + " AND " + upperBound;
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitBetween(this, context);
    }

    public Expression getCompareExpr() {
        return compareExpr;
    }

    public Expression getLowerBound() {
        return lowerBound;
    }

    public Expression getUpperBound() {
        return upperBound;
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new Between(children.get(0), children.get(1), children.get(2));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Between that = (Between) o;
        return Objects.equals(compareExpr, that.compareExpr)
                && Objects.equals(lowerBound, that.lowerBound)
                && Objects.equals(upperBound, that.upperBound)
                && Objects.equals(children, that.children);
    }

    @Override
    public int hashCode() {
        return Objects.hash(compareExpr, lowerBound, upperBound);
    }
}
