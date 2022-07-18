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

import java.util.List;
import java.util.Objects;

/**
 * Compound predicate expression.
 * Such as &&,||,AND,OR.
 */
public class CompoundPredicate extends Expression implements BinaryExpression {

    /**
     * Desc: Constructor for CompoundPredicate.
     *
     * @param type  type of expression
     * @param left  left child of comparison predicate
     * @param right right child of comparison predicate
     */
    public CompoundPredicate(ExpressionType type, Expression left, Expression right) {
        super(type, left, right);
    }

    @Override
    public String toSql() {
        String nodeType = getType().toString();
        return "(" + left().toSql() + " " + nodeType + " " + right().toSql() + ")";
    }

    @Override
    public boolean nullable() throws UnboundException {
        return left().nullable() || right().nullable();
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCompoundPredicate(this, context);
    }

    @Override
    public Expression withChildren(List<Expression> children) {
        return new CompoundPredicate(getType(), children.get(0), children.get(1));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundPredicate other = (CompoundPredicate) o;
        return (type == other.getType()) && Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public String toString() {
        String nodeType = getType().toString();
        return nodeType + "(" + left() + ", " + right() + ")";
    }

    public ExpressionType flip() {
        if (getType() == ExpressionType.AND) {
            return ExpressionType.OR;
        }
        return ExpressionType.AND;
    }
}

