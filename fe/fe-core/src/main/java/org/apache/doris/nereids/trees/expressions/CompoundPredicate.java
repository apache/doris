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

import java.util.Objects;

/**
 * Compound predicate expression.
 * Such as &&,||,AND,OR.
 */
public abstract class CompoundPredicate extends Expression implements BinaryExpression {
    protected final String symbol;

    /**
     * Desc: Constructor for CompoundPredicate.
     *
     * @param left  left child of comparison predicate
     * @param right right child of comparison predicate
     */
    public CompoundPredicate(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    @Override
    public String toSql() {
        return "(" + left().toSql() + " " + symbol + " " + right().toSql() + ")";
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CompoundPredicate other = (CompoundPredicate) o;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, left(), right());
    }

    @Override
    public String toString() {
        return "(" + left().toString() + " " + symbol + " " + right().toString() + ")";
    }

    /**
     * Flip logical `and` and `or` operator with original children.
     */
    public abstract CompoundPredicate flip();

    /**
     * Flip logical `and` and `or` operator with new children.
     */
    public abstract CompoundPredicate flip(Expression left, Expression right);

    public abstract Class<? extends CompoundPredicate> flipType();
}

