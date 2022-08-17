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
import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ImplicitCastInputTypes;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.AbstractDataType;
import org.apache.doris.nereids.types.coercion.TypeCollection;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * string regex expression.
 * Such as: like, regexp
 */
public abstract class StringRegexPredicate extends Expression implements BinaryExpression, ImplicitCastInputTypes {

    // used in interface expectedInputTypes to avoid new list in each time it be called
    private static final List<AbstractDataType> EXPECTED_INPUT_TYPES = ImmutableList.of(
            TypeCollection.CHARACTER_TYPE_COLLECTION,
            TypeCollection.CHARACTER_TYPE_COLLECTION
    );

    /**
     * like or regexp
     */
    protected final String symbol;

    /**
     * Constructor of StringRegexPredicate.
     *
     * @param left     left child of string regex
     * @param right    right child of string regex
     * @param symbol   operator symbol
     */
    public StringRegexPredicate(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    @Override
    public DataType getDataType() throws UnboundException {
        return BooleanType.INSTANCE;
    }

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return EXPECTED_INPUT_TYPES;
    }

    @Override
    public String toSql() {
        return '(' + left().toSql() + ' ' + symbol + ' ' + right().toSql() + ')';
    }

    @Override
    public String toString() {
        return "(" + left() + " " + symbol + " " + right() + ")";
    }

    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStringRegexPredicate(this, context);
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, left(), right());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        StringRegexPredicate other = (StringRegexPredicate) o;
        return Objects.equals(left(), other.left())
                && Objects.equals(right(), other.right());
    }
}
