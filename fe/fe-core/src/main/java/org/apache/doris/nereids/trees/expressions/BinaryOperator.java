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

import org.apache.doris.nereids.trees.expressions.shape.BinaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.types.coercion.AbstractDataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Abstract for all binary operator, include binary arithmetic, compound predicate, comparison predicate.
 */
public abstract class BinaryOperator extends Expression implements BinaryExpression, ExpectsInputTypes {

    protected final String symbol;

    public BinaryOperator(Expression left, Expression right, String symbol) {
        super(left, right);
        this.symbol = symbol;
    }

    public abstract AbstractDataType inputType();

    @Override
    public List<AbstractDataType> expectedInputTypes() {
        return ImmutableList.of(inputType(), inputType());
    }

    @Override
    public String toSql() {
        return "(" + left().toSql() + " " + symbol + " " + right().toSql() + ")";
    }

    @Override
    public String toString() {
        return "(" + left().toString() + " " + symbol + " " + right().toString() + ")";
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
        BinaryOperator other = (BinaryOperator) o;
        return Objects.equals(left(), other.left()) && Objects.equals(right(), other.right());
    }
}
