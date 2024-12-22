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

import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.typecoercion.ExpectsInputTypes;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Abstract for all binary operator, include binary arithmetic, compound predicate, comparison predicate.
 */
public abstract class UnaryOperator extends Expression implements UnaryExpression, ExpectsInputTypes {

    protected final String symbol;

    protected UnaryOperator(List<Expression> child, String symbol) {
        super(child);
        this.symbol = symbol;
    }

    public abstract DataType inputType();

    @Override
    public List<DataType> expectedInputTypes() {
        return ImmutableList.of(inputType());
    }

    @Override
    public String computeToSql() {
        return "(" + symbol + " " + child().toSql() + ")";
    }

    @Override
    public String toString() {
        return "(" + symbol + " " + child().toString() + ")";
    }

    @Override
    public int hashCode() {
        return Objects.hash(symbol, child());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UnaryOperator other = (UnaryOperator) o;
        return Objects.equals(child(), other.child());
    }
}
