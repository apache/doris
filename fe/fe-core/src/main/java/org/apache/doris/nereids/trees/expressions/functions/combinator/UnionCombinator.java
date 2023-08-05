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

package org.apache.doris.nereids.trees.expressions.functions.combinator;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggStateFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * AggState combinator union
 */
public class UnionCombinator extends AggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable {

    private final AggregateFunction nested;
    private final AggStateType inputType;

    public UnionCombinator(List<Expression> arguments, AggregateFunction nested) {
        super(nested.getName() + AggStateFunctionBuilder.UNION_SUFFIX, arguments);

        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        inputType = (AggStateType) arguments.get(0).getDataType();
    }

    @Override
    public UnionCombinator withChildren(List<Expression> children) {
        return new UnionCombinator(children, nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream().map(sig -> {
            return sig.withArgumentTypes(false, ImmutableList.of(inputType)).withReturnType(inputType);
        }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitUnionCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return inputType;
    }

    public AggregateFunction getNestedFunction() {
        return nested;
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withDistinctAndChildren'");
    }
}
