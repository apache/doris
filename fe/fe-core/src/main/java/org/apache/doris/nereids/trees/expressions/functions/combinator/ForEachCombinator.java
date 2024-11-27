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
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * combinator foreach
 */
public class ForEachCombinator extends NullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, Combinator {

    private final AggregateFunction nested;

    /**
     * constructor of ForEachCombinator
     */
    public ForEachCombinator(List<Expression> arguments, AggregateFunction nested) {
        this(arguments, false, nested);
    }

    public ForEachCombinator(List<Expression> arguments, boolean alwaysNullable, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.FOREACH_SUFFIX, false, alwaysNullable, arguments);

        this.nested = Objects.requireNonNull(nested, "nested can not be null");
    }

    public static ForEachCombinator create(AggregateFunction nested) {
        return new ForEachCombinator(nested.getArguments(), nested);
    }

    @Override
    public ForEachCombinator withChildren(List<Expression> children) {
        return new ForEachCombinator(children, alwaysNullable, nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream().map(sig -> {
            return sig.withReturnType(ArrayType.of(sig.returnType)).withArgumentTypes(sig.hasVarArgs,
                    sig.argumentsTypes.stream().map(arg -> {
                        return ArrayType.of(arg);
                    }).collect(ImmutableList.toImmutableList()));
        }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitForEachCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return ArrayType.of(nested.getDataType(), nested.nullable());
    }

    @Override
    public AggregateFunction getNestedFunction() {
        return nested;
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        throw new UnsupportedOperationException("Unimplemented method 'withDistinctAndChildren'");
    }

    @Override
    public NullableAggregateFunction withAlwaysNullable(boolean alwaysNullable) {
        return new ForEachCombinator(children, alwaysNullable, nested);
    }
}
