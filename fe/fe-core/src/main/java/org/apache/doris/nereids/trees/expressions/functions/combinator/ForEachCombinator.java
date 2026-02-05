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
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunctionParams;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * combinator foreach
 */
public class ForEachCombinator extends NullableAggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, Combinator {

    public static final Set<String> UNSUPPORTED_AGGREGATE_FUNCTION = Collections.unmodifiableSet(new HashSet<String>() {
        {
            add("percentile");
            add("percentile_array");
            add("percentile_approx");
            add("percentile_approx_weighted");
        }
    });

    private final AggregateFunction nested;

    /**
     * constructor of ForEachCombinator
     */
    public ForEachCombinator(List<Expression> arguments, AggregateFunction nested) {
        this(arguments, false, nested);
    }

    /**
     * Constructs a new instance of {@code ForEachCombinator}.
     *
     * <p>This constructor initializes a combinator that will iterate over each item in the input list
     * and apply the nested aggregate function.
     * If the provided aggregate function name is within the list of unsupported functions,
     * an {@link UnsupportedOperationException} will be thrown.
     *
     * @param arguments A list of {@code Expression} objects that serve as parameters to the aggregate function.
     * @param alwaysNullable A boolean flag indicating whether this combinator should always return a nullable result.
     * @param nested The nested aggregate function to apply to each element. It must not be {@code null}.
     * @throws NullPointerException If the provided nested aggregate function is {@code null}.
     * @throws UnsupportedOperationException If nested aggregate function is one of the unsupported aggregate functions
     */
    public ForEachCombinator(List<Expression> arguments, boolean alwaysNullable, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.FOREACH_SUFFIX, false, alwaysNullable, arguments);

        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        if (UNSUPPORTED_AGGREGATE_FUNCTION.contains(nested.getName().toLowerCase())) {
            throw new UnsupportedOperationException("Unsupport the func:" + nested.getName() + " use in foreach");
        }
    }

    private ForEachCombinator(NullableAggregateFunctionParams functionParams, AggregateFunction nested) {
        super(functionParams);

        this.nested = nested;
        if (UNSUPPORTED_AGGREGATE_FUNCTION.contains(nested.getName().toLowerCase())) {
            throw new UnsupportedOperationException("Unsupport the func:" + nested.getName() + " use in foreach");
        }
    }

    public static ForEachCombinator create(AggregateFunction nested) {
        return new ForEachCombinator(nested.getArguments(), nested);
    }

    @Override
    public ForEachCombinator withChildren(List<Expression> children) {
        return new ForEachCombinator(getFunctionParams(children), nested);
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
        return ArrayType.of(nested.getDataType(), true);
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
        return new ForEachCombinator(getAlwaysNullableFunctionParams(alwaysNullable), nested);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        nested.checkLegalityBeforeTypeCoercion();
    }
}
