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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ComputeNullable;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * AggState combinator merge
 */
public class MergeCombinator extends AggregateFunction
        implements UnaryExpression, ExplicitlyCastableSignature, ComputeNullable, Combinator, RollUpTrait {

    private final AggregateFunction nested;
    private final AggStateType inputType;

    public MergeCombinator(List<Expression> arguments, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.MERGE_SUFFIX, arguments);

        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        inputType = (AggStateType) arguments.get(0).getDataType();
    }

    @Override
    public MergeCombinator withChildren(List<Expression> children) {
        return new MergeCombinator(children, nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream().map(sig -> {
            return sig.withArgumentTypes(false, ImmutableList.of(inputType));
        }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitMergeCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return nested.getDataType();
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
    public boolean nullable() {
        return nested.nullable();
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(getName(), param);
        Pair<? extends Expression, ? extends BoundFunction> targetExpressionPair = functionBuilder.build(getName(),
                param);
        return (Function) targetExpressionPair.key();
    }

    @Override
    public boolean canRollUp() {
        return false;
    }
}
