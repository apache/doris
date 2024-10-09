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
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.ExplicitlyCastableSignature;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ScalarFunction;
import org.apache.doris.nereids.trees.expressions.shape.UnaryExpression;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * AggState combinator state
 */
public class StateCombinator extends ScalarFunction
        implements UnaryExpression, ExplicitlyCastableSignature, AlwaysNotNullable, Combinator, RollUpTrait {

    private final AggregateFunction nested;
    private final AggStateType returnType;

    /**
     * constructor of StateCombinator
     */
    public StateCombinator(List<Expression> arguments, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.STATE_SUFFIX, arguments);
        for (Expression arg : arguments) {
            if (arg instanceof OrderExpression) {
                throw new AnalysisException(String
                        .format("%s_state doesn't support order by expression", nested.getName()));
            }
        }

        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        this.returnType = new AggStateType(nested.getName(), arguments.stream().map(arg -> {
            return arg.getDataType();
        }).collect(ImmutableList.toImmutableList()), arguments.stream().map(arg -> {
            return arg.nullable();
        }).collect(ImmutableList.toImmutableList()));
    }

    public static StateCombinator create(AggregateFunction nested) {
        return new StateCombinator(nested.getArguments(), nested);
    }

    @Override
    public StateCombinator withChildren(List<Expression> children) {
        return new StateCombinator(children, nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream().map(sig -> {
            return sig.withReturnType(returnType);
        }).collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitStateCombinator(this, context);
    }

    @Override
    public DataType getDataType() {
        return returnType;
    }

    @Override
    public AggregateFunction getNestedFunction() {
        return nested;
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        String nestedName = AggCombinerFunctionBuilder.getNestedName(getName());
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        // state combinator roll up result should be union combinator
        String combinatorName = nestedName + AggCombinerFunctionBuilder.UNION_SUFFIX;
        FunctionBuilder functionBuilder = functionRegistry.findFunctionBuilder(combinatorName, param);
        Pair<? extends Expression, ? extends BoundFunction> targetExpressionPair =
                functionBuilder.build(combinatorName, param);
        return (Function) targetExpressionPair.key();
    }

    @Override
    public boolean canRollUp() {
        return true;
    }
}
