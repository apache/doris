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

import org.apache.doris.catalog.BuiltinAggregateFunctions;
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
import org.apache.doris.nereids.trees.expressions.functions.ExpressionTrait;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunctionParams;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.AggStateType;
import org.apache.doris.nereids.types.DataType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

/**
 * Aggregate inputs into the nested function's serialized state.
 */
public class CombineCombinator extends AggregateFunction
        implements ExplicitlyCastableSignature, AlwaysNotNullable, Combinator, RollUpTrait {

    private final AggregateFunction nested;
    private final AggStateType returnType;

    /** Constructor of CombineCombinator. */
    public CombineCombinator(List<Expression> arguments, AggregateFunction nested) {
        super(nested.getName() + AggCombinerFunctionBuilder.COMBINE_SUFFIX, arguments);
        checkArguments(arguments, nested);
        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        this.returnType = createReturnType(arguments, nested);
    }

    private CombineCombinator(AggregateFunctionParams functionParams, AggregateFunction nested) {
        super(functionParams);
        checkArguments(functionParams.arguments, nested);
        this.nested = Objects.requireNonNull(nested, "nested can not be null");
        this.returnType = createReturnType(functionParams.arguments, nested);
    }

    private static void checkArguments(List<Expression> arguments, AggregateFunction nested) {
        if (arguments.isEmpty()) {
            throw new AnalysisException(String.format(
                    "%s_combine requires at least one argument", nested.getName()));
        }
        for (Expression argument : arguments) {
            if (argument instanceof OrderExpression) {
                throw new AnalysisException(String.format(
                        "%s_combine doesn't support order by expression", nested.getName()));
            }
        }
    }

    private static AggStateType createReturnType(List<Expression> arguments, AggregateFunction nested) {
        // The raw arguments determine the nested signature. Retargeting this state through a loose
        // AggState cast would make FE metadata disagree with the state produced by the aggregate.
        return new AggStateType(nested.getName(),
                arguments.stream().map(ExpressionTrait::getDataType)
                        .collect(ImmutableList.toImmutableList()),
                arguments.stream().map(ExpressionTrait::nullable)
                        .collect(ImmutableList.toImmutableList()),
                BuiltinAggregateFunctions.INSTANCE.aggFuncNameNullableMap.get(nested.getName()),
                false);
    }

    @Override
    public CombineCombinator withChildren(List<Expression> children) {
        return new CombineCombinator(getFunctionParams(children), nested);
    }

    @Override
    public AggregateFunction withDistinctAndChildren(boolean distinct, List<Expression> children) {
        if (distinct) {
            throw new AnalysisException(getName() + " doesn't support DISTINCT");
        }
        return new CombineCombinator(getFunctionParams(false, children), nested);
    }

    @Override
    public List<FunctionSignature> getSignatures() {
        return nested.getSignatures().stream()
                .map(signature -> signature.withReturnType(returnType))
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitCombineCombinator(this, context);
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
    public boolean supportAggregatePhase(AggregatePhase aggregatePhase) {
        return nested.supportAggregatePhase(aggregatePhase);
    }

    @Override
    protected List<DataType> intermediateTypes() {
        return nested.getIntermediateTypes().getIntermediateTypes();
    }

    @Override
    public Function constructRollUp(Expression param, Expression... varParams) {
        String nestedName = AggCombinerFunctionBuilder.getNestedName(getName());
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
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

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        nested.checkLegalityBeforeTypeCoercion();
    }

    @Override
    public void checkLegalityAfterRewrite() {
        nested.withChildren(children()).checkLegalityAfterRewrite();
    }
}
