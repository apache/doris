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

package org.apache.doris.nereids.rules.exploration.mv.rollup;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Any;
import org.apache.doris.nereids.trees.expressions.BinaryArithmetic;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 * Try to roll up function which contains distinct, if the param in function is in
 * materialized view group by dimension.
 * For example
 * materialized view def is select empid, deptno, count(salary) from distinctQuery group by empid, deptno;
 * query is select deptno, count(distinct empid) from distinctQuery group by deptno;
 * should rewrite successfully, count(distinct empid) should use the group by empid dimension in query.
 */
public class ContainDistinctFunctionRollupHandler extends AggFunctionRollUpHandler {

    public static final ContainDistinctFunctionRollupHandler INSTANCE = new ContainDistinctFunctionRollupHandler();
    public static Set<AggregateFunction> SUPPORTED_AGGREGATE_FUNCTION_SET = ImmutableSet.of(
            new Max(true, Any.INSTANCE), new Min(true, Any.INSTANCE),
            new Max(false, Any.INSTANCE), new Min(false, Any.INSTANCE),
            new Count(true, Any.INSTANCE), new Sum(true, Any.INSTANCE),
            new Avg(true, Any.INSTANCE));

    @Override
    public boolean canRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBased) {
        Set<AggregateFunction> queryAggregateFunctions =
                queryAggregateFunctionShuttled.collectToSet(AggregateFunction.class::isInstance);
        if (queryAggregateFunctions.size() > 1) {
            return false;
        }
        for (AggregateFunction aggregateFunction : queryAggregateFunctions) {
            if (SUPPORTED_AGGREGATE_FUNCTION_SET.stream()
                    .noneMatch(supportFunction -> Any.equals(supportFunction, aggregateFunction))) {
                return false;
            }
            if (aggregateFunction.getArguments().size() > 1) {
                return false;
            }
        }
        Set<Expression> mvExpressionsQueryBased = mvExprToMvScanExprQueryBased.keySet();
        Set<Slot> aggregateFunctionParamSlots = queryAggregateFunctionShuttled.collectToSet(Slot.class::isInstance);
        if (aggregateFunctionParamSlots.stream().anyMatch(slot -> !mvExpressionsQueryBased.contains(slot))) {
            return false;
        }
        return true;
    }

    @Override
    public Function doRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled, Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression argument = queryAggregateFunction.children().get(0);
        RollupResult<Boolean> rollupResult = RollupResult.of(true);
        Expression rewrittenArgument = argument.accept(new DefaultExpressionRewriter<RollupResult<Boolean>>() {
            @Override
            public Expression visitSlot(Slot slot, RollupResult<Boolean> context) {
                if (!mvExprToMvScanExprQueryBasedMap.containsKey(slot)) {
                    context.param = false;
                    return slot;
                }
                return mvExprToMvScanExprQueryBasedMap.get(slot);
            }

            @Override
            public Expression visit(Expression expr, RollupResult<Boolean> context) {
                if (!context.param) {
                    return expr;
                }
                if (expr instanceof Literal || expr instanceof BinaryArithmetic || expr instanceof Slot) {
                    return super.visit(expr, context);
                }
                context.param = false;
                return expr;
            }
        }, rollupResult);
        if (!rollupResult.param) {
            return null;
        }
        return (Function) queryAggregateFunction.withChildren(rewrittenArgument);
    }

    private static class RollupResult<T> {
        public T param;

        private RollupResult(T param) {
            this.param = param;
        }

        public static <T> RollupResult<T> of(T param) {
            return new RollupResult<>(param);
        }

        public T getParam() {
            return param;
        }
    }
}
