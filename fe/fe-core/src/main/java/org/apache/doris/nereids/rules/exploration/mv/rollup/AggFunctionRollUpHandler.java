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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.functions.combinator.Combinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.CombineCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.MergeCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Aggregate function roll up handler
 */
public abstract class AggFunctionRollUpHandler {

    /**
     * Decide the query and view function can roll up or not
     */
    public boolean canRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression viewExpression = mvExprToMvScanExprQueryBasedPair.key();
        if (!(viewExpression instanceof RollUpTrait) || !((RollUpTrait) viewExpression).canRollUp()) {
            return false;
        }
        AggregateFunction aggregateFunction = (AggregateFunction) viewExpression;
        return !aggregateFunction.isDistinct();
    }

    /**
     * Do the aggregate function roll up
     */
    public abstract Function doRollup(
            AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap);

    /**
     * Extract the function arguments by functionWithAny pattern
     * Such as functionWithAny def is bitmap_union(to_bitmap(Any.INSTANCE)),
     * actualExpression is bitmap_union(to_bitmap(case when a = 5 then 1 else 2 end))
     * after extracting, the return argument is: case when a = 5 then 1 else 2 end
     */
    protected static List<Expression> extractArguments(Expression functionWithAny, Expression actualExpression) {
        Set<Object> exprSetToRemove = functionWithAny.collectToSet(expr -> !(expr instanceof Any));
        return actualExpression.collectFirst(expr ->
                        exprSetToRemove.stream().noneMatch(exprToRemove -> exprToRemove.equals(expr)))
                .map(expr -> ImmutableList.of((Expression) expr)).orElse(ImmutableList.of());
    }

    /**
     * Unwrap direct MERGE/UNION state chains for the same aggregate function.
     * STATE and COMBINE consume values, so keep their complete argument expressions for comparison.
     * For example, sum_combine(avg_finalize(avg_state(v))) must retain SUM and its finalized value.
     */
    protected static Combinator extractRollupCombinator(Combinator current) {
        while (current instanceof MergeCombinator || current instanceof UnionCombinator) {
            Expression argument = current.getArguments().get(0);
            if (!(argument instanceof StateCombinator || argument instanceof CombineCombinator
                    || argument instanceof UnionCombinator)) {
                break;
            }
            Combinator next = (Combinator) argument;
            if (!current.getNestedFunction().getName().equals(next.getNestedFunction().getName())) {
                break;
            }
            current = next;
        }
        return current;
    }
}
