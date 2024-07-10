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
     * actualFunction is bitmap_union(to_bitmap(case when a = 5 then 1 else 2 end))
     * after extracting, the return argument is: case when a = 5 then 1 else 2 end
     */
    protected static List<Expression> extractArguments(Expression functionWithAny, Function actualFunction) {
        Set<Object> exprSetToRemove = functionWithAny.collectToSet(expr -> !(expr instanceof Any));
        return actualFunction.collectFirst(expr ->
                        exprSetToRemove.stream().noneMatch(exprToRemove -> exprToRemove.equals(expr)))
                .map(expr -> ImmutableList.of((Expression) expr)).orElse(ImmutableList.of());
    }

    /**
     * Extract the target expression in actualFunction by targetClazz
     * Such as actualFunction def is avg_merge(avg_union(c1)), target Clazz is Combinator
     * after extracting, the return argument is avg_union(c1)
     */
    protected static <T> T extractLastExpression(Expression actualFunction, Class<T> targetClazz) {
        List<Expression> expressions = actualFunction.collectToList(targetClazz::isInstance);
        return targetClazz.cast(expressions.get(expressions.size() - 1));
    }
}
