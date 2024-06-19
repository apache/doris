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

import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionRegistry;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AggCombinerFunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.FunctionBuilder;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.combinator.Combinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.StateCombinator;
import org.apache.doris.nereids.trees.expressions.functions.combinator.UnionCombinator;

import java.util.Map;
import java.util.Objects;

/**
 * Handle the combinator aggregate function roll up, Only view is combinator, query is aggregate function.
 * Such as query is select c1 sum(c2) from orders group by c1;
 * view is select c1 sum_union(sum_state(c2)) from orders group by c1;
 * */
public class SingleCombinatorRollupHandler extends AggFunctionRollUpHandler {

    public static SingleCombinatorRollupHandler INSTANCE = new SingleCombinatorRollupHandler();

    @Override
    public boolean canRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression viewFunction = mvExprToMvScanExprQueryBasedPair.key();
        if (!super.canRollup(queryAggregateFunction, queryAggregateFunctionShuttled,
                mvExprToMvScanExprQueryBasedPair, mvExprToMvScanExprQueryBasedMap)) {
            return false;
        }
        if (!(queryAggregateFunction instanceof Combinator)
                && (viewFunction instanceof UnionCombinator || viewFunction instanceof StateCombinator)) {
            Combinator viewCombinator = extractLastExpression(viewFunction, Combinator.class);
            return Objects.equals(queryAggregateFunction,
                    viewCombinator.getNestedFunction().withChildren(viewCombinator.getArguments()));
        }
        return false;
    }

    @Override
    public Function doRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        FunctionRegistry functionRegistry = Env.getCurrentEnv().getFunctionRegistry();
        String combinatorName = queryAggregateFunction.getName() + AggCombinerFunctionBuilder.MERGE_SUFFIX;
        Expression rollupParam = mvExprToMvScanExprQueryBasedPair.value();
        FunctionBuilder functionBuilder =
                functionRegistry.findFunctionBuilder(combinatorName, rollupParam);
        Pair<? extends Expression, ? extends BoundFunction> targetExpressionPair =
                functionBuilder.build(combinatorName, rollupParam);
        return (Function) targetExpressionPair.key();
    }
}
