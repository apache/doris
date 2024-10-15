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
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.RollUpTrait;
import org.apache.doris.nereids.trees.expressions.functions.combinator.Combinator;

import java.util.Map;

/**
 * Roll up directly, for example,
 * query is select c1, sum(c2) from t1 group by c1
 * view is select c1, c2, sum(c2) from t1 group by c1, c2,
 * the aggregate function in query and view is same, This handle the sum aggregate function roll up
 */
public class DirectRollupHandler extends AggFunctionRollUpHandler {

    public static DirectRollupHandler INSTANCE = new DirectRollupHandler();

    @Override
    public boolean canRollup(
            AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression viewExpression = mvExprToMvScanExprQueryBasedPair.key();
        if (!super.canRollup(queryAggregateFunction, queryAggregateFunctionShuttled,
                mvExprToMvScanExprQueryBasedPair, mvExprToMvScanExprQueryBasedMap)) {
            return false;
        }
        boolean isEquals = queryAggregateFunctionShuttled instanceof NullableAggregateFunction
                && viewExpression instanceof NullableAggregateFunction
                ? ((NullableAggregateFunction) queryAggregateFunctionShuttled).equalsIgnoreNullable(viewExpression)
                : queryAggregateFunctionShuttled.equals(viewExpression);
        return isEquals && MappingRollupHandler.AGGREGATE_ROLL_UP_EQUIVALENT_FUNCTION_MAP.keySet().stream()
                .noneMatch(aggFunction -> aggFunction.equals(queryAggregateFunction))
                && !(queryAggregateFunction instanceof Combinator);
    }

    @Override
    public Function doRollup(AggregateFunction queryAggregateFunction,
            Expression queryAggregateFunctionShuttled,
            Pair<Expression, Expression> mvExprToMvScanExprQueryBasedPair,
            Map<Expression, Expression> mvExprToMvScanExprQueryBasedMap) {
        Expression rollupParam = mvExprToMvScanExprQueryBasedPair.value();
        if (rollupParam == null) {
            return null;
        }
        return ((RollUpTrait) queryAggregateFunction).constructRollUp(rollupParam);
    }
}
