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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**
 * AvgDistinctToSumDivCount.
 *
 * change avg( distinct a ) into sum( distinct a ) / count( distinct a ) if there are more than 1 distinct arguments
 */
public class AvgDistinctToSumDivCount extends OneRewriteRuleFactory {
    @Override
    public Rule build() {
        return RuleType.AVG_DISTINCT_TO_SUM_DIV_COUNT.build(
                logicalAggregate().when(agg -> agg.getDistinctArguments().size() > 1).then(agg -> {
                    Map<AggregateFunction, Expression> avgToSumDivCount = agg.getAggregateFunctions()
                            .stream()
                            .filter(function -> function instanceof Avg && function.isDistinct())
                            .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                                Sum sum = (Sum) TypeCoercionUtils.processBoundFunction(
                                        new Sum(true, ((Avg) function).isAlwaysNullable(), ((Avg) function).child()));
                                Count count = (Count) TypeCoercionUtils.processBoundFunction(
                                        new Count(true, ((Avg) function).child()));
                                return TypeCoercionUtils.processDivide(new Divide(sum, count));
                            }));
                    if (!avgToSumDivCount.isEmpty()) {
                        List<NamedExpression> newOutput = agg.getOutputExpressions().stream()
                                .map(expr -> ExpressionUtils.replaceNameExpression(expr, avgToSumDivCount))
                                .collect(ImmutableList.toImmutableList());
                        return new LogicalAggregate<>(agg.getGroupByExpressions(), newOutput, agg.child());
                    } else {
                        return agg;
                    }
                })
        );
    }
}
