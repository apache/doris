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
import org.apache.doris.nereids.trees.expressions.Divide;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * AvgDistinctToSumDivCount.
 *
 * change avg( distinct a ) into sum( distinct a ) / count( distinct a ) if there are more than 1 distinct arguments
 */
public class AvgDistinctToSumDivCount extends OneAnalysisRuleFactory {
    @Override
    public Rule build() {
        return RuleType.AVG_DISTINCT_TO_SUM_DIV_COUNT.build(
                logicalAggregate().when(agg -> agg.getDistinctArguments().size() > 1).then(agg -> {
                    Map<AggregateFunction, Expression> avgToSumDivCount = agg.getAggregateFunctions()
                            .stream()
                            .filter(function -> function instanceof Avg && function.isDistinct())
                            .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                                Sum sum = new Sum(true, ((Avg) function).child());
                                Count count = new Count(true, ((Avg) function).child());
                                Divide divide = new Divide(sum, count);
                                return divide;
                            }));
                    if (!avgToSumDivCount.isEmpty()) {
                        List<NamedExpression> newOutput = agg.getOutputExpressions().stream()
                                .map(expr -> (NamedExpression) ExpressionUtils.replace(expr, avgToSumDivCount))
                                .collect(Collectors.toList());
                        return new LogicalAggregate<>(agg.getGroupByExpressions(), newOutput,
                                agg.child());
                    } else {
                        return agg;
                    }
                })
        );
    }
}

