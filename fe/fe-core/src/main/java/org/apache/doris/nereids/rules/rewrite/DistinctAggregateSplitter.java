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

package org.apache.doris.nereids.rules.rewrite;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Rewrites queries containing DISTINCT aggregate functions by splitting them into two processing layers:
 * 1. Lower layer: Performs deduplication on both grouping columns and DISTINCT columns
 * 2. Upper layer: Applies simple aggregation on the deduplicated results
 *
 * For example, transforms:
 *   SELECT COUNT(DISTINCT a), count(c) FROM t GROUP BY b
 * Into:
 *   SELECT COUNT(a), sum0(cnt) FROM (
 *     SELECT a, b, count(c) cnt FROM t GROUP BY a, b
 *   ) GROUP BY b
 */
public class DistinctAggregateSplitter extends OneRewriteRuleFactory {
    public static final DistinctAggregateSplitter INSTANCE = new DistinctAggregateSplitter();
    private static final Set<Class<? extends AggregateFunction>> supportSplitOtherFunctions = ImmutableSet.of(
            Sum.class, Min.class, Max.class, Count.class, Sum0.class, AnyValue.class);
    private static final Map<Class<? extends AggregateFunction>,
            Pair<Class<? extends AggregateFunction>, Class<? extends AggregateFunction>>> aggFunctionMap =
            ImmutableMap.of(
                    Sum.class, Pair.of(Sum.class, Sum.class),
                    Min.class, Pair.of(Min.class, Min.class),
                    Max.class, Pair.of(Max.class, Max.class),
                    Count.class, Pair.of(Count.class, Sum0.class),
                    Sum0.class, Pair.of(Sum0.class, Sum0.class),
                    AnyValue.class, Pair.of(AnyValue.class, AnyValue.class)
                    );

    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(agg -> agg.getGroupByExpressions().isEmpty())
                .then(this::apply).toRule(RuleType.DISTINCT_AGGREGATE_SPLIT);
    }

    private boolean checkByStatistics(LogicalAggregate<? extends Plan> aggregate) {
        // 带group by的场景, group by key ndv低, distinct key ndv高, 则转为multi_distinct
        //      其他情况都拆分
        // 不带group by的场景, distinct key的ndv低,使用multi_distinct, ndv高,使用cte拆分
        return true;
    }

    private Plan apply(LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateFunction> aggFuncs = aggregate.getAggregateFunctions();
        //这个函数同时也要处理count(distinct a,b)这种
        // 需要保证1.只有一个count(distinct)函数
        // 如果是multi_distinct不处理
        Set<AggregateFunction> distinctAggFuncs = new HashSet<>();
        Set<AggregateFunction> otherFunctions = new HashSet<>();
        for (AggregateFunction aggFunc : aggFuncs) {
            if (aggFunc.isDistinct()) {
                distinctAggFuncs.add(aggFunc);
            } else {
                otherFunctions.add(aggFunc);
            }
        }
        if (distinctAggFuncs.size() != 1) {
            return null;
        }

        // 并不是所有的都能拆分, other function里面如果有一些不能拆分的函数,就不拆
        // 这里先实现一下,然后后续再考虑一下什么函数可以拆分.
        for (AggregateFunction aggFunc : otherFunctions) {
            if (!supportSplitOtherFunctions.contains(aggFunc.getClass())) {
                return null;
            }
        }

        // 满足条件了,开始写拆分的代码
        // 先构造下面进行去重的AGG
        // group by key为group by key + distinct key
        Set<Expression> groupByKeys = ImmutableSet.<Expression>builder()
                .addAll(aggregate.getGroupByExpressions())
                .addAll(distinctAggFuncs.iterator().next().getDistinctArguments())
                .build();
        // 需要把otherFunction输出一下
        ImmutableList.Builder<NamedExpression> bottomAggOtherFunctions = ImmutableList.builder();
        Map<AggregateFunction, NamedExpression> aggFuncToSlot = new HashMap<>();
        for (AggregateFunction aggFunc : otherFunctions) {
            Alias bottomAggFuncAlias = new Alias(aggFunc);
            bottomAggOtherFunctions.add(bottomAggFuncAlias);
            aggFuncToSlot.put(aggFunc, bottomAggFuncAlias.toSlot());
        }

        List<NamedExpression> aggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((Set) groupByKeys)
                .addAll(bottomAggOtherFunctions.build())
                .build();

        LogicalAggregate<Plan> bottomAgg = new LogicalAggregate<>(Utils.fastToImmutableList(groupByKeys),
                aggOutput, aggregate.child());

        // 然后构造上面的AGG
        List<NamedExpression> topAggOutput = ExpressionUtils.rewriteDownShortCircuit(aggregate.getOutputExpressions(),
                expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        // 如果是distinct function,那么直接把distinct去掉
                        if (aggFunc.isDistinct()) {
                            if (aggFunc.arity() == 1) {
                                return aggFunc.withDistinctAndChildren(false, aggFunc.children());
                            } else if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                                return AggregateUtils.countDistinctMultiExprToCountIf((Count) aggFunc);
                            }
                        } else {
                            if (aggFuncToSlot.get(aggFunc) != null) {
                                if (aggFunc instanceof Count) {
                                    return new Count(aggFuncToSlot.get(aggFunc));
                                } else {
                                    return aggFunc.withChildren(aggFuncToSlot.get(aggFunc));
                                }
                            }
                            return aggFunc;
                        }
                    }
                    return expr;
                }
        );
        LogicalAggregate<Plan> topAgg = new LogicalAggregate<>(aggregate.getGroupByExpressions(),
                topAggOutput, bottomAgg);
        return topAgg;
    }
}
