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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.stats.ExpressionEstimation;
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
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Process aggregate with a DISTINCT AGGFunction
 * Decide whether to split DISTINCT aggregate into top-bottom aggregation or use multi_distinct;
 * For example, transforms:
 *   SELECT COUNT(DISTINCT a), count(c) FROM t GROUP BY b
 * 1.Rewrites queries containing DISTINCT aggregate functions by splitting them into two processing layers:
 *   Lower layer: Performs deduplication on both grouping columns and DISTINCT columns
 *   Upper layer: Applies simple aggregation on the deduplicated results
 * rewrite into:
 *   SELECT COUNT(a), sum0(cnt) FROM (
 *     SELECT a, b, count(c) cnt FROM t GROUP BY a, b
 *   ) GROUP BY b
 *
 * use multi_distinct:
 * rewrite into:
 *.  SELECT MULTI_DISTINCT_COUNT(a), count(c) FROM t GROUP BY b
 */
public class DistinctAggregateRewriter implements RewriteRuleFactory {
    public static final DistinctAggregateRewriter INSTANCE = new DistinctAggregateRewriter();
    // TODO: add other functions
    private static final Set<Class<? extends AggregateFunction>> supportSplitOtherFunctions = ImmutableSet.of(
            Sum.class, Min.class, Max.class, Count.class, Sum0.class, AnyValue.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate()
                        .whenNot(agg -> agg.getGroupByExpressions().isEmpty())
                        .whenNot(Aggregate::canSkewRewrite)
                        .thenApply(ctx -> rewrite(ctx.root, ctx.connectContext))
                        .toRule(RuleType.DISTINCT_AGGREGATE_SPLIT),
                logicalAggregate()
                        .when(agg -> agg.getGroupByExpressions().isEmpty()
                                && agg.mustUseMultiDistinctAgg())
                        .then(this::convertToMultiDistinct)
                        .toRule(RuleType.PROCESS_SCALAR_AGG_MUST_USE_MULTI_DISTINCT)
        );
    }

    private boolean shouldUseMultiDistinct(LogicalAggregate<? extends Plan> aggregate) {
        // count(distinct a,b) cannot use multi_distinct
        if (AggregateUtils.containsCountDistinctMultiExpr(aggregate)) {
            return false;
        }
        if (aggregate.mustUseMultiDistinctAgg()) {
            return true;
        }
        ConnectContext ctx = ConnectContext.get();
        if (ctx.getSessionVariable().aggPhase == 1 || ctx.getSessionVariable().aggPhase == 2) {
            return true;
        }
        if (aggregate.getStats() == null || aggregate.child().getStats() == null) {
            StatsDerive derive = new StatsDerive(false);
            aggregate.accept(derive, new DeriveContext());
        }
        Statistics aggStats = aggregate.getStats();
        Statistics aggChildStats = aggregate.child().getStats();
        Set<Expression> dstArgs = aggregate.getDistinctArguments();
        // has unknown statistics, split to bottom and top agg
        if (AggregateUtils.hasUnknownStatistics(aggregate.getGroupByExpressions(), aggChildStats)
                || AggregateUtils.hasUnknownStatistics(dstArgs, aggChildStats)) {
            return true;
        }

        double gbyNdv = aggStats.getRowCount();
        Expression dstKey = dstArgs.iterator().next();
        ColumnStatistic dstKeyStats = aggChildStats.findColumnStatistics(dstKey);
        if (dstKeyStats == null) {
            dstKeyStats = ExpressionEstimation.estimate(dstKey, aggChildStats);
        }
        double dstNdv = dstKeyStats.ndv;
        double inputRows = aggChildStats.getRowCount();
        // group by key ndv is low, distinct key ndv is high, multi_distinct is better
        // otherwise split to bottom and top agg
        return gbyNdv < inputRows * AggregateUtils.LOW_CARDINALITY_THRESHOLD
                && dstNdv > inputRows * AggregateUtils.HIGH_CARDINALITY_THRESHOLD;
    }

    private Plan rewrite(LogicalAggregate<? extends Plan> aggregate, ConnectContext ctx) {
        if (aggregate.distinctFuncNum() == 0) {
            return null;
        }
        if (ctx.getSessionVariable().aggPhase == 3 || ctx.getSessionVariable().aggPhase == 4) {
            return null;
        }
        if (shouldUseMultiDistinct(aggregate)) {
            return convertToMultiDistinct(aggregate);
        } else {
            return splitDistinctAgg(aggregate);
        }
    }

    private Plan convertToMultiDistinct(LogicalAggregate<? extends Plan> aggregate) {
        return MultiDistinctFunctionStrategy.rewrite(aggregate);
    }

    private Plan splitDistinctAgg(LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateFunction> aggFuncs = aggregate.getAggregateFunctions();
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
        // If there are some functions that cannot be split in other function, AGG cannot be split
        for (AggregateFunction aggFunc : otherFunctions) {
            if (!supportSplitOtherFunctions.contains(aggFunc.getClass())) {
                return null;
            }
        }

        // construct bottom agg
        // group by key: group by key + distinct key
        Set<NamedExpression> groupByKeys = AggregateUtils.getAllKeySet(aggregate);
        ImmutableList.Builder<NamedExpression> bottomAggOtherFunctions = ImmutableList.builder();
        Map<AggregateFunction, NamedExpression> aggFuncToSlot = new HashMap<>();
        for (AggregateFunction aggFunc : otherFunctions) {
            Alias bottomAggFuncAlias = new Alias(aggFunc);
            bottomAggOtherFunctions.add(bottomAggFuncAlias);
            aggFuncToSlot.put(aggFunc, bottomAggFuncAlias.toSlot());
        }

        List<NamedExpression> aggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(groupByKeys)
                .addAll(bottomAggOtherFunctions.build())
                .build();

        LogicalAggregate<Plan> bottomAgg = new LogicalAggregate<>(Utils.fastToImmutableList(groupByKeys),
                aggOutput, aggregate.child());

        // construct top agg
        List<NamedExpression> topAggOutput = ExpressionUtils.rewriteDownShortCircuit(aggregate.getOutputExpressions(),
                expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                                return AggregateUtils.countDistinctMultiExprToCountIf((Count) aggFunc);
                            } else {
                                return aggFunc.withDistinctAndChildren(false, aggFunc.children());
                            }
                        } else {
                            if (aggFuncToSlot.get(aggFunc) != null) {
                                if (aggFunc instanceof Count) {
                                    return new Sum0(aggFuncToSlot.get(aggFunc));
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
        return new LogicalAggregate<Plan>(aggregate.getGroupByExpressions(), topAggOutput, bottomAgg);
    }
}
