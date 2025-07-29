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

package org.apache.doris.nereids.rules.exploration;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**SplitAggMultiPhase
 * only process agg with distinct function, split Agg into multi phase
 * */
public class SplitAggMultiPhase extends SplitAggRule implements ExplorationRuleFactory {
    public static final SplitAggMultiPhase INSTANCE = new SplitAggMultiPhase();

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate()
                        .when(agg -> agg.getAggregateParam().needSplit)
                        .when(Aggregate::hasDistinctFunc)
                        .when(agg -> !agg.getGroupByExpressions().isEmpty())
                        .thenApplyMulti(ctx -> rewrite(ctx.root))
                        .toRule(RuleType.SPLIT_AGG_MULTI_PHASE)
        );
    }

    private List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate) {
        if (shouldUseThreePhase(aggregate)) {
            return ImmutableList.<Plan>builder()
                    // .add(splitToTwoPlusOnePhase(aggregate))
                    // .add(splitToOnePlusOnePhase(aggregate))
                    .add(splitToOnePlusTwoPhase(aggregate))
                    .build();
        } else {
            return ImmutableList.<Plan>builder()
                    // .add(splitToOnePlusOnePhase(aggregate))
                    // .add(splitToTwoPlusTwoPhase(aggregate))
                    .add(splitToOnePlusTwoPhase(aggregate))
                    .build();
        }
    }

    private Plan splitToTwoPlusOnePhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan middleAgg = splitDeduplicateTwoPhase(aggregate, middleAggFunctionToAlias,
                aggregate.getGroupByExpressions(), localAggGroupBySet);

        // third phase
        AggregateParam inputToResultParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT,
                false);
        return splitDistinctOnePhase(aggregate, inputToResultParam, middleAggFunctionToAlias, middleAgg);
    }

    private Plan splitToOnePlusOnePhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER, false);
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet, inputToResultParamFirst,
                localAggFunctionToAlias, aggregate.child(),
                Utils.fastToImmutableList(aggregate.getDistinctArguments()));

        // second phase
        AggregateParam inputToResultParamSecond = new AggregateParam(AggPhase.DISTINCT_GLOBAL,
                AggMode.INPUT_TO_RESULT, false);
        return splitDistinctOnePhase(aggregate, inputToResultParamSecond, localAggFunctionToAlias, localAgg);
    }

    private Plan splitToTwoPlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan middleAgg = splitDeduplicateTwoPhase(aggregate, middleAggFunctionToAlias,
                Utils.fastToImmutableList(localAggGroupBySet), localAggGroupBySet);

        return splitDistinctTwoPhase(aggregate, middleAggFunctionToAlias, middleAgg);
    }

    private Plan splitToOnePlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER, false);
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet, inputToResultParamFirst,
                localAggFunctionToAlias, aggregate.child(),
                Utils.fastToImmutableList(aggregate.getDistinctArguments()));
        return splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, localAgg);
    }

    private Plan splitDistinctOnePhase(LogicalAggregate<? extends Plan> aggregate,
            AggregateParam inputToResultParamSecond, Map<AggregateFunction, Alias> childAggFuncMap, Plan child) {
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            // 测试一下为什么需要checkArgument here
                            return new AggregateExpression(
                                    aggFunc.withDistinctAndChildren(false, aggFunc.children()),
                                    inputToResultParamSecond);
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                                    childAggFuncMap.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                });
        return aggregate.withAggParam(globalOutput, aggregate.getGroupByExpressions(),
                inputToResultParamSecond, aggregate.getLogicalProperties(),
                aggregate.getGroupByExpressions(), child);
    }

    private boolean shouldUseThreePhase(LogicalAggregate<? extends Plan> aggregate) {
        Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        for (Expression groupByExpr : aggregate.getGroupByExpressions()) {
            ColumnStatistic columnStat = aggChildStats.findColumnStatistics(groupByExpr);
            if (columnStat == null) {
                columnStat = ExpressionEstimation.estimate(groupByExpr, aggChildStats);
            }
            if (columnStat.isUnKnown) {
                return true;
            }
        }
        double ndv = aggStats.getRowCount();
        // 当ndv非常低的情况下,不能使用三阶段AGG,会有倾斜
        if (ndv < 1000) {
            return false;
        }
        return true;
    }

}
