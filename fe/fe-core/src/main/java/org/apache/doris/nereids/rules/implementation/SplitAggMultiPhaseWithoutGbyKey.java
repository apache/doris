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

package org.apache.doris.nereids.rules.implementation;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.GroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctCount;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctGroupConcat;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinctSum0;
import org.apache.doris.nereids.trees.expressions.functions.agg.MultiDistinction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**SplitAggMultiPhaseWithoutGbyKey*/
public class SplitAggMultiPhaseWithoutGbyKey extends SplitAggRule implements ExplorationRuleFactory {
    public static final SplitAggMultiPhaseWithoutGbyKey INSTANCE = new SplitAggMultiPhaseWithoutGbyKey();
    public static final List<Class<? extends AggregateFunction>> finalMultiDistinctSupportFunc =
            ImmutableList.of(Count.class, Sum.class, Sum0.class);
    public static final List<Class<? extends AggregateFunction>> finalMultiDistinctSupportOtherFunc =
            ImmutableList.of(Count.class, Sum.class, Min.class, Max.class, Sum0.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalAggregate()
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .when(Aggregate::hasDistinctFunc)
                    .thenApplyMulti(ctx -> rewrite(ctx.root))
                    .toRule(RuleType.SPLIT_AGG_MULTI_PHASE_WITHOUT_GBY_KEY)
        );
    }

    /**
     * select count(distinct a) from t
     * splitToThreePhase:
     * agg(count(a))
     *   +--gather
     *     +--agg(count(a))
     *       +--agg(group by a)
     *         +--hashShuffle(a)
     * splitToFourPhase:
     * agg(count(a))
     *   +--gather
     *     +--agg(count(a))
     *       +--agg(group by a)
     *         +--hashShuffle(a)
     *           +--agg(group by a)
     * twoPhaseAggregateWithFinalMultiDistinct:
     * agg(sum0(c1))
     *   +--gather
     *     +--agg(multi_distinct(a) as c1)
     *       +--hashShuffle(a)
     * */
    List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate) {
        // 这里还要再加上限制不能有其他的不带distinct的聚合函数,
        if (canUseFinalMultiDistinct(aggregate)) {
            // 为啥我这里twoPhaseAggregateWithFinalMultiDistinct和splitToThreePhase都要实现一下？我有点忘了
            return ImmutableList.of(
                    twoPhaseAggregateWithFinalMultiDistinct(aggregate),
                    splitToThreePhase(aggregate),
                    splitToFourPhase(aggregate)
            );
        } else {
            return ImmutableList.of(
                    splitToThreePhase(aggregate),
                    splitToFourPhase(aggregate)
            );
        }
    }

    Plan splitToFourPhase(LogicalAggregate<? extends Plan> aggregate) {
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        Plan secondAgg = splitDeduplicateTwoPhase(aggregate, localAggFuncToAlias,
                Utils.fastToImmutableList(aggregate.getDistinctArguments()), (Set) aggregate.getDistinctArguments());
        return splitDistinctTwoPhase(aggregate, localAggFuncToAlias, secondAgg);
    }

    Plan splitToThreePhase(LogicalAggregate<? extends Plan> aggregate) {
        AggregateParam inputToResult = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        Set<NamedExpression> keySet = getAllKeySet(aggregate);
        Plan localAgg = splitDeduplicateOnePhase(aggregate, keySet, inputToResult,
                paramForAggFunc, localAggFuncToAlias, aggregate.child(), Utils.fastToImmutableList(keySet));
        return splitDistinctTwoPhase(aggregate, localAggFuncToAlias, localAgg);
    }

    // 如果是sum/count等场景,可以使用
    private PhysicalHashAggregate<? extends Plan> twoPhaseAggregateWithFinalMultiDistinct(
            LogicalAggregate<? extends Plan> logicalAgg) {
        Set<AggregateFunction> aggregateFunctions = logicalAgg.getAggregateFunctions();

        AggregateParam inputToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);

        Map<AggregateFunction, Alias> originFuncToAliasPhase1 = new HashMap<>();
        for (AggregateFunction function : aggregateFunctions) {
            AggregateFunction aggFunc = AggregateUtils.tryConvertToMultiDistinct(function);
            AggregateExpression localAggExpr = new AggregateExpression(aggFunc, inputToResultParam);
            originFuncToAliasPhase1.put(function, new Alias(localAggExpr));
        }

        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(originFuncToAliasPhase1.values())
                .build();
        Plan anyLocalAgg = new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), localAggOutput,
                Optional.of(Utils.fastToImmutableList(logicalAgg.getDistinctArguments())), inputToResultParam,
                AggregateUtils.maybeUsingStreamAgg(logicalAgg.getGroupByExpressions(), inputToResultParam),
                null, null, logicalAgg.child());

        AggregateParam param = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT, false);
        // 如果是普通聚合函数，那么就正常处理
        // 如果是distinct聚合函数，count_distinct -> 上层变成sum0; sum_distinct -> 上层还是sum;
        // group_concat_distinct -> 上层还是group_concat 。
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                logicalAgg.getOutputExpressions(), outputChild -> {
                    if (outputChild instanceof AggregateFunction) {
                        Alias alias = originFuncToAliasPhase1.get(outputChild);
                        AggregateExpression localAggExpr = (AggregateExpression) alias.child();
                        AggregateFunction aggFunc = localAggExpr.getFunction();
                        Slot childSlot = alias.toSlot();
                        if (aggFunc instanceof MultiDistinction) {
                            Map<Class<? extends AggregateFunction>, Supplier<AggregateFunction>> functionMap =
                                    ImmutableMap.of(
                                        MultiDistinctCount.class, () -> new Sum0(childSlot),
                                        MultiDistinctSum.class, () -> new Sum(childSlot),
                                        MultiDistinctSum0.class, () -> new Sum0(childSlot),
                                        MultiDistinctGroupConcat.class, () -> new GroupConcat(childSlot));
                            return new AggregateExpression(functionMap.get(aggFunc.getClass()).get(), param);
                        } else {
                            Map<Class<? extends AggregateFunction>, Supplier<AggregateFunction>> functionMap =
                                    ImmutableMap.of(
                                        Count.class, () -> new Sum0(childSlot),
                                        Sum.class, () -> new Sum(childSlot),
                                        Sum0.class, () -> new Sum0(childSlot),
                                        Min.class, () -> new Min(childSlot),
                                        Max.class, () -> new Max(childSlot),
                                        GroupConcat.class, () -> new GroupConcat(childSlot));
                            return new AggregateExpression(functionMap.get(aggFunc.getClass()).get(), param, childSlot);
                        }
                    } else {
                        return outputChild;
                    }
                });
        return new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), globalOutput, param,
                AggregateUtils.maybeUsingStreamAgg(logicalAgg.getGroupByExpressions(), param),
                logicalAgg.getLogicalProperties(), null, anyLocalAgg);
    }

    private boolean canUseFinalMultiDistinct(Aggregate<? extends Plan> agg) {
        for (AggregateFunction aggFunc : agg.getAggregateFunctions()) {
            if (aggFunc.isDistinct()) {
                if (!finalMultiDistinctSupportFunc.contains(aggFunc.getClass())) {
                    return false;
                }
                if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                    return false;
                }
            } else {
                if (!finalMultiDistinctSupportOtherFunc.contains(aggFunc.getClass())) {
                    return false;
                }
            }
        }
        return true;
    }
}
