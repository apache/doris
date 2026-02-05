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
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
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
import org.apache.doris.qe.ConnectContext;

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
public class SplitAggMultiPhaseWithoutGbyKey extends SplitAggBaseRule implements ImplementationRuleFactory {
    public static final SplitAggMultiPhaseWithoutGbyKey INSTANCE = new SplitAggMultiPhaseWithoutGbyKey();
    public static final List<Class<? extends AggregateFunction>> finalMultiDistinctSupportFunc =
            ImmutableList.of(Count.class, Sum.class, Sum0.class);
    public static final List<Class<? extends AggregateFunction>> finalMultiDistinctSupportOtherFunc =
            ImmutableList.of(Count.class, Sum.class, Min.class, Max.class, Sum0.class, AnyValue.class);

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            logicalAggregate()
                    .when(agg -> agg.getGroupByExpressions().isEmpty())
                    .when(agg -> agg.getDistinctArguments().size() == 1 || agg.distinctFuncNum() == 1)
                    .thenApplyMulti(ctx -> rewrite(ctx.root))
                    .toRule(RuleType.SPLIT_AGG_MULTI_PHASE_WITHOUT_GBY_KEY)
        );
    }

    List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate) {
        if (canUseFinalMultiDistinct(aggregate)) {
            return ImmutableList.<Plan>builder()
                    .addAll(twoPhaseAggregateWithFinalMultiDistinct(aggregate))
                    .addAll(splitToFourPhase(aggregate))
                    .build();
        } else {
            return ImmutableList.<Plan>builder()
                    .addAll(splitToThreePhase(aggregate))
                    .addAll(splitToFourPhase(aggregate))
                    .build();
        }
    }

    /**
     * select count(distinct a) from t
     * splitToFourPhase:
     * agg(count(a); agg_phase: distinct_global)
     *   +--gather
     *     +--agg(count(a); agg_phase: distinct_local)
     *       +--agg(group by a; agg_phase: global)
     *         +--hashShuffle(a)
     *           +--agg(group by a; agg_phase: local)
     */
    private List<Plan> splitToFourPhase(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.supportAggregatePhase(AggregatePhase.FOUR)) {
            return ImmutableList.of();
        }
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        Plan secondAgg = splitDeduplicateTwoPhase(aggregate, localAggFuncToAlias,
                Utils.fastToImmutableList(aggregate.getDistinctArguments()), AggregateUtils.getAllKeySet(aggregate));
        return ImmutableList.of(splitDistinctTwoPhase(aggregate, localAggFuncToAlias, secondAgg));
    }

    /**
     * select count(distinct a) from t
     * splitToThreePhase:
     * agg(count(a); agg_phase: distinct_global)
     *   +--gather
     *     +--agg(count(a); agg_phase: distinct_local)
     *       +--agg(group by a; agg_phase: global)
     *         +--hashShuffle(a)
     */
    private List<Plan> splitToThreePhase(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.supportAggregatePhase(AggregatePhase.THREE)) {
            return ImmutableList.of();
        }
        AggregateParam inputToResult = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> localAggFuncToAlias = new LinkedHashMap<>();
        Set<NamedExpression> keySet = AggregateUtils.getAllKeySet(aggregate);
        Plan localAgg = splitDeduplicateOnePhase(aggregate, keySet, inputToResult,
                paramForAggFunc, localAggFuncToAlias, aggregate.child(), Utils.fastToImmutableList(keySet));
        return ImmutableList.of(splitDistinctTwoPhase(aggregate, localAggFuncToAlias, localAgg));
    }

    /**
     * select count(distinct a) from t
     * twoPhaseAggregateWithFinalMultiDistinct:
     * agg(sum0(c1), agg_phase: final)
     *   +--gather
     *     +--agg(multi_distinct_count(a) as c1, agg_phase: final)
     *       +--hashShuffle(a)
     * */
    private List<Plan> twoPhaseAggregateWithFinalMultiDistinct(
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
                null, logicalAgg.child());

        AggregateParam param = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT, false);

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
                                        // TODO: now we don't support group_concat,
                                        // we need add support for group_concat without order by,
                                        // and add test for group_concat
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
                                        AnyValue.class, () -> new AnyValue(childSlot),
                                        // TODO: now we don't support group_concat,
                                        // we need add support for group_concat without order by,
                                        // and add test for group_concat
                                        GroupConcat.class, () -> new GroupConcat(childSlot));
                            return new AggregateExpression(functionMap.get(aggFunc.getClass()).get(), param, childSlot);
                        }
                    } else {
                        return outputChild;
                    }
                });
        return ImmutableList.of(new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), globalOutput, param,
                AggregateUtils.maybeUsingStreamAgg(logicalAgg.getGroupByExpressions(), param),
                logicalAgg.getLogicalProperties(), anyLocalAgg));
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
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && (ctx.getSessionVariable().aggPhase == 3 || ctx.getSessionVariable().aggPhase == 4)) {
            return false;
        }
        return true;
    }
}
