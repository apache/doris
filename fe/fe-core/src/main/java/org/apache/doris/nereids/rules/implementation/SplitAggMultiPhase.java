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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.pattern.MatchingContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.ExplorationRuleFactory;
import org.apache.doris.nereids.stats.ExpressionEstimation;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.functions.agg.SupportMultiDistinct;
import org.apache.doris.nereids.trees.expressions.functions.scalar.XxHash32;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.SmallIntType;
import org.apache.doris.nereids.types.StringType;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**SplitAggMultiPhase
 * only process agg with distinct function, split Agg into multi phase
 * */
public class SplitAggMultiPhase extends SplitAggBaseRule implements ExplorationRuleFactory {
    public static final SplitAggMultiPhase INSTANCE = new SplitAggMultiPhase();
    private static final String SALT_EXPR = "saltExpr";

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate()
                        .when(Aggregate::hasDistinctFunc)
                        .when(agg -> !agg.getGroupByExpressions().isEmpty())
                        .thenApplyMulti(this::rewrite)
                        .toRule(RuleType.SPLIT_AGG_MULTI_PHASE)
        );
    }

    /**
     * select count(distinct a) group by b (deduplicated agg hashShuffle by group by key b)
     * splitToTwoPlusOnePhase:
     *   agg(group by b, count(a); distinct global)
     *     +--agg(group by a,b; global)
     *       +--hashShuffle(b)
     *         +--agg(group by a,b; local)
     * splitToOnePlusOnePhase:
     *   agg(group by b, count(a); distinct global)
     *     +--agg(group by a,b; global)
     *       +--hashShuffle(b)
     * splitToOnePlusTwoPhase: (deduplicated agg hashShuffle by distinct key a)
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by a,b; global)
     *         +--hashShuffle(a)
     *
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local)
     *         +--agg(group by a,b; global)
     *           +--hashShuffle(a)
     * splitToTwoPlusTwoPhase:
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local)
     *         +--agg(group by a,b; global)
     *           +--hashShuffle(a,b)
     *             +--agg(group by a,b; local)
     */
    private List<Plan> rewrite(MatchingContext<LogicalAggregate<GroupPlan>> ctx) {
        LogicalAggregate<? extends Plan> aggregate = ctx.root;
        if (aggregate.canSkewRewrite()) {
            return ImmutableList.of(aggSkewRewrite(aggregate, ctx.cascadesContext));
        } else {
            if (shouldUseThreePhase(aggregate)) {
                return ImmutableList.<Plan>builder()
                        .add(splitToTwoPlusOnePhase(aggregate))
                        .add(splitToOnePlusOnePhase(aggregate))
                        .add(splitToOnePlusTwoPhase(aggregate)) //implement 1+2 and 1+1
                        .build();
            } else {
                return ImmutableList.<Plan>builder()
                        .addAll(splitToTwoPlusTwoPhase(aggregate))
                        .add(splitToOnePlusOnePhase(aggregate))
                        .add(splitToOnePlusTwoPhase(aggregate))  //implement 1+2 and 1+1
                        .build();
            }
        }

    }

    private Plan splitToTwoPlusOnePhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan middleAgg = splitDeduplicateTwoPhase(aggregate, middleAggFunctionToAlias,
                aggregate.getGroupByExpressions(), localAggGroupBySet);

        // third phase
        AggregateParam inputToResultParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        return splitDistinctOnePhase(aggregate, inputToResultParam, middleAggFunctionToAlias, middleAgg);
    }

    private Plan splitToOnePlusOnePhase(LogicalAggregate<? extends Plan> aggregate) {
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new HashMap<>();
        Plan localAgg = splitToOnePhase(aggregate, Utils.fastToImmutableList(aggregate.getGroupByExpressions()),
                localAggFunctionToAlias);
        // second phase
        AggregateParam inputToResultParamSecond = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        return splitDistinctOnePhase(aggregate, inputToResultParamSecond, localAggFunctionToAlias, localAgg);
    }

    private Plan splitToOnePhase(LogicalAggregate<? extends Plan> aggregate,
            List<Expression> partitionExpressions, Map<AggregateFunction, Alias> localAggFunctionToAlias) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);
        return splitDeduplicateOnePhase(aggregate, localAggGroupBySet, inputToResultParamFirst,
                paramForAggFunc, localAggFunctionToAlias, aggregate.child(),
                partitionExpressions);
    }

    private List<Plan> splitToTwoPlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
        Plan twoPhaseAgg = splitDeduplicateTwoPhase(aggregate, middleAggFunctionToAlias,
                Utils.fastToImmutableList(localAggGroupBySet), localAggGroupBySet);
        return ImmutableList.<Plan>builder()
                .add(splitDistinctTwoPhase(aggregate, middleAggFunctionToAlias, twoPhaseAgg))
                // .add(splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, onePhaseAgg))
                .build();

        // Map<AggregateFunction, Alias> localAggFunctionToAlias = new HashMap<>();
        // Plan onePhaseAgg = splitToOnePhase(aggregate, Utils.fastToImmutableList(localAggGroupBySet),
        //         localAggFunctionToAlias);

        // Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        // Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        // AggregateParam param = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        // if (AggregateUtils.hasUnknownStatistics(aggregate, aggChildStats)
        //         || AggregateUtils.shouldUseLocalAgg(aggStats, aggChildStats, localAggGroupBySet)) {
        //     return ImmutableList.<Plan>builder()
        //             .add(splitDistinctTwoPhase(aggregate, middleAggFunctionToAlias, twoPhaseAgg))
        //             .add(splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, onePhaseAgg))
        //             .build();
        // } else {
        //     return ImmutableList.<Plan>builder()
        //             .add(splitDistinctOnePhase(aggregate, param, middleAggFunctionToAlias, twoPhaseAgg))
        //             .add(splitDistinctOnePhase(aggregate, param, localAggFunctionToAlias, onePhaseAgg))
        //             .build();
        // }
    }

    private Plan splitToOnePlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        Set<NamedExpression> localAggGroupBySet = getAllKeySet(aggregate);
        // first phase
        AggregateParam paramForAgg = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet, paramForAgg, paramForAggFunc,
                localAggFunctionToAlias, aggregate.child(),
                Utils.fastToImmutableList(aggregate.getDistinctArguments()));
        return splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, localAgg);
        // AggregateParam param = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        // 这个地方用统计信息判断一下保留一阶段还是二阶段,
        // Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        // Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        // if (AggregateUtils.hasUnknownStatistics(aggregate, aggChildStats)
        //         || AggregateUtils.shouldUseLocalAgg(aggStats, aggChildStats, localAggGroupBySet)) {
        //     return splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, localAgg);
        // } else {
        //     return splitDistinctOnePhase(aggregate, param, localAggFunctionToAlias, localAgg);
        // }
    }

    // inputToResultParamSecond这个参数在调用的时候传入的都是一样的，可以考虑删除参数
    private PhysicalHashAggregate<? extends Plan> splitDistinctOnePhase(LogicalAggregate<? extends Plan> aggregate,
            AggregateParam inputToResultParamSecond, Map<AggregateFunction, Alias> childAggFuncMap, Plan child) {
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            // 测试一下为什么需要checkArgument here
                            if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                                Expression countIf = AggregateUtils.countDistinctMultiExprToCountIf((Count) aggFunc);
                                return new AggregateExpression((Count) countIf, inputToResultParamSecond);
                            } else {
                                return new AggregateExpression(
                                        aggFunc.withDistinctAndChildren(false, aggFunc.children()),
                                        inputToResultParamSecond);
                            }
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                                    childAggFuncMap.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                });
        return new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(), globalOutput,
                Optional.ofNullable(aggregate.getGroupByExpressions()), inputToResultParamSecond,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), inputToResultParamSecond),
                aggregate.getLogicalProperties(), null, child);
    }

    private boolean shouldUseThreePhase(Aggregate<? extends Plan> aggregate) {
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


    /**
     * LogicalAggregate(groupByExpr=[a], outputExpr=[a,count(distinct b)])
     * ->
     * +--PhysicalHashAggregate(groupByExpr=[a], outputExpr=[a, sum0(partial_sum0(m))]
     *   +--PhysicalDistribute(shuffleColumn=[a])
     *     +--PhysicalHashAggregate(groupByExpr=[a], outputExpr=[a, partial_sum0(m)]
     *       +--PhysicalHashAggregate(groupByExpr=[a, saltExpr], outputExpr=[a, multi_distinct_count(b) as m])
     *         +--PhysicalDistribute(shuffleColumn=[a, saltExpr])
     *           +--PhysicalProject(projects=[a, b, xxhash_32(b)%512 as saltExpr])
     *             +--PhysicalHashAggregate(groupByExpr=[a, b], outputExpr=[a, b])
     * */
    private PhysicalHashAggregate<Plan> aggSkewRewrite(LogicalAggregate<? extends Plan> logicalAgg,
            CascadesContext cascadesContext) {
        // 1.local agg
        ImmutableList.Builder<Expression> localAggGroupByBuilder = ImmutableList.builderWithExpectedSize(
                logicalAgg.getGroupByExpressions().size() + 1);
        localAggGroupByBuilder.addAll(logicalAgg.getGroupByExpressions());
        AggregateFunction aggFunc = logicalAgg.getAggregateFunctions().iterator().next();
        localAggGroupByBuilder.add(aggFunc.child(0));
        List<Expression> localAggGroupBy = localAggGroupByBuilder.build();
        List<NamedExpression> localAggOutput = Utils.fastToImmutableList((List) localAggGroupBy);
        AggregateParam localParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        boolean maybeUsingStreamAgg = AggregateUtils.maybeUsingStreamAgg(localAggGroupBy, localParam);
        PhysicalHashAggregate<Plan> localAgg = new PhysicalHashAggregate<>(localAggGroupBy, localAggOutput,
                Optional.empty(), localParam, maybeUsingStreamAgg, Optional.empty(), null,
                null, logicalAgg.child());
        // add shuffle expr in project
        ImmutableList.Builder<NamedExpression> projections = ImmutableList.builderWithExpectedSize(
                localAgg.getOutputs().size() + 1);
        projections.addAll(localAgg.getOutputs());
        Alias modAlias = getShuffleExpr(aggFunc, cascadesContext);
        projections.add(modAlias);
        PhysicalProject<Plan> physicalProject = new PhysicalProject<>(projections.build(), null, localAgg);

        // 2.second phase agg: multi_distinct_count(b) group by a,h
        ImmutableList.Builder<Expression> secondPhaseAggGroupByBuilder = ImmutableList.builderWithExpectedSize(
                logicalAgg.getGroupByExpressions().size() + 1);
        secondPhaseAggGroupByBuilder.addAll(logicalAgg.getGroupByExpressions());
        secondPhaseAggGroupByBuilder.add(modAlias.toSlot());
        List<Expression> secondPhaseAggGroupBy = secondPhaseAggGroupByBuilder.build();
        ImmutableList.Builder<NamedExpression> secondPhaseAggOutput = ImmutableList.builderWithExpectedSize(
                secondPhaseAggGroupBy.size() + 1);
        secondPhaseAggOutput.addAll((List) secondPhaseAggGroupBy);
        Alias aliasTarget = new Alias(new TinyIntLiteral((byte) 0));
        for (NamedExpression ne : logicalAgg.getOutputExpressions()) {
            if (ne instanceof Alias) {
                if (((Alias) ne).child().equals(aggFunc)) {
                    aliasTarget = (Alias) ne;
                }
            }
        }
        AggregateParam secondParam = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT, false);
        AggregateFunction multiDistinct = ((SupportMultiDistinct) aggFunc).convertToMultiDistinct();
        Alias multiDistinctAlias = new Alias(new AggregateExpression(multiDistinct, secondParam));
        secondPhaseAggOutput.add(multiDistinctAlias);
        PhysicalHashAggregate<Plan> secondPhaseAgg = new PhysicalHashAggregate<>(
                secondPhaseAggGroupBy, secondPhaseAggOutput.build(),
                Optional.of(secondPhaseAggGroupBy), secondParam, false, Optional.empty(), null,
                null, physicalProject);

        // 3. third phase agg
        List<Expression> thirdPhaseAggGroupBy = Utils.fastToImmutableList(logicalAgg.getGroupByExpressions());
        ImmutableList.Builder<NamedExpression> thirdPhaseAggOutput = ImmutableList.builderWithExpectedSize(
                thirdPhaseAggGroupBy.size() + 1);
        thirdPhaseAggOutput.addAll((List) thirdPhaseAggGroupBy);
        AggregateParam thirdParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER);
        AggregateFunction thirdAggFunc = getAggregateFunction(aggFunc, multiDistinctAlias.toSlot());
        Alias thirdCountAlias = new Alias(new AggregateExpression(thirdAggFunc, thirdParam));
        thirdPhaseAggOutput.add(thirdCountAlias);
        PhysicalHashAggregate<Plan> thirdPhaseAgg = new PhysicalHashAggregate<>(
                thirdPhaseAggGroupBy, thirdPhaseAggOutput.build(),
                Optional.empty(), thirdParam, false, Optional.empty(), null,
                null, secondPhaseAgg);

        // 4. fourth phase agg
        ImmutableList.Builder<NamedExpression> fourthPhaseAggOutput = ImmutableList.builderWithExpectedSize(
                thirdPhaseAggGroupBy.size() + 1);
        fourthPhaseAggOutput.addAll((List) thirdPhaseAggGroupBy);
        AggregateParam fourthParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias sumAliasFour = new Alias(aliasTarget.getExprId(),
                new AggregateExpression(thirdAggFunc, fourthParam, thirdCountAlias.toSlot()),
                aliasTarget.getName());
        DataType t = sumAliasFour.child().getDataType();
        fourthPhaseAggOutput.add(sumAliasFour);
        return new PhysicalHashAggregate<>(thirdPhaseAggGroupBy,
                fourthPhaseAggOutput.build(), Optional.of(logicalAgg.getGroupByExpressions()), fourthParam,
                false, Optional.empty(), logicalAgg.getLogicalProperties(),
                null, thirdPhaseAgg);
    }

    private AggregateFunction getAggregateFunction(AggregateFunction aggFunc, Slot child) {
        if (aggFunc instanceof Count) {
            return new Sum0(false, child);
        } else {
            return aggFunc.withDistinctAndChildren(false, ImmutableList.of(child));
        }
    }

    private Alias getShuffleExpr(AggregateFunction aggFunc, CascadesContext cascadesContext) {
        int bucketNum = cascadesContext.getConnectContext().getSessionVariable().skewRewriteAggBucketNum;
        // divide bucketNum by 2 is because XxHash32 return negative and positive number
        int bucket = bucketNum / 2;
        DataType type = bucket <= 128 ? TinyIntType.INSTANCE : SmallIntType.INSTANCE;
        Mod mod = new Mod(new XxHash32(TypeCoercionUtils.castIfNotSameType(
                aggFunc.child(0), StringType.INSTANCE)), new IntegerLiteral((short) bucket));
        Cast cast = new Cast(mod, type);
        return new Alias(cast, SALT_EXPR + cascadesContext.getStatementContext().generateColumnName());
    }
}
