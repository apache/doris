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
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
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
import org.apache.doris.qe.ConnectContext;
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
public class SplitAggMultiPhase extends SplitAggBaseRule implements ImplementationRuleFactory {
    public static final SplitAggMultiPhase INSTANCE = new SplitAggMultiPhase();
    private static final String SALT_EXPR = "saltExpr";

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                logicalAggregate()
                        .when(agg -> !agg.getGroupByExpressions().isEmpty())
                        .when(agg -> agg.getDistinctArguments().size() == 1 || agg.distinctFuncNum() == 1)
                        .thenApplyMulti(this::rewrite)
                        .toRule(RuleType.SPLIT_AGG_MULTI_PHASE)
        );
    }

    private List<Plan> rewrite(MatchingContext<LogicalAggregate<GroupPlan>> ctx) {
        LogicalAggregate<? extends Plan> aggregate = ctx.root;
        if (aggregate.canSkewRewrite()) {
            return ImmutableList.of(aggSkewRewrite(aggregate, ctx.cascadesContext));
        } else {
            if (twoPlusOneBetterThanTwoPlusTwo(aggregate)) {
                return ImmutableList.<Plan>builder()
                        .addAll(splitToTwoPlusOnePhase(aggregate))
                        .addAll(splitToOnePlusTwoPhase(aggregate))
                        .build();
            } else {
                return ImmutableList.<Plan>builder()
                        .addAll(splitToTwoPlusTwoPhase(aggregate))
                        .build();
            }
        }
    }

    /**
     * select count(distinct a) group by b (deduplicated agg hashShuffle by group by key b)
     * splitToTwoPlusOnePhase:
     *   agg(group by b, count(a); distinct global)
     *     +--agg(group by a,b; global)
     *       +--hashShuffle(b)
     *         +--agg(group by a,b; local)
     *   agg(group by b, count(a); distinct global)
     *     +--agg(group by a,b; global)
     *       +--hashShuffle(b)
     */
    private List<Plan> splitToTwoPlusOnePhase(LogicalAggregate<? extends Plan> aggregate) {
        ImmutableList.Builder<Plan> builder = ImmutableList.builder();
        if (aggregate.supportAggregatePhase(AggregatePhase.THREE)) {
            Set<NamedExpression> localAggGroupBySet = AggregateUtils.getAllKeySet(aggregate);
            Map<AggregateFunction, Alias> middleAggFuncToAlias = new LinkedHashMap<>();
            Plan middleAgg = splitDeduplicateTwoPhase(aggregate, middleAggFuncToAlias,
                    aggregate.getGroupByExpressions(), localAggGroupBySet);
            builder.add(splitDistinctOnePhase(aggregate, middleAggFuncToAlias, middleAgg));
        }
        if (aggregate.supportAggregatePhase(AggregatePhase.TWO)) {
            Map<AggregateFunction, Alias> localAggFuncToAlias = new HashMap<>();
            Plan localAgg = splitToOnePhase(aggregate, Utils.fastToImmutableList(aggregate.getGroupByExpressions()),
                    localAggFuncToAlias);
            builder.add(splitDistinctOnePhase(aggregate, localAggFuncToAlias, localAgg));
        }
        return builder.build();
    }

    private Plan splitToOnePhase(LogicalAggregate<? extends Plan> aggregate,
            List<Expression> partitionExpressions, Map<AggregateFunction, Alias> localAggFunctionToAlias) {
        Set<NamedExpression> localAggGroupBySet = AggregateUtils.getAllKeySet(aggregate);
        // first phase
        AggregateParam inputToResultParamFirst = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);
        return splitDeduplicateOnePhase(aggregate, localAggGroupBySet, inputToResultParamFirst,
                paramForAggFunc, localAggFunctionToAlias, aggregate.child(),
                partitionExpressions);
    }

    /**
     * select count(distinct a) group by b (deduplicated agg hashShuffle by group by key b)
     * splitToTwoPlusTwoPhase:
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local)
     *         +--agg(group by a,b; global)
     *           +--hashShuffle(a,b)
     *             +--agg(group by a,b; local)
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local)
     *         +--agg(group by a,b; global)
     *           +--hashShuffle(a,b)
     */
    private List<Plan> splitToTwoPlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        ImmutableList.Builder<Plan> builder = ImmutableList.builder();
        Set<NamedExpression> localAggGroupBySet = AggregateUtils.getAllKeySet(aggregate);
        if (aggregate.supportAggregatePhase(AggregatePhase.FOUR)) {
            Map<AggregateFunction, Alias> middleAggFunctionToAlias = new LinkedHashMap<>();
            Plan twoPhaseAgg = splitDeduplicateTwoPhase(aggregate, middleAggFunctionToAlias,
                    Utils.fastToImmutableList(localAggGroupBySet), localAggGroupBySet);
            builder.add(splitDistinctTwoPhase(aggregate, middleAggFunctionToAlias, twoPhaseAgg));
        }
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new HashMap<>();
        Plan onePhaseAgg = splitToOnePhase(aggregate, Utils.fastToImmutableList(localAggGroupBySet),
                localAggFunctionToAlias);
        if (aggregate.supportAggregatePhase(AggregatePhase.THREE)) {
            builder.add(splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, onePhaseAgg));
        }
        if (aggregate.supportAggregatePhase(AggregatePhase.TWO)) {
            builder.add(splitDistinctOnePhase(aggregate, localAggFunctionToAlias, onePhaseAgg));
        }
        return builder.build();
    }

    /**
     * select count(distinct a) group by b (deduplicated agg hashShuffle by group by key b)
     * splitToOnePlusTwoPhase: (deduplicated agg hashShuffle by distinct key a)
     *   agg(group by b, count(a); distinct global)
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local)
     *         +--agg(group by a,b; global)
     *           +--hashShuffle(a)
     */
    private List<Plan> splitToOnePlusTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.supportAggregatePhase(AggregatePhase.THREE)) {
            return ImmutableList.of();
        }
        Set<NamedExpression> localAggGroupBySet = AggregateUtils.getAllKeySet(aggregate);
        // first phase
        AggregateParam paramForAgg = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT);
        AggregateParam paramForAggFunc = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_BUFFER);

        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        Plan localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet, paramForAgg, paramForAggFunc,
                localAggFunctionToAlias, aggregate.child(),
                Utils.fastToImmutableList(aggregate.getDistinctArguments()));
        return ImmutableList.of(splitDistinctTwoPhase(aggregate, localAggFunctionToAlias, localAgg));
    }

    private PhysicalHashAggregate<? extends Plan> splitDistinctOnePhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> childAggFuncMap, Plan child) {
        AggregateParam inputToResultParamSecond = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.INPUT_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            // TODO: add test about aggFun arity
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
                aggregate.getLogicalProperties(), child);
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
    private static PhysicalHashAggregate<Plan> aggSkewRewrite(LogicalAggregate<? extends Plan> logicalAgg,
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
                Optional.empty(), localParam, maybeUsingStreamAgg,
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
                Optional.of(secondPhaseAggGroupBy), secondParam, false,
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
                Optional.empty(), thirdParam, false,
                null, secondPhaseAgg);

        // 4. fourth phase agg
        ImmutableList.Builder<NamedExpression> fourthPhaseAggOutput = ImmutableList.builderWithExpectedSize(
                thirdPhaseAggGroupBy.size() + 1);
        fourthPhaseAggOutput.addAll((List) thirdPhaseAggGroupBy);
        AggregateParam fourthParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        Alias sumAliasFour = new Alias(aliasTarget.getExprId(),
                new AggregateExpression(thirdAggFunc, fourthParam, thirdCountAlias.toSlot()),
                aliasTarget.getName());
        fourthPhaseAggOutput.add(sumAliasFour);
        return new PhysicalHashAggregate<>(thirdPhaseAggGroupBy,
                fourthPhaseAggOutput.build(), Optional.of(logicalAgg.getGroupByExpressions()), fourthParam,
                false, logicalAgg.getLogicalProperties(), thirdPhaseAgg);
    }

    private static AggregateFunction getAggregateFunction(AggregateFunction aggFunc, Slot child) {
        if (aggFunc instanceof Count) {
            return new Sum0(false, child);
        } else {
            return aggFunc.withDistinctAndChildren(false, ImmutableList.of(child));
        }
    }

    private static Alias getShuffleExpr(AggregateFunction aggFunc, CascadesContext cascadesContext) {
        int bucketNum = cascadesContext.getConnectContext().getSessionVariable().skewRewriteAggBucketNum;
        // divide bucketNum by 2 is because XxHash32 return negative and positive number
        int bucket = bucketNum / 2;
        DataType type = bucket <= 128 ? TinyIntType.INSTANCE : SmallIntType.INSTANCE;
        Mod mod = new Mod(new XxHash32(TypeCoercionUtils.castIfNotSameType(
                aggFunc.child(0), StringType.INSTANCE)), new IntegerLiteral((short) bucket));
        Cast cast = new Cast(mod, type);
        return new Alias(cast, SALT_EXPR + cascadesContext.getStatementContext().generateColumnName());
    }

    /**twoPlusOneBetterThanTwoPlusTwo*/
    public boolean twoPlusOneBetterThanTwoPlusTwo(LogicalAggregate<? extends Plan> aggregate) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return true;
        }
        switch (ctx.getSessionVariable().aggPhase) {
            case 3:
                return true;
            case 4:
                return false;
            default:
                break;
        }
        // select count(distinct a) from t group by a; should use twoPlusOne
        Set<NamedExpression> groupBySet = AggregateUtils.getGroupBySetNamedExpr(aggregate);
        Set<NamedExpression> distinctSet = AggregateUtils.getDistinctNamedExpr(aggregate);
        if (groupBySet.containsAll(distinctSet)) {
            return true;
        }
        Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        if (aggStats == null || aggChildStats == null) {
            return true;
        }
        if (AggregateUtils.hasUnknownStatistics(aggregate.getGroupByExpressions(), aggChildStats)) {
            return true;
        }
        double ndv = aggStats.getRowCount();
        // When ndv is very low, three-stage AGG cannot be used and there will be data skew
        return ndv > AggregateUtils.LOW_NDV_THRESHOLD;
    }
}
