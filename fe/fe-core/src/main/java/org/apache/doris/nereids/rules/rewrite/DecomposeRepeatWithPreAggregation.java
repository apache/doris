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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.rules.rewrite.DistinctAggStrategySelector.DistinctSelectorContext;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum0;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation.Qualifier;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEAnchor;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.Statistics;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * This rule will rewrite grouping sets. eg:
 *      select a, b, c, d, e sum(f) from t group by rollup(a, b, c, d, e);
 * rewrite to:
 *    with cte1 as (select a, b, c, d, e, sum(f) x from t group by rollup(a, b, c, d, e))
 *      select * fom cte1
 *      union all
 *      select a, b, c, d, null, sum(x) x from t group by rollup(a, b, c, d)
 *
 * LogicalAggregate(gby: a,b,c,d,e,grouping_id output:a,b,c,d,e,grouping_id,sum(f))
 *   +--LogicalRepeat(grouping sets: (a,b,c,d,e),(a,b,c,d),(a,b,c),(a,b),(a),())
 * ->
 * LogicalCTEAnchor
 *   +--LogicalCTEProducer(cte)
 *     +--LogicalAggregate(gby: a,b,c,d,e; aggFunc: sum(f) as x)
 *   +--LogicalUnionAll
 *     +--LogicalProject(a,b,c,d, null as e, sum(x))
 *       +--LogicalAggregate(gby:a,b,c,d,grouping_id; aggFunc: sum(x))
 *         +--LogicalRepeat(grouping sets: (a,b,c,d),(a,b,c),(a,b),(a),())
 *           +--LogicalCTEConsumer(aggregateConsumer)
 *     +--LogicalCTEConsumer(directConsumer)
 */
public class DecomposeRepeatWithPreAggregation extends DefaultPlanRewriter<DistinctSelectorContext>
        implements CustomRewriter {
    public static final DecomposeRepeatWithPreAggregation INSTANCE = new DecomposeRepeatWithPreAggregation();
    private static final Set<Class<? extends AggregateFunction>> SUPPORT_AGG_FUNCTIONS =
            ImmutableSet.of(Sum.class, Sum0.class, Min.class, Max.class, AnyValue.class, Count.class);

    @Override
    public Plan rewriteRoot(Plan plan, JobContext jobContext) {
        DistinctSelectorContext ctx = new DistinctSelectorContext(jobContext.getCascadesContext().getStatementContext(),
                jobContext.getCascadesContext());
        plan = plan.accept(this, ctx);
        for (int i = ctx.cteProducerList.size() - 1; i >= 0; i--) {
            LogicalCTEProducer<? extends Plan> producer = ctx.cteProducerList.get(i);
            plan = new LogicalCTEAnchor<>(producer.getCteId(), producer, plan);
        }
        return plan;
    }

    @Override
    public Plan visitLogicalCTEAnchor(
            LogicalCTEAnchor<? extends Plan, ? extends Plan> anchor, DistinctSelectorContext ctx) {
        Plan child1 = anchor.child(0).accept(this, ctx);
        DistinctSelectorContext consumerContext =
                new DistinctSelectorContext(ctx.statementContext, ctx.cascadesContext);
        Plan child2 = anchor.child(1).accept(this, consumerContext);
        for (int i = consumerContext.cteProducerList.size() - 1; i >= 0; i--) {
            LogicalCTEProducer<? extends Plan> producer = consumerContext.cteProducerList.get(i);
            child2 = new LogicalCTEAnchor<>(producer.getCteId(), producer, child2);
        }
        return anchor.withChildren(ImmutableList.of(child1, child2));
    }

    @Override
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, DistinctSelectorContext ctx) {
        aggregate = visitChildren(this, aggregate, ctx);
        int maxGroupIndex = canOptimize(aggregate, ctx.cascadesContext.getConnectContext());
        if (maxGroupIndex < 0) {
            return aggregate;
        }
        Map<Slot, Slot> preToProducerSlotMap = new HashMap<>();
        LogicalCTEProducer<LogicalAggregate<Plan>> producer = constructProducer(aggregate, maxGroupIndex, ctx,
                preToProducerSlotMap, ctx.cascadesContext.getConnectContext());
        LogicalCTEConsumer aggregateConsumer = new LogicalCTEConsumer(ctx.statementContext.getNextRelationId(),
                producer.getCteId(), "", producer);
        LogicalCTEConsumer directConsumer = new LogicalCTEConsumer(ctx.statementContext.getNextRelationId(),
                producer.getCteId(), "", producer);

        // build map : origin slot to consumer slot
        Map<Slot, Slot> producerToConsumerMap = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : aggregateConsumer.getProducerToConsumerOutputMap().entries()) {
            producerToConsumerMap.put(entry.getKey(), entry.getValue());
        }
        Map<Slot, Slot> originToConsumerMap = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : preToProducerSlotMap.entrySet()) {
            originToConsumerMap.put(entry.getKey(), producerToConsumerMap.get(entry.getValue()));
        }

        LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) aggregate.child();
        List<List<Expression>> newGroupingSets = new ArrayList<>();
        for (int i = 0; i < repeat.getGroupingSets().size(); ++i) {
            if (i == maxGroupIndex) {
                continue;
            }
            newGroupingSets.add(repeat.getGroupingSets().get(i));
        }
        List<NamedExpression> groupingFunctionSlots = new ArrayList<>();
        LogicalRepeat<Plan> newRepeat = constructRepeat(repeat, aggregateConsumer, newGroupingSets,
                originToConsumerMap, groupingFunctionSlots);
        Set<Expression> needRemovedExprSet = getNeedAddNullExpressions(repeat, newGroupingSets, maxGroupIndex);
        Map<AggregateFunction, Slot> aggFuncToSlot = new HashMap<>();
        LogicalAggregate<Plan> topAgg = constructAgg(aggregate, originToConsumerMap, newRepeat, groupingFunctionSlots,
                aggFuncToSlot);
        LogicalProject<Plan> project = constructProject(aggregate, originToConsumerMap, needRemovedExprSet,
                groupingFunctionSlots, topAgg, aggFuncToSlot);
        LogicalPlan directChild = getDirectChild(directConsumer, groupingFunctionSlots);
        return constructUnion(project, directChild, aggregate);
    }

    /**
     * Get the direct child plan for the union operation.
     * If there are grouping function slots, wrap the consumer with a project that adds
     * zero literals for each grouping function slot to match the output schema.
     *
     * @param directConsumer the CTE consumer for the direct path
     * @param groupingFunctionSlots the list of grouping function slots to handle
     * @return the direct child plan, possibly wrapped with a project
     */
    private LogicalPlan getDirectChild(LogicalCTEConsumer directConsumer, List<NamedExpression> groupingFunctionSlots) {
        LogicalPlan directChild = directConsumer;
        if (!groupingFunctionSlots.isEmpty()) {
            ImmutableList.Builder<NamedExpression> builder = ImmutableList.builder();
            builder.addAll(directConsumer.getOutput());
            for (int i = 0; i < groupingFunctionSlots.size(); ++i) {
                builder.add(new Alias(new BigIntLiteral(0)));
            }
            directChild = new LogicalProject<Plan>(builder.build(), directConsumer);
        }
        return directChild;
    }

    /**
     * Build a map from aggregate function to its corresponding slot.
     *
     * @param outputExpressions the output expressions containing aggregate functions
     * @param pToc the map from producer slot to consumer slot
     * @return a map from aggregate function to its corresponding slot in consumer outputs
     */
    private Map<AggregateFunction, Slot> getAggFuncSlotMap(List<NamedExpression> outputExpressions,
            Map<Slot, Slot> pToc) {
        // build map : aggFunc to Slot
        Map<AggregateFunction, Slot> aggFuncSlotMap = new HashMap<>();
        for (NamedExpression expr : outputExpressions) {
            if (expr instanceof Alias) {
                Optional<Expression> aggFunc = expr.child(0).collectFirst(e -> e instanceof AggregateFunction);
                aggFunc.ifPresent(
                        func -> aggFuncSlotMap.put((AggregateFunction) func, pToc.get(expr.toSlot())));
            }
        }
        return aggFuncSlotMap;
    }

    /**
     * Get the set of expressions that need to be replaced with null in the new grouping sets.
     * These are expressions that exist in the maximum grouping set but not in other grouping sets.
     *
     * @param repeat the original LogicalRepeat plan
     * @param newGroupingSets the new grouping sets after removing the maximum grouping set
     * @param maxGroupIndex the index of the maximum grouping set
     * @return the set of expressions that need to be replaced with null
     */
    private Set<Expression> getNeedAddNullExpressions(LogicalRepeat<Plan> repeat,
            List<List<Expression>> newGroupingSets, int maxGroupIndex) {
        Set<Expression> otherGroupExprSet = new HashSet<>();
        for (List<Expression> groupingSet : newGroupingSets) {
            otherGroupExprSet.addAll(groupingSet);
        }
        List<Expression> maxGroupByList = repeat.getGroupingSets().get(maxGroupIndex);
        Set<Expression> needRemovedExprSet = new HashSet<>(maxGroupByList);
        needRemovedExprSet.removeAll(otherGroupExprSet);
        return needRemovedExprSet;
    }

    /**
     * Construct a LogicalAggregate for the decomposed repeat.
     *
     * @param aggregate the original aggregate plan
     * @param originToConsumerMap the map from original slots to consumer slots
     * @param newRepeat the new LogicalRepeat plan with reduced grouping sets
     * @param groupingFunctionSlots the list of new grouping function slots
     * @param aggFuncToSlot output parameter: map from original aggregate functions to their slots in the new aggregate
     * @return a LogicalAggregate for the decomposed repeat
     */
    private LogicalAggregate<Plan> constructAgg(LogicalAggregate<? extends Plan> aggregate,
            Map<Slot, Slot> originToConsumerMap, LogicalRepeat<Plan> newRepeat,
            List<NamedExpression> groupingFunctionSlots, Map<AggregateFunction, Slot> aggFuncToSlot) {
        Map<AggregateFunction, Slot> aggFuncSlotMap = getAggFuncSlotMap(aggregate.getOutputExpressions(),
                originToConsumerMap);
        Set<Slot> groupingSetsUsedSlot = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions((List) newRepeat.getGroupingSets()));
        List<Expression> topAggGby = new ArrayList<>(groupingSetsUsedSlot);
        topAggGby.add(newRepeat.getGroupingId().get());
        topAggGby.addAll(groupingFunctionSlots);
        List<NamedExpression> topAggOutput = new ArrayList<>((List) topAggGby);
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            if (expr instanceof Alias && expr.containsType(AggregateFunction.class)) {
                NamedExpression aggFuncAfterRewrite = (NamedExpression) expr.rewriteDownShortCircuit(e -> {
                    if (e instanceof AggregateFunction) {
                        if (e instanceof Count) {
                            return new Sum(aggFuncSlotMap.get(e));
                        } else {
                            return e.withChildren(aggFuncSlotMap.get(e));
                        }
                    } else {
                        return e;
                    }
                });
                aggFuncAfterRewrite = ((Alias) aggFuncAfterRewrite)
                        .withExprId(StatementScopeIdGenerator.newExprId());
                NamedExpression replacedExpr = (NamedExpression) aggFuncAfterRewrite.rewriteDownShortCircuit(
                        e -> {
                            if (originToConsumerMap.containsKey(e)) {
                                return originToConsumerMap.get(e);
                            } else {
                                return e;
                            }
                        }
                );
                topAggOutput.add(replacedExpr);
                aggFuncToSlot.put((AggregateFunction) expr.collectFirst(e -> e instanceof AggregateFunction).get(),
                        replacedExpr.toSlot());
            }
        }
        // NOTE: shuffle key selection is applied on the pre-agg (producer) side by setting
        // LogicalAggregate.partitionExpressions. See constructProducer().
        return new LogicalAggregate<>(topAggGby, topAggOutput, Optional.of(newRepeat), newRepeat);
    }

    /**
     * Construct a LogicalProject that wraps the aggregate and handles output expressions.
     * This method replaces removed expressions with null literals, and output the grouping scalar functions
     * at the end of the projections.
     *
     * @param aggregate the original aggregate plan
     * @param originToConsumerMap the map from original slots to consumer slots
     * @param needRemovedExprSet the set of expressions that need to be replaced with null
     * @param groupingFunctionSlots the list of grouping function slots to add to the project
     * @param topAgg the aggregate plan to wrap
     * @param aggFuncToSlot the map from aggregate functions to their slots
     * @return a LogicalProject wrapping the aggregate with proper output expressions
     */
    private LogicalProject<Plan> constructProject(LogicalAggregate<? extends Plan> aggregate,
            Map<Slot, Slot> originToConsumerMap, Set<Expression> needRemovedExprSet,
            List<NamedExpression> groupingFunctionSlots, LogicalAggregate<Plan> topAgg,
            Map<AggregateFunction, Slot> aggFuncToSlot) {
        LogicalRepeat<?> repeat = (LogicalRepeat<?>) aggregate.child(0);
        Set<ExprId> originGroupingFunctionId = new HashSet<>();
        for (NamedExpression namedExpression : repeat.getGroupingScalarFunctionAlias()) {
            originGroupingFunctionId.add(namedExpression.getExprId());
        }
        ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            if (needRemovedExprSet.contains(expr)) {
                projects.add(new Alias(new NullLiteral(expr.getDataType()), expr.getName()));
            } else if (expr instanceof Alias && expr.containsType(AggregateFunction.class)) {
                AggregateFunction aggregateFunction = (AggregateFunction) expr.collectFirst(
                        e -> e instanceof AggregateFunction).get();
                projects.add(aggFuncToSlot.get(aggregateFunction));
            } else if (expr.getExprId().equals(repeat.getGroupingId().get().getExprId())
                    || originGroupingFunctionId.contains(expr.getExprId())) {
                continue;
            } else {
                NamedExpression replacedExpr = (NamedExpression) expr.rewriteDownShortCircuit(
                        e -> {
                            if (originToConsumerMap.containsKey(e)) {
                                return originToConsumerMap.get(e);
                            } else {
                                return e;
                            }
                        }
                );
                projects.add(replacedExpr.toSlot());
            }
        }
        projects.addAll(groupingFunctionSlots);
        return new LogicalProject<>(projects.build(), topAgg);
    }

    /**
     * Construct a LogicalUnion that combines the results from the decomposed repeat
     * and the CTE consumer.
     *
     * @param aggregateProject the first child plan (project with aggregate)
     * @param directConsumer the second child plan (CTE consumer)
     * @param aggregate the original aggregate plan for output reference
     * @return a LogicalUnion combining the two children
     */
    private LogicalUnion constructUnion(LogicalPlan aggregateProject, LogicalPlan directConsumer,
            LogicalAggregate<? extends Plan> aggregate) {
        LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) aggregate.child();
        List<NamedExpression> unionOutputs = new ArrayList<>();
        List<List<SlotReference>> childrenOutputs = new ArrayList<>();
        childrenOutputs.add((List) aggregateProject.getOutput());
        childrenOutputs.add((List) directConsumer.getOutput());
        Set<ExprId> groupingFunctionId = new HashSet<>();
        for (NamedExpression alias : repeat.getGroupingScalarFunctionAlias()) {
            groupingFunctionId.add(alias.getExprId());
        }
        List<NamedExpression> groupingFunctionSlots = new ArrayList<>();
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            if (expr.getExprId().equals(repeat.getGroupingId().get().getExprId())) {
                continue;
            }
            if (groupingFunctionId.contains(expr.getExprId())) {
                groupingFunctionSlots.add(expr.toSlot());
                continue;
            }
            unionOutputs.add(expr.toSlot());
        }
        unionOutputs.addAll(groupingFunctionSlots);
        return new LogicalUnion(Qualifier.ALL, unionOutputs, childrenOutputs, ImmutableList.of(),
                false, ImmutableList.of(aggregateProject, directConsumer));
    }

    /**
     * Determine if optimization is possible; if so, return the index of the largest group.
     * The optimization requires:
     * 1. The aggregate's child must be a LogicalRepeat
     * 2. All aggregate functions must be in SUPPORT_AGG_FUNCTIONS.
     * 3. More than 3 grouping sets
     * 4. There exists a grouping set that contains all other grouping sets
     * @param aggregate the aggregate plan to check
     * @return value -1 means can not be optimized, values other than -1
     *      represent the index of the set that contains all other sets
     */
    private int canOptimize(LogicalAggregate<? extends Plan> aggregate, ConnectContext connectContext) {
        Plan aggChild = aggregate.child();
        if (!(aggChild instanceof LogicalRepeat)) {
            return -1;
        }
        // check agg func
        Set<AggregateFunction> aggFunctions = aggregate.getAggregateFunctions();
        for (AggregateFunction aggFunction : aggFunctions) {
            if (!SUPPORT_AGG_FUNCTIONS.contains(aggFunction.getClass())) {
                return -1;
            }
            if (aggFunction.isDistinct()) {
                return -1;
            }
        }
        LogicalRepeat<Plan> repeat = (LogicalRepeat) aggChild;
        List<List<Expression>> groupingSets = repeat.getGroupingSets();
        // This is an empirical threshold: when there are too few grouping sets,
        // the overhead of creating CTE and union may outweigh the benefits.
        // The value 3 is chosen heuristically based on practical experience.
        if (groupingSets.size() <= connectContext.getSessionVariable().decomposeRepeatThreshold) {
            return -1;
        }
        return findMaxGroupingSetIndex(groupingSets);
    }

    /**
     * Find the index of the grouping set that contains all other grouping sets.
     * First pass: find the grouping set with the maximum size (if multiple have the same size, take the first one).
     * Second pass: verify that this max-size grouping set contains all other grouping sets.
     * A grouping set A contains grouping set B if A contains all expressions in B.
     *
     * @param groupingSets the list of grouping sets to search
     * @return the index of the grouping set that contains all others, or -1 if no such set exists
     */
    private int findMaxGroupingSetIndex(List<List<Expression>> groupingSets) {
        if (groupingSets.isEmpty()) {
            return -1;
        }
        // First pass: find the grouping set with maximum size
        int maxSize = groupingSets.get(0).size();
        int maxGroupIndex = 0;
        for (int i = 1; i < groupingSets.size(); ++i) {
            if (groupingSets.get(i).size() > maxSize) {
                maxSize = groupingSets.get(i).size();
                maxGroupIndex = i;
            }
        }
        if (groupingSets.get(maxGroupIndex).isEmpty()) {
            return -1;
        }
        // Second pass: verify that the max-size grouping set contains all other grouping sets
        ImmutableSet<Expression> maxGroup = ImmutableSet.copyOf(groupingSets.get(maxGroupIndex));
        for (int i = 0; i < groupingSets.size(); ++i) {
            if (i == maxGroupIndex) {
                continue;
            }
            if (!maxGroup.containsAll(groupingSets.get(i))) {
                return -1;
            }
        }
        return maxGroupIndex;
    }

    /**
     * Construct a LogicalCTEProducer that pre-aggregates data using the maximum grouping set.
     * This producer will be used by consumers to avoid recomputing the same aggregation.
     *
     * @param aggregate the original aggregate plan
     * @param maxGroupIndex the index of the maximum grouping set
     * @param ctx context
     * @param preToCloneSlotMap output parameter: map from pre-aggregate slots to cloned slots
     * @return a LogicalCTEProducer containing the pre-aggregation
     */
    private LogicalCTEProducer<LogicalAggregate<Plan>> constructProducer(LogicalAggregate<? extends Plan> aggregate,
            int maxGroupIndex, DistinctSelectorContext ctx, Map<Slot, Slot> preToCloneSlotMap,
            ConnectContext connectContext) {
        LogicalRepeat<? extends Plan> repeat = (LogicalRepeat<? extends Plan>) aggregate.child();
        List<Expression> maxGroupByList = repeat.getGroupingSets().get(maxGroupIndex);
        List<NamedExpression> originAggOutputs = aggregate.getOutputExpressions();
        Set<NamedExpression> preAggOutputSet = new HashSet<NamedExpression>((List) maxGroupByList);
        for (NamedExpression aggOutput : originAggOutputs) {
            if (aggOutput.containsType(AggregateFunction.class)) {
                preAggOutputSet.add(aggOutput);
            }
        }
        List<NamedExpression> orderedAggOutputs = new ArrayList<>();
        // keep order
        for (NamedExpression aggOutput : originAggOutputs) {
            if (preAggOutputSet.contains(aggOutput)) {
                orderedAggOutputs.add(aggOutput);
            }
        }

        LogicalAggregate<Plan> preAgg = new LogicalAggregate<>(maxGroupByList, orderedAggOutputs, repeat.child());
        Optional<List<Expression>> partitionExprs = choosePreAggShuffleKeyPartitionExprs(
                repeat, maxGroupIndex, maxGroupByList, connectContext);
        if (partitionExprs.isPresent() && !partitionExprs.get().isEmpty()) {
            preAgg = preAgg.withPartitionExpressions(partitionExprs);
        }
        LogicalAggregate<Plan> preAggClone = (LogicalAggregate<Plan>) LogicalPlanDeepCopier.INSTANCE
                .deepCopy(preAgg, new DeepCopierContext());
        for (int i = 0; i < preAgg.getOutputExpressions().size(); ++i) {
            preToCloneSlotMap.put(preAgg.getOutput().get(i), preAggClone.getOutput().get(i));
        }
        LogicalCTEProducer<LogicalAggregate<Plan>> producer =
                new LogicalCTEProducer<>(ctx.statementContext.getNextCTEId(), preAggClone);
        ctx.cteProducerList.add(producer);
        return producer;
    }

    /**
     * Choose partition expressions (shuffle key) for pre-aggregation (producer agg).
     */
    private Optional<List<Expression>> choosePreAggShuffleKeyPartitionExprs(
            LogicalRepeat<? extends Plan> repeat, int maxGroupIndex, List<Expression> maxGroupByList,
            ConnectContext connectContext) {
        int idx = connectContext.getSessionVariable().decomposeRepeatShuffleIndexInMaxGroup;
        if (idx >= 0 && idx < maxGroupByList.size()) {
            return Optional.of(ImmutableList.of(maxGroupByList.get(idx)));
        }
        if (repeat.child().getStats() == null) {
            repeat.child().accept(new StatsDerive(false), new DeriveContext());
        }
        Statistics inputStats = repeat.child().getStats();
        if (inputStats == null) {
            return Optional.empty();
        }
        int beNumber = Math.max(1, connectContext.getEnv().getClusterInfo().getBackendsNumber(true));
        int parallelInstance = Math.max(1, connectContext.getSessionVariable().getParallelExecInstanceNum());
        int totalInstanceNum = beNumber * parallelInstance;
        Optional<Expression> chosen;
        switch (repeat.getRepeatType()) {
            case CUBE:
                // Prefer larger NDV to improve balance
                chosen = chooseOneBalancedKey(maxGroupByList, inputStats, totalInstanceNum);
                break;
            case GROUPING_SETS:
                chosen = chooseByAppearanceThenNdv(repeat.getGroupingSets(), maxGroupIndex, maxGroupByList,
                        inputStats, totalInstanceNum);
                break;
            case ROLLUP:
                chosen = chooseOneBalancedKey(maxGroupByList, inputStats, totalInstanceNum);
                break;
            default:
                chosen = Optional.empty();
        }
        return chosen.map(ImmutableList::of);
    }

    private Optional<Expression> chooseOneBalancedKey(List<Expression> candidates, Statistics inputStats,
            int totalInstanceNum) {
        if (inputStats == null) {
            return Optional.empty();
        }
        for (Expression candidate : candidates) {
            ColumnStatistic columnStatistic = inputStats.findColumnStatistics(candidate);
            if (columnStatistic == null || columnStatistic.isUnKnown()) {
                continue;
            }
            if (StatisticsUtil.isBalanced(columnStatistic, inputStats.getRowCount(), totalInstanceNum)) {
                return Optional.of(candidate);
            }
        }
        return Optional.empty();
    }

    /**
     * GROUPING_SETS: prefer keys appearing in more (non-max) grouping sets, tie-break by larger NDV.
     */
    private Optional<Expression> chooseByAppearanceThenNdv(List<List<Expression>> groupingSets, int maxGroupIndex,
            List<Expression> candidates, Statistics inputStats, int totalInstanceNum) {
        Map<Expression, Integer> appearCount = new HashMap<>();
        for (Expression c : candidates) {
            appearCount.put(c, 0);
        }
        for (int i = 0; i < groupingSets.size(); i++) {
            if (i == maxGroupIndex) {
                continue;
            }
            List<Expression> set = groupingSets.get(i);
            for (Expression c : candidates) {
                if (set.contains(c)) {
                    appearCount.put(c, appearCount.get(c) + 1);
                }
            }
        }
        TreeMap<Integer, List<Expression>> countToCandidate = new TreeMap<>();
        for (Map.Entry<Expression, Integer> entry : appearCount.entrySet()) {
            countToCandidate.computeIfAbsent(entry.getValue(), v -> new ArrayList<>()).add(entry.getKey());
        }
        for (Map.Entry<Integer, List<Expression>> entry : countToCandidate.descendingMap().entrySet()) {
            Optional<Expression> chosen = chooseOneBalancedKey(entry.getValue(), inputStats, totalInstanceNum);
            if (chosen.isPresent()) {
                return chosen;
            }
        }
        return Optional.empty();
    }

    /**
     * Construct a new LogicalRepeat with reduced grouping sets and replaced expressions.
     * The grouping sets and output expressions are replaced using the slot mapping from producer to consumer.
     *
     * @param repeat the original LogicalRepeat plan
     * @param child the child plan (usually a CTE consumer)
     * @param newGroupingSets the new grouping sets after removing the maximum grouping set
     * @param producerToDirectConsumerSlotMap the map from producer slots to consumer slots
     * @return a new LogicalRepeat with replaced expressions
     */
    private LogicalRepeat<Plan> constructRepeat(LogicalRepeat<Plan> repeat, LogicalPlan child,
            List<List<Expression>> newGroupingSets, Map<Slot, Slot> producerToDirectConsumerSlotMap,
            List<NamedExpression> groupingFunctionSlots) {
        List<List<Expression>> replacedNewGroupingSets = new ArrayList<>();
        for (List<Expression> groupingSet : newGroupingSets) {
            replacedNewGroupingSets.add(ExpressionUtils.replace(groupingSet, producerToDirectConsumerSlotMap));
        }
        List<NamedExpression> replacedRepeatOutputs = new ArrayList<>(child.getOutput());
        List<NamedExpression> newGroupingFunctions = new ArrayList<>();
        for (NamedExpression groupingFunction : repeat.getGroupingScalarFunctionAlias()) {
            newGroupingFunctions.add(new Alias(groupingFunction.child(0), groupingFunction.getName()));
        }
        replacedRepeatOutputs.addAll(ExpressionUtils.replace((List) newGroupingFunctions,
                producerToDirectConsumerSlotMap));
        for (NamedExpression groupingFunction : newGroupingFunctions) {
            groupingFunctionSlots.add(groupingFunction.toSlot());
        }
        return repeat.withNormalizedExpr(replacedNewGroupingSets, replacedRepeatOutputs,
                repeat.getGroupingId().get(), child);
    }
}
