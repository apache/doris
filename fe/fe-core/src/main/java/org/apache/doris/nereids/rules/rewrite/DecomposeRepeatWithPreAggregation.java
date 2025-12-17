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
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SessionVarGuardExpr;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
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
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * RewriteRepeatByCte will rewrite grouping sets. eg:
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
 *           +--LogicalCTEConsumer(cteConsumer1)
 *     +--LogicalCTEConsumer(cteConsumer2)
 */
public class DecomposeRepeatWithPreAggregation extends DefaultPlanRewriter<DistinctSelectorContext>
        implements CustomRewriter {
    public static final DecomposeRepeatWithPreAggregation INSTANCE = new DecomposeRepeatWithPreAggregation();
    private static final Set<Class<? extends AggregateFunction>> SUPPORT_AGG_FUNCTIONS =
            ImmutableSet.of(Sum.class, Min.class, Max.class);

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
    public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, DistinctSelectorContext ctx) {
        aggregate = visitChildren(this, aggregate, ctx);
        int maxGroupIndex = canOptimize(aggregate);
        if (maxGroupIndex < 0) {
            return aggregate;
        }
        Map<Slot, Slot> preToProducerSlotMap = new HashMap<>();
        LogicalCTEProducer<LogicalAggregate<Plan>> producer = constructProducer(aggregate, maxGroupIndex, ctx,
                preToProducerSlotMap);
        LogicalCTEConsumer consumer1 = new LogicalCTEConsumer(ctx.statementContext.getNextRelationId(),
                producer.getCteId(), "", producer);
        LogicalCTEConsumer consumer2 = new LogicalCTEConsumer(ctx.statementContext.getNextRelationId(),
                producer.getCteId(), "", producer);

        // build map : origin slot to consumer slot
        Map<Slot, Slot> producerToConsumerMap = new HashMap<>();
        for (Map.Entry<Slot, Slot> entry : consumer1.getProducerToConsumerOutputMap().entries()) {
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
        LogicalRepeat<Plan> newRepeat = constructRepeat(repeat, consumer1, newGroupingSets, originToConsumerMap);
        Set<Expression> needRemovedExprSet = getNeedAddNullExpressions(repeat, newGroupingSets, maxGroupIndex);
        LogicalProject<Plan> project = constructProjectAggregate(aggregate,
                originToConsumerMap, repeat, newRepeat, needRemovedExprSet);
        return constructUnion(project, consumer2, aggregate);
    }

    private Map<AggregateFunction, Slot> getAggFuncSlotMap(List<NamedExpression> outputExpressions,
            Map<Slot, Slot> pToc) {
        // build map : aggFunc to Slot
        Map<AggregateFunction, Slot> aggFuncSlotMap = new HashMap<>();
        for (NamedExpression expr : outputExpressions) {
            if (expr instanceof Alias) {
                Expression aggFunc = SessionVarGuardExpr.getSessionVarGuardChild(expr.child(0));
                if (!(aggFunc instanceof AggregateFunction)) {
                    continue;
                }
                aggFuncSlotMap.put((AggregateFunction) aggFunc, pToc.get(expr.toSlot()));
            }
        }
        return aggFuncSlotMap;
    }

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

    private LogicalProject<Plan> constructProjectAggregate(LogicalAggregate<? extends Plan> aggregate,
            Map<Slot, Slot> originToConsumerMap,
            LogicalRepeat<Plan> repeat, LogicalRepeat<Plan> newRepeat, Set<Expression> needRemovedExprSet) {
        Map<AggregateFunction, Slot> aggFuncSlotMap = getAggFuncSlotMap(aggregate.getOutputExpressions(),
                originToConsumerMap);
        List<NamedExpression> topAggOutput = new ArrayList<>();
        List<NamedExpression> projects = new ArrayList<>();
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            if (needRemovedExprSet.contains(expr)) {
                projects.add(new Alias(new NullLiteral(expr.getDataType()), expr.getName()));
            } else {
                if (expr instanceof Alias && expr.containsType(AggregateFunction.class)) {
                    NamedExpression aggFuncAfterRewrite = (NamedExpression) expr.rewriteDownShortCircuit(e -> {
                        if (e instanceof AggregateFunction) {
                            return e.withChildren(aggFuncSlotMap.get(e));
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
                    projects.add(replacedExpr.toSlot());
                } else {
                    if (expr.getExprId().equals(repeat.getGroupingId().get().getExprId())) {
                        topAggOutput.add(expr);
                        continue;
                    }
                    NamedExpression replacedExpr = (NamedExpression) expr.rewriteDownShortCircuit(
                            e -> {
                                if (originToConsumerMap.containsKey(e)) {
                                    return originToConsumerMap.get(e);
                                } else {
                                    return e;
                                }
                            }
                    );
                    topAggOutput.add(replacedExpr);
                    projects.add(replacedExpr.toSlot());
                }
            }
        }
        Set<Slot> groupingSetsUsedSlot = ImmutableSet.copyOf(
                ExpressionUtils.flatExpressions((List) newRepeat.getGroupingSets()));
        List<Expression> topAggGby2 = new ArrayList<>(groupingSetsUsedSlot);
        topAggGby2.add(newRepeat.getGroupingId().get());
        List<Expression> replacedAggGby = ExpressionUtils.replace(topAggGby2, originToConsumerMap);
        LogicalAggregate<Plan> topAgg = new LogicalAggregate<>(replacedAggGby, topAggOutput,
                Optional.of(newRepeat), newRepeat);
        return new LogicalProject<>(projects, topAgg);
    }

    private LogicalUnion constructUnion(LogicalPlan child1, LogicalPlan child2,
            LogicalAggregate<? extends Plan> aggregate) {
        LogicalRepeat<Plan> repeat = (LogicalRepeat<Plan>) aggregate.child();
        List<NamedExpression> unionOutputs = new ArrayList<>();
        List<List<SlotReference>> childrenOutputs = new ArrayList<>();
        childrenOutputs.add((List) child1.getOutput());
        childrenOutputs.add((List) child2.getOutput());
        for (NamedExpression expr : aggregate.getOutputExpressions()) {
            if (expr.getExprId().equals(repeat.getGroupingId().get().getExprId())) {
                continue;
            }
            unionOutputs.add(expr.toSlot());
        }
        return new LogicalUnion(Qualifier.ALL, unionOutputs, childrenOutputs, ImmutableList.of(),
                false, ImmutableList.of(child1, child2));
    }

    private int canOptimize(LogicalAggregate<? extends Plan> aggregate) {
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
        // find max group
        LogicalRepeat<Plan> repeat = (LogicalRepeat) aggChild;
        for (NamedExpression expr : repeat.getOutputExpressions()) {
            if (expr.containsType(GroupingScalarFunction.class)) {
                return -1;
            }
        }
        List<List<Expression>> groupingSets = repeat.getGroupingSets();
        if (groupingSets.size() <= 3) {
            return -1;
        }
        ImmutableSet<Expression> maxGroup = ImmutableSet.copyOf(groupingSets.get(0));
        int maxGroupIndex = 0;
        for (int i = 1; i < groupingSets.size(); ++i) {
            List<Expression> groupingSet = groupingSets.get(i);
            if (groupingSet.size() < maxGroup.size()) {
                continue;
            }
            if (maxGroup.containsAll(groupingSet)) {
                continue;
            }
            ImmutableSet<Expression> currentSet = ImmutableSet.copyOf(groupingSet);
            if (currentSet.containsAll(maxGroup)) {
                maxGroup = currentSet;
                maxGroupIndex = i;
            } else {
                maxGroupIndex = -1;
                break;
            }
        }
        return maxGroupIndex;
    }

    private LogicalCTEProducer<LogicalAggregate<Plan>> constructProducer(LogicalAggregate<? extends Plan> aggregate,
            int maxGroupIndex, DistinctSelectorContext ctx, Map<Slot, Slot> preToCloneSlotMap) {
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

        LogicalAggregate<Plan> preAgg = new LogicalAggregate<>(Utils.fastToImmutableList(maxGroupByList),
                orderedAggOutputs, repeat.child());
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

    private LogicalRepeat<Plan> constructRepeat(LogicalRepeat<Plan> repeat, LogicalPlan child,
            List<List<Expression>> newGroupingSets, Map<Slot, Slot> producerToConsumer2SlotMap) {
        List<List<Expression>> replacedNewGroupingSets = new ArrayList<>();
        for (List<Expression> groupingSet : newGroupingSets) {
            replacedNewGroupingSets.add(ExpressionUtils.replace(groupingSet, producerToConsumer2SlotMap));
        }
        List<NamedExpression> replacedRepeatOutputs = new ArrayList<>(child.getOutput());
        return repeat.withNormalizedExpr(replacedNewGroupingSets, replacedRepeatOutputs,
                repeat.getGroupingId().get(), child);
    }
}
