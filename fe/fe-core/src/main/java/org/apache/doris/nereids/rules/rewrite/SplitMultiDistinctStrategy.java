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

import org.apache.doris.nereids.rules.rewrite.DistinctAggStrategySelector.DistinctSelectorContext;
import org.apache.doris.nereids.rules.rewrite.StatsDerive.DeriveContext;
import org.apache.doris.nereids.trees.copier.DeepCopierContext;
import org.apache.doris.nereids.trees.copier.LogicalPlanDeepCopier;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.NullSafeEqual;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEProducer;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Split multi distinct strategy
 * */
public class SplitMultiDistinctStrategy {
    /** rewrite function*/
    public static Plan rewrite(LogicalAggregate<? extends Plan> agg, DistinctSelectorContext ctx) {
        List<List<Alias>> distinctFuncWithAlias = new ArrayList<>();
        List<Alias> otherAggFuncs = new ArrayList<>();
        collectDistinctAndNonDistinctFunctions(agg, distinctFuncWithAlias, otherAggFuncs);
        LogicalAggregate<Plan> cloneAgg = (LogicalAggregate<Plan>) LogicalPlanDeepCopier.INSTANCE
                .deepCopy(agg, new DeepCopierContext());
        LogicalCTEProducer<Plan> producer = new LogicalCTEProducer<>(ctx.statementContext.getNextCTEId(),
                cloneAgg.child());
        ctx.cteProducerList.add(producer);
        StatsDerive derive = new StatsDerive(false);
        producer.accept(derive, new DeriveContext());

        Map<Slot, Slot> originToProducerSlot = new HashMap<>();
        for (int i = 0; i < agg.child().getOutput().size(); ++i) {
            Slot originSlot = agg.child().getOutput().get(i);
            Slot cloneSlot = cloneAgg.child().getOutput().get(i);
            originToProducerSlot.put(originSlot, cloneSlot);
        }
        List<List<Alias>> distinctFuncWithAliasReplaced = new ArrayList<>();
        for (List<Alias> aliasList : distinctFuncWithAlias) {
            distinctFuncWithAliasReplaced.add(ExpressionUtils.replace((List) aliasList, originToProducerSlot));
        }
        otherAggFuncs = ExpressionUtils.replace((List) otherAggFuncs, originToProducerSlot);
        // construct cte consumer and aggregate
        List<LogicalAggregate<Plan>> newAggs = new ArrayList<>();
        // All otherAggFuncs are placed in the first one
        Map<Alias, Alias> newToOriginDistinctFuncAlias = new HashMap<>();
        List<Expression> outputJoinGroupBys = new ArrayList<>();
        for (int i = 0; i < distinctFuncWithAliasReplaced.size(); ++i) {
            List<Alias> aliases = distinctFuncWithAliasReplaced.get(i);
            List<Expression> aggFuncs = new ArrayList<>();
            for (Alias alias : aliases) {
                aggFuncs.add(alias.child());
            }
            Map<Slot, Slot> producerToConsumerSlotMap = new HashMap<>();
            List<NamedExpression> outputExpressions = new ArrayList<>();
            List<Expression> replacedGroupBy = new ArrayList<>();
            LogicalCTEConsumer consumer = constructConsumerAndReplaceGroupBy(ctx, producer, cloneAgg, outputExpressions,
                    producerToConsumerSlotMap, replacedGroupBy);
            List<Expression> newDistinctAggFuncs = ExpressionUtils.replace(aggFuncs, producerToConsumerSlotMap);
            List<Alias> newAliases = new ArrayList<>();
            for (Expression expr : newDistinctAggFuncs) {
                newAliases.add(new Alias(expr));
            }
            outputExpressions.addAll(newAliases);
            if (i == 0) {
                // save replacedGroupBy
                outputJoinGroupBys.addAll(replacedGroupBy);
            }
            LogicalAggregate<Plan> newAgg = new LogicalAggregate<>(replacedGroupBy, outputExpressions, consumer);
            newAggs.add(newAgg);
            for (int j = 0; j < aliases.size(); ++j) {
                newToOriginDistinctFuncAlias.put(newAliases.get(j), aliases.get(j));
            }
        }
        buildOtherAggFuncAggregate(otherAggFuncs, producer, ctx, cloneAgg, newToOriginDistinctFuncAlias, newAggs);
        List<Expression> groupBy = agg.getGroupByExpressions();
        LogicalJoin<Plan, Plan> join = constructJoin(newAggs, groupBy);
        return constructProject(groupBy, newToOriginDistinctFuncAlias, outputJoinGroupBys, join);
    }

    private static void buildOtherAggFuncAggregate(List<Alias> otherAggFuncs, LogicalCTEProducer<Plan> producer,
            DistinctSelectorContext ctx, LogicalAggregate<Plan> cloneAgg,
            Map<Alias, Alias> newToOriginDistinctFuncAlias, List<LogicalAggregate<Plan>> newAggs) {
        if (otherAggFuncs.isEmpty()) {
            return;
        }
        Map<Slot, Slot> producerToConsumerSlotMap = new HashMap<>();
        List<NamedExpression> outputExpressions = new ArrayList<>();
        List<Expression> replacedGroupBy = new ArrayList<>();
        LogicalCTEConsumer consumer = constructConsumerAndReplaceGroupBy(ctx, producer, cloneAgg, outputExpressions,
                producerToConsumerSlotMap, replacedGroupBy);
        List<Expression> otherAggFuncAliases = otherAggFuncs.stream()
                .map(e -> ExpressionUtils.replace(e, producerToConsumerSlotMap)).collect(Collectors.toList());
        for (Expression otherAggFuncAlias : otherAggFuncAliases) {
            // otherAggFunc is instance of Alias
            Alias outputOtherFunc = new Alias(otherAggFuncAlias.child(0));
            outputExpressions.add(outputOtherFunc);
            newToOriginDistinctFuncAlias.put(outputOtherFunc, (Alias) otherAggFuncAlias);
        }
        LogicalAggregate<Plan> newAgg = new LogicalAggregate<>(replacedGroupBy, outputExpressions, consumer);
        newAggs.add(newAgg);
    }

    private static LogicalCTEConsumer constructConsumerAndReplaceGroupBy(
            DistinctSelectorContext ctx,
            LogicalCTEProducer<Plan> producer, LogicalAggregate<Plan> cloneAgg, List<NamedExpression> outputExpressions,
            Map<Slot, Slot> producerToConsumerSlotMap, List<Expression> replacedGroupBy) {
        LogicalCTEConsumer consumer = new LogicalCTEConsumer(ctx.statementContext.getNextRelationId(),
                producer.getCteId(), "", producer);
        ctx.cascadesContext.putCTEIdToConsumer(consumer);
        for (Map.Entry<Slot, Slot> entry : consumer.getConsumerToProducerOutputMap().entrySet()) {
            producerToConsumerSlotMap.put(entry.getValue(), entry.getKey());
        }
        replacedGroupBy.addAll(ExpressionUtils.replace(cloneAgg.getGroupByExpressions(), producerToConsumerSlotMap));
        outputExpressions.addAll(replacedGroupBy.stream().map(Slot.class::cast).collect(Collectors.toList()));
        return consumer;
    }

    private static boolean isDistinctMultiColumns(AggregateFunction func) {
        if (func.arity() <= 1) {
            return false;
        }
        for (int i = 1; i < func.arity(); ++i) {
            // think about group_concat(distinct col_1, ',')
            if (!(func.child(i) instanceof OrderExpression) && !func.child(i).getInputSlots().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    private static void collectDistinctAndNonDistinctFunctions(LogicalAggregate<? extends Plan> agg,
            List<List<Alias>> aliases, List<Alias> otherAggFuncs) {
        // TODO with source repeat aggregate need to be supported in future
        // 这个可能也没有关系，可以先注释掉，之后加一下关于grouping的测试
        // if (agg.getSourceRepeat().isPresent()) {
        //     return false;
        // }
        // boolean distinctMultiColumns = false;
        Map<Set<Expression>, List<Alias>> distinctArgToAliasList = new LinkedHashMap<>();
        for (NamedExpression namedExpression : agg.getOutputExpressions()) {
            if (!(namedExpression instanceof Alias) || !(namedExpression.child(0) instanceof AggregateFunction)) {
                continue;
            }
            AggregateFunction aggFunc = (AggregateFunction) namedExpression.child(0);
            if (aggFunc.isDistinct()) {
                distinctArgToAliasList.computeIfAbsent(ImmutableSet.copyOf(aggFunc.getDistinctArguments()),
                        k -> new ArrayList<>()).add((Alias) namedExpression);
            } else {
                otherAggFuncs.add((Alias) namedExpression);
            }
        }
        aliases.addAll(distinctArgToAliasList.values());
    }

    private static LogicalProject<Plan> constructProject(List<Expression> groupBy, Map<Alias, Alias> joinOutput,
            List<Expression> outputJoinGroupBys, LogicalJoin<Plan, Plan> join) {
        List<NamedExpression> projects = new ArrayList<>();
        for (Map.Entry<Alias, Alias> entry : joinOutput.entrySet()) {
            projects.add(new Alias(entry.getValue().getExprId(), entry.getKey().toSlot(), entry.getValue().getName()));
        }
        for (int i = 0; i < groupBy.size(); ++i) {
            Slot slot = (Slot) groupBy.get(i);
            projects.add(new Alias(slot.getExprId(), outputJoinGroupBys.get(i), slot.getName()));
        }
        return new LogicalProject<>(projects, join);
    }

    private static LogicalJoin<Plan, Plan> constructJoin(List<LogicalAggregate<Plan>> newAggs,
            List<Expression> groupBy) {
        LogicalJoin<Plan, Plan> join;
        if (groupBy.isEmpty()) {
            join = new LogicalJoin<>(JoinType.CROSS_JOIN, newAggs.get(0), newAggs.get(1), null);
            for (int j = 2; j < newAggs.size(); ++j) {
                join = new LogicalJoin<>(JoinType.CROSS_JOIN, join, newAggs.get(j), null);
            }
        } else {
            int len = groupBy.size();
            List<Slot> leftSlots = newAggs.get(0).getOutput();
            List<Slot> rightSlots = newAggs.get(1).getOutput();
            List<Expression> hashConditions = new ArrayList<>();
            for (int i = 0; i < len; ++i) {
                hashConditions.add(new NullSafeEqual(leftSlots.get(i), rightSlots.get(i)));
            }
            join = new LogicalJoin<>(JoinType.INNER_JOIN, hashConditions, newAggs.get(0), newAggs.get(1), null);
            for (int j = 2; j < newAggs.size(); ++j) {
                List<Slot> belowJoinSlots = join.left().getOutput();
                List<Slot> belowRightSlots = newAggs.get(j).getOutput();
                List<Expression> aboveHashConditions = new ArrayList<>();
                for (int i = 0; i < len; ++i) {
                    aboveHashConditions.add(new NullSafeEqual(belowJoinSlots.get(i), belowRightSlots.get(i)));
                }
                join = new LogicalJoin<>(JoinType.INNER_JOIN, aboveHashConditions, join, newAggs.get(j), null);
            }
        }
        return join;
    }
}
