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

import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.types.TinyIntType;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.collect.ImmutableList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**SplitAggRule*/
public abstract class SplitAggBaseRule {
    /**
     * This functions is used to split bottom deduplicate Aggregate(phase1):
     * e.g.select count(distinct a) group by b
     *   agg(group by b, count(a); distinct global) ---phase2
     *     +--agg(group by a,b; global) ---phase1
     *       +--hashShuffle(b)
     * */
    protected PhysicalHashAggregate<? extends Plan> splitDeduplicateOnePhase(LogicalAggregate<? extends Plan> aggregate,
            Set<NamedExpression> localAggGroupBySet, AggregateParam inputToBufferParam, AggregateParam paramForAggFunc,
            Map<AggregateFunction, Alias> localAggFunctionToAlias, Plan child, List<Expression> partitionExpressions) {
        aggregate.getAggregateFunctions().stream()
                .filter(aggFunc -> !aggFunc.isDistinct())
                .collect(Collectors.toMap(
                        expr -> expr,
                        expr -> {
                            AggregateExpression localAggExpr = new AggregateExpression(expr, paramForAggFunc);
                            return new Alias(localAggExpr);
                        },
                        (existing, replacement) -> existing,
                        () -> localAggFunctionToAlias
                ));
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(localAggFunctionToAlias.values())
                .build();
        List<Expression> localAggGroupBy = Utils.fastToImmutableList(localAggGroupBySet);
        boolean isGroupByEmptySelectEmpty = localAggGroupBy.isEmpty() && localAggOutput.isEmpty();
        // be not recommend generate an aggregate node with empty group by and empty output,
        // so add a null int slot to group by slot and output
        if (isGroupByEmptySelectEmpty) {
            localAggGroupBy = ImmutableList.of(new NullLiteral(TinyIntType.INSTANCE));
            localAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }
        return new PhysicalHashAggregate<>(localAggGroupBy, localAggOutput, Optional.ofNullable(partitionExpressions),
                inputToBufferParam, AggregateUtils.maybeUsingStreamAgg(localAggGroupBy, inputToBufferParam),
                null, child);
    }

    /**
     * This functions is used to split bottom deduplicate Aggregate(phase1 and phase2):
     * e.g.select count(distinct a) group by b
     *   agg(group by b, count(a); distinct global) ---phase3
     *     +--agg(group by a,b; global) ---phase2
     *       +--hashShuffle(b)
     *         +--agg(group by a,b; local) ---phase1
     * */
    protected PhysicalHashAggregate<? extends Plan> splitDeduplicateTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, List<Expression> partitionExpressions,
            Set<NamedExpression> localAggGroupBySet) {
        // first phase
        AggregateParam inputToBufferParam = AggregateParam.LOCAL_BUFFER;
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        PhysicalHashAggregate<? extends Plan> localAgg = splitDeduplicateOnePhase(aggregate, localAggGroupBySet,
                inputToBufferParam, inputToBufferParam, localAggFunctionToAlias, aggregate.child(), ImmutableList.of());

        // second phase
        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        localAggFunctionToAlias.entrySet()
                .stream()
                .collect(Collectors.toMap(Entry::getKey,
                        kv -> {
                            AggregateExpression middleAggExpr = new AggregateExpression(kv.getKey(),
                                    bufferToBufferParam, kv.getValue().toSlot());
                            return new Alias(middleAggExpr);
                        },
                        (existing, replacement) -> existing,
                        () -> middleAggFunctionToAlias));
        List<NamedExpression> middleAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(middleAggFunctionToAlias.values())
                .build();
        if (middleAggOutput.isEmpty()) {
            middleAggOutput = ImmutableList.of(new Alias(new NullLiteral(TinyIntType.INSTANCE)));
        }
        return new PhysicalHashAggregate<>(localAgg.getGroupByExpressions(), middleAggOutput,
                Optional.ofNullable(partitionExpressions), bufferToResultParam,
                AggregateUtils.maybeUsingStreamAgg(localAgg.getGroupByExpressions(), bufferToResultParam),
                null, localAgg);
    }

    /**
     * This functions is used to split distinct phase Aggregate(phase2 and phase3):
     * e.g. select count(distinct a) group by b
     *   agg(group by b, count(a); distinct global) --phase3
     *     +--hashShuffle(b)
     *       +--agg(group by b, count(a); distinct local) --phase2
     *         +--agg(group by a,b; global) --phase1
     *           +--hashShuffle(a)
     * */
    protected PhysicalHashAggregate<? extends Plan> splitDistinctTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, Plan child) {
        AggregateParam thirdParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFuncToAliasThird = new LinkedHashMap<>();
        middleAggFunctionToAlias.entrySet().stream().collect(
                Collectors.toMap(Entry::getKey,
                        entry -> new Alias(new AggregateExpression(entry.getKey(),
                                new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.BUFFER_TO_BUFFER),
                                entry.getValue().toSlot())),
                        (k1, k2) -> k1,
                        () -> aggFuncToAliasThird
                )
        );
        for (AggregateFunction func : aggregate.getAggregateFunctions()) {
            if (!func.isDistinct()) {
                continue;
            }
            if (func instanceof Count && func.arity() > 1) {
                Expression countIf = AggregateUtils.countDistinctMultiExprToCountIf((Count) func);
                aggFuncToAliasThird.put(func, new Alias(new AggregateExpression((Count) countIf, thirdParam)));
            } else {
                aggFuncToAliasThird.put(func, new Alias(new AggregateExpression(
                        func.withDistinctAndChildren(false, func.children()), thirdParam)));
            }
        }
        List<NamedExpression> thirdAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(aggFuncToAliasThird.values())
                .build();
        Plan thirdAgg = new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(), thirdAggOutput, thirdParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), thirdParam), null, child);

        // fourth phase
        AggregateParam fourthParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            Alias alias = aggFuncToAliasThird.get(aggFunc);
                            if (aggFunc instanceof Count && aggFunc.arity() > 1) {
                                return new AggregateExpression(((AggregateExpression) alias.child()).getFunction(),
                                        fourthParam, alias.toSlot());
                            } else {
                                return new AggregateExpression(
                                        aggFunc.withDistinctAndChildren(false, aggFunc.children()),
                                        fourthParam, alias.toSlot());
                            }
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                                    aggFuncToAliasThird.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                }
        );
        return new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(), globalOutput,
                Optional.ofNullable(aggregate.getGroupByExpressions()), fourthParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), fourthParam),
                aggregate.getLogicalProperties(), thirdAgg);
    }
}
