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

import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**SplitAggRule*/
public abstract class SplitAggRule {
    protected LogicalAggregate<? extends Plan> splitDeduplicateAgg(LogicalAggregate<? extends Plan> aggregate,
            Set<NamedExpression> localAggGroupBySet, AggregateParam inputToBufferParam, ConnectContext connectContext,
            Map<AggregateFunction, Alias> localAggFunctionToAlias, Plan child, List<Expression> partitionExpressions) {
        aggregate.getAggregateFunctions().stream()
                .filter(aggFunc -> !aggFunc.isDistinct())
                .collect(Collectors.toMap(
                        expr -> expr,
                        expr -> {
                            AggregateExpression localAggExpr = new AggregateExpression(expr, inputToBufferParam);
                            return new Alias(localAggExpr);
                        },
                        (existing, replacement) -> existing,  // 如果有重复键，保留旧值
                        () -> localAggFunctionToAlias  // 直接收集到目标 Map
                ));
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll(localAggGroupBySet)
                .addAll(localAggFunctionToAlias.values())
                .build();
        List<Expression> localAggGroupBy = Utils.fastToImmutableList(localAggGroupBySet);

        return aggregate.withAggParam(localAggOutput, localAggGroupBy,
                AggregateUtils.maybeUsingStreamAgg(connectContext, aggregate), inputToBufferParam,
                null, Optional.of(partitionExpressions), child);
    }

    protected LogicalAggregate<? extends Plan> splitLocalTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, List<Expression> partitionExpressions,
            Set<NamedExpression> localAggGroupBySet, ConnectContext connectContext) {
        // first phase
        AggregateParam inputToBufferParam = AggregateParam.LOCAL_BUFFER;
        Map<AggregateFunction, Alias> localAggFunctionToAlias = new LinkedHashMap<>();
        LogicalAggregate<? extends Plan> localAgg = splitDeduplicateAgg(aggregate, localAggGroupBySet,
                inputToBufferParam, connectContext, localAggFunctionToAlias, aggregate.child(), ImmutableList.of());

        // second phase
        AggregateParam bufferToBufferParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_BUFFER);
        localAggFunctionToAlias.entrySet()
                .stream()
                .collect(Collectors.toMap(entry -> entry.getKey(),
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
        return aggregate.withAggParam(middleAggOutput, localAgg.getGroupByExpressions(),
                false, bufferToBufferParam, null,
                Optional.of(partitionExpressions), localAgg);
    }

    protected Set<NamedExpression> getAllKeySet(LogicalAggregate<? extends Plan> aggregate) {
        Set<Expression> distinctArguments = aggregate.getDistinctArguments();
        return ImmutableSet.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(distinctArguments)
                .build();
    }

    protected LogicalAggregate<? extends Plan> splitDistinctTwoPhase(LogicalAggregate<? extends Plan> aggregate,
            Map<AggregateFunction, Alias> middleAggFunctionToAlias, Plan child) {
        AggregateParam thirdParam = new AggregateParam(AggPhase.DISTINCT_LOCAL, AggMode.INPUT_TO_BUFFER, false);
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
        //然后把distinct函数再加一下
        for (AggregateFunction func : aggregate.getAggregateFunctions()) {
            if (!func.isDistinct()) {
                continue;
            }
            aggFuncToAliasThird.put(func, new Alias(new AggregateExpression(
                    func.withDistinctAndChildren(false, func.children()), thirdParam)));
        }
        List<NamedExpression> thirdAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(aggFuncToAliasThird.values())
                .build();
        LogicalAggregate<? extends Plan> thirdAgg = aggregate.withAggParam(thirdAggOutput,
                aggregate.getGroupByExpressions(), false, thirdParam, null,
                Optional.empty(), child);

        // fourth phase
        AggregateParam fourthParam = new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT, false);
        List<NamedExpression> globalOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (expr instanceof AggregateFunction) {
                        AggregateFunction aggFunc = (AggregateFunction) expr;
                        if (aggFunc.isDistinct()) {
                            Alias alias = aggFuncToAliasThird.get(aggFunc);
                            return new AggregateExpression(
                                    aggFunc.withDistinctAndChildren(false, aggFunc.children()), fourthParam, alias.toSlot());
                        } else {
                            return new AggregateExpression(aggFunc,
                                    new AggregateParam(AggPhase.DISTINCT_GLOBAL, AggMode.BUFFER_TO_RESULT),
                                    aggFuncToAliasThird.get(aggFunc).toSlot());
                        }
                    }
                    return expr;
                }
        );
        return aggregate.withAggParam(globalOutput, aggregate.getGroupByExpressions(),
                false, fourthParam, aggregate.getLogicalProperties(),
                Optional.of(aggregate.getGroupByExpressions()), thirdAgg);
    }
}
