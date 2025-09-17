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
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregatePhase;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.util.AggregateUtils;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**SplitAgg
 * only process agg without distinct function, split Agg into 2 phase: local agg and global agg
 * */
public class SplitAggWithoutDistinct extends OneImplementationRuleFactory {
    public static final SplitAggWithoutDistinct INSTANCE = new SplitAggWithoutDistinct();

    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(Aggregate::hasDistinctFunc)
                .thenApplyMulti(ctx -> rewrite(ctx.root, ctx.connectContext))
                .toRule(RuleType.SPLIT_AGG_WITHOUT_DISTINCT);
    }

    private List<Plan> rewrite(LogicalAggregate<? extends Plan> aggregate, ConnectContext ctx) {
        switch (ctx.getSessionVariable().aggPhase) {
            case 1:
                return ImmutableList.<Plan>builder()
                        .addAll(implementOnePhase(aggregate))
                        .build();
            case 2:
                return ImmutableList.<Plan>builder()
                        .addAll(splitTwoPhase(aggregate))
                        .build();
            default:
                return ImmutableList.<Plan>builder()
                        .addAll(implementOnePhase(aggregate))
                        .addAll(splitTwoPhase(aggregate))
                        .build();
        }
    }

    /**
     * select sum(a) from t group by b;
     * LogicalAggregate(group by b, outputExpr: sum(a), b)
     * ->
     * PhysicalHashAggregate(group by b, outputExpr: sum(a), b; AGG_PHASE:GLOBAL)
     * */
    private List<Plan> implementOnePhase(LogicalAggregate<? extends Plan> logicalAgg) {
        if (!logicalAgg.supportAggregatePhase(AggregatePhase.ONE)) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<NamedExpression> builder = ImmutableList.builder();
        boolean changed = false;
        for (NamedExpression expr : logicalAgg.getOutputExpressions()) {
            if (expr instanceof Alias && expr.child(0) instanceof AggregateFunction) {
                Alias alias = (Alias) expr;
                AggregateExpression aggExpr = new AggregateExpression((AggregateFunction) expr.child(0),
                        AggregateParam.GLOBAL_RESULT);
                builder.add(alias.withChildren(ImmutableList.of(aggExpr)));
                changed = true;
            } else {
                builder.add(expr);
            }
        }
        AggregateParam param = new AggregateParam(AggPhase.GLOBAL, AggMode.INPUT_TO_RESULT, !skipRegulator(logicalAgg));
        List<NamedExpression> aggOutput = changed ? builder.build() : logicalAgg.getOutputExpressions();
        return ImmutableList.of(new PhysicalHashAggregate<>(logicalAgg.getGroupByExpressions(), aggOutput, param,
                AggregateUtils.maybeUsingStreamAgg(logicalAgg.getGroupByExpressions(), param),
                null, logicalAgg.child()));
    }

    /**
     * select sum(a) from t group by b;
     * LogicalAggregate(group by b, outputExpr: sum(a), b)
     * ->
     * PhysicalHashAggregate(group by b, outputExpr: sum(a), b; AGG_PHASE:GLOBAL)
     *   +--PhysicalHashAggregate(group by b, outputExpr: partial_sum(a), b; AGG_PHASE:LOCAL)
     * */
    private List<Plan> splitTwoPhase(LogicalAggregate<? extends Plan> aggregate) {
        if (!aggregate.supportAggregatePhase(AggregatePhase.TWO)) {
            return ImmutableList.of();
        }
        AggregateParam inputToBufferParam = new AggregateParam(AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER);
        Map<AggregateFunction, Alias> aggFunctionToAlias = aggregate.getAggregateFunctions().stream()
                .collect(ImmutableMap.toImmutableMap(function -> function, function -> {
                    AggregateExpression localAggFunc = new AggregateExpression(function, inputToBufferParam);
                    return new Alias(localAggFunc);
                }));
        List<NamedExpression> localAggOutput = ImmutableList.<NamedExpression>builder()
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(aggFunctionToAlias.values())
                .build();

        PhysicalHashAggregate<? extends Plan> localAgg = new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(),
                localAggOutput, inputToBufferParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), inputToBufferParam),
                null, aggregate.child());

        // global agg
        AggregateParam bufferToResultParam = new AggregateParam(AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT);
        List<NamedExpression> globalAggOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), expr -> {
                    if (!(expr instanceof AggregateFunction)) {
                        return expr;
                    }
                    Alias alias = aggFunctionToAlias.get(expr);
                    if (alias == null) {
                        return expr;
                    }
                    AggregateFunction aggFunc = (AggregateFunction) expr;
                    return new AggregateExpression(aggFunc, bufferToResultParam, alias.toSlot());
                });
        return ImmutableList.of(new PhysicalHashAggregate<>(aggregate.getGroupByExpressions(),
                globalAggOutput, bufferToResultParam,
                AggregateUtils.maybeUsingStreamAgg(aggregate.getGroupByExpressions(), bufferToResultParam),
                aggregate.getLogicalProperties(), localAgg));
    }

    private boolean shouldUseLocalAgg(LogicalAggregate<? extends Plan> aggregate) {
        Statistics aggStats = aggregate.getGroupExpression().get().getOwnerGroup().getStatistics();
        Statistics aggChildStats = aggregate.getGroupExpression().get().childStatistics(0);
        // if gbyNdv is high, should not use local agg
        double rows = aggChildStats.getRowCount();
        double gbyNdv = aggStats.getRowCount();
        return gbyNdv * 10 < rows;
    }

    private boolean skipRegulator(LogicalAggregate<? extends Plan> aggregate) {
        for (AggregateFunction aggregateFunction : aggregate.getAggregateFunctions()) {
            if (aggregateFunction.forceSkipRegulator(AggregatePhase.ONE)) {
                return true;
            }
        }
        return false;
    }
}
