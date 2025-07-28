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

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

/**SplitAgg
 * only process agg without distinct function, split Agg into 2 phase: local agg and global agg
 * */
public class SplitAgg extends OneExplorationRuleFactory {
    public static final SplitAgg INSTANCE = new SplitAgg();

    @Override
    public Rule build() {
        return logicalAggregate()
                .whenNot(agg -> agg.getAggregateParam().isSplit)
                .whenNot(Aggregate::isAggregateDistinct)
                .thenApply(ctx -> rewrite(ctx.root))
                .toRule(RuleType.SPLIT_AGG);
    }

    private Plan rewrite(LogicalAggregate<? extends Plan> aggregate) {
        // 如果认为什么条件下不需要拆分,使用一阶段AGG更快,那么可以在这里进行判断.
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

        LogicalAggregate<? extends Plan> localAgg = aggregate.withAggParam(localAggOutput,
                aggregate.getGroupByExpressions(), inputToBufferParam, null, null, aggregate.child());

        //global agg做final聚合
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
        return aggregate.withAggParam(globalAggOutput, aggregate.getGroupByExpressions(),
                bufferToResultParam, aggregate.getLogicalProperties(), null, localAgg);
    }
}
