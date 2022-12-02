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

import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.trees.expressions.AggregateExpression;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateParam;
import org.apache.doris.nereids.trees.plans.AggMode;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Used to generate the merge agg node for distributed execution.
 * NOTICE: GLOBAL output expressions' ExprId should SAME with ORIGIN output expressions' ExprId.
 * <pre>
 * If we have a query: SELECT SUM(v1 * v2) + 1 FROM t GROUP BY k + 1
 * the initial plan is:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(k + 1) #1, Alias(SUM(v1 * v2) + 1) #2], groupByExpr: [k + 1])
 *   +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [GLOBAL], outputExpr: [Alias(b) #1, Alias(SUM(a) + 1) #2], groupByExpr: [b])
 *   +-- Aggregate(phase: [LOCAL], outputExpr: [SUM(v1 * v2) as a, (k + 1) as b], groupByExpr: [k + 1])
 *       +-- childPlan
 * </pre>
 *
 * TODO:
 *     1. if instance count is 1, shouldn't disassemble the agg plan
 */
@DependsRules(NormalizeAggregate.class)
public class DisassembleAggregate extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalAggregate()
                .when(agg -> agg.isNormalized() && agg.getAggMode() == AggMode.INPUT_TO_RESULT)
                .then(this::disassembleAggregateFunction)
                .toRule(RuleType.AGGREGATE_DISASSEMBLE);
    }

    private LogicalAggregate<LogicalAggregate<? extends Plan>> disassembleAggregateFunction(
            LogicalAggregate<? extends Plan> aggregate) {
        Set<AggregateExpression> aggregateExpressions = ExpressionUtils.collect(
                aggregate.getOutputExpressions(), AggregateExpression.class::isInstance);
        Map<AggregateExpression, Alias> inputToBufferAliases = aggregateExpressions.stream()
                .collect(ImmutableMap.toImmutableMap(expr -> expr, expr -> {
                    AggregateParam inputToBufferParam = new AggregateParam(
                            expr.isDistinct(), false, AggPhase.LOCAL,
                            AggMode.INPUT_TO_BUFFER, true);
                    AggregateExpression inputToBuffer =
                            new AggregateExpression(expr.getFunction(), inputToBufferParam);
                    return new Alias(inputToBuffer, inputToBuffer.toSql());
                }));

        List<NamedExpression> inputToBufferOutput = ImmutableList.<NamedExpression>builder()
                // we already normalized the group by expressions to List<Slot> by the NormalizeAggregate rule
                .addAll((List) aggregate.getGroupByExpressions())
                .addAll(inputToBufferAliases.values())
                .build();
        LogicalAggregate<? extends Plan> inputToBufferAgg = aggregate.withDisassemble(
                inputToBufferOutput, AggPhase.LOCAL, AggMode.INPUT_TO_BUFFER, false, aggregate.child());

        List<NamedExpression> bufferToResultOutput = ExpressionUtils.rewriteDownShortCircuit(
                aggregate.getOutputExpressions(), outputChild -> {
                    Alias inputToBufferAlias = inputToBufferAliases.get(outputChild);
                    if (inputToBufferAlias == null) {
                        return outputChild;
                    }
                    AggregateExpression inputToResult = (AggregateExpression) outputChild;
                    AggregateParam bufferToResultParam = new AggregateParam(
                            inputToResult.isDistinct(), true, AggPhase.GLOBAL,
                            AggMode.BUFFER_TO_RESULT, true);
                    return new AggregateExpression(inputToResult.getFunction(),
                            bufferToResultParam, inputToBufferAlias.toSlot());
                });

        return aggregate.withDisassemble(
                bufferToResultOutput, AggPhase.GLOBAL, AggMode.BUFFER_TO_RESULT, true, inputToBufferAgg);
    }
}
