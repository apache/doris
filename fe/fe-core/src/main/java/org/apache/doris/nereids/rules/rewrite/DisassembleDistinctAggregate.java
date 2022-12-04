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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.annotation.DependsRules;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.logical.NormalizeAggregate;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Used to generate the merge agg node for distributed execution.
 * NOTICE: DISTINCT GLOBAL output expressions' ExprId should SAME with ORIGIN output expressions' ExprId.
 * <pre>
 * If we have a query: SELECT COUNT(distinct v1 * v2) + 1 FROM t
 * the initial plan is:
 *   +-- Aggregate(phase: [LOCAL], outputExpr: [Alias(COUNT(distinct v1 * v2) + 1) #2])
 *       +-- childPlan
 * we should rewrite to:
 *   Aggregate(phase: [GLOBAL DISTINCT], outputExpr: [Alias(SUM(c) + 1) #2])
 *   +-- Aggregate(phase: [LOCAL DISTINCT], outputExpr: [SUM(b) as c] )
 *       +-- Aggregate(phase: [GLOBAL], outputExpr: [COUNT(distinct a) as b])
 *           +-- Aggregate(phase: [LOCAL], outputExpr: [COUNT(distinct v1 * v2) as a])
 *               +-- childPlan
 * </pre>
 */
@DependsRules(NormalizeAggregate.class)
public class DisassembleDistinctAggregate extends OneRewriteRuleFactory {

    @Override
    public Rule build() {
        return logicalAggregate()
                .then(this::disassembleAggregateFunction)
                .toRule(RuleType.DISTINCT_AGGREGATE_DISASSEMBLE);
    }

    private LogicalAggregate<Plan> disassembleAggregateFunction(LogicalAggregate<? extends Plan> aggregate) {
        // Double-check to prevent incorrect changes
        List<Expression> groupByExpressions = aggregate.getGroupByExpressions();
        if (groupByExpressions == null || groupByExpressions.isEmpty()) {
            // If there are no group by expressions, in order to parallelize,
            // we need to manually use the distinct function argument as group by expressions
            groupByExpressions = getDistinctFunctionParams(aggregate);
        }
        Pair<List<NamedExpression>, List<NamedExpression>> localAndGlobal =
                disassemble(aggregate.getOutputExpressions(), groupByExpressions,
                        AggPhase.LOCAL, AggPhase.GLOBAL);
        Pair<List<NamedExpression>, List<NamedExpression>> globalAndDistinctLocal =
                disassemble(localAndGlobal.second, groupByExpressions,
                        AggPhase.GLOBAL, AggPhase.DISTINCT_LOCAL);
        Pair<List<NamedExpression>, List<NamedExpression>> distinctLocalAndDistinctGlobal =
                disassemble(globalAndDistinctLocal.second, aggregate.getGroupByExpressions(),
                        AggPhase.DISTINCT_LOCAL, AggPhase.DISTINCT_GLOBAL);
        // generate new plan
        LogicalAggregate<Plan> localAggregate = new LogicalAggregate<>(
                groupByExpressions,
                localAndGlobal.first,
                true,
                aggregate.isNormalized(),
                aggregate.getSourceRepeat(),
                aggregate.child()
        );
        LogicalAggregate<LogicalAggregate<Plan>> globalAggregate = new LogicalAggregate<>(
                groupByExpressions,
                globalAndDistinctLocal.first,
                true,
                aggregate.isNormalized(),
                aggregate.getSourceRepeat(),
                localAggregate
        );
        LogicalAggregate<LogicalAggregate<LogicalAggregate<Plan>>> distinctLocalAggregate =
                new LogicalAggregate<>(
                        aggregate.getGroupByExpressions(),
                        distinctLocalAndDistinctGlobal.first,
                        true,
                        aggregate.isNormalized(),
                        aggregate.getSourceRepeat(),
                        globalAggregate
                );
        return new LogicalAggregate<>(
                aggregate.getGroupByExpressions(),
                distinctLocalAndDistinctGlobal.second,
                true,
                aggregate.isNormalized(),
                aggregate.getSourceRepeat(),
                distinctLocalAggregate
        );
    }

    private Pair<List<NamedExpression>, List<NamedExpression>> disassemble(
            List<NamedExpression> originOutputExprs, // agg.output
            List<Expression> childGroupByExprs, // distinct params
            AggPhase childPhase,
            AggPhase parentPhase) {
        Map<Expression, Expression> inputSubstitutionMap = Maps.newHashMap();

        List<NamedExpression> childOutputExprs = Lists.newArrayList();
        // The groupBy slots are placed at the beginning of the output, in line with the stale optimiser
        childGroupByExprs.stream().forEach(expression -> childOutputExprs.add((SlotReference) expression));
        for (NamedExpression originOutputExpr : originOutputExprs) {
            Set<AggregateFunction> aggregateFunctions
                    = originOutputExpr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                if (inputSubstitutionMap.containsKey(aggregateFunction)) {
                    continue;
                }
                AggregateFunction childAggregateFunction = aggregateFunction.withAggregateParam(
                        aggregateFunction.getAggregateParam()
                                .withPhaseAndDisassembled(false, childPhase, true)
                );
                NamedExpression childOutputExpr = new Alias(childAggregateFunction, aggregateFunction.toSql());
                AggregateFunction substitutionValue = aggregateFunction
                        // save the origin input types to the global aggregate functions
                        .withAggregateParam(aggregateFunction.getAggregateParam()
                                .withPhaseAndDisassembled(true, parentPhase, true))
                        .withChildren(Lists.newArrayList(childOutputExpr.toSlot()));

                inputSubstitutionMap.put(aggregateFunction, substitutionValue);
                childOutputExprs.add(childOutputExpr);
            }
        }

        // 3. replace expression in parentOutputExprs
        List<NamedExpression> parentOutputExprs = originOutputExprs.stream()
                .map(e -> ExpressionUtils.replace(e, inputSubstitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        return Pair.of(childOutputExprs, parentOutputExprs);
    }

    private List<Expression> getDistinctFunctionParams(LogicalAggregate<? extends Plan> agg) {
        Builder<Expression> params = ImmutableList.builder();
        for (NamedExpression originOutputExpr : agg.getOutputExpressions()) {
            Set<AggregateFunction> aggregateFunctions
                    = originOutputExpr.collect(AggregateFunction.class::isInstance);
            for (AggregateFunction aggregateFunction : aggregateFunctions) {
                if (aggregateFunction.isDistinct()) {
                    params.addAll(aggregateFunction.children());
                }
            }
        }
        return params.build();
    }
}
