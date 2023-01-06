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

package org.apache.doris.nereids.rules.rewrite.logical;

import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.OneRewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

/**
 * normalize aggregate's group keys and AggregateFunction's child to SlotReference
 * and generate a LogicalProject top on LogicalAggregate to hold to order of aggregate output,
 * since aggregate output's order could change when we do translate.
 * <p>
 * Apply this rule could simplify the processing of enforce and translate.
 * <pre>
 * Original Plan:
 * Aggregate(
 *   keys:[k1#1, K2#2 + 1],
 *   outputs:[k1#1, Alias(K2# + 1)#4, Alias(k1#1 + 1)#5, Alias(SUM(v1#3))#6,
 *            Alias(SUM(v1#3 + 1))#7, Alias(SUM(v1#3) + 1)#8])
 * </pre>
 * After rule:
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 *   +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * <p>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate extends OneRewriteRuleFactory implements NormalizeToSlot {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {
            // push expression to bottom project
            Set<Alias> existsAliases = ExpressionUtils.collect(
                    aggregate.getOutputExpressions(), Alias.class::isInstance);
            Set<Expression> needToSlots = collectGroupByAndArgumentsOfAggregateFunctions(aggregate);
            NormalizeToSlotContext groupByAndArgumentToSlotContext =
                    NormalizeToSlotContext.buildContext(existsAliases, needToSlots);
            Set<NamedExpression> bottomProjects =
                    groupByAndArgumentToSlotContext.pushDownToNamedExpression(needToSlots);
            Plan normalizedChild = bottomProjects.isEmpty()
                    ? aggregate.child()
                    : new LogicalProject<>(ImmutableList.copyOf(bottomProjects), aggregate.child());

            // begin normalize aggregate

            // replace groupBy and arguments of aggregate function to slot, may be this output contains
            // some expression on the aggregate functions, e.g. `sum(value) + 1`, we should replace
            // the sum(value) to slot and move the `slot + 1` to the upper project later.
            List<NamedExpression> normalizeOutputPhase1 = groupByAndArgumentToSlotContext
                    .normalizeToUseSlotRef(aggregate.getOutputExpressions());
            Set<AggregateFunction> normalizedAggregateFunctions =
                    ExpressionUtils.collect(normalizeOutputPhase1, AggregateFunction.class::isInstance);

            existsAliases = ExpressionUtils.collect(normalizeOutputPhase1, Alias.class::isInstance);

            // now reuse the exists alias for the aggregate functions,
            // or create new alias for the aggregate functions
            NormalizeToSlotContext aggregateFunctionToSlotContext =
                    NormalizeToSlotContext.buildContext(existsAliases, normalizedAggregateFunctions);

            Set<NamedExpression> normalizedAggregateFunctionsWithAlias =
                    aggregateFunctionToSlotContext.pushDownToNamedExpression(normalizedAggregateFunctions);

            List<Slot> normalizedGroupBy =
                    (List) groupByAndArgumentToSlotContext
                            .normalizeToUseSlotRef(aggregate.getGroupByExpressions());

            // we can safely add all groupBy and aggregate functions to output, because we will
            // add a project on it, and the upper project can protect the scope of visible of slot
            List<NamedExpression> normalizedAggregateOutput = ImmutableList.<NamedExpression>builder()
                    .addAll(normalizedGroupBy)
                    .addAll(normalizedAggregateFunctionsWithAlias)
                    .build();

            LogicalAggregate<Plan> normalizedAggregate = aggregate.withNormalized(
                    (List) normalizedGroupBy, normalizedAggregateOutput, normalizedChild);

            // replace aggregate function to slot
            List<NamedExpression> upperProjects =
                    aggregateFunctionToSlotContext.normalizeToUseSlotRef(normalizeOutputPhase1);
            return new LogicalProject<>(upperProjects, normalizedAggregate);
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    private Set<Expression> collectGroupByAndArgumentsOfAggregateFunctions(LogicalAggregate<? extends Plan> aggregate) {
        // 2 parts need push down:
        // groupingByExpr, argumentsOfAggregateFunction

        Set<Expression> groupingByExpr = ImmutableSet.copyOf(aggregate.getGroupByExpressions());

        Set<AggregateFunction> aggregateFunctions = ExpressionUtils.collect(
                aggregate.getOutputExpressions(), AggregateFunction.class::isInstance);

        ImmutableSet<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(function -> function.getArguments().stream())
                .collect(ImmutableSet.toImmutableSet());

        ImmutableSet<Expression> needPushDown = ImmutableSet.<Expression>builder()
                // group by should be pushed down, e.g. group by (k + 1),
                // we should push down the `k + 1` to the bottom plan
                .addAll(groupingByExpr)
                // e.g. sum(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfAggregateFunction)
                .build();
        return needPushDown;
    }
}
