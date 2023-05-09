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
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
 * <pre>
 * Project(k1#1, Alias(SR#9)#4, Alias(k1#1 + 1)#5, Alias(SR#10))#6, Alias(SR#11))#7, Alias(SR#10 + 1)#8)
 * +-- Aggregate(keys:[k1#1, SR#9], outputs:[k1#1, SR#9, Alias(SUM(v1#3))#10, Alias(SUM(v1#3 + 1))#11])
 *   +-- Project(k1#1, Alias(K2#2 + 1)#9, v1#3)
 * </pre>
 * Note: window function will be moved to upper project
 * all agg functions except the top agg should be pushed to Aggregate node.
 * example 1:
 * <pre>
 *    select min(x), sum(x) over () ...
 * the 'sum(x)' is top agg of window function, it should be moved to upper project
 * plan:
 *    project(sum(x) over())
 *        Aggregate(min(x), x)
 * </pre>
 * example 2:
 * <pre>
 *    select min(x), avg(sum(x)) over() ...
 * the 'sum(x)' should be moved to Aggregate
 * plan:
 *    project(avg(y) over())
 *         Aggregate(min(x), sum(x) as y)
 * </pre>
 * example 3:
 * <pre>
 *    select sum(x+1), x+1, sum(x+1) over() ...
 * window function should use x instead of x+1
 * plan:
 *    project(sum(x+1) over())
 *        Agg(sum(y), x)
 *            project(x+1 as y)
 * </pre>
 * More example could get from UT {NormalizeAggregateTest}
 */
public class NormalizeAggregate extends OneRewriteRuleFactory implements NormalizeToSlot {
    @Override
    public Rule build() {
        return logicalAggregate().whenNot(LogicalAggregate::isNormalized).then(aggregate -> {
            // push expression to bottom project
            Set<Alias> existsAliases = ExpressionUtils.mutableCollect(
                    aggregate.getOutputExpressions(), Alias.class::isInstance);
            Set<AggregateFunction> aggregateFunctionsInWindow = collectAggregateFunctionsInWindow(
                    aggregate.getOutputExpressions());
            Set<Expression> existsAggAlias = existsAliases.stream().map(UnaryNode::child)
                    .filter(AggregateFunction.class::isInstance)
                    .collect(Collectors.toSet());

            /*
             * agg-functions inside window function is regarded as an output of aggregate.
             * select sum(avg(c)) over ...
             * is regarded as
             * select avg(c), sum(avg(c)) over ...
             *
             * the plan:
             * project(sum(y) over)
             *    Aggregate(avg(c) as y)
             *
             * after Aggregate, the 'y' is removed by upper project.
             *
             * aliasOfAggFunInWindowUsedAsAggOutput = {alias(avg(c))}
             */
            List<Alias> aliasOfAggFunInWindowUsedAsAggOutput = Lists.newArrayList();

            for (AggregateFunction aggFun : aggregateFunctionsInWindow) {
                if (!existsAggAlias.contains(aggFun)) {
                    Alias alias = new Alias(aggFun, aggFun.toSql());
                    existsAliases.add(alias);
                    aliasOfAggFunInWindowUsedAsAggOutput.add(alias);
                }
            }
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
            List<NamedExpression> normalizeOutputPhase1 = Stream.concat(
                            aggregate.getOutputExpressions().stream(),
                            aliasOfAggFunInWindowUsedAsAggOutput.stream())
                    .map(expr -> groupByAndArgumentToSlotContext
                            .normalizeToUseSlotRefUp(expr, WindowExpression.class::isInstance))
                    .collect(Collectors.toList());

            Set<Slot> windowInputSlots = collectWindowInputSlots(aggregate.getOutputExpressions());
            Set<Expression> itemsInWindow = Sets.newHashSet(windowInputSlots);
            itemsInWindow.addAll(aggregateFunctionsInWindow);
            NormalizeToSlotContext windowToSlotContext =
                    NormalizeToSlotContext.buildContext(existsAliases, itemsInWindow);
            normalizeOutputPhase1 = normalizeOutputPhase1.stream()
                    .map(expr -> windowToSlotContext
                            .normalizeToUseSlotRefDown(expr, WindowExpression.class::isInstance, true))
                    .collect(Collectors.toList());

            Set<AggregateFunction> normalizedAggregateFunctions =
                    collectNonWindowedAggregateFunctions(normalizeOutputPhase1);

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

            normalizeOutputPhase1.removeAll(aliasOfAggFunInWindowUsedAsAggOutput);
            // exclude same-name functions in WindowExpression
            List<NamedExpression> upperProjects = normalizeOutputPhase1.stream()
                    .map(aggregateFunctionToSlotContext::normalizeToUseSlotRef).collect(Collectors.toList());
            return new LogicalProject<>(upperProjects, normalizedAggregate);
        }).toRule(RuleType.NORMALIZE_AGGREGATE);
    }

    private Set<Expression> collectGroupByAndArgumentsOfAggregateFunctions(LogicalAggregate<? extends Plan> aggregate) {
        // 2 parts need push down:
        // groupingByExpr, argumentsOfAggregateFunction

        Set<Expression> groupingByExpr = ImmutableSet.copyOf(aggregate.getGroupByExpressions());

        Set<AggregateFunction> aggregateFunctions = collectNonWindowedAggregateFunctions(
                aggregate.getOutputExpressions());

        Set<Expression> argumentsOfAggregateFunction = aggregateFunctions.stream()
                .flatMap(function -> function.getArguments().stream()
                        .map(expr -> expr instanceof OrderExpression ? expr.child(0) : expr))
                .collect(ImmutableSet.toImmutableSet());

        Set<Expression> windowFunctionKeys = collectWindowFunctionKeys(aggregate.getOutputExpressions());

        Set<Expression> needPushDown = ImmutableSet.<Expression>builder()
                // group by should be pushed down, e.g. group by (k + 1),
                // we should push down the `k + 1` to the bottom plan
                .addAll(groupingByExpr)
                // e.g. sum(k + 1), we should push down the `k + 1` to the bottom plan
                .addAll(argumentsOfAggregateFunction)
                .addAll(windowFunctionKeys)
                .build();
        return needPushDown;
    }

    private Set<Expression> collectWindowFunctionKeys(List<NamedExpression> aggOutput) {
        Set<Expression> windowInputs = Sets.newHashSet();
        for (Expression expr : aggOutput) {
            Set<WindowExpression> windows = expr.collect(WindowExpression.class::isInstance);
            for (WindowExpression win : windows) {
                windowInputs.addAll(win.getPartitionKeys().stream().flatMap(pk -> pk.getInputSlots().stream()).collect(
                        Collectors.toList()));
                windowInputs.addAll(win.getOrderKeys().stream().flatMap(ok -> ok.getInputSlots().stream()).collect(
                        Collectors.toList()));
            }
        }
        return windowInputs;
    }

    /**
     * select sum(c2), avg(min(c2)) over (partition by max(c1) order by count(c1)) from T ...
     * extract {sum, min, max, count}. avg is not extracted.
     */
    private Set<AggregateFunction> collectNonWindowedAggregateFunctions(List<NamedExpression> aggOutput) {
        return ExpressionUtils.collect(aggOutput, expr -> {
            if (expr instanceof AggregateFunction) {
                return !((AggregateFunction) expr).isWindowFunction();
            }
            return false;
        });
    }

    private Set<AggregateFunction> collectAggregateFunctionsInWindow(List<NamedExpression> aggOutput) {

        List<WindowExpression> windows = Lists.newArrayList(
                ExpressionUtils.collect(aggOutput, WindowExpression.class::isInstance));
        return ExpressionUtils.collect(windows, expr -> {
            if (expr instanceof AggregateFunction) {
                return !((AggregateFunction) expr).isWindowFunction();
            }
            return false;
        });
    }

    private Set<Slot> collectWindowInputSlots(List<NamedExpression> aggOutput) {
        List<WindowExpression> windows = Lists.newArrayList(
                ExpressionUtils.collect(aggOutput, WindowExpression.class::isInstance));
        return windows.stream().flatMap(win -> win.getInputSlots().stream()).collect(Collectors.toSet());
    }
}
