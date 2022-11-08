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

package org.apache.doris.nereids.rules.analysis;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Resolve having clause to the aggregation.
 */
public class ResolveAggregateFunctions implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.RESOLVE_SORT_AGGREGATE_FUNCTIONS.build(
                        logicalSort(logicalAggregate())
                                .when(sort -> sort.getExpressions().stream()
                                        .anyMatch(e -> e.containsType(AggregateFunction.class)))
                                .then(sort -> {
                                    LogicalAggregate<GroupPlan> aggregate = sort.child();
                                    Resolver resolver = new Resolver(aggregate);
                                    sort.getExpressions().forEach(resolver::resolve);
                                    return createPlan(resolver, sort.child(), (r, a) -> {
                                        List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                                                .map(ok -> new OrderKey(
                                                        ExpressionUtils.replace(ok.getExpr(), r.getSubstitution()),
                                                        ok.isAsc(),
                                                        ok.isNullFirst()))
                                                .collect(ImmutableList.toImmutableList());
                                        return new LogicalSort<>(newOrderKeys, a);
                                    });
                                })
                ),
                RuleType.RESOLVE_SORT_HAVING_AGGREGATE_FUNCTIONS.build(
                        logicalSort(logicalHaving(logicalAggregate()))
                                .when(sort -> sort.getExpressions().stream()
                                        .anyMatch(e -> e.containsType(AggregateFunction.class)))
                                .then(sort -> {
                                    LogicalAggregate<GroupPlan> aggregate = sort.child().child();
                                    Resolver resolver = new Resolver(aggregate);
                                    sort.getExpressions().forEach(resolver::resolve);
                                    return createPlan(resolver, sort.child().child(), (r, a) -> {
                                        List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                                                .map(ok -> new OrderKey(
                                                        ExpressionUtils.replace(ok.getExpr(), r.getSubstitution()),
                                                        ok.isAsc(),
                                                        ok.isNullFirst()))
                                                .collect(ImmutableList.toImmutableList());
                                        return new LogicalSort<>(newOrderKeys, sort.child().withChildren(a));
                                    });
                                })
                ),
                RuleType.RESOLVE_HAVING_AGGREGATE_FUNCTIONS.build(
                        logicalHaving(logicalAggregate()).then(having -> {
                            LogicalAggregate<GroupPlan> aggregate = having.child();
                            Resolver resolver = new Resolver(aggregate);
                            resolver.resolve(having.getPredicates());
                            return createPlan(resolver, having.child(), (r, a) -> {
                                Expression newPredicates = ExpressionUtils.replace(
                                        having.getPredicates(), r.getSubstitution());
                                return new LogicalFilter<>(newPredicates, a);
                            });
                        })
                )
        );
    }

    static class Resolver {

        private final List<NamedExpression> outputExpressions;
        private final List<Expression> groupByExpressions;
        private final Map<Expression, Slot> substitution = Maps.newHashMap();
        private final List<NamedExpression> newOutputSlots = Lists.newArrayList();

        Resolver(LogicalAggregate<? extends Plan> aggregate) {
            outputExpressions = aggregate.getOutputExpressions();
            groupByExpressions = aggregate.getGroupByExpressions();
        }

        public void resolve(Expression expression) {
            Pair<Optional<Expression>, Boolean> result = lookUp(expression);
            Optional<Expression> found = result.first;
            boolean isFoundInOutputExpressions = result.second;

            if (found.isPresent()) {
                // If we found the equivalent slot or alias in the output expressions or group-by expressions,
                // We should replace the expression in having clause with the one in aggregation.
                if (found.get() instanceof NamedExpression) {
                    substitution.put(expression, ((NamedExpression) found.get()).toSlot());
                    if (!isFoundInOutputExpressions) {
                        // If the equivalent expression wasn't found in the output expressions, we should
                        // push it down to the aggregation.
                        newOutputSlots.add(((NamedExpression) found.get()).toSlot());
                    }
                } else {
                    // If the equivalent expression (neither slot nor alias) was found in group-by expressions (
                    // E.g. group by (a + 1) having (a + 1)), we should generate an alias for it and
                    // push it down to the aggregation.
                    generateAliasForNewOutputSlots(expression);
                }
            } else {
                // We couldn't find the equivalent expression in output expressions and group-by expressions,
                // so we should check whether the expression is valid.
                if (expression instanceof SlotReference) {
                    throw new AnalysisException(expression.toSql() + " in having clause should be grouped by.");
                } else if (expression instanceof AggregateFunction) {
                    if (checkWhetherNestedAggregateFunctionsExist((AggregateFunction) expression)) {
                        throw new AnalysisException("Aggregate functions in having clause can't be nested: "
                                + expression.toSql() + ".");
                    }
                    generateAliasForNewOutputSlots(expression);
                } else {
                    // Try to resolve the children.
                    for (Expression child : expression.children()) {
                        resolve(child);
                    }
                }
            }
        }

        /**
         * Look up the expression in aggregation.
         * @param expression Expression in predicates of having clause.
         * @return {@code Pair<Optional<Expression>, Boolean>}
         *     first: the expression in aggregation which is equivalent to input expression.
         *     second: whether the expression is found in output expressions of aggregation.
         */
        private Pair<Optional<Expression>, Boolean> lookUp(Expression expression) {
            Optional<Expression> found = outputExpressions.stream()
                    .filter(source -> isEquivalent(source, expression))
                    .map(source -> (Expression) source)
                    .findFirst();
            if (found.isPresent()) {
                return Pair.of(found, true);
            }

            found = groupByExpressions.stream().filter(source -> isEquivalent(source, expression)).findFirst();
            return Pair.of(found, false);
        }

        /**
         * Check whether the two expressions are equivalent.
         * @param source The expression in aggregation.
         * @param expression The expression used to compared to the one in aggregation.
         * @return true if the expressions are equivalent.
         */
        private boolean isEquivalent(Expression source, Expression expression) {
            if (source.equals(expression)) {
                return true;
            } else if (source instanceof Alias) {
                Alias alias = (Alias) source;
                return alias.toSlot().equals(expression) || alias.child().equals(expression);
            }
            return false;
        }

        private boolean checkWhetherNestedAggregateFunctionsExist(AggregateFunction function) {
            return function.children().stream().anyMatch(child -> child.anyMatch(AggregateFunction.class::isInstance));
        }

        private void generateAliasForNewOutputSlots(Expression expression) {
            Alias alias = new Alias(expression, expression.toSql());
            newOutputSlots.add(alias);
            substitution.put(expression, alias.toSlot());
        }

        public Map<Expression, Slot> getSubstitution() {
            return substitution;
        }

        public List<NamedExpression> getNewOutputSlots() {
            return newOutputSlots;
        }
    }

    interface PlanGenerator {
        Plan apply(Resolver resolver, LogicalAggregate<Plan> aggregate);
    }

    private Plan createPlan(Resolver resolver, LogicalAggregate<? extends Plan> aggregate,
            PlanGenerator planGenerator) {
        List<NamedExpression> projections = aggregate.getOutputExpressions().stream()
                .map(NamedExpression::toSlot).collect(Collectors.toList());
        List<NamedExpression> newOutputExpressions = Streams.concat(
                aggregate.getOutputExpressions().stream(), resolver.getNewOutputSlots().stream()
        ).collect(Collectors.toList());
        LogicalAggregate<Plan> newAggregate = aggregate.withGroupByAndOutput(
                aggregate.getGroupByExpressions(), newOutputExpressions);
        Plan plan = planGenerator.apply(resolver, newAggregate);
        return new LogicalProject<>(projections, plan);
    }
}
