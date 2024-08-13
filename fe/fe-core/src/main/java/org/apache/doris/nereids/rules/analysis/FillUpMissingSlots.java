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
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Resolve having clause to the aggregation/repeat.
 * need Top to Down to traverse plan,
 * because we need to process FILL_UP_SORT_HAVING_AGGREGATE before FILL_UP_HAVING_AGGREGATE.
 */
public class FillUpMissingSlots implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FILL_UP_SORT_PROJECT.build(
                logicalSort(logicalProject())
                    .then(sort -> {
                        LogicalProject<Plan> project = sort.child();
                        Set<Slot> projectOutputSet = project.getOutputSet();
                        Set<Slot> notExistedInProject = sort.getOrderKeys().stream()
                                .map(OrderKey::getExpr)
                                .map(Expression::getInputSlots)
                                .flatMap(Set::stream)
                                .filter(s -> !projectOutputSet.contains(s))
                                .collect(Collectors.toSet());
                        if (notExistedInProject.isEmpty()) {
                            return null;
                        }
                        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                                .addAll(project.getProjects()).addAll(notExistedInProject).build();
                        return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()),
                                sort.withChildren(new LogicalProject<>(projects, project.child())));
                    })
            ),
            RuleType.FILL_UP_SORT_AGGREGATE_HAVING_AGGREGATE.build(
                logicalSort(
                    aggregate(logicalHaving(aggregate()))
                        .when(a -> a.getOutputExpressions().stream().allMatch(SlotReference.class::isInstance))
                ).when(this::checkSort)
                    .then(sort -> processDistinctProjectWithAggregate(sort, sort.child(), sort.child().child().child()))
            ),
            // ATTN: process aggregate with distinct project, must run this rule before FILL_UP_SORT_AGGREGATE
            //   because this pattern will always fail in FILL_UP_SORT_AGGREGATE
            RuleType.FILL_UP_SORT_AGGREGATE_AGGREGATE.build(
                logicalSort(
                    aggregate(aggregate())
                        .when(a -> a.getOutputExpressions().stream().allMatch(SlotReference.class::isInstance))
                ).when(this::checkSort)
                    .then(sort -> processDistinctProjectWithAggregate(sort, sort.child(), sort.child().child()))
            ),
            RuleType.FILL_UP_SORT_AGGREGATE.build(
                logicalSort(aggregate())
                    .when(this::checkSort)
                    .then(sort -> {
                        Aggregate<Plan> agg = sort.child();
                        Resolver resolver = new Resolver(agg);
                        sort.getExpressions().forEach(resolver::resolve);
                        return createPlan(resolver, agg, (r, a) -> {
                            List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                                    .map(ok -> new OrderKey(
                                            ExpressionUtils.replace(ok.getExpr(), r.getSubstitution()),
                                            ok.isAsc(),
                                            ok.isNullFirst()))
                                    .collect(ImmutableList.toImmutableList());
                            boolean notChanged = newOrderKeys.equals(sort.getOrderKeys());
                            if (notChanged && a.equals(agg)) {
                                return null;
                            }
                            return notChanged ? sort.withChildren(a) : new LogicalSort<>(newOrderKeys, a);
                        });
                    })
            ),
            RuleType.FILL_UP_SORT_HAVING_AGGREGATE.build(
                logicalSort(logicalHaving(aggregate()))
                    .when(this::checkSort)
                    .then(sort -> {
                        LogicalHaving<Aggregate<Plan>> having = sort.child();
                        Aggregate<Plan> agg = having.child();
                        Resolver resolver = new Resolver(agg);
                        sort.getExpressions().forEach(resolver::resolve);
                        return createPlan(resolver, agg, (r, a) -> {
                            List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                                    .map(key -> key.withExpression(
                                            ExpressionUtils.replace(key.getExpr(), r.getSubstitution())))
                                    .collect(ImmutableList.toImmutableList());
                            boolean notChanged = newOrderKeys.equals(sort.getOrderKeys());
                            if (notChanged && a.equals(agg)) {
                                return null;
                            }
                            return notChanged ? sort.withChildren(sort.child().withChildren(a))
                                    : new LogicalSort<>(newOrderKeys, sort.child().withChildren(a));
                        });
                    })
            ),
            RuleType.FILL_UP_SORT_HAVING_PROJECT.build(
                    logicalSort(logicalHaving(logicalProject())).then(sort -> {
                        Set<Slot> childOutput = sort.child().getOutputSet();
                        Set<Slot> notExistedInProject = sort.getOrderKeys().stream()
                                .map(OrderKey::getExpr)
                                .map(Expression::getInputSlots)
                                .flatMap(Set::stream)
                                .filter(s -> !childOutput.contains(s))
                                .collect(Collectors.toSet());
                        if (notExistedInProject.isEmpty()) {
                            return null;
                        }
                        LogicalProject<?> project = sort.child().child();
                        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                                .addAll(project.getProjects())
                                .addAll(notExistedInProject).build();
                        Plan child = sort.withChildren(sort.child().withChildren(project.withProjects(projects)));
                        return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()), child);
                    })
            ),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalHaving(aggregate()).then(having -> {
                    Aggregate<Plan> agg = having.child();
                    Resolver resolver = new Resolver(agg);
                    having.getConjuncts().forEach(resolver::resolve);
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                having.getConjuncts(), r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(having.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? having.withChildren(a) : new LogicalHaving<>(newConjuncts, a);
                    });
                })
            ),
            // Convert having to filter
            RuleType.FILL_UP_HAVING_PROJECT.build(
                    logicalHaving(logicalProject()).then(having -> {
                        if (having.getExpressions().stream().anyMatch(e -> e.containsType(AggregateFunction.class))) {
                            // This is very weird pattern.
                            // There are some aggregate functions in having, but its child is project.
                            // There are some slot from project in having too.
                            // Having should execute after project.
                            // But aggregate function should execute before project.
                            // Since no aggregate here, we should add an empty aggregate before project.
                            // We should push aggregate function into aggregate node first.
                            // Then put aggregate result slots and original project slots into new project.
                            // The final plan should be
                            //   Having
                            //   +-- Project
                            //       +-- Aggregate
                            // Since aggregate node have no group by key.
                            // So project should not contain any slot from its original child.
                            // Or otherwise slot cannot find will be thrown.
                            LogicalProject<Plan> project = having.child();
                            // new an empty agg here
                            LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                                    ImmutableList.of(), ImmutableList.of(), project.child());
                            // avoid throw exception even if having have slot from its child.
                            // because we will add a project between having and project.
                            Resolver resolver = new Resolver(agg, false);
                            having.getConjuncts().forEach(resolver::resolve);
                            agg = agg.withAggOutput(resolver.getNewOutputSlots());
                            Set<Expression> newConjuncts = ExpressionUtils.replace(
                                    having.getConjuncts(), resolver.getSubstitution());
                            ImmutableList.Builder<NamedExpression> projects = ImmutableList.builder();
                            projects.addAll(project.getOutputs()).addAll(agg.getOutput());
                            return new LogicalHaving<>(newConjuncts, new LogicalProject<>(projects.build(), agg));
                        } else {
                            LogicalProject<Plan> project = having.child();
                            Set<Slot> projectOutputSet = project.getOutputSet();
                            Set<Slot> notExistedInProject = having.getExpressions().stream()
                                    .map(Expression::getInputSlots)
                                    .flatMap(Set::stream)
                                    .filter(s -> !projectOutputSet.contains(s))
                                    .collect(Collectors.toSet());
                            if (notExistedInProject.isEmpty()) {
                                return null;
                            }
                            List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                                    .addAll(project.getProjects()).addAll(notExistedInProject).build();
                            return new LogicalProject<>(ImmutableList.copyOf(project.getOutput()),
                                    having.withChildren(new LogicalProject<>(projects, project.child())));
                        }
                    })
            )
        );
    }

    static class Resolver {

        private final List<NamedExpression> outputExpressions;
        private final List<Expression> groupByExpressions;
        private final Map<Expression, Slot> substitution = Maps.newHashMap();
        private final List<NamedExpression> newOutputSlots = Lists.newArrayList();
        private final Map<Slot, Expression> outputSubstitutionMap;
        private final boolean checkSlot;

        Resolver(Aggregate<?> aggregate, boolean checkSlot) {
            outputExpressions = aggregate.getOutputExpressions();
            groupByExpressions = aggregate.getGroupByExpressions();
            outputSubstitutionMap = outputExpressions.stream().filter(Alias.class::isInstance)
                    .collect(Collectors.toMap(NamedExpression::toSlot, alias -> alias.child(0),
                            (k1, k2) -> k1));
            this.checkSlot = checkSlot;
        }

        Resolver(Aggregate<?> aggregate) {
            this(aggregate, true);
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
                    if (checkSlot) {
                        throw new AnalysisException(expression.toSql() + " should be grouped by.");
                    }
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

        private boolean checkWhetherNestedAggregateFunctionsExist(AggregateFunction aggregateFunction) {
            return aggregateFunction.children()
                    .stream()
                    .anyMatch(child -> child.anyMatch(AggregateFunction.class::isInstance));
        }

        private void generateAliasForNewOutputSlots(Expression expression) {
            Expression replacedExpr = ExpressionUtils.replace(expression, outputSubstitutionMap);
            Alias alias = new Alias(replacedExpr);
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
        Plan apply(Resolver resolver, Aggregate<?> aggregate);
    }

    private Plan createPlan(Resolver resolver, Aggregate<? extends Plan> aggregate, PlanGenerator planGenerator) {
        Aggregate<? extends Plan> newAggregate;
        if (resolver.getNewOutputSlots().isEmpty()) {
            newAggregate = aggregate;
        } else {
            List<NamedExpression> newOutputExpressions = Streams
                    .concat(aggregate.getOutputExpressions().stream(), resolver.getNewOutputSlots().stream())
                    .collect(ImmutableList.toImmutableList());
            newAggregate = aggregate.withAggOutput(newOutputExpressions);
        }
        Plan plan = planGenerator.apply(resolver, newAggregate);
        if (plan == null) {
            return null;
        }
        List<NamedExpression> projections = aggregate.getOutputExpressions().stream()
                .map(NamedExpression::toSlot).collect(ImmutableList.toImmutableList());
        return new LogicalProject<>(projections, plan);
    }

    private boolean checkSort(LogicalSort<? extends Plan> logicalSort) {
        Plan child = logicalSort.child();
        for (OrderKey orderKey : logicalSort.getOrderKeys()) {
            Expression expr = orderKey.getExpr();
            if (expr.anyMatch(AggregateFunction.class::isInstance)) {
                return true;
            }
            for (Slot inputSlot : expr.getInputSlots()) {
                if (!child.getOutputSet().contains(inputSlot)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * for sql like SELECT DISTINCT a FROM t GROUP BY a HAVING b > 0 ORDER BY a.
     * there order by need to bind with bottom aggregate's output and bottom aggregate's child's output.
     * this function used to fill up missing slot for these situations correctly.
     *
     * @param sort top sort
     * @param upperAggregate upper aggregate used to check slot in order by should be in select list
     * @param bottomAggregate bottom aggregate used to bind with its and its child's output
     *
     * @return filled up plan
     */
    private Plan processDistinctProjectWithAggregate(LogicalSort<?> sort,
            Aggregate<?> upperAggregate, Aggregate<Plan> bottomAggregate) {
        Resolver resolver = new Resolver(bottomAggregate);
        sort.getExpressions().forEach(resolver::resolve);
        return createPlan(resolver, bottomAggregate, (r, a) -> {
            List<OrderKey> newOrderKeys = sort.getOrderKeys().stream()
                    .map(ok -> new OrderKey(
                            ExpressionUtils.replace(ok.getExpr(), r.getSubstitution()),
                            ok.isAsc(),
                            ok.isNullFirst()))
                    .collect(ImmutableList.toImmutableList());
            boolean sortNotChanged = newOrderKeys.equals(sort.getOrderKeys());
            boolean aggNotChanged = a.equals(bottomAggregate);
            if (sortNotChanged && aggNotChanged) {
                return null;
            }
            if (aggNotChanged) {
                // since sort expr must in select list, we should not change agg at all.
                return new LogicalSort<>(newOrderKeys, sort.child());
            } else {
                Set<NamedExpression> upperAggOutputs = Sets.newHashSet(upperAggregate.getOutputExpressions());
                for (int i = 0; i < newOrderKeys.size(); i++) {
                    OrderKey orderKey = newOrderKeys.get(i);
                    Expression expression = orderKey.getExpr();
                    if (!upperAggOutputs.containsAll(expression.getInputSlots())) {
                        throw new AnalysisException(sort.getOrderKeys().get(i).getExpr().toSql()
                                + " of ORDER BY clause is not in SELECT list");
                    }
                }
                throw new AnalysisException("Expression of ORDER BY clause is not in SELECT list");
            }
        });
    }
}
