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
import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Resolve having clause to the aggregation/repeat.
 * need Top to Down to traverse plan,
 * because we need to process FILL_UP_SORT_HAVING_AGGREGATE before FILL_UP_HAVING_AGGREGATE.
 * be aware that when filling up the missing slots, we should exclude outer query's correlated slots.
 * because these correlated slots belong to outer query, so should not try to find them in child node.
 */
public class FillUpMissingSlots implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FILL_UP_SORT_AGGREGATE_HAVING_AGGREGATE.build(
                logicalSort(
                    aggregate(logicalHaving(aggregate()))
                        .when(a -> a.getOutputExpressions().stream().allMatch(SlotReference.class::isInstance))
                ).when(this::checkSort)
                    .thenApply(ctx -> processDistinctProjectWithAggregate(ctx.root,
                            ctx.root.child(), ctx.root.child().child().child(),
                            ctx.cascadesContext.getOuterScope()))
            ),
            // ATTN: process aggregate with distinct project, must run this rule before FILL_UP_SORT_AGGREGATE
            //   because this pattern will always fail in FILL_UP_SORT_AGGREGATE
            RuleType.FILL_UP_SORT_AGGREGATE_AGGREGATE.build(
                logicalSort(
                    aggregate(aggregate())
                        .when(a -> a.getOutputExpressions().stream().allMatch(SlotReference.class::isInstance))
                ).when(this::checkSort)
                    .thenApply(ctx -> processDistinctProjectWithAggregate(ctx.root,
                            ctx.root.child(), ctx.root.child().child(),
                            ctx.cascadesContext.getOuterScope()))
            ),
            RuleType.FILL_UP_SORT_AGGREGATE.build(
                logicalSort(aggregate())
                    .when(this::checkSort)
                    .thenApply(ctx -> {
                        LogicalSort<Aggregate<Plan>> sort = ctx.root;
                        Aggregate<Plan> agg = sort.child();
                        Resolver resolver = new Resolver(agg, ctx.cascadesContext.getOuterScope());
                        sort.getExpressions().forEach(expr -> resolver.resolve(expr, ResolvePlanType.SORT));
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
                    .thenApply(ctx -> {
                        LogicalSort<LogicalHaving<Aggregate<Plan>>> sort = ctx.root;
                        LogicalHaving<Aggregate<Plan>> having = sort.child();
                        Aggregate<Plan> agg = having.child();
                        Resolver resolver = new Resolver(agg, ctx.cascadesContext.getOuterScope());
                        sort.getExpressions().forEach(expr -> resolver.resolve(expr, ResolvePlanType.SORT));
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
            // handle with:
            // sort -> having -> project
            // sort -> project
            RuleType.FILL_UP_SORT_PROJECT.build(
                    logicalSort().thenApply(ctx -> {
                        LogicalSort<Plan> sort = ctx.root;
                        Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                        return processWithSortHavingProject(sort, outerScope);
                    })
            ),
            RuleType.FILL_UP_HAVING_AGGREGATE.build(
                logicalHaving(aggregate()).thenApply(ctx -> {
                    LogicalHaving<Aggregate<Plan>> having = ctx.root;
                    Aggregate<Plan> agg = having.child();
                    Resolver resolver = new Resolver(agg, ctx.cascadesContext.getOuterScope());
                    having.getConjuncts().forEach(expr -> resolver.resolve(expr, ResolvePlanType.HAVING));
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
            // having -> project
            RuleType.FILL_UP_HAVING_PROJECT.build(
                    logicalHaving().thenApply(ctx -> {
                        LogicalHaving<Plan> having = ctx.root;
                        Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                        return processWithSortHavingProject(having, outerScope);
                    })
             )
        );
    }

    /**
     * The type of plan to resolve.
     */
    public enum ResolvePlanType {
        PROJECT,
        HAVING,
        QUALIFY,
        SORT,
    }

    static class Resolver {

        private final List<NamedExpression> outputExpressions;
        private final List<Expression> groupByExpressions;
        private final Map<Expression, Slot> substitution = Maps.newHashMap();
        private final List<NamedExpression> newOutputSlots = Lists.newArrayList();
        private final Map<Slot, Expression> outputSubstitutionMap;
        private final Optional<Scope> outerScope;
        private final boolean isRepeat;

        Resolver(Aggregate<?> aggregate, Optional<Scope> outerScope) {
            outputExpressions = aggregate.getOutputExpressions();
            groupByExpressions = aggregate.getGroupByExpressions();
            outputSubstitutionMap = outputExpressions.stream().filter(Alias.class::isInstance)
                    .collect(Collectors.toMap(NamedExpression::toSlot, alias -> alias.child(0),
                            (k1, k2) -> k1));
            this.outerScope = outerScope;
            this.isRepeat = aggregate instanceof LogicalRepeat;
        }

        Resolver(Aggregate<?> aggregate) {
            this(aggregate, Optional.empty());
        }

        public void resolve(Expression expression, ResolvePlanType planType) {
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
                    if ((!outerScope.isPresent()
                            || !outerScope.get().getCorrelatedSlots().contains(expression))) {
                        if (!SqlModeHelper.hasOnlyFullGroupBy()) {
                            // ATTN: we should add any_value to agg's output here, but not add slot directly.
                            //   because normalize agg cannot replace upper slot with new output.
                            Alias alias = new Alias(new AnyValue(false, groupByExpressions.isEmpty(), expression),
                                    ((SlotReference) expression).getName());
                            newOutputSlots.add(alias);
                            substitution.put(expression, alias.toSlot());
                        } else {
                            throw new AnalysisException(planType + " expression '" + expression.toSql()
                                    + "' must appear in the GROUP BY clause or be used in an aggregate function.");
                        }
                    }
                } else if (expression instanceof GroupingScalarFunction && isRepeat) {
                    generateAliasForNewOutputSlots(expression);
                } else if (expression instanceof AggregateFunction) {
                    if (checkWhetherNestedAggregateFunctionsExist((AggregateFunction) expression)) {
                        throw new AnalysisException(planType + " aggregate functions can't be nested: "
                                + expression.toSql() + ".");
                    }
                    generateAliasForNewOutputSlots(expression);
                } else if (expression instanceof WindowExpression) {
                    generateAliasForNewOutputSlots(expression);
                } else {
                    // Try to resolve the children.
                    for (Expression child : expression.children()) {
                        resolve(child, planType);
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

    protected Plan createPlan(Resolver resolver, Aggregate<? extends Plan> aggregate, PlanGenerator planGenerator) {
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
            if (ExpressionUtils.hasNonWindowAggregateFunction(expr)
                    || expr.containsType(GroupingScalarFunction.class)) {
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
            Aggregate<?> upperAggregate, Aggregate<Plan> bottomAggregate, Optional<Scope> outerScope) {
        Resolver resolver = new Resolver(bottomAggregate, outerScope);
        sort.getExpressions().forEach(expr -> resolver.resolve(expr, ResolvePlanType.SORT));
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

    // process with sort -> having -> project,
    // project must exist, the other at least one may exist.
    private Plan processWithSortHavingProject(Plan plan, Optional<Scope> outerScope) {
        Optional<LogicalSort<Plan>> oldSort = Optional.empty();
        Optional<LogicalHaving<Plan>> oldHaving = Optional.empty();
        if (plan instanceof LogicalSort) {
            oldSort = Optional.of((LogicalSort<Plan>) plan);
            plan = plan.child(0);
        }
        if (plan instanceof LogicalHaving) {
            oldHaving = Optional.of((LogicalHaving<Plan>) plan);
            plan = plan.child(0);
        }
        if (!(plan instanceof LogicalProject) || !oldSort.isPresent() && !oldHaving.isPresent()) {
            return null;
        }

        LogicalProject<Plan> oldProject = (LogicalProject<Plan>) plan;
        Set<Slot> oldProjectSlots = oldProject.getOutputSet();
        List<Expression> oldSortExpressions = ImmutableList.of();
        if (oldSort.isPresent()) {
            LogicalSort<Plan> sort = oldSort.get();
            oldSortExpressions = Lists.newArrayListWithExpectedSize(sort.getOrderKeys().size());
            for (OrderKey key : sort.getOrderKeys()) {
                oldSortExpressions.add(key.getExpr());
            }
        }
        Set<Expression> oldHavingExpressions
                = oldHaving.isPresent() ? oldHaving.get().getConjuncts() : ImmutableSet.of();
        Set<Slot> notExistsSlotInProject = Sets.newLinkedHashSet();
        AtomicBoolean hasAggregateFunc = new AtomicBoolean(false);
        for (Expression expression : oldSortExpressions) {
            collectNotExistsSlotAndAggFunc(
                    expression, oldProjectSlots, outerScope, notExistsSlotInProject, hasAggregateFunc);
        }
        for (Expression expression : oldHavingExpressions) {
            collectNotExistsSlotAndAggFunc(
                    expression, oldProjectSlots, outerScope, notExistsSlotInProject, hasAggregateFunc);
        }
        if (notExistsSlotInProject.isEmpty() && !hasAggregateFunc.get()) {
            return null;
        }
        List<Expression> newSortExpressions = oldSortExpressions;
        Set<Expression> newHavingExpressions = oldHavingExpressions;
        LogicalProject<Plan> newProject;
        if (hasAggregateFunc.get()) {
            LogicalAggregate<Plan> agg = new LogicalAggregate<>(
                    ImmutableList.of(), ImmutableList.of(), oldProject.child());
            // avoid throw exception even if having have slot from its child.
            // because we will add a project between having and project.
            Resolver resolver = new Resolver(agg, outerScope);
            newSortExpressions = resolveAndRewriteExprsForEmptyGroupBy(
                    oldSortExpressions, resolver, ResolvePlanType.SORT);
            newHavingExpressions = ImmutableSet.copyOf(
                    resolveAndRewriteExprsForEmptyGroupBy(oldHavingExpressions, resolver, ResolvePlanType.HAVING));
            // select a from t having sum(a) > 0, check 'a' not in group by list.
            List<Expression> newProjectExpressions = resolveAndRewriteExprsForEmptyGroupBy(
                    oldProject.getProjects(), resolver, ResolvePlanType.PROJECT);
            agg = agg.withAggOutput(resolver.getNewOutputSlots());
            ImmutableList.Builder<NamedExpression> newProjectBuilder
                    = ImmutableList.builderWithExpectedSize(newProjectExpressions.size() + agg.getOutput().size());
            for (Expression expr : newProjectExpressions) {
                newProjectBuilder.add((NamedExpression) expr);
            }
            newProjectBuilder.addAll(agg.getOutput());
            newProject = new LogicalProject<>(newProjectBuilder.build(), agg);
        } else {
            ImmutableList.Builder<NamedExpression> newProjectBuilder = ImmutableList.builderWithExpectedSize(
                    oldProject.getProjects().size() + notExistsSlotInProject.size());
            newProjectBuilder.addAll(oldProject.getProjects()).addAll(notExistsSlotInProject);
            newProject = oldProject.withProjects(newProjectBuilder.build());
        }

        // rebuild result from bottom-up
        Plan result = newProject;
        if (oldHaving.isPresent()) {
            result = oldHaving.get().withConjunctsAndChild(newHavingExpressions, result);
        }
        if (oldSort.isPresent()) {
            List<OrderKey> oldOrderKeys = oldSort.get().getOrderKeys();
            List<OrderKey> newOrderKeys = oldOrderKeys;
            if (!newSortExpressions.equals(oldSortExpressions)) {
                ImmutableList.Builder<OrderKey> newOrderKeysBuilder
                        = ImmutableList.builderWithExpectedSize(oldOrderKeys.size());
                for (int i = 0; i < oldOrderKeys.size(); i++) {
                    newOrderKeysBuilder.add(oldOrderKeys.get(i).withExpression(newSortExpressions.get(i)));
                }
                newOrderKeys = newOrderKeysBuilder.build();
            }
            result = oldSort.get().withOrderKeysAndChild(newOrderKeys, result);
        }
        if (!hasAggregateFunc.get()) {
            // handle for miss slots case, add a top project
            result = new LogicalProject<>(ImmutableList.copyOf(oldProject.getOutput()), result);
        }
        return result;
    }

    private void collectNotExistsSlotAndAggFunc(Expression expression, Set<Slot> oldProjectSlots,
            Optional<Scope> outerScope, Set<Slot> notExistsSlots, AtomicBoolean hasAggregateFunc) {
        for (Slot slot : expression.getInputSlots()) {
            if (!oldProjectSlots.contains(slot)
                    && !(outerScope.isPresent() && outerScope.get().getCorrelatedSlots().contains(slot))) {
                notExistsSlots.add(slot);
            }
        }
        if (!hasAggregateFunc.get() && ExpressionUtils.hasNonWindowAggregateFunction(expression)) {
            hasAggregateFunc.set(true);
        }
    }

    private List<Expression> resolveAndRewriteExprsForEmptyGroupBy(
            Collection<? extends Expression> expressions, Resolver resolver, ResolvePlanType planType) {
        List<Expression> result = Lists.newArrayListWithExpectedSize(expressions.size());
        for (Expression expr : expressions) {
            // for empty group by, the NullableAggregateFunction is always nullable
            Expression newExpr = AdjustAggregateNullableForEmptySet.replaceExpression(expr, true);
            resolver.resolve(newExpr, planType);
            result.add(ExpressionUtils.replace(newExpr, resolver.getSubstitution()));
        }
        return result;
    }
}
