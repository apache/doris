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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.window.SupportWindowAnalytic;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.CatalogRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * change the plan:
 * logicalFilter(logicalApply(any(), logicalAggregate()))
 * to
 * logicalProject(logicalFilter(logicalWindow(logicalFilter(any()))))
 * <p>
 * refer paper: WinMagic - Subquery Elimination Using Window Aggregation
 * <p>
 * TODO: use materialized view pattern match to do outer and inner tree match.
 */

public class AggScalarSubQueryToWindowFunction extends DefaultPlanRewriter<JobContext> implements CustomRewriter {

    private static final Set<Class<? extends LogicalPlan>> OUTER_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalJoin.class,
            LogicalProject.class,
            LogicalRelation.class
    );

    private static final Set<Class<? extends LogicalPlan>> INNER_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalAggregate.class,
            LogicalFilter.class,
            LogicalJoin.class,
            LogicalProject.class,
            LogicalRelation.class
    );

    private final List<LogicalPlan> outerPlans = Lists.newArrayList();
    private final List<LogicalPlan> innerPlans = Lists.newArrayList();
    private final List<AggregateFunction> functions = Lists.newArrayList();
    private final Map<Expression, Expression> innerOuterSlotMap = Maps.newHashMap();

    /**
     * the entrance of this rule. we only override one visitor: visitLogicalFilter
     * because we need to process the filter of outer plan. It is on the top of Apply.
     */
    @Override
    public Plan rewriteRoot(Plan plan, JobContext context) {
        return plan.accept(this, context);
    }

    /**
     * we need to process Filter and Apply, but sometimes there are project between Filter and Apply.
     * According to {@link org.apache.doris.nereids.rules.analysis.SubqueryToApply} rule. The project
     * is used to project apply output to original output, it is not affect this rule at all. so we ignore it.
     */
    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> plan, JobContext context) {
        LogicalFilter<? extends Plan> filter = visitChildren(this, plan, context);
        return findApply(filter)
                .filter(a -> check(filter, a))
                .map(a -> rewrite(filter, a))
                .orElse(filter);
    }

    private Optional<LogicalApply<Plan, Plan>> findApply(LogicalFilter<? extends Plan> filter) {
        return Optional.of(filter.child())
                .map(p -> p instanceof LogicalProject ? p.child(0) : p)
                .filter(LogicalApply.class::isInstance)
                .map(p -> (LogicalApply<Plan, Plan>) p);
    }

    private boolean check(LogicalFilter<? extends Plan> outerFilter, LogicalApply<Plan, Plan> apply) {
        outerPlans.addAll(apply.child(0).collect(LogicalPlan.class::isInstance));
        innerPlans.addAll(apply.child(1).collect(LogicalPlan.class::isInstance));

        return checkPlanType()
                && checkApply(apply)
                && checkAggregate()
                && checkJoin()
                && checkProject()
                && checkRelation(apply.getCorrelationSlot())
                && checkFilter(outerFilter);
    }

    // check children's nodes because query process will be changed
    private boolean checkPlanType() {
        return outerPlans.stream().allMatch(p -> OUTER_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)))
                && innerPlans.stream().allMatch(p -> INNER_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    /**
     * Apply should be
     *   1. scalar
     *   2. is not mark join
     *   3. is correlated
     *   4. correlated conjunct should be {@link ComparisonPredicate}
     *   5. the top plan of Apply inner should be {@link LogicalAggregate}
     */
    private boolean checkApply(LogicalApply<Plan, Plan> apply) {
        return apply.isScalar()
                && !apply.isMarkJoin()
                && apply.right() instanceof LogicalAggregate
                && apply.isCorrelated();
    }

    /**
     * check aggregation of inner scope, it should be only one Aggregate and only one AggregateFunction in it
     */
    private boolean checkAggregate() {
        List<LogicalAggregate<Plan>> aggSet = innerPlans.stream().filter(LogicalAggregate.class::isInstance)
                .map(p -> (LogicalAggregate<Plan>) p)
                .collect(Collectors.toList());
        if (aggSet.size() != 1) {
            // window functions don't support nesting.
            return false;
        }
        LogicalAggregate<Plan> aggOp = aggSet.get(0);
        functions.addAll(ExpressionUtils.collectAll(
                aggOp.getOutputExpressions(), AggregateFunction.class::isInstance));
        if (functions.size() != 1) {
            return false;
        }
        return functions.stream().allMatch(f -> f instanceof SupportWindowAnalytic && !f.isDistinct());
    }

    /**
     * check inner scope only have one filter. and inner filter is a sub collection of outer filter
     */
    private boolean checkFilter(LogicalFilter<? extends Plan> outerFilter) {
        List<LogicalFilter<Plan>> innerFilters = innerPlans.stream()
                .filter(LogicalFilter.class::isInstance)
                .map(p -> (LogicalFilter<Plan>) p).collect(Collectors.toList());
        if (innerFilters.size() != 1) {
            return false;
        }
        Set<Expression> outerConjunctSet = Sets.newHashSet(outerFilter.getConjuncts());
        Set<Expression> innerConjunctSet = innerFilters.get(0).getConjuncts().stream()
                .map(e -> ExpressionUtils.replace(e, innerOuterSlotMap))
                .collect(Collectors.toSet());
        Iterator<Expression> innerIterator = innerConjunctSet.iterator();
        // inner predicate should be the sub-set of outer predicate.
        while (innerIterator.hasNext()) {
            Expression innerExpr = innerIterator.next();
            Iterator<Expression> outerIterator = outerConjunctSet.iterator();
            while (outerIterator.hasNext()) {
                Expression outerExpr = outerIterator.next();
                if (ExpressionIdenticalChecker.INSTANCE.check(innerExpr, outerExpr)) {
                    innerIterator.remove();
                    outerIterator.remove();
                }
            }
        }
        // now the expressions are all like 'expr op literal' or flipped, and whose expr is not correlated.
        return innerConjunctSet.isEmpty();
    }

    /**
     * check join to ensure no condition on it.
     * this is because we cannot do accurate pattern match between outer scope and inner scope
     * so, we currently forbid join with condition here.
     */
    private boolean checkJoin() {
        return outerPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .noneMatch(j -> j.getOnClauseCondition().isPresent())
                && innerPlans.stream()
                .filter(LogicalJoin.class::isInstance)
                .map(p -> (LogicalJoin<Plan, Plan>) p)
                .noneMatch(j -> j.getOnClauseCondition().isPresent());
    }

    /**
     * check inner and outer project to ensure no project except column pruning
     */
    private boolean checkProject() {
        return outerPlans.stream()
                .filter(LogicalProject.class::isInstance)
                .map(p -> (LogicalProject<Plan>) p)
                .allMatch(p -> p.getExpressions().stream().allMatch(SlotReference.class::isInstance))
                && innerPlans.stream()
                .filter(LogicalProject.class::isInstance)
                .map(p -> (LogicalProject<Plan>) p)
                .allMatch(p -> p.getExpressions().stream().allMatch(SlotReference.class::isInstance));
    }

    /**
     * check inner and outer relation
     * 1. outer table size - inner table size must equal to 1
     * 2. outer table list - inner table list should only remain 1 table
     * 3. the remaining table in step 2 should be correlated table for inner plan
     */
    private boolean checkRelation(List<Expression> correlatedSlots) {
        List<CatalogRelation> outerTables = outerPlans.stream().filter(CatalogRelation.class::isInstance)
                .map(CatalogRelation.class::cast)
                .collect(Collectors.toList());
        List<CatalogRelation> innerTables = innerPlans.stream().filter(CatalogRelation.class::isInstance)
                .map(CatalogRelation.class::cast)
                .collect(Collectors.toList());

        List<Long> outerIds = outerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        List<Long> innerIds = innerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toList());
        if (Sets.newHashSet(outerIds).size() != outerIds.size()
                || Sets.newHashSet(innerIds).size() != innerIds.size()) {
            return false;
        }
        if (outerIds.size() - innerIds.size() != 1) {
            return false;
        }
        innerIds.forEach(outerIds::remove);
        if (outerIds.size() != 1) {
            return false;
        }

        createSlotMapping(outerTables, innerTables);

        Set<ExprId> correlatedRelationOutput = outerTables.stream()
                .filter(node -> outerIds.contains(node.getTable().getId()))
                .map(LogicalRelation.class::cast)
                .map(LogicalRelation::getOutputExprIdSet).flatMap(Collection::stream).collect(Collectors.toSet());
        return ExpressionUtils.collect(correlatedSlots, NamedExpression.class::isInstance).stream()
                .map(NamedExpression.class::cast)
                .allMatch(e -> correlatedRelationOutput.contains(e.getExprId()));
    }

    private void createSlotMapping(List<CatalogRelation> outerTables, List<CatalogRelation> innerTables) {
        for (CatalogRelation outerTable : outerTables) {
            for (CatalogRelation innerTable : innerTables) {
                if (innerTable.getTable().getId() == outerTable.getTable().getId()) {
                    for (Slot innerSlot : innerTable.getOutput()) {
                        for (Slot outerSlot : outerTable.getOutput()) {
                            if (innerSlot.getName().equals(outerSlot.getName())) {
                                innerOuterSlotMap.put(innerSlot, outerSlot);
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    private Plan rewrite(LogicalFilter<? extends Plan> filter, LogicalApply<Plan, Plan> apply) {
        Preconditions.checkArgument(apply.right() instanceof LogicalAggregate,
                "right child of Apply should be LogicalAggregate");
        LogicalAggregate<Plan> agg = (LogicalAggregate<Plan>) apply.right();

        // transform algorithm
        // first: find the slot in outer scope corresponding to the slot in aggregate function in inner scope.
        // second: find the aggregation function in inner scope, and replace it to window function, and the aggregate
        // slot is the slot in outer scope in the first step.
        // third: the expression containing aggregation function in inner scope will be the child of an alias,
        // so in the predicate between outer and inner, we change the alias to expression which is the alias's child,
        // and change the aggregation function to the alias of window function.

        // for example, in tpc-h Q17
        // window filter conjuncts is
        // cast(l_quantity#id1 as decimal(27, 9)) < `0.2 * avg(l_quantity)`#id2
        // and
        // 0.2 * avg(l_quantity#id3) as `0.2 * l_quantity`#id2
        // is aggregate's output expression
        // we change it to
        // cast(l_quantity#id1 as decimal(27, 9)) < 0.2 * `avg(l_quantity#id1) over(window)`#id4
        // and
        // avg(l_quantity#id1) over(window) as `avg(l_quantity#id1) over(window)`#id4

        // it's a simple case, but we may meet some complex cases in ut.
        // TODO: support compound predicate and multi apply node.

        Map<Boolean, Set<Expression>> conjuncts = filter.getConjuncts().stream()
                .collect(Collectors.groupingBy(conjunct -> Sets
                        .intersection(conjunct.getInputSlotExprIds(), agg.getOutputExprIdSet())
                        .isEmpty(), Collectors.toSet()));
        Set<Expression> correlatedConjuncts = conjuncts.get(false);
        if (correlatedConjuncts.isEmpty() || correlatedConjuncts.size() > 1
                || !(correlatedConjuncts.iterator().next() instanceof ComparisonPredicate)) {
            //TODO: only support simple comparison predicate now
            return filter;
        }
        Expression windowFilterConjunct = correlatedConjuncts.iterator().next();
        windowFilterConjunct = PlanUtils.maybeCommuteComparisonPredicate(
                (ComparisonPredicate) windowFilterConjunct, apply.left());

        AggregateFunction function = functions.get(0);
        if (function instanceof NullableAggregateFunction) {
            // adjust agg function's nullable.
            function = ((NullableAggregateFunction) function).withAlwaysNullable(false);
        }

        WindowExpression windowFunction = createWindowFunction(apply.getCorrelationSlot(),
                (AggregateFunction) ExpressionUtils.replace(function, innerOuterSlotMap));
        NamedExpression windowFunctionAlias = new Alias(windowFunction);

        // build filter conjunct, get the alias of the agg output and extract its child.
        // then replace the agg to window function, then build conjunct
        // we ensure aggOut is Alias.
        NamedExpression aggOut = agg.getOutputExpressions().get(0);
        Expression aggOutExpr = aggOut.child(0);
        // change the agg function to window function alias.
        aggOutExpr = ExpressionUtils.replace(aggOutExpr, ImmutableMap
                .of(functions.get(0), windowFunctionAlias.toSlot()));

        windowFilterConjunct = ExpressionUtils.replace(windowFilterConjunct,
                ImmutableMap.of(aggOut.toSlot(), aggOutExpr));

        LogicalFilter<Plan> newFilter = filter.withConjunctsAndChild(conjuncts.get(true), apply.left());
        LogicalWindow<Plan> newWindow = new LogicalWindow<>(ImmutableList.of(windowFunctionAlias), newFilter);
        LogicalFilter<Plan> windowFilter = new LogicalFilter<>(ImmutableSet.of(windowFilterConjunct), newWindow);
        return windowFilter;
    }

    private WindowExpression createWindowFunction(List<Expression> correlatedSlots, AggregateFunction function) {
        // partition by clause is set by all the correlated slots.
        Preconditions.checkArgument(correlatedSlots.stream().allMatch(Slot.class::isInstance));
        return new WindowExpression(function, correlatedSlots, Collections.emptyList());
    }

    private static class ExpressionIdenticalChecker extends DefaultExpressionVisitor<Boolean, Expression> {
        public static final ExpressionIdenticalChecker INSTANCE = new ExpressionIdenticalChecker();

        public boolean check(Expression expression, Expression expression1) {
            return expression.accept(this, expression1);
        }

        private boolean isClassMatch(Object o1, Object o2) {
            return o1.getClass().equals(o2.getClass());
        }

        private boolean isSameChild(Expression expression, Expression expression1) {
            if (expression.children().size() != expression1.children().size()) {
                return false;
            }
            for (int i = 0; i < expression.children().size(); ++i) {
                if (!expression.children().get(i).accept(this, expression1.children().get(i))) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public Boolean visit(Expression expression, Expression expression1) {
            return isClassMatch(expression, expression1) && isSameChild(expression, expression1);
        }

        @Override
        public Boolean visitSlotReference(SlotReference slotReference, Expression other) {
            return slotReference.equals(other);
        }

        @Override
        public Boolean visitLiteral(Literal literal, Expression other) {
            return literal.equals(other);
        }

        @Override
        public Boolean visitComparisonPredicate(ComparisonPredicate cp, Expression other) {
            return cp.equals(other) || cp.commute().equals(other);
        }
    }
}
