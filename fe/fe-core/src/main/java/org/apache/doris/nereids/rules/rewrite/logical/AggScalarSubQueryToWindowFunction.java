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

import org.apache.doris.nereids.jobs.JobContext;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.ExprId;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Avg;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.agg.NullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PlanUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * change the plan:
 * logicalFilter(logicalApply(any(), logicalAggregate()))
 * to
 * logicalProject((logicalFilter(logicalWindow(logicalFilter(any())))))
 * refer paper: WinMagic - Subquery Elimination Using Window Aggregation
 */

public class AggScalarSubQueryToWindowFunction extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private static final Set<Class<? extends AggregateFunction>> SUPPORTED_FUNCTION = ImmutableSet.of(
            Min.class, Max.class, Count.class, Sum.class, Avg.class
    );
    private static final Set<Class<? extends LogicalPlan>> LEFT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalRelation.class, LogicalJoin.class, LogicalProject.class, LogicalFilter.class, LogicalLimit.class
    );
    private static final Set<Class<? extends LogicalPlan>> RIGHT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalRelation.class, LogicalJoin.class, LogicalProject.class, LogicalFilter.class, LogicalAggregate.class
    );
    private List<LogicalPlan> outerPlans = null;
    private List<LogicalPlan> innerPlans = null;
    private LogicalAggregate aggOp = null;
    private List<AggregateFunction> functions = null;

    @Override
    public Plan rewriteRoot(Plan plan, JobContext context) {
        return plan.accept(this, context);
    }

    @Override
    public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter, JobContext context) {
        LogicalApply<Plan, LogicalAggregate<Plan>> apply = checkPattern(filter);
        if (apply == null) {
            return filter;
        }
        if (!check(filter, apply)) {
            return filter;
        }
        return trans(filter, apply);
    }

    private LogicalApply<Plan, LogicalAggregate<Plan>> checkPattern(LogicalFilter<? extends Plan> filter) {
        LogicalPlan plan = ((LogicalPlan) filter.child());
        if (plan instanceof LogicalProject) {
            plan = ((LogicalPlan) ((LogicalProject) plan).child());
        }
        if (!(plan instanceof LogicalApply)) {
            return null;
        }
        LogicalApply apply = (LogicalApply) plan;
        if (!checkApplyNode(apply)) {
            return null;
        }
        return apply.right() instanceof LogicalAggregate ? apply : null;
    }

    private boolean check(LogicalFilter<? extends Plan> filter, LogicalApply<Plan, LogicalAggregate<Plan>> apply) {
        LogicalPlan outer = ((LogicalPlan) apply.child(0));
        LogicalPlan inner = ((LogicalPlan) apply.child(1));
        outerPlans = PlanCollector.INSTANCE.collect(outer);
        innerPlans = PlanCollector.INSTANCE.collect(inner);
        Optional<LogicalFilter> innerFilter = innerPlans.stream()
                .filter(LogicalFilter.class::isInstance)
                .map(LogicalFilter.class::cast).findFirst();
        return innerFilter.isPresent()
                && checkPlanType() && checkAggType()
                && checkRelation(apply.getCorrelationSlot())
                && checkPredicate(Sets.newHashSet(filter.getConjuncts()),
                Sets.newHashSet(innerFilter.get().getConjuncts()));
    }

    // check children's nodes because query process will be changed
    private boolean checkPlanType() {
        return outerPlans.stream().allMatch(p -> LEFT_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)))
                && innerPlans.stream().allMatch(p -> RIGHT_SUPPORTED_PLAN.stream().anyMatch(c -> c.isInstance(p)));
    }

    private boolean checkApplyNode(LogicalApply apply) {
        return apply.isScalar() && apply.isCorrelated() && apply.getSubCorrespondingConjunct().isPresent()
                && apply.getSubCorrespondingConjunct().get() instanceof ComparisonPredicate;
    }

    // check aggregation of inner scope
    private boolean checkAggType() {
        List<LogicalAggregate> aggSet = innerPlans.stream().filter(LogicalAggregate.class::isInstance)
                .map(LogicalAggregate.class::cast)
                .collect(Collectors.toList());
        if (aggSet.size() > 1) {
            // window functions don't support nesting.
            return false;
        }
        aggOp = aggSet.get(0);
        functions = ((List<AggregateFunction>) ExpressionUtils.<AggregateFunction>collectAll(
                aggOp.getOutputExpressions(), AggregateFunction.class::isInstance));
        Preconditions.checkArgument(functions.size() == 1);
        return functions.stream().allMatch(f -> SUPPORTED_FUNCTION.contains(f.getClass()) && !f.isDistinct());
    }

    // check if the relations of the outer's includes the inner's
    private boolean checkRelation(List<Expression> correlatedSlots) {
        List<LogicalRelation> outerTables = outerPlans.stream().filter(LogicalRelation.class::isInstance)
                .map(LogicalRelation.class::cast)
                .collect(Collectors.toList());
        List<LogicalRelation> innerTables = innerPlans.stream().filter(LogicalRelation.class::isInstance)
                .map(LogicalRelation.class::cast)
                .collect(Collectors.toList());

        Set<Long> outerIds = outerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toSet());
        Set<Long> innerIds = innerTables.stream().map(node -> node.getTable().getId()).collect(Collectors.toSet());

        Set<Long> outerCopy = Sets.newHashSet(outerIds);
        outerIds.removeAll(innerIds);
        innerIds.removeAll(outerCopy);
        if (outerIds.isEmpty() || !innerIds.isEmpty()) {
            return false;
        }

        Set<ExprId> correlatedRelationOutput = outerTables.stream()
                .filter(node -> outerIds.contains(node.getTable().getId()))
                .map(LogicalRelation::getOutputExprIdSet).flatMap(Collection::stream).collect(Collectors.toSet());
        return ExpressionUtils.collect(correlatedSlots, NamedExpression.class::isInstance).stream()
                .map(NamedExpression.class::cast)
                .allMatch(e -> correlatedRelationOutput.contains(e.getExprId()));
    }

    private boolean checkPredicate(Set<Expression> outerConjuncts, Set<Expression> innerConjuncts) {
        Iterator<Expression> innerIter = innerConjuncts.iterator();
        // inner predicate should be the sub-set of outer predicate.
        while (innerIter.hasNext()) {
            Expression innerExpr = innerIter.next();
            Iterator<Expression> outerIter = outerConjuncts.iterator();
            while (outerIter.hasNext()) {
                Expression outerExpr = outerIter.next();
                if (ExpressionIdenticalChecker.INSTANCE.check(innerExpr, outerExpr)) {
                    innerIter.remove();
                    outerIter.remove();
                }
            }
        }
        // now the expressions are all like 'expr op literal' or flipped, and whose expr is not correlated.
        return innerConjuncts.size() == 0;
    }

    private Plan trans(LogicalFilter<? extends Plan> filter, LogicalApply<Plan, LogicalAggregate<Plan>> apply) {
        LogicalAggregate<Plan> agg = apply.right();

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
        // is agg's output expression
        // we change it to
        // cast(l_quantity#id1 as decimal(27, 9)) < 0.2 * `avg(l_quantity#id1) over(window)`#id4
        // and
        // avg(l_quantity#id1) over(window) as `avg(l_quantity#id1) over(window)`#id4

        // it's a simple case, but we may meet some complex cases in ut.
        // TODO: support compound predicate and multi apply node.

        Expression windowFilterConjunct = apply.getSubCorrespondingConjunct().get();
        windowFilterConjunct = PlanUtils.maybeCommuteComparisonPredicate(
                (ComparisonPredicate) windowFilterConjunct, apply.left());

        // build window function, replace the slot
        List<Expression> windowAggSlots = windowFilterConjunct.child(0).collectToList(Slot.class::isInstance);

        AggregateFunction function = functions.get(0);
        if (function instanceof NullableAggregateFunction) {
            // adjust agg function's nullable.
            function = ((NullableAggregateFunction) function).withAlwaysNullable(false);
        }

        WindowExpression windowFunction = createWindowFunction(apply.getCorrelationSlot(),
                function.withChildren(windowAggSlots));
        NamedExpression windowFunctionAlias = new Alias(windowFunction, windowFunction.toSql());

        // build filter conjunct, get the alias of the agg output and extract its child.
        // then replace the agg to window function, then build conjunct
        // we ensure aggOut is Alias.
        NamedExpression aggOut = agg.getOutputExpressions().get(0);
        Expression aggOutExpr = aggOut.child(0);
        // change the agg function to window function alias.
        aggOutExpr = MapReplacer.INSTANCE.replace(aggOutExpr, ImmutableMap
                .of(AggregateFunction.class, e -> windowFunctionAlias.toSlot()));

        // we change the child contains the original agg output to agg output expr.
        // for comparison predicate, it is always the child(1), since we ensure the window agg slot is in child(0)
        // for in predicate, we should extract the options and find the corresponding child.
        windowFilterConjunct = windowFilterConjunct
                .withChildren(windowFilterConjunct.child(0), aggOutExpr);

        LogicalFilter newFilter = ((LogicalFilter) filter.withChildren(apply.left()));
        LogicalWindow newWindow = new LogicalWindow<>(ImmutableList.of(windowFunctionAlias), newFilter);
        LogicalFilter windowFilter = new LogicalFilter<>(ImmutableSet.of(windowFilterConjunct), newWindow);
        return windowFilter;
    }

    private WindowExpression createWindowFunction(List<Expression> correlatedSlots, AggregateFunction function) {
        // partition by clause is set by all the correlated slots.
        Preconditions.checkArgument(correlatedSlots.stream().allMatch(Slot.class::isInstance));
        return new WindowExpression(function, correlatedSlots, Collections.emptyList());
    }

    private static class PlanCollector extends DefaultPlanVisitor<Void, List<LogicalPlan>> {
        public static final PlanCollector INSTANCE = new PlanCollector();

        public List<LogicalPlan> collect(LogicalPlan plan) {
            List<LogicalPlan> buffer = Lists.newArrayList();
            plan.accept(this, buffer);
            return buffer;
        }

        @Override
        public Void visit(Plan plan, List<LogicalPlan> buffer) {
            Preconditions.checkArgument(plan instanceof LogicalPlan);
            buffer.add(((LogicalPlan) plan));
            plan.children().forEach(child -> child.accept(this, buffer));
            return null;
        }
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

        private boolean isSameObjects(Object... o) {
            Preconditions.checkArgument(o.length % 2 == 0);
            for (int i = 0; i < o.length; i += 2) {
                if (!Objects.equals(o[i], o[i + 1])) {
                    return false;
                }
            }
            return true;
        }

        private boolean isSameOperator(Expression expression, Expression expression1, Object... o) {
            return isSameObjects(o) && isSameChild(expression, expression1);
        }

        @Override
        public Boolean visit(Expression expression, Expression expression1) {
            return isClassMatch(expression, expression1) && isSameChild(expression, expression1);
        }

        @Override
        public Boolean visitNamedExpression(NamedExpression namedExpression, Expression expr) {
            return isClassMatch(namedExpression, expr)
                    && isSameOperator(namedExpression, expr, namedExpression.getName(),
                    ((NamedExpression) expr).getName());
        }

        @Override
        public Boolean visitLiteral(Literal literal, Expression expr) {
            return isClassMatch(literal, expr)
                    && isSameOperator(literal, expr, literal.getValue(), ((Literal) expr).getValue());
        }

        @Override
        public Boolean visitEqualTo(EqualTo equalTo, Expression expr) {
            return isSameChild(equalTo, expr) || isSameChild(equalTo.commute(), expr);
        }
    }

    private static class MapReplacer extends DefaultExpressionRewriter<Map<Class<? extends Expression>,
                Function<Expression, Expression>>> {
        public static final MapReplacer INSTANCE = new MapReplacer();

        public Expression replace(Expression e, Map<Class<? extends Expression>,
                Function<Expression, Expression>> context) {
            return e.accept(this, context);
        }

        @Override
        public Expression visit(Expression e, Map<Class<? extends Expression>,
                Function<Expression, Expression>> context) {
            Expression replaced = e;
            for (Class c : context.keySet()) {
                if (c.isInstance(e)) {
                    replaced = context.get(c).apply(e);
                    break;
                }
            }
            return super.visit(replaced, context);
        }
    }
}
