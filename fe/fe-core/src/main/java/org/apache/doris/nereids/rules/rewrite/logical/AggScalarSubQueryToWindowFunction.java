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
import org.apache.doris.nereids.trees.expressions.functions.agg.Sum;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.CustomRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * change scalar sub query containing agg to window function. such as:
 * SELECT SUM(l_extendedprice) / 7.0 AS avg_yearly
 *  FROM lineitem, part
 *  WHERE p_partkey = l_partkey AND
 *  p_brand = 'Brand#23' AND
 *  p_container = 'MED BOX' AND
 *  l_quantity<(SELECT 0.2*avg(l_quantity)
 *  FROM lineitem
 *  WHERE l_partkey = p_partkey);
 * to:
 * SELECT SUM(l_extendedprice) / 7.0 as avg_yearly
 *  FROM (SELECT l_extendedprice, l_quantity,
 *    0.2 * avg(l_quantity)over(partition by p_partkey)
 *    AS avg_l_quantity
 *    FROM lineitem, part
 *    WHERE p_partkey = l_partkey and
 *    p_brand = 'Brand#23' and
 *    p_container = 'MED BOX') t
 * WHERE l_quantity < avg_l_quantity;
 */

public class AggScalarSubQueryToWindowFunction extends DefaultPlanRewriter<JobContext> implements CustomRewriter {
    private static final ImmutableSet<Class<? extends AggregateFunction>> SUPPORTED_FUNCTION = ImmutableSet.of(
            Min.class, Max.class, Count.class, Sum.class, Avg.class
    );
    private static final ImmutableSet<Class<? extends LogicalPlan>> LEFT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalOlapScan.class, LogicalLimit.class, LogicalJoin.class, LogicalProject.class
    );
    private static final ImmutableSet<Class<? extends LogicalPlan>> RIGHT_SUPPORTED_PLAN = ImmutableSet.of(
            LogicalOlapScan.class, LogicalJoin.class, LogicalProject.class, LogicalAggregate.class, LogicalFilter.class
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
    public Plan visitLogicalFilter(LogicalFilter filter, JobContext context) {
        if (!checkPattern(filter)) {
            return filter;
        }
        LogicalFilter<LogicalProject<LogicalApply<Plan, LogicalAggregate<Plan>>>> node
                = ((LogicalFilter<LogicalProject<LogicalApply<Plan, LogicalAggregate<Plan>>>>) filter);
        if (!check(node)) {
            return filter;
        }
        return trans(node);
    }

    private boolean checkPattern(LogicalFilter filter) {
        if (!(filter.child() instanceof LogicalProject)) {
            return false;
        }
        LogicalProject project = (LogicalProject) filter.child();
        if (project.child() == null || !(project.child() instanceof LogicalApply)) {
            return false;
        }
        LogicalApply apply = ((LogicalApply<?, ?>) project.child());
        if (!apply.isScalar() || !apply.isCorrelated()) {
            return false;
        }
        return apply.left() != null && apply.right() instanceof LogicalAggregate
                && ((LogicalAggregate<?>) apply.right()).child() != null;
    }

    private boolean check(LogicalFilter<LogicalProject<LogicalApply<Plan, LogicalAggregate<Plan>>>> node) {
        LogicalApply<?, ?> apply = node.child().child();
        LogicalPlan outer = ((LogicalPlan) apply.child(0));
        LogicalPlan inner = ((LogicalPlan) apply.child(1));
        outerPlans = new PlanCollector().collect(outer);
        innerPlans = new PlanCollector().collect(inner);
        LogicalFilter outerFilter = node;
        Optional<LogicalFilter> innerFilter = innerPlans.stream()
                .filter(LogicalFilter.class::isInstance)
                .map(LogicalFilter.class::cast).findFirst();
        return innerFilter.filter(
                logicalFilter -> checkPlanType() && checkAggType()
                        && checkRelation(apply.getCorrelationSlot())
                        && checkPredicate(Sets.newHashSet(outerFilter.getConjuncts()),
                        Sets.newHashSet(logicalFilter.getConjuncts()))
        ).isPresent();
    }

    private Plan trans(LogicalFilter<LogicalProject<LogicalApply<Plan, LogicalAggregate<Plan>>>> node) {
        LogicalApply<Plan, LogicalAggregate<Plan>> apply = node.child().child();

        Expression windowFilterConjunct = apply.getSubCorrespondingConject().get();
        Preconditions.checkArgument(apply.right().getOutputExpressions().get(0) instanceof Alias);
        Alias aggOutAlias = ((Alias) apply.right().getOutputExpressions().get(0));
        ExprId aggOutId = aggOutAlias.getExprId();
        Expression aggOut = aggOutAlias.child(0);

        int flag = 0;
        if (windowFilterConjunct.child(0) instanceof Alias
                && ((Alias) windowFilterConjunct.child(0)).getExprId().equals(aggOutId)) {
            flag = 1;
        }
        WindowExpression windowFunction = createWindowFunction(apply.getCorrelationSlot(),
                functions.get(0).withChildren(ImmutableList.of(windowFilterConjunct.child(flag))));
        Alias windowAlias = new Alias(windowFunction, "wf");
        aggOut = new FunctionReplacer().replace(aggOut, windowAlias.toSlot());
        List<Expression> children = Lists.newArrayList(null, null);
        children.set(flag, windowFilterConjunct.child(flag));
        children.set(flag ^ 1, aggOut);
        windowFilterConjunct = windowFilterConjunct.withChildren(children);

        LogicalFilter newFilter = ((LogicalFilter) node.withChildren(apply.left()));
        LogicalWindow newWindow = new LogicalWindow<>(ImmutableList.of(windowAlias), newFilter);
        LogicalFilter windowFilter = new LogicalFilter<>(ImmutableSet.of(windowFilterConjunct), newWindow);
        return node.child().withChildren(windowFilter);
    }

    // check children's nodes because query process will be changed
    private boolean checkPlanType() {
        return outerPlans.stream().allMatch(p -> LEFT_SUPPORTED_PLAN.contains(p.getClass()))
                && innerPlans.stream().allMatch(p -> RIGHT_SUPPORTED_PLAN.contains(p.getClass()));
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

        if (outerTables.size() != outerIds.size() || innerTables.size() != innerIds.size()
                || outerIds.size() != innerIds.size() + 1) {
            return false;
        }

        outerIds.removeAll(innerIds);
        if (outerIds.size() != 1) {
            return false;
        }

        LogicalRelation correlatedRelation = outerTables.stream()
                .filter(node -> node.getTable().getId() == outerIds.iterator().next()).findFirst().get();
        return ExpressionUtils.collect(correlatedSlots, NamedExpression.class::isInstance).stream()
                .map(NamedExpression.class::cast)
                .allMatch(e -> correlatedRelation.getOutputExprIdSet().contains(e.getExprId()));
    }

    private boolean checkPredicate(Set<Expression> outerConjuncts, Set<Expression> innerConjuncts) {
        Iterator<Expression> innerIter = innerConjuncts.iterator();
        // inner predicate should be the sub-set of outer predicate.
        while (innerIter.hasNext()) {
            Expression innerExpr = innerIter.next();
            Iterator<Expression> outerIter = outerConjuncts.iterator();
            while (outerIter.hasNext()) {
                Expression outerExpr = outerIter.next();
                if (new ExpressionIdenticalChecker().check(innerExpr, outerExpr)) {
                    innerIter.remove();
                    outerIter.remove();
                }
            }
        }
        // now the expressions are all like 'expr op literal' or flipped, and whose expr is not correlated.
        return innerConjuncts.size() == 0;
    }

    private WindowExpression createWindowFunction(List<Expression> correlatedSlots, AggregateFunction function) {
        // partition by clause is set by all the correlated slots.
        Preconditions.checkArgument(correlatedSlots.stream().allMatch(Slot.class::isInstance));
        return new WindowExpression(function, correlatedSlots, Collections.emptyList());
    }

    private static class PlanCollector extends DefaultPlanVisitor<Void, List<LogicalPlan>> {
        public List<LogicalPlan> collect(LogicalPlan plan) {
            List<LogicalPlan> buffer = Lists.newArrayList();
            plan.accept(this, buffer);
            return buffer;
        }

        @Override
        public Void visit(Plan plan, List<LogicalPlan> buffer) {
            Preconditions.checkArgument(plan instanceof LogicalPlan);
            buffer.add(((LogicalPlan) plan));
            plan.children().forEach(child -> {
                if (child instanceof GroupPlan) {
                    ((GroupPlan) child).getGroup().getLogicalExpression().getPlan().accept(this, buffer);
                } else {
                    child.accept(this, buffer);
                }
            });
            return null;
        }
    }

    private static class ExpressionIdenticalChecker extends DefaultExpressionVisitor<Boolean, Expression> {
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

    private static class FunctionReplacer extends DefaultExpressionRewriter<Expression> {
        public Expression replace(Expression e, Expression wf) {
            return e.accept(this, wf);
        }

        @Override
        public Expression visitAggregateFunction(AggregateFunction f, Expression wf) {
            return wf;
        }
    }
}
