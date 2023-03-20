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
import org.apache.doris.nereids.rules.rewrite.RewriteRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
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
 *    avg(l_quantity)over(partition by p_partkey)
 *    AS avg_l_quantity
 *    FROM lineitem, part
 *    WHERE p_partkey = l_partkey and
 *    p_brand = 'Brand#23' and
 *    p_container = 'MED BOX') t
 * WHERE l_quantity < 0.2 * avg_l_quantity;
 */

public class AggScalarSubQueryToWindowFunction implements RewriteRuleFactory {
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

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
                RuleType.AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION.build(
                        logicalFilter(logicalProject(logicalApply(any(), logicalAggregate())))
                                .when(node -> {
                                    LogicalApply<?, ?> apply = node.child().child();
                                    return apply.isScalar() && apply.isCorrelated();
                                })
                                .when(this::check)
                                .then(this::trans)
                )
        );
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
        AggregateFunction function = ((AggregateFunction) ExpressionUtils.collectAll(
                apply.right().getOutputExpressions(),
                AggregateFunction.class::isInstance).get(0));
        WindowExpression windowFunction = createWindowFunction(apply.getCorrelationSlot(),
                function);

        Alias aggOut = ((Alias) apply.right().getOutputExpressions().get(0))
                .withChildren(ImmutableList.of(windowFunction));
        Expression windowFilterConjunct = apply.getSubCorrespondingConject().get();
        if (windowFilterConjunct.child(0) instanceof Alias
                && ((Alias) windowFilterConjunct.child(0)).getExprId().equals(aggOut.getExprId())) {
            windowFilterConjunct.withChildren(windowFunction, windowFilterConjunct.child(1));
        } else {
            windowFilterConjunct.withChildren(windowFilterConjunct.child(0), windowFunction);
        }
        LogicalFilter newFilter = ((LogicalFilter) node.withChildren(apply.left()));
        LogicalWindow newWindow = new LogicalWindow<>(ImmutableList.of(aggOut), newFilter);
        LogicalFilter windowFilter = new LogicalFilter(ImmutableSet.of(windowFilterConjunct), newWindow);
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
        return ((List<AggregateFunction>) ExpressionUtils.<AggregateFunction>collectAll(
                aggSet.get(0).getOutputExpressions(), AggregateFunction.class::isInstance))
                .stream().allMatch(f -> SUPPORTED_FUNCTION.contains(f.getClass()) && !f.isDistinct());
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
            return isClassMatch(expression, expression1) && isSameObjects(o) && isSameChild(expression, expression1);
        }

        @Override
        public Boolean visit(Expression expression, Expression expression1) {
            return isClassMatch(expression, expression1) && isSameChild(expression, expression1);
        }

        @Override
        public Boolean visitNamedExpression(NamedExpression namedExpression, Expression expr) {
            return isSameOperator(namedExpression, expr, namedExpression.getName(), ((NamedExpression) expr).getName());
        }

        @Override
        public Boolean visitLiteral(Literal literal, Expression expr) {
            return isSameOperator(literal, expr, literal.getValue(), ((Literal) expr).getValue());
        }
    }
}
