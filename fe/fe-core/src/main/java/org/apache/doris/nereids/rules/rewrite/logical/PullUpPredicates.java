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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * poll up effective predicates from operator's children.
 */
public class PullUpPredicates extends PlanVisitor<ImmutableSet<Expression>, Void> {

    PredicatePropagation propagation = new PredicatePropagation();
    Map<Plan, ImmutableSet<Expression>> cache = new IdentityHashMap<>();

    @Override
    public ImmutableSet<Expression> visit(Plan plan, Void context) {
        if (plan.arity() == 1) {
            return plan.child(0).accept(this, context);
        }
        return ImmutableSet.of();
    }

    @Override
    public ImmutableSet<Expression> visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return cacheOrElse(filter, () -> {
            List<Expression> predicates = Lists.newArrayList(filter.getConjuncts());
            predicates.addAll(filter.child().accept(this, context));
            return getAvailableExpressions(predicates, filter);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join, Void context) {
        return cacheOrElse(join, () -> {
            Set<Expression> predicates = Sets.newHashSet();
            ImmutableSet<Expression> leftPredicates = join.left().accept(this, context);
            ImmutableSet<Expression> rightPredicates = join.right().accept(this, context);
            switch (join.getJoinType()) {
                case INNER_JOIN:
                case CROSS_JOIN:
                    predicates.addAll(leftPredicates);
                    predicates.addAll(rightPredicates);
                    join.getOnClauseCondition().map(on -> predicates.addAll(ExpressionUtils.extractConjunction(on)));
                    break;
                case LEFT_SEMI_JOIN:
                    predicates.addAll(leftPredicates);
                    join.getOnClauseCondition().map(on -> predicates.addAll(ExpressionUtils.extractConjunction(on)));
                    break;
                case RIGHT_SEMI_JOIN:
                    predicates.addAll(rightPredicates);
                    join.getOnClauseCondition().map(on -> predicates.addAll(ExpressionUtils.extractConjunction(on)));
                    break;
                case LEFT_OUTER_JOIN:
                case LEFT_ANTI_JOIN:
                    predicates.addAll(leftPredicates);
                    break;
                case RIGHT_OUTER_JOIN:
                case RIGHT_ANTI_JOIN:
                    predicates.addAll(rightPredicates);
                    break;
                default:
            }
            return getAvailableExpressions(predicates, join);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return cacheOrElse(project, () -> {
            ImmutableSet<Expression> childPredicates = project.child().accept(this, context);
            Map<Expression, Slot> expressionSlotMap = project.getAliasToProducer()
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(Entry::getValue, Entry::getKey));
            Expression expression = ExpressionUtils.replace(ExpressionUtils.and(Lists.newArrayList(childPredicates)),
                    expressionSlotMap);
            List<Expression> predicates = ExpressionUtils.extractConjunction(expression);
            return getAvailableExpressions(predicates, project);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return cacheOrElse(aggregate, () -> {
            ImmutableSet<Expression> childPredicates = aggregate.child().accept(this, context);
            Map<Expression, Slot> expressionSlotMap = aggregate.getOutputExpressions()
                    .stream()
                    .filter(this::hasAgg)
                    .collect(Collectors.toMap(
                            namedExpr -> {
                                if (namedExpr instanceof Alias) {
                                    return ((Alias) namedExpr).child();
                                } else {
                                    return namedExpr;
                                }
                            }, NamedExpression::toSlot)
                    );
            Expression expression = ExpressionUtils.replace(ExpressionUtils.and(Lists.newArrayList(childPredicates)),
                    expressionSlotMap);
            List<Expression> predicates = ExpressionUtils.extractConjunction(expression);
            return getAvailableExpressions(predicates, aggregate);
        });
    }

    private ImmutableSet<Expression> cacheOrElse(Plan plan, Supplier<ImmutableSet<Expression>> predicatesSupplier) {
        ImmutableSet<Expression> predicates = cache.get(plan);
        if (predicates != null) {
            return predicates;
        }
        predicates = predicatesSupplier.get();
        cache.put(plan, predicates);
        return predicates;
    }

    private ImmutableSet<Expression> getAvailableExpressions(Collection<Expression> predicates, Plan plan) {
        Set<Expression> expressions = Sets.newHashSet(predicates);
        expressions.addAll(propagation.infer(expressions));
        return expressions.stream()
                .filter(p -> plan.getOutputSet().containsAll(p.getInputSlots()))
                .collect(ImmutableSet.toImmutableSet());
    }

    private boolean hasAgg(Expression expression) {
        return expression.anyMatch(AggregateFunction.class::isInstance);
    }
}
