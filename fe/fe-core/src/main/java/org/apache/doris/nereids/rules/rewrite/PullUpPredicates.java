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

import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

/**
 * poll up effective predicates from operator's children.
 */
public class PullUpPredicates extends PlanVisitor<ImmutableSet<Expression>, Void> {

    Map<Plan, ImmutableSet<Expression>> cache = new IdentityHashMap<>();

    @Override
    public ImmutableSet<Expression> visit(Plan plan, Void context) {
        if (plan.arity() == 1) {
            return plan.child(0).accept(this, context);
        }
        return ImmutableSet.of();
    }

    @Override
    public ImmutableSet<Expression> visitLogicalOneRowRelation(LogicalOneRowRelation r, Void context) {
        ImmutableSet.Builder<Expression> predicates = ImmutableSet.builder();
        for (NamedExpression expr : r.getProjects()) {
            if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                predicates.add(new EqualTo(expr.toSlot(), expr.child(0)));
            }
        }
        return predicates.build();
    }

    @Override
    public ImmutableSet<Expression> visitLogicalIntersect(LogicalIntersect intersect, Void context) {
        return cacheOrElse(intersect, () -> {
            ImmutableSet.Builder<Expression> builder = ImmutableSet.builder();
            for (int i = 0; i < intersect.children().size(); ++i) {
                Plan child = intersect.child(i);
                Set<Expression> childFilters = child.accept(this, context);
                if (childFilters.isEmpty()) {
                    continue;
                }
                Map<Expression, Expression> replaceMap = new HashMap<>();
                for (int j = 0; j < intersect.getOutput().size(); ++j) {
                    NamedExpression output = intersect.getOutput().get(j);
                    replaceMap.put(intersect.getRegularChildOutput(i).get(j), output);
                }
                builder.addAll(ExpressionUtils.replace(childFilters, replaceMap));
            }
            return getAvailableExpressions(builder.build(), intersect);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalExcept(LogicalExcept except, Void context) {
        return cacheOrElse(except, () -> {
            if (except.arity() < 1) {
                return ImmutableSet.of();
            }
            Set<Expression> firstChildFilters = except.child(0).accept(this, context);
            if (firstChildFilters.isEmpty()) {
                return ImmutableSet.of();
            }
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int i = 0; i < except.getOutput().size(); ++i) {
                NamedExpression output = except.getOutput().get(i);
                replaceMap.put(except.getRegularChildOutput(0).get(i), output);
            }
            return ImmutableSet.copyOf(ExpressionUtils.replace(firstChildFilters, replaceMap));
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalUnion(LogicalUnion union, Void context) {
        return cacheOrElse(union, () -> {
            if (!union.getConstantExprsList().isEmpty() && union.arity() == 0) {
                return getFiltersFromUnionConstExprs(union);
            } else if (union.getConstantExprsList().isEmpty() && union.arity() != 0) {
                return getFiltersFromUnionChild(union, context);
            } else if (!union.getConstantExprsList().isEmpty() && union.arity() != 0) {
                HashSet<Expression> fromChildFilters = new HashSet<>(getFiltersFromUnionChild(union, context));
                if (fromChildFilters.isEmpty()) {
                    return ImmutableSet.of();
                }
                if (!ExpressionUtils.unionConstExprsSatisfyConjuncts(union, fromChildFilters)) {
                    return ImmutableSet.of();
                }
                return ImmutableSet.copyOf(fromChildFilters);
            }
            return ImmutableSet.of();
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalFilter(LogicalFilter<? extends Plan> filter, Void context) {
        return cacheOrElse(filter, () -> {
            Set<Expression> predicates = Sets.newLinkedHashSet(filter.getConjuncts());
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
            predicates.addAll(leftPredicates);
            predicates.addAll(rightPredicates);
            if (join.getJoinType() == JoinType.CROSS_JOIN || join.getJoinType() == JoinType.INNER_JOIN) {
                predicates.addAll(join.getHashJoinConjuncts());
                predicates.addAll(join.getOtherJoinConjuncts());
            }
            return getAvailableExpressions(predicates, join);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return cacheOrElse(project, () -> {
            ImmutableSet<Expression> childPredicates = project.child().accept(this, context);
            Set<Expression> allPredicates = Sets.newLinkedHashSet(childPredicates);
            for (Entry<Slot, Expression> kv : project.getAliasToProducer().entrySet()) {
                Slot k = kv.getKey();
                Expression v = kv.getValue();
                for (Expression childPredicate : childPredicates) {
                    allPredicates.add(childPredicate.rewriteDownShortCircuit(c -> c.equals(v) ? k : c));
                }
            }
            for (NamedExpression expr : project.getProjects()) {
                if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                    allPredicates.add(new EqualTo(expr.toSlot(), expr.child(0)));
                }
            }
            return getAvailableExpressions(allPredicates, project);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return cacheOrElse(aggregate, () -> {
            ImmutableSet<Expression> childPredicates = aggregate.child().accept(this, context);
            // TODO
            List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();

            Map<Expression, Slot> expressionSlotMap
                    = Maps.newLinkedHashMapWithExpectedSize(outputExpressions.size());
            for (NamedExpression output : outputExpressions) {
                if (hasAgg(output)) {
                    expressionSlotMap.putIfAbsent(
                            output instanceof Alias ? output.child(0) : output, output.toSlot()
                    );
                }
            }
            Expression expression = ExpressionUtils.replace(
                    ExpressionUtils.and(Lists.newArrayList(childPredicates)),
                    expressionSlotMap
            );
            Set<Expression> predicates = Sets.newLinkedHashSet(ExpressionUtils.extractConjunction(expression));
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

    private ImmutableSet<Expression> getAvailableExpressions(Set<Expression> predicates, Plan plan) {
        if (predicates.isEmpty()) {
            return ImmutableSet.of();
        }
        Set<Expression> inferPredicates = PredicatePropagation.infer(predicates);
        Builder<Expression> newPredicates = ImmutableSet.builderWithExpectedSize(predicates.size() + 10);
        Set<Slot> outputSet = plan.getOutputSet();

        for (Expression predicate : predicates) {
            if (outputSet.containsAll(predicate.getInputSlots())) {
                newPredicates.add(predicate);
            }
        }

        for (Expression inferPredicate : inferPredicates) {
            if (outputSet.containsAll(inferPredicate.getInputSlots())) {
                newPredicates.add(inferPredicate);
            }
        }
        return newPredicates.build();
    }

    private boolean hasAgg(Expression expression) {
        return expression.anyMatch(AggregateFunction.class::isInstance);
    }

    private ImmutableSet<Expression> getFiltersFromUnionChild(LogicalUnion union, Void context) {
        Set<Expression> filters = new HashSet<>();
        for (int i = 0; i < union.getArity(); ++i) {
            Plan child = union.child(i);
            Set<Expression> childFilters = child.accept(this, context);
            if (childFilters.isEmpty()) {
                return ImmutableSet.of();
            }
            Map<Expression, Expression> replaceMap = new HashMap<>();
            for (int j = 0; j < union.getOutput().size(); ++j) {
                NamedExpression output = union.getOutput().get(j);
                replaceMap.put(union.getRegularChildOutput(i).get(j), output);
            }
            Set<Expression> unionFilters = ExpressionUtils.replace(childFilters, replaceMap);
            if (0 == i) {
                filters.addAll(unionFilters);
            } else {
                filters.retainAll(unionFilters);
            }
            if (filters.isEmpty()) {
                return ImmutableSet.of();
            }
        }
        return ImmutableSet.copyOf(filters);
    }

    private ImmutableSet<Expression> getFiltersFromUnionConstExprs(LogicalUnion union) {
        List<List<NamedExpression>> constExprs = union.getConstantExprsList();
        ImmutableSet.Builder<Expression> filtersFromConstExprs = ImmutableSet.builder();
        for (int col = 0; col < union.getOutput().size(); ++col) {
            Expression compareExpr = union.getOutput().get(col);
            Set<Expression> options = new HashSet<>();
            for (List<NamedExpression> constExpr : constExprs) {
                if (constExpr.get(col) instanceof Alias
                        && ((Alias) constExpr.get(col)).child() instanceof Literal) {
                    options.add(((Alias) constExpr.get(col)).child());
                } else {
                    options.clear();
                    break;
                }
            }
            options.removeIf(option -> option instanceof NullLiteral);
            if (options.size() > 1) {
                filtersFromConstExprs.add(new InPredicate(compareExpr, options));
            } else if (options.size() == 1) {
                filtersFromConstExprs.add(new EqualTo(compareExpr, options.iterator().next()));
            }
        }
        return filtersFromConstExprs.build();
    }
}
