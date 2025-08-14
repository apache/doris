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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.FoldConstantRuleOnFE;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InPredicate;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.PredicateInferUtils;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;

/**
 * poll up effective predicates from operator's children.
 */
public class PullUpPredicates extends PlanVisitor<ImmutableSet<Expression>, Void> {

    private static final ImmutableSet<Class<? extends Expression>> supportAggFunctions = ImmutableSet.of(
            Max.class, Min.class, AnyValue.class);
    Map<Plan, ImmutableSet<Expression>> cache = new IdentityHashMap<>();
    private final boolean getAllPredicates;
    private final ExpressionRewriteContext rewriteContext;

    public PullUpPredicates(boolean all, CascadesContext cascadesContext) {
        getAllPredicates = all;
        rewriteContext = new ExpressionRewriteContext(cascadesContext);
    }

    @Override
    public ImmutableSet<Expression> visit(Plan plan, Void context) {
        return ImmutableSet.of();
    }

    @Override
    public ImmutableSet<Expression> visitLogicalSort(LogicalSort<? extends Plan> sort, Void context) {
        return cacheOrElse(sort, () -> sort.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalLimit(LogicalLimit<? extends Plan> limit, Void context) {
        return cacheOrElse(limit, () -> limit.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalTopN(LogicalTopN<? extends Plan> topN, Void context) {
        return cacheOrElse(topN, () -> topN.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalPartitionTopN(LogicalPartitionTopN<? extends Plan> topN, Void context) {
        return cacheOrElse(topN, () -> topN.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalGenerate(LogicalGenerate<? extends Plan> generate, Void context) {
        return cacheOrElse(generate, () -> generate.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalWindow(LogicalWindow<? extends Plan> window, Void context) {
        return cacheOrElse(window, () -> window.child(0).accept(this, context));
    }

    @Override
    public ImmutableSet<Expression> visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat, Void context) {
        return cacheOrElse(repeat, () -> {
            ImmutableSet<Expression> childPredicates = repeat.child().accept(this, context);
            Set<Expression> commonGroupingSetExpressions = repeat.getCommonGroupingSetExpressions();
            if (commonGroupingSetExpressions.isEmpty()) {
                return ImmutableSet.of();
            }

            Set<Expression> pulledPredicates = new LinkedHashSet<>();
            for (Expression conjunct : childPredicates) {
                Set<Slot> conjunctSlots = conjunct.getInputSlots();
                if (commonGroupingSetExpressions.containsAll(conjunctSlots)) {
                    pulledPredicates.add(conjunct);
                }
            }
            return ImmutableSet.copyOf(pulledPredicates);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalOneRowRelation(LogicalOneRowRelation r, Void context) {
        return cacheOrElse(r, () -> {
            Set<Expression> predicates = new LinkedHashSet<>();
            for (NamedExpression expr : r.getProjects()) {
                if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                    predicates.add(generateEqual(expr));
                }
            }
            return ImmutableSet.copyOf(predicates);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalIntersect(LogicalIntersect intersect, Void context) {
        return cacheOrElse(intersect, () -> {
            Set<Expression> predicates = new LinkedHashSet<>();
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
                predicates.addAll(ExpressionUtils.replace(childFilters, replaceMap));
            }
            return getAvailableExpressions(ImmutableSet.copyOf(predicates), intersect);
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
                Set<Expression> fromChildFilters = new LinkedHashSet<>(getFiltersFromUnionChild(union, context));
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
            Set<Expression> predicates = new LinkedHashSet<>();
            Supplier<ImmutableSet<Expression>> leftPredicates = Suppliers.memoize(
                    () -> join.left().accept(this, context));
            Supplier<ImmutableSet<Expression>> rightPredicates = Suppliers.memoize(
                    () -> join.right().accept(this, context));
            switch (join.getJoinType()) {
                case CROSS_JOIN:
                case INNER_JOIN: {
                    predicates.addAll(leftPredicates.get());
                    predicates.addAll(rightPredicates.get());
                    predicates.addAll(join.getHashJoinConjuncts());
                    predicates.addAll(join.getOtherJoinConjuncts());
                    break;
                }
                case LEFT_OUTER_JOIN:
                case LEFT_SEMI_JOIN:
                case LEFT_ANTI_JOIN:
                case NULL_AWARE_LEFT_ANTI_JOIN: {
                    predicates.addAll(leftPredicates.get());
                    break;
                }
                case RIGHT_OUTER_JOIN:
                case RIGHT_SEMI_JOIN:
                case RIGHT_ANTI_JOIN: {
                    predicates.addAll(rightPredicates.get());
                    break;
                }
                default:
                    break;
            }
            return getAvailableExpressions(predicates, join);
        });
    }

    @Override
    public ImmutableSet<Expression> visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
        return cacheOrElse(project, () -> {
            ImmutableSet<Expression> childPredicates = project.child().accept(this, context);
            Set<Expression> allPredicates = Sets.newLinkedHashSet();
            /* this generateMap is used to
            * e.g LogicalProject(t.a)   the qualifier t may come from LogicalSubQueryAlias
            *       +--LogicalFilter(a>1)
            * use generateMap to make sure a>1 is pulled up and turn into t.a>1
            * */
            for (Entry<Slot, Expression> kv : generateMap(project.getProjects()).entrySet()) {
                Slot k = kv.getKey();
                Expression v = kv.getValue();
                for (Expression childPredicate : childPredicates) {
                    allPredicates.add(childPredicate.rewriteDownShortCircuit(c -> c.equals(v) ? k : c));
                }
            }
            for (NamedExpression expr : project.getProjects()) {
                if (expr instanceof Alias && expr.child(0) instanceof Literal) {
                    allPredicates.add(generateEqual(expr));
                }
            }
            return getAvailableExpressions(allPredicates, project);
        });
    }

    /* e.g. LogicalAggregate(output:max(a), min(a), avg(a))
              +--LogicalFilter(a>1, a<10)
       when a>1 is pulled up, we can have max(a)>1, min(a)>1 and avg(a)>1
       and a<10 is pulled up, we can have max(a)<10, min(a)<10 and avg(a)<10
    * */
    @Override
    public ImmutableSet<Expression> visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
        return cacheOrElse(aggregate, () -> {
            ImmutableSet<Expression> childPredicates = aggregate.child().accept(this, context);
            List<NamedExpression> outputExpressions = aggregate.getOutputExpressions();
            Map<Expression, List<Slot>> expressionSlotMap
                    = Maps.newHashMapWithExpectedSize(outputExpressions.size());
            for (NamedExpression output : outputExpressions) {
                if (output instanceof Alias && supportPullUpAgg(output.child(0))) {
                    expressionSlotMap.computeIfAbsent(output.child(0).child(0),
                            k -> new ArrayList<>()).add(output.toSlot());
                }
            }
            Set<Expression> pullPredicates = new LinkedHashSet<>(childPredicates);
            boolean isScalar = aggregate.getGroupByExpressions().isEmpty();
            for (Expression childPredicate : childPredicates) {
                if (childPredicate instanceof ComparisonPredicate) {
                    ComparisonPredicate cmp = (ComparisonPredicate) childPredicate;
                    if (cmp.left() instanceof SlotReference && cmp.right() instanceof Literal
                            && expressionSlotMap.containsKey(cmp.left())) {
                        for (Slot slot : expressionSlotMap.get(cmp.left())) {
                            Expression genPredicates = TypeCoercionUtils.processComparisonPredicate(
                                     (ComparisonPredicate) cmp.withChildren(slot, cmp.right()));
                            genPredicates = FoldConstantRuleOnFE.evaluate(genPredicates, rewriteContext);
                            if (isScalar) {
                                // Aggregation will return null if there are no matching rows
                                pullPredicates.add(new Or(new IsNull(slot), genPredicates));
                            } else {
                                pullPredicates.add(genPredicates);
                            }
                        }
                    }
                }
            }
            return getAvailableExpressions(pullPredicates, aggregate);
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
        Set<Expression> inferPredicates = new LinkedHashSet<>();
        if (getAllPredicates) {
            inferPredicates.addAll(PredicateInferUtils.inferAllPredicate(predicates));
        } else {
            inferPredicates.addAll(PredicateInferUtils.inferPredicate(predicates));
        }
        Set<Expression> newPredicates = new LinkedHashSet<>(inferPredicates.size());
        Set<Slot> outputSet = plan.getOutputSet();

        for (Expression inferPredicate : inferPredicates) {
            if (outputSet.containsAll(inferPredicate.getInputSlots())) {
                newPredicates.add(inferPredicate);
            }
        }
        return ImmutableSet.copyOf(newPredicates);
    }

    private boolean supportPullUpAgg(Expression expr) {
        return supportAggFunctions.contains(expr.getClass());
    }

    private ImmutableSet<Expression> getFiltersFromUnionChild(LogicalUnion union, Void context) {
        Set<Expression> filters = new LinkedHashSet<>();
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
        Set<Expression> filtersFromConstExprs = new LinkedHashSet<>();
        for (int col = 0; col < union.getOutput().size(); ++col) {
            Expression compareExpr = union.getOutput().get(col);
            Set<Expression> options = new LinkedHashSet<>();
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
        return ImmutableSet.copyOf(filtersFromConstExprs);
    }

    private Expression generateEqual(NamedExpression expr) {
        // IsNull have better performance and compatibility than NullSafeEqualTo
        if (expr.child(0) instanceof NullLiteral) {
            return new IsNull(expr.toSlot());
        } else {
            return new EqualTo(expr.toSlot(), expr.child(0));
        }
    }

    private Map<Slot, Expression> generateMap(List<NamedExpression> namedExpressions) {
        Map<Slot, Expression> replaceMap = new LinkedHashMap<>(namedExpressions.size());
        for (NamedExpression namedExpression : namedExpressions) {
            if (namedExpression instanceof Alias) {
                replaceMap.putIfAbsent(namedExpression.toSlot(), namedExpression.child(0));
            } else if (namedExpression instanceof SlotReference) {
                replaceMap.putIfAbsent((Slot) namedExpression, namedExpression);
            }
        }
        return replaceMap;
    }
}
