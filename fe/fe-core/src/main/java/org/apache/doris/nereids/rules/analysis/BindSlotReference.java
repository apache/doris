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

import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.analyzer.UnboundStar;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.UnaryNode;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.ListQuery;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.VirtualSlotReference;
import org.apache.doris.nereids.trees.expressions.functions.PropagateNullable;
import org.apache.doris.nereids.trees.expressions.functions.agg.AggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.GroupingScalarFunction;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.AggPhase;
import org.apache.doris.nereids.trees.plans.GroupPlan;
import org.apache.doris.nereids.trees.plans.LeafPlan;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.GroupingSetShape;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.types.BigIntType;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.planner.PlannerContext;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * BindSlotReference.
 */
@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class BindSlotReference implements AnalysisRuleFactory {

    private final Optional<Scope> outerScope;

    public BindSlotReference(Optional<Scope> outerScope) {
        this.outerScope = Objects.requireNonNull(outerScope, "outerScope cannot be null");
    }

    private Scope toScope(List<Slot> slots) {
        if (outerScope.isPresent()) {
            return new Scope(outerScope, slots, outerScope.get().getSubquery());
        } else {
            return new Scope(slots);
        }
    }

    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.BINDING_PROJECT_SLOT.build(
                logicalProject().when(Plan::canBind).thenApply(ctx -> {
                    LogicalProject<GroupPlan> project = ctx.root;
                    List<NamedExpression> boundSlots =
                            bind(project.getProjects(), project.children(), project, ctx.cascadesContext);
                    return new LogicalProject<>(flatBoundStar(boundSlots), project.child());
                })
            ),
            RuleType.BINDING_FILTER_SLOT.build(
                logicalFilter().when(Plan::canBind).thenApply(ctx -> {
                    LogicalFilter<GroupPlan> filter = ctx.root;
                    Expression boundPredicates = bind(filter.getPredicates(), filter.children(),
                            filter, ctx.cascadesContext);
                    return new LogicalFilter<>(boundPredicates, filter.child());
                })
            ),
            RuleType.BINDING_JOIN_SLOT.build(
                logicalJoin().when(Plan::canBind).thenApply(ctx -> {
                    LogicalJoin<GroupPlan, GroupPlan> join = ctx.root;
                    List<Expression> cond = join.getOtherJoinConjuncts().stream()
                            .map(expr -> bind(expr, join.children(), join, ctx.cascadesContext))
                            .collect(Collectors.toList());
                    List<Expression> hashJoinConjuncts = join.getHashJoinConjuncts().stream()
                            .map(expr -> bind(expr, join.children(), join, ctx.cascadesContext))
                            .collect(Collectors.toList());
                    return new LogicalJoin<>(join.getJoinType(),
                            hashJoinConjuncts, cond, join.left(), join.right());
                })
            ),
            RuleType.BINDING_AGGREGATE_SLOT.build(
                logicalAggregate().when(Plan::canBind).thenApply(ctx -> {
                    LogicalAggregate<GroupPlan> agg = ctx.root;
                    List<NamedExpression> output =
                            bind(agg.getOutputExpressions(), agg.children(), agg, ctx.cascadesContext);
                    final Map<String, Expression> substitutions = output.stream()
                            .filter(ne -> ne instanceof Alias)
                            .map(Alias.class::cast)
                            .collect(Collectors.toMap(Alias::getName, UnaryNode::child));
                    List<Expression> replacedGroupBy = agg.getGroupByExpressions().stream()
                            .map(g -> {
                                if (g instanceof UnboundSlot) {
                                    UnboundSlot unboundSlot = (UnboundSlot) g;
                                    if (unboundSlot.getNameParts().size() == 1) {
                                        String name = unboundSlot.getNameParts().get(0);
                                        if (substitutions.containsKey(name)) {
                                            return substitutions.get(name);
                                        }
                                    }
                                }
                                return g;
                            }).collect(Collectors.toList());

                    List<Expression> groupBy = bind(replacedGroupBy, agg.children(), agg, ctx.cascadesContext);
                    return agg.withGroupByAndOutput(groupBy, output);
                })
            ),
            RuleType.BINDING_REPEAT_SLOT.build(
                    logicalRepeat().when(Plan::canBind).thenApply(ctx -> {
                        LogicalRepeat<GroupPlan> repeat = ctx.root;

                        List<List<Expression>> groupingSets = repeat.getGroupingSets().stream()
                                .map(expr -> bind(expr, repeat.children(), repeat, ctx.cascadesContext))
                                .collect(Collectors.toList());
                        List<Expression> nonVirtualGroupByExpressions =
                                bind(repeat.getNonVirtualGroupByExpressions(),
                                        repeat.children(), repeat, ctx.cascadesContext);
                        List<NamedExpression> output =
                                bind(repeat.getOutputExpressions(),
                                        repeat.children(), repeat, ctx.cascadesContext);

                        NewOutputsAndVirtualSlot outputAndVirtualSlot =
                                buildNewOutputByReplaceGroupingFunc(output);
                        List<GroupingSetShape> groupingSetIdSlots = repeat.buildGroupingSetShapes(
                                groupingSets, nonVirtualGroupByExpressions, outputAndVirtualSlot.getOutputs());

                        List<Expression> newGroupByExpressions = new ImmutableList.Builder<Expression>()
                                .addAll(nonVirtualGroupByExpressions) // nonVirtualSlot
                                .addAll(repeat.getVirtualGroupByExpressions()) // extra virtualSlot
                                .addAll(outputAndVirtualSlot.getVirtualSlotRefs()) // groupingFunc virtualSlot
                                .build();
                        if (newGroupByExpressions.size() > LogicalRepeat.MAX_GROUPING_SETS_NUM) {
                            throw new AnalysisException(
                                    "Too many sets in GROUP BY clause, the max grouping sets item is "
                                    + LogicalRepeat.MAX_GROUPING_SETS_NUM);
                        }

                        return buildNewAggWithNormalizeRepeat(newGroupByExpressions,
                                outputAndVirtualSlot.getOutputs(), groupingSets, groupingSetIdSlots, repeat);
                    })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalAggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalAggregate<GroupPlan>> sort = ctx.root;
                    LogicalAggregate<GroupPlan> aggregate = sort.child();
                    return bindSortWithAggregateFunction(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalHaving(logicalAggregate())).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalHaving<LogicalAggregate<GroupPlan>>> sort = ctx.root;
                    LogicalAggregate<GroupPlan> aggregate = sort.child().child();
                    return bindSortWithAggregateFunction(sort, aggregate, ctx.cascadesContext);
                })
            ),
            RuleType.BINDING_SORT_SLOT.build(
                logicalSort(logicalProject()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalSort<LogicalProject<GroupPlan>> sort = ctx.root;
                    List<OrderKey> sortItemList = sort.getOrderKeys()
                            .stream()
                            .map(orderKey -> {
                                Expression item = bind(orderKey.getExpr(), sort.children(), sort, ctx.cascadesContext);
                                if (item.containsType(UnboundSlot.class)) {
                                    item = bind(item, sort.child().children(), sort, ctx.cascadesContext);
                                }
                                return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                            }).collect(Collectors.toList());

                    return new LogicalSort<>(sortItemList, sort.child());
                })
            ),
            RuleType.BINDING_HAVING_SLOT.build(
                logicalHaving(logicalAggregate()).when(Plan::canBind).thenApply(ctx -> {
                    LogicalHaving<LogicalAggregate<GroupPlan>> having = ctx.root;
                    LogicalAggregate<GroupPlan> aggregate = having.child();
                    // We should deduplicate the slots, otherwise the binding process will fail due to the
                    // ambiguous slots exist.
                    Set<Slot> boundSlots = Stream.concat(Stream.of(aggregate), aggregate.children().stream())
                            .flatMap(plan -> plan.getOutput().stream())
                            .collect(Collectors.toSet());
                    Expression boundPredicates = new SlotBinder(
                            toScope(new ArrayList<>(boundSlots)), having, ctx.cascadesContext
                    ).bind(having.getPredicates());
                    return new LogicalHaving<>(boundPredicates, having.child());
                })
            ),
            RuleType.BINDING_HAVING_REPEAT_SLOT.build(
                logicalHaving(logicalProject(logicalAggregate(logicalRepeat(logicalProject())))).when(Plan::canBind)
                    .thenApply(ctx -> {
                        LogicalHaving<LogicalProject
                                <LogicalAggregate<LogicalRepeat<LogicalProject<GroupPlan>>>>> having = ctx.root;
                        LogicalProject<LogicalAggregate
                                <LogicalRepeat<LogicalProject<GroupPlan>>>> project = having.child();
                        LogicalAggregate<LogicalRepeat<LogicalProject<GroupPlan>>> aggregate = project.child();
                        LogicalRepeat<LogicalProject<GroupPlan>> repeat = aggregate.child();
                        LogicalProject<GroupPlan> bottomProject = repeat.child();
                        // We should deduplicate the slots, otherwise the binding process will fail due to the
                        // ambiguous slots exist.
                        Set<Slot> boundSlots = Stream.concat(
                                Stream.of(bottomProject), bottomProject.children().stream())
                                .flatMap(plan -> plan.getOutput().stream())
                                .collect(Collectors.toSet());
                        Expression boundPredicates = new SlotBinder(
                                toScope(new ArrayList<>(boundSlots)), having, ctx.cascadesContext
                        ).bind(having.getPredicates());
                        return new LogicalHaving<>(boundPredicates, having.child());
                    })
            ),
            RuleType.BINDING_ONE_ROW_RELATION_SLOT.build(
                    // we should bind UnboundAlias in the UnboundOneRowRelation
                    unboundOneRowRelation().thenApply(ctx -> {
                        UnboundOneRowRelation oneRowRelation = ctx.root;
                        List<NamedExpression> projects = oneRowRelation.getProjects()
                                .stream()
                                .map(project -> bind(project, ImmutableList.of(), oneRowRelation, ctx.cascadesContext))
                                .collect(Collectors.toList());
                        return new LogicalOneRowRelation(projects);
                    })
            ),

            RuleType.BINDING_NON_LEAF_LOGICAL_PLAN.build(
                logicalPlan()
                        .when(plan -> plan.canBind() && !(plan instanceof LeafPlan))
                        .then(LogicalPlan::recomputeLogicalProperties)
            )
        );
    }

    private Plan bindSortWithAggregateFunction(
            LogicalSort<? extends Plan> sort, LogicalAggregate<? extends Plan> aggregate, CascadesContext ctx) {
        // We should deduplicate the slots, otherwise the binding process will fail due to the
        // ambiguous slots exist.
        Set<Slot> boundSlots = Stream.concat(Stream.of(aggregate), aggregate.children().stream())
                .flatMap(plan -> plan.getOutput().stream())
                .collect(Collectors.toSet());
        List<OrderKey> sortItemList = sort.getOrderKeys()
                .stream()
                .map(orderKey -> {
                    Expression item = new SlotBinder(toScope(new ArrayList<>(boundSlots)), sort, ctx)
                            .bind(orderKey.getExpr());
                    return new OrderKey(item, orderKey.isAsc(), orderKey.isNullFirst());
                }).collect(Collectors.toList());
        return new LogicalSort<>(sortItemList, sort.child());
    }

    private List<NamedExpression> flatBoundStar(List<NamedExpression> boundSlots) {
        return boundSlots
            .stream()
            .flatMap(slot -> {
                if (slot instanceof BoundStar) {
                    return ((BoundStar) slot).getSlots().stream();
                } else {
                    return Stream.of(slot);
                }
            }).collect(Collectors.toList());
    }

    private <E extends Expression> List<E> bind(List<E> exprList, List<Plan> inputs, Plan plan,
            CascadesContext cascadesContext) {
        return exprList.stream()
            .map(expr -> bind(expr, inputs, plan, cascadesContext))
            .collect(Collectors.toList());
    }

    private <E extends Expression> E bind(E expr, List<Plan> inputs, Plan plan, CascadesContext cascadesContext) {
        List<Slot> boundedSlots = inputs.stream()
                .flatMap(input -> input.getOutput().stream())
                .collect(Collectors.toList());
        return (E) new SlotBinder(toScope(boundedSlots), plan, cascadesContext).bind(expr);
    }

    private class SlotBinder extends SubExprAnalyzer {
        private final Plan plan;

        public SlotBinder(Scope scope, Plan plan, CascadesContext cascadesContext) {
            super(scope, cascadesContext);
            this.plan = plan;
        }

        public Expression bind(Expression expression) {
            return expression.accept(this, null);
        }

        @Override
        public Expression visitUnboundAlias(UnboundAlias unboundAlias, PlannerContext context) {
            Expression child = unboundAlias.child().accept(this, context);
            if (unboundAlias.getAlias().isPresent()) {
                return new Alias(child, unboundAlias.getAlias().get());
            }
            if (child instanceof NamedExpression) {
                return new Alias(child, ((NamedExpression) child).getName());
            } else {
                // TODO: resolve aliases
                return new Alias(child, child.toSql());
            }
        }

        @Override
        public Slot visitUnboundSlot(UnboundSlot unboundSlot, PlannerContext context) {
            Optional<List<Slot>> boundedOpt = Optional.of(bindSlot(unboundSlot, getScope().getSlots()));
            boolean foundInThisScope = !boundedOpt.get().isEmpty();
            // Currently only looking for symbols on the previous level.
            if (!foundInThisScope && getScope().getOuterScope().isPresent()) {
                boundedOpt = Optional.of(bindSlot(unboundSlot,
                        getScope()
                        .getOuterScope()
                        .get()
                        .getSlots()));
            }
            List<Slot> bounded = boundedOpt.get();
            switch (bounded.size()) {
                case 0:
                    // just return, give a chance to bind on another slot.
                    // if unbound finally, check will throw exception
                    return unboundSlot;
                case 1:
                    if (!foundInThisScope) {
                        getScope().getOuterScope().get().getCorrelatedSlots().add(bounded.get(0));
                    }
                    return bounded.get(0);
                default:
                    throw new AnalysisException(String.format("%s is ambiguous: %s.",
                            unboundSlot.toSql(),
                            bounded.stream()
                                    .map(Slot::toString)
                                    .collect(Collectors.joining(", "))));
            }
        }

        @Override
        public Expression visitUnboundStar(UnboundStar unboundStar, PlannerContext context) {
            if (!(plan instanceof LogicalProject)) {
                throw new AnalysisException("UnboundStar must exists in Projection");
            }
            List<String> qualifier = unboundStar.getQualifier();
            switch (qualifier.size()) {
                case 0: // select *
                    return new BoundStar(getScope().getSlots());
                case 1: // select table.*
                case 2: // select db.table.*
                    return bindQualifiedStar(qualifier);
                default:
                    throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
            }
        }

        private BoundStar bindQualifiedStar(List<String> qualifierStar) {
            // FIXME: compatible with previous behavior:
            // https://github.com/apache/doris/pull/10415/files/3fe9cb0c3f805ab3a9678033b281b16ad93ec60a#r910239452
            List<Slot> slots = getScope().getSlots().stream().filter(boundSlot -> {
                switch (qualifierStar.size()) {
                    // table.*
                    case 1:
                        List<String> boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0: return false;
                            case 1: // bound slot is `table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0));
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, "."));
                        }
                    case 2: // db.table.*
                        boundSlotQualifier = boundSlot.getQualifier();
                        switch (boundSlotQualifier.size()) {
                            // bound slot is `column` and no qualified
                            case 0:
                            case 1: // bound slot is `table`.`column`
                                return false;
                            case 2:// bound slot is `db`.`table`.`column`
                                return qualifierStar.get(0).equalsIgnoreCase(boundSlotQualifier.get(0))
                                        && qualifierStar.get(1).equalsIgnoreCase(boundSlotQualifier.get(1));
                            default:
                                throw new AnalysisException("Not supported qualifier: "
                                    + StringUtils.join(qualifierStar, ".") + ".*");
                        }
                    default:
                        throw new AnalysisException("Not supported name: "
                            + StringUtils.join(qualifierStar, ".") + ".*");
                }
            }).collect(Collectors.toList());

            return new BoundStar(slots);
        }

        private List<Slot> bindSlot(UnboundSlot unboundSlot, List<Slot> boundSlots) {
            return boundSlots.stream().filter(boundSlot -> {
                List<String> nameParts = unboundSlot.getNameParts();
                if (nameParts.size() == 1) {
                    return nameParts.get(0).equalsIgnoreCase(boundSlot.getName());
                } else if (nameParts.size() <= 3) {
                    int size = nameParts.size();
                    // if nameParts.size() == 3, nameParts.get(0) is cluster name.
                    return handleNamePartsTwoOrThree(boundSlot, nameParts.subList(size - 2, size));
                }
                //TODO: handle name parts more than three.
                throw new AnalysisException("Not supported name: "
                        + StringUtils.join(nameParts, "."));
            }).collect(Collectors.toList());
        }
    }

    private boolean handleNamePartsTwoOrThree(Slot boundSlot, List<String> nameParts) {
        List<String> qualifier = boundSlot.getQualifier();
        String name = boundSlot.getName();
        switch (qualifier.size()) {
            case 2:
                // qualifier is `db`.`table`
                return nameParts.get(0).equalsIgnoreCase(qualifier.get(1))
                        && nameParts.get(1).equalsIgnoreCase(name);
            case 1:
                // qualifier is `table`
                return nameParts.get(0).equalsIgnoreCase(qualifier.get(0))
                        && nameParts.get(1).equalsIgnoreCase(name);
            case 0:
                // has no qualifiers
                return nameParts.get(1).equalsIgnoreCase(name);
            default:
                throw new AnalysisException("Not supported qualifier: "
                        + StringUtils.join(qualifier, "."));
        }
    }

    /** BoundStar is used to wrap list of slots for temporary. */
    private class BoundStar extends NamedExpression implements PropagateNullable {
        public BoundStar(List<Slot> children) {
            super(children.toArray(new Slot[0]));
            Preconditions.checkArgument(children.stream().noneMatch(slot -> slot instanceof UnboundSlot),
                    "BoundStar can not wrap UnboundSlot"
            );
        }

        public String toSql() {
            return children.stream().map(Expression::toSql).collect(Collectors.joining(", "));
        }

        public List<Slot> getSlots() {
            return (List) children();
        }
    }

    /**
     * Use the visitor to iterate sub expression.
     */
    private static class SubExprAnalyzer extends DefaultExpressionRewriter<PlannerContext> {
        private final Scope scope;
        private final CascadesContext cascadesContext;

        public SubExprAnalyzer(Scope scope, CascadesContext cascadesContext) {
            this.scope = scope;
            this.cascadesContext = cascadesContext;
        }

        @Override
        public Expression visitNot(Not not, PlannerContext context) {
            Expression child = not.child();
            if (child instanceof Exists) {
                return visitExistsSubquery(
                        new Exists(((Exists) child).getQueryPlan(), true), context);
            } else if (child instanceof InSubquery) {
                return visitInSubquery(new InSubquery(((InSubquery) child).getCompareExpr(),
                        ((InSubquery) child).getListQuery(), true), context);
            }
            return visit(not, context);
        }

        @Override
        public Expression visitExistsSubquery(Exists exists, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(exists);

            return new Exists(analyzedResult.getLogicalPlan(),
                    analyzedResult.getCorrelatedSlots(), exists.isNot());
        }

        @Override
        public Expression visitInSubquery(InSubquery expr, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(expr);

            checkOutputColumn(analyzedResult.getLogicalPlan());
            checkHasGroupBy(analyzedResult);

            return new InSubquery(
                    expr.getCompareExpr().accept(this, context),
                    new ListQuery(analyzedResult.getLogicalPlan()),
                    analyzedResult.getCorrelatedSlots(), expr.isNot());
        }

        @Override
        public Expression visitScalarSubquery(ScalarSubquery scalar, PlannerContext context) {
            AnalyzedResult analyzedResult = analyzeSubquery(scalar);

            checkOutputColumn(analyzedResult.getLogicalPlan());
            checkRootIsAgg(analyzedResult);
            checkHasGroupBy(analyzedResult);

            return new ScalarSubquery(analyzedResult.getLogicalPlan(), analyzedResult.getCorrelatedSlots());
        }

        private void checkOutputColumn(LogicalPlan plan) {
            if (plan.getOutput().size() != 1) {
                throw new AnalysisException("Multiple columns returned by subquery are not yet supported. Found "
                        + plan.getOutput().size());
            }
        }

        private void checkRootIsAgg(AnalyzedResult analyzedResult) {
            if (!analyzedResult.isCorrelated()) {
                return;
            }
            if (!analyzedResult.rootIsAgg()) {
                throw new AnalysisException("The select item in correlated subquery of binary predicate "
                        + "should only be sum, min, max, avg and count. Current subquery: "
                        + analyzedResult.getLogicalPlan());
            }
        }

        private void checkHasGroupBy(AnalyzedResult analyzedResult) {
            if (!analyzedResult.isCorrelated()) {
                return;
            }
            if (analyzedResult.hasGroupBy()) {
                throw new AnalysisException("Unsupported correlated subquery with grouping and/or aggregation "
                        + analyzedResult.getLogicalPlan());
            }
        }

        private AnalyzedResult analyzeSubquery(SubqueryExpr expr) {
            CascadesContext subqueryContext = new Memo(expr.getQueryPlan())
                    .newCascadesContext((cascadesContext.getStatementContext()));
            Scope subqueryScope = genScopeWithSubquery(expr);
            subqueryContext
                    .newAnalyzer(Optional.of(subqueryScope))
                    .analyze();
            return new AnalyzedResult((LogicalPlan) subqueryContext.getMemo().copyOut(false),
                    subqueryScope.getCorrelatedSlots());
        }

        private Scope genScopeWithSubquery(SubqueryExpr expr) {
            return new Scope(getScope().getOuterScope(),
                    getScope().getSlots(),
                    Optional.ofNullable(expr));
        }

        public Scope getScope() {
            return scope;
        }

        public CascadesContext getCascadesContext() {
            return cascadesContext;
        }
    }

    private static class AnalyzedResult {
        private final LogicalPlan logicalPlan;
        private final List<Slot> correlatedSlots;

        public AnalyzedResult(LogicalPlan logicalPlan, List<Slot> correlatedSlots) {
            this.logicalPlan = Objects.requireNonNull(logicalPlan, "logicalPlan can not be null");
            this.correlatedSlots = correlatedSlots == null ? new ArrayList<>() : ImmutableList.copyOf(correlatedSlots);
        }

        public LogicalPlan getLogicalPlan() {
            return logicalPlan;
        }

        public List<Slot> getCorrelatedSlots() {
            return correlatedSlots;
        }

        public boolean isCorrelated() {
            return !correlatedSlots.isEmpty();
        }

        public boolean rootIsAgg() {
            return logicalPlan instanceof LogicalAggregate;
        }

        public boolean hasGroupBy() {
            if (rootIsAgg()) {
                return !((LogicalAggregate<? extends Plan>) logicalPlan).getGroupByExpressions().isEmpty();
            }
            return false;
        }
    }

    private NewOutputsAndVirtualSlot buildNewOutputByReplaceGroupingFunc(
            List<NamedExpression> outputs) {
        Set<VirtualSlotReference> newVirtualSlotRefsSets = new LinkedHashSet<>();
        List<VirtualSlotReference> newVirtualSlotRefs = new ArrayList<>();
        // resolve grouping func in repeat
        List<NamedExpression> newOutputs = new ArrayList<>();
        for (Expression output : outputs) {
            GenerateVirtualSlotResult result = resolve(output);
            if (result.getVirtualSlotReference() != null) {
                if (!newVirtualSlotRefsSets.contains(result.getVirtualSlotReference())) {
                    newVirtualSlotRefsSets.add(result.getVirtualSlotReference());
                    newVirtualSlotRefs.add(result.getVirtualSlotReference());
                }
            }
            newOutputs.add(((NamedExpression) result.getExpression()));
        }
        return new NewOutputsAndVirtualSlot(newOutputs, newVirtualSlotRefs);
    }

    private static class NewOutputsAndVirtualSlot {
        private final List<NamedExpression> outputs;
        private final List<VirtualSlotReference> virtualSlotRefs;

        public NewOutputsAndVirtualSlot(
                List<NamedExpression> outputs, List<VirtualSlotReference> virtualSlotRefs) {
            this.outputs = ImmutableList.copyOf(outputs);
            this.virtualSlotRefs = ImmutableList.copyOf(virtualSlotRefs);
        }

        public List<NamedExpression> getOutputs() {
            return outputs;
        }

        public List<VirtualSlotReference> getVirtualSlotRefs() {
            return virtualSlotRefs;
        }
    }

    /**
     * Replace expr in GroupingFunction with virtualSlotReference.
     *
     * @return new GroupingFunction and new virtualSlotReference.
     */
    private GenerateVirtualSlotResult resolve(Expression expr) {
        GenVirtualSlotForGroupingFunction genVirtualSlotForGroupingFunction = new GenVirtualSlotForGroupingFunction();
        Expression newGroupingFunction = genVirtualSlotForGroupingFunction.resolve(expr);
        return new GenerateVirtualSlotResult(
                genVirtualSlotForGroupingFunction.getNewVirtualSlotRef(),
                newGroupingFunction);
    }

    /**
     * Replace the parameters in groupingFunction with virtualSlotReference.
     */
    private static class GenVirtualSlotForGroupingFunction
            extends DefaultExpressionRewriter<PlannerContext> {
        private VirtualSlotReference newVirtualSlotRef;

        public VirtualSlotReference getNewVirtualSlotRef() {
            return newVirtualSlotRef;
        }

        public Expression resolve(Expression expr) {
            return expr.accept(this, null);
        }

        @Override
        public Expression visitGroupingScalarFunction(
                GroupingScalarFunction groupingScalarFunction, PlannerContext ctx) {
            if (groupingScalarFunction.child(0) instanceof VirtualSlotReference) {
                return groupingScalarFunction;
            }
            return genVirtualSlotReference(groupingScalarFunction);
        }

        private Expression genVirtualSlotReference(GroupingScalarFunction groupingScalarFunction) {
            String colName = groupingScalarFunction.children().stream()
                    .map(Expression::toSql).collect(Collectors.joining("_"));
            colName = LogicalRepeat.GROUPING_PREFIX + colName;
            newVirtualSlotRef = new VirtualSlotReference(
                    colName, BigIntType.INSTANCE, groupingScalarFunction.children(), false);
            return groupingScalarFunction.repeatChildrenWithVirtualRef(newVirtualSlotRef,
                    Optional.of(groupingScalarFunction.children()));
        }
    }

    private static class GenerateVirtualSlotResult {
        private final VirtualSlotReference virtualSlotReference;
        private final Expression expression;

        public GenerateVirtualSlotResult(VirtualSlotReference virtualSlotReference, Expression expression) {
            this.virtualSlotReference = virtualSlotReference;
            this.expression = Objects.requireNonNull(expression, "expression can not be null");
        }

        public VirtualSlotReference getVirtualSlotReference() {
            return virtualSlotReference;
        }

        public Expression getExpression() {
            return expression;
        }
    }

    /**
     * eg: select sum(k2 + 1), grouping(k1) from t1 group by grouping sets ((k1));
     * Original Plan:
     *     +-- GroupingSets(
     *         keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
     *         outputs:sum(k2#2 + 1) as `sum(k2 + 1)`#3, group(grouping_prefix(k1#1)#7) as `grouping(k1 + 1)`#4
     *
     * After:
     * Project(sum((k2 + 1)#8) AS `sum((k2 + 1))`#9, grouping(GROUPING_PREFIX_(k1#1)#7)) as `grouping(k1)`#10)
     *   +-- Aggregate(
     *          keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
     *          outputs:[(K2 + 1)#8), grouping_prefix(k1#1)#7]
     *         +-- GropingSets(
     *             keys:[k1#1, grouping_id()#0, grouping_prefix(k1#1)#7]
     *             outputs:k1#1, (k2 + 1)#8, grouping_id()#0, grouping_prefix(k1#1)#7
     *             +-- Project(k1#1, (K2#2 + 1) as `(k2 + 1)`#8)
     */
    private LogicalPlan buildNewAggWithNormalizeRepeat(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            List<List<Expression>> groupingSets,
            List<GroupingSetShape> groupingSetIdSlots,
            LogicalRepeat<GroupPlan> repeat) {
        Set<AggregateFunction> aggregateFunctions = collectAggregateFunctions(outputExpressions);
        Set<GroupingScalarFunction> groupingFunctions = collectGroupingFunctions(outputExpressions);

        // 1. generate new substituteMap
        // substitution map used to substitute expression in repeat's output to use it as top projections
        Map<Expression, Expression> substitutionMap =
                buildSubstitutionMap(groupByExpressions, aggregateFunctions, groupingFunctions);

        // 3. generate new groupByExpression
        List<Expression> newGroupByExpressions = generateNewGroupByExpressions(
                groupByExpressions, substitutionMap);

        // 4. generate new agg outputs
        List<NamedExpression> aggOutputs =
                buildAggOutputs(groupByExpressions, outputExpressions,
                        substitutionMap, aggregateFunctions, groupingFunctions);

        // 5. generate new repeat outputs
        Set<NamedExpression> repeatOutputs = buildRepeatOutputs(groupByExpressions,
                substitutionMap, aggregateFunctions, groupingFunctions);

        // 6. build bottom Projections
        List<NamedExpression> bottomProjects = buildBottomProjections(
                groupByExpressions, aggregateFunctions, substitutionMap);

        // 7. update substituteMap for aggFunc
        Map<Expression, Expression> updatedSubstitutionMap =
                updateSubstitutionMap(substitutionMap, aggregateFunctions);

        // assemble
        List<NamedExpression> newRepeatOutputs =
                reorderProjections(new ArrayList<>(repeatOutputs));
        List<NamedExpression> newAggOutputs =
                reorderProjections(aggOutputs);
        LogicalAggregate<GroupPlan> agg = new LogicalAggregate(newGroupByExpressions, newAggOutputs,
                false, true, true, AggPhase.LOCAL,
                repeat.replaceWithChild(groupingSets, newGroupByExpressions, newRepeatOutputs, groupingSetIdSlots,
                        new LogicalProject<>(bottomProjects, repeat.child())));
        List<NamedExpression> projections = outputExpressions.stream()
                .map(e -> ExpressionUtils.replace(e, updatedSubstitutionMap))
                .map(NamedExpression.class::cast)
                .collect(Collectors.toList());
        return new LogicalProject<>(projections, agg);
    }

    private Set<AggregateFunction> collectAggregateFunctions(List<NamedExpression> outputs) {
        Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                .collect(Collectors.groupingBy(e -> e.anyMatch(AggregateFunction.class::isInstance)));
        return partitionedOutputs.containsKey(true)
                ? partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<AggregateFunction>>collect(
                        AggregateFunction.class::isInstance).stream())
                .collect(Collectors.toSet())
                : Sets.newHashSet();
    }

    /**
     * get groupingFunc from outputExpressions
     */
    private Set<GroupingScalarFunction> collectGroupingFunctions(List<NamedExpression> outputs) {
        Map<Boolean, List<NamedExpression>> partitionedOutputs = outputs.stream()
                .collect(Collectors.groupingBy(e -> e.anyMatch(GroupingScalarFunction.class::isInstance)));
        return partitionedOutputs.containsKey(true)
                ? partitionedOutputs.get(true).stream()
                .flatMap(e -> e.<Set<GroupingScalarFunction>>collect(
                        GroupingScalarFunction.class::isInstance).stream())
                .collect(Collectors.toSet())
                : Sets.newHashSet();
    }

    /**
     * generate new groupByExpressions.
     */
    private List<Expression> generateNewGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        Map<Boolean, List<Expression>> whetherIsVirtualSlots = groupByExpressions.stream()
                .collect(Collectors.groupingBy(VirtualSlotReference.class::isInstance));
        List<Expression> newGroupByExpressions = new ImmutableList.Builder<Expression>()
                .addAll(buildNewGroupByWithNonVirtualSlot(whetherIsVirtualSlots, substitutionMap))
                .addAll(buildVirtualSlotForNewGroupBy(whetherIsVirtualSlots, substitutionMap))
                .build();

        return newGroupByExpressions;
    }

    private List<Expression> buildNewGroupByWithNonVirtualSlot(
            Map<Boolean, List<Expression>> whetherIsVirtualSlots,
            Map<Expression, Expression> substitutionMap) {
        List<Expression> newGroupByExpressions = new ArrayList<>();
        if (whetherIsVirtualSlots.containsKey(false)) {
            for (Expression groupByExpression : whetherIsVirtualSlots.get(false)) {
                if (groupByExpression instanceof SlotReference) {
                    newGroupByExpressions.add(groupByExpression);
                } else {
                    newGroupByExpressions.add(substitutionMap.get(groupByExpression));
                }
            }
        }
        return newGroupByExpressions;
    }

    private List<Expression> buildVirtualSlotForNewGroupBy(
            Map<Boolean, List<Expression>> whetherIsVirtualSlots,
            Map<Expression, Expression> substitutionMap) {
        List<Expression> newGroupByExpressions = new ArrayList<>();
        for (Expression virtualSLot : whetherIsVirtualSlots.get(true)) {
            if (((VirtualSlotReference) virtualSLot).getRealSlots().isEmpty()) {
                newGroupByExpressions.add(virtualSLot);
            } else {
                newGroupByExpressions.add((substitutionMap.get(virtualSLot)));
            }
        }
        return newGroupByExpressions;
    }

    private Map<Expression, Expression> buildSubstitutionMap(
            List<Expression> groupByExpressions,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingFunctions) {
        Map<Expression, Expression> nonVirtualSlotMap = buildSubstitutionMapByNonVirtualSlot(groupByExpressions);
        Map<Expression, Expression> virtualSlotMap = buildSubstitutionMapByGroupingFunc(
                groupingFunctions, nonVirtualSlotMap);
        Map<Expression, Expression> aggFuncMap =
                buildSubstitutionMapByAggFunc(aggregateFunctions, nonVirtualSlotMap, virtualSlotMap);
        return new ImmutableMap.Builder<Expression, Expression>()
                .putAll(nonVirtualSlotMap)
                .putAll(virtualSlotMap)
                .putAll(aggFuncMap)
                .build();
    }

    private Map<Expression, Expression> buildSubstitutionMapByNonVirtualSlot(
            List<Expression> groupByExpressions) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        // build with nonVirtual slot
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip groupingFunction
                if (groupByExpression instanceof VirtualSlotReference
                        && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty()) {
                    continue;
                }
                substitutionMap.put(groupByExpression, groupByExpression);
            } else {
                Alias alias = new Alias(groupByExpression, groupByExpression.toSql());
                substitutionMap.put(groupByExpression, alias.toSlot());
                substitutionMap.put(alias.toSlot(), alias);
            }
        }
        return substitutionMap;
    }

    private Map<Expression, Expression> buildSubstitutionMapByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> nonVirtualSlotMap,
            Map<Expression, Expression> virtualSlotMap) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (!(child instanceof SlotReference || child instanceof Literal)) {
                    if (nonVirtualSlotMap.containsKey(child)) {
                        newChildren.add(((Alias) nonVirtualSlotMap.get(child)).toSlot());
                    } else if (virtualSlotMap.containsKey(child)) {
                        newChildren.add(((Alias) virtualSlotMap.get(child)).toSlot());
                    } else {
                        Alias alias = new Alias(child, child.toSql());
                        newChildren.add(alias.toSlot());
                        substitutionMap.put(child, alias);
                    }
                } else {
                    newChildren.add(child);
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            Alias alias = new Alias(newFunction, newFunction.toSql());
            substitutionMap.put(aggregateFunction, alias.toSlot());
            substitutionMap.put(newFunction, alias);
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    /**
     * Generate a new virtualSlotReference for each groupingFunc by
     * replacing non-slotReference internal expressions with alisa.
     *
     * eg:
     *      old: GROUPING_PREFIX_k1(k1#0 + 1)
     *      new: GROUPING_PREFIX_k1((k1 + 1)#2)
     * @return List(Pair(old, new))
     */
    private Map<Expression, Expression> buildSubstitutionMapByGroupingFunc(
            Set<GroupingScalarFunction> groupingSetsFunctions,
            Map<Expression, Expression> nonVirtualSlotMap) {
        Map<Expression, Expression> substitutionMap = new HashMap<>();
        for (GroupingScalarFunction groupingSetsFunction : groupingSetsFunctions) {
            List<Expression> outerChildren = Lists.newArrayList();
            for (Expression child : groupingSetsFunction.getArguments()) {
                List<Expression> innerChildren = Lists.newArrayList();
                for (Expression realChild : ((VirtualSlotReference) child).getRealSlots()) {
                    if (realChild instanceof SlotReference || realChild instanceof Literal) {
                        innerChildren.add(realChild);
                    } else {
                        if (!nonVirtualSlotMap.containsKey(realChild)) {
                            Alias alias = new Alias(realChild, realChild.toSql());
                            innerChildren.add(alias.toSlot());
                            substitutionMap.put(realChild, alias);
                        } else {
                            innerChildren.add(nonVirtualSlotMap.get(realChild));
                        }
                    }
                }
                VirtualSlotReference newVirtual =
                        new VirtualSlotReference(((VirtualSlotReference) child).getExprId(),
                                ((VirtualSlotReference) child).getName(),
                                child.getDataType(), child.nullable(),
                                ((VirtualSlotReference) child).getQualifier(),
                                innerChildren, ((VirtualSlotReference) child).hasCast());
                substitutionMap.put(child, newVirtual);
                outerChildren.add(newVirtual);
            }
            GroupingScalarFunction newFunction =
                    (GroupingScalarFunction) groupingSetsFunction.withChildren(outerChildren);
            substitutionMap.put(groupingSetsFunction, newFunction);
        }
        return ImmutableMap.copyOf(substitutionMap);
    }

    /**
     * build AggOutputs order by outputExpressions.
     * Use oldOutputsToNewOutputs to represent the mapping from the original output to the new output.
     */
    private List<NamedExpression> buildAggOutputs(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputExpressions,
            Map<Expression, Expression> substitutionMap,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingScalarFunctions) {
        Map<Expression, NamedExpression> oldOutputsToNewOutputs =
                new ImmutableMap.Builder<Expression, NamedExpression>()
                // 1. Extract slotReference and additionally generated dummy columns in GroupByExpressions
                .putAll(buildAggOutputsByGroupByExpressions(
                        groupByExpressions, outputExpressions, substitutionMap))
                // 2. Generate new Outputs in GroupingFunction.
                .putAll(buildAggOutputsByGroupingFunc(
                        groupingScalarFunctions, substitutionMap))
                // 3. Generate new Outputs in AggFunction.
                .putAll(buildAggOutputsByAggFunc(aggregateFunctions, substitutionMap))
                .build();
        List<NamedExpression> newOutputs = new ArrayList<>();
        for (NamedExpression expression : outputExpressions) {
            if (expression instanceof Alias) {
                if (expression.anyMatch(AggregateFunction.class::isInstance)
                        || expression.anyMatch(GroupingScalarFunction.class::isInstance)) {
                    newOutputs.add(oldOutputsToNewOutputs.get(((Alias) expression).child()));
                } else {
                    Expression oldOutput = ((Alias) expression).child();
                    if (oldOutputsToNewOutputs.containsKey(oldOutput)) {
                        newOutputs.add(oldOutputsToNewOutputs.get(oldOutput).toSlot());
                    } else {
                        newOutputs.addAll(((ImmutableSet) oldOutput.collect(Slot.class::isInstance)).asList());
                    }
                }
            }
            if (expression instanceof SlotReference) {
                newOutputs.add(oldOutputsToNewOutputs.get(expression));
            }
        }
        return ImmutableList.copyOf(newOutputs);
    }

    private Map<Expression, NamedExpression> buildAggOutputsByGroupByExpressions(
            List<Expression> groupByExpressions,
            List<NamedExpression> outputs,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        groupByExpressions.stream().filter(k -> !(k instanceof VirtualSlotReference))
                .filter(e -> isInOutputExpressions(outputs, e))
                .forEach(e -> newOutputs.put(e,
                        (NamedExpression) substitutionMap.get(e)));
        return newOutputs;
    }

    private Map<Expression, NamedExpression> buildAggOutputsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            List<Expression> newChildren = Lists.newArrayList();
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    newChildren.add(child);
                } else {
                    newChildren.add(((Alias) substitutionMap.get(child)).toSlot());
                }
            }
            AggregateFunction newFunction = aggregateFunction.withChildren(newChildren);
            newOutputs.put(aggregateFunction, (Alias) substitutionMap.get(newFunction));
        }
        return newOutputs;
    }

    private Map<Expression, NamedExpression> buildAggOutputsByGroupingFunc(
            Set<GroupingScalarFunction> groupingScalarFunctions,
            Map<Expression, Expression> substitutionMap) {
        Map<Expression, NamedExpression> newOutputs = new HashMap<>();
        for (GroupingScalarFunction groupingScalarFunction : groupingScalarFunctions) {
            List<Expression> outerChildren = Lists.newArrayList();
            for (Expression child : groupingScalarFunction.getArguments()) {
                outerChildren.add(substitutionMap.get(child));
            }
            newOutputs.put(groupingScalarFunction, (VirtualSlotReference) outerChildren.get(0));
        }
        return newOutputs;
    }

    private Set<NamedExpression> buildRepeatOutputs(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap,
            Set<AggregateFunction> aggregateFunctions,
            Set<GroupingScalarFunction> groupingScalarFunctions) {
        return new ImmutableSet.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(buildRepeatOutputsByGroupByExpressions(groupByExpressions, substitutionMap))
                // 2. generate in AggregateFunction
                .addAll(buildRepeatOutputsByAggFunc(aggregateFunctions, substitutionMap))
                // 3. generate in GroupingFunc
                .addAll(buildRepeatOutputsByGroupingFunc(groupingScalarFunctions, substitutionMap))
                .build();
    }

    private List<NamedExpression> buildRepeatOutputsByGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof SlotReference) {
                // skip VirtualSlotReference generate with GroupingFunc
                if (isGroupingFuncVirtualSlot(groupByExpression)) {
                    continue;
                }
                repeatOutputs.add((NamedExpression) substitutionMap.get(groupByExpression));
            } else {
                repeatOutputs.add(getAlias(groupByExpression, substitutionMap).toSlot());
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private NamedExpression getAlias(Expression expression, Map<Expression, Expression> substitutionMap) {
        return (NamedExpression) substitutionMap.get(substitutionMap.get(expression));
    }

    private boolean isGroupingFuncVirtualSlot(Expression groupByExpression) {
        return groupByExpression instanceof VirtualSlotReference
                && !((VirtualSlotReference) groupByExpression).getRealSlots().isEmpty();
    }

    private boolean isInOutputExpressions(
            List<NamedExpression> outputExpressions,
            Expression expression) {
        return outputExpressions.stream().anyMatch(e -> e.anyMatch(expression::equals));
    }

    private List<NamedExpression> buildRepeatOutputsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        repeatOutputs.add((SlotReference) child);
                    }
                } else {
                    repeatOutputs.add(((Alias) substitutionMap.get(child)).toSlot());
                }
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private List<NamedExpression> buildRepeatOutputsByGroupingFunc(
            Set<GroupingScalarFunction> groupingScalarFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> repeatOutputs = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (GroupingScalarFunction groupingScalarFunction : groupingScalarFunctions) {
            for (Expression child : groupingScalarFunction.getArguments()) {
                // add virtualSlot
                repeatOutputs.add((VirtualSlotReference) substitutionMap.get(child));
            }
        }
        return ImmutableList.copyOf(repeatOutputs);
    }

    private List<NamedExpression> buildBottomProjections(
            List<Expression> groupByExpressions,
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        return new ImmutableList.Builder<NamedExpression>()
                // 1. generate in groupByExpressions
                .addAll(buildBottomProjectionsByGroupByExpressions(groupByExpressions, substitutionMap))
                // 2. generate in AggregateFunction
                .addAll(buildBottomProjectionsByAggFunc(aggregateFunctions, substitutionMap))
                .build();
    }

    List<NamedExpression> buildBottomProjectionsByAggFunc(
            Set<AggregateFunction> aggregateFunctions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> bottomProjections = new ArrayList<>();

        // replace all non-slot expression in agg functions children.
        for (AggregateFunction aggregateFunction : aggregateFunctions) {
            for (Expression child : aggregateFunction.getArguments()) {
                if (child instanceof SlotReference || child instanceof Literal) {
                    if (child instanceof SlotReference) {
                        bottomProjections.add((SlotReference) child);
                    }
                } else {
                    bottomProjections.add((Alias) substitutionMap.get(child));
                }
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    /**
     * generate bottom projections with groupByExpressions.
     * eg:
     * groupByExpressions: k1#0, k2#1 + 1;
     * bottom: k1#0, (k2#1 + 1) AS (k2 + 1)#2;
     */
    private List<NamedExpression> buildBottomProjectionsByGroupByExpressions(
            List<Expression> groupByExpressions,
            Map<Expression, Expression> substitutionMap) {
        List<NamedExpression> bottomProjections = new ArrayList<>();
        for (Expression groupByExpression : groupByExpressions) {
            if (groupByExpression instanceof VirtualSlotReference) {
                continue;
            } else if (groupByExpression instanceof SlotReference) {
                bottomProjections.add((NamedExpression) groupByExpression);
            } else {
                bottomProjections.add(getAlias(groupByExpression, substitutionMap));
            }
        }
        return ImmutableList.copyOf(bottomProjections);
    }

    /**
     * Convert the alias corresponding to agg to the corresponding slot.
     */
    private Map<Expression, Expression> updateSubstitutionMap(
            Map<Expression, Expression> oldSubstitution,
            Set<AggregateFunction> aggregateFunctions) {
        Map<Expression, Expression> newSubstitution = new HashMap<>();
        oldSubstitution.entrySet().stream()
                .forEach(e -> {
                    if (aggregateFunctions.contains(e.getKey())) {
                        newSubstitution.put(e.getKey(), ((NamedExpression) e.getValue()).toSlot());
                    } else {
                        newSubstitution.put(e.getKey(), e.getValue());
                    }
                });
        return ImmutableMap.copyOf(newSubstitution);
    }

    /**
     * Rearrange the order of the projects to ensure that
     * slotReference is in the front and virtualSlotReference is in the back.
     */
    private List<NamedExpression> reorderProjections(List<NamedExpression> projections) {
        Map<Boolean, List<NamedExpression>> partitionProjections = projections.stream()
                .collect(Collectors.groupingBy(VirtualSlotReference.class::isInstance,
                        LinkedHashMap::new, Collectors.toList()));
        List<NamedExpression> newProjections = partitionProjections.containsKey(false)
                ? partitionProjections.get(false) : new ArrayList<NamedExpression>();
        if (partitionProjections.containsKey(true)) {
            newProjections.addAll(partitionProjections.get(true));
        }
        return newProjections;
    }
}
