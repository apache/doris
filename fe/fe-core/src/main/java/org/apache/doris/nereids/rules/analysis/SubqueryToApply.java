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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.BinaryOperator;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * SubqueryToApply. translate from subquery to LogicalApply.
 * In two steps
 * The first step is to replace the predicate corresponding to the filter where the subquery is located.
 * The second step converts the subquery into an apply node.
 */
public class SubqueryToApply implements AnalysisRuleFactory {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            RuleType.FILTER_SUBQUERY_TO_APPLY.build(
                logicalFilter().thenApply(ctx -> {
                    LogicalFilter<Plan> filter = ctx.root;

                    ImmutableList<Set<SubqueryExpr>> subqueryExprsList = filter.getConjuncts().stream()
                            .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                            .collect(ImmutableList.toImmutableList());
                    if (subqueryExprsList.stream()
                            .flatMap(Collection::stream).noneMatch(SubqueryExpr.class::isInstance)) {
                        return filter;
                    }

                    List<Expression> oldConjuncts = ImmutableList.copyOf(filter.getConjuncts());
                    ImmutableList.Builder<Expression> newConjuncts = new ImmutableList.Builder<>();
                    LogicalPlan applyPlan = null;
                    LogicalPlan tmpPlan = (LogicalPlan) filter.child();

                    // Subquery traversal with the conjunct of and as the granularity.
                    for (int i = 0; i < subqueryExprsList.size(); ++i) {
                        Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                        if (subqueryExprs.isEmpty()) {
                            newConjuncts.add(oldConjuncts.get(i));
                            continue;
                        }

                        // first step: Replace the subquery of predicate in LogicalFilter
                        // second step: Replace subquery with LogicalApply
                        ReplaceSubquery replaceSubquery = new ReplaceSubquery(
                                ctx.statementContext, false);
                        SubqueryContext context = new SubqueryContext(subqueryExprs);
                        Expression conjunct = replaceSubquery.replace(oldConjuncts.get(i), context);

                        applyPlan = subqueryToApply(subqueryExprs.stream()
                                    .collect(ImmutableList.toImmutableList()), tmpPlan,
                                context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext,
                                Optional.of(conjunct), false);
                        tmpPlan = applyPlan;
                        newConjuncts.add(conjunct);
                    }
                    Set<Expression> conjuncts = ImmutableSet.copyOf(newConjuncts.build());
                    Plan newFilter = new LogicalFilter<>(conjuncts, applyPlan);
                    if (conjuncts.stream().flatMap(c -> c.children().stream())
                            .anyMatch(MarkJoinSlotReference.class::isInstance)) {
                        return new LogicalProject<>(applyPlan.getOutput().stream()
                                .filter(s -> !(s instanceof MarkJoinSlotReference))
                                .collect(ImmutableList.toImmutableList()), newFilter);
                    }
                    return new LogicalFilter<>(conjuncts, applyPlan);
                })
            ),
            RuleType.PROJECT_SUBQUERY_TO_APPLY.build(logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;
                ImmutableList<Set<SubqueryExpr>> subqueryExprsList = project.getProjects().stream()
                        .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                        .collect(ImmutableList.toImmutableList());
                if (subqueryExprsList.stream().flatMap(Collection::stream).count() == 0) {
                    return project;
                }
                List<NamedExpression> oldProjects = ImmutableList.copyOf(project.getProjects());
                ImmutableList.Builder<NamedExpression> newProjects = new ImmutableList.Builder<>();
                LogicalPlan childPlan = (LogicalPlan) project.child();
                LogicalPlan applyPlan;
                for (int i = 0; i < subqueryExprsList.size(); ++i) {
                    Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                    if (subqueryExprs.isEmpty()) {
                        newProjects.add(oldProjects.get(i));
                        continue;
                    }

                    // first step: Replace the subquery in logcialProject's project list
                    // second step: Replace subquery with LogicalApply
                    ReplaceSubquery replaceSubquery =
                            new ReplaceSubquery(ctx.statementContext, true);
                    SubqueryContext context = new SubqueryContext(subqueryExprs);
                    Expression newProject =
                            replaceSubquery.replace(oldProjects.get(i), context);

                    applyPlan = subqueryToApply(
                            subqueryExprs.stream().collect(ImmutableList.toImmutableList()),
                            childPlan, context.getSubqueryToMarkJoinSlot(),
                            ctx.cascadesContext,
                            Optional.of(newProject), true);
                    childPlan = applyPlan;
                    newProjects.add((NamedExpression) newProject);
                }

                return project.withProjectsAndChild(newProjects.build(), childPlan);
            })),
            RuleType.ONE_ROW_RELATION_SUBQUERY_TO_APPLY.build(logicalOneRowRelation()
                .when(ctx -> ctx.getProjects().stream()
                        .anyMatch(project -> project.containsType(SubqueryExpr.class)))
                .thenApply(ctx -> {
                    LogicalOneRowRelation oneRowRelation = ctx.root;
                    // create a LogicalProject node with the same project lists above LogicalOneRowRelation
                    // create a LogicalOneRowRelation with a dummy output column
                    // so PROJECT_SUBQUERY_TO_APPLY rule can handle the subquery unnest thing
                    return new LogicalProject<Plan>(oneRowRelation.getProjects(),
                            oneRowRelation.withProjects(
                                    ImmutableList.of(new Alias(BooleanLiteral.of(true),
                                            ctx.statementContext.generateColumnName()))));
                })),
            RuleType.JOIN_SUBQUERY_TO_APPLY
                .build(logicalJoin()
                .when(join -> join.getHashJoinConjuncts().isEmpty() && !join.getOtherJoinConjuncts().isEmpty())
                .thenApply(ctx -> {
                    LogicalJoin<Plan, Plan> join = ctx.root;
                    Map<Boolean, List<Expression>> joinConjuncts = join.getOtherJoinConjuncts().stream()
                            .collect(Collectors.groupingBy(conjunct -> conjunct.containsType(SubqueryExpr.class),
                                    Collectors.toList()));
                    List<Expression> subqueryConjuncts = joinConjuncts.get(true);
                    if (subqueryConjuncts == null || subqueryConjuncts.stream()
                            .anyMatch(expr -> !isValidSubqueryConjunct(expr, join.left()))) {
                        return join;
                    }

                    ImmutableList<Set<SubqueryExpr>> subqueryExprsList = subqueryConjuncts.stream()
                            .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                            .collect(ImmutableList.toImmutableList());
                    ImmutableList.Builder<Expression> newConjuncts = new ImmutableList.Builder<>();
                    LogicalPlan applyPlan = null;
                    LogicalPlan leftChildPlan = (LogicalPlan) join.left();

                    // Subquery traversal with the conjunct of and as the granularity.
                    for (int i = 0; i < subqueryExprsList.size(); ++i) {
                        Set<SubqueryExpr> subqueryExprs = subqueryExprsList.get(i);
                        if (subqueryExprs.size() > 1) {
                            // only support the conjunct contains one subquery expr
                            return join;
                        }

                        // first step: Replace the subquery of predicate in LogicalFilter
                        // second step: Replace subquery with LogicalApply
                        ReplaceSubquery replaceSubquery = new ReplaceSubquery(ctx.statementContext, true);
                        SubqueryContext context = new SubqueryContext(subqueryExprs);
                        Expression conjunct = replaceSubquery.replace(subqueryConjuncts.get(i), context);

                        applyPlan = subqueryToApply(
                                subqueryExprs.stream().collect(ImmutableList.toImmutableList()),
                                leftChildPlan, context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext, Optional.of(conjunct), false);
                        leftChildPlan = applyPlan;
                        newConjuncts.add(conjunct);
                    }
                    List<Expression> simpleConjuncts = joinConjuncts.get(false);
                    if (simpleConjuncts != null) {
                        newConjuncts.addAll(simpleConjuncts);
                    }
                    Plan newJoin = join.withConjunctsChildren(join.getHashJoinConjuncts(),
                            newConjuncts.build(), applyPlan, join.right());
                    return newJoin;
                }))
        );
    }

    private static boolean isValidSubqueryConjunct(Expression expression, Plan leftChild) {
        // the subquery must be uncorrelated subquery or only correlated to the left child
        // currently only support the following 4 simple scenarios
        // 1. col ComparisonPredicate subquery
        // 2. col in (subquery)
        // 3. exists (subquery)
        // 4. col1 ComparisonPredicate subquery or xxx (no more subquery)
        List<Slot> slots = leftChild.getOutput();
        if (expression instanceof ComparisonPredicate && expression.child(1) instanceof ScalarSubquery) {
            ScalarSubquery subquery = (ScalarSubquery) expression.child(1);
            return slots.containsAll(subquery.getCorrelateSlots());
        } else if (expression instanceof InSubquery) {
            return slots.containsAll(((InSubquery) expression).getCorrelateSlots());
        } else if (expression instanceof Exists) {
            return slots.containsAll(((Exists) expression).getCorrelateSlots());
        } else {
            List<SubqueryExpr> subqueryExprs = expression.collectToList(SubqueryExpr.class::isInstance);
            if (subqueryExprs.size() == 1) {
                return slots.containsAll(subqueryExprs.get(0).getCorrelateSlots());
            }
        }
        return false;
    }

    private LogicalPlan subqueryToApply(List<SubqueryExpr> subqueryExprs, LogicalPlan childPlan,
                                        Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
                                        CascadesContext ctx,
                                        Optional<Expression> conjunct, boolean isProject) {
        LogicalPlan tmpPlan = childPlan;
        for (int i = 0; i < subqueryExprs.size(); ++i) {
            SubqueryExpr subqueryExpr = subqueryExprs.get(i);
            if (nonMarkJoinExistsWithAgg(subqueryExpr, subqueryToMarkJoinSlot)) {
                continue;
            }

            if (!ctx.subqueryIsAnalyzed(subqueryExpr)) {
                tmpPlan = addApply(subqueryExpr, tmpPlan,
                    subqueryToMarkJoinSlot, ctx, conjunct,
                    isProject, subqueryExprs.size() == 1);
            }
        }
        return tmpPlan;
    }

    private boolean nonMarkJoinExistsWithAgg(SubqueryExpr exists,
                 Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot) {
        return exists instanceof Exists
            && exists.getQueryPlan().anyMatch(Aggregate.class::isInstance)
            && !subqueryToMarkJoinSlot.get(exists).isPresent();
    }

    private LogicalPlan addApply(SubqueryExpr subquery, LogicalPlan childPlan,
                                 Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
                                 CascadesContext ctx, Optional<Expression> conjunct,
                                 boolean isProject, boolean singleSubquery) {
        ctx.setSubqueryExprIsAnalyzed(subquery, true);
        boolean needAddScalarSubqueryOutputToProjects = isConjunctContainsScalarSubqueryOutput(
                subquery, conjunct, isProject, singleSubquery);
        LogicalApply newApply = new LogicalApply(
                subquery.getCorrelateSlots(),
                subquery, Optional.empty(),
                subqueryToMarkJoinSlot.get(subquery),
                needAddScalarSubqueryOutputToProjects, isProject,
                childPlan, subquery.getQueryPlan());

        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                    // left child
                    .addAll(childPlan.getOutput())
                    // markJoinSlotReference
                    .addAll(subqueryToMarkJoinSlot.get(subquery).isPresent()
                        ? ImmutableList.of(subqueryToMarkJoinSlot.get(subquery).get()) : ImmutableList.of())
                    // scalarSubquery output
                    .addAll(needAddScalarSubqueryOutputToProjects
                        ? ImmutableList.of(subquery.getQueryPlan().getOutput().get(0)) : ImmutableList.of())
                    .build();

        return new LogicalProject(projects, newApply);
    }

    private boolean isConjunctContainsScalarSubqueryOutput(
            SubqueryExpr subqueryExpr, Optional<Expression> conjunct, boolean isProject, boolean singleSubquery) {
        return subqueryExpr instanceof ScalarSubquery
            && ((conjunct.isPresent() && ((ImmutableSet) conjunct.get().collect(SlotReference.class::isInstance))
                    .contains(subqueryExpr.getQueryPlan().getOutput().get(0)))
                || isProject);
    }

    /**
     * The Subquery in the LogicalFilter will change to LogicalApply, so we must replace the origin Subquery.
     * LogicalFilter(predicate(contain subquery)) -> LogicalFilter(predicate(not contain subquery)
     * Replace the subquery in logical with the relevant expression.
     *
     * The replacement rules are as follows:
     * before:
     *      1.filter(t1.a = scalarSubquery(output b));
     *      2.filter(inSubquery);   inSubquery = (t1.a in select ***);
     *      3.filter(exists);   exists = (select ***);
     *
     * after:
     *      1.filter(t1.a = b);
     *      2.isMarkJoin ? filter(MarkJoinSlotReference) : filter(True);
     *      3.isMarkJoin ? filter(MarkJoinSlotReference) : filter(True);
     */
    private static class ReplaceSubquery extends DefaultExpressionRewriter<SubqueryContext> {
        private final StatementContext statementContext;
        private boolean isMarkJoin;

        private final boolean shouldOutputMarkJoinSlot;

        public ReplaceSubquery(StatementContext statementContext,
                               boolean shouldOutputMarkJoinSlot) {
            this.statementContext = Objects.requireNonNull(statementContext, "statementContext can't be null");
            this.shouldOutputMarkJoinSlot = shouldOutputMarkJoinSlot;
        }

        public Expression replace(Expression expression, SubqueryContext subqueryContext) {
            return expression.accept(this, subqueryContext);
        }

        @Override
        public Expression visitExistsSubquery(Exists exists, SubqueryContext context) {
            // The result set when NULL is specified in the subquery and still evaluates to TRUE by using EXISTS
            // When the number of rows returned is empty, agg will return null, so if there is more agg,
            // it will always consider the returned result to be true
            boolean needCreateMarkJoinSlot = isMarkJoin || shouldOutputMarkJoinSlot;
            MarkJoinSlotReference markJoinSlotReference = null;
            if (exists.getQueryPlan().anyMatch(Aggregate.class::isInstance) && needCreateMarkJoinSlot) {
                markJoinSlotReference =
                        new MarkJoinSlotReference(statementContext.generateColumnName(), true);
            } else if (needCreateMarkJoinSlot) {
                markJoinSlotReference =
                        new MarkJoinSlotReference(statementContext.generateColumnName());
            }
            if (needCreateMarkJoinSlot) {
                context.setSubqueryToMarkJoinSlot(exists, Optional.of(markJoinSlotReference));
            }
            return needCreateMarkJoinSlot ? markJoinSlotReference : BooleanLiteral.TRUE;
        }

        @Override
        public Expression visitInSubquery(InSubquery in, SubqueryContext context) {
            MarkJoinSlotReference markJoinSlotReference =
                    new MarkJoinSlotReference(statementContext.generateColumnName());
            boolean needCreateMarkJoinSlot = isMarkJoin || shouldOutputMarkJoinSlot;
            if (needCreateMarkJoinSlot) {
                context.setSubqueryToMarkJoinSlot(in, Optional.of(markJoinSlotReference));
            }
            return needCreateMarkJoinSlot ? markJoinSlotReference : BooleanLiteral.TRUE;
        }

        @Override
        public Expression visitScalarSubquery(ScalarSubquery scalar, SubqueryContext context) {
            return scalar.getSubqueryOutput();
        }

        @Override
        public Expression visitBinaryOperator(BinaryOperator binaryOperator, SubqueryContext context) {
            // update isMarkJoin flag
            isMarkJoin =
                    isMarkJoin || ((binaryOperator.left().anyMatch(SubqueryExpr.class::isInstance)
                            || binaryOperator.right().anyMatch(SubqueryExpr.class::isInstance))
                            && (binaryOperator instanceof Or));

            Expression left = replace(binaryOperator.left(), context);
            Expression right = replace(binaryOperator.right(), context);

            return binaryOperator.withChildren(left, right);
        }
    }

    /**
     * subqueryToMarkJoinSlot: The markJoinSlot corresponding to each subquery.
     * rule:
     * For inSubquery and exists: it will be directly replaced by markSlotReference
     *  e.g.
     *  logicalFilter(predicate=exists) ---> logicalFilter(predicate=$c$1)
     * For scalarSubquery: it will be replaced by scalarSubquery's output slot
     *  e.g.
     *  logicalFilter(predicate=k1 > scalarSubquery) ---> logicalFilter(predicate=k1 > $c$1)
     *
     * subqueryCorrespondingConjunct: Record the conject corresponding to the subquery.
     * rule:
     *
     *
     */
    private static class SubqueryContext {
        private final Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot;

        public SubqueryContext(Set<SubqueryExpr> subqueryExprs) {
            this.subqueryToMarkJoinSlot = new LinkedHashMap<>(subqueryExprs.size());
            subqueryExprs.forEach(subqueryExpr -> subqueryToMarkJoinSlot.put(subqueryExpr, Optional.empty()));
        }

        private Map<SubqueryExpr, Optional<MarkJoinSlotReference>> getSubqueryToMarkJoinSlot() {
            return subqueryToMarkJoinSlot;
        }

        private void setSubqueryToMarkJoinSlot(SubqueryExpr subquery,
                                              Optional<MarkJoinSlotReference> markJoinSlotReference) {
            subqueryToMarkJoinSlot.put(subquery, markJoinSlotReference);
        }

    }

}
