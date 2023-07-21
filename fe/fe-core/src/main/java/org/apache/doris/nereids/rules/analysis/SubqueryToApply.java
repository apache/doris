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
import org.apache.doris.nereids.trees.expressions.CaseWhen;
import org.apache.doris.nereids.trees.expressions.ComparisonPredicate;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Not;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

                    ImmutableList<Set> subqueryExprsList = filter.getConjuncts().stream()
                            .map(e -> (Set) e.collect(SubqueryExpr.class::isInstance))
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
                                context.getSubqueryCorrespondingConjunct(), ctx.cascadesContext,
                                Optional.of(conjunct), false);
                        tmpPlan = applyPlan;
                        if (!(subqueryExprs.size() == 1
                                && subqueryExprs.stream().anyMatch(ScalarSubquery.class::isInstance))) {
                            newConjuncts.add(conjunct);
                        }
                    }
                    Set<Expression> conjects = new LinkedHashSet<>();
                    conjects.addAll(newConjuncts.build());
                    Plan newFilter = new LogicalFilter<>(conjects, applyPlan);
                    if (conjects.stream().flatMap(c -> c.children().stream())
                            .anyMatch(MarkJoinSlotReference.class::isInstance)) {
                        return new LogicalProject<>(applyPlan.getOutput().stream()
                                .filter(s -> !(s instanceof MarkJoinSlotReference))
                                .collect(ImmutableList.toImmutableList()), newFilter);
                    }
                    return new LogicalFilter<>(conjects, applyPlan);
                })
            ),
            RuleType.PROJECT_SUBQUERY_TO_APPLY.build(
               logicalProject().thenApply(ctx -> {
                   LogicalProject<Plan> project = ctx.root;
                   Set<SubqueryExpr> subqueryExprs = new LinkedHashSet<>();
                   project.getProjects().stream()
                           .filter(Alias.class::isInstance)
                           .map(Alias.class::cast)
                           .filter(alias -> alias.child() instanceof CaseWhen)
                           .forEach(alias -> alias.child().children().stream()
                                   .forEach(e ->
                                       subqueryExprs.addAll(e.collect(SubqueryExpr.class::isInstance))));
                   if (subqueryExprs.isEmpty()) {
                       return project;
                   }

                   SubqueryContext context = new SubqueryContext(subqueryExprs);
                   return new LogicalProject(project.getProjects().stream()
                           .map(p -> p.withChildren(
                               new ReplaceSubquery(ctx.statementContext, true)
                                   .replace(p, context)))
                           .collect(ImmutableList.toImmutableList()),
                           subqueryToApply(
                               subqueryExprs.stream().collect(ImmutableList.toImmutableList()),
                               (LogicalPlan) project.child(),
                               context.getSubqueryToMarkJoinSlot(), context.getSubqueryCorrespondingConjunct(),
                               ctx.cascadesContext,
                               Optional.empty(), true
                           ));
               })
            )
        );
    }

    private LogicalPlan subqueryToApply(List<SubqueryExpr> subqueryExprs, LogicalPlan childPlan,
                                        Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
                                        Map<SubqueryExpr, Expression> subqueryCorrespondingConject,
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
                    subqueryToMarkJoinSlot, subqueryCorrespondingConject, ctx, conjunct,
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
                                 Map<SubqueryExpr, Expression> subqueryCorrespondingConject,
                                 CascadesContext ctx, Optional<Expression> conjunct,
                                 boolean isProject, boolean singleSubquery) {
        ctx.setSubqueryExprIsAnalyzed(subquery, true);
        boolean needAddSubOutputToProjects = isScalarAndFilterContainsSubqueryOutput(
                subquery, conjunct, isProject, singleSubquery);
        LogicalApply newApply = new LogicalApply(
                subquery.getCorrelateSlots(),
                subquery, Optional.empty(),
                subqueryToMarkJoinSlot.get(subquery),
                mergeScalarSubConjectAndFilterConject(
                    subquery, subqueryCorrespondingConject,
                    conjunct, needAddSubOutputToProjects, singleSubquery), isProject,
                childPlan, subquery.getQueryPlan());

        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                    // left child
                    .addAll(childPlan.getOutput())
                    // markJoinSlotReference
                    .addAll(subqueryToMarkJoinSlot.get(subquery).isPresent()
                        ? ImmutableList.of(subqueryToMarkJoinSlot.get(subquery).get()) : ImmutableList.of())
                    // scalarSubquery output
                    .addAll(needAddSubOutputToProjects
                        ? ImmutableList.of(subquery.getQueryPlan().getOutput().get(0)) : ImmutableList.of())
                    .build();

        return new LogicalProject(projects, newApply);
    }

    private boolean checkSingleScalarWithOr(SubqueryExpr subquery,
                                            Optional<Expression> conjunct) {
        return subquery instanceof ScalarSubquery
                && conjunct.isPresent() && conjunct.get() instanceof Or
                && subquery.getCorrelateSlots().isEmpty();
    }

    private boolean isScalarAndFilterContainsSubqueryOutput(
            SubqueryExpr subqueryExpr, Optional<Expression> conjunct, boolean isProject, boolean singleSubquery) {
        return subqueryExpr instanceof ScalarSubquery
            && ((!singleSubquery && conjunct.isPresent()
                && ((ImmutableSet) conjunct.get().collect(SlotReference.class::isInstance))
                    .contains(subqueryExpr.getQueryPlan().getOutput().get(0)))
                || isProject);
    }

    /**
     * For a single scalarSubQuery, when there is a disjunction,
     * directly use all connection conditions as the join conjunct of scalarSubQuery.
     * e.g.
     * select * from t1 where k1 > scalarSub(sum(c1)) or k2 > 10;
     *  LogicalJoin(otherConjunct[k1 > sum(c1) or k2 > 10])
     *
     * For other scalarSubQuery, you only need to use the connection as the join conjunct.
     * e.g.
     * select * from t1 where k1 > scalarSub(sum(c1)) or k2 in inSub(c2) or k2 > 10;
     *  LogicalFilter($c$1 or $c$2 or k2 > 10)
     *      LogicalJoin(otherConjunct[k2 = c2])  ---> inSub
     *          LogicalJoin(otherConjunct[k1 > sum(c1)])  ---> scalarSub
     */
    private Optional<Expression> mergeScalarSubConjectAndFilterConject(
                    SubqueryExpr subquery,
                    Map<SubqueryExpr, Expression> subqueryCorrespondingConject,
                    Optional<Expression> conjunct,
                    boolean isProject,
                    boolean singleSubquery) {
        if (singleSubquery && checkSingleScalarWithOr(subquery, conjunct)) {
            return conjunct;
        } else if (subqueryCorrespondingConject.containsKey(subquery) && !isProject) {
            return Optional.of(subqueryCorrespondingConject.get(subquery));
        }
        return Optional.empty();
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

        private final boolean isProject;

        public ReplaceSubquery(StatementContext statementContext,
                               boolean isProject) {
            this.statementContext = Objects.requireNonNull(statementContext, "statementContext can't be null");
            this.isProject = isProject;
        }

        public Set<Expression> replace(Set<Expression> expressions, SubqueryContext subqueryContext) {
            return expressions.stream().map(expr -> expr.accept(this, subqueryContext))
                    .collect(ImmutableSet.toImmutableSet());
        }

        public Expression replace(Expression expressions, SubqueryContext subqueryContext) {
            return expressions.accept(this, subqueryContext);
        }

        @Override
        public Expression visitExistsSubquery(Exists exists, SubqueryContext context) {
            // The result set when NULL is specified in the subquery and still evaluates to TRUE by using EXISTS
            // When the number of rows returned is empty, agg will return null, so if there is more agg,
            // it will always consider the returned result to be true
            MarkJoinSlotReference markJoinSlotReference = null;
            if (exists.getQueryPlan().anyMatch(Aggregate.class::isInstance) && isMarkJoin) {
                markJoinSlotReference =
                        new MarkJoinSlotReference(statementContext.generateColumnName(), true);
            } else if (isMarkJoin) {
                markJoinSlotReference =
                        new MarkJoinSlotReference(statementContext.generateColumnName());
            }
            if (isMarkJoin) {
                context.setSubqueryToMarkJoinSlot(exists, Optional.of(markJoinSlotReference));
            }
            return isMarkJoin ? markJoinSlotReference : BooleanLiteral.TRUE;
        }

        @Override
        public Expression visitInSubquery(InSubquery in, SubqueryContext context) {
            MarkJoinSlotReference markJoinSlotReference =
                    new MarkJoinSlotReference(statementContext.generateColumnName());
            if (isMarkJoin) {
                context.setSubqueryToMarkJoinSlot(in, Optional.of(markJoinSlotReference));
            }
            return isMarkJoin ? markJoinSlotReference : BooleanLiteral.TRUE;
        }

        @Override
        public Expression visitScalarSubquery(ScalarSubquery scalar, SubqueryContext context) {
            context.setSubqueryCorrespondingConject(scalar, scalar.getSubqueryOutput());
            // When there is only one scalarSubQuery and CorrelateSlots is empty
            // it will not be processed by MarkJoin, so it can be returned directly
            if (context.onlySingleSubquery() && scalar.getCorrelateSlots().isEmpty()) {
                return scalar.getSubqueryOutput();
            }

            MarkJoinSlotReference markJoinSlotReference =
                    new MarkJoinSlotReference(statementContext.generateColumnName());
            if (isMarkJoin) {
                context.setSubqueryToMarkJoinSlot(scalar, Optional.of(markJoinSlotReference));
            }
            return isMarkJoin ? markJoinSlotReference : scalar.getSubqueryOutput();
        }

        @Override
        public Expression visitNot(Not not, SubqueryContext context) {
            // Need to re-update scalarSubQuery unequal conditions into subqueryCorrespondingConject
            if (not.child() instanceof BinaryOperator
                    && (((BinaryOperator) not.child()).left().containsType(ScalarSubquery.class)
                    || ((BinaryOperator) not.child()).right().containsType(ScalarSubquery.class))) {
                Expression newChild = replace(not.child(), context);
                ScalarSubquery subquery = collectScalarSubqueryForBinaryOperator((BinaryOperator) not.child());
                context.updateSubqueryCorrespondingConjunctInNot(subquery);
                return context.getSubqueryToMarkJoinSlotValue(subquery).isPresent() ? newChild : new Not(newChild);
            }

            return visit(not, context);
        }

        @Override
        public Expression visitIsNull(IsNull isNull, SubqueryContext context) {
            // Need to re-update scalarSubQuery unequal conditions into subqueryCorrespondingConject
            if (isNull.child() instanceof BinaryOperator
                    && (((BinaryOperator) isNull.child()).left().containsType(ScalarSubquery.class)
                    || ((BinaryOperator) isNull.child()).right().containsType(ScalarSubquery.class))) {
                Expression newChild = replace(isNull.child(), context);
                ScalarSubquery subquery = collectScalarSubqueryForBinaryOperator((BinaryOperator) isNull.child());
                context.updateSubqueryCorrespondingConjunctIsNull(subquery);
                return context.getSubqueryToMarkJoinSlotValue(subquery).isPresent() ? newChild : new IsNull(newChild);
            }

            return visit(isNull, context);
        }

        @Override
        public Expression visitBinaryOperator(BinaryOperator binaryOperator, SubqueryContext context) {
            boolean atLeastOneChildContainsScalarSubquery =
                    binaryOperator.left().containsType(ScalarSubquery.class)
                        || binaryOperator.right().containsType(ScalarSubquery.class);
            boolean currentMarkJoin = ((binaryOperator.left().anyMatch(SubqueryExpr.class::isInstance)
                                        || binaryOperator.right().anyMatch(SubqueryExpr.class::isInstance))
                                      && (binaryOperator instanceof Or)) || isMarkJoin;
            isMarkJoin = currentMarkJoin;
            Expression left = replace(binaryOperator.left(), context);
            isMarkJoin = currentMarkJoin;
            Expression right = replace(binaryOperator.right(), context);

            if (atLeastOneChildContainsScalarSubquery && !(binaryOperator instanceof CompoundPredicate)) {
                return context.replaceBinaryOperator(binaryOperator, left, right, isProject);
            }

            return binaryOperator.withChildren(left, right);
        }
    }

    /**
     * subqueryToMarkJoinSlot: The markJoinSlot corresponding to each subquery.
     * rule:
     * For inSubquery and exists: it will be directly replaced by markSlotReference
     *  e.g.
     *  logicalFilter(predicate=exists) ---> logicalFilter(predicate=$c$1)
     * For scalarSubquery: will replace the connected ComparisonPredicate with markSlotReference
     *  e.g.
     *  logicalFilter(predicate=k1 > scalarSubquery) ---> logicalFilter(predicate=$c$1)
     *
     * subqueryCorrespondingConject: Record the conject corresponding to the subquery.
     * rule:
     *
     *
     */
    private static class SubqueryContext {
        private final Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot;

        private final Map<SubqueryExpr, Expression> subqueryCorrespondingConjunct;

        public SubqueryContext(Set<SubqueryExpr> subqueryExprs) {
            this.subqueryToMarkJoinSlot = new LinkedHashMap<>(subqueryExprs.size());
            this.subqueryCorrespondingConjunct = new LinkedHashMap<>(subqueryExprs.size());
            subqueryExprs.forEach(subqueryExpr -> subqueryToMarkJoinSlot.put(subqueryExpr, Optional.empty()));
        }

        private Map<SubqueryExpr, Optional<MarkJoinSlotReference>> getSubqueryToMarkJoinSlot() {
            return subqueryToMarkJoinSlot;
        }

        private Map<SubqueryExpr, Expression> getSubqueryCorrespondingConjunct() {
            return subqueryCorrespondingConjunct;
        }

        private Optional<MarkJoinSlotReference> getSubqueryToMarkJoinSlotValue(SubqueryExpr subqueryExpr) {
            return subqueryToMarkJoinSlot.get(subqueryExpr);
        }

        private void setSubqueryToMarkJoinSlot(SubqueryExpr subquery,
                                              Optional<MarkJoinSlotReference> markJoinSlotReference) {
            subqueryToMarkJoinSlot.put(subquery, markJoinSlotReference);
        }

        private void setSubqueryCorrespondingConject(SubqueryExpr subquery,
                                                    Expression expression) {
            subqueryCorrespondingConjunct.put(subquery, expression);
        }

        private boolean onlySingleSubquery() {
            return subqueryToMarkJoinSlot.size() == 1;
        }

        private void updateSubqueryCorrespondingConjunctInNot(SubqueryExpr subquery) {
            if (subqueryCorrespondingConjunct.containsKey(subquery)) {
                subqueryCorrespondingConjunct.replace(subquery,
                    new Not(subqueryCorrespondingConjunct.get(subquery)));
            }
        }

        private void updateSubqueryCorrespondingConjunctIsNull(SubqueryExpr subquery) {
            if (subqueryCorrespondingConjunct.containsKey(subquery)) {
                subqueryCorrespondingConjunct.replace(subquery,
                        new IsNull(subqueryCorrespondingConjunct.get(subquery)));
            }
        }

        /**
         * For scalarSubQuery and MarkJoin, it will be replaced by markSlotReference
         *  e.g.
         *  logicalFilter(predicate=k1 > scalarSub or exists)
         *  -->
         *  logicalFilter(predicate=$c$1 or $c$2)
         */
        private Expression replaceBinaryOperator(BinaryOperator binaryOperator,
                                                Expression left,
                                                Expression right,
                                                boolean isProject) {
            boolean leftContaionsScalar = binaryOperator.left().containsType(ScalarSubquery.class);
            ScalarSubquery subquery = collectScalarSubqueryForBinaryOperator(binaryOperator);

            // record the result in subqueryCorrespondingConjunct
            Expression newLeft = leftContaionsScalar && subqueryToMarkJoinSlot.get(subquery).isPresent()
                    ? subqueryCorrespondingConjunct.get(subquery) : left;
            Expression newRight = !leftContaionsScalar && subqueryToMarkJoinSlot.get(subquery).isPresent()
                    ? subqueryCorrespondingConjunct.get(subquery) : right;
            Expression newBinary = binaryOperator.withChildren(newLeft, newRight);
            subqueryCorrespondingConjunct.put(subquery,
                    (isProject ? (leftContaionsScalar ? newLeft : newRight) : newBinary));

            if (subqueryToMarkJoinSlot.get(subquery).isPresent() && binaryOperator instanceof ComparisonPredicate) {
                return subqueryToMarkJoinSlot.get(subquery).get();
            }
            return newBinary;
        }
    }

    private static ScalarSubquery collectScalarSubqueryForBinaryOperator(BinaryOperator binaryOperator) {
        boolean leftContaionsScalar = binaryOperator.left().containsType(ScalarSubquery.class);
        return leftContaionsScalar
                ? (ScalarSubquery) ((ImmutableSet) binaryOperator.left()
                .collect(ScalarSubquery.class::isInstance)).asList().get(0)
                : (ScalarSubquery) ((ImmutableSet) binaryOperator.right()
                .collect(ScalarSubquery.class::isInstance)).asList().get(0);
    }
}
