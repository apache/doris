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

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.expression.ExpressionRewriteContext;
import org.apache.doris.nereids.rules.expression.rules.TrySimplifyPredicateWithMarkJoinSlot;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.CompoundPredicate;
import org.apache.doris.nereids.trees.expressions.Exists;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.InSubquery;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Or;
import org.apache.doris.nereids.trees.expressions.ScalarSubquery;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.agg.AnyValue;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.NotNullableAggregateFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.AssertTrue;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalApply;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.util.ExpressionUtils;
import org.apache.doris.nereids.util.Utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
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

                    Set<Expression> conjuncts = filter.getConjuncts();
                    CollectSubquerys collectSubquerys = collectSubquerys(conjuncts);
                    if (!collectSubquerys.hasSubquery) {
                        return filter;
                    }

                    List<Boolean> shouldOutputMarkJoinSlot = shouldOutputMarkJoinSlot(conjuncts);

                    List<Expression> oldConjuncts = Utils.fastToImmutableList(conjuncts);
                    ImmutableSet.Builder<Expression> newConjuncts = new ImmutableSet.Builder<>();
                    LogicalPlan applyPlan = null;
                    LogicalPlan tmpPlan = (LogicalPlan) filter.child();

                    List<Set<SubqueryExpr>> subqueryExprsList = collectSubquerys.subqueies;
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
                                ctx.statementContext, shouldOutputMarkJoinSlot.get(i));
                        SubqueryContext context = new SubqueryContext(subqueryExprs);
                        Expression conjunct = replaceSubquery.replace(oldConjuncts.get(i), context);
                        /*
                        * the idea is replacing each mark join slot with null and false literal
                        * then run FoldConstant rule, if the evaluate result are:
                        * 1. all true
                        * 2. all null and false (in logicalFilter, we discard both null and false values)
                        * the mark slot can be non-nullable boolean
                        * we pass this info to LogicalApply. And in InApplyToJoin rule
                        * if it's semi join with non-null mark slot
                        * we can safely change the mark conjunct to hash conjunct
                        */
                        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx.cascadesContext);
                        boolean isMarkSlotNotNull = conjunct.containsType(MarkJoinSlotReference.class)
                                        ? ExpressionUtils.canInferNotNullForMarkSlot(
                                                TrySimplifyPredicateWithMarkJoinSlot.INSTANCE.rewrite(conjunct,
                                                        rewriteContext), rewriteContext)
                                        : false;
                        Pair<LogicalPlan, Optional<Expression>> result = subqueryToApply(subqueryExprs.stream()
                                    .collect(ImmutableList.toImmutableList()), tmpPlan,
                                context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext,
                                Optional.of(conjunct), isMarkSlotNotNull);
                        applyPlan = result.first;
                        tmpPlan = applyPlan;
                        newConjuncts.add(result.second.isPresent() ? result.second.get() : conjunct);
                    }
                    Plan newFilter = new LogicalFilter<>(newConjuncts.build(), applyPlan);
                    return new LogicalProject<>(filter.getOutput().stream().collect(ImmutableList.toImmutableList()),
                        newFilter);
                })
            ),
            RuleType.PROJECT_SUBQUERY_TO_APPLY.build(logicalProject().thenApply(ctx -> {
                LogicalProject<Plan> project = ctx.root;

                List<NamedExpression> projects = project.getProjects();
                CollectSubquerys collectSubquerys = collectSubquerys(projects);
                if (!collectSubquerys.hasSubquery) {
                    return project;
                }

                List<Set<SubqueryExpr>> subqueryExprsList = collectSubquerys.subqueies;
                List<NamedExpression> oldProjects = ImmutableList.copyOf(projects);
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

                    Pair<LogicalPlan, Optional<Expression>> result =
                            subqueryToApply(Utils.fastToImmutableList(subqueryExprs), childPlan,
                                    context.getSubqueryToMarkJoinSlot(), ctx.cascadesContext,
                                    Optional.of(newProject), false);
                    applyPlan = result.first;
                    childPlan = applyPlan;
                    newProjects.add(
                            result.second.isPresent() ? (NamedExpression) result.second.get()
                                    : (NamedExpression) newProject);
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
                            .anyMatch(expr -> !isValidSubqueryConjunct(expr))) {
                        return join;
                    }

                    List<RelatedInfo> relatedInfoList = collectRelatedInfo(
                            subqueryConjuncts, join.left(), join.right());
                    if (relatedInfoList.stream().anyMatch(info -> info == RelatedInfo.UnSupported)) {
                        return join;
                    }

                    ImmutableList<Set<SubqueryExpr>> subqueryExprsList = subqueryConjuncts.stream()
                            .<Set<SubqueryExpr>>map(e -> e.collect(SubqueryExpr.class::isInstance))
                            .collect(ImmutableList.toImmutableList());
                    ImmutableList.Builder<Expression> newConjuncts = new ImmutableList.Builder<>();
                    LogicalPlan applyPlan;
                    LogicalPlan leftChildPlan = (LogicalPlan) join.left();
                    LogicalPlan rightChildPlan = (LogicalPlan) join.right();

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
                        /*
                        * the idea is replacing each mark join slot with null and false literal
                        * then run FoldConstant rule, if the evaluate result are:
                        * 1. all true
                        * 2. all null and false (in logicalFilter, we discard both null and false values)
                        * the mark slot can be non-nullable boolean
                        * we pass this info to LogicalApply. And in InApplyToJoin rule
                        * if it's semi join with non-null mark slot
                        * we can safely change the mark conjunct to hash conjunct
                        */
                        ExpressionRewriteContext rewriteContext = new ExpressionRewriteContext(ctx.cascadesContext);
                        boolean isMarkSlotNotNull = conjunct.containsType(MarkJoinSlotReference.class)
                                ? ExpressionUtils.canInferNotNullForMarkSlot(
                                    TrySimplifyPredicateWithMarkJoinSlot.INSTANCE.rewrite(conjunct, rewriteContext),
                                    rewriteContext)
                                : false;
                        Pair<LogicalPlan, Optional<Expression>> result = subqueryToApply(
                                subqueryExprs.stream().collect(ImmutableList.toImmutableList()),
                                relatedInfoList.get(i) == RelatedInfo.RelatedToLeft ? leftChildPlan : rightChildPlan,
                                context.getSubqueryToMarkJoinSlot(),
                                ctx.cascadesContext, Optional.of(conjunct), isMarkSlotNotNull);
                        applyPlan = result.first;
                        if (relatedInfoList.get(i) == RelatedInfo.RelatedToLeft) {
                            leftChildPlan = applyPlan;
                        } else {
                            rightChildPlan = applyPlan;
                        }
                        newConjuncts.add(result.second.isPresent() ? result.second.get() : conjunct);
                    }
                    List<Expression> simpleConjuncts = joinConjuncts.get(false);
                    if (simpleConjuncts != null) {
                        newConjuncts.addAll(simpleConjuncts);
                    }
                    Plan newJoin = join.withConjunctsChildren(join.getHashJoinConjuncts(),
                            newConjuncts.build(), leftChildPlan, rightChildPlan, null);
                    return newJoin;
                }))
        );
    }

    private static boolean isValidSubqueryConjunct(Expression expression) {
        // only support 1 subquery expr in the expression
        // don't support expression like subquery1 or subquery2
        return expression
                .collectToList(SubqueryExpr.class::isInstance)
                .size() == 1;
    }

    private enum RelatedInfo {
        // both subquery and its output don't related to any child. like (select sum(t.a) from t) > 1
        Unrelated,
        // either subquery or its output only related to left child. like bellow:
        // tableLeft.a in (select t.a from t)
        // 3 in (select t.b from t where t.a = tableLeft.a)
        // tableLeft.a > (select sum(t.a) from t where tableLeft.b = t.b)
        RelatedToLeft,
        // like above, but related to right child
        RelatedToRight,
        // subquery related to both left and child is not supported:
        // tableLeft.a > (select sum(t.a) from t where t.b = tableRight.b)
        UnSupported
    }

    private ImmutableList<RelatedInfo> collectRelatedInfo(List<Expression> subqueryConjuncts,
            Plan leftChild, Plan rightChild) {
        int size = subqueryConjuncts.size();
        ImmutableList.Builder<RelatedInfo> correlatedInfoList = new ImmutableList.Builder<>();
        Set<Slot> leftOutputSlots = leftChild.getOutputSet();
        Set<Slot> rightOutputSlots = rightChild.getOutputSet();
        for (int i = 0; i < size; ++i) {
            Expression expression = subqueryConjuncts.get(i);
            List<SubqueryExpr> subqueryExprs = expression.collectToList(SubqueryExpr.class::isInstance);
            RelatedInfo relatedInfo = RelatedInfo.UnSupported;
            if (subqueryExprs.size() == 1) {
                SubqueryExpr subqueryExpr = subqueryExprs.get(0);
                List<Slot> correlatedSlots = subqueryExpr.getCorrelateSlots();
                if (subqueryExpr instanceof ScalarSubquery) {
                    Set<Slot> inputSlots = subqueryExpr.getInputSlots();
                    if (correlatedSlots.isEmpty() && inputSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.Unrelated;
                    } else if (leftOutputSlots.containsAll(inputSlots)
                            && leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(inputSlots)
                            && rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                } else if (subqueryExpr instanceof InSubquery) {
                    InSubquery inSubquery = (InSubquery) subqueryExpr;
                    Set<Slot> compareSlots = inSubquery.getCompareExpr().getInputSlots();
                    if (compareSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.UnSupported;
                    } else if (leftOutputSlots.containsAll(compareSlots)
                            && leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(compareSlots)
                            && rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                } else if (subqueryExpr instanceof Exists) {
                    if (correlatedSlots.isEmpty()) {
                        relatedInfo = RelatedInfo.Unrelated;
                    } else if (leftOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToLeft;
                    } else if (rightOutputSlots.containsAll(correlatedSlots)) {
                        relatedInfo = RelatedInfo.RelatedToRight;
                    }
                }
            }
            correlatedInfoList.add(relatedInfo);
        }
        return correlatedInfoList.build();
    }

    private Pair<LogicalPlan, Optional<Expression>> subqueryToApply(
            List<SubqueryExpr> subqueryExprs, LogicalPlan childPlan,
            Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
            CascadesContext ctx, Optional<Expression> correlatedOuterExpr, boolean isMarkJoinSlotNotNull) {
        Pair<LogicalPlan, Optional<Expression>> tmpPlan = Pair.of(childPlan, correlatedOuterExpr);
        for (int i = 0; i < subqueryExprs.size(); ++i) {
            SubqueryExpr subqueryExpr = subqueryExprs.get(i);
            if (subqueryExpr instanceof Exists && hasTopLevelScalarAgg(subqueryExpr.getQueryPlan())) {
                // because top level scalar agg always returns a value or null(for empty input)
                // so Exists and Not Exists correlatedOuterExpr are always evaluated to
                // True and false literals respectively, we don't create apply node for it
                continue;
            }

            if (!ctx.subqueryIsAnalyzed(subqueryExpr)) {
                tmpPlan = addApply(subqueryExpr, tmpPlan.first,
                    subqueryToMarkJoinSlot, ctx, tmpPlan.second, isMarkJoinSlotNotNull);
            }
        }
        return tmpPlan;
    }

    private static boolean hasTopLevelScalarAgg(Plan plan) {
        if (plan instanceof LogicalAggregate) {
            return ((LogicalAggregate) plan).getGroupByExpressions().isEmpty();
        } else if (plan instanceof LogicalProject || plan instanceof LogicalSort) {
            return hasTopLevelScalarAgg(plan.child(0));
        }
        return false;
    }

    private Pair<LogicalPlan, Optional<Expression>> addApply(SubqueryExpr subquery, LogicalPlan childPlan,
            Map<SubqueryExpr, Optional<MarkJoinSlotReference>> subqueryToMarkJoinSlot,
            CascadesContext ctx, Optional<Expression> correlatedOuterExpr, boolean isMarkJoinSlotNotNull) {
        ctx.setSubqueryExprIsAnalyzed(subquery, true);
        Optional<MarkJoinSlotReference> markJoinSlot = subqueryToMarkJoinSlot.get(subquery);
        boolean needAddScalarSubqueryOutputToProjects = isScalarSubqueryOutputUsedInOuterScope(
                subquery, correlatedOuterExpr);
        // for scalar subquery, we need ensure it output at most 1 row
        // by doing that, we add an aggregate function any_value() to the project list
        // we use needRuntimeAnyValue to indicate if any_value() is needed
        // if needRuntimeAnyValue is true, we will add it to the project list
        boolean needRuntimeAnyValue = false;
        NamedExpression subqueryOutput = subquery.getQueryPlan().getOutput().get(0);
        if (subquery instanceof ScalarSubquery) {
            // scalar sub query may adjust output slot's nullable.
            subqueryOutput = ((ScalarSubquery) subquery).getOutputSlotAdjustNullable();
        }
        Slot countSlot = null;
        Slot anyValueSlot = null;
        Optional<Expression> newCorrelatedOuterExpr = correlatedOuterExpr;
        if (needAddScalarSubqueryOutputToProjects && !subquery.getCorrelateSlots().isEmpty()) {
            if (((ScalarSubquery) subquery).hasTopLevelScalarAgg()) {
                // consider sql: SELECT * FROM t1 WHERE t1.a <= (SELECT COUNT(t2.a) FROM t2 WHERE (t1.b = t2.b));
                // when unnest correlated subquery, we create a left join node.
                // outer query is left table and subquery is right one
                // if there is no match, the row from right table is filled with nulls
                // but COUNT function is always not nullable.
                // so wrap COUNT with Nvl to ensure its result is 0 instead of null to get the correct result
                if (correlatedOuterExpr.isPresent()) {
                    List<NamedExpression> aggFunctions = ScalarSubquery.getTopLevelScalarAggFunctions(
                            subquery.getQueryPlan(), subquery.getCorrelateSlots());
                    SubQueryRewriteResult result = addNvlForScalarSubqueryOutput(aggFunctions, subqueryOutput,
                            subquery, correlatedOuterExpr);
                    subqueryOutput = result.subqueryOutput;
                    subquery = result.subquery;
                    newCorrelatedOuterExpr = result.correlatedOuterExpr;
                }
            } else {
                // if scalar subquery doesn't have top level scalar agg we will create one, for example
                // select (select t2.c1 from t2 where t2.c2 = t1.c2) from t1;
                // the original output of the correlate subquery is t2.c1, after adding a scalar agg, it will be
                // select (select count(*), any_value(t2.c1) from t2 where t2.c2 = t1.c2) from t1;
                Alias anyValueAlias = new Alias(new AnyValue(subqueryOutput));
                LogicalAggregate<Plan> aggregate;
                if (((ScalarSubquery) subquery).limitOneIsEliminated()) {
                    aggregate = new LogicalAggregate<>(ImmutableList.of(),
                            ImmutableList.of(anyValueAlias), subquery.getQueryPlan());
                } else {
                    Alias countAlias = new Alias(new Count());
                    countSlot = countAlias.toSlot();
                    aggregate = new LogicalAggregate<>(ImmutableList.of(),
                            ImmutableList.of(countAlias, anyValueAlias), subquery.getQueryPlan());
                }
                anyValueSlot = anyValueAlias.toSlot();
                subquery = subquery.withSubquery(aggregate);
                if (correlatedOuterExpr.isPresent()) {
                    Map<Expression, Expression> replaceMap = new HashMap<>();
                    replaceMap.put(subqueryOutput, anyValueSlot);
                    newCorrelatedOuterExpr = Optional.of(ExpressionUtils.replace(correlatedOuterExpr.get(),
                            replaceMap));
                }
                needRuntimeAnyValue = true;
            }
        }
        LogicalApply.SubQueryType subQueryType;
        boolean isNot = false;
        Optional<Expression> compareExpr = Optional.empty();
        if (subquery instanceof InSubquery) {
            subQueryType = LogicalApply.SubQueryType.IN_SUBQUERY;
            isNot = ((InSubquery) subquery).isNot();
            compareExpr = Optional.of(((InSubquery) subquery).getCompareExpr());
        } else if (subquery instanceof Exists) {
            subQueryType = LogicalApply.SubQueryType.EXITS_SUBQUERY;
            isNot = ((Exists) subquery).isNot();
        } else if (subquery instanceof ScalarSubquery) {
            subQueryType = LogicalApply.SubQueryType.SCALAR_SUBQUERY;
        } else {
            throw new AnalysisException(String.format("Unsupported subquery : %s", subquery.toString()));
        }
        LogicalApply newApply = new LogicalApply(
                subquery.getCorrelateSlots(),
                subQueryType, isNot, compareExpr, subquery.getTypeCoercionExpr(), Optional.empty(),
                markJoinSlot,
                needAddScalarSubqueryOutputToProjects, isMarkJoinSlotNotNull,
                childPlan, subquery.getQueryPlan());

        ImmutableList.Builder<NamedExpression> projects =
                ImmutableList.builderWithExpectedSize(childPlan.getOutput().size() + 3);
        // left child
        projects.addAll(childPlan.getOutput());
        // markJoinSlotReference
        markJoinSlot.map(projects::add);
        LogicalProject logicalProject;
        if (needAddScalarSubqueryOutputToProjects) {
            if (needRuntimeAnyValue) {
                // if we create a new subquery in previous step, we need add the any_value() and assert_true()
                // into the project list. So BE will use assert_true to check if the subquery return only 1 row
                projects.add(anyValueSlot);
                if (countSlot != null) {
                    List<NamedExpression> upperProjects = new ArrayList<>();
                    upperProjects.addAll(projects.build());
                    projects.add(new Alias(new AssertTrue(
                            ExpressionUtils.or(new IsNull(countSlot),
                                    new LessThanEqual(countSlot, new IntegerLiteral(1))),
                            new VarcharLiteral("correlate scalar subquery must return only 1 row"))));
                    logicalProject = new LogicalProject(projects.build(), newApply);
                } else {
                    logicalProject = new LogicalProject(projects.build(), newApply);
                }
            } else {
                projects.add(subqueryOutput);
                logicalProject = new LogicalProject(projects.build(), newApply);
            }
        } else {
            logicalProject = new LogicalProject(projects.build(), newApply);
        }

        return Pair.of(logicalProject, newCorrelatedOuterExpr);
    }

    /**
     * SubQueryRewriteResult
     */
    @VisibleForTesting
    protected class SubQueryRewriteResult {
        public SubqueryExpr subquery;
        public NamedExpression subqueryOutput;
        public Optional<Expression> correlatedOuterExpr;

        public SubQueryRewriteResult(SubqueryExpr subquery, NamedExpression subqueryOutput,
                                     Optional<Expression> correlatedOuterExpr) {
            this.subquery = subquery;
            this.subqueryOutput = subqueryOutput;
            this.correlatedOuterExpr = correlatedOuterExpr;
        }
    }

    /**
     * for correlated scalar subquery like select c1, (select count(c1) from t2 where t1.c2 = t2.c2) as c from t1
     * if we don't add extra nvl for not nullable agg functions, the plan will be like bellow:
     * +--LogicalProject(projects=[c1#0, count(c1)#4 AS `c`#5])
     *    +--LogicalJoin(type=LEFT_OUTER_JOIN, hashJoinConjuncts=[(c2#1 = c2#3)])
     *       |--LogicalOlapScan (t1)
     *       +--LogicalAggregate[108] (groupByExpr=[c2#3], outputExpr=[c2#3, count(c1#2) AS `count(c1)`#4])
     *          +--LogicalOlapScan (t2)
     *
     * the count(c1)#4 may be null because of unmatched row of left outer join, but count is not nullable agg function,
     * it should never be null, we need use nvl to wrap it and change the plan like bellow:
     * +--LogicalProject(projects=[c1#0, ifnull(count(c1)#4, 0) AS `c`#5])
     *    +--LogicalJoin(type=LEFT_OUTER_JOIN, hashJoinConjuncts=[(c2#1 = c2#3)])
     *       |--LogicalOlapScan (t1)
     *       +--LogicalAggregate[108] (groupByExpr=[c2#3], outputExpr=[c2#3, count(c1#2) AS `count(c1)`#4])
     *          +--LogicalOlapScan (t2)
     *
     * in order to do that, we need change subquery's output and replace the correlated outer expr
     */
    @VisibleForTesting
    protected SubQueryRewriteResult addNvlForScalarSubqueryOutput(List<NamedExpression> aggFunctions,
                                                                NamedExpression subqueryOutput,
                                                                SubqueryExpr subquery,
                                                                Optional<Expression> correlatedOuterExpr) {
        SubQueryRewriteResult result = new SubQueryRewriteResult(subquery, subqueryOutput, correlatedOuterExpr);
        Map<Expression, Expression> replaceMapForSubqueryProject = new HashMap<>();
        Map<Expression, Expression> replaceMapForCorrelatedOuterExpr = new HashMap<>();
        for (NamedExpression agg : aggFunctions) {
            if (agg instanceof Alias && ((Alias) agg).child() instanceof NotNullableAggregateFunction) {
                NotNullableAggregateFunction notNullableAggFunc =
                        (NotNullableAggregateFunction) ((Alias) agg).child();
                if (subquery.getQueryPlan() instanceof LogicalProject) {
                    // if the top node of subquery is LogicalProject, we need replace the agg slot in
                    // project list by nvl(agg), and this project will be placed above LogicalApply node
                    Slot aggSlot = agg.toSlot();
                    replaceMapForSubqueryProject.put(aggSlot, new Alias(new Nvl(aggSlot,
                            notNullableAggFunc.resultForEmptyInput())));
                } else {
                    replaceMapForCorrelatedOuterExpr.put(subqueryOutput, new Nvl(subqueryOutput,
                            notNullableAggFunc.resultForEmptyInput()));
                }
            }
        }
        if (!replaceMapForSubqueryProject.isEmpty()) {
            Preconditions.checkState(subquery.getQueryPlan() instanceof LogicalProject,
                    "Scalar subquery's top plan node should be LogicalProject");
            LogicalProject logicalProject =
                    (LogicalProject) subquery.getQueryPlan();
            Preconditions.checkState(logicalProject.getOutputs().size() == 1,
                    "Scalar subuqery's should only output 1 column");
            NamedExpression newOutput = (NamedExpression) ExpressionUtils
                    .replace((NamedExpression) logicalProject.getProjects().get(0),
                            replaceMapForSubqueryProject);
            replaceMapForCorrelatedOuterExpr.put(subqueryOutput, newOutput.toSlot());
            result.subqueryOutput = newOutput;
            // logicalProject will be placed above LogicalApply later, so we remove it from subquery
            result.subquery = subquery.withSubquery((LogicalPlan) logicalProject.child());
        }
        if (!replaceMapForCorrelatedOuterExpr.isEmpty()) {
            result.correlatedOuterExpr = Optional.of(ExpressionUtils.replace(correlatedOuterExpr.get(),
                    replaceMapForCorrelatedOuterExpr));
        }
        return result;
    }

    private boolean isScalarSubqueryOutputUsedInOuterScope(
            SubqueryExpr subqueryExpr, Optional<Expression> correlatedOuterExpr) {
        return subqueryExpr instanceof ScalarSubquery
            && ((correlatedOuterExpr.isPresent()
                && ((ImmutableSet) correlatedOuterExpr.get().collect(SlotReference.class::isInstance))
                    .contains(subqueryExpr.getQueryPlan().getOutput().get(0))));
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
            if (hasTopLevelScalarAgg(exists.getQueryPlan())) {
                /*
                top level scalar agg and always return a value or null for empty input
                so Exists and Not Exists conjunct are always evaluated to True and False literals respectively
                    SELECT *
                    FROM t1
                    WHERE EXISTS (
                            SELECT SUM(a)
                            FROM t2
                            WHERE t1.a = t2.b and t1.a = 1;
                        );
                 */
                return exists.isNot() ? BooleanLiteral.FALSE : BooleanLiteral.TRUE;
            } else {
                boolean needCreateMarkJoinSlot = isMarkJoin || shouldOutputMarkJoinSlot;
                if (needCreateMarkJoinSlot) {
                    MarkJoinSlotReference markJoinSlotReference =
                            new MarkJoinSlotReference(statementContext.generateColumnName());
                    context.setSubqueryToMarkJoinSlot(exists, Optional.of(markJoinSlotReference));
                    return new Nvl(markJoinSlotReference, BooleanLiteral.FALSE);
                } else {
                    return BooleanLiteral.TRUE;
                }
            }
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
        public Expression visitCompoundPredicate(CompoundPredicate compound, SubqueryContext context) {
            // update isMarkJoin flag
            if (compound instanceof Or) {
                for (Expression child : compound.children()) {
                    if (child.anyMatch(SubqueryExpr.class::isInstance)) {
                        isMarkJoin = true;
                        break;
                    }
                }
            }
            return compound.withChildren(
                    compound.children().stream().map(c -> replace(c, context)).collect(Collectors.toList())
            );
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

    private List<Boolean> shouldOutputMarkJoinSlot(Collection<Expression> conjuncts) {
        ImmutableList.Builder<Boolean> result = ImmutableList.builderWithExpectedSize(conjuncts.size());
        for (Expression expr : conjuncts) {
            result.add(!(expr instanceof SubqueryExpr) && expr.containsType(SubqueryExpr.class));
        }
        return result.build();
    }

    private CollectSubquerys collectSubquerys(Collection<? extends Expression> exprs) {
        boolean hasSubqueryExpr = false;
        ImmutableList.Builder<Set<SubqueryExpr>> subqueryExprsListBuilder = ImmutableList.builder();
        for (Expression expression : exprs) {
            Set<SubqueryExpr> subqueries = expression.collect(SubqueryExpr.class::isInstance);
            hasSubqueryExpr |= !subqueries.isEmpty();
            subqueryExprsListBuilder.add(subqueries);
        }
        return new CollectSubquerys(subqueryExprsListBuilder.build(), hasSubqueryExpr);
    }

    private static class CollectSubquerys {
        final List<Set<SubqueryExpr>> subqueies;
        final boolean hasSubquery;

        public CollectSubquerys(List<Set<SubqueryExpr>> subqueies, boolean hasSubquery) {
            this.subqueies = subqueies;
            this.hasSubquery = hasSubquery;
        }
    }
}
