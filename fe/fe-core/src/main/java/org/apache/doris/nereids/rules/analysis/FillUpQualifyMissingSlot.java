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

import org.apache.doris.nereids.analyzer.Scope;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.rules.Rule;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionRewriter;
import org.apache.doris.nereids.trees.expressions.visitor.DefaultExpressionVisitor;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Aggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
import org.apache.doris.nereids.util.ExpressionUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * We don't fill the missing slots in FillUpMissingSlots.
 * Because for distinct queries,
 * for example:
 * select distinct year,country from sales having year > 2000 qualify row_number() over (order by year + 1) > 1;
 * It would be converted into the form of agg.
 * before logical plan:
 * qualify
 *   |
 * project(distinct)
 *   |
 * scan
 * apply ProjectWithDistinctToAggregate rule
 * after logical plan:
 * qualify
 *   |
 *  agg
 *   |
 * scan
 * if fill the missing slots in FillUpMissingSlots(after ProjectWithDistinctToAggregate). qualify could hardly be
 * pushed under the agg of distinct.
 * But apply FillUpQualifyMissingSlot rule before ProjectWithDistinctToAggregate
 * logical plan:
 * project(distinct)
 *   |
 * qualify
 *   |
 * project
 *   |
 * scan
 * and then apply ProjectWithDistinctToAggregate rule
 * logical plan:
 * agg
 *   |
 * qualify
 *   |
 * project
 *   |
 * scan
 * So it is easy to handle.
 */
public class FillUpQualifyMissingSlot extends FillUpMissingSlots {
    @Override
    public List<Rule> buildRules() {
        return ImmutableList.of(
            /*
               qualify -> project
               qualify -> project(distinct)
               qualify -> project(distinct) -> agg
               qualify -> project(distinct) -> having -> agg
            */
            RuleType.FILL_UP_QUALIFY_PROJECT.build(
                logicalQualify(logicalProject())
                    .thenApply(ctx -> {
                        LogicalQualify<LogicalProject<Plan>> qualify = ctx.root;
                        checkWindow(qualify);
                        Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                        LogicalProject<Plan> project = qualify.child();
                        return createPlan(project, qualify.getConjuncts(), ImmutableSet.of(), outerScope,
                                (newConjuncts, newHavingConjuncts, projects) -> {
                                    LogicalProject<Plan> bottomProject =
                                            new LogicalProject<>(projects, project.child());
                                    LogicalQualify<Plan> logicalQualify =
                                            new LogicalQualify<>(newConjuncts, bottomProject);
                                    ImmutableList<NamedExpression> copyOutput =
                                            ImmutableList.copyOf(project.getOutput());
                                    return new LogicalProject<>(copyOutput, project.isDistinct(), logicalQualify);
                                });
                    })
            ),
            /*
               qualify -> agg
             */
            RuleType.FILL_UP_QUALIFY_AGGREGATE.build(
                logicalQualify(aggregate()).thenApply(ctx -> {
                    LogicalQualify<Aggregate<Plan>> qualify = ctx.root;
                    checkWindow(qualify);
                    Aggregate<Plan> agg = qualify.child();
                    Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                    // resolve aggregate-output aliases that only depend on outer correlated slots,
                    // so the outer dependency is visible to subquery unnesting. The alias itself is
                    // kept in the aggregate output because it may be referenced by the select list.
                    Set<Expression> qualifyConjuncts = resolveCorrelatedAggregateOutputAlias(
                            qualify.getConjuncts(), ImmutableSet.of(), agg.getOutputExpressions(), outerScope);
                    Resolver resolver = new Resolver(agg, outerScope);
                    qualifyConjuncts.forEach(expr -> resolver.resolve(expr, ResolvePlanType.QUALIFY));
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                qualifyConjuncts, r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        return notChanged ? qualify.withChildren(a) : new LogicalQualify<>(newConjuncts, a);
                    });
                })
            ),
            /*
               qualify -> having -> agg
             */
            RuleType.FILL_UP_QUALIFY_HAVING_AGGREGATE.build(
                logicalQualify(logicalHaving(aggregate())).thenApply(ctx -> {
                    LogicalQualify<LogicalHaving<Aggregate<Plan>>> qualify = ctx.root;
                    checkWindow(qualify);
                    LogicalHaving<Aggregate<Plan>> having = qualify.child();
                    Aggregate<Plan> agg = qualify.child().child();
                    Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                    // The window expression in qualify will be extracted into a project above the
                    // having during NormalizeAggregate. A correlated predicate left in the having
                    // would then sit below the window project and be silently dropped during subquery
                    // unnesting. A having predicate that only depends on the outer row (no aggregate
                    // function and no subquery, all input slots correlated) is constant over the
                    // aggregate rows, so it is equivalent before and after the window and can safely
                    // be conjoined into the qualify to be decorrelated together. A correlated having
                    // predicate that depends on the aggregate result (or contains a subquery) must be
                    // evaluated on the aggregate rows below the window, which is not supported
                    // together with QUALIFY; reject it explicitly instead of silently dropping it or
                    // changing the evaluation order.
                    Set<Expression> newHavingConjuncts = new LinkedHashSet<>();
                    Set<Expression> qualifyConjuncts = new LinkedHashSet<>(qualify.getConjuncts());
                    if (outerScope.isPresent()) {
                        Set<Slot> correlatedSlots = outerScope.get().getCorrelatedSlots();
                        for (Expression conjunct : having.getConjuncts()) {
                            Set<Slot> inputSlots = conjunct.getInputSlots();
                            if (inputSlots.isEmpty()) {
                                newHavingConjuncts.add(conjunct);
                            } else if (correlatedSlots.containsAll(inputSlots)
                                    && !ExpressionUtils.hasNonWindowAggregateFunction(conjunct)
                                    && !conjunct.containsType(SubqueryExpr.class)
                                    // a volatile predicate (e.g. `random() < 0.5`) is not constant over
                                    // the aggregate rows, so moving it above the window would change
                                    // how many times it is evaluated; keep it below the window, or
                                    // reject it if it is correlated (it would otherwise be dropped).
                                    && !conjunct.containsVolatileExpression()) {
                                // the predicate only depends on the outer row, so it can safely be
                                // evaluated above the window project inside the qualify.
                                qualifyConjuncts.add(conjunct);
                            } else if (inputSlots.stream().anyMatch(correlatedSlots::contains)) {
                                // the predicate is correlated but depends on the aggregate result, a
                                // subquery, or a volatile expression, so it must be evaluated below
                                // the window; not supported.
                                throw new AnalysisException("Correlated predicate '" + conjunct.toSql()
                                        + "' in HAVING depending on the aggregate result or containing "
                                        + "a volatile expression is not supported together with QUALIFY");
                            } else {
                                newHavingConjuncts.add(conjunct);
                            }
                        }
                    } else {
                        newHavingConjuncts.addAll(having.getConjuncts());
                    }
                    Set<Expression> resolvedQualifyConjuncts = resolveCorrelatedAggregateOutputAlias(
                            qualifyConjuncts, newHavingConjuncts, agg.getOutputExpressions(), outerScope);
                    Resolver resolver = new Resolver(agg, outerScope);
                    resolvedQualifyConjuncts.forEach(expr -> resolver.resolve(expr, ResolvePlanType.QUALIFY));
                    return createPlan(resolver, agg, (r, a) -> {
                        Set<Expression> newConjuncts = ExpressionUtils.replace(
                                resolvedQualifyConjuncts, r.getSubstitution());
                        boolean notChanged = newConjuncts.equals(qualify.getConjuncts())
                                && newHavingConjuncts.equals(having.getConjuncts());
                        if (notChanged && a.equals(agg)) {
                            return null;
                        }
                        LogicalHaving<Plan> newHaving = having.withConjuncts(newHavingConjuncts);
                        return notChanged ? qualify.withChildren(newHaving.withChildren(a)) :
                            new LogicalQualify<>(newConjuncts, newHaving.withChildren(a));
                    });
                })
            ),
            /*
               qualify -> having -> project
               qualify -> having -> project(distinct)
             */
            RuleType.FILL_UP_QUALIFY_HAVING_PROJECT.build(
                logicalQualify(logicalHaving(logicalProject())).thenApply(ctx -> {
                    LogicalQualify<LogicalHaving<LogicalProject<Plan>>> qualify = ctx.root;
                    checkWindow(qualify);
                    Optional<Scope> outerScope = ctx.cascadesContext.getOuterScope();
                    LogicalHaving<LogicalProject<Plan>> having = qualify.child();
                    LogicalProject<Plan> project = qualify.child().child();
                    // The having conjuncts must take part in createPlan's alias classification too:
                    // an outer-only producer used through a SELECT alias consumed only by HAVING
                    // (e.g. `HAVING f = 1` where f = o.flag) would otherwise be left bound locally,
                    // and the window-bearing project would prevent later correlation extraction,
                    // leaving the outer slot dangling in the right subtree.
                    return createPlan(project, qualify.getConjuncts(), having.getConjuncts(), outerScope,
                            (newConjuncts, newHavingConjuncts, projects) -> {
                                ImmutableList<NamedExpression> copyOutput = ImmutableList.copyOf(project.getOutput());
                                if (project.isDistinct()) {
                                    // Keep correlated predicates that only depend on outer slots together with the
                                    // having's own correlated predicates, on the same decorrelatable side of the
                                    // distinct barrier, so subquery unnesting can collect and decorrelate them
                                    // together (otherwise one of them is left dangling in the apply's right side).
                                    // A predicate that is constant per outer row is equivalent before/after
                                    // distinct, so moving it above the distinct project preserves semantics.
                                    Set<Expression> relocatedHavingConjuncts =
                                            new LinkedHashSet<>(newHavingConjuncts);
                                    Set<Expression> distinctQualifyConjuncts = new LinkedHashSet<>();
                                    if (outerScope.isPresent()) {
                                        Set<Slot> correlatedSlots = outerScope.get().getCorrelatedSlots();
                                        for (Expression conjunct : newConjuncts) {
                                            Set<Slot> inputSlots = conjunct.getInputSlots();
                                            // Only relocate deterministic predicates: visible outer slots do
                                            // not make a predicate constant (e.g. `o.flag <> 1 OR random() < 0.5`
                                            // has only {o.flag} as input slots), and moving a volatile
                                            // predicate changes its evaluation domain (per row before DISTINCT
                                            // vs once after DISTINCT), so keep volatile predicates on their
                                            // original side of the distinct barrier.
                                            // A conjunct containing a subquery is never constant per outer row
                                            // even when all its visible input slots are correlated outer slots:
                                            // getInputSlots() does not traverse the subquery's inner plan, so
                                            // the subquery may still depend on inner rows (e.g. `o.flag = 1 OR
                                            // EXISTS (SELECT ... WHERE j.v = i.not_grouped)` reports only
                                            // {o.flag}); moving it above the distinct would force the nested
                                            // apply above the distinct project where its inner correlation has
                                            // no owner. Keep such conjuncts on their original side of the
                                            // distinct barrier.
                                            if (!inputSlots.isEmpty() && correlatedSlots.containsAll(inputSlots)
                                                    && !conjunct.containsVolatileExpression()
                                                    && !conjunct.containsType(SubqueryExpr.class)) {
                                                relocatedHavingConjuncts.add(conjunct);
                                            } else {
                                                distinctQualifyConjuncts.add(conjunct);
                                            }
                                        }
                                    } else {
                                        distinctQualifyConjuncts.addAll(newConjuncts);
                                    }
                                    Set<Slot> missingSlots = relocatedHavingConjuncts.stream()
                                            .map(Expression::getInputSlots)
                                            .flatMap(Set::stream)
                                            .filter(s -> !projects.contains(s))
                                            .filter(s -> !(outerScope.isPresent()
                                                    && outerScope.get().getCorrelatedSlots().contains(s)))
                                            .collect(Collectors.toSet());
                                    List<NamedExpression> output = ImmutableList.<NamedExpression>builder()
                                            .addAll(projects).addAll(missingSlots).build();
                                    LogicalQualify<LogicalProject<Plan>> logicalQualify =
                                            new LogicalQualify<>(distinctQualifyConjuncts,
                                                    new LogicalProject<>(output, project.child()));
                                    return having.withConjuncts(relocatedHavingConjuncts)
                                            .withChildren(project.withProjects(copyOutput)
                                                    .withChildren(logicalQualify));
                                } else {
                                    return new LogicalProject<>(copyOutput, new LogicalQualify<>(newConjuncts,
                                            having.withConjuncts(newHavingConjuncts)
                                                    .withChildren(project.withProjects(projects))));
                                }
                            });
                })
            )
        );
    }

    interface PlanGenerator {
        Plan apply(Set<Expression> newConjuncts, Set<Expression> newHavingConjuncts,
                List<NamedExpression> projects);
    }

    private Plan createPlan(LogicalProject<Plan> project, Set<Expression> qualifyConjuncts,
            Set<Expression> havingConjuncts, Optional<Scope> outerScope, PlanGenerator planGenerator) {
        Set<Slot> projectOutputSet = project.getOutputSet();
        List<NamedExpression> newOutputSlots = Lists.newArrayList();
        Set<Expression> newConjuncts = new LinkedHashSet<>();
        Set<Expression> newHavingConjuncts = new LinkedHashSet<>();

        // A correlated column referenced in qualify or having may be hidden behind a project
        // alias, e.g. `QUALIFY f = 1` or `HAVING f = 1` where f is aliased as an outer column
        // o.flag. If the project also contains a window expression, filter pushdown cannot
        // rewrite f back to its producer before apply decorrelation, so the alias-producer
        // dependency would be lost and the correlation slot would never be collected into the
        // apply. Resolve such aliases whose producers reference only outer correlated slots, so
        // the correlation stays visible to subquery unnesting. The HAVING conjuncts must take
        // part in this classification too: an outer-only producer used through a SELECT alias
        // consumed only by HAVING would otherwise be left bound locally (FillUpMissingSlots sees
        // the alias in the project output and does nothing), leaving the outer slot dangling
        // below the window-bearing project.
        Map<Slot, Expression> correlatedAliasToProducer = Maps.newHashMap();
        // Split the conjuncts (both qualify and having): conjuncts that contain a subquery
        // (IN/NOT IN/scalar/EXISTS) must not be rewritten, because ExpressionUtils.replace would
        // descend into the subquery (e.g. the IN compare expression) and break the apply's slot
        // ownership. Aliases in the remaining conjuncts are still resolved normally, so a
        // replaceable correlated alias is not blocked just because another conjunct happens to
        // contain a subquery.
        Set<Expression> qualifyReplaceableConjuncts = new LinkedHashSet<>();
        Set<Expression> qualifySubqueryConjuncts = new LinkedHashSet<>();
        Set<Expression> havingReplaceableConjuncts = new LinkedHashSet<>();
        Set<Expression> havingSubqueryConjuncts = new LinkedHashSet<>();
        for (Expression conjunct : qualifyConjuncts) {
            if (conjunct.containsType(SubqueryExpr.class)) {
                qualifySubqueryConjuncts.add(conjunct);
            } else {
                qualifyReplaceableConjuncts.add(conjunct);
            }
        }
        for (Expression conjunct : havingConjuncts) {
            if (conjunct.containsType(SubqueryExpr.class)) {
                havingSubqueryConjuncts.add(conjunct);
            } else {
                havingReplaceableConjuncts.add(conjunct);
            }
        }
        Set<Expression> classificationConjuncts = new LinkedHashSet<>();
        classificationConjuncts.addAll(qualifyConjuncts);
        classificationConjuncts.addAll(havingConjuncts);
        if (outerScope.isPresent()) {
            Set<Slot> correlatedSlots = outerScope.get().getCorrelatedSlots();
            for (Map.Entry<Slot, Expression> entry : project.getAliasToProducer().entrySet()) {
                Slot aliasSlot = entry.getKey();
                Expression producer = entry.getValue();
                if (!producer.getInputSlots().isEmpty()
                        && correlatedSlots.containsAll(producer.getInputSlots())) {
                    boolean referenced = classificationConjuncts.stream()
                            .anyMatch(c -> c.getInputSlots().contains(aliasSlot));
                    if (!producer.containsType(WindowExpression.class)
                            // a window producer would be re-extracted into a fresh alias, which would be
                            // rewritten again, causing an infinite rewrite loop before a fixed point.
                            && !producer.containsType(SubqueryExpr.class)
                            // a producer containing a subquery would be copied into both the project and
                            // the rewritten qualify/having, and only one copy would be unnested, leaving a
                            // dangling slot in the other.
                            && !producer.containsVolatileExpression()) {
                        correlatedAliasToProducer.put(aliasSlot, producer);
                    } else if (referenced && producer.containsVolatileExpression()) {
                        // The alias only depends on outer correlated slots through a volatile
                        // expression (e.g. `random() + o.flag AS f`). Substituting it into the qualify
                        // or having would evaluate the volatile expression twice with different values,
                        // breaking the identity between the predicate and the returned value; keeping it
                        // in the project hides the correlation and leaves a dangling outer slot. Reject
                        // this usage explicitly.
                        throw new AnalysisException("QUALIFY referencing a correlated outer column "
                                + "through a volatile expression ('" + producer.toSql() + "') is not "
                                + "supported");
                    } else if (referenced && producer.containsType(SubqueryExpr.class)) {
                        // The alias only depends on outer correlated slots through a subquery
                        // (e.g. `o.flag + (SELECT max(j) FROM t_j) AS f`). Substituting it into the
                        // qualify or having would copy the subquery into two plan nodes and only one
                        // copy would be unnested, leaving a dangling slot in the other; keeping it in
                        // the project also leaves a dangling outer slot. Reject this usage explicitly.
                        throw new AnalysisException("QUALIFY referencing a correlated outer column "
                                + "through a subquery ('" + producer.toSql() + "') is not supported");
                    } else if (referenced && producer.containsType(WindowExpression.class)) {
                        // The alias only depends on outer correlated slots through a window expression
                        // (e.g. `row_number() over (order by o.flag) AS rn`). Substituting it into the
                        // qualify or having would be re-extracted into a fresh alias endlessly (no fixed
                        // point), while keeping it in the project leaves a dangling outer slot. Reject
                        // this usage explicitly.
                        throw new AnalysisException("QUALIFY referencing a correlated outer column "
                                + "through a window expression ('" + producer.toSql() + "') is not "
                                + "supported");
                    }
                }
            }
            // A correlated alias referenced inside a subquery-containing conjunct cannot be resolved
            // (replacement would descend into the subquery and break slot ownership), and keeping it
            // would leave a dangling outer slot in the inner project. Reject this usage explicitly.
            for (Expression conjunct : classificationConjuncts) {
                if (conjunct.containsType(SubqueryExpr.class)) {
                    for (Map.Entry<Slot, Expression> entry : correlatedAliasToProducer.entrySet()) {
                        if (conjunct.getInputSlots().contains(entry.getKey())) {
                            throw new AnalysisException("QUALIFY referencing a correlated outer column "
                                    + "through a subquery ('" + entry.getValue().toSql() + "') is not "
                                    + "supported");
                        }
                    }
                }
            }
        }
        boolean conjunctsRewritten = false;
        if (!correlatedAliasToProducer.isEmpty()) {
            Set<Expression> rewrittenQualifyReplaceable =
                    ExpressionUtils.replace(qualifyReplaceableConjuncts, correlatedAliasToProducer);
            Set<Expression> rewrittenHavingReplaceable =
                    ExpressionUtils.replace(havingReplaceableConjuncts, correlatedAliasToProducer);
            conjunctsRewritten = !rewrittenQualifyReplaceable.equals(qualifyReplaceableConjuncts)
                    || !rewrittenHavingReplaceable.equals(havingReplaceableConjuncts);
            qualifyReplaceableConjuncts = rewrittenQualifyReplaceable;
            havingReplaceableConjuncts = rewrittenHavingReplaceable;
        }

        Set<Expression> rewrittenQualifyConjuncts = new LinkedHashSet<>();
        rewrittenQualifyConjuncts.addAll(qualifyReplaceableConjuncts);
        rewrittenQualifyConjuncts.addAll(qualifySubqueryConjuncts);
        for (Expression conjunct : rewrittenQualifyConjuncts) {
            conjunct = conjunct.accept(new DefaultExpressionRewriter<List<NamedExpression>>() {
                @Override
                public Expression visitWindow(WindowExpression window, List<NamedExpression> context) {
                    Alias alias = new Alias(window);
                    context.add(alias);
                    return alias.toSlot();
                }
            }, newOutputSlots);
            newConjuncts.addAll(ExpressionUtils.extractConjunctionToSet(conjunct));
        }
        // The having conjuncts cannot contain window expressions, so only the alias substitution
        // above applies to them; the rewritten conjuncts stay on the having node.
        newHavingConjuncts.addAll(havingReplaceableConjuncts);
        newHavingConjuncts.addAll(havingSubqueryConjuncts);
        Set<Slot> notExistedInProject = new LinkedHashSet<>();
        notExistedInProject.addAll(rewrittenQualifyConjuncts.stream()
                .map(Expression::getInputSlots)
                .flatMap(Set::stream)
                .filter(s -> !projectOutputSet.contains(s))
                // ATTN: exclude outer query's correlated slots, they belong to outer query
                // and should not be filled up into the inner project's output.
                .filter(s -> !(outerScope.isPresent()
                        && outerScope.get().getCorrelatedSlots().contains(s)))
                .collect(Collectors.toSet()));

        // getInputSlots() deliberately does not traverse a subquery's inner plan, so a nested
        // subquery in QUALIFY may correlate to a column of the inner query that is not directly
        // referenced in any other conjunct (e.g. `EXISTS (SELECT ... WHERE j.v = i.not_grouped)`).
        // For a plain project there is no distinct/aggregate barrier: the correlation slot is owned
        // by the child plan, so it can be surfaced in the project output without changing semantics
        // (the upper/lower project split then gives the nested apply a left child that owns the
        // slot, and the unchanged upper project strips the helper). Add such slots to the missing
        // slots. For a DISTINCT project, extending the distinct key with a non-output column would
        // change the distinct semantics, so that shape is rejected with a clear error instead of
        // failing with a cryptic slot-validation error.
        for (Expression conjunct : rewrittenQualifyConjuncts) {
            if (conjunct.containsType(SubqueryExpr.class)) {
                Set<SubqueryExpr> subqueryExprs =
                        conjunct.collect(e -> e instanceof SubqueryExpr);
                for (SubqueryExpr subqueryExpr : subqueryExprs) {
                    for (Slot correlatedSlot : subqueryExpr.getCorrelateSlots()) {
                        if (!projectOutputSet.contains(correlatedSlot)
                                && !(outerScope.isPresent()
                                        && outerScope.get().getCorrelatedSlots().contains(correlatedSlot))) {
                            if (project.isDistinct()) {
                                throw new AnalysisException("QUALIFY nested subquery referencing "
                                        + "column '" + correlatedSlot.toSql()
                                        + "' that is not in the inner query output is not supported "
                                        + "(subqueries in QUALIFY can only reference columns in the "
                                        + "SELECT list)");
                            }
                            notExistedInProject.add(correlatedSlot);
                        }
                    }
                }
            }
        }

        newOutputSlots.addAll(notExistedInProject);
        if (newOutputSlots.isEmpty() && !conjunctsRewritten) {
            return null;
        }
        List<NamedExpression> projects = ImmutableList.<NamedExpression>builder()
                .addAll(project.getProjects())
                .addAll(newOutputSlots).build();

        return planGenerator.apply(newConjuncts, newHavingConjuncts, projects);
    }

    /**
     * Reject QUALIFY or HAVING references to aggregate output expressions that only depend on
     * outer correlated slots. Such an expression cannot be produced by the aggregate (its child
     * has no producer for the outer column), and after NormalizeAggregate/aggregate elimination
     * the outer slot would be left dangling in an output project. This covers both an aggregate
     * output ALIAS whose producer only depends on outer columns (e.g. `SELECT o.flag AS f ...
     * GROUP BY ... QUALIFY f = ...` or `HAVING f = ...`) and a raw, unaliased aggregate output
     * that is itself an outer correlated slot (e.g. `SELECT o.flag ... GROUP BY ... QUALIFY
     * o.flag = ...`: with ONLY_FULL_GROUP_BY enabled the select item stays a raw SlotReference,
     * so without this check Resolver.lookUp would self-match it as an aggregate output and
     * NormalizeAggregate would report a false GROUP BY error). The HAVING conjuncts must take
     * part in this classification too: an outer-only aggregate output alias consumed only by
     * HAVING would otherwise be left in the aggregate output below the window project with the
     * outer slot dangling. These shapes are not supported; reject them explicitly instead of
     * failing with a cryptic slot-validation or GROUP BY error.
     */
    private static Set<Expression> resolveCorrelatedAggregateOutputAlias(Set<Expression> qualifyConjuncts,
            Set<Expression> havingConjuncts, List<NamedExpression> aggregateOutput,
            Optional<Scope> outerScope) {
        if (!outerScope.isPresent()) {
            return qualifyConjuncts;
        }
        Set<Slot> correlatedSlots = outerScope.get().getCorrelatedSlots();
        Set<Expression> classificationConjuncts = new LinkedHashSet<>();
        classificationConjuncts.addAll(qualifyConjuncts);
        classificationConjuncts.addAll(havingConjuncts);
        for (NamedExpression output : aggregateOutput) {
            if (output instanceof Alias) {
                Expression producer = ((Alias) output).child();
                if (!producer.getInputSlots().isEmpty()
                        && correlatedSlots.containsAll(producer.getInputSlots())
                        && classificationConjuncts.stream()
                                .anyMatch(c -> c.getInputSlots().contains(output.toSlot()))) {
                    throw new AnalysisException("Aggregate output alias '" + output.toSql()
                            + "' that only depends on outer correlated columns is not supported "
                            + "together with QUALIFY in a correlated subquery");
                }
            } else if (!output.getInputSlots().isEmpty()
                    && correlatedSlots.containsAll(output.getInputSlots())
                    && classificationConjuncts.stream()
                            .anyMatch(c -> c.getInputSlots().contains(output.toSlot()))) {
                throw new AnalysisException("Aggregate output column '" + output.toSql()
                        + "' that is an outer correlated column is not supported together with "
                        + "QUALIFY in a correlated subquery");
            }
        }
        return qualifyConjuncts;
    }

    private void checkWindow(LogicalQualify<? extends Plan> qualify) throws AnalysisException {
        Set<SlotReference> inputSlots = new HashSet<>();
        AtomicBoolean hasWindow = new AtomicBoolean(false);
        for (Expression conjunct : qualify.getConjuncts()) {
            conjunct.accept(new DefaultExpressionVisitor<Void, Set<SlotReference>>() {
                @Override
                public Void visitWindow(WindowExpression windowExpression, Set<SlotReference> context) {
                    hasWindow.set(true);
                    return null;
                }

                @Override
                public Void visitSlotReference(SlotReference slotReference, Set<SlotReference> context) {
                    context.add(slotReference);
                    return null;
                }

            }, inputSlots);
        }
        if (hasWindow.get()) {
            return;
        }
        qualify.accept(new DefaultPlanVisitor<Void, Void>() {
            private void findWindow(List<NamedExpression> namedExpressions) {
                for (NamedExpression slot : namedExpressions) {
                    if (slot instanceof Alias && slot.child(0) instanceof WindowExpression) {
                        if (inputSlots.contains(slot.toSlot())) {
                            hasWindow.set(true);
                        }
                    }
                }
            }

            @Override
            public Void visitLogicalProject(LogicalProject<? extends Plan> project, Void context) {
                findWindow(project.getProjects());
                return visit(project, context);
            }

            @Override
            public Void visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate, Void context) {
                findWindow(aggregate.getOutputExpressions());
                return visit(aggregate, context);
            }
        }, null);
        if (!hasWindow.get()) {
            throw new AnalysisException("qualify only used for window expression");
        }
    }
}
