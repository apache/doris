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

package org.apache.doris.nereids.spm;

import org.apache.doris.analysis.TableScanParams;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.MarkJoinSlotReference;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.Variable;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ConnectionId;
import org.apache.doris.nereids.trees.expressions.functions.scalar.CurrentUser;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Database;
import org.apache.doris.nereids.trees.expressions.functions.scalar.SessionUser;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalHaving;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOneRowRelation;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalQualify;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.logical.LogicalUsingJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalView;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.util.RelationUtil;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * SPMPlanTreeSupport - whole-query plan-tree SPM engine.
 *
 * SPM binds and rewrites a whole parsed (still unbound) SELECT plan tree, not only the
 * per-query-block WHERE predicates. The three lifecycle phases run over the whole tree:
 *
 * - transform: applies an expression transform to every literal-carrying
 *   expression of every plan node (filter/having predicates, projections, aggregate
 *   group-by/output, ...) and rebuilds the touched nodes. Used both for parameterization
 *   (SPMPlaceholderBuilder) and for substitution (SPMPlaceholderReplacer).
 * - check: structurally compares a parameterized bind tree against the user's
 *   original tree node by node and expression by expression, extracting the actual user
 *   values for every placeholder id (Level 3).
 * - containsPlaceholder: whether a tree still carries an unbound placeholder
 *   (safety net before a rewritten tree is handed to the analyzer).
 *
 * A Literal that lives inside a SubqueryExpr's own query plan (e.g. the constant of a
 * scalar subquery in the SELECT list) is reached through the expression transform: the
 * expression visitors recurse into the subquery plan with transform. Nested
 * query blocks that appear as plain plan nodes (derived tables, CTE bodies) are reached
 * by the normal children / CTE-alias traversal below.
 */
public final class SPMPlanTreeSupport {

    /** Expression transform used by transform. */
    public interface ExprTransform {
        Expression apply(Expression expr);
    }

    /**
     * An ExprTransform that also changes what its children see while the tree is rebuilt.
     * Only namespace qualification is scope-sensitive: the set of CTE aliases visible in
     * the query text grows when the rebuild enters a CTE body or a CTE's main query, so
     * those two entry points receive a different transform than the surrounding tree.
     * Plain transforms (parameterize / substitute) are not scope-aware and are passed
     * through unchanged.
     */
    private interface ScopedTransform extends ExprTransform {
        /** The transform to use for the main query of a CTE (sees every alias of it). */
        ExprTransform enterCteMain(LogicalCTE<? extends Plan> cte);

        /** The transform to use for the body of alias #aliasIndex (sees the earlier ones). */
        ExprTransform enterCteAlias(LogicalCTE<? extends Plan> cte, int aliasIndex);
    }

    /** Re-descends into expression subquery plans so their hints are stripped as well. */
    private static final ExprTransform HINT_STRIP_EXPR = expr -> {
        if (expr instanceof SubqueryExpr) {
            LogicalPlan subPlan = ((SubqueryExpr) expr).getQueryPlan();
            LogicalPlan stripped = stripSelectHints(subPlan);
            if (stripped != subPlan) {
                return ((SubqueryExpr) expr).withSubquery(stripped);
            }
        }
        return expr;
    };

    private SPMPlanTreeSupport() {
    }

    // ==================== whole-tree transform (parameterize / substitute) ====================

    /**
     * Rebuilds the whole plan tree applying transform to every literal-carrying
     * expression of every plan node (in bottom-up order). Nodes whose type is not
     * explicitly handled are rebuilt through withChildren only (their
     * expressions are kept untouched, which only narrows what can be parameterized /
     * substituted - never breaks the tree).
     *
     * @param plan      the parsed (unbound) plan tree
     * @param transform the expression transform (parameterize or substitute)
     * @return the rebuilt tree (the original instance when nothing changed)
     */
    public static LogicalPlan transform(LogicalPlan plan, ExprTransform transform) {
        Plan result = plan.accept(new TreeTransformer(), transform);
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /** Plan visitor that rebuilds every node, transforming its expressions. */
    private static class TreeTransformer extends PlanVisitor<Plan, ExprTransform> {
        @Override
        public Plan visit(Plan plan, ExprTransform transform) {
            // LogicalUsingJoin.accept() dispatches to this generic visit (PlanVisitor has no
            // using-join overload), so the ASOF MATCH_CONDITION is handled here: it is
            // stored OUTSIDE children() and getExpressions() and would otherwise keep its
            // literal concrete.
            if (plan instanceof LogicalUsingJoin) {
                return visitUsingJoin((LogicalUsingJoin<?, ?>) plan, transform);
            }
            List<Plan> children = plan.children();
            boolean changed = false;
            List<Plan> newChildren = new ArrayList<>(children.size());
            for (Plan child : children) {
                Plan newChild = child.accept(this, transform);
                newChildren.add(newChild);
                if (newChild != child) {
                    changed = true;
                }
            }
            if (!changed) {
                return plan;
            }
            try {
                return plan.withChildren(newChildren);
            } catch (RuntimeException e) {
                // plan type without withChildren -> keep the original children
                return plan;
            }
        }

        @Override
        public Plan visitLogicalCTE(LogicalCTE<? extends Plan> cte, ExprTransform transform) {
            // the main query subtree, then the CTE bodies (kept in aliasQueries, not in
            // children()) - a WHERE inside a CTE definition is transformed too.
            // A ScopedTransform needs per-scope contexts here: the main query sees every
            // alias of this WITH, alias body i only the aliases defined before it.
            ExprTransform mainContext = transform instanceof ScopedTransform
                    ? ((ScopedTransform) transform).enterCteMain(cte) : transform;
            Plan newChild = cte.child(0) == null ? null : cte.child(0).accept(this, mainContext);
            boolean childChanged = newChild != cte.child(0);
            List<LogicalSubQueryAlias<Plan>> newAliasQueries =
                    new ArrayList<>(cte.getAliasQueries().size());
            boolean aliasChanged = false;
            for (int i = 0; i < cte.getAliasQueries().size(); i++) {
                LogicalSubQueryAlias<Plan> aliasQuery = cte.getAliasQueries().get(i);
                ExprTransform aliasContext = transform instanceof ScopedTransform
                        ? ((ScopedTransform) transform).enterCteAlias(cte, i) : transform;
                Plan newAliasQuery = aliasQuery.accept(this, aliasContext);
                newAliasQueries.add((LogicalSubQueryAlias<Plan>) newAliasQuery);
                if (newAliasQuery != aliasQuery) {
                    aliasChanged = true;
                }
            }
            if (!childChanged && !aliasChanged) {
                return cte;
            }
            try {
                return new LogicalCTE<Plan>(cte.isRecursive(), newAliasQueries, newChild);
            } catch (RuntimeException e) {
                return cte;
            }
        }

        @Override
        public Plan visitUnboundRelation(UnboundRelation relation, ExprTransform transform) {
            // of the whole-tree transforms, only namespace qualification rewrites relation
            // references; parameterization / substitution leave them untouched
            return transform instanceof QualifyTransform
                    ? ((QualifyTransform) transform).qualify(relation) : relation;
        }

        @Override
        public Plan visitLogicalProject(LogicalProject<? extends Plan> project,
                ExprTransform transform) {
            Plan child = project.child().accept(this, transform);
            boolean changed = child != project.child();
            List<NamedExpression> newProjects = transformNamed(project.getProjects(), transform);
            if (newProjects != project.getProjects()) {
                changed = true;
            }
            if (!changed) {
                return project;
            }
            return project.withProjectsAndChild(newProjects, child);
        }

        @Override
        public Plan visitLogicalFilter(LogicalFilter<? extends Plan> filter,
                ExprTransform transform) {
            Plan child = filter.child().accept(this, transform);
            boolean changed = child != filter.child();
            Set<Expression> newConjuncts = transformConjuncts(filter.getConjuncts(), transform);
            if (newConjuncts != filter.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return filter;
            }
            return filter.withConjunctsAndChild(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalJoin(LogicalJoin<? extends Plan, ? extends Plan> join,
                ExprTransform transform) {
            // A literal can live directly in a JOIN's ON clause (e.g.
            // "a JOIN b ON a.k = b.k AND a.cat = 'x'"); it is NOT pushed into a Filter
            // on the unbound (raw) tree that parameterization runs over. Without handling
            // the join's own conjuncts here those literals would stay concrete in the
            // parameterized bind tree, so a structurally identical query with a different
            // literal would fail the Level 3 structural match and never hit the baseline.
            Plan left = join.left().accept(this, transform);
            Plan right = join.right().accept(this, transform);
            List<Expression> hash = transformExprs(join.getHashJoinConjuncts(), transform);
            List<Expression> other = transformExprs(join.getOtherJoinConjuncts(), transform);
            List<Expression> mark = transformExprs(join.getMarkJoinConjuncts(), transform);
            boolean changed = left != join.left() || right != join.right()
                    || hash != join.getHashJoinConjuncts()
                    || other != join.getOtherJoinConjuncts()
                    || mark != join.getMarkJoinConjuncts();
            if (!changed) {
                return join;
            }
            return join.withConjunctsChildren(hash, other, mark, left, right,
                    new JoinReorderContext());
        }

        /**
         * Rebuilds an ASOF / USING join, transforming the MATCH_CONDITION. The condition is
         * stored in {@code matchCondition}, OUTSIDE both children() and
         * getExpressions() (which returns the USING slots), so the generic pass would
         * leave its literal concrete on the bind side - the Level 3 match would then
         * compare only the USING slots and accept a user variant with a different
         * temporal boundary, replaying the captured one.
         */
        private Plan visitUsingJoin(LogicalUsingJoin<? extends Plan, ? extends Plan> join,
                ExprTransform transform) {
            Plan left = join.left().accept(this, transform);
            Plan right = join.right().accept(this, transform);
            Optional<Expression> match = join.getMatchCondition();
            Optional<Expression> newMatch = match.isPresent()
                    ? Optional.of(transform.apply(match.get())) : match;
            if (left == join.left() && right == join.right() && newMatch.equals(match)) {
                return join;
            }
            return new LogicalUsingJoin<>(join.getJoinType(), left, right,
                    join.getUsingSlots(), newMatch, join.getDistributeHint());
        }

        @Override
        public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having, ExprTransform transform) {
            Plan child = having.child().accept(this, transform);
            boolean changed = child != having.child();
            Set<Expression> newConjuncts = transformConjuncts(having.getConjuncts(), transform);
            if (newConjuncts != having.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return having;
            }
            return having.withConjunctsAndChild(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalAggregate(LogicalAggregate<? extends Plan> aggregate,
                ExprTransform transform) {
            Plan child = aggregate.child().accept(this, transform);
            boolean changed = child != aggregate.child();
            List<Expression> newGroupBy = new ArrayList<>(aggregate.getGroupByExpressions().size());
            for (Expression groupByExpr : aggregate.getGroupByExpressions()) {
                Expression newExpr = transform.apply(groupByExpr);
                newGroupBy.add(newExpr);
                if (newExpr != groupByExpr) {
                    changed = true;
                }
            }
            List<NamedExpression> newOutput = transformNamed(aggregate.getOutputExpressions(),
                    transform);
            if (newOutput != aggregate.getOutputExpressions()) {
                changed = true;
            }
            if (!changed) {
                return aggregate;
            }
            return aggregate.withChildGroupByAndOutput(newGroupBy, newOutput, child);
        }

        @Override
        public Plan visitUnboundOneRowRelation(UnboundOneRowRelation oneRow,
                ExprTransform transform) {
            List<NamedExpression> newProjects = transformNamed(oneRow.getProjects(), transform);
            if (newProjects == oneRow.getProjects()) {
                return oneRow;
            }
            return new UnboundOneRowRelation(oneRow.getRelationId(), newProjects);
        }

        @Override
        public Plan visitLogicalOneRowRelation(LogicalOneRowRelation oneRow,
                ExprTransform transform) {
            List<NamedExpression> newProjects = transformNamed(oneRow.getProjects(), transform);
            if (newProjects == oneRow.getProjects()) {
                return oneRow;
            }
            return new LogicalOneRowRelation(oneRow.getRelationId(), newProjects);
        }

        @Override
        public Plan visitLogicalSort(LogicalSort<? extends Plan> sort,
                ExprTransform transform) {
            // ORDER BY keys can carry literals (e.g. substr(w_warehouse_name, 1, 20) in a
            // TPCDS query): they must be parameterized / substituted like any other
            // expression, otherwise the rewritten tree would keep the bind-side order key
            // while group-by / project got the user's value -> analyze error / wrong order.
            //
            // EXCEPTION: an ORDER BY whose key is a BARE integer literal is an ORDINAL
            // (position reference, e.g. ORDER BY 1, 2), resolved by BindExpression during
            // analyze. It must NOT be parameterized: parameterizing would turn the ordinal
            // into a placeholder function and the optimizer would then sort by the
            // constant value (projecting it as an extra column) instead of by the
            // referenced output column - silently changing the query's row order.
            Plan child = sort.child().accept(this, transform);
            boolean changed = child != sort.child();
            List<OrderKey> orderKeys = sort.getOrderKeys();
            List<OrderKey> newOrderKeys = new ArrayList<>(orderKeys.size());
            for (OrderKey orderKey : orderKeys) {
                Expression keyExpr = orderKey.getExpr();
                Expression newExpr = keyExpr instanceof IntegerLikeLiteral
                        ? keyExpr : transform.apply(keyExpr);
                if (newExpr != keyExpr) {
                    changed = true;
                }
                newOrderKeys.add(orderKey.withExpression(newExpr));
            }
            if (!changed) {
                return sort;
            }
            return sort.withOrderKeysAndChild(newOrderKeys, child);
        }

        @Override
        public Plan visitLogicalQualify(LogicalQualify<? extends Plan> qualify,
                ExprTransform transform) {
            // QUALIFY <window-predicate>: the predicates live on the LogicalQualify node
            // itself (e.g. "row_number() over (...) = 1") and would otherwise keep their
            // literals concrete in the parameterized tree, so a user query with a
            // different QUALIFY threshold never matches the baseline.
            Plan child = qualify.child().accept(this, transform);
            boolean changed = child != qualify.child();
            Set<Expression> newConjuncts = transformConjuncts(qualify.getConjuncts(), transform);
            if (newConjuncts != qualify.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return qualify;
            }
            return new LogicalQualify<Plan>(newConjuncts, child);
        }

        @Override
        public Plan visitLogicalGenerate(LogicalGenerate<? extends Plan> generate,
                ExprTransform transform) {
            // LATERAL VIEW / UNNEST: the generator arguments (e.g.
            // explode(split('a,b,c', ','))) decide which rows/values the generator emits -
            // they are NOT predicates (no WHERE-like semantics), so a different argument
            // means a different query. They are therefore intentionally left concrete and
            // never parameterized: SPM only matches a user query carrying the exact same
            // generator argument. The conjuncts field is the only WHERE-like part of this
            // node (empty at parse time), so it is still transformed for future-proofing.
            Plan child = generate.child().accept(this, transform);
            boolean changed = child != generate.child();
            List<Expression> newConjuncts = transformExprs(generate.getConjuncts(), transform);
            if (newConjuncts != generate.getConjuncts()) {
                changed = true;
            }
            if (!changed) {
                return generate;
            }
            return new LogicalGenerate<Plan>(generate.getGenerators(),
                    generate.getGeneratorOutput(), generate.getExpandColumnAlias(),
                    newConjuncts, child);
        }

        @Override
        public Plan visitLogicalRepeat(LogicalRepeat<? extends Plan> repeat,
                ExprTransform transform) {
            // GROUPING SETS / ROLLUP / CUBE.
            // The outputExpressions are the SELECT-list items of the query block (constants
            // like "SELECT 5 AS tag" live here) and need parameterizes select items
            // like any other expression -> parameterize them so a user value change can
            // match and be substituted. The groupingSets are the grouping keys (GROUP BY
            // columns); we does not traverse them -> they stay concrete.
            Plan child = repeat.child().accept(this, transform);
            boolean changed = child != repeat.child();
            List<NamedExpression> newOutput = transformNamed(repeat.getOutputExpressions(),
                    transform);
            if (newOutput != repeat.getOutputExpressions()) {
                changed = true;
            }
            if (!changed) {
                return repeat;
            }
            // NOTE: the overload that takes the grouping-id VALUES rebuilds through a
            // constructor that forces withInProjection=true, which flips toDigest() from
            // "SELECT <outputs> FROM <child>" to "<child>". A rebuild must keep the
            // parse-time rendering state: only the qualification below changes a digest,
            // not the fact that a node was rebuilt on the way. The 4-arg overload reuses
            // the existing grouping-id values and keeps withInProjection as-is.
            return repeat.withGroupingIdValues(repeat.getGroupingSets(), newOutput,
                    repeat.getGroupingId().orElse(null), child);
        }
    }

    /**
     * Transforms an ordinary (non-Named) expression list in place-free way; returns the
     * original list when no element changed.
     *
     * Used for expression LISTS that are not SELECT items and carry no output name/alias
     * of their own, e.g. a join's conjuncts or a LogicalGenerate's conjuncts. Example -
     * the ON / residual predicates of
     *
     *     SELECT ... FROM t1 JOIN t2 ON t1.k = t2.k WHERE t2.a > 100 AND t2.b = 'x'
     *
     * are parameterized one conjunct at a time, so a user query whose literal differs
     * (e.g. t2.a > 200) still structurally matches the baseline. The list keeps its
     * order (no Set normalization - join conjunct order is meaningful).
     *
     * @param expressions the expression list to transform
     * @param transform   the per-expression transform (parameterize or substitute)
     * @return the transformed list, or the original list instance when nothing changed
     */
    private static List<Expression> transformExprs(List<Expression> expressions,
            ExprTransform transform) {
        boolean changed = false;
        List<Expression> newExpressions = new ArrayList<>(expressions.size());
        for (Expression expression : expressions) {
            Expression transformed = transform.apply(expression);
            newExpressions.add(transformed);
            if (transformed != expression) {
                changed = true;
            }
        }
        return changed ? newExpressions : expressions;
    }

    /**
     * Transforms a NamedExpression list (SELECT items / aggregate outputs) in place-free
     * way; returns the original list when no element changed.
     *
     * A NamedExpression is an expression paired with an output name/alias (e.g.
     * Alias(expr, name)). The transform runs over the WHOLE NamedExpression - including
     * the alias name itself when it is derived from the expression text - so its inner
     * literals are parameterized while the output column name is kept consistent with
     * the (also parameterized) bind side. Example - the SELECT list of
     *
     *     SELECT l_returnflag, sum(l_quantity * 2) AS total FROM lineitem
     *     GROUP BY l_returnflag
     *
     * is transformed per item: the plain column l_returnflag is unchanged, while the
     * aggregate item's constant 2 becomes a placeholder
     * (sum(l_quantity * SpmConstVar(1, 2)) AS total), so a similar user query with a
     * different multiplier still matches. Only a transform result that stays a
     * NamedExpression is adopted (an Alias must not collapse into a bare expression,
     * which would drop the output name).
     *
     * @param expressions the NamedExpression list (projects / aggregate outputs) to
     *                    transform
     * @param transform   the per-expression transform (parameterize or substitute)
     * @return the transformed list, or the original list instance when nothing changed
     */
    private static List<NamedExpression> transformNamed(List<NamedExpression> expressions,
            ExprTransform transform) {
        boolean changed = false;
        List<NamedExpression> newExpressions = new ArrayList<>(expressions.size());
        for (NamedExpression expression : expressions) {
            Expression transformed = transform.apply(expression);
            if (transformed instanceof NamedExpression && transformed != expression) {
                newExpressions.add((NamedExpression) transformed);
                changed = true;
            } else {
                newExpressions.add(expression);
            }
        }
        return changed ? newExpressions : expressions;
    }

    /**
     * Transforms a conjunct set in a deterministic order; returns the original set when
     * no element changed. The conjuncts of a filter / having are a Set whose iteration
     * order is not stable across separately parsed trees, so they are ordered by SQL
     * text before transforming: the bind tree and the (separately parsed) plan tree of
     * one baseline then parameterize their conjuncts in the same order and the
     * placeholder ids stay aligned (no cross-tree id skew for identical conjuncts).
     */
    private static Set<Expression> transformConjuncts(Set<Expression> conjuncts,
            ExprTransform transform) {
        List<Expression> ordered = new ArrayList<>(conjuncts);
        ordered.sort(Comparator.comparing(Expression::toSql));
        boolean changed = false;
        Set<Expression> newConjuncts = new LinkedHashSet<>(conjuncts.size());
        for (Expression conjunct : ordered) {
            Expression transformed = transform.apply(conjunct);
            newConjuncts.add(transformed);
            if (transformed != conjunct) {
                changed = true;
            }
        }
        return changed ? newConjuncts : conjuncts;
    }

    // ==================== namespace qualification (creation catalog / database) ====================

    /**
     * Returns a copy of the tree where every UNQUALIFIED relation reference (nameParts.size()
     * == 1) that binds to a BASE TABLE is prefixed with the given catalog / database, so the
     * SPM digest / hash key of "FROM t" depends on the query's effective namespace. The copy
     * is only used to compute the matching key / compare against a baseline - it is never
     * analyzed, optimized or executed.
     *
     * Without this, "SELECT ... FROM t" has the same key under db1 and db2: a baseline
     * captured under db1 would silently match the same text executed under db2 and - because
     * the frozen planSql is fully qualified - keep executing against db1.t.
     *
     * A reference to a CTE alias is NOT prefixed: its binding comes from the WITH clause of
     * the query itself and therefore means the same thing in every database (prefixing it
     * made "WITH c AS (...) SELECT * FROM c" match only in the database it was created in).
     * Whether a single-part name is a CTE reference is decided with the analyzer's scoping
     * rules (AnalyzeCTE): an alias body sees the aliases defined before it, plus itself when
     * it is a real recursive CTE (WITH RECURSIVE plus a self-reference in its body); a CTE's
     * main query sees every alias of that WITH; nested WITH nodes extend the enclosing
     * scope; expression subqueries inherit the scope of the point they appear in
     * (SubExprAnalyzer). A name that is NOT a visible alias - including a forward reference
     * to a later alias and a self reference under a plain (non-RECURSIVE) WITH, both of which
     * bind as base tables - is always prefixed, so this can only narrow the match key, never
     * make a base-table reference namespace-independent.
     *
     * @param plan    the parsed (unbound) tree
     * @param catalog the effective catalog, may be null / empty (then only the db is prefixed)
     * @param db      the effective database; when null / empty the tree is returned as-is
     * @return the qualified tree (a rebuilt copy; the argument is not modified)
     */
    public static LogicalPlan namespaceQualified(LogicalPlan plan, String catalog, String db) {
        boolean hasCatalog = catalog != null && !catalog.isEmpty();
        boolean hasDb = db != null && !db.isEmpty();
        // A TWO-part name (db.t) is relative to the current CATALOG, not to the current
        // database, so it must be prefixed whenever the catalog is known - a session can
        // switch to cat1 WITHOUT a USE db, and an unprefixed db.t would key the same text
        // under cat2 to the same digest while the frozen SQL still reads cat1.db.t. Only
        // when NEITHER the catalog nor the db is known can nothing be made
        // namespace-independent.
        if (plan == null || (!hasCatalog && !hasDb)) {
            return plan;
        }
        Plan result = plan.accept(new TreeTransformer(), new QualifyTransform(catalog, db));
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /**
     * Namespace-qualification scope: prefixes single-part base-table references with the
     * effective [catalog, db] and keeps references to the CTE aliases visible at the current
     * point verbatim. Immutable: entering a CTE scope returns a new instance whose visible
     * set is the enclosing one plus the aliases visible in that scope.
     */
    private static final class QualifyTransform implements ScopedTransform {
        private final String catalog;
        private final String db;
        /** normalized names of the CTE aliases visible at the current point */
        private final Set<String> visibleCtes;

        QualifyTransform(String catalog, String db) {
            this(catalog, db, Collections.emptySet());
        }

        private QualifyTransform(String catalog, String db, Set<String> visibleCtes) {
            this.catalog = catalog;
            this.db = db;
            this.visibleCtes = visibleCtes;
        }

        @Override
        public Expression apply(Expression expr) {
            return qualifyExpression(expr, this);
        }

        @Override
        public ExprTransform enterCteMain(LogicalCTE<? extends Plan> cte) {
            Set<String> extended = new LinkedHashSet<>(visibleCtes);
            for (LogicalSubQueryAlias<Plan> alias : cte.getAliasQueries()) {
                extended.add(normalizeCteName(alias.getAlias()));
            }
            return new QualifyTransform(catalog, db, Collections.unmodifiableSet(extended));
        }

        @Override
        public ExprTransform enterCteAlias(LogicalCTE<? extends Plan> cte, int aliasIndex) {
            List<LogicalSubQueryAlias<Plan>> aliases = cte.getAliasQueries();
            Set<String> extended = new LinkedHashSet<>(visibleCtes);
            for (int i = 0; i < aliasIndex; i++) {
                extended.add(normalizeCteName(aliases.get(i).getAlias()));
            }
            // a self reference binds to the WITH clause only in a real recursive CTE;
            // under a plain WITH it is an ordinary base-table reference
            LogicalSubQueryAlias<Plan> alias = aliases.get(aliasIndex);
            if (cte.isRecursive() && alias.isRecursiveCte()) {
                extended.add(normalizeCteName(alias.getAlias()));
            }
            return new QualifyTransform(catalog, db, Collections.unmodifiableSet(extended));
        }

        /** Prefixes a one- or two-part relation with the effective namespace. */
        Plan qualify(UnboundRelation relation) {
            List<String> parts = relation.getNameParts();
            if (parts.size() > 2) {
                return relation; // fully qualified (catalog.db.table): nothing to add
            }
            if (parts.size() == 1 && isVisibleCte(parts.get(0))) {
                return relation; // bound by a WITH clause, not a base-table reference
            }
            // A one-part name is relative to the current db, but a TWO-part name is only
            // relative to the current CATALOG ("db.t" means current_catalog.db.t):
            // leaving "db.t" verbatim would let a baseline created in cat1 match the
            // same text executed in cat2, after which the frozen fully-qualified replay
            // keeps reading cat1.db.t. Only three-part names are complete.
            List<String> qualified = new ArrayList<>(3);
            if (parts.size() == 1) {
                if (db == null || db.isEmpty()) {
                    // A one-part name needs the DATABASE to become absolute; prefixing only
                    // the catalog would turn the table name into a database name.
                    return relation;
                }
                if (catalog != null && !catalog.isEmpty()) {
                    qualified.add(catalog);
                }
                qualified.add(db);
            } else {
                // two-part name: the catalog completes it
                if (catalog == null || catalog.isEmpty()) {
                    return relation;
                }
                qualified.add(catalog);
            }
            qualified.addAll(parts);
            try {
                return copyWithNameParts(relation, qualified);
            } catch (RuntimeException e) {
                return relation;
            }
        }

        /**
         * Rebuilds the relation with new name parts while preserving EVERY scan modifier
         * (partition list, tablet ids, hints, sample, index, scan params, snapshot):
         * dropping them here would hide a PARTITION(...) / TABLESAMPLE / FOR VERSION
         * selection from the Level 3 comparison and allow a baseline captured under a
         * different selection to match.
         */
        private static UnboundRelation copyWithNameParts(UnboundRelation relation,
                List<String> nameParts) {
            return new UnboundRelation(relation.getRelationId(), nameParts,
                    relation.getPartNames(), relation.isTempPart(), relation.getTabletIds(),
                    relation.getHints(), relation.getTableSample(), relation.getIndexName(),
                    relation.getScanParams(), relation.getIndexInSqlString(),
                    relation.getTableSnapshot());
        }

        private boolean isVisibleCte(String name) {
            return !visibleCtes.isEmpty() && visibleCtes.contains(normalizeCteName(name));
        }
    }

    /** Mirrors the analyzer's CTE name comparison (CTEContext.findCTEContext). */
    private static String normalizeCteName(String name) {
        int lowerCaseTableNames = GlobalVariable.lowerCaseTableNames;
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getCurrentCatalog() != null) {
            lowerCaseTableNames = ctx.getCurrentCatalog().getLowerCaseTableNames();
        }
        return lowerCaseTableNames != 0 ? name.toLowerCase(Locale.ROOT) : name;
    }

    /**
     * Qualifies the relations of every subquery plan owned by an expression, recursively
     * (IN / scalar / EXISTS subqueries anywhere in the tree). The subquery inherits the
     * qualification scope of the point it appears in, i.e. the CTE aliases visible there.
     */
    private static Expression qualifyExpression(Expression expr, QualifyTransform transform) {
        if (expr instanceof SubqueryExpr) {
            LogicalPlan subPlan = ((SubqueryExpr) expr).getQueryPlan();
            Plan qualified = subPlan.accept(new TreeTransformer(), transform);
            if (qualified instanceof LogicalPlan && qualified != subPlan) {
                return ((SubqueryExpr) expr).withSubquery((LogicalPlan) qualified);
            }
            return expr;
        }
        if (expr.children().isEmpty()) {
            return expr;
        }
        boolean changed = false;
        List<Expression> newChildren = new ArrayList<>(expr.children().size());
        for (Expression child : expr.children()) {
            Expression newChild = qualifyExpression(child, transform);
            newChildren.add(newChild);
            if (newChild != child) {
                changed = true;
            }
        }
        return changed ? expr.withChildren(newChildren) : expr;
    }

    // ==================== hint stripping (in-memory fallback tree) ====================

    /**
     * Removes EVERY LogicalSelectHint from a plan tree: the root block, nested query
     * blocks, CTE bodies and expression subqueries. The frozen-text replay path re-parses
     * its planSql INCLUDING the hints deliberately; the in-memory fallback tree must not
     * re-apply the BASELINE's captured SET_VAR on top of a user query that came with
     * different session variables. A root-only peel let an INNER hint survive (e.g. a
     * plan-side SET_VAR(time_zone='+08:00') inside a scalar subquery) and the hint was
     * then applied during ordinary replay analysis, although the matching user query is
     * hint-free and runs under -08:00 - from_unixtime returned different values.
     *
     * @param plan the parameterized fallback tree
     * @return the tree without any hint wrapper (the original instance when there was none)
     */
    public static LogicalPlan stripSelectHints(LogicalPlan plan) {
        if (plan == null) {
            return null;
        }
        Plan result = plan.accept(new HintStripper(), HINT_STRIP_EXPR);
        return result instanceof LogicalPlan ? (LogicalPlan) result : plan;
    }

    /** TreeTransformer that DROPS the LogicalSelectHint wrapper instead of rebuilding it. */
    private static class HintStripper extends TreeTransformer {
        @Override
        public Plan visit(Plan plan, ExprTransform transform) {
            if (plan instanceof LogicalSelectHint) {
                Plan child = plan.child(0);
                return child == null ? plan : child.accept(this, transform);
            }
            return super.visit(plan, transform);
        }
    }

    // ==================== replay-time context expression detection ====================

    /**
     * Whether the tree contains an expression whose value belongs to the CREATOR's
     * replay-time context: a session / user variable (@v) or current_user(),
     * session_user(), database(), connection_id(). Such leaves survive
     * parameterization, the creator-context optimization then resolves them to LITERALS,
     * and the frozen SQL persists the CREATOR's value - while matching still compares the
     * ORIGINAL unbound bind tree, so a global baseline (e.g.
     * {@code SELECT current_user(), k FROM t WHERE k = 1}) could match another user's
     * query and return the creator's identity.
     *
     * @param plan the parsed (unbound) tree
     * @return true when such an expression is found anywhere (including subqueries and an
     *         ASOF join's out-of-band MATCH_CONDITION)
     */
    public static boolean containsReplayContextExpression(LogicalPlan plan) {
        return plan.accept(new ReplayContextScanVisitor(), null);
    }

    /** Whether an expression tree contains a replay-time context expression. */
    public static boolean containsReplayContextExpression(Expression expr) {
        if (expr instanceof Variable || expr instanceof CurrentUser || expr instanceof SessionUser
                || expr instanceof Database || expr instanceof ConnectionId) {
            return true;
        }
        if (expr instanceof UnboundFunction) {
            // the same functions can reach the unbound tree as a plain function call
            // (e.g. "current_user()" with parentheses parses as UnboundFunction)
            String name = ((UnboundFunction) expr).getName();
            if (name != null) {
                switch (name.toLowerCase(Locale.ROOT)) {
                    case "current_user":
                    case "session_user":
                    case "database":
                    case "connection_id":
                        return true;
                    default:
                        break;
                }
            }
        }
        if (expr instanceof SubqueryExpr) {
            if (containsReplayContextExpression(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsReplayContextExpression(child)) {
                return true;
            }
        }
        return false;
    }

    /** Plan visitor that scans every node's expressions (and subquery plans). */
    private static class ReplayContextScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalUsingJoin) {
                // matchCondition lives outside children() and getExpressions()
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) plan).getMatchCondition();
                if (matchCondition.isPresent()
                        && containsReplayContextExpression(matchCondition.get())) {
                    return true;
                }
            }
            for (Expression expr : plan.getExpressions()) {
                if (containsReplayContextExpression(expr)) {
                    return true;
                }
            }
            for (Plan child : plan.children()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalCTE(LogicalCTE<? extends Plan> cte, Void context) {
            if (cte.child(0) != null && cte.child(0).accept(this, context)) {
                return true;
            }
            for (LogicalSubQueryAlias<Plan> aliasQuery : cte.getAliasQueries()) {
                if (aliasQuery.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    // ==================== whole-tree placeholder detection ====================

    /**
     * Whether the plan tree (including filters/having/projections and every subquery
     * plan reachable from an expression) still contains an unbound placeholder.
     *
     * @param plan the plan tree to scan
     * @return true when a SpmConstVar / SpmConstList is found anywhere
     */
    public static boolean containsPlaceholder(LogicalPlan plan) {
        return plan.accept(new PlaceholderScanVisitor(), null);
    }

    /** Plan visitor that scans every node's expressions (and subquery plans) for placeholders. */
    private static class PlaceholderScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalUsingJoin) {
                // The USING join's matchCondition is OUT OF BAND: it is neither a child
                // nor in getExpressions(). A bind/plan pair whose ASOF boundary differs
                // only in an interval literal leaves a plan-only id in this field; the
                // in-memory fallback substitutes the original LogicalUsingJoin, so a
                // visitor that only walks USING slots + children would return an invalid
                // tree (an unresolved placeholder id) to analysis.
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) plan).getMatchCondition();
                if (matchCondition.isPresent() && containsPlaceholder(matchCondition.get())) {
                    return true;
                }
            }
            for (Expression expr : plan.getExpressions()) {
                if (containsPlaceholder(expr)) {
                    return true;
                }
            }
            for (Plan child : plan.children()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalCTE(LogicalCTE<? extends Plan> cte, Void context) {
            if (cte.child(0) != null && cte.child(0).accept(this, context)) {
                return true;
            }
            for (LogicalSubQueryAlias<Plan> aliasQuery : cte.getAliasQueries()) {
                if (aliasQuery.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Whether an expression tree (recursing into subquery plans) contains a placeholder.
     */
    public static boolean containsPlaceholder(Expression expr) {
        if (expr instanceof SpmConstVar || expr instanceof SpmConstList) {
            return true;
        }
        if (expr instanceof SubqueryExpr) {
            if (containsPlaceholder(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsPlaceholder(child)) {
                return true;
            }
        }
        return false;
    }

    // ==================== frozen-tree placeholder detection (M3) ====================

    /**
     * Whether a plan tree (re-parsed from the frozen planSql) still carries an
     * unsubstituted placeholder call (_spm_const_var(id) / _spm_const_list(id) as a raw
     * UnboundFunction). Such a call would reach the analyzer as an unregistered function
     * and fail, so the rewrite must reject the tree instead of returning it.
     *
     * @param plan the frozen (re-parsed) plan tree to scan
     * @return true when an unsubstituted placeholder call is found anywhere
     */
    public static boolean containsFrozenPlaceholder(Plan plan) {
        return plan.accept(new FrozenPlaceholderScanVisitor(), null);
    }

    /** Plan visitor that scans every node's expressions (and subquery plans) for
     * unsubstituted frozen-tree placeholder calls. */
    private static class FrozenPlaceholderScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
            if (plan instanceof LogicalUsingJoin) {
                // Same out-of-band field as PlaceholderScanVisitor: an unresolved
                // placeholder id inside the ASOF boundary must reject the fallback tree.
                Optional<Expression> matchCondition =
                        ((LogicalUsingJoin<?, ?>) plan).getMatchCondition();
                if (matchCondition.isPresent() && containsFrozenPlaceholder(matchCondition.get())) {
                    return true;
                }
            }
            for (Expression expr : plan.getExpressions()) {
                if (containsFrozenPlaceholder(expr)) {
                    return true;
                }
            }
            for (Plan child : plan.children()) {
                if (child.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public Boolean visitLogicalCTE(LogicalCTE<? extends Plan> cte, Void context) {
            if (cte.child(0) != null && cte.child(0).accept(this, context)) {
                return true;
            }
            for (LogicalSubQueryAlias<Plan> aliasQuery : cte.getAliasQueries()) {
                if (aliasQuery.accept(this, context)) {
                    return true;
                }
            }
            return false;
        }
    }

    /** Whether an expression tree contains an unsubstituted frozen-tree placeholder call. */
    public static boolean containsFrozenPlaceholder(Expression expr) {
        if (SPMFrozenTreeReplacer.isUnsubstitutedPlaceholder(expr)) {
            return true;
        }
        if (expr instanceof SubqueryExpr) {
            if (containsFrozenPlaceholder(((SubqueryExpr) expr).getQueryPlan())) {
                return true;
            }
        }
        for (Expression child : expr.children()) {
            if (containsFrozenPlaceholder(child)) {
                return true;
            }
        }
        return false;
    }

    // ==================== whole-tree structural check (Level 3) ====================

    /**
     * Compares a parameterized bind tree with the user's original tree node by node and
     * expression by expression. Placeholder values are extracted from the user side into
     * placeholderValues (a repeated id must resolve to the same user value).
     *
     * @param bindPlan          the parameterized bind tree (contains placeholders)
     * @param userPlan          the user's original tree (contains real literals)
     * @param placeholderValues placeholder id -> user value (filled in here)
     * @return whether the two trees match
     */
    public static boolean check(LogicalPlan bindPlan, LogicalPlan userPlan,
            Map<Long, Expression> placeholderValues) {
        return checkPlan(bindPlan, userPlan, placeholderValues, false);
    }

    /**
     * Level 3 check for one subquery-EXPRESSION plan pair. Inside a subquery's plan the
     * LIMIT / OFFSET fields are compared EXACTLY (see checkPlan): they are plain long
     * fields that mergeLimits cannot merge (the subquery plan does not sit on a
     * plan-child path of the main tree), so a "subquery LIMIT 2" query must not match a
     * baseline captured with a different subquery limit and replay the captured value.
     */
    public static boolean checkSubqueryPlan(LogicalPlan bindPlan, LogicalPlan userPlan,
            Map<Long, Expression> placeholderValues) {
        return checkPlan(bindPlan, userPlan, placeholderValues, true);
    }

    /** Node-by-node recursive structural check. */
    private static boolean checkPlan(Plan bind, Plan user, Map<Long, Expression> placeholderValues,
            boolean insideSubquery) {
        if (bind.getClass() != user.getClass()) {
            return false;
        }
        // LIMIT / OFFSET are plain long fields, not expressions, so the generic node check
        // cannot see them. A top-level LIMIT is adopted from the user query later
        // (mergeLimits); a LIMIT inside a subquery expression's plan is NOT reachable by
        // that merge, so it must take part in the match exactly - otherwise a query with
        // "subquery LIMIT 2" would replay a baseline captured with "subquery LIMIT 1"
        // and return a truncated result.
        if (insideSubquery && bind instanceof LogicalLimit && user instanceof LogicalLimit) {
            if (((LogicalLimit<?>) bind).getLimit() != ((LogicalLimit<?>) user).getLimit()
                    || ((LogicalLimit<?>) bind).getOffset() != ((LogicalLimit<?>) user).getOffset()) {
                return false;
            }
        }
        // compare this node's expressions first (bind side is parameterized)
        if (!checkNodeExpressions(bind, user, placeholderValues)) {
            return false;
        }
        List<Plan> bindChildren = bind.children();
        List<Plan> userChildren = user.children();
        if (bindChildren.size() != userChildren.size()) {
            return false;
        }
        for (int i = 0; i < bindChildren.size(); i++) {
            // A derived table (LogicalSubQueryAlias) opens a nested query block: its own
            // LIMIT / OFFSET cannot rely on the positional merge (mergeLimits gives up on
            // a class mismatch - e.g. a frozen join order that differs from the user's -
            // and would silently keep the captured slice), so nested limits are part of
            // the exact match (see the LIMIT check above).
            boolean childInsideSubquery = insideSubquery || bind instanceof LogicalSubQueryAlias;
            if (!checkPlan(bindChildren.get(i), userChildren.get(i), placeholderValues,
                    childInsideSubquery)) {
                return false;
            }
        }
        // CTE bodies are not children() - compare them explicitly
        if (bind instanceof LogicalCTE && user instanceof LogicalCTE) {
            LogicalCTE<?> bindCte = (LogicalCTE<?>) bind;
            LogicalCTE<?> userCte = (LogicalCTE<?>) user;
            // WITH RECURSIVE changes how a self-reference binds (work table vs base
            // table): the same syntax against a same-named real table means different
            // queries, so the recursion flag is part of the identity
            if (bindCte.isRecursive() != userCte.isRecursive()) {
                return false;
            }
            List<LogicalSubQueryAlias<Plan>> bindAliases = bindCte.getAliasQueries();
            List<LogicalSubQueryAlias<Plan>> userAliases = userCte.getAliasQueries();
            if (bindAliases.size() != userAliases.size()) {
                return false;
            }
            for (int i = 0; i < bindAliases.size(); i++) {
                // CTE bodies are nested query blocks: their LIMIT / OFFSET are compared
                // exactly (the positional LIMIT merge cannot reach them reliably)
                if (!checkPlan(bindAliases.get(i), userAliases.get(i), placeholderValues, true)) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Compares the expressions of one node pair. Set-typed conjuncts (filter / having)
     * are ordered deterministically before pairing; list-typed expressions (projections,
     * group-by, ...) keep their parse order.
     */
    private static boolean checkNodeExpressions(Plan bind, Plan user,
            Map<Long, Expression> placeholderValues) {
        // Base-table relation: UnboundRelation.toDigest() omits partition names / sample /
        // hints and normalizes tablet / snapshot / scan-parameter values, and the
        // namespace is only covered by the L1/L2 digest pre-filter. Compare every
        // non-parameterizable scan identity field exactly here, so "t PARTITION(p1)" can
        // never match "PARTITION(p2)" (the frozen replay would keep reading p1). The
        // relation NAMES are deliberately not compared at this level: the stored bind
        // tree is the raw parse while the user side is namespace-qualified; name
        // equality is enforced by the digest pre-filter.
        if (bind instanceof UnboundRelation && user instanceof UnboundRelation) {
            return sameScanIdentity((UnboundRelation) bind, (UnboundRelation) user);
        }
        // Table-valued function: UnboundTVFRelation.toDigest() reduces every call to
        // "fn(?)" and the node has no expressions/children, so the generic comparison
        // cannot distinguish the properties. numbers('number'='100') must never match a
        // baseline captured with numbers('number'='10'): the frozen replay would run the
        // captured properties. Compare the function name and the property map exactly.
        if (bind instanceof UnboundTVFRelation && user instanceof UnboundTVFRelation) {
            UnboundTVFRelation bindTvf = (UnboundTVFRelation) bind;
            UnboundTVFRelation userTvf = (UnboundTVFRelation) user;
            return bindTvf.getFunctionName().equals(userTvf.getFunctionName())
                    && Objects.equals(bindTvf.getProperties(), userTvf.getProperties());
        }
        // Positional subquery / CTE column aliases: LogicalSubQueryAlias does not expose
        // them through getExpressions() (toDigest merely computes the joined alias list
        // without appending it), so s(x, y) and s(y, x) over the same derived table would
        // otherwise match - replaying the captured column mapping and returning the wrong
        // column. Compare the alias lists explicitly.
        if (bind instanceof LogicalSubQueryAlias && user instanceof LogicalSubQueryAlias) {
            Optional<List<String>> bindAliases =
                    ((LogicalSubQueryAlias<?>) bind).getColumnAliases();
            Optional<List<String>> userAliases =
                    ((LogicalSubQueryAlias<?>) user).getColumnAliases();
            if (!Objects.equals(bindAliases, userAliases)) {
                return false;
            }
        }
        // MARK join: LogicalJoin uses one class for both plain and MARK joins; neither
        // the mark flag nor the mark conjuncts are reachable through the generic
        // expression comparison (toDigest omits markJoinSlotReference). CROSS MARK JOIN
        // must never match a plain CROSS JOIN: the frozen replay would keep one row per
        // left row instead of the cartesian multiplicity. Compare the mark state and the
        // mark conjuncts explicitly.
        if (bind instanceof LogicalJoin && user instanceof LogicalJoin) {
            LogicalJoin<?, ?> bindJoin = (LogicalJoin<?, ?>) bind;
            LogicalJoin<?, ?> userJoin = (LogicalJoin<?, ?>) user;
            if (bindJoin.isMarkJoin() != userJoin.isMarkJoin()) {
                return false;
            }
            // The MARK_SLOT name is the user-visible result header of a "SELECT *" over a
            // mark join: neither toDigest nor getExpressions exposes it, so without this
            // comparison MARK_SLOT m1 and MARK_SLOT m2 match and the frozen replay would
            // restore the captured header instead of the requested one. Identifiers are
            // matched case-insensitively (the same rule the analyzer applies).
            Optional<MarkJoinSlotReference> bindMarkSlot = bindJoin.getMarkJoinSlotReference();
            Optional<MarkJoinSlotReference> userMarkSlot = userJoin.getMarkJoinSlotReference();
            if (bindMarkSlot.isPresent() != userMarkSlot.isPresent()) {
                return false;
            }
            if (bindMarkSlot.isPresent() && !bindMarkSlot.get().getName()
                    .equalsIgnoreCase(userMarkSlot.get().getName())) {
                return false;
            }
            List<Expression> bindMark = bindJoin.getMarkJoinConjuncts();
            List<Expression> userMark = userJoin.getMarkJoinConjuncts();
            if (bindMark.size() != userMark.size()) {
                return false;
            }
            for (int i = 0; i < bindMark.size(); i++) {
                if (!checkExpression(bindMark.get(i), userMark.get(i), placeholderValues)) {
                    return false;
                }
            }
        }
        // USING / ASOF join: getExpressions() returns the USING slots only, so the
        // MATCH_CONDITION (a temporal boundary on ASOF joins) would never take part in the
        // match - a user variant with a different boundary would replay the captured one
        // and select another right-side row. Compare the optional condition explicitly.
        if (bind instanceof LogicalUsingJoin && user instanceof LogicalUsingJoin) {
            Optional<Expression> bindMatch = ((LogicalUsingJoin<?, ?>) bind).getMatchCondition();
            Optional<Expression> userMatch = ((LogicalUsingJoin<?, ?>) user).getMatchCondition();
            if (bindMatch.isPresent() != userMatch.isPresent()) {
                return false;
            }
            if (bindMatch.isPresent()
                    && !checkExpression(bindMatch.get(), userMatch.get(), placeholderValues)) {
                return false;
            }
        }
        if (bind instanceof LogicalFilter || bind instanceof LogicalHaving) {
            Set<Expression> bindConjuncts;
            Set<Expression> userConjuncts;
            if (bind instanceof LogicalFilter) {
                bindConjuncts = ((LogicalFilter<?>) bind).getConjuncts();
                userConjuncts = ((LogicalFilter<?>) user).getConjuncts();
            } else {
                bindConjuncts = ((LogicalHaving<?>) bind).getConjuncts();
                userConjuncts = ((LogicalHaving<?>) user).getConjuncts();
            }
            if (bindConjuncts.size() != userConjuncts.size()) {
                return false;
            }
            // The conjunct SETS are unordered, and the two sides render differently: the
            // bind side carries "_spm_const_var(id)" placeholders while the user side
            // carries the concrete literal text, so a lexical (toSql) sort can pair the
            // WRONG expressions whenever the literal ordering reverses the structural
            // order - rejecting a valid baseline. Match the two conjunct multisets
            // structurally instead: every tentative pairing extracts placeholder values
            // transactionally and rolls them back on failure, so a pairing that only
            // looked compatible cannot poison the rest of the match.
            List<Expression> remaining = new ArrayList<>(userConjuncts);
            for (Expression bindConjunct : bindConjuncts) {
                int matched = -1;
                for (int i = 0; i < remaining.size(); i++) {
                    Map<Long, Expression> snapshot = new HashMap<>(placeholderValues);
                    if (checkExpression(bindConjunct, remaining.get(i), placeholderValues)) {
                        matched = i;
                        break;
                    }
                    placeholderValues.clear();
                    placeholderValues.putAll(snapshot);
                }
                if (matched < 0) {
                    return false;
                }
                remaining.remove(matched);
            }
            return true;
        }

        List<? extends Expression> bindExprs = bind.getExpressions();
        List<? extends Expression> userExprs = user.getExpressions();
        if (bindExprs.size() != userExprs.size()) {
            return false;
        }
        for (int i = 0; i < bindExprs.size(); i++) {
            if (!checkExpression(bindExprs.get(i), userExprs.get(i), placeholderValues)) {
                return false;
            }
        }
        // LATERAL VIEW / UNNEST: generator arguments are intentionally kept concrete
        // (see TreeTransformer.visitLogicalGenerate), so a baseline only matches a user
        // query carrying EXACTLY the same generator arguments and output aliases.
        // LogicalGenerate does not expose them through getExpressions(), compare them
        // explicitly - otherwise a query with a different array/generator argument would
        // match and be replayed with the baseline's old arguments.
        if (bind instanceof LogicalGenerate && user instanceof LogicalGenerate) {
            LogicalGenerate<?> bindGenerate = (LogicalGenerate<?>) bind;
            LogicalGenerate<?> userGenerate = (LogicalGenerate<?>) user;
            List<Function> bindGenerators = bindGenerate.getGenerators();
            List<Function> userGenerators = userGenerate.getGenerators();
            if (bindGenerators.size() != userGenerators.size()) {
                return false;
            }
            for (int i = 0; i < bindGenerators.size(); i++) {
                if (!checkExpression(bindGenerators.get(i), userGenerators.get(i), placeholderValues)) {
                    return false;
                }
            }
            List<? extends Expression> bindOutput = bindGenerate.getGeneratorOutput();
            List<? extends Expression> userOutput = userGenerate.getGeneratorOutput();
            if (bindOutput.size() != userOutput.size()) {
                return false;
            }
            for (int i = 0; i < bindOutput.size(); i++) {
                if (!checkExpression(bindOutput.get(i), userOutput.get(i), placeholderValues)) {
                    return false;
                }
            }
        }
        return true;
    }

    /** Single expression pair check (bind side parameterized, user side raw). */
    private static boolean checkExpression(Expression bindExpr, Expression userExpr,
            Map<Long, Expression> placeholderValues) {
        // Explicit output aliases (SELECT ... AS name) are part of the result contract:
        // the rewritten plan reuses the frozen text, so a baseline matched with a
        // different alias would report the CAPTURED column header. Derived names
        // (nameFromChild - the parser's fallback from the expression text, which may
        // carry the captured literal) stay out of the comparison. Both the analyzed
        // Alias and the parse-time UnboundAlias are covered: a raw parsed tree (the
        // bind side is re-parsed from the frozen text) only ever carries UnboundAlias,
        // whose name is only reachable through getAlias().
        String bindAliasName = explicitAliasName(bindExpr);
        String userAliasName = explicitAliasName(userExpr);
        if (bindAliasName != null && userAliasName != null
                && !bindAliasName.equals(userAliasName)) {
            return false;
        }
        // Alias KIND parity: an explicit alias is part of the result contract, a
        // nameFromChild alias is the parser's fallback from the expression text (and may
        // carry the captured literal). One side explicit + the other derived is not the
        // same contract - replay would expose the captured header. Now that the
        // transforms preserve nameFromChild, this only fires on genuinely different
        // spellings, but the parity requirement keeps the comparison symmetric.
        if (isDerivedAlias(bindExpr) != isDerivedAlias(userExpr)) {
            return false;
        }
        return new SPMAstCheckVisitor().checkExpression(bindExpr, userExpr, placeholderValues);
    }

    /** Whether the expression is a nameFromChild (derived) alias. */
    private static boolean isDerivedAlias(Expression expr) {
        if (expr instanceof Alias) {
            return ((Alias) expr).isNameFromChild();
        }
        if (expr instanceof UnboundAlias) {
            return ((UnboundAlias) expr).isNameFromChild();
        }
        return false;
    }

    /**
     * The user-written name of an explicit output alias ({@code Alias} or parse-time
     * {@code UnboundAlias}), or null when the expression is not one. A nameFromChild
     * fallback is the expression text rather than an identifier and does not qualify.
     */
    private static String explicitAliasName(Expression expr) {
        if (expr instanceof Alias) {
            Alias alias = (Alias) expr;
            return alias.isNameFromChild() ? null : alias.getName();
        }
        if (expr instanceof UnboundAlias) {
            UnboundAlias alias = (UnboundAlias) expr;
            return !alias.isNameFromChild() && alias.getAlias().isPresent()
                    ? alias.getAlias().get() : null;
        }
        return null;
    }

    /**
     * Compares every non-parameterizable scan identity field of a base-table relation:
     * partition selection, tablet selection, hints, index, sample, snapshot and scan
     * parameters. {@code TableSnapshot} / {@code TableScanParams} have no value-based
     * equals, so their stable textual form is compared as well.
     */
    private static boolean sameScanIdentity(UnboundRelation bind, UnboundRelation user) {
        return sameSelectionIgnoreOrder(bind.getPartNames(), user.getPartNames())
                && sameSelectionIgnoreOrder(bind.getTabletIds(), user.getTabletIds())
                && Objects.equals(bind.getHints(), user.getHints())
                && Objects.equals(bind.getIndexName(), user.getIndexName())
                && sameOptionalValue(bind.getTableSample(), user.getTableSample())
                && sameOptionalValue(bind.getTableSnapshot(), user.getTableSnapshot())
                && sameScanParams(bind.getScanParams(), user.getScanParams());
    }

    /**
     * Partition and tablet selections are sets: FROM t PARTITION(p1, p2) / TABLET(1, 2)
     * read exactly the same data as the opposite order, while the decompiler emits the
     * selection in id order regardless of how the user ordered it. A multiset comparison
     * (with a size check, so PARTITION(p1, p1) stays distinct from PARTITION(p1)) keeps
     * the two spellings matchable without letting a duplicated entry hide a member.
     */
    private static boolean sameSelectionIgnoreOrder(List<?> bind, List<?> user) {
        if (bind == null || user == null) {
            return bind == user;
        }
        return bind.equals(user)
                || (bind.size() == user.size() && new HashSet<>(bind).equals(new HashSet<>(user)));
    }

    // ==================== view guard ====================

    /**
     * Whether a plan references a VIEW anywhere in the statement.
     *
     * SPM freezes / replays plans over the BASE tables a view expands to (InlineLogicalView
     * replaces the LogicalView wrapper during analysis), and the replay is planned BEFORE
     * the normal authorization pass: authorizing the expanded plan checks the base tables
     * instead of the view, so a view-only user is denied on the base tables while a
     * base-table user passes the same view query without ever being checked against the
     * view. A plan that references a view must therefore never be replayed, and a frozen
     * plan must never be produced from it: the parameterized bind / plan trees keep the
     * view reference text, so the rewrite replays them through normal analysis and
     * authorization. Unresolvable relations are reported as not-a-view here; the normal
     * analysis pass surfaces the resolution error.
     *
     * The WHOLE statement is inspected, not just children(): a view behind a CTE body or
     * an IN / EXISTS / scalar subquery would otherwise be missed, because those plans are
     * held by getAliasQueries() / extraPlans() / SubqueryExpr.queryPlan instead of
     * children() (see {@link #walkPlans}).
     */
    public static boolean referencesView(ConnectContext ctx, Plan plan) {
        if (plan == null || ctx == null || ctx.getStatementContext() == null) {
            return false;
        }
        final boolean[] viewReferenced = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (viewReferenced[0]) {
                return;
            }
            if (node instanceof LogicalView) {
                viewReferenced[0] = true;
            } else if (node instanceof UnboundRelation && isViewRelation(ctx, (UnboundRelation) node)) {
                viewReferenced[0] = true;
            }
        });
        return viewReferenced[0];
    }

    /**
     * Visits every plan node reachable from a statement: the regular children, the plans
     * a node holds OUTSIDE children() - CTE bodies (LogicalCTE.getAliasQueries() through
     * extraPlans()), IN / EXISTS / scalar subquery plans (SubqueryExpr.queryPlan, surfaced
     * by LogicalFilter.extraPlans() and by the node's own expressions) - and the plans
     * reachable through expression coercions. An inspection that walks only children()
     * would not see a view (referencesView) or a SET_VAR hint
     * (SPMOptimizer#checkProtectedSetVarHints) that lives in a CTE body / subquery.
     *
     * @param root    the plan to start from (may be null)
     * @param visitor called once per reachable node; may throw
     * @param <E>     the visitor's exception type, propagated to the caller
     */
    public static <E extends Exception> void walkPlans(Plan root, PlanWalker<E> visitor) throws E {
        if (root == null) {
            return;
        }
        visitor.visit(root);
        for (Plan child : root.children()) {
            walkPlans(child, visitor);
        }
        for (Plan extra : root.extraPlans()) {
            walkPlans(extra, visitor);
        }
        for (Expression expression : root.getExpressions()) {
            walkSubqueryPlans(expression, visitor);
        }
    }

    /** Recurses one expression tree looking for subquery plans (coercions included). */
    private static <E extends Exception> void walkSubqueryPlans(Expression expression, PlanWalker<E> visitor)
            throws E {
        if (expression instanceof SubqueryExpr) {
            walkPlans(((SubqueryExpr) expression).getQueryPlan(), visitor);
        }
        for (Expression child : expression.children()) {
            walkSubqueryPlans(child, visitor);
        }
    }

    /** Plan visitor for {@link #walkPlans}; the exception type is chosen by the caller. */
    public interface PlanWalker<E extends Exception> {
        void visit(Plan plan) throws E;
    }

    /**
     * Whether the statement writes to a file destination (SELECT ... INTO OUTFILE). The
     * destination (path / format / properties) and the sink's metadata live OUTSIDE the
     * expression list, so the generic SPM comparison would accept a user export targeting
     * a different destination - and the rewritten tree keeps the CAPTURED sink fields
     * (LogicalSink.withChildren preserves them), so the replay would write to the
     * baseline's destination. SPM rejects such statements at freeze and never rewrites
     * them.
     */
    public static boolean containsFileSink(Plan plan) {
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof LogicalFileSink) {
                found[0] = true;
            }
        });
        return found[0];
    }

    private static boolean isViewRelation(ConnectContext ctx, UnboundRelation relation) {
        try {
            TableIf table = ctx.getStatementContext().getAndCacheTable(
                    RelationUtil.getQualifierName(ctx, relation.getNameParts()),
                    StatementContext.TableFrom.QUERY, Optional.of(relation));
            return table instanceof View;
        } catch (RuntimeException e) {
            // unresolvable / plugin table without metadata here: not treated as a view,
            // the normal analysis pass reports the error
            return false;
        }
    }

    // ==================== LIMIT / OFFSET canonical digest ====================

    /**
     * Canonicalizes an SPM digest for the Level 1/2 matching key: the TOP-LEVEL LIMIT /
     * OFFSET values are adopted from the user query at rewrite time
     * (SPMPlanTreeSupport#mergeLimits) and must therefore not be part of the key - but
     * LogicalLimit.toDigest() renders " OFFSET ?" only for a NON-ZERO offset, so a
     * baseline captured as "LIMIT 10" and a matching "LIMIT 20 OFFSET 5" query produced
     * different digests, the candidate list came back empty and the OFFSET query could
     * never reach the structural match that adopts its values. Dropping every
     * " OFFSET ?" suffix makes the key offset-independent exactly like it already is
     * limit-independent.
     *
     * A LIMIT / OFFSET inside a subquery plan is NOT reachable by the positional merge:
     * those values stay exact-compared at Level 3 (checkPlan insideSubquery), so removing
     * them from the digest can only skip/accept a candidate, never replay a wrong slice.
     *
     * @param digest the raw toSpmDigest() text
     * @return the offset-independent matching key
     */
    public static String canonicalSpmDigest(String digest) {
        return digest == null ? null : digest.replace(" OFFSET ?", "");
    }

    // ==================== schema identity (frozen-baseline binding) ====================

    /**
     * Fingerprint of the base tables referenced by a (still unbound) statement: one
     * deterministic entry per resolvable catalog relation,
     * {@code tableName|tableId|schemaHash}, sorted and joined with ';'. CREATE persists
     * it with the baseline and the rewrite validates it BEFORE replaying a frozen plan:
     * the bind key is built from the unbound query, so {@code SELECT * FROM t WHERE k=1}
     * keeps the same digest and Level-3 tree after {@code ALTER TABLE t ADD COLUMN extra}
     * (or after a DROP + CREATE), while the frozen result sink still emits the
     * creator-time output columns - the matched replay would silently return the old
     * column set instead of the current star expansion. The table id also catches
     * drop / recreate (a new id), and the schema hash catches column add / drop / type
     * changes in place.
     *
     * Resolution mirrors {@link #isViewRelation}: unresolvable relations are skipped
     * (the normal analysis pass reports the error). A statement without a resolvable
     * relation (SELECT 1, TVF-only, ...) yields an EMPTY fingerprint, which disables
     * the check - there is no schema identity to bind to.
     *
     * @param ctx  the session context (null yields an empty fingerprint)
     * @param plan the unbound plan (null yields an empty fingerprint)
     * @return the fingerprint (possibly empty, never null)
     */
    public static String schemaFingerprint(ConnectContext ctx, Plan plan) {
        if (plan == null || ctx == null || ctx.getStatementContext() == null) {
            return "";
        }
        TreeSet<String> entries = new TreeSet<>();
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            if (node instanceof UnboundRelation) {
                UnboundRelation relation = (UnboundRelation) node;
                try {
                    TableIf table = ctx.getStatementContext().getAndCacheTable(
                            RelationUtil.getQualifierName(ctx, relation.getNameParts()),
                            StatementContext.TableFrom.QUERY, Optional.of(relation));
                    entries.add(describeTableForFingerprint(table));
                } catch (RuntimeException e) {
                    // unresolvable: not part of the fingerprint, the analysis pass reports
                }
            }
        });
        if (entries.isEmpty()) {
            return "";
        }
        StringBuilder fingerprint = new StringBuilder();
        for (String entry : entries) {
            if (fingerprint.length() > 0) {
                fingerprint.append(';');
            }
            fingerprint.append(entry);
        }
        return fingerprint.toString();
    }

    /** One fingerprint entry of one resolved table: name + id + base-schema hash. */
    private static String describeTableForFingerprint(TableIf table) {
        StringBuilder schema = new StringBuilder();
        for (Column column : table.getBaseSchema()) {
            schema.append(column.getName()).append(':')
                    .append(column.getType().toString()).append(',');
        }
        return table.getName() + "|" + table.getId() + "|" + SPMUtils.hashOf(schema.toString());
    }

    /** Optional value equality with a textual fallback for value types without equals. */
    private static <T> boolean sameOptionalValue(Optional<T> bind, Optional<T> user) {
        if (!bind.isPresent() || !user.isPresent()) {
            return bind.isPresent() == user.isPresent();
        }
        return Objects.equals(bind.get(), user.get())
                || Objects.equals(bind.get().toString(), user.get().toString());
    }

    private static boolean sameScanParams(TableScanParams bind, TableScanParams user) {
        if (bind == null || user == null) {
            return bind == user;
        }
        // TableScanParams overrides neither equals nor toString and every parse creates a
        // fresh instance, so identity and default equality / string comparison are all
        // false even for identical @branch / @tag / @options / @incr syntax - a baseline
        // using scan parameters could never hit. Compare the type and the payloads.
        return Objects.equals(bind.getParamType(), user.getParamType())
                && Objects.equals(bind.getMapParams(), user.getMapParams())
                && Objects.equals(bind.getListParams(), user.getListParams());
    }

    // ==================== non-expression literal merge (LIMIT / OFFSET) ====================

    /**
     * Merges the user query's non-expression literals (LogicalLimit limit/offset) into
     * the rewritten plan tree, position aligned. Such literals are plain long fields on
     * the plan node (not Expressions), so they are not parameterized like scalar
     * literals; a user query whose LIMIT differs from the bind SQL must still be
     * rewritten with the USER limit/offset, otherwise the rewritten plan would run with
     * the captured limit and return a truncated result.
     *
     * @param plan     the rewritten plan tree (placeholders already substituted)
     * @param userPlan the user's original plan tree (source of the limit / offset)
     * @return the plan tree with the user's limit / offset merged in
     */
    public static LogicalPlan mergeLimits(LogicalPlan plan, LogicalPlan userPlan) {
        if (plan == null || userPlan == null) {
            return plan;
        }
        Plan merged = mergeLimitNode(plan, userPlan);
        return merged instanceof LogicalPlan ? (LogicalPlan) merged : plan;
    }

    /** Recursively merges the user's limit / offset into the plan, node by node. */
    private static Plan mergeLimitNode(Plan plan, Plan user) {
        if (plan.getClass() != user.getClass()) {
            // structural guard: never reshape a mismatching tree
            return plan;
        }
        if (plan instanceof LogicalCTE && user instanceof LogicalCTE) {
            LogicalCTE<?> cte = (LogicalCTE<?>) plan;
            LogicalCTE<?> userCte = (LogicalCTE<?>) user;
            Plan newChild = cte.child(0) == null ? null
                    : mergeLimitNode(cte.child(0), userCte.child(0));
            boolean childChanged = newChild != cte.child(0);
            List<LogicalSubQueryAlias<Plan>> newAliasQueries =
                    new ArrayList<>(cte.getAliasQueries().size());
            boolean aliasChanged = false;
            List<LogicalSubQueryAlias<Plan>> userAliasQueries = userCte.getAliasQueries();
            for (int i = 0; i < cte.getAliasQueries().size(); i++) {
                LogicalSubQueryAlias<Plan> alias = cte.getAliasQueries().get(i);
                LogicalSubQueryAlias<Plan> userAlias = userAliasQueries.get(i);
                Plan newAlias = mergeLimitNode(alias, userAlias);
                newAliasQueries.add((LogicalSubQueryAlias<Plan>) newAlias);
                if (newAlias != alias) {
                    aliasChanged = true;
                }
            }
            if (!childChanged && !aliasChanged) {
                return cte;
            }
            try {
                return new LogicalCTE<Plan>(cte.isRecursive(), newAliasQueries, newChild);
            } catch (RuntimeException e) {
                return cte;
            }
        }

        // merge the children first
        List<Plan> children = plan.children();
        List<Plan> userChildren = user.children();
        boolean changed = false;
        List<Plan> newChildren = new ArrayList<>(children.size());
        for (int i = 0; i < children.size(); i++) {
            Plan newChild = mergeLimitNode(children.get(i), userChildren.get(i));
            newChildren.add(newChild);
            if (newChild != children.get(i)) {
                changed = true;
            }
        }
        Plan current = plan;
        if (changed) {
            try {
                current = plan.withChildren(newChildren);
            } catch (RuntimeException e) {
                current = plan;
            }
        }

        // adopt the user's limit / offset on the limit node itself
        if (current instanceof LogicalLimit && user instanceof LogicalLimit) {
            LogicalLimit<?> limit = (LogicalLimit<?>) current;
            LogicalLimit<?> userLimit = (LogicalLimit<?>) user;
            if (limit.getLimit() == userLimit.getLimit()
                    && limit.getOffset() == userLimit.getOffset()) {
                return current;
            }
            return new LogicalLimit<>(userLimit.getLimit(), userLimit.getOffset(),
                    limit.getPhase(), limit.child());
        }
        return current;
    }
}
