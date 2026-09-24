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
import org.apache.doris.analysis.TableSnapshot;
import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundTVFRelation;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.rules.exploration.join.JoinReorderContext;
import org.apache.doris.nereids.spm.matcher.SPMAstCheckVisitor;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.TableSample;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLikeLiteral;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTE;
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
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalSubQueryAlias;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.GlobalVariable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

        @Override
        public Plan visitLogicalHaving(LogicalHaving<? extends Plan> having,
                ExprTransform transform) {
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
        if (plan == null || db == null || db.isEmpty()) {
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
            if (catalog != null && !catalog.isEmpty()) {
                qualified.add(catalog);
            }
            if (parts.size() == 1) {
                qualified.add(db);
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
    public static boolean containsFrozenPlaceholder(LogicalPlan plan) {
        return plan.accept(new FrozenPlaceholderScanVisitor(), null);
    }

    /** Plan visitor that scans every node's expressions (and subquery plans) for
     * unsubstituted frozen-tree placeholder calls. */
    private static class FrozenPlaceholderScanVisitor extends PlanVisitor<Boolean, Void> {
        @Override
        public Boolean visit(Plan plan, Void context) {
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
            List<Expression> sortedBind = new ArrayList<>(bindConjuncts);
            List<Expression> sortedUser = new ArrayList<>(userConjuncts);
            sortedBind.sort(Comparator.comparing(Expression::toSql));
            sortedUser.sort(Comparator.comparing(Expression::toSql));
            if (sortedBind.size() != sortedUser.size()) {
                return false;
            }
            for (int i = 0; i < sortedBind.size(); i++) {
                if (!checkExpression(sortedBind.get(i), sortedUser.get(i), placeholderValues)) {
                    return false;
                }
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
        return new SPMAstCheckVisitor().checkExpression(bindExpr, userExpr, placeholderValues);
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
        return Objects.equals(bind.getPartNames(), user.getPartNames())
                && Objects.equals(bind.getTabletIds(), user.getTabletIds())
                && Objects.equals(bind.getHints(), user.getHints())
                && Objects.equals(bind.getIndexName(), user.getIndexName())
                && sameOptionalValue(bind.getTableSample(), user.getTableSample())
                && sameOptionalValue(bind.getTableSnapshot(), user.getTableSnapshot())
                && sameScanParams(bind.getScanParams(), user.getScanParams());
    }

    /** Optional value equality with a textual fallback for value types without equals. */
    private static <T> boolean sameOptionalValue(Optional<T> bind, Optional<T> user) {
        if (!bind.isPresent() || !user.isPresent()) {
            return bind.isPresent() == user.isPresent();
        }
        return Objects.equals(bind.get(), user.get())
                || Objects.equals(bind.get().toString(), user.get().toString());
    }

    /** TableScanParams equality (the analysis type has no value-based equals). */
    private static boolean sameScanParams(TableScanParams bind, TableScanParams user) {
        if (bind == null || user == null) {
            return bind == user;
        }
        return bind == user || bind.equals(user)
                || Objects.equals(bind.toString(), user.toString());
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
