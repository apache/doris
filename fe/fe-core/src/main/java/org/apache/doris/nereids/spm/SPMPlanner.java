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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.matcher.SPMPlaceholderReplacer;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.qe.ConnectContext;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SPMPlanner - the controller of the whole-query SPM bind and rewrite.
 *
 * SPM works on the WHOLE parsed (still unbound) SELECT plan tree, not only on the
 * per-query-block WHERE predicates:
 *
 * 1. Build (CREATE BASELINE PLAN / auto capture): parse bindSql, replace EVERY literal
 *    of the whole tree (filters, having, projections, aggregate group-by/output and
 *    every subquery inside an expression, recursively) with a placeholder through one
 *    shared SPMPlaceholderBuilder, and do the same for planSql so both trees share the
 *    same placeholder ids. The baseline keeps the parameterized bind tree (used for the
 *    Level 3 structural match) and the parameterized plan tree (used to produce the
 *    rewritten query).
 * 2. Rewrite (user query, called from StmtExecutor / EXPLAIN): compute the value-free
 *    full-query digest / hash, find candidate baselines (Level 1 hash + Level 2 digest),
 *    structurally compare the candidate's parameterized bind tree with the user's tree
 *    over the whole tree (Level 3, extracting the user values for every placeholder id),
 *    and on a match replay the baseline's FROZEN planSql (M3): the frozen text - the
 *    decompiled optimal plan carrying placeholder ids, join distribution hints and the
 *    pushed-down structure - is re-parsed and the extracted values are substituted by
 *    placeholder id, so the rewrite reproduces the frozen optimal structure (SR-aligned).
 *    Baselines without a frozen placeholder text fall back to substituting the candidate's
 *    parameterized plan tree. The substituted tree (still unbound, no placeholders left)
 *    is returned and planned normally by the caller.
 *
 * A query without any query-block WHERE - e.g. one whose SELECT-list scalar subquery
 * carries its own WHERE - is handled naturally: the subquery's literals are part of the
 * whole tree and take part in parameterization, matching, value extraction and rewrite,
 * so no special case or rejection is needed.
 *
 * A failed rewrite, a timeout or a no-match returns null (the caller executes the
 * original query normally; SPM degrades transparently).
 */
public class SPMPlanner {

    private static final Logger LOG = LogManager.getLogger(SPMPlanner.class);

    /** Id of the baseline used by the last successful rewrite (-1 when none). */
    private long usedBaselineId = -1;

    /**
     * Returns the id of the baseline used by the last successful rewrite.
     *
     * @return the baseline id, or -1 when the last rewrite did not hit a baseline
     */
    public long getUsedBaselineId() {
        return usedBaselineId;
    }

    // ==================== query rewrite (called from StmtExecutor / EXPLAIN) ====================

    /**
     * Rewrites a user query (parsed, still unbound) via SPM.
     *
     * Runs the three-level match against every candidate baseline and, on a hit,
     * substitutes the user's actual literal values into the candidate's parameterized
     * plan tree and returns the resulting (unbound) plan. The caller (StmtExecutor)
     * plans the returned tree normally, so the rewritten query goes through the regular
     * analyze / optimize pipeline.
     *
     * @param userPlan the parsed (unbound) plan of the user query
     * @param deadline rewrite deadline (epoch millis) for the spm_rewrite_timeout_ms budget
     * @return the rewritten unbound plan, or null when no baseline matched / timed out /
     *         the substituted plan is unusable
     */
    public LogicalPlan tryRewritePlan(LogicalPlan userPlan, long deadline) {
        if (System.currentTimeMillis() > deadline) {
            LOG.info("SPM tryRewritePlan: timeout before matching, degrade to the original plan");
            return null;
        }
        ConnectContext ctx = ConnectContext.get();
        SessionBaselineStore sessionStore = ctx == null ? null : ctx.getSessionBaselineStore();
        // Fast path: with no baseline anywhere (global or session) there is nothing to
        // match, so skip the whole-tree digest rendering below - it walks every literal
        // of the whole query and is the most expensive part of a rewrite attempt.
        if (!BaselineManager.getInstance().hasBaselines()
                && (sessionStore == null || sessionStore.isEmpty())) {
            return null;
        }
        // Level 1/2 use the FULL-QUERY value-free digest (Plan.toSpmDigest() renders every
        // literal as "?", so the digest is value-independent). The digest is computed on a
        // namespace-qualified COPY: "FROM t" is keyed as the CURRENT catalog/db's t, so a
        // baseline captured under db1 can never match the same text executed under db2
        // (the frozen planSql is fully qualified and would silently keep running against
        // db1.t). Explicitly qualified references stay verbatim on both sides, and so do
        // references to a CTE alias: those bind inside the query's own WITH clause and are
        // therefore namespace-independent.
        LogicalPlan matchPlan = SPMPlanTreeSupport.namespaceQualified(userPlan,
                captureCatalogName(ctx), captureDatabaseName(ctx));
        String queryDigest = matchPlan.toSpmDigest();
        long queryHash = SPMUtils.hashOf(queryDigest);
        // SESSION-scope baselines of the current connection are consulted BEFORE the
        // global ones, so a session baseline can override a global baseline for this
        // session only; each candidate list is already priority-ordered.
        List<BaselinePlan> candidates = new ArrayList<>();
        if (sessionStore != null) {
            candidates.addAll(sessionStore.findCandidateBaselines(queryDigest, queryHash));
        }
        candidates.addAll(
                BaselineManager.getInstance().findCandidateBaselines(queryDigest, queryHash));
        if (candidates.isEmpty()) {
            return null;
        }
        // View guard (computed lazily on the first structural match, so the per-query
        // hot path pays nothing): the replay of a matched baseline is planned before the
        // normal authorization pass and resolves a view to its base tables, so
        // authorization would check the base tables instead of the view - a view-only
        // user is denied on the base tables, while a base-table user passes the same
        // view query without any view check. View queries keep the original plan.
        boolean viewChecked = false;
        boolean viewReferenced = false;
        for (BaselinePlan candidate : candidates) {
            if (System.currentTimeMillis() > deadline) {
                LOG.info("SPM tryRewritePlan: timeout before matching baseline {}, "
                        + "degrade to the original plan", candidate.getId());
                return null;
            }
            LogicalPlan bindTree = candidate.getParameterizedBindPlan();
            if (bindTree == null) {
                continue;
            }
            // Level 3: whole-tree structural match + value extraction
            Map<Long, Expression> placeholderValues = new HashMap<>();
            if (!SPMPlanTreeSupport.check(bindTree, matchPlan, placeholderValues)) {
                continue;
            }
            LOG.info("SPM tryRewritePlan: baseline {} matched, extracted {} placeholder values",
                    candidate.getId(), placeholderValues.size());
            if (!viewChecked) {
                viewChecked = true;
                viewReferenced = SPMPlanTreeSupport.referencesView(ctx, userPlan);
            }
            if (viewReferenced) {
                LOG.info("SPM tryRewritePlan: the query references a view; keeping the original"
                        + " plan so view authorization is preserved");
                return null;
            }
            // The expensive part (value-free digest, candidate lookup, whole-tree check) is
            // already done and a baseline has matched: finish the (cheap) value substitution
            // even when the budget has been consumed. Previously this path re-checked the
            // deadline here and returned null on expiry, silently throwing the completed
            // match away (rewrite degraded to the original plan).
            // M3: replay the FROZEN optimal plan. A baseline whose frozen planSql carries
            // placeholder calls (decompiled from the SPM-optimized physical plan, structure
            // + join distribution hints preserved) is re-parsed and the user values are
            // substituted by placeholder id - the rewrite reproduces the frozen optimal
            // structure exactly (SR-aligned). Baselines without a frozen placeholder text
            // (in-memory engine, decompile-fallback planSql, legacy "?" frozen text) fall
            // back to substituting the parameterized plan tree.
            LogicalPlan rewritten = rewriteFromFrozenTree(candidate, placeholderValues);
            if (rewritten != null) {
                // non-expression literals (LIMIT / OFFSET) are long fields, not
                // placeholders; adopt the user's values so a structurally identical query
                // with a different limit is rewritten with the USER limit
                usedBaselineId = candidate.getId();
                return SPMPlanTreeSupport.mergeLimits(rewritten, matchPlan);
            }
            LOG.info("SPM tryRewritePlan: baseline {} frozen planSql replay unavailable, "
                    + "falling back to parameterized plan tree", candidate.getId());
            LogicalPlan planTree = stripSelectHints(candidate.getParameterizedPlanPlan());
            if (planTree == null) {
                continue;
            }
            // fallback: substitute the extracted user values into the parameterized plan
            // tree
            SPMPlaceholderReplacer replacer = new SPMPlaceholderReplacer();
            rewritten = SPMPlanTreeSupport.transform(
                    planTree, expr -> expr.accept(replacer, placeholderValues));
            // safety net: a plan-only placeholder (no value extracted from the user query)
            // would reach the analyzer - never rewrite with this candidate; keep trying the
            // remaining candidates (they are validated independently) and degrade to the
            // original plan when none works
            if (SPMPlanTreeSupport.containsPlaceholder(rewritten)) {
                continue;
            }
            usedBaselineId = candidate.getId();
            return SPMPlanTreeSupport.mergeLimits(rewritten, matchPlan);
        }
        return null;
    }

    /**
     * M3: replays a baseline from its frozen planSql text.
     *
     * When the baseline's planSql carries placeholder calls (the decompiled frozen
     * optimal plan), the text is re-parsed into an unbound logical plan - preserving the
     * frozen structure (join order / subquery nesting / pushed-down filters) and the join
     * distribution hints ([BROADCAST] / [SHUFFLE]) - and the user values are substituted
     * by placeholder id (SPMFrozenTreeReplacer). The caller plans the returned tree
     * normally, so the optimizer starts from the frozen optimal structure instead of the
     * user's raw SQL structure (aligned with SR's PlaceholderReplacer flow).
     *
     * @param candidate         the matched baseline
     * @param placeholderValues placeholder id -> user value (extracted by the Level 3
     *                          structural check on the bind tree)
     * @return the substituted unbound plan, or null when the baseline has no frozen
     *         placeholder text / re-parsing fails / a placeholder has no user value
     */
    private LogicalPlan rewriteFromFrozenTree(BaselinePlan candidate,
            Map<Long, Expression> placeholderValues) {
        String planSql = candidate.getPlanSql();
        if (planSql == null
                || (!planSql.contains(SPMFrozenTreeReplacer.CONST_VAR_FUNC)
                && !planSql.contains(SPMFrozenTreeReplacer.CONST_LIST_FUNC))) {
            return null; // not a frozen (placeholder-carrying) plan text
        }
        // re-parsing the frozen text needs the session context (join-hint / statement
        // context handling); when absent (pure in-memory callers) fall back
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return null;
        }
        try {
            Plan parsed = new NereidsParser().parseSingle(planSql);
            if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
                return null;
            }
            SPMFrozenTreeReplacer replacer = new SPMFrozenTreeReplacer();
            LogicalPlan rewritten = SPMPlanTreeSupport.transform(
                    (LogicalPlan) parsed, expr -> expr.accept(replacer, placeholderValues));
            // safety net: a plan-only placeholder (no user value extracted) would reach
            // the analyzer as an unregistered function - never rewrite with it
            if (SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten)) {
                return null;
            }
            if (LOG.isInfoEnabled()) {
                LOG.info("SPM replay baseline {} from frozen planSql (id substitution)",
                        candidate.getId());
            }
            return rewritten;
        } catch (Throwable t) {
            // legacy "?" frozen text or any other re-parse failure -> fall back to the
            // parameterized plan tree path
            return null;
        }
    }

    /**
     * The effective catalog name of the creation / matching context (null when unknown);
     * part of the namespace-qualified matching key.
     */
    private static String captureCatalogName(ConnectContext ctx) {
        return ctx == null || ctx.getCurrentCatalog() == null
                ? null : ctx.getCurrentCatalog().getName();
    }

    /** The effective database of the creation / matching context (null when unknown). */
    private static String captureDatabaseName(ConnectContext ctx) {
        return ctx == null ? null : ctx.getDatabase();
    }

    /**
     * Removes the root LogicalSelectHint (its SET_VAR payload) from the FALLBACK
     * parameterized plan tree. The primary frozen-text path re-parses planSql including
     * its hints deliberately; the in-memory fallback must not re-apply the BASELINE's
     * captured SET_VAR on top of a user query that came with different session
     * variables (a time_zone='+08:00' baseline would override the user's -08:00
     * from_unixtime and return wrong values - the user's own SET_VAR was already applied
     * during parsing).
     */
    private static LogicalPlan stripSelectHints(LogicalPlan plan) {
        LogicalPlan current = plan;
        while (current instanceof LogicalSelectHint) {
            Plan child = current.child(0);
            if (!(child instanceof LogicalPlan)) {
                break;
            }
            current = (LogicalPlan) child;
        }
        return current;
    }

    // ==================== baseline creation ====================

    /**
     * Builds a baseline from SQL texts (in-memory engine, no optimization).
     *
     * UT-only convenience entry (the production path is
     * buildBaselineFromSql). Parses bindSql and planSql (a single parse when
     * the two texts are identical), parameterizes both whole trees with one shared
     * SPMPlaceholderBuilder and stores the parameterized trees together with the
     * value-free digest / hash of the bind tree.
     *
     * @param bindSql the binding SQL (SELECT)
     * @param planSql the plan SQL (SELECT, may contain SET_VAR hints)
     * @return the constructed BaselinePlan (not yet stored)
     * @throws UserException when a SQL cannot be parsed as a SELECT
     */
    @VisibleForTesting
    BaselinePlan buildBaseline(String bindSql, String planSql) throws UserException {
        LogicalPlan bindPlan = parseSelect(bindSql, "SPM bindSql must be a SELECT statement: " + bindSql);
        // bindSql == planSql: one parse is enough and the parameterized trees are the
        // same by construction (ids trivially aligned)
        LogicalPlan planPlan = bindSql.equals(planSql)
                ? bindPlan : parseSelect(planSql, "SPM planSql must be a SELECT statement: " + planSql);
        return buildBaseline(bindPlan, planPlan, bindSql, planSql, 0.0);
    }

    /**
     * Builds a baseline from two already parsed (unbound) plan trees.
     *
     * UT-only entry. Parameterizes both whole trees with ONE shared
     * SPMPlaceholderBuilder so placeholder ids stay globally unique and aligned across
     * the two trees (a literal that appears in both bind and plan trees reuses the same
     * id), then assembles the baseline. Stores the parameterized bind tree (Level 3
     * matching) and plan tree (rewrite source).
     *
     * @param bindPlan the parsed (unbound) bind plan
     * @param planPlan the parsed (unbound) plan plan (may be the same object as
     *                 bindPlan when bindSql == planSql)
     * @param bindSql  the original binding SQL text (shown by SHOW)
     * @param planSql  the frozen plan SQL text
     * @param cost     the CBO estimated cost (0 when unknown)
     * @return the constructed BaselinePlan (not yet stored)
     */
    @VisibleForTesting
    BaselinePlan buildBaseline(LogicalPlan bindPlan, LogicalPlan planPlan,
            String bindSql, String planSql, double cost) {
        Pair<LogicalPlan, LogicalPlan> trees = parameterizeWholeTrees(bindPlan, planPlan);
        // the matching key is namespace-qualified like tryRewritePlan's user side, so the
        // in-memory (UT) create / rewrite pair stays consistent in any context
        ConnectContext ctx = ConnectContext.get();
        return assembleBaseline(bindPlan, trees.first, trees.second, bindSql, planSql, cost,
                captureCatalogName(ctx), captureDatabaseName(ctx));
    }

    /**
     * Builds a baseline from SQL texts with a SPM-mode optimized planSql (used by
     * CREATE BASELINE PLAN and auto capture).
     *
     * 1. Parse bindSql (its value-free digest / hash become the matching key; a
     *    single parse when bindSql == planSql).
     * 2. Parameterize bindSql and planSql over their WHOLE plan trees with ONE shared
     *    SPMPlaceholderBuilder (placeholder ids are globally unique and aligned between
     *    the two trees - a literal that appears in both trees reuses the same id).
     * 3. Optimize the PARAMETERIZED plan tree in SPM mode (SPMOptimizer, state-sensitive
     *    rules disabled): the placeholders (SpmConstVar / SpmConstList) travel through
     *    the optimizer and survive into the physical plan, so the decompiled frozen
     *    planSql keeps the same placeholder ids. This is what makes the frozen planSql
     *    replayable at rewrite time (values substituted by id), aligned with the SR
     *    model.
     * 4. Decompile the physical plan back to the frozen planSql text (SPMPlan2SQLBuilder,
     *    placeholder ids preserved). When the physical plan cannot be decompiled (e.g. a
     *    recursive CTE whose plan carries PhysicalRecursiveUnion / anchor / producer, or
     *    any other future unsupported operator), the user-supplied planSql text is kept
     *    as-is instead of failing the whole CREATE (Doris-specific improvement: SR fails
     *    the CREATE in that case).
     *
     * @param ctx     the connect context (catalog + session variables)
     * @param bindSql the binding SQL (SELECT)
     * @param planSql the plan SQL (SELECT, may contain SET_VAR hints)
     * @return the constructed BaselinePlan (not yet stored)
     * @throws UserException when the SQLs cannot be parsed / planned
     */
    public BaselinePlan buildBaselineFromSql(ConnectContext ctx, String bindSql, String planSql)
            throws UserException {
        LogicalPlan bindPlan = parseSelect(bindSql, "SPM bindSql must be a SELECT statement: " + bindSql);
        LogicalPlan planPlan = bindSql.equals(planSql)
                ? bindPlan : parseSelect(planSql, "SPM planSql must be a SELECT statement: " + planSql);
        // Parameterize both whole trees with ONE shared builder (placeholder ids aligned
        // across bind / plan), then optimize the PARAMETERIZED plan tree so the frozen
        // planSql keeps the placeholder ids for the rewrite-time value substitution.
        Pair<LogicalPlan, LogicalPlan> trees = parameterizeWholeTrees(bindPlan, planPlan);
        LogicalPlan parameterizedPlan = trees.second;
        // View guard: freeze no planSql from a tree that references a view. The frozen
        // text is replayed ahead of authorization, so replaying a view's expansion would
        // authorize the base tables instead of the view; keeping the user planSql lets
        // the rewrite fall back to the parameterized tree, which re-resolves (and
        // authorizes) the view during its own analysis.
        boolean referencesView = SPMPlanTreeSupport.referencesView(ctx, bindPlan)
                || SPMPlanTreeSupport.referencesView(ctx, planPlan);

        SPMOptimizer.OptimizeResult optimizeResult;
        String frozenPlanSql;
        try {
            // optimize the parameterized plan tree in SPM mode: placeholders travel
            // through analyze / rewrite / CBO and survive into the physical plan
            optimizeResult = SPMOptimizer.optimize(ctx, parameterizedPlan, planSql);
            frozenPlanSql = decompileFrozenPlan(referencesView, optimizeResult, planSql);
        } catch (UserException | RuntimeException e) {
            // When the parameterized tree cannot be planned (e.g. a placeholder cannot
            // survive some analyzer path yet), fall back to optimizing the raw planSql -
            // the CREATE must not fail because of the parameterized-tree optimization.
            // NOTE: the fallback decompiles a plan built from the RAW literals, so the
            // frozen planSql loses its placeholders and the baseline degrades to the
            // parameterized-plan-tree rewrite path.
            LOG.warn("SPM parameterized plan optimization failed, falling back to raw planSql",
                    e);
            optimizeResult = SPMOptimizer.optimize(ctx, planSql);
            frozenPlanSql = decompileFrozenPlan(referencesView, optimizeResult, planSql);
        }
        return assembleBaseline(bindPlan, trees.first, parameterizedPlan, bindSql,
                frozenPlanSql, optimizeResult.getCost(),
                captureCatalogName(ctx), captureDatabaseName(ctx));
    }

    /**
     * Decompiles the optimized physical plan into the frozen planSql. The user-supplied
     * planSql text is kept when the decompiler does not support the plan (recursive CTE,
     * future operators, ...) or when the plan references a view: the frozen text is
     * replayed ahead of authorization, and replaying a view's base-table expansion would
     * authorize those base tables instead of the view.
     */
    private static String decompileFrozenPlan(boolean referencesView,
            SPMOptimizer.OptimizeResult optimizeResult, String planSql) {
        if (referencesView) {
            LOG.info("SPM freeze skipped: the plan references a view; keeping the user planSql"
                    + " so the rewrite replays the parameterized tree (view authorization"
                    + " preserved)");
            return planSql;
        }
        try {
            return new SPMPlan2SQLBuilder().toSQL(optimizeResult.getPhysicalPlan());
        } catch (UnsupportedOperationException e) {
            LOG.info("SPM decompile unsupported ({}):\n{}", e.getMessage(),
                    optimizeResult.getPhysicalPlan().treeString());
            return planSql;
        }
    }

    /**
     * Parameterizes both whole trees with ONE shared SPMPlaceholderBuilder so that
     * placeholder ids are globally unique and stay aligned across the two trees (a
     * literal that appears in both trees reuses the same id). When the caller parsed the
     * two SQLs into the SAME plan object (bindSql == planSql), the second transform is
     * skipped and the shared parameterized tree serves both roles.
     *
     * Id alignment follows (value + parent structure + child position), so a literal of
     * the plan tree reuses the id of the identical bind-tree slot; the values extracted
     * from the bind tree can then never be substituted into a different literal slot of
     * the plan tree. Plan-only literals get fresh ids and are rejected by the
     * placeholder-residue safety net at rewrite time. This keeps the CREATE-time id
     * mapping identical to the post-restart rebuild (rebuildParameterizedTrees).
     */
    private static Pair<LogicalPlan, LogicalPlan> parameterizeWholeTrees(
            LogicalPlan bindPlan, LogicalPlan planPlan) {
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = parameterizeWholeTree(builder, bindPlan);
        if (planPlan == bindPlan) {
            // bindSql == planSql: one parse, one parameterization serves both roles
            return Pair.of(parameterizedBind, parameterizedBind);
        }
        LogicalPlan parameterizedPlan = parameterizeWholeTree(builder, planPlan);
        return Pair.of(parameterizedBind, parameterizedPlan);
    }

    /** Parameterizes one whole tree with the given (possibly shared) builder. */
    private static LogicalPlan parameterizeWholeTree(SPMPlaceholderBuilder builder,
            LogicalPlan plan) {
        return SPMPlanTreeSupport.transform(plan, expr -> expr.accept(builder, null));
    }

    /**
     * Assembles a BaselinePlan from the parameterized trees and the persisted scalar
     * fields (bindSql / planSql texts, value-free digest / hash of the bind tree, cost).
     * Shared by every creation entry so the digest / hash / tree wiring stays in one
     * place.
     */
    private static BaselinePlan assembleBaseline(LogicalPlan bindPlan, LogicalPlan parameterizedBind,
            LogicalPlan parameterizedPlan, String bindSql, String planSql, double cost,
            String catalog, String db) {
        BaselinePlan baseline = new BaselinePlan();
        baseline.setBindSql(bindSql);
        // value-free full-query digest of the bind tree (Level 1/2 matching key). The
        // digest is computed on a namespace-qualified copy of the bind tree so unqualified
        // relations are keyed by the CREATION catalog/db: the same text under a different
        // namespace must not match (see SPMPlanTreeSupport.namespaceQualified; CTE-alias
        // references are exempt because they bind inside the query itself).
        // Consistency with the placeholder parameterization: toSpmDigest() renders every
        // Expression-leaf Literal as "?" through the generic digest machinery, while
        // SPMPlaceholderBuilder walks the same whole tree - both normalize exactly the
        // literals SPM can parameterize. The digest is only a cheap Level 1/2
        // pre-filter; Level 3 (the placeholder-tree structural match) is authoritative,
        // so even a future divergence between the two mechanisms (e.g. a node whose
        // toDigest leaks a literal value) can only cause a missed rewrite (safe), never
        // a wrong rewrite.
        String digest = SPMPlanTreeSupport.namespaceQualified(bindPlan, catalog, db).toSpmDigest();
        baseline.setBindSqlDigest(digest);
        baseline.setBindSqlHash(SPMUtils.hashOf(digest));
        baseline.setPlanSql(planSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setParameterizedPlanPlan(parameterizedPlan);
        baseline.setCost(cost);
        return baseline;
    }

    /** Parses a single SQL text into an unbound logical plan (rejects non-SELECT). */
    private static LogicalPlan parseSelect(String sql, String errorMessage) throws UserException {
        Plan parsed = new NereidsParser().parseSingle(sql);
        if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
            throw new AnalysisException(errorMessage);
        }
        return (LogicalPlan) parsed;
    }

    /**
     * Rebuilds the transient parameterized trees of a persisted baseline after an FE
     * restart / during the periodic refresh (the trees are not stored in the internal
     * table). Both texts are parsed and parameterized with ONE shared builder in the SAME
     * order the CREATE path uses (bind first, then plan), so the placeholder ids of the
     * two trees stay aligned even when bindSql and planSql differ - a value extracted from
     * the bind tree can never be substituted into an id that belongs to a different
     * literal of the plan text. Parsing plus whole-tree parameterization is pure AST work
     * and needs no catalog / session, so it can run during the startup load.
     *
     * @param bindSql the stored bindSql (a parse failure skips the row)
     * @param planSql the stored planSql, or null when the baseline replays a frozen
     *                (placeholder-carrying) text and needs no plan tree
     * @return (parameterized bind tree, parameterized plan tree); first is null when the
     *         bindSql cannot be parsed, second is null when planSql is null / cannot be
     *         parsed (the caller keeps the row either way)
     */
    public static Pair<LogicalPlan, LogicalPlan> rebuildParameterizedTrees(
            String bindSql, String planSql) {
        LogicalPlan bindPlan;
        try {
            bindPlan = parseSelect(bindSql, "SPM rebuild parameterized bind tree: " + bindSql);
        } catch (Throwable t) {
            LOG.warn("SPM rebuild parameterized bind tree failed: {}", t.getMessage());
            bindPlan = null;
        }
        if (bindPlan == null) {
            return Pair.of(bindPlan, bindPlan); // both null: the caller skips the row
        }
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(
                bindPlan, expr -> expr.accept(builder, null));
        if (planSql == null || bindSql.equals(planSql)) {
            // bind == plan: one shared parameterized tree serves both roles (like CREATE)
            return Pair.of(parameterizedBind, parameterizedBind);
        }
        try {
            LogicalPlan planPlan =
                    parseSelect(planSql, "SPM rebuild parameterized plan tree: " + planSql);
            LogicalPlan parameterizedPlan = SPMPlanTreeSupport.transform(
                    planPlan, expr -> expr.accept(builder, null));
            return Pair.of(parameterizedBind, parameterizedPlan);
        } catch (Throwable t) {
            LOG.warn("SPM rebuild parameterized plan tree failed: {}", t.getMessage());
            LogicalPlan noPlanTree = null;
            return Pair.of(parameterizedBind, noPlanTree);
        }
    }
}
