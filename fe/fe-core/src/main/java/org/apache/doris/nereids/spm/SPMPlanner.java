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
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;

import com.google.common.annotations.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * SPMPlanner - the controller of the whole-query SPM bind and rewrite.
 *
 * SPM works on the WHOLE parsed (still unbound) SELECT plan tree
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
 *    placeholder id, so the rewrite reproduces the frozen optimal structure.
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
     * The matched baseline OBJECT (see verifyReplayMetadata): the post-plan
     * validation must keep the fingerprint of the SAME incarnation the match used, so a
     * concurrent DROP / refresh cannot make it silently skip the check.
     */
    private BaselinePlan usedBaseline;

    /**
     * Returns the id of the baseline used by the last successful rewrite.
     *
     * @return the baseline id, or -1 when the last rewrite did not hit a baseline
     */
    public long getUsedBaselineId() {
        return usedBaselineId;
    }

    /** The matched baseline object (see verifyReplayMetadata). */
    public BaselinePlan getUsedBaseline() {
        return usedBaseline;
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
        // SELECT ... INTO OUTFILE writes to a destination that lives OUTSIDE the plan
        // expressions: the generic match cannot see a different path / format and the
        // rewritten tree keeps the CAPTURED sink fields, so a replay would export to the
        // baseline's destination. SPM refuses to rewrite such statements (the original
        // query runs normally, which is always correct).
        if (SPMPlanTreeSupport.containsFileSink(userPlan)) {
            LOG.info("SPM tryRewritePlan: the statement writes to a file sink (OUTFILE); keeping"
                    + " the original plan");
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
        // A caller SET_VAR hint is applied by EliminateLogicalSelectHint while the
        // statement is planned; the SPM replay replaces the whole tree BEFORE that pass
        // and the frozen text carries no hint, so the replay would run under the
        // session's ORIGINAL settings - a SET_VAR(time_zone='+08:00') caller matching an
        // identical baseline evaluated from_unixtime(epoch_col) in its own zone and
        // changed result values. The caller's SET_VAR hints are re-attached to the
        // replayed tree at the return sites (see SPMPlanTreeSupport#carrySetVarHints);
        // the hint writes a STATEMENT-scoped session variable, so its block does not
        // matter.
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
        String queryDigest = SPMPlanTreeSupport.canonicalSpmDigest(matchPlan.toSpmDigest());
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
        // Schema-identity guard state (computed lazily, once per query, only after the
        // first structural match): see the check inside the loop.
        boolean schemaChecked = false;
        String currentSchemaFingerprint = null;
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
            // Schema-identity guard (see BaselinePlan#schemaFingerprint): the bind key is
            // built from the STILL-UNBOUND query, so `SELECT * FROM t WHERE k = 1` keeps
            // the same digest and Level-3 tree after `ALTER TABLE t ADD COLUMN extra`
            // (or a DROP + CREATE of t), while the frozen plan still emits the
            // creator-time output columns - the matched replay would silently return the
            // old column set. A baseline whose referenced-table fingerprint no longer
            // matches fails closed (the user query keeps its own plan); pre-column rows
            // without a fingerprint skip the check. The current fingerprint is computed
            // once per query, and only after a structural match (hot path pays nothing).
            // The stored fingerprint is the BIND-SIDE tables UNION the PLAN-side tables
            // of the frozen plan; here - BEFORE the query is planned - only the bind side
            // can be recomputed, so the guard checks that every current bind-side entry is
            // still present (a DROP + CREATE / ALTER REPLACES its entry, so drift is still
            // caught). The full comparison, plan side included, runs after planning
            // (see verifyReplayMetadata).
            String bindFingerprint = candidate.getSchemaFingerprint();
            if (bindFingerprint != null && !bindFingerprint.isEmpty()) {
                if (!schemaChecked) {
                    schemaChecked = true;
                    currentSchemaFingerprint =
                            SPMPlanTreeSupport.schemaFingerprint(ctx, userPlan);
                }
                if (!SPMPlanTreeSupport.schemaFingerprintBindSideContained(
                        bindFingerprint, currentSchemaFingerprint)) {
                    LOG.info("SPM tryRewritePlan: baseline {} skipped: the referenced table"
                            + " schema changed since the baseline was created", candidate.getId());
                    continue;
                }
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
                // the frozen sink pinned the CAPTURED output labels: expose the caller's
                // own ones (a value variant must not report the captured header). A shape
                // whose labels CANNOT be aligned skips this baseline instead of exposing
                // the captured header (see UnalignableOutputLabelsException).
                LogicalPlan alignedReplay;
                try {
                    alignedReplay = SPMPlanTreeSupport.alignRootOutputLabels(rewritten,
                            matchPlan);
                } catch (SPMPlanTreeSupport.UnalignableOutputLabelsException unalignable) {
                    LOG.info("SPM tryRewritePlan: baseline {} skipped: {}",
                            candidate.getId(), unalignable.getMessage());
                    continue;
                }
                LogicalPlan replay = SPMPlanTreeSupport.mergeLimits(alignedReplay, matchPlan);
                if (!limitContractPreserved(replay, matchPlan, bindTree)
                        || !SPMPlanTreeSupport.orderContractPreserved(replay, matchPlan)) {
                    LOG.info("SPM tryRewritePlan: baseline {} skipped: its plan cannot carry"
                                    + " the caller's LIMIT / OFFSET / ORDER BY contract"
                                    + " (caller {}, replayed {}, replayedCapsWithinCaller={},"
                                    + " callerCapsSurvive={}, callerOrder={}, replayOrder={})",
                            candidate.getId(),
                            Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(matchPlan)),
                            Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(replay)),
                            SPMPlanTreeSupport.rowLimitsWithin(replay, matchPlan),
                            SPMPlanTreeSupport.rowLimitsSurviveReplay(replay, matchPlan),
                            SPMPlanTreeSupport.rootOrderContractForTest(matchPlan),
                            SPMPlanTreeSupport.rootOrderContractForTest(replay));
                    continue;
                }
                usedBaselineId = candidate.getId();
                usedBaseline = candidate;
                return SPMPlanTreeSupport.carrySetVarHints(replay, matchPlan);
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
            // same label contract as the frozen path: the caller's own headers win; an
            // unalignable shape skips the candidate instead of exposing captured headers
            LogicalPlan fallbackAligned;
            try {
                fallbackAligned = SPMPlanTreeSupport.alignRootOutputLabels(rewritten, matchPlan);
            } catch (SPMPlanTreeSupport.UnalignableOutputLabelsException unalignable) {
                LOG.info("SPM tryRewritePlan: baseline {} skipped: {}",
                        candidate.getId(), unalignable.getMessage());
                continue;
            }
            LogicalPlan replay = SPMPlanTreeSupport.mergeLimits(fallbackAligned, matchPlan);
            // The cap/order identity compared below is NAME-SPELLING sensitive (the table
            // qualifiers AND the CTE aliases are part of the key, see
            // SPMPlanTreeSupport#collectRowLimits) and the two sides spell the same query
            // differently: the caller's tree was NAMESPACE-QUALIFIED for matching, while
            // the stored parameterized tree keeps the names as written (unqualified, and
            // with the ORIGINAL CTE aliases - the decompiled frozen text of other baselines
            // carries the regenerated ones). Compare against a namespace-qualified VIEW of
            // the fallback replay so an identical-text baseline is not rejected by
            // spelling: qualification resolves the same session defaults the planner will
            // apply to the bare names when the replayed tree is actually planned, so the
            // view describes exactly what the replay will read.
            LogicalPlan capView = SPMPlanTreeSupport.namespaceQualified(replay,
                    captureCatalogName(ctx), captureDatabaseName(ctx));
            if (!limitContractPreserved(capView, matchPlan, bindTree)
                    || !SPMPlanTreeSupport.orderContractPreserved(capView, matchPlan)) {
                LOG.info("SPM tryRewritePlan: baseline {} skipped: its plan cannot carry"
                                + " the caller's LIMIT / OFFSET / ORDER BY contract"
                                + " (caller {}, replayed {}, replayedCapsWithinCaller={},"
                                + " callerCapsSurvive={}, callerOrder={}, replayOrder={})",
                        candidate.getId(),
                        Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(matchPlan)),
                        Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(replay)),
                        SPMPlanTreeSupport.rowLimitsWithin(capView, matchPlan),
                        SPMPlanTreeSupport.rowLimitsSurviveReplay(capView, matchPlan),
                        SPMPlanTreeSupport.rootOrderContractForTest(matchPlan),
                        SPMPlanTreeSupport.rootOrderContractForTest(capView));
                continue;
            }
            usedBaselineId = candidate.getId();
            usedBaseline = candidate;
            return SPMPlanTreeSupport.carrySetVarHints(replay, matchPlan);
        }
        return null;
    }

    /**
     * Whether the CALLER's top-level LIMIT / OFFSET is honored by the replayed tree.
     *
     * Top-level limit VALUES are deliberately ignored by the match (a limit variant
     * reuses the baseline and gets its own values transferred by mergeLimits), but the
     * transfer is a POSITIONAL merge: it only replaces values at nodes that align with
     * the caller's tree. A manual plan may keep its OWN limit below a node the merge
     * cannot align - bind 'SELECT k FROM t ORDER BY k LIMIT 1' WITH 'SELECT
     * DISTINCT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s' - and replaying it for a
     * caller asking LIMIT 2 would then return the CAPTURED row count (one row instead of
     * two distinct keys). Such a candidate is skipped unless the caller asks for exactly
     * the captured limit / offset, in which case the plan's own placement already IS the
     * caller's contract.
     *
     * @param replayed the replayed tree AFTER the merge (its top-level limit is the one
     *                 the caller would observe)
     * @param userPlan the caller's own tree (top-level limit source)
     * @param bindTree the stored parameterized bind tree (captured limit values)
     * @return whether the replay may be used for this caller
     */
    private static boolean limitContractPreserved(LogicalPlan replayed, LogicalPlan userPlan,
            LogicalPlan bindTree) {
        long[] userLimit = SPMPlanTreeSupport.topLevelLimitOf(userPlan);
        if (userLimit == null) {
            // The caller asks for NO top-level cap: the capture-side LIMIT
            // VALUES are NOT the caller's contract here, so the replay must carry no
            // row-limiting node at all. A manual planSql like 'SELECT k FROM t LIMIT 1'
            // frozen over an unbounded bind otherwise passed creation and this early
            // return silently returned ONE row for every matching unbounded SELECT.
            return SPMPlanTreeSupport.rowLimitsWithin(replayed, userPlan);
        }
        if (Arrays.equals(userLimit, SPMPlanTreeSupport.topLevelLimitOf(replayed))) {
            // The caller's own top-level limit is in place. Every OTHER cap the replay
            // still exposes must be one the caller's own tree also has:
            // equal OUTER limits do not make the plans equivalent - a manual plan
            // 'SELECT k FROM (SELECT k FROM t ORDER BY k ASC LIMIT 2) s ORDER BY k DESC
            // LIMIT 2' passes the single-scan CREATE guard and answers a LIMIT 2 caller
            // with (2,1) although the caller's own plan returns (3,2): its INNER cap
            // truncates a DIFFERENT slice before the outer sort. Only the captured caps
            // themselves may justify the replay's caps - the captured (bind) tree is
            // what the caller's own tree structurally matched, so an identical-text
            // baseline keeps passing. additionally requires the CALLER's
            // caps to survive: 'SELECT k FROM (SELECT k FROM t ORDER BY k LIMIT 1) s
            // ORDER BY k LIMIT 2' against a manual plan carrying only the outer cap
            // passed the one-directional check (replayed caps ⊆ caller caps) although
            // the replay returns TWO rows where the caller returns one.
            if (SPMPlanTreeSupport.rowLimitsWithin(replayed, userPlan)
                    && SPMPlanTreeSupport.rowLimitsSurviveReplay(replayed, userPlan)) {
                return true;
            }
            // The captured-cap justification the comment above promises: the frozen
            // replay is the decompiled OPTIMIZED plan of the BIND tree, and the
            // optimizer folds a top-level LIMIT n into the plan (one cap per input
            // scan, all with the SAME n) - so the IDENTICAL query's frozen plan
            // legitimately exposes caps the RAW caller tree has no node for. Rejecting
            // those skipped the original query's own baseline: tpcds q28/q77 re-ran
            // their own query and got NO hit although caller and replay limits are
            // identical. Three guards keep the reviewer's cases rejected:
            //  - the top-level LIMIT must be UNCHANGED from the BIND tree's own: a
            //    variant that changed the limit transfers the value POSITIONALLY and
            //    the captured inner caps would truncate its different slice - unless the
            //    replay carries NO cap besides the (merged) top one, where nothing can
            //    truncate a different slice at all (tpcds q76's LIMIT 101 variant);
            //  - every replay cap must be one the caller's own tree has, or a
            //    same-valued pushdown of the caller's own top-level cap, with the
            //    caller asking for NO order contract (an equal-valued inner cap below
            //    an ordering-dependent slice truncates a DIFFERENT slice);
            //  - every caller cap must still survive in the replay.
            boolean topUnchangedFromBind = Arrays.equals(userLimit,
                    SPMPlanTreeSupport.topLevelLimitOf(bindTree));
            if ((topUnchangedFromBind || SPMPlanTreeSupport.onlyTopCap(replayed))
                    && (SPMPlanTreeSupport.topCapContractPreserved(replayed, userPlan)
                            || (topUnchangedFromBind
                                    && SPMPlanTreeSupport.rowLimitsSurviveReplay(
                                            replayed, userPlan)
                                    && SPMPlanTreeSupport.replayCapsJustifiedByCallerTopLimit(
                                            replayed, userPlan)))) {
                return true;
            }
            return false;
        }
        // Matching the CAPTURED limit VALUE is not enough - the replayed tree
        // must actually CARRY the caller's cap. A manual plan 'SELECT k FROM t' freezes a
        // text with NO limit node: mergeLimits only replaces the VALUE of a cap that
        // aligns positionally, it cannot add a missing one, so accepting this replay
        // returned every row although the caller asked for the captured LIMIT 2 (the two
        // branches above are the only proofs that the cap survived - equal top-level caps
        // plus every nested cap justified).
        return false;
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
        // Persisted provenance first (see BaselinePlan#planFrozen): a row explicitly
        // marked as NOT decompiled (raw user fallback text) must never be replayed as
        // frozen SQL just because its text contains a call shaped like a placeholder -
        // a real db._spm_const_var(1) UDF would otherwise be replaced by the user's
        // literal and return the literal instead of evaluating the function.
        Boolean persistedFrozen = candidate.getPlanFrozen();
        if (persistedFrozen != null && !persistedFrozen) {
            LOG.info("SPM replay baseline {} frozen replay unavailable: the row is marked as"
                    + " raw planSql fallback text (the decompiler refused at creation)",
                    candidate.getId());
            return null;
        }
        String planSql = candidate.getPlanSql();
        if (planSql == null) {
            return null;
        }
        boolean textHasPlaceholder = planSql.contains(SPMFrozenTreeReplacer.CONST_VAR_FUNC)
                || planSql.contains(SPMFrozenTreeReplacer.CONST_LIST_FUNC);
        if (persistedFrozen == null && !textHasPlaceholder) {
            return null; // legacy row, not a frozen (placeholder-carrying) plan text
        }
        // re-parsing the frozen text needs the session context (join-hint / statement
        // context handling); when absent (pure in-memory callers) fall back
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return null;
        }
        try {
            // The frozen text is SPM's own rendering, produced for the DEFAULT sql_mode
            // (backslash escapes active - see SPMPlan2SQLBuilder.quoteSqlString). Pin that
            // mode for this re-parse: inheriting a NO_BACKSLASH_ESCAPES session would
            // decode the doubled backslashes to a different value and could select
            // another external ref.
            Plan parsed = SqlModeHelper.withSqlMode(SqlModeHelper.MODE_DEFAULT,
                    () -> new NereidsParser().parseSingle(planSql));
            if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
                return null;
            }
            // Classify on the PARSED TREE instead of the raw text: the fallback path
            // stores the ORIGINAL planSql when the decompiler rejects a node (e.g.
            // PhysicalAssertNumRows), and that ordinary SQL may merely CONTAIN one of
            // the placeholder function names inside a string literal, identifier or
            // comment. Substituting nothing and returning such a tree would replay the
            // CAPTURED literals - the user's values must go through the parameterized
            // fallback tree instead.
            //
            // The classification only guards rows WITHOUT provenance: when the row is
            // EXPLICITLY marked frozen (planFrozen=true), the text is the decompiler's
            // own rendering, and a literal-free optimized plan legitimately contains no
            // placeholder call - it must replay as-is with an EMPTY substitution map
            // (the frozen structure / hints are the point), not fall back to the
            // original pre-optimization tree.
            boolean parsedHasPlaceholder =
                    SPMPlanTreeSupport.containsFrozenPlaceholder((LogicalPlan) parsed);
            if (persistedFrozen == null && !parsedHasPlaceholder) {
                return null;
            }
            SPMFrozenTreeReplacer replacer = new SPMFrozenTreeReplacer();
            LogicalPlan rewritten = SPMPlanTreeSupport.transform(
                    (LogicalPlan) parsed, expr -> expr.accept(replacer, placeholderValues));
            // safety net: a plan-only placeholder (no user value extracted) would reach
            // the analyzer as an unregistered function - never rewrite with it
            if (SPMPlanTreeSupport.containsFrozenPlaceholder(rewritten)) {
                LOG.info("SPM replay baseline {} frozen replay unavailable: placeholder ids"
                        + " without an extracted value remain after substitution",
                        candidate.getId());
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
            LOG.info("SPM replay baseline {} frozen replay unavailable: the frozen text does"
                            + " not re-parse ({}: {})", candidate.getId(),
                    t.getClass().getSimpleName(), t.getMessage());
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
     * Removes EVERY root LogicalSelectHint (its SET_VAR payload) from the FALLBACK
     * parameterized plan tree. The primary frozen-text path re-parses planSql including
     * its hints deliberately; the in-memory fallback must not re-apply the BASELINE's
     * captured SET_VAR on top of a user query that came with different session
     * variables (a time_zone='+08:00' baseline would override the user's -08:00
     * from_unixtime and return wrong values - the user's own SET_VAR was already applied
     * during parsing). Nested query blocks (subqueries / CTE bodies) are stripped too:
     * an inner hint survives a root-only peel and a plan-side SET_VAR inside a scalar
     * subquery would then be applied during ordinary replay analysis.
     */
    private static LogicalPlan stripSelectHints(LogicalPlan plan) {
        return SPMPlanTreeSupport.stripSelectHints(plan);
    }

    /**
     * Post-planning metadata revalidation of a replayed baseline (the replay-side half
     * of the frozen-metadata binding, see
     * SPMPlanTreeSupport#schemaFingerprintForCreate): the pre-match fingerprint guard
     * runs BEFORE the query planner takes its metadata locks, so an ALTER TABLE
     * committing in between would let the frozen SQL be planned against a DIFFERENT
     * schema than the one it was validated against (and than the one its output slots
     * were frozen with). This re-checks the stored fingerprint against the metadata
     * the REPLAYED plan was actually planned with - the planned PhysicalPlan's catalog
     * relations plus the baseline's own bind tree resolved in this context. A mismatch
     * surfaces as a rewrite failure; the caller's enable_spm_fallback policy decides
     * whether to re-plan the original query (enabled) or surface the error (disabled).
     *
     * @param ctx         the replaying session
     * @param baselineId  the baseline the rewritten tree came from (-1 = none)
     * @param plannedPlan the replayed plan after planning
     */
    public static void verifyReplayMetadata(ConnectContext ctx, long baselineId,
            Plan plannedPlan) {
        if (ctx == null || plannedPlan == null || baselineId < 0) {
            return;
        }
        // Prefer the baseline OBJECT retained on the statement context: re-fetching by id
        // can silently miss it after a concurrent DROP / refresh, and returning then
        // would skip the post-plan schema check exactly when a table DDL may have
        // committed between the pre-match validation and the replay planning.
        BaselinePlan baseline = null;
        if (ctx.getStatementContext() != null
                && ctx.getStatementContext().getSpmUsedBaseline() != null
                && ctx.getStatementContext().getSpmUsedBaseline().getId() == baselineId) {
            baseline = ctx.getStatementContext().getSpmUsedBaseline();
        }
        if (baseline == null) {
            baseline = ctx.getSessionBaselineStore() == null
                    ? null : ctx.getSessionBaselineStore().getBaseline(baselineId);
        }
        if (baseline == null) {
            baseline = BaselineManager.getInstance().getBaseline(baselineId);
        }
        if (baseline == null) {
            // A recorded id whose baseline is gone (concurrent DROP / refresh): the stored
            // fingerprint can no longer be revalidated and the frozen SQL may be a stale
            // SELECT * output list - fail so the caller applies its fallback policy
            // instead of silently keeping the replay.
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM baseline " + baselineId + " disappeared before the replay"
                            + " validation; keeping the original query");
        }
        String stored = baseline.getSchemaFingerprint();
        if (stored == null || stored.isEmpty()) {
            return;
        }
        String current = SPMPlanTreeSupport.schemaFingerprintForReplay(
                ctx, baseline.getParameterizedBindPlan(), plannedPlan, baseline.getPlanSql());
        // tolerance: a row persisted before the nullability section existed carries
        // section-less entries (see SPMPlanTreeSupport#legacyEntryOf); a row written
        // since must match exactly, so a nullability change between the pre-match
        // validation and this post-plan check still fails closed
        if (!SPMPlanTreeSupport.schemaFingerprintEquivalent(stored, current)) {
            LOG.warn("SPM replay metadata mismatch for baseline {}: stored=[{}] current=[{}]",
                    baselineId, stored, current);
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM replay metadata changed between validation and planning; keeping"
                            + " the original query for baseline " + baselineId);
        }
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
        // both CREATE inputs are read inside ONE mode window, exactly like the production
        // entry (parseSelectIsolated): a SET_VAR(sql_mode=...) hint carried by the first
        // text must not decide how the second text is read.
        final long creatorMode = SqlModeHelper.currentMode();
        LogicalPlan bindPlan = parseSelectIsolated(creatorMode, bindSql,
                "SPM bindSql must be a SELECT statement: " + bindSql);
        // bindSql == planSql: one parse is enough and the parameterized trees are the
        // same by construction (ids trivially aligned)
        LogicalPlan planPlan = bindSql.equals(planSql)
                ? bindPlan
                : parseSelectIsolated(creatorMode, planSql,
                        "SPM planSql must be a SELECT statement: " + planSql);
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
        try {
            rejectReplayContextExpressions(bindPlan, planPlan, bindSql);
            // A manual plan may not choose its own scan selection (see the method): the
            // bind text is the matching key, so a divergent selection silently changes
            // which rows the replayed baseline reads.
            SPMPlanTreeSupport.rejectScanSelectorMismatch(bindPlan, planPlan, bindSql);
            // The plan must implement the SAME logical query as
            // the bind text (same sources, output expressions / arity, row filters and
            // top-level ordering) - see the guard for the silent result changes it
            // prevents.
            SPMPlanTreeSupport.rejectManualPlanDivergence(bindPlan, planPlan, bindSql);
        } catch (AnalysisException e) {
            // the checked-exception entry point is buildBaselineFromSql; this test-only
            // overload keeps its signature and surfaces the rejection as-is
            throw new RuntimeException(e.getMessage(), e);
        }
        // key(...) folds a named secret into a constant during optimization and the parsed
        // form of `KEY db.key` is a bound EncryptKeyRef (not an UnboundFunction): reject on
        // BOTH inputs before parameterizing / assembling anything.
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(bindPlan, planPlan);
        Pair<LogicalPlan, LogicalPlan> trees = parameterizeWholeTrees(bindPlan, planPlan);
        // the matching key is namespace-qualified like tryRewritePlan's user side, so the
        // in-memory (UT) create / rewrite pair stays consistent in any context
        ConnectContext ctx = ConnectContext.get();
        BaselinePlan baseline = assembleBaseline(bindPlan, trees.first, trees.second, bindSql, planSql,
                cost, captureCatalogName(ctx), captureDatabaseName(ctx), creatorSqlMode());
        // no decompile happens on this in-memory path: the planSql is ordinary user text
        // (never a frozen rendering), and it is re-parsed with the creator's own mode
        baseline.setPlanFrozen(false);
        baseline.setPlanSqlMode(baseline.getCreatorSqlMode());
        baseline.setSchemaFingerprint(SPMPlanTreeSupport.schemaFingerprint(ctx, bindPlan));
        return baseline;
    }

    /**
     * The parser-relevant sql_mode bits of the CREATING session (see
     * BaselinePlan#getCreatorSqlMode): re-parsing the stored bindSql under the default
     * mode can change its meaning (PIPES_AS_CONCAT: a || b is concat(a, b), not
     * a boolean Or).
     */
    private static long creatorSqlMode() {
        return SqlModeHelper.currentMode();
    }

    /**
     * Rejects statements whose frozen SQL would persist the CREATOR's session state: a
     * session / user variable or current_user() / session_user() / database() /
     * connection_id() survives parameterization, the creator-context optimization then
     * resolves it to a LITERAL, and the frozen planSql persists that value - while
     * matching still compares the original unbound bind tree, so a global baseline would
     * serve every other user the creator's identity / variables. Clock functions are the
     * same class of value: FE constant folding evaluates now() / current_timestamp() from
     * the CREATE statement's start time (see DateTimeAcquire), so a baseline for
     * SELECT now() AS ts FROM t returned the CREATE timestamp on every later
     * matching query.
     */
    private static void rejectReplayContextExpressions(LogicalPlan bindPlan, LogicalPlan planPlan,
            String bindSql) throws AnalysisException {
        if (SPMPlanTreeSupport.containsReplayContextExpression(bindPlan)
                || SPMPlanTreeSupport.containsReplayContextExpression(planPlan)) {
            throw new AnalysisException("SPM does not support replay-time context expressions"
                    + " (user/session variables including a parsed @v, current_user(),"
                    + " session_user(), user(), database(), current_catalog(), connection_id(),"
                    + " last_query_id(), version(), a clock function evaluated at the"
                    + " statement start - now() / current_timestamp() / current_date() /"
                    + " utc_timestamp() / localtime() / unix_timestamp() without arguments -"
                    + " and any * REPLACE payload carrying one):"
                    + " the frozen SQL would persist the creator's value: "
                    + bindSql);
        }
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
        // The 3-arg entry is the GLOBAL create / capture path; temporary tables are
        // rejected there (the frozen planSql would carry the creator session's internal
        // temp name). The CREATE command uses the 4-arg overload to allow a SESSION-scope
        // baseline over the session's own temporary table.
        return buildBaselineFromSql(ctx, bindSql, planSql, true);
    }

    /**
     * Builds a baseline from SQL texts with a SPM-mode optimized planSql, with an explicit
     * storage scope for the mutable-constraint / temporary-table guards.
     *
     * @param ctx        the connect context (catalog + session variables)
     * @param bindSql    the binding SQL (SELECT)
     * @param planSql    the plan SQL (SELECT, may contain SET_VAR hints)
     * @param globalScope whether the baseline is stored in the shared GLOBAL store
     * @return the constructed BaselinePlan (not yet stored)
     * @throws UserException when the SQLs cannot be parsed / planned
     */
    public BaselinePlan buildBaselineFromSql(ConnectContext ctx, String bindSql, String planSql,
            boolean globalScope) throws UserException {
        // Capture the parser-relevant mode of the creating session BEFORE anything is
        // parsed (the hint application below happens during optimization and must never
        // decide what the persisted creatorSqlMode is): the parsers build expressions
        // (including `a || b`) while this mode is in force, and the persisted value must
        // describe exactly that parse. A statement that changes sql_mode through a
        // /*+ SET_VAR(sql_mode=...) */ hint is parsed with the AMBIENT mode; reading the
        // live session after parsing would persist the hint's mode, and after a
        // refresh / restart the stored bindSql would rebuild under that mode while the
        // frozen key was produced under the ambient one - no query could ever match both
        // again and the durable baseline stayed dead.
        final long creatorMode = SqlModeHelper.currentMode();
        // Each parse runs INSIDE a window that pins the captured mode: a SET_VAR(sql_mode=...)
        // hint inside the bind text is applied to the creating session while that text is
        // processed, so the SECOND parse (the plan text, parsed separately) would otherwise
        // read the ALREADY-SWITCHED session mode - `s1 || s2` built under OR in the bind
        // text would then mean CONCAT in the plan text, and the frozen tree would carry an
        // operator the persisted creatorSqlMode cannot express (every later re-parse of the
        // stored bindSql under that mode would disagree with it). The window holds whatever
        // the parse does to the session variable, so both texts are always read under ONE
        // mode.
        LogicalPlan bindPlan = parseSelectIsolated(creatorMode, bindSql,
                "SPM bindSql must be a SELECT statement: " + bindSql);
        LogicalPlan planPlan = bindSql.equals(planSql)
                ? bindPlan
                : parseSelectIsolated(creatorMode, planSql,
                        "SPM planSql must be a SELECT statement: " + planSql);
        // The destination of SELECT ... INTO OUTFILE lives outside the plan expressions:
        // the match cannot compare it and the rewritten tree keeps the captured sink, so
        // replay would export to the baseline's destination. Reject instead of freezing.
        if (SPMPlanTreeSupport.containsFileSink(bindPlan)
                || SPMPlanTreeSupport.containsFileSink(planPlan)) {
            throw new AnalysisException(
                    "SPM does not support SELECT ... INTO OUTFILE statements: " + bindSql);
        }
        rejectReplayContextExpressions(bindPlan, planPlan, bindSql);
        // A manual plan may not choose its own scan selection (see the method): the bind
        // text is the matching key, so a divergent selection silently changes which rows
        // the replayed baseline reads (and the fingerprint cannot see the selectors).
        SPMPlanTreeSupport.rejectScanSelectorMismatch(bindPlan, planPlan, bindSql);
        // The plan must implement the SAME logical query as the
        // bind text (same sources, output expressions / arity, row filters and top-level
        // ordering) - a plan that drops a filter / reads another table / renames an
        // output column / flips the sort silently changes a matching caller's result.
        SPMPlanTreeSupport.rejectManualPlanDivergence(bindPlan, planPlan, bindSql);
        // key(...) folds a named secret into a constant during optimization, and the parsed
        // form of `KEY db.key` is an EncryptKeyRef (NOT an UnboundFunction): reject on BOTH
        // parsed inputs BEFORE optimizing / storing anything.
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(bindPlan, planPlan);
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
        // An inline VALUES cell / * REPLACE payload lives OUTSIDE the placeholder machinery
        // (its literals are matched CONCRETELY), so the creator-context optimization FOLDS
        // a constant-argument zone-sensitive call there (from_unixtime(0) -> the CREATOR's
        // zone literal) into the decompiled text while the caller's identical query still
        // matches the stored tree: a frozen replay would serve the creator's value in every
        // other session zone. Decline the freeze for such a statement - the raw planSql is
        // kept and the rewrite replays the parameterized tree, whose re-planning evaluates
        // the expression in the CALLER's own session (the same degradation the view guard
        // uses).
        boolean foldedSessionSensitivePayload =
                SPMPlanTreeSupport.containsFoldedSessionSensitivePayload(bindPlan)
                        || SPMPlanTreeSupport.containsFoldedSessionSensitivePayload(planPlan);
        // An explicit `FROM t INDEX mv` pin is a SEMANTIC choice (on an aggregate-key
        // table a coarser rollup returns aggregated rows where the base table returns one
        // per record), and the decompiled text cannot carry it: the physical scan only
        // knows the SELECTED index id - which every optimizer-side rollup / MV choice sets
        // as well - while the matching key DOES carry the pin (the digest renders INDEX
        // <name>, so only callers with the same pin match). A frozen "FROM t" would then
        // read the base table for a pinned caller, silently returning finer-grained rows;
        // the replan probe does not catch it (the text is valid) and the schema
        // fingerprint does not hash the selection. Decline the freeze exactly like the
        // view / session-sensitive-payload guards: the raw planSql is kept and the rewrite
        // replays the parameterized tree, whose own analysis re-applies the pin.
        boolean pinsExplicitIndex = SPMPlanTreeSupport.pinsExplicitIndex(bindPlan)
                || SPMPlanTreeSupport.pinsExplicitIndex(planPlan);

        SPMOptimizer.OptimizeResult optimizeResult;
        DecompiledPlan frozen;
        boolean rawPlanOptimized = false;
        try {
            // optimize the parameterized plan tree in SPM mode: placeholders travel
            // through analyze / rewrite / CBO and survive into the physical plan.
            // The tree handed to the optimizer is POLICY-FREE: a creator (non-root ADMIN)
            // may carry a row filter / data mask, and resolving it here would freeze the
            // CREATOR's policy into the planSql as an ordinary predicate / projection -
            // every later user matching the same bind query would replay it, even without
            // such a policy (valid rows disappear / values stay masked). The STORED
            // parameterized trees keep their CHECK markers and a frozen text is re-parsed
            // at replay, so the EXECUTING user's own policy checks still run exactly as
            // for an ordinary query.
            optimizeResult = SPMOptimizer.optimize(ctx,
                    SPMPlanTreeSupport.stripCheckPolicy(parameterizedPlan), planSql);
            frozen = decompileFrozenPlan(referencesView, foldedSessionSensitivePayload,
                    pinsExplicitIndex, optimizeResult, planSql);
        } catch (UserException | RuntimeException e) {
            // When the parameterized tree cannot be planned (e.g. a placeholder cannot
            // survive some analyzer path yet), fall back to optimizing the raw planSql -
            // the CREATE must not fail because of the parameterized-tree optimization.
            // The fallback replays through the parameterized-plan TREE (planFrozen=false),
            // so the stored text must be the ORIGINAL planSql: the decompiled rendering of
            // the RAW plan carries its CONCRETE literals, and the next reload
            // re-parameterizes that TRANSFORMED text (BETWEEN 1 AND 2 comes back as
            // k >= 1 AND k <= 2), whose new placeholder ids the bind tree never extracted -
            // every replay then failed the residue check. The original text maps its
            // literals to the same ids as the bind tree. The fallback tree is stripped of
            // check policy markers for the same reason as above.
            LOG.warn("SPM parameterized plan optimization failed, falling back to raw planSql",
                    e);
            rawPlanOptimized = true;
            optimizeResult = SPMOptimizer.optimize(ctx,
                    SPMPlanTreeSupport.stripCheckPolicy(
                            parseSelectIsolated(creatorMode, planSql,
                                    "SPM planSql must be a SELECT statement: " + planSql)),
                    planSql);
            frozen = new DecompiledPlan(planSql, false);
        }
        // A successful decompilation is not proof the text can be re-planned: the
        // optimizer's output slots may use names the emitted SQL never exposes (an
        // ORDER BY over columns a lower scope dropped). The builder prevents the known
        // forms at the source - the ORDER BY hoist re-exports every key it moves above
        // its scope (hoistableOrderBy / reExportedLabels; the step that made tpcds q78
        // legal: its hoisted "ORDER BY c_14 ..." now re-exports c_4 AS c_14 through the
        // wrapper below) - but a future shape may still slip through, and EVERY replay
        // of such a text fails as a planning error ("SPM rewritten plan failed"), which
        // is worse than the documented fallback; downgrade HERE so the baseline stays
        // usable through the parameterized tree. The check plans the frozen text once -
        // CREATE is a one-shot operation.
        if (frozen.decompiled) {
            frozen = validateFrozenTextReplannable(ctx, frozen, planSql, optimizeResult.getPhysicalPlan());
        }
        if (globalScope) {
            // A GLOBAL baseline must never be frozen over a temporary table: the physical
            // relation's catalog object carries the CREATOR session's internal name
            // (<sessionId>_#TEMP#_<name>). The bind key for "FROM t" is only
            // catalog.db.t, so a second session running the same text matched the same
            // digest, and the frozen SQL (emitting the internal name) resolved the
            // creator's still-live temporary table instead of the second session's t.
            rejectTemporaryRelations(optimizeResult, bindSql);
        }
        // Explicit provenance of the stored planSql, persisted so a reload never has to
        // GUESS whether the text is SPM's decompiled frozen rendering or the user's raw
        // fallback (see BaselinePlan#planFrozen / #planSqlMode).
        //
        // The provenance is the DECOMPILER's success over the PARAMETERIZED tree - NOT
        // the mere presence of a placeholder call: a literal-free optimized join (e.g. a
        // hinted broadcast shuffled join with no literal value) decompiles successfully
        // yet contains no spm_const* call. Recording planFrozen=false for it made every
        // replay reject the frozen text and fall back to the ORIGINAL pre-optimization
        // logical tree - losing exactly the stored join / distribution choice - while
        // after a GLOBAL refresh (the row is rebuilt from the same decompiled planSql)
        // the behavior flipped to frozen.
        //
        // EXCEPTION: the RAW-PLAN fallback re-uses ordinary user text (it never
        // decompiles): replaying it as frozen would skip the user-value substitution and
        // return the captured literals - such a text must keep the parameterized-plan-tree
        // rewrite path (planFrozen=false), exactly like a failed decompile.
        Boolean planFrozen = frozen.decompiled && !rawPlanOptimized;
        long planSqlMode = frozen.decompiled ? SqlModeHelper.MODE_DEFAULT : creatorMode;
        BaselinePlan baseline = assembleBaseline(bindPlan, trees.first, parameterizedPlan, bindSql,
                frozen.sql, optimizeResult.getCost(),
                captureCatalogName(ctx), captureDatabaseName(ctx), creatorMode);
        baseline.setPlanFrozen(planFrozen);
        baseline.setPlanSqlMode(planSqlMode);
        // Canonical digest of the SUBMITTED plan text (NOT the stored / decompiled one):
        // the plan-side identity a follower precomputes to attribute a persisted row to
        // THIS statement - two baselines may share the bind digest with different plan
        // texts (see BaselineManager.ForwardedDdlExpectation). Computed from the RAW
        // parse input, exactly like SPMPlanner#canonicalPlanDigest over the submitted
        // text.
        baseline.setPlanSqlDigest(SPMPlanTreeSupport.canonicalSpmDigest(
                SPMPlanTreeSupport.namespaceQualified(planPlan,
                        captureCatalogName(ctx), captureDatabaseName(ctx)).toSpmDigest()));
        // Schema identity of the referenced tables, validated again before every replay
        // (see SPMPlanTreeSupport#schemaFingerprintForCreate): the PLAN side comes from
        // the OPTIMIZED plan's own relations - the under-lock metadata snapshot the
        // frozen output slots were built from - and the BIND side keeps the bind-tree
        // resolution; the union also covers tables only the stored plan uses. The
        // function names come from the STORED TEXT (frozen.sql), so a re-plan that picks
        // an equivalent builtin with another name (years_add vs date_add) cannot break
        // the symmetric revalidation.
        baseline.setSchemaFingerprint(SPMPlanTreeSupport.schemaFingerprintForCreate(
                ctx, bindPlan, optimizeResult.getPhysicalPlan(), frozen.sql));
        return baseline;
    }

    /**
     * Rejects statements that reference a temporary table. Only the GLOBAL create /
     * capture path calls this: the frozen SQL (or the decompiled text) would carry the
     * creator session's internal sessionId_#TEMP#_name, which Database.getTableNullable
     * accepts unchanged - the replay would read the creator's temporary table from a
     * DIFFERENT session. A SESSION-scope baseline may legitimately target the session's
     * own temporary table: it never leaves the connection, and the decompiler refuses
     * the internal name anyway (raw-text fallback resolves per session).
     */
    private static void rejectTemporaryRelations(SPMOptimizer.OptimizeResult optimizeResult,
            String bindSql) throws AnalysisException {
        final boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(optimizeResult.getPhysicalPlan(), node -> {
            if (!found[0] && node instanceof PhysicalCatalogRelation
                    && ((PhysicalCatalogRelation) node).getTable().isTemporary()) {
                found[0] = true;
            }
        });
        if (found[0]) {
            throw new AnalysisException("SPM does not support GLOBAL baselines over temporary tables: "
                    + bindSql);
        }
    }

    /**
     * The freeze step's result: the planSql text plus its provenance. decompiled
     * is false when the user-supplied planSql text was kept (decompiler rejected the
     * physical plan, or the plan references a view) - such text is user-authored and
     * reloads with the CREATOR's parser mode, and it must never be replayed as frozen
     * placeholder SQL.
     */
    private static final class DecompiledPlan {
        private final String sql;
        private final boolean decompiled;

        private DecompiledPlan(String sql, boolean decompiled) {
            this.sql = sql;
            this.decompiled = decompiled;
        }
    }

    /**
     * Downgrades a decompiled frozen text that cannot be re-planned back to the user
     * planSql (planFrozen=false), the same degradation the decompiler's own refusals
     * use: a replay re-plans the frozen text on EVERY hit, so a text that fails
     * analysis would surface as "SPM rewritten plan failed" for the matching caller
     * instead of a working baseline. The validation is one nested SPM plan of the
     * text; the nested planner applies the same SPM rule mask the capture used, so
     * the analysis / binding that a replay hits first behaves identically.
     */
    private static DecompiledPlan validateFrozenTextReplannable(ConnectContext ctx,
            DecompiledPlan frozen, String planSql, Plan writePlan) {
        try {
            LogicalPlan parsed = parseSelectIsolated(SqlModeHelper.MODE_DEFAULT, frozen.sql,
                    "SPM planSql must be a SELECT statement: " + frozen.sql);
            // Substitute the placeholders with the WRITER plan's own literal payloads
            // before re-planning: the replay substitutes the caller's values by id, so
            // re-planning the raw marker calls can only fail ("Can not found function
            // '_spm_const_var'") and the downgrade would mask every scalar-subquery
            // baseline. List placeholders (IN lists) need the caller's whole predicate,
            // which the capture does not keep - such a text is accepted unvalidated.
            Map<Long, Expression> placeholderValues = collectPlaceholderValues(writePlan);
            SPMFrozenTreeReplacer replacer = new SPMFrozenTreeReplacer();
            LogicalPlan substituted = SPMPlanTreeSupport.transform(parsed,
                    expr -> expr.accept(replacer, placeholderValues));
            if (SPMPlanTreeSupport.containsFrozenPlaceholder(substituted)) {
                return frozen;
            }
            SPMOptimizer.optimize(ctx, substituted, frozen.sql);
            return frozen;
        } catch (UserException | RuntimeException e) {
            LOG.warn("SPM decompiled text cannot be re-planned; storing the user planSql"
                    + " instead (the baseline replays through the parameterized tree)", e);
            return new DecompiledPlan(planSql, false);
        }
    }

    /**
     * Placeholder id -> original literal, collected from the writer plan's SpmConstVar
     * payloads (the value child every placeholder keeps for readability).
     */
    private static Map<Long, Expression> collectPlaceholderValues(Plan plan) {
        Map<Long, Expression> values = new HashMap<>();
        collectPlaceholderValues(plan, values);
        return values;
    }

    private static void collectPlaceholderValues(Plan plan, Map<Long, Expression> values) {
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, (Plan node) -> {
            for (Expression expr : node.getExpressions()) {
                collectPlaceholderValues(expr, values);
            }
        });
    }

    private static void collectPlaceholderValues(Expression expr, Map<Long, Expression> values) {
        if (expr instanceof SpmConstVar) {
            values.putIfAbsent(((SpmConstVar) expr).getId(), ((SpmConstVar) expr).getValue());
        }
        if (expr instanceof SubqueryExpr) {
            Plan subqueryPlan = ((SubqueryExpr) expr).getQueryPlan();
            collectPlaceholderValues(subqueryPlan, values);
        }
        for (Expression child : expr.children()) {
            collectPlaceholderValues(child, values);
        }
    }

    /**
     * Decompiles the optimized physical plan into the frozen planSql. The user-supplied
     * planSql text is kept when the decompiler does not support the plan (recursive CTE,
     * future operators, ...), when the plan references a view (the frozen text is
     * replayed ahead of authorization, and replaying a view's base-table expansion would
     * authorize those base tables instead of the view), when an inline VALUES cell /
     * * REPLACE payload folds a session-zone dependent call the CALLER's session must
     * evaluate for itself (see
     * SPMPlanTreeSupport#containsFoldedSessionSensitivePayload), or when the statement
     * pins a materialized view / rollup with an explicit INDEX clause (the decompiled
     * text cannot carry the pin, and dropping it would read the base table instead of the
     * pinned - possibly coarser - index; see SPMPlanTreeSupport#pinsExplicitIndex).
     */
    private static DecompiledPlan decompileFrozenPlan(boolean referencesView,
            boolean foldedSessionSensitivePayload, boolean pinsExplicitIndex,
            SPMOptimizer.OptimizeResult optimizeResult, String planSql) {
        if (referencesView) {
            LOG.info("SPM freeze skipped: the plan references a view; keeping the user planSql"
                    + " so the rewrite replays the parameterized tree (view authorization"
                    + " preserved)");
            return new DecompiledPlan(planSql, false);
        }
        if (foldedSessionSensitivePayload) {
            LOG.info("SPM freeze skipped: an inline VALUES cell / * REPLACE payload folds a"
                    + " session-time-zone dependent value; keeping the user planSql so the"
                    + " rewrite replays the parameterized tree, where the caller's own"
                    + " session evaluates the expression");
            return new DecompiledPlan(planSql, false);
        }
        if (pinsExplicitIndex) {
            LOG.info("SPM freeze skipped: the statement pins a materialized view / rollup"
                    + " with an explicit INDEX clause, which the decompiled text cannot"
                    + " carry (dropping it would read the base table instead of the pinned"
                    + " index); keeping the user planSql so the rewrite replays the"
                    + " parameterized tree, whose analysis re-applies the pin");
            return new DecompiledPlan(planSql, false);
        }
        try {
            return new DecompiledPlan(
                    new SPMPlan2SQLBuilder().toSQL(optimizeResult.getPhysicalPlan()), true);
        } catch (UnsupportedOperationException e) {
            LOG.info("SPM decompile unsupported ({}):\n{}", e.getMessage(),
                    optimizeResult.getPhysicalPlan().treeString());
            return new DecompiledPlan(planSql, false);
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
        // Every plan-side placeholder must be SUPPLIED by the bind side: matching
        // extracts values only for the bind tree's ids, and both replay paths reject an
        // id they have no value for - such a baseline could never match (a manual plan
        // whose only difference is a renamed table alias parameterizes its literals
        // under a different parent signature: "FROM t a WHERE a.k = 1" vs "FROM t b
        // WHERE b.k = 1"). Reject the CREATE with a clear error instead of storing a
        // permanently unusable row.
        java.util.Set<Long> bindIds = SPMPlanTreeSupport.collectPlaceholderIds(parameterizedBind);
        java.util.Set<Long> planOnly = SPMPlanTreeSupport.collectPlaceholderIds(parameterizedPlan);
        planOnly.removeAll(bindIds);
        if (!planOnly.isEmpty()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "SPM cannot align the plan SQL with the bind SQL:"
                    + " the plan side introduces placeholder literals (ids " + planOnly
                    + ") the bind side never supplies. Most often the two statements"
                    + " differ in a table alias (t a vs t b) or in an expression the"
                    + " optimizer rewrote; write both statements with the same aliases"
                    + " and literals.");
        }
        return Pair.of(parameterizedBind, parameterizedPlan);
    }

    /** Parameterizes one whole tree with the given (possibly shared) builder. */
    private static LogicalPlan parameterizeWholeTree(SPMPlaceholderBuilder builder,
            LogicalPlan plan) {
        // one tree = one block-numbering run: corresponding blocks of the bind tree and
        // the (separately parsed) plan tree keep corresponding numbers
        builder.startNewTree();
        return SPMPlanTreeSupport.transform(plan, builder);
    }

    /**
     * Assembles a BaselinePlan from the parameterized trees and the persisted scalar
     * fields (bindSql / planSql texts, value-free digest / hash of the bind tree, cost).
     * Shared by every creation entry so the digest / hash / tree wiring stays in one
     * place.
     */
    private static BaselinePlan assembleBaseline(LogicalPlan bindPlan, LogicalPlan parameterizedBind,
            LogicalPlan parameterizedPlan, String bindSql, String planSql, double cost,
            String catalog, String db, long creatorSqlMode) {
        // Volatile non-table dependencies (key(...)) are rejected before anything is
        // stored - on both inputs, including a bound EncryptKeyRef; alias-UDF bodies are
        // tracked by the schema fingerprint instead.
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(bindPlan, parameterizedPlan);
        BaselinePlan baseline = new BaselinePlan();
        baseline.setCreatorSqlMode(creatorSqlMode);
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
        // The digest is canonicalized (see SPMPlanTreeSupport#canonicalSpmDigest): the
        // top-level LIMIT / OFFSET values are adopted from the user query at rewrite time
        // and must not split the matching key (an OFFSET query has to reach the
        // structural match that merges its offset).
        String digest = SPMPlanTreeSupport.canonicalSpmDigest(
                SPMPlanTreeSupport.namespaceQualified(bindPlan, catalog, db).toSpmDigest());
        baseline.setBindSqlDigest(digest);
        baseline.setBindSqlHash(SPMUtils.hashOf(digest));
        baseline.setPlanSql(planSql);
        baseline.setParameterizedBindPlan(parameterizedBind);
        baseline.setParameterizedPlanPlan(parameterizedPlan);
        baseline.setCost(cost);
        return baseline;
    }

    /**
     * The canonical (namespace-qualified, value-free) bind digest a CREATE of bindSql
     * persists (see assembleBaseline), computed WITHOUT the optimize / decompile halves.
     * The follower's forwarded-CREATE confirmation uses it as the statement's stable
     * identity (see BaselineManager.ForwardedDdlExpectation#created(String, String,
     * String, String)): an IDEMPOTENT duplicate CREATE is answered by the master with the
     * EXISTING row, whose query id belongs to the FIRST statement and whose raw plan /
     * bind texts are the frozen ones - possibly with different whitespace - while the
     * canonical digest is equal by construction. The forwarded request carries the
     * statement's catalog / database and session variables to the master
     * (MasterOpExecutor sets db / defaultCatalog, ProtocolAdapter the variables), so the
     * digest computed here over the SAME text under the SAME parse mode equals the stored
     * one.
     *
     * @param ctx     the forwarding statement's context (may be null in tests)
     * @param bindSql the forwarded CREATE's bind SQL
     * @return the digest, or "" when the text cannot be parsed / hashed here (the caller
     *         then falls back to the raw identity checks)
     */
    public static String canonicalBindDigest(ConnectContext ctx, String bindSql) {
        return canonicalSqlDigest(ctx, bindSql, "bind");
    }

    /**
     * The CANONICAL DIGEST of a forwarded CREATE's SUBMITTED PLAN SQL (same computation
     * as SPMPlanner#canonicalBindDigest, over the plan text). The two sides are separate
     * identities: two baselines may share the bind digest while carrying different plan
     * texts (one baseline per plan), so a confirmation that only compared the bind
     * digest could accept the OTHER plan's row for this statement - and the persisted
     * plan text cannot be compared directly because the master stores the DECOMPILED
     * rendering.
     *
     * @param ctx     the forwarding statement's context (may be null in tests)
     * @param planSql the forwarded CREATE's plan SQL
     * @return the digest, or "" when the text cannot be parsed / hashed here (the caller
     *         then confirms on the bind digest alone)
     */
    public static String canonicalPlanDigest(ConnectContext ctx, String planSql) {
        return canonicalSqlDigest(ctx, planSql, "plan");
    }

    /**
     * The CURRENT bind-side schema fingerprint of a forwarded CREATE's bind SQL (see
     * SPMPlanTreeSupport#schemaFingerprint): computed on the forwarding FE at forward
     * time, so the confirmation of the created row can reject a matching-but-STALE one -
     * after a schema change the repeated CREATE retires the old row (identical bind /
     * plan digests) and writes the replacement under the new fingerprint, and the
     * retiring DELETE may still be invisible when the follower reads the snapshot (see
     * BaselineManager.ForwardedDdlExpectation).
     *
     * @param ctx     the forwarding statement's context (may be null in tests)
     * @param bindSql the forwarded CREATE's bind SQL
     * @return the fingerprint (possibly empty, never null); "" when the text cannot be
     *         parsed here (the confirmation then falls back to the digest identity)
     */
    public static String canonicalSchemaFingerprint(ConnectContext ctx, String bindSql) {
        try {
            final long creatorMode = SqlModeHelper.currentMode();
            LogicalPlan plan = parseSelectIsolated(creatorMode, bindSql,
                    "SPM bindSql must be a SELECT statement: " + bindSql);
            return SPMPlanTreeSupport.schemaFingerprint(ctx, plan);
        } catch (Throwable t) {
            LOG.debug("SPM cannot precompute the schema fingerprint of a forwarded"
                    + " CREATE ({}); the confirmation falls back to the digest identity",
                    t.getMessage());
            return "";
        }
    }

    /**
     * The SPM match key of a statement - the (query digest, structural hash) pair
     * tryRewritePlan matches baselines on - or null when the text is not a
     * plan-rewritable query (a command / DDL, unparsable text, or a statement batch).
     *
     * The forwarding FE uses the key to carry only the SESSION baselines the forwarded
     * statement could actually match (see SPMForwardedSession#serializeForStatement):
     * the master REBUILDS every carried row before it even classifies the statement
     * (each bind and plan text is re-parsed), so the unfiltered payload let a large
     * session store add that many parses to EVERY forwarded statement - outside
     * spm_rewrite_timeout_ms, which only bounds the planner side.
     *
     * The key is computed exactly like the rewrite's: the same parse mode, the same
     * namespace qualification (the statement's catalog / database travel with the
     * forwarded request, so the master qualifies with the same names) and the same
     * value-free digest.
     *
     * @param ctx the forwarding statement's context (may be null in tests)
     * @param sql the statement text
     * @return the digest plus its structural hash, or null when no baseline can apply
     */
    public static Pair<String, Long> queryMatchKey(ConnectContext ctx, String sql) {
        try {
            LogicalPlan plan = parseSelectIsolated(SqlModeHelper.currentMode(), sql,
                    "SPM query match key needs a query: " + sql);
            LogicalPlan matchPlan = SPMPlanTreeSupport.namespaceQualified(plan,
                    captureCatalogName(ctx), captureDatabaseName(ctx));
            String digest = SPMPlanTreeSupport.canonicalSpmDigest(matchPlan.toSpmDigest());
            return Pair.of(digest, SPMUtils.hashOf(digest));
        } catch (Throwable t) {
            LOG.debug("SPM cannot compute the match key of a forwarded statement ({});"
                    + " its session baselines are carried unfiltered", t.getMessage());
            return null;
        }
    }

    private static String canonicalSqlDigest(ConnectContext ctx, String sql, String side) {
        try {
            final long creatorMode = SqlModeHelper.currentMode();
            LogicalPlan plan = parseSelectIsolated(creatorMode, sql,
                    "SPM " + side + "Sql must be a SELECT statement: " + sql);
            return SPMPlanTreeSupport.canonicalSpmDigest(SPMPlanTreeSupport
                    .namespaceQualified(plan, captureCatalogName(ctx),
                            captureDatabaseName(ctx)).toSpmDigest());
        } catch (Throwable t) {
            LOG.debug("SPM cannot precompute the canonical {} digest of a forwarded"
                    + " CREATE ({}); the confirmation falls back to the raw identity"
                    + " checks", side, t.getMessage());
            return "";
        }
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
     * Parses one CREATE input under the captured creator mode (see buildBaselineFromSql):
     * the mode window keeps a SET_VAR(sql_mode=...) hint of ANOTHER input from leaking into
     * this parse, and keeps this parse's own hint from leaking into the next one.
     */
    private static LogicalPlan parseSelectIsolated(long creatorMode, String sql,
            String errorMessage) throws UserException {
        final UserException[] failure = new UserException[1];
        final LogicalPlan[] result = new LogicalPlan[1];
        SqlModeHelper.withSqlMode(creatorMode, () -> {
            try {
                result[0] = parseSelect(sql, errorMessage);
            } catch (UserException e) {
                failure[0] = e;
            }
            return null;
        });
        if (failure[0] != null) {
            throw failure[0];
        }
        return result[0];
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
            String bindSql, String planSql, long creatorSqlMode) {
        return rebuildParameterizedTrees(bindSql, planSql, creatorSqlMode, SqlModeHelper.MODE_DEFAULT);
    }

    /**
     * Rebuilds the transient parameterized trees with an explicit mode for the plan text
     * (see BaselinePlan#getPlanSqlMode()): the planSql is SPM's decompiled
     * rendering when the freeze succeeded (MODE_DEFAULT) or the user's raw fallback text
     * (the CREATOR's mode) when the physical plan could not be decompiled - re-parsing
     * that fallback under the default mode would turn a PIPES_AS_CONCAT clause into a
     * boolean Or, so the bind tree still matches while the replayed plan computes
     * different projection semantics.
     *
     * @param bindSql        the stored bindSql (a parse failure skips the row)
     * @param planSql        the stored planSql, or null when the baseline replays a
     *                       frozen (placeholder-carrying) text and needs no plan tree
     * @param creatorSqlMode the parser mode of the creating session (bind text)
     * @param planSqlMode    the parser mode of the stored planSql (fallback text);
     *                       MODE_DEFAULT / 0 when unknown
     * @return (parameterized bind tree, parameterized plan tree); first is null when the
     *         bindSql cannot be parsed, second is null when planSql is null / cannot be
     *         parsed (the caller keeps the row either way)
     */
    public static Pair<LogicalPlan, LogicalPlan> rebuildParameterizedTrees(
            String bindSql, String planSql, long creatorSqlMode, long planSqlMode) {
        LogicalPlan bindPlan;
        try {
            // The bindSql is USER-authored text: re-parse it with the SAME parser mode the
            // CREATE used. Under MODE_DEFAULT a PIPES_AS_CONCAT statement's "a || b"
            // rebuilds as a boolean Or, so the stored digest still finds the row while
            // Level-3 structural matching rejects every CONCAT-mode query - the baseline
            // silently stops applying after a reload.
            bindPlan = parseStoredSelect(bindSql, creatorSqlMode);
        } catch (Throwable t) {
            LOG.warn("SPM rebuild parameterized bind tree failed: {}", t.getMessage());
            bindPlan = null;
        }
        if (bindPlan == null) {
            return Pair.of(bindPlan, bindPlan); // both null: the caller skips the row
        }
        SPMPlaceholderBuilder builder = new SPMPlaceholderBuilder();
        LogicalPlan parameterizedBind = SPMPlanTreeSupport.transform(bindPlan, builder);
        if (planSql == null || bindSql.equals(planSql)) {
            // bind == plan: one shared parameterized tree serves both roles (like CREATE)
            return Pair.of(parameterizedBind, parameterizedBind);
        }
        try {
            // A DIFFERENT planSql is either SPM's decompiled rendering (always emitted for
            // the default mode) or the user's raw fallback text: the persisted
            // planSqlMode distinguishes them (legacy rows default to MODE_DEFAULT).
            LogicalPlan planPlan = parseStoredSelect(planSql, planSqlMode);
            // Mirror the CREATE path (parameterizeWholeTree): every text starts its own
            // block-numbering run. Without the reset the bind tree's nested literal
            // advances the shared counter, the SAME nested literal of the (separately
            // parsed) plan text gets a different block id, cannot reuse the bind
            // placeholder, and the placeholder-residue check then rejects a baseline
            // that worked before a refresh / restart.
            builder.startNewTree();
            LogicalPlan parameterizedPlan = SPMPlanTreeSupport.transform(planPlan, builder);
            return Pair.of(parameterizedBind, parameterizedPlan);
        } catch (Throwable t) {
            LOG.warn("SPM rebuild parameterized plan tree failed: {}", t.getMessage());
            LogicalPlan noPlanTree = null;
            return Pair.of(parameterizedBind, noPlanTree);
        }
    }

    /**
     * Rebuilds the transient parameterized trees with the default parser mode (tests /
     * rows persisted before the sql_mode column existed).
     */
    public static Pair<LogicalPlan, LogicalPlan> rebuildParameterizedTrees(
            String bindSql, String planSql) {
        return rebuildParameterizedTrees(bindSql, planSql, SqlModeHelper.MODE_DEFAULT);
    }

    /**
     * Whether a STORED planSql is a FROZEN (placeholder-carrying) text. The classifier
     * parses the text (SPM-authored text is always rendered for the default mode) and
     * checks the tree for REAL placeholder calls: a raw substring test would misclassify
     * an ordinary fallback text whose literal / identifier merely contains one of the
     * names - the replay would return the CAPTURED literals and the reload would discard
     * the parameterized fallback tree (see the caller in BaselineManager#parsePersistedRow).
     *
     * @param planSql the stored planSql
     * @return true when the text re-parses into a tree carrying placeholder calls
     */
    public static boolean isFrozenPlanSql(String planSql) {
        return isFrozenPlanSql(planSql, null);
    }

    /**
     * Whether a STORED planSql is a FROZEN (placeholder-carrying) text, honouring the
     * PERSISTED provenance when the row carries it (see
     * BaselinePlan#getPlanFrozen()): an explicit flag removes every
     * classification guess, so a raw-fallback text that merely CONTAINS a placeholder
     * name (e.g. a real db._spm_const_var(1) UDF call) keeps its parameterized
     * fallback tree instead of being replayed as frozen SQL. Pre-column rows fall back
     * to the parse-based classifier.
     *
     * @param planSql          the stored planSql
     * @param persistedFrozen  the persisted provenance, or null when absent
     * @return true when the text is the SPM decompiled placeholder rendering
     */
    public static boolean isFrozenPlanSql(String planSql, Boolean persistedFrozen) {
        if (persistedFrozen != null) {
            return persistedFrozen;
        }
        if (planSql == null
                || (!planSql.contains(SPMFrozenTreeReplacer.CONST_VAR_FUNC)
                && !planSql.contains(SPMFrozenTreeReplacer.CONST_LIST_FUNC))) {
            return false;
        }
        try {
            return SPMPlanTreeSupport.containsFrozenPlaceholder(parseStoredSelect(planSql));
        } catch (Throwable t) {
            // unparsable text is not a replayable frozen text; treat it as ordinary so
            // the row still loads (with its parameterized fallback when it parses there)
            return false;
        }
    }

    /**
     * Parses a STORED SQL text (frozen planSql, persisted bindSql / planSql). SPM renders
     * its own semantic string literals for the DEFAULT sql_mode (backslash escapes
     * active), so a stored text must be read with that mode: under NO_BACKSLASH_ESCAPES the
     * doubled backslashes would decode to a different value. The mode is PINNED instead of
     * inherited - the text is SPM's, not the user's (user SQL is parsed unchanged).
     */
    private static LogicalPlan parseStoredSelect(String sql) {
        return parseStoredSelect(sql, SqlModeHelper.MODE_DEFAULT);
    }

    /**
     * Parses a STORED SQL text with an explicit parser mode (see
     * parseStoredSelect(String)): SPM-authored texts are pinned to
     * MODE_DEFAULT, a user-authored bindSql keeps the mode it was created with.
     */
    private static LogicalPlan parseStoredSelect(String sql, long sqlMode) {
        long mode = (sqlMode & SqlModeHelper.MODE_ALLOWED_MASK) == 0
                ? SqlModeHelper.MODE_DEFAULT : (sqlMode & SqlModeHelper.MODE_ALLOWED_MASK);
        Plan parsed = SqlModeHelper.withSqlMode(mode,
                () -> new NereidsParser().parseSingle(sql));
        if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
            throw new RuntimeException("SPM stored SQL is not a SELECT statement: " + sql);
        }
        return (LogicalPlan) parsed;
    }
}
