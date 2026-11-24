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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.spm.builder.SPMPlan2SQLBuilder;
import org.apache.doris.nereids.spm.manager.BaselineManager;
import org.apache.doris.nereids.spm.manager.SessionBaselineStore;
import org.apache.doris.nereids.spm.matcher.SPMFrozenTreeReplacer;
import org.apache.doris.nereids.spm.matcher.SPMPlaceholderReplacer;
import org.apache.doris.nereids.spm.placeholder.SPMPlaceholderBuilder;
import org.apache.doris.nereids.spm.placeholder.SpmConstList;
import org.apache.doris.nereids.spm.placeholder.SpmConstVar;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.SubqueryExpr;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.VariableMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Creates baseline metadata and attempts query replay. Creation parameterizes supported literal positions in bind
 * and plan trees with one builder, optimizes the plan, and stores either decompiled frozen SQL or the original SQL
 * for fallback planning.
 *
 * Replay uses a structural hash, exact canonical digest and tree comparison to select candidates and collect
 * replacements. It tries frozen SQL first, then the parameterized fallback tree when available. Output labels, row
 * limits, ordering and metadata are checked before the caller plans the returned unbound tree. No usable match
 * returns null so the caller can plan the original query.
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
     * Returns: the baseline id, or -1 when the last rewrite did not hit a baseline
     */
    public long getUsedBaselineId() {
        return usedBaselineId;
    }

    /** The matched baseline object (see verifyReplayMetadata). */
    public BaselinePlan getUsedBaseline() {
        return usedBaseline;
    }

    // query rewrite (called from StmtExecutor / EXPLAIN).

    /**
     * Finds SESSION candidates before GLOBAL candidates and attempts replay using extracted marker values. Returns
     * a usable unbound plan with caller labels, limits and SET_VAR hints, or null if no candidate can be used. The
     * epoch-millisecond deadline is checked before matching each candidate; it is not a hard interruption of
     * parsing, matching or substitution.
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
        // SELECT... INTO OUTFILE writes to a destination that lives OUTSIDE the plan
        // expressions: the generic match cannot see a different path / format and the
        // rewritten tree keeps the CAPTURED sink fields, so a replay would export to the
        // baseline's destination. SPM refuses to rewrite such statements (the original
        // query runs normally, which is always correct).
        if (SPMPlanTreeSupport.containsFileSink(userPlan)) {
            LOG.info("SPM tryRewritePlan: the statement writes to a file sink (OUTFILE); keeping"
                    + " the original plan");
            return null;
        }
        // Matching includes the caller's hints against bindSql. Replay uses planSql, whose SET_VAR hints
        // can differ or be absent after capture. Remove captured settings and attach the caller's own ones
        // at both return sites so planning the replacement keeps the query's statement-scoped assignments.
        // Level 1/2 use the FULL-QUERY value-free digest (Plan.toSpmDigest renders every
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
                BaselineManager.getInstance().findCandidateBaselines(queryDigest, queryHash, deadline));
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
        SPMPlanTreeSupport.ReplayContracts userContracts = new SPMPlanTreeSupport.ReplayContracts(matchPlan);
        for (BaselinePlan candidate : candidates) {
            if (System.currentTimeMillis() > deadline) {
                LOG.info("SPM tryRewritePlan: timeout before matching baseline {}, "
                        + "degrade to the original plan", candidate.getId());
                return null;
            }
            // A permissive capture can freeze an implicit ANY_VALUE. Strict callers must
            // analyse their own grouping expressions before that permissive result can be used.
            if ((candidate.getCreatorSqlMode() & SqlModeHelper.MODE_ONLY_FULL_GROUP_BY) == 0
                    && (ctx.getSessionVariable().getSqlMode() & SqlModeHelper.MODE_ONLY_FULL_GROUP_BY) != 0) {
                continue;
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
            // Older frozen rows may already contain a permissively folded conversion.
            // A strict caller must analyze the concrete cast instead of consuming that value.
            if (ctx != null && ctx.getSessionVariable().enableStrictCast
                    && !Boolean.FALSE.equals(candidate.getPlanFrozen())
                    && SPMPlanTreeSupport.containsFoldedStrictCastPayload(matchPlan)) {
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
            // Schema-identity guard (see BaselinePlan.schemaFingerprint): the bind key is
            // built from the STILL-UNBOUND query, so SELECT * FROM t WHERE k = 1 keeps
            // the same digest and Level-3 tree after ALTER TABLE t ADD COLUMN extra
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
            // Replay the frozen plan SQL. A baseline whose frozen planSql carries
            // placeholder calls (decompiled from the SPM-optimized physical plan, structure
            // + join distribution hints preserved) is re-parsed and the user values are
            // substituted by placeholder id - the rewrite reproduces the frozen optimal
            // structure for normal re-planning. Baselines without frozen SQL
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
                SPMPlanTreeSupport.ReplayContracts replayContracts = new SPMPlanTreeSupport.ReplayContracts(replay);
                // The cap contract keys every row cap by the OCCURRENCE of the relation it
                // truncates, and the occurrence is the relation's ordinal in the tree's walk
                // order. That ordinal is only stable when the caller's tree and the FROZEN plan
                // render the occurrences in the same relative order - which the optimizer need
                // not do: it may place a derived table on the OTHER side of a JOIN than the user
                // wrote it (a derived-table TopN joined with the same table), and the very same
                // cap then keys as t#0 on one side and t#1 on the other. For a caller that runs
                // the statement the frozen plan was BUILT FROM (its canonical plan digest equals
                // the baseline's planSqlDigest) the cap contract cannot prove anything the text
                // identity does not already prove, so the replay is kept; any other caller - a
                // value variant, whose digest matches but whose placement could genuinely differ -
                // still has to satisfy it (see the corresponding regression test).
                // The digest alone is VALUE-FREE (every literal is masked), so it cannot carry
                // this exception on its own: a caller raising only the outer
                // LIMIT from 1 to 3 has the SAME digest, yet its query legitimately needs the
                // optimizer-generated input caps rebuilt - skipping the cap contract there
                // replayed one pair where the caller returns three. The exception therefore also
                // requires the caller to carry EXACTLY the row caps the baseline was created
                // the SAME cap multiset as the replayed tree, modulo the occurrence ordinal the
                // optimizer's own reordering flips: any LIMIT / OFFSET the caller changed - or an
                // optimizer-generated input cap the caller's tree has no counterpart for - breaks
                // the equality and takes the strict path.
                boolean sameFrozenStatement = candidate.getPlanSqlDigest() != null
                        && candidate.getPlanSqlDigest().equals(queryDigest)
                        && SPMPlanTreeSupport.rowLimitsEquivalentModuloOccurrence(replayContracts, userContracts);
                if (!SPMPlanTreeSupport.orderContractPreserved(replayContracts, userContracts)
                        || (!sameFrozenStatement
                                && !limitContractPreserved(replayContracts, userContracts, bindTree))) {
                    LOG.info("SPM tryRewritePlan: baseline {} skipped: its plan cannot carry"
                                    + " the caller's LIMIT / OFFSET / ORDER BY contract"
                                    + " (caller {}, replayed {}, replayedCapsWithinCaller={},"
                                    + " callerCapsSurvive={}, callerOrder={}, replayOrder={})",
                            candidate.getId(),
                            Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(matchPlan)),
                            Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(replay)),
                            SPMPlanTreeSupport.rowLimitsWithin(replayContracts, userContracts),
                            SPMPlanTreeSupport.rowLimitsSurviveReplay(replayContracts, userContracts),
                            userContracts.order(),
                            replayContracts.order());
                    continue;
                }
                usedBaselineId = candidate.getId();
                usedBaseline = candidate;
                return SPMPlanTreeSupport.attachCallerSetVarHints(replay, matchPlan);
            }
            LOG.info("SPM tryRewritePlan: baseline {} frozen planSql replay unavailable, "
                    + "falling back to parameterized plan tree", candidate.getId());
            LogicalPlan planTree = SPMPlanTreeSupport.stripSetVarHints(rawReplayTree(candidate));
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
            // SPMPlanTreeSupport.collectRowLimits) and the two sides spell the same query
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
            SPMPlanTreeSupport.ReplayContracts replayContracts = new SPMPlanTreeSupport.ReplayContracts(capView);
            if (!limitContractPreserved(replayContracts, userContracts, bindTree)
                    || !SPMPlanTreeSupport.orderContractPreserved(replayContracts, userContracts)) {
                LOG.info("SPM tryRewritePlan: baseline {} skipped: its plan cannot carry"
                                + " the caller's LIMIT / OFFSET / ORDER BY contract"
                                + " (caller {}, replayed {}, replayedCapsWithinCaller={},"
                                + " callerCapsSurvive={}, callerOrder={}, replayOrder={})",
                        candidate.getId(),
                        Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(matchPlan)),
                        Arrays.toString(SPMPlanTreeSupport.topLevelLimitOf(replay)),
                        SPMPlanTreeSupport.rowLimitsWithin(replayContracts, userContracts),
                        SPMPlanTreeSupport.rowLimitsSurviveReplay(replayContracts, userContracts),
                        userContracts.order(),
                        replayContracts.order());
                continue;
            }
            usedBaselineId = candidate.getId();
            usedBaseline = candidate;
            return SPMPlanTreeSupport.attachCallerSetVarHints(replay, matchPlan);
        }
        return null;
    }

    private static LogicalPlan rawReplayTree(BaselinePlan candidate) {
        if (!Boolean.FALSE.equals(candidate.getPlanFrozen())) {
            return candidate.getParameterizedPlanPlan();
        }
        // The stored tree's alias and relation IDs belong to the CREATE or reload context.
        // Rebuild both texts in the executing statement to allocate fresh IDs and reproduce the
        // same bind-to-plan marker mapping. The parser can reuse the stored syntax templates.
        Long planMode = candidate.getPlanSqlMode();
        return rebuildParameterizedTrees(candidate.getBindSql(), candidate.getPlanSql(), candidate.getCreatorSqlMode(),
                planMode == null ? candidate.getCreatorSqlMode() : planMode).second;
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
     * Parameter replayed: the replayed tree AFTER the merge (its top-level limit is the one
     *                 the caller would observe)
     * Parameter userPlan: the caller's own tree (top-level limit source)
     * Parameter bindTree: the stored parameterized bind tree (captured limit values)
     * Returns: whether the replay may be used for this caller
     */
    private static boolean limitContractPreserved(SPMPlanTreeSupport.ReplayContracts replayed,
            SPMPlanTreeSupport.ReplayContracts userPlan, LogicalPlan bindTree) {
        long[] userLimit = userPlan.topLimit();
        if (userLimit == null) {
            // The caller asks for NO top-level cap: the capture-side LIMIT
            // VALUES are NOT the caller's contract here, so the replay must carry no
            // row-limiting node at all. A manual planSql like 'SELECT k FROM t LIMIT 1'
            // frozen over an unbounded bind otherwise passed creation and this early
            // return silently returned ONE row for every matching unbounded SELECT.
            return SPMPlanTreeSupport.rowLimitsWithin(replayed, userPlan);
        }
        if (Arrays.equals(userLimit, replayed.topLimit())) {
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
            // identical. Three guards keep the cases rejected:
            //  the top-level LIMIT must be UNCHANGED from the BIND tree's own: a
            //    variant that changed the limit transfers the value POSITIONALLY and
            //    the captured inner caps would truncate its different slice - unless the
            //    replay carries NO cap besides the (merged) top one, where nothing can
            //    truncate a different slice at all (tpcds q76's LIMIT 101 variant);
            //  every replay cap must be one the caller's own tree has, or a
            //    same-valued pushdown of the caller's own top-level cap, with the
            //    caller asking for NO order contract (an equal-valued inner cap below
            //    an ordering-dependent slice truncates a DIFFERENT slice);
            //  every caller cap must still survive in the replay.
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
     * Parses frozen SQL under the default SQL mode and substitutes matched expressions by marker id. Persisted
     * provenance takes precedence over legacy marker-based classification. Returns null for raw fallback rows,
     * unavailable context, parse failures or unresolved markers. Successful SQL replay remains subject to ordinary
     * planning; it does not force the same physical plan on every execution.
     */
    private LogicalPlan rewriteFromFrozenTree(BaselinePlan candidate,
            Map<Long, Expression> placeholderValues) {
        // Persisted provenance first (see BaselinePlan.planFrozen): a row explicitly
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
            Plan parsed = SPMPlanTreeSupport.parseFrozenSqlWithoutSetVarSideEffects(planSql);
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
            // The frozen text keeps its STRUCTURAL hints (INDEX pin, [BROADCAST] /
            // [SHUFFLE] join distribution) - that is what the frozen replay exists for - but
            // NOT the CREATOR's SET_VAR payload: a SET_VAR writes a STATEMENT-scoped session
            // variable, and the caller's own hints are re-attached from the CALLER's tree by
            // attachCallerSetVarHints at the return sites. An INDEX-pinned baseline
            // used to replay the captured time_zone='+08:00' inside a nested scalar subquery
            // and change from_unixtime for a caller that carried no hint at all; the peel
            // reaches expression-owned subquery plans too (see
            // SPMPlanTreeSupport.stripSetVarHints).
            LogicalPlan replayable = SPMPlanTreeSupport.stripSetVarHints(rewritten);
            return replayable == null ? rewritten : replayable;
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
     * Post-planning metadata revalidation of a replayed baseline (the replay-side half
     * of the frozen-metadata binding, see
     * SPMPlanTreeSupport.schemaFingerprintForCreate): the pre-match fingerprint guard
     * runs BEFORE the query planner takes its metadata locks, so an ALTER TABLE
     * committing in between would let the frozen SQL be planned against a DIFFERENT
     * schema than the one it was validated against (and than the one its output slots
     * were frozen with). This re-checks the stored fingerprint against the metadata
     * the REPLAYED plan was actually planned with - the planned PhysicalPlan's catalog
     * relations plus the baseline's own bind tree resolved in this context. A mismatch
     * surfaces as a rewrite failure; the caller's enable_spm_fallback policy decides
     * whether to re-plan the original query (enabled) or surface the error (disabled).
     *
     * Parameter ctx: the replaying session
     * Parameter baselineId: the baseline the rewritten tree came from (-1 = none)
     * Parameter plannedPlan: the replayed plan after planning
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
        // section-less entries (see SPMPlanTreeSupport.legacyEntryOf); a row written
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

    // baseline creation.

    /**
     * Rejects statements whose frozen SQL would persist the CREATOR's session state: a
     * session / user variable or current_user / session_user / database /
     * connection_id survives parameterization, the creator-context optimization then
     * resolves it to a LITERAL, and the frozen planSql persists that value - while
     * matching still compares the original unbound bind tree, so a global baseline would
     * serve every other user the creator's identity / variables. Clock functions are the
     * same class of value: FE constant folding evaluates now / current_timestamp from
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
        // The CALL alone is not enough: the guard above walks the PARSED trees, but
        // an ALIAS UDF's registry-owned body only enters the plan when analysis INLINES it -
        // CREATE ALIAS FUNCTION f() WITH PARAMETER() AS current_user() then had that
        // CurrentUser folded into a literal by the creator-context optimization (no
        // NeedSessionVarGuard on CurrentUser) and frozen into the planSql, while matching still
        // compared the caller's own f() text: a GLOBAL baseline answered every other user with
        // the creating identity. The resolved bodies are inspected TRANSITIVELY (and in the
        // same resolved scope the call will use) before anything is parameterized.
        ConnectContext currentCtx = ConnectContext.get();
        if (SPMPlanTreeSupport.containsReplayContextInAliasBodies(currentCtx, bindPlan)
                || SPMPlanTreeSupport.containsReplayContextInAliasBodies(currentCtx, planPlan)) {
            throw new AnalysisException("SPM does not support replay-time context expressions"
                    + " inside a resolved ALIAS FUNCTION body (an alias whose body calls"
                    + " current_user() / session_user() / database() / @v / a clock function,"
                    + " possibly through another alias): analysis INLINES the body and the"
                    + " creator-context folding then persists the CREATOR's value in the frozen"
                    + " SQL while matching compares the caller's own call: "
                    + bindSql);
        }
    }

    /**
     * Builds a GLOBAL baseline candidate from bind and plan SQL. Parses both texts in the creator mode, validates
     * supported correspondence, parameterizes supported literals and optimizes the plan. Stores frozen SQL when
     * decompilation and replay checks allow it; otherwise retains the original plan SQL and fallback tree. The
     * caller stores the returned candidate.
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
     * Parameter ctx: the connect context (catalog + session variables)
     * Parameter bindSql: the binding SQL (SELECT)
     * Parameter planSql: the plan SQL (SELECT, may contain SET_VAR hints)
     * Parameter globalScope: whether the baseline is stored in the shared GLOBAL store
     * Returns: the constructed BaselinePlan (not yet stored)
     * Throws UserException: when the SQLs cannot be parsed / planned
     */
    public BaselinePlan buildBaselineFromSql(ConnectContext ctx, String bindSql, String planSql,
            boolean globalScope) throws UserException {
        // Save the mode before either input is parsed. Parsing applies SET_VAR to the live session,
        // and planning applies it again, but neither assignment may decide the persisted parser mode.
        // A later reload must build bind expressions such as a || b exactly as CREATE did.
        final long creatorMode = SqlModeHelper.currentMode();
        // Pin both inputs to that mode. The bind hint can change the real session between these calls;
        // without the override, identical a || b expressions could become OR in bind and CONCAT in plan.
        // This controls parser reads only; SET_VAR assignments remain active until statement cleanup.
        LogicalPlan bindPlan = parseSelectWithSqlMode(creatorMode, bindSql,
                "SPM bindSql must be a SELECT statement: " + bindSql);
        boolean sameSql = bindSql.equals(planSql);
        // Snapshot the bind's settings before parsing the plan can apply its own hints.
        SessionVariable bindSession = sameSql ? null : VariableMgr.cloneSessionVariable(ctx.getSessionVariable());
        LogicalPlan planPlan = sameSql
                ? bindPlan
                : parseSelectWithSqlMode(creatorMode, planSql,
                        "SPM planSql must be a SELECT statement: " + planSql);
        // The destination of SELECT... INTO OUTFILE lives outside the plan expressions:
        // the match cannot compare it and the rewritten tree keeps the captured sink, so
        // replay would export to the baseline's destination. Reject instead of freezing.
        if (SPMPlanTreeSupport.containsFileSink(bindPlan)
                || SPMPlanTreeSupport.containsFileSink(planPlan)) {
            throw new AnalysisException(
                    "SPM does not support SELECT ... INTO OUTFILE statements: " + bindSql);
        }
        rejectReplayContextExpressions(bindPlan, planPlan, bindSql);
        // Collect shared facts once, then validate scan selectors and logical query equivalence.
        SPMPlanTreeSupport.rejectManualPlanMismatch(bindPlan, planPlan, bindSql);
        // key(...) folds a named secret into a constant during optimization, and the parsed
        // form of KEY db.key is an EncryptKeyRef (NOT an UnboundFunction): reject on BOTH
        // parsed inputs BEFORE optimizing / storing anything.
        SPMPlanTreeSupport.rejectVolatileFunctionDependencies(bindPlan, planPlan);
        if (!sameSql && SPMOptimizer.hasSqlModeHint(planPlan)) {
            // A permissive plan-side hint can turn an invalid strict bind into an explicit ANY_VALUE.
            // Validate the original bind under its own settings before storing that transformation.
            SPMOptimizer.validateBindAnalysis(ctx, bindPlan, bindSql, bindSession);
        }
        // Parameterize both whole trees with ONE shared builder (placeholder ids aligned
        // across bind / plan), then optimize the PARAMETERIZED plan tree so the frozen
        // planSql keeps the placeholder ids for the rewrite-time value substitution.
        SPMPlaceholderBuilder parameterizeBuilder = new SPMPlaceholderBuilder();
        Pair<LogicalPlan, LogicalPlan> trees = parameterizeWholeTrees(bindPlan, planPlan,
                parameterizeBuilder);
        LogicalPlan parameterizedPlan = trees.second;
        // Keep original plan SQL when either input references a view rather than storing its base-table
        // expansion. The rewrite entry currently skips view queries before replay so their ordinary planning
        // retains view resolution and authorization.
        boolean referencesView = SPMPlanTreeSupport.referencesView(ctx, bindPlan)
                || SPMPlanTreeSupport.referencesView(ctx, planPlan);
        // Preserve omitted session-dependent arguments and keep concrete VALUES / REPLACE payloads
        // that must be evaluated using the caller's settings.
        boolean sessionSensitiveReplay =
                SPMPlanTreeSupport.requiresSessionSensitiveReplay(bindPlan)
                        || (!sameSql && SPMPlanTreeSupport.requiresSessionSensitiveReplay(planPlan));
        // An explicit FROM t INDEX mv pin is a SEMANTIC choice (on an aggregate-key
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
            frozen = decompileFrozenPlan(referencesView, sessionSensitiveReplay,
                    pinsExplicitIndex,
                    parameterizeBuilder.hasParameterizedGroupByOrdinalTarget(),
                    optimizeResult, planSql);
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
                            parseSelectWithSqlMode(creatorMode, planSql,
                                    "SPM planSql must be a SELECT statement: " + planSql)),
                    planSql);
            frozen = new DecompiledPlan(planSql, false);
        }
        // A successful decompilation is not proof the text can be re-planned: the
        // optimizer's output slots may use names the emitted SQL never exposes (an
        // ORDER BY over columns a lower scope dropped). The builder prevents the known
        // forms at the source - the ORDER BY hoist re-exports every key it moves above
        // its scope (hoistableOrderBy / reExportedLabels; the step that made tpcds q78
        // legal: its hoisted "ORDER BY c_14..." now re-exports c_4 AS c_14 through the
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
            // (<sessionId>_#TEMP._<name>). The bind key for "FROM t" is only
            // catalog.db.t, so a second session running the same text matched the same
            // digest, and the frozen SQL (emitting the internal name) resolved the
            // creator's still-live temporary table instead of the second session's t.
            rejectTemporaryRelations(optimizeResult, bindSql);
        }
        // Explicit provenance of the stored planSql, persisted so a reload never has to
        // GUESS whether the text is SPM's decompiled frozen rendering or the user's raw
        // fallback (see BaselinePlan.planFrozen / #planSqlMode).
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
        // parse input, exactly like SPMPlanner.canonicalPlanDigest over the submitted
        // text.
        baseline.setPlanSqlDigest(SPMPlanTreeSupport.canonicalSpmDigest(
                SPMPlanTreeSupport.namespaceQualified(planPlan,
                        captureCatalogName(ctx), captureDatabaseName(ctx)).toSpmDigest()));
        // Schema identity of the referenced tables, validated again before every replay
        // (see SPMPlanTreeSupport.schemaFingerprintForCreate): the PLAN side comes from
        // the OPTIMIZED plan's own relations - the under-lock metadata snapshot the
        // frozen output slots were built from - and the BIND side keeps the bind-tree
        // resolution; the union also covers tables only the stored plan uses. The
        // function names come from the STORED TEXT (frozen.sql), so a re-plan that picks
        // an equivalent builtin with another name (years_add vs date_add) cannot break
        // the symmetric revalidation.
        baseline.setSchemaFingerprint(
                SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries(
                        ctx, bindPlan, frozen.sql, optimizeResult.getTableFingerprints(),
                        optimizeResult.getBindFunctionDependencies(),
                        optimizeResult.getFunctionRegistryVersion()));
        return baseline;
    }

    /**
     * Rejects a temporary relation in the optimized GLOBAL plan. SESSION baselines can use the owning session
     * temporary table through raw SQL fallback, whose names are resolved in the executing context.
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
     * Probes decompiled SQL by substituting payloads collected from the writer plan and running nested SPM
     * optimization. A planning or parsing failure returns the original user SQL as a fallback. If unresolved
     * marker calls remain, the current implementation retains the frozen text without running the optimization
     * probe.
     */
    private static DecompiledPlan validateFrozenTextReplannable(ConnectContext ctx,
            DecompiledPlan frozen, String planSql, Plan writePlan) {
        try {
            LogicalPlan parsed = SPMPlanTreeSupport.parseFrozenSqlWithoutSetVarSideEffects(frozen.sql);
            // Substitute the placeholders with the WRITER plan's own literal payloads
            // before re-planning: the replay substitutes the caller's values by id, so
            // re-planning the raw marker calls can only fail ("Can not found function
            // '_spm_const_var'") and the downgrade would mask every scalar-subquery
            // baseline. List markers keep their option payloads and are supplied as
            // SpmConstList replacements to the same probe.
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
     * Collects scalar payload expressions and list marker nodes by id from the writer plan for the CREATE-time
     * replay probe.
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
        if (expr instanceof SpmConstList) {
            // The IN-list placeholder KEEPS its original values as children, so the
            // create-time replan probe can substitute it as well: the validation of a
            // list-containing frozen text used to be skipped entirely (accepted
            // unvalidated), which is how texts that only fail on re-analysis were
            // stored.
            values.putIfAbsent(((SpmConstList) expr).getId(), expr);
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
     * planSql text is kept when the decompiler does not support the plan shape, when the plan references a view
     * (the frozen text is
     * replayed ahead of authorization, and replaying a view's base-table expansion would
     * authorize those base tables instead of the view), when an inline VALUES cell /
     * REPLACE payload or analyzed concrete expression folds a value the caller's session must
     * evaluate for itself, when binding inserts the session's encryption mode or AI resource (see
     * SPMPlanTreeSupport.requiresSessionSensitiveReplay), or when the statement
     * pins a materialized view / rollup with an explicit INDEX clause (the decompiled
     * text cannot carry the pin, and dropping it would read the base table instead of the
     * pinned - possibly coarser - index; see SPMPlanTreeSupport.pinsExplicitIndex). A GROUP BY ordinal
     * that references a parameterized SELECT item also keeps the original statement.
     */
    private static DecompiledPlan decompileFrozenPlan(boolean referencesView,
            boolean sessionSensitiveReplay, boolean pinsExplicitIndex,
            boolean parameterizedGroupByOrdinal, SPMOptimizer.OptimizeResult optimizeResult,
            String planSql) {
        if (referencesView) {
            LOG.info("SPM freeze skipped: the plan references a view; keeping the user planSql"
                    + " so the rewrite replays the parameterized tree (view authorization"
                    + " preserved)");
            return new DecompiledPlan(planSql, false);
        }
        if (sessionSensitiveReplay || optimizeResult.hasConcreteSessionSensitiveExpression()) {
            LOG.info("SPM freeze skipped: binding or folding can capture a session-dependent"
                    + " value; keeping the user planSql so the"
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
        if (parameterizedGroupByOrdinal) {
            LOG.info("SPM freeze skipped: a GROUP BY ordinal references a parameterized SELECT item;"
                    + " the parameterized tree preserves the grouping");
            return new DecompiledPlan(planSql, false);
        }
        try {
            return new DecompiledPlan(
                    new SPMPlan2SQLBuilder(optimizeResult.getPartitionNames())
                            .toSQL(optimizeResult.getPhysicalPlan()), true);
        } catch (UnsupportedOperationException e) {
            LOG.info("SPM decompile unsupported ({}):\n{}", e.getMessage(),
                    optimizeResult.getPhysicalPlan().treeString());
            return new DecompiledPlan(planSql, false);
        }
    }

    /**
     * Parameterizes bind and plan trees with one builder. Each tree resets traversal scopes so corresponding
     * occurrences can reuse ids. Identical plan objects share one result. A separate plan tree whose marker ids
     * are not supplied by the bind tree is rejected during creation.
     */
    private static Pair<LogicalPlan, LogicalPlan> parameterizeWholeTrees(
            LogicalPlan bindPlan, LogicalPlan planPlan, SPMPlaceholderBuilder builder) {
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
        // Consistency with the placeholder parameterization: toSpmDigest renders every
        // Expression-leaf Literal as "?" through the generic digest machinery, while
        // SPMPlaceholderBuilder visits supported expression positions. The structural
        // matcher checks positions that parameterization leaves concrete. The digest is only a cheap Level 1/2
        // pre-filter; Level 3 (the placeholder-tree structural match) is authoritative,
        // so even a future divergence between the two mechanisms (e.g. a node whose
        // toDigest leaks a literal value) can only cause a missed rewrite (safe), never
        // a wrong rewrite.
        // The digest is canonicalized (see SPMPlanTreeSupport.canonicalSpmDigest): the
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
     * Computes the namespace-qualified canonical structural digest used for the bind key without optimization or
     * decompilation. Returns an empty string when computation fails, allowing forwarded confirmation to use other
     * available identity fields.
     */
    public static String canonicalBindDigest(ConnectContext ctx, String bindSql) {
        return canonicalSqlDigest(ctx, bindSql, "bind");
    }

    /**
     * Computes the canonical structural digest of submitted plan SQL, independently of its later decompiled
     * rendering. Returns an empty string when computation fails; forwarded confirmation decides whether other
     * available identity fields suffice.
     */
    public static String canonicalPlanDigest(ConnectContext ctx, String planSql) {
        return canonicalSqlDigest(ctx, planSql, "plan");
    }

    /**
     * The CURRENT bind-side schema fingerprint of a forwarded CREATE's bind SQL (see
     * SPMPlanTreeSupport.schemaFingerprint): computed on the forwarding FE at forward
     * time, so the confirmation of the created row can reject a matching-but-STALE one -
     * after a schema change the repeated CREATE retires the old row (identical bind /
     * plan digests) and writes the replacement under the new fingerprint, and the
     * retiring DELETE may still be invisible when the follower reads the snapshot (see
     * BaselineManager.ForwardedDdlExpectation).
     *
     * Parameter ctx: the forwarding statement's context (may be null in tests)
     * Parameter bindSql: the forwarded CREATE's bind SQL
     * Returns: the fingerprint (possibly empty, never null); "" when the text cannot be
     *         parsed here (the confirmation then falls back to the digest identity)
     */
    public static String canonicalSchemaFingerprint(ConnectContext ctx, String bindSql) {
        try {
            final long creatorMode = SqlModeHelper.currentMode();
            LogicalPlan plan = parseSelectWithSqlMode(creatorMode, bindSql,
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
     * Computes bind digest, submitted plan digest and bind-side schema fingerprint under one originating parse
     * mode. Each failed identity computation returns an empty string. This keeps a SQL-mode hint in one input from
     * changing how the next input is parsed.
     */
    public static String[] canonicalForwardedCreateIdentities(ConnectContext ctx,
            String bindSql, String planSql) {
        return SqlModeHelper.withSqlMode(originSqlMode(ctx), () -> new String[] {
                canonicalBindDigest(ctx, bindSql),
                canonicalPlanDigest(ctx, planSql),
                canonicalSchemaFingerprint(ctx, bindSql)});
    }

    /**
     * The SPM match key of a statement - the (query digest, structural hash) pair
     * tryRewritePlan matches baselines on - or null when the text is not a
     * plan-rewritable query (a command / DDL, unparsable text, or a statement batch).
     *
     * The forwarding FE uses the key to carry only the SESSION baselines the forwarded
     * statement could actually match (see SPMForwardedSession.serializeForStatement):
     * the master REBUILDS every carried row before it even classifies the statement
     * (each bind and plan text is re-parsed), so the unfiltered payload let a large
     * session store add that many parses to EVERY forwarded statement - outside
     * spm_rewrite_timeout_ms, which only bounds the planner side.
     *
     * An EXPLAIN of a query gets a key too: the rewrite classifies the very plan the
     * ExplainCommand wraps (ExplainCommand.run hands getLogicalPlan to
     * tryRewritePlan), so leaving the command unwrapped made every EXPLAIN carry the
     * WHOLE session store while the query itself carried one row - the very cost this
     * key exists to remove. EXPLAIN ANALYZE / an EXPLAIN of a command stay null: no
     * baseline can ever apply to them.
     *
     * The key is computed exactly like the rewrite's: the same parse mode, the same
     * namespace qualification (the statement's catalog / database travel with the
     * forwarded request, so the master qualifies with the same names) and the same
     * value-free digest.
     *
     * The parse mode is the one the statement's TEXT was written for: the mode captured
     * before its own SET_VAR(sql_mode=...) hint overwrote the session's (see
     * StatementContext.getSpmOriginSqlMode). Reading the session mode instead computed
     * the key under a mode the statement's text was never parsed with - a query whose
     * hint turned PIPES_AS_CONCAT on produced a different digest here than the rewrite
     * had matched, so the forwarded statement carried the wrong (or no) session rows.
     *
     * Parameter ctx: the forwarding statement's context (may be null in tests)
     * Parameter sql: the statement text
     * Returns: the digest plus its structural hash, or null when no baseline can apply
     */
    public static Pair<String, Long> queryMatchKey(ConnectContext ctx, String sql) {
        try {
            LogicalPlan plan = parseQueryWithSqlMode(originSqlMode(ctx), sql,
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

    /**
     * Returns the statement's recorded parser mode. A direct/internal parse without a recorded mode uses
     * the current mode instead. Batch parsing records a mode even for unhinted statements, because an earlier
     * statement's SET_VAR can already have changed the live session.
     */
    public static long originSqlMode(ConnectContext ctx) {
        if (ctx == null) {
            return SqlModeHelper.currentMode();
        }
        StatementContext statementContext = ctx.getStatementContext();
        long captured = statementContext == null ? -1 : statementContext.getSpmOriginSqlMode();
        return captured < 0 ? SqlModeHelper.currentMode() : captured;
    }

    private static String canonicalSqlDigest(ConnectContext ctx, String sql, String side) {
        try {
            final long creatorMode = SqlModeHelper.currentMode();
            LogicalPlan plan = parseSelectWithSqlMode(creatorMode, sql,
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
     * Parses one statement text into the plan the REWRITE would classify: a plain SELECT,
     * or the QUERY an EXPLAIN wraps (the very plan ExplainCommand.run hands to
     * tryRewritePlan). A DDL command, an EXPLAIN of a command (EXPLAIN ANALYZE /
     * EXPLAIN INSERT...) and a statement batch are rejected exactly like before.
     */
    private static LogicalPlan parseQueryOrExplainedQuery(String sql, String errorMessage)
            throws UserException {
        Plan parsed = new NereidsParser().parseSingle(sql);
        if (parsed instanceof ExplainCommand) {
            parsed = ((ExplainCommand) parsed).getLogicalPlan();
        }
        if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
            throw new AnalysisException(errorMessage);
        }
        return (LogicalPlan) parsed;
    }

    /**
     * Parses a query or explained query under the supplied SQL-mode read override. SET_VAR hints still
     * update the real session; only mode reads during this parse are fixed.
     */
    private static LogicalPlan parseQueryWithSqlMode(long creatorMode, String sql,
            String errorMessage) throws UserException {
        final UserException[] failure = new UserException[1];
        final LogicalPlan[] result = new LogicalPlan[1];
        SqlModeHelper.withSqlMode(creatorMode, () -> {
            try {
                result[0] = parseQueryOrExplainedQuery(sql, errorMessage);
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
     * Parses one SELECT under the supplied SQL-mode read override. CREATE uses the same override for
     * both inputs, even when the first input's SET_VAR changes the live session before the second parse.
     * This method does not suppress SET_VAR assignments or restore session variables.
     */
    private static LogicalPlan parseSelectWithSqlMode(long creatorMode, String sql,
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
     * Rebuilds stored trees using the creator mode for bind SQL and the default mode for plan SQL. The overload
     * with explicit plan mode handles user-authored fallback SQL. A failed bind parse returns two null trees. A
     * null plan SQL or identical SQL text returns the bind tree in both positions; a separate failed plan rebuild
     * returns a null second tree.
     */
    public static Pair<LogicalPlan, LogicalPlan> rebuildParameterizedTrees(
            String bindSql, String planSql, long creatorSqlMode) {
        return rebuildParameterizedTrees(bindSql, planSql, creatorSqlMode, SqlModeHelper.MODE_DEFAULT);
    }

    /**
     * Rebuilds transient trees with explicit parser modes and one shared marker builder. Bind SQL is rebuilt
     * first. Null plan SQL or text identical to bind SQL reuses that tree; otherwise the plan text is parsed and
     * parameterized with reset traversal scopes. Bind parse failure returns two nulls; a separate plan rebuild
     * failure retains the bind tree and returns null for the plan tree.
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
     * the parameterized fallback tree (see the caller in BaselineManager.parsePersistedRow).
     *
     * Parameter planSql: the stored planSql
     * Returns: true when the text re-parses into a tree carrying placeholder calls
     */
    public static boolean isFrozenPlanSql(String planSql) {
        return isFrozenPlanSql(planSql, null);
    }

    /**
     * Whether a STORED planSql is a FROZEN (placeholder-carrying) text, honouring the
     * PERSISTED provenance when the row carries it (see
     * BaselinePlan.getPlanFrozen): an explicit flag removes every
     * classification guess, so a raw-fallback text that merely CONTAINS a placeholder
     * name (e.g. a real db._spm_const_var(1) UDF call) keeps its parameterized
     * fallback tree instead of being replayed as frozen SQL. Pre-column rows fall back
     * to the parse-based classifier.
     *
     * Parameter planSql: the stored planSql
     * Parameter persistedFrozen: the persisted provenance, or null when absent
     * Returns: true when the text is the SPM decompiled placeholder rendering
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
     * Parses stored SQL under the default parser mode. Used for decompiled SQL and legacy classification;
     * user-authored texts can use the explicit-mode overload.
     */
    private static LogicalPlan parseStoredSelect(String sql) {
        return parseStoredSelect(sql, SqlModeHelper.MODE_DEFAULT);
    }

    /**
     * Parses stored SELECT text without applying SET_VAR assignments to the live session. Decompiled text
     * uses MODE_DEFAULT; original bind and fallback plan text use their persisted creator mode.
     */
    private static LogicalPlan parseStoredSelect(String sql, long sqlMode) {
        long mode = (sqlMode & SqlModeHelper.MODE_ALLOWED_MASK) == 0
                ? SqlModeHelper.MODE_DEFAULT : (sqlMode & SqlModeHelper.MODE_ALLOWED_MASK);
        Plan parsed = SPMPlanTreeSupport.parseStoredSqlWithoutSetVarSideEffects(sql, mode);
        if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
            throw new RuntimeException("SPM stored SQL is not a SELECT statement: " + sql);
        }
        return (LogicalPlan) parsed;
    }
}
