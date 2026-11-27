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
import org.apache.doris.common.UserException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalSelectHint;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Optimizes a baseline plan in a nested statement context. The context carries a rule-exclusion mask that combines
 * SPM restrictions with the session rule whitelist. The public enable_nereids_rules value is not replaced, and the
 * session disable list still applies.
 *
 * Capture temporarily disables TopN lazy materialization and changes CTE settings so the physical plan can be
 * decompiled. The original settings and outer statement context are restored, and the nested context is closed in
 * finally.
 */
public class SPMOptimizer {

    /**
     * RuleType names excluded from the SPM whitelist.
     * All names must exist in RuleType (the whitelist is parsed with
     * RuleType.valueOf, which throws on unknown names).
     */
    public static final List<String> SPM_EXCLUDED_RULE_NAMES = List.of(
            // category 1: data-state sensitive.
            // simple aggregate to constant (wrong result on an empty table)
            "REWRITE_SIMPLE_AGG_TO_CONSTANT",
            // skewed JOIN salt splitting (skew pattern changes over time)
            "SALT_JOIN",
            // grouping sets decomposition (topology change, cardinality dependent)
            "DECOMPOSE_REPEAT",

            // category 1b: TYPE-changing lowerings.
            // avg(DISTINCT x) -> sum(DISTINCT CAST(x AS LARGEINT)) / count(...): the
            // LARGEINT accumulator cast is part of the lowered expression's SEMANTICS, but
            // the frozen text is re-planned for a caller whose literal may have ANOTHER type
            // (avg(DISTINCT k + 1) captured, avg(DISTINCT k + 0.25) matching): the replay
            // then casts the substituted DECIMAL to an integer first and returns a different
            // value. Keeping AVG itself leaves the coercion to the replay's own analysis.
            "AVG_DISTINCT_TO_SUM_DIV_COUNT",

            // category 2: MV rewrite (all).
            // Excluded via the whole RuleTypeClass.MATERIALIZE_VIEW family (see
            // getMaterializedViewRuleNames), not listed here.
            //
            // category 3: table pruning + UKFK.
            "ELIMINATE_JOIN_BY_UK",              // UK constraint JOIN elimination
            "ELIMINATE_JOIN_BY_FK",              // FK constraint JOIN elimination
            "ELIMINATE_GROUP_BY_KEY",            // UKFK GROUP BY key elimination
            "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM", // uniform-distribution GROUP BY key elimination
            // GROUP BY elimination to a row-wise projection on a DECLARED UNIQUE key:
            // SELECT k, SUM(v) FROM t GROUP BY k collapses to SELECT k, v FROM t once
            // DataTrait proves {k} UNIQUE+NOT NULL - a proof the DECLARED UNIQUE(k)
            // constraint feeds, and which schemaFingerprint does NOT capture (it hashes
            // only the table id + base columns). After DROP CONSTRAINT and a second row
            // with the same k the original query returns one summed row while the frozen
            // replay returns two unsummed rows. Same uniqueness family as the _KEY /
            // unlock rules above - exclude it, and the replay re-plans the aggregate
            // against the CURRENT constraint state.
            "ELIMINATE_GROUP_BY",
            // ORDER BY key elimination by a declared UNIQUE key: ORDER BY a, b LIMIT 1 can
            // collapse to ORDER BY a LIMIT 1, and the frozen SQL keeps the reduced order.
            // Dropping the UNIQUE declaration (or its backing constraint state) changes no
            // value the fingerprint hashes, so the replay may pick another row with the
            // same smallest a - exclude the rule like the other UKFK ones.
            "ELIMINATE_ORDER_BY_KEY",
            // PK/FK-derived aggregate push down below the (FK) join: the rewritten
            // topology stops being correct once the constraint state changes. The rule
            // derives its rewrite from canEliminateByFk, so dropping the constraints and
            // adding duplicate keys on the former primary side makes the original
            // aggregate above the multiplying join return one doubled group while the
            // frozen pre-aggregate replay returns duplicate undoubled rows - a wrong
            // result, not just a missed rewrite. Audit (mutable-constraint consumers in
            // the whitelist): the remaining canEliminateByFk / canEliminateByUk consumers
            // are EliminateJoinByFK / EliminateJoinByUK (both excluded above) and the MV
            // comparator family (whole RuleTypeClass excluded); this rule was the only
            // unexcluded one.
            "PUSH_DOWN_AGG_THROUGH_JOIN_ON_PKFK",
            // uniqueness-dependent rewrites: each fires only when DataTrait proves the
            // relevant slots UNIQUE (and NOT NULL), and that proof may come from a
            // DECLARED UNIQUE constraint - which schemaFingerprint does not capture (it
            // hashes only the table id + base columns), so dropping the declaration
            // after freezing would leave the rewrite in place:
            // SIMPLIFY_WINDOW_EXPRESSION / AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION:
            //   Window(SUM(v) PARTITION BY k) -> Project(v) -> Scan(t) once t declares
            //   UNIQUE(k); after dropping the declaration and adding (k,10),(k,20) the
            //   original window returns 30,30 while the frozen replay returns 10,20;
            // PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN / _PROJECT_JOIN: a hard TopN
            //   pushed through the join / project retains rows justified by the
            //   declared uniqueness.
            "SIMPLIFY_WINDOW_EXPRESSION",
            "AGG_SCALAR_SUBQUERY_TO_WINDOW_FUNCTION",
            "PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN",
            "PUSH_DOWN_TOP_N_DISTINCT_THROUGH_PROJECT_JOIN",

            // category 4: equivalence derivation.
            "INFER_PREDICATES",                  // predicate derivation
            "INFER_FILTER_NOT_NULL",             // Filter NOT NULL derivation
            "INFER_JOIN_NOT_NULL",               // JOIN NOT NULL derivation
            "CONSTANT_PROPAGATION",              // constant propagation

            // category 5: structural rewrite.
            "EXTRACT_SINGLE_TABLE_EXPRESSION_FROM_DISJUNCTION", // split OR into single table
            "OR_EXPANSION",                      // OR expansion into UNION
            "PUSH_DOWN_FILTER_THROUGH_WINDOW",   // window predicate push down
            "ELIMINATE_AGG_CASE_WHEN",           // aggregate CASE WHEN elimination
            "ELIMINATE_OUTER_JOIN",              // outer join elimination
            "ELIMINATE_LIMIT",                   // LIMIT elimination
            // ELIMINATE_LIMIT is registered in the same rule class (EliminateLimit) as
            // ELIMINATE_LIMIT_ON_ONE_ROW_RELATION; excluding only the former let
            // "SELECT 1 LIMIT 1" freeze the one-row child WITHOUT its LIMIT, while the
            // sibling digest of "SELECT 1 LIMIT 0" still matched it (top-level LIMIT
            // values are deliberately ignored during matching) - the replay then
            // returned one row instead of none.
            "ELIMINATE_LIMIT_ON_ONE_ROW_RELATION",
            "ELIMINATE_AGGREGATE",               // aggregate elimination
            // two-phase LIMIT split GLOBAL(l, o) -> LOCAL(l + o, 0): the freeze keeps
            // only the UPPER limit topology, the decompiled SQL then carries both
            // phases as two query blocks, and the rewrite-time LIMIT merge (which only
            // reaches the block that has a user-tree counterpart) can never grow the
            // inner one - a captured order-free LIMIT 10 kept returning 10 rows when a
            // matching user query asked for LIMIT 20. The split is an execution
            // detail; freezing the single-phase limit keeps LIMIT... OFFSET
            // semantics (see SPMPlan2SQLBuilder.visitPhysicalLimit for the
            // collapse of already-frozen pairs).
            "SPLIT_LIMIT",

            // category 6: external sources / empty relations (data dependent).
            "PUSH_FILTER_INTO_SCHEMA_SCAN",      // schema table predicate push down
            "ELIMINATE_JOIN_ON_EMPTYRELATION",   // empty-relation operator elimination
            "ELIMINATE_FILTER_ON_EMPTYRELATION",
            "ELIMINATE_AGG_ON_EMPTYRELATION",
            "ELIMINATE_PROJECT_ON_EMPTYRELATION",
            "ELIMINATE_UNION_ON_EMPTYRELATION",
            "ELIMINATE_TOPN_ON_EMPTYRELATION",
            "ELIMINATE_SORT_ON_EMPTYRELATION",
            "ELIMINATE_INTERSECTION_ON_EMPTYRELATION",
            "ELIMINATE_EXCEPT_ON_EMPTYRELATION",
            "ELIMINATE_LIMIT_ON_EMPTY_RELATION",
            "PRUNE_EMPTY_PARTITION",
            // Partition pruning freezes the CURRENT partition set: SELECT p FROM t
            // WHERE p = 99 with only p1 VALUES IN (1) prunes every partition and the
            // rule emits a LogicalEmptyRelation, which the decompiler stores as WHERE
            // FALSE. ADD PARTITION p99 + a 99 row changes no value schemaFingerprint
            // hashes (the table id and base columns are unchanged), so the same query
            // kept hitting the frozen baseline and returned zero rows instead of the
            // new row. Excluding the rule keeps the live scan +
            // filter in the captured plan; the frozen SQL carries no partition pin
            // (only a MANUAL PARTITION(...) is ever rendered), so the replay re-derives
            // the selection against the partition set of ITS time - exactly like a
            // normal query.
            "OLAP_SCAN_PARTITION_PRUNE"
    );

    /**
     * Plan-SQL SET_VAR keys rejected during capture. TopN and CTE assignments can undo serialization
     * settings installed by optimize. Rule selection is fixed from the session before planning, so plan hints
     * may not replace enable_nereids_rules either; the separate statement rule mask remains in force.
     */
    private static final Set<String> SPM_PROTECTED_SET_VAR_KEYS = ImmutableSet.of(
            SessionVariable.ENABLE_NEREIDS_RULES,
            "topn_lazy_materialization_threshold",
            SessionVariable.ENABLE_CTE_MATERIALIZE,
            SessionVariable.INLINE_CTE_REFERENCED_THRESHOLD,
            SessionVariable.CTE_INLINE_MODE);

    /**
     * Whether an SPM CAPTURE optimization is running on this thread (see
     * isCaptureInProgress): the parameterized plan of a baseline is optimized by
     * this class, and a rule that must not lose a piece of USER-VISIBLE intent while that
     * plan is turned into the frozen text (an explicit CAST over a substitutable literal)
     * consults it.
     */
    private static final ThreadLocal<Boolean> CAPTURE_IN_PROGRESS = new ThreadLocal<>();

    private SPMOptimizer() {
    }

    /**
     * Installs the REPLAY-side rule mask on a replaying statement's context: every
     * MATERIALIZED_VIEW rewrite is forbidden while the frozen plan is re-planned.
     *
     * The frozen plan was produced under the SPM whitelist (see optimize) -
     * every MV rewrite excluded - so its fingerprint pins the SOURCE tables. The replay
     * is normally planned with the session's full rule set, and an MV that became
     * eligible AFTER the baseline was created (the reported case: an async MTMV over the
     * very query, built and refreshed later) then substitutes the MV's storage table for
     * the source table. The post-plan fingerprint guard
     * (SPMPlanner.verifyReplayMetadata, which pins the frozen plan's tables)
     * rejects its own replay and, with the default enable_spm_fallback=false, the
     * SELECT fails although the source table has not changed.
     *
     * The mask is deliberately NARROWER than the CREATE-side whitelist: it forbids
     * exactly the rules that SUBSTITUTE one table for another. The remaining
     * SPM-excluded rules are optimization opportunities that a re-plan may legally take
     * (they either keep the frozen tables - salt join, structural rewrites, predicate
     * inference - or their absence would break semantics: ELIMINATE_LIMIT turns the
     * CALLER's LIMIT 0 into an empty relation, and a full CREATE mask forbids it, which
     * replayed LIMIT 0 as one row). Rules that REMOVE a table (UK / FK join
     * elimination, aggregate-to-constant) keep their established fail-closed behavior:
     * the fingerprint mismatch surfaces through the caller's fallback policy.
     *
     * Parameter statementContext: the REPLAYING statement's context; its rule cache must not
     *                         have been computed yet (the callers install the mask before
     *                         the planner runs)
     */
    public static void installSpmReplayRuleMask(StatementContext statementContext) {
        statementContext.setSpmExcludedRules(materializedViewRuleMask());
    }

    /**
     * The replay mask: one bit per materialized-view rewrite rule
     * (RuleType.isMaterializedViewRule, so newly added MV rules are covered
     * automatically).
     *
     * Returns: the forbidden-rule mask of the whole MV family
     */
    private static BitSet materializedViewRuleMask() {
        BitSet mask = new BitSet();
        for (RuleType ruleType : RuleType.values()) {
            if (ruleType.isMaterializedViewRule()) {
                mask.set(ruleType.ordinal());
            }
        }
        return mask;
    }

    /**
     * All materialized view rewrite rule names (RuleTypeClass.MATERIALIZE_VIEW),
     * enumerated programmatically so newly added MV rules are excluded from the SPM
     * whitelist automatically.
     *
     * Returns: the immutable list of MV rewrite RuleType names
     */
    public static List<String> getMaterializedViewRuleNames() {
        return ImmutableList.copyOf(Arrays.stream(RuleType.values())
                .filter(RuleType::isMaterializedViewRule)
                .map(Enum::name)
                .collect(Collectors.toList()));
    }

    /**
     * The full set of RuleType names excluded from the SPM whitelist: the explicit
     * state-sensitive list plus the whole MV rewrite family (deduplicated).
     *
     * Returns: the immutable list of all SPM-excluded rule names
     */
    public static List<String> getSpmExcludedRuleNames() {
        return ImmutableList.copyOf(Stream.concat(
                        SPM_EXCLUDED_RULE_NAMES.stream(),
                        getMaterializedViewRuleNames().stream())
                .distinct()
                .collect(Collectors.toList()));
    }

    /**
     * Builds the SPM rule whitelist written into enable_nereids_rules while a baseline is
     * created: every RuleType name except the SPM-excluded set, intersected with the
     * caller's existing whitelist when one is configured (a session whitelist is
     * preserved, never widened). The engine turns a non-empty enable_nereids_rules into
     * the statement-level forbidden-rule mask (StatementContext.getOrCacheDisableRules),
     * so the excluded rules cannot apply during baseline creation.
     *
     * Parameter originalEnabled: the previous raw enable_nereids_rules value (may be null /
     *                        empty)
     * Returns: the comma-separated whitelist (RuleType declaration order)
     * Throws AnalysisException: when the original value names an unknown rule, or when
     *                           its intersection with the SPM whitelist is empty (the
     *                           session whitelists only rules that SPM excludes)
     */
    public static String buildSpmEnabledRules(String originalEnabled) throws AnalysisException {
        Set<String> excluded = new LinkedHashSet<>(getSpmExcludedRuleNames());
        Set<String> original = new LinkedHashSet<>();
        if (originalEnabled != null && !originalEnabled.isEmpty()) {
            for (String ruleName : originalEnabled.split(",")) {
                String normalized = ruleName.trim().toUpperCase(Locale.ROOT);
                if (normalized.isEmpty()) {
                    continue;
                }
                try {
                    RuleType.valueOf(normalized);
                } catch (IllegalArgumentException e) {
                    throw new AnalysisException(
                            "Unknown rule in enable_nereids_rules: " + normalized);
                }
                original.add(normalized);
            }
        }
        List<String> enabled = new ArrayList<>();
        for (RuleType ruleType : RuleType.values()) {
            String name = ruleType.name();
            if (excluded.contains(name)) {
                continue;
            }
            if (!original.isEmpty() && !original.contains(name)) {
                continue;
            }
            enabled.add(name);
        }
        if (enabled.isEmpty()) {
            throw new AnalysisException("enable_nereids_rules only whitelists rules that SPM excludes: "
                    + originalEnabled);
        }
        return String.join(",", enabled);
    }

    /**
     * Optimizes an already parsed logical plan with the nested statement rule mask and capture settings.
     * Parameterized marker nodes can survive into the physical plan, allowing frozen SQL to retain ids for replay
     * substitution. SPM's temporary capture settings and the outer context are restored when planning finishes
     * or fails. SET_VAR assignments remain statement-scoped and are restored by statement cleanup.
     */
    public static OptimizeResult optimize(ConnectContext ctx, LogicalPlan logicalPlan, String originSql)
            throws UserException {
        return optimize(ctx, logicalPlan, originSql, true);
    }

    /** Skips the freeze-specific analysis scan when the caller already knows the plan will stay raw. */
    static OptimizeResult optimize(ConnectContext ctx, LogicalPlan logicalPlan, String originSql,
            boolean attemptFreeze) throws UserException {
        // Planning applies the hints again after the capture settings below are installed. Reject assignments
        // to protected settings before entering the planner. Raw fallback uses the same check, so it also fails.
        rejectProtectedSetVarHints(logicalPlan);
        // The FUNCTION definitions the bind text resolves to, captured BEFORE the nested plan is
        // analyzed: analysis INLINES an alias-UDF body, so the frozen text keeps the OLD body
        // while a fingerprint derived afterwards would describe whatever definition the registry
        // holds then. CREATE compares this snapshot with the entries it derives (see
        // SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries) and refuses to certify
        // a baseline whose expansion and fingerprint disagree.
        java.util.TreeSet<String> bindFunctionSnapshot =
                SPMPlanTreeSupport.bindFunctionDependencyEntries(ctx, logicalPlan);
        // The alias-body safety check of the CREATE entry point ran BEFORE this snapshot, so a
        // definition REPLACED in between (f() -> abs(1) swapped for f() -> current_user())
        // was certified by both dependency snapshots (they already describe the unsafe body)
        // while the capture inlined it and folded the CREATOR's identity into the frozen SQL
        // . Re-running the check against the SAME registry state the snapshot
        // describes closes that window: any LATER change is caught by the registry version
        // fence below.
        if (SPMPlanTreeSupport.containsCreatorContextInAliasBodies(ctx, logicalPlan)) {
            throw new AnalysisException("SPM does not support replay-time context expressions"
                    + " inside a resolved ALIAS FUNCTION body (an alias whose body calls"
                    + " current_user() / session_user() / database() / @v / a clock function,"
                    + " possibly through another alias): analysis INLINES the body and the"
                    + " creator-context folding then persists the CREATOR's value in the frozen"
                    + " SQL: " + originSql);
        }
        // The registry's monotonic UDF version at the SAME instant: equal entry sets before /
        // after analysis do not prove nothing changed - a definition recreated and then restored
        // leaves both snapshots identical while the expansion inlined the INTERMEDIATE body, so
        // CREATE also requires that the definitions did not MOVE at all.
        long functionRegistryVersionBefore = SPMPlanTreeSupport.functionRegistryVersion();
        CaptureStatementContext statementContext = createNestedStatementContext(ctx, originSql);
        NereidsPlanner planner = new NereidsPlanner(statementContext);
        boolean[] concreteSessionSensitiveExpression = {false};
        if (attemptFreeze) {
            statementContext.addPlannerHook(new PlannerHook() {
                @Override
                public void afterAnalyze(NereidsPlanner planner) {
                    Plan analyzed = planner.getCascadesContext().getRewritePlan();
                    concreteSessionSensitiveExpression[0] =
                            SPMPlanTreeSupport.containsConcreteSessionSensitiveExpression(analyzed);
                    SPMPlanTreeSupport.recordPartitionNames(statementContext.getPartitionNames(), analyzed);
                }
            });
        }

        SessionVariable sessionVar = ctx.getSessionVariable();
        String originalEnabled = sessionVar.getEnableNereidsRulesStr();
        StatementContext originalCtx = ctx.getStatementContext();
        int originalTopnLazyThreshold = SessionVariable.getTopNLazyMaterializationThreshold();
        boolean originalCteMaterialize = sessionVar.enableCTEMaterialize;
        int originalInlineCteThreshold = sessionVar.inlineCTEReferencedThreshold;
        int originalCteInlineMode = sessionVar.cteInlineMode;
        try {
            // WHITELIST mode: only the rules SPM allows may apply while the baseline plan
            // is produced. The mask is installed on THIS nested statement's context - NOT
            // by redefining the public enable_nereids_rules variable, which would change
            // its established behavior for every ordinary statement in the session (a
            // session value like enable_nereids_rules='ELIMINATE_GROUP_BY_KEY_BY_UNIFORM'
            // would forbid every binding / implementation rule not named there). The
            // user's own disable_nereids_rules keeps applying on top of the mask.
            statementContext.setSpmExcludedRules(buildSpmExcludedRuleMask(originalEnabled));
            // TopN lazy materialization is an execution detail (post-process): it prunes
            // base-table columns from the physical plan and re-reads them later by rowid
            // (PhysicalLazyMaterialize). Such pruned columns would be missing from the
            // decompiled frozen planSql ("Unknown column in table list" on replay), so it
            // is disabled while the baseline plan is produced - the frozen SQL must carry
            // the full column set; a re-plan at rewrite time may still apply lazy
            // materialization itself.
            sessionVar.setTopNLazyMaterializationThreshold(-1);
            // The WITH structure of the user query must survive into the frozen planSql:
            // the decompiler turns PhysicalCTEAnchor / PhysicalCTEProducer /
            // PhysicalCTEConsumer into a real WITH clause (one shared definition,
            // referenced by alias). Three settings are overridden while
            // the baseline plan is produced:
            // enable_cte_materialize / inline_cte_referenced_threshold: by default the
            //   engine inlines a CTE with a single consumer (a common TPCDS shape), which
            //   deletes the anchor from the optimized plan before the decompiler can see it;
            // cte_inline_mode = -1: mode 0 (the default) builds an alternative fully
            //   inlined plan and uses it when consumer filters can eliminate union branches
            //   of the CTE body, which silently drops WITH for such queries (TPCDS q04/
            //   q11/q74); SPM needs the anchored plan, and a re-plan at rewrite time still
            //   applies the user's own cte_inline_mode.
            // CTEInline still inlines the CTEs a recursive CTE requires inlined
            // (StatementContext mustInlineCTEs), so WITH RECURSIVE planning is unaffected.
            sessionVar.enableCTEMaterialize = true;
            sessionVar.inlineCTEReferencedThreshold = 0;
            sessionVar.cteInlineMode = -1;
            // Install the FRESH statement context for the whole nested plan: the table
            // collector caches the SELECT tables into the ConnectContext's CURRENT
            // statement context, and collectAndLockTable then locks THAT object. When
            // the caller (StmtExecutor for a CREATE BASELINE statement) already installed
            // the outer command's context, leaving it active made collectRelation cache
            // into the outer object while lock ran on the fresh (empty) one: the nested
            // plan was optimized / decompiled WITHOUT its metadata-stability locks and
            // could race concurrent ALTER / DROP. The exact original (possibly null) is
            // restored in the finally below.
            ctx.setStatementContext(statementContext);
            // planWithLock runs preprocess (SET_VAR hint) -> analyze -> rewrite ->
            // optimize -> postProcess; distribution planning is not needed for the
            // decompiler (the physical plan already carries distribution specs).
            // The root physical plan is the RETURN value of planWithLock: the planner's
            // physicalPlan field is only assigned through the lockCallback used by
            // plan, so planner.getPhysicalPlan would be null here.
            //
            // The result becomes the FROZEN text, so the thread is marked as a CAPTURE
            // while it is produced: a rule that would erase user-visible intent the frozen
            // text must keep (an explicit CAST over a substitutable literal, see
            // SimplifyCastRule) holds back.
            boolean wasCapturing = isCaptureInProgress();
            CAPTURE_IN_PROGRESS.set(true);
            Plan resultPlan;
            try {
                resultPlan = planner.planWithLock(logicalPlan,
                        PhysicalProperties.ANY, ExplainLevel.NONE);
            } finally {
                // A nested optimization must not clear the enclosing capture's state.
                if (!wasCapturing) {
                    CAPTURE_IN_PROGRESS.remove();
                }
            }
            if (!(resultPlan instanceof PhysicalPlan)) {
                throw new AnalysisException("SPM failed to plan SQL: " + originSql);
            }
            // Belt-and-braces for any hint path the pre-scan does not model: the protected
            // variables must still carry the SPM values after planning, otherwise the plan
            // may have been produced without a guard - fail the freeze instead of
            // publishing it.
            verifySpmOverridesIntact(sessionVar);
            // Keep entries hashed under the planning locks; catalog table objects can change after unlock.
            return new OptimizeResult((PhysicalPlan) resultPlan,
                    extractCost((PhysicalPlan) resultPlan),
                    statementContext.getTableFingerprints(),
                    bindFunctionSnapshot,
                    functionRegistryVersionBefore,
                    statementContext.getPartitionNames(), concreteSessionSensitiveExpression[0]);
        } finally {
            sessionVar.setTopNLazyMaterializationThreshold(originalTopnLazyThreshold);
            sessionVar.enableCTEMaterialize = originalCteMaterialize;
            sessionVar.inlineCTEReferencedThreshold = originalInlineCteThreshold;
            sessionVar.cteInlineMode = originalCteInlineMode;
            ctx.setStatementContext(originalCtx);
            // The nested statement context OWNS everything the nested plan
            // registered on it - the connector scope an external (e.g. Iceberg) bind
            // installed, whose table-cache lease / tracked tables are released only by
            // close (connectorStatementScope.closeAll), plus its planner resources
            // (releasePlannerResources). Restoring the outer context above is NOT a
            // substitute: the statement's own teardown (ConnectProcessor / the
            // forwarded-request finally) later closes only the context the session
            // carries at that point, so without this close every CREATE / capture refresh
            // left the nested scope pinned for the lifetime of the FE.
            statementContext.close();
        }
    }

    /** Checks for a SQL-mode assignment in any block of the submitted plan. */
    static boolean hasSqlModeHint(LogicalPlan plan) {
        boolean[] found = {false};
        SPMPlanTreeSupport.<RuntimeException>walkPlans(plan, node -> {
            if (!found[0] && node instanceof LogicalSelectHint) {
                for (SelectHint hint : ((LogicalSelectHint<?>) node).getHints()) {
                    if (hint instanceof SelectHintSetVar && ((SelectHintSetVar) hint).getParameters()
                            .entrySet().stream().anyMatch(entry -> SessionVariable.SQL_MODE.equalsIgnoreCase(
                                    entry.getKey()) && entry.getValue().isPresent())) {
                        found[0] = true;
                        return;
                    }
                }
            }
        });
        return found[0];
    }

    /** Analyzes the unparameterized bind under its own statement settings. */
    static void validateBindAnalysis(ConnectContext ctx, LogicalPlan bindPlan, String bindSql,
            SessionVariable bindSession) {
        StatementContext outerContext = ctx.getStatementContext();
        SessionVariable planSession = ctx.getSessionVariable();
        CaptureStatementContext validation = createNestedStatementContext(ctx, bindSql);
        try {
            ctx.setSessionVariable(bindSession);
            ctx.setStatementContext(validation);
            new NereidsPlanner(validation).planWithLock(bindPlan, PhysicalProperties.ANY, ExplainLevel.ANALYZED_PLAN);
        } finally {
            ctx.setSessionVariable(planSession);
            ctx.setStatementContext(outerContext);
            validation.close();
        }
    }

    /**
     * Creates the nested statement context of one baseline-plan optimization. The context
     * is closed in the caller's finally.
     */
    private static CaptureStatementContext createNestedStatementContext(ConnectContext ctx,
            String originSql) {
        CaptureStatementContext nested = new CaptureStatementContext(ctx, originSql);
        // The plan tree handed to optimize was PARSED under the caller's statement context, and
        // parsing already allocates ExprIds: LogicalPlanBuilder builds a BOUND Alias for a literal
        // select item (SELECT 7 AS b), so such an item carries an id from the OUTER generator.
        // A nested context starts its own generators at 0, so the scan columns bound below get the
        // SAME small ids, and because SlotReference.equals / hashCode compare only the exprId,
        // every Map<Slot, Expression> substitution (projection merging / column pruning) then
        // conflates the derived constant column with a real one: SELECT k,... FROM
        // (SELECT k, 7 AS b FROM t) x froze as SELECT _spm_const_var(1) AS k,... and replayed
        // the CONSTANT for every k. Continuing the generators keeps every id this statement
        // allocates above the ids the swapped-in tree already carries.
        StatementContext outer = ctx.getStatementContext();
        if (outer != null && outer != nested) {
            nested.continueIdGeneratorsFrom(outer);
        }
        return nested;
    }

    /**
     * Rejects SET_VAR assignments that would override SPM's capture settings.
     */
    private static void rejectProtectedSetVarHints(List<SelectHint> hints) throws AnalysisException {
        for (SelectHint hint : hints) {
            if (!(hint instanceof SelectHintSetVar)) {
                continue;
            }
            for (String key : ((SelectHintSetVar) hint).getParameters().keySet()) {
                if (key != null && SPM_PROTECTED_SET_VAR_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
                    throw new AnalysisException("SPM cannot freeze a plan whose SET_VAR hint targets the"
                            + " SPM-protected session variable '" + key + "'");
                }
            }
        }
    }

    /**
     * Walks the WHOLE statement and rejects any SELECT hint targeting a protected
     * variable. The walk must follow the plans held outside children (CTE bodies via
     * extraPlans, IN / EXISTS / scalar subquery plans via SubqueryExpr.queryPlan and
     * through expression coercions): a hint inside a CTE definition or a subquery would
     * otherwise escape the pre-scan and EliminateLogicalSelectHint would apply it AFTER
     * the SPM CTE / TopN serialization overrides were installed, letting CTE inlining or
     * TopN materialization change the frozen plan.
     *
     * Package-visible for tests.
     */
    static void rejectProtectedSetVarHints(Plan node) throws AnalysisException {
        SPMPlanTreeSupport.<AnalysisException>walkPlans(node, (Plan current) -> {
            if (current instanceof LogicalSelectHint) {
                rejectProtectedSetVarHints(((LogicalSelectHint<?>) current).getHints());
            }
        });
    }

    /**
     * Fails when the SPM overrides were altered while the nested statement was planned.
     */
    private static void verifySpmOverridesIntact(SessionVariable sessionVar) throws AnalysisException {
        if (SessionVariable.getTopNLazyMaterializationThreshold() != -1
                || !sessionVar.enableCTEMaterialize
                || sessionVar.inlineCTEReferencedThreshold != 0
                || sessionVar.cteInlineMode != -1) {
            throw new AnalysisException("SPM safety overrides were altered during planning;"
                    + " the plan cannot be frozen");
        }
    }

    /**
     * The per-statement rule mask for a baseline-creation plan: every RuleType outside the
     * SPM whitelist (built from the session's own enable_nereids_rules value, if any) is
     * forbidden, except the engine-essential privilege / row-policy checks which are never
     * gated.
     */
    public static BitSet buildSpmExcludedRuleMask(String originalEnabled) throws AnalysisException {
        Set<String> enabled = new HashSet<>(Arrays.asList(buildSpmEnabledRules(originalEnabled).split(",")));
        BitSet mask = new BitSet();
        for (RuleType ruleType : RuleType.values()) {
            if (ruleType == RuleType.CHECK_PRIVILEGES || ruleType == RuleType.CHECK_ROW_POLICY) {
                continue;
            }
            if (!enabled.contains(ruleType.name())) {
                mask.set(ruleType.type());
            }
        }
        return mask;
    }

    /**
     * Reads the root group cost for PhysicalProperties.ANY, returning zero when unavailable or when lookup fails.
     * For parameterized plans the estimate uses marker expressions and is a relative candidate-ranking input,
     * rather than a measurement of a concrete query execution.
     */
    private static double extractCost(PhysicalPlan physicalPlan) {
        try {
            if (physicalPlan.getGroupExpression().isPresent()) {
                GroupExpression groupExpression = physicalPlan.getGroupExpression().get();
                Cost cost = groupExpression.getCostValueByProperties(PhysicalProperties.ANY);
                if (cost != null) {
                    return cost.getValue();
                }
            }
        } catch (RuntimeException e) {
            // cost is best-effort; fall through to 0
        }
        return 0;
    }

    /**
     * Result of an SPM-mode optimization: the best physical plan and its estimated cost.
     */
    public static class OptimizeResult {
        private final PhysicalPlan physicalPlan;
        private final double cost;
        private final boolean concreteSessionSensitiveExpression;
        /**
         * Table fingerprint entries hashed WHILE the planning locks were held (see
         * CaptureStatementContext): lower-case bare / db-qualified /
         * catalog-qualified table name to the entry. The TableIf objects are MUTABLE in
         * place, so a fingerprint computed after optimize returned could pair the frozen
         * output slots with a NEWER schema.
         */
        private final java.util.Map<String, String> tableFingerprints;

        /**
         * The FUNCTION dependency entries the bind text resolved to BEFORE the nested plan was
         * analyzed (see SPMPlanTreeSupport.bindFunctionDependencyEntries), or null when the
         * caller did not capture one.
         *
         * Analysis inlines an alias-UDF body, so the frozen text is a snapshot of the
         * definition that was live at expansion time while the CREATE fingerprint derives its
         * function entries from the LIVE registry afterwards. A redefinition inside that window
         * made both checks pass with a body the registry no longer holds, and the replay
         * returned the OLD implementation's result. The snapshot lets CREATE
         * prove the two agree.
         */
        private final java.util.Set<String> bindFunctionDependencies;

        /**
         * The registry's UDF-definition version read together with the snapshot above, or null
         * when the caller recorded none (see
         * SPMPlanTreeSupport.schemaFingerprintForCreateFromLockedEntries).
         */
        private final Long functionRegistryVersion;

        /**
         * The names of explicitly pinned OLAP partitions, snapshotted WHILE the
         * planning locks were held: table id to partition id to name. The frozen
         * PARTITION(...) clause names partitions by id, and the renderer runs AFTER
         * optimize released the locks - a RENAME PARTITION in that window made it emit
         * the NEW name while the stored bind text still said the old one, so a later caller
         * pinning the old name (on another partition) matched the bind and replayed the renamed
         * one.
         */
        private final java.util.Map<Long, java.util.Map<Long, SPMPlanTreeSupport.PartitionPin>> partitionNames;

        /**
         * The full result including the under-lock PARTITION NAMES (see the field javadoc).
         *
         * Parameter physicalPlan: the optimized physical plan
         * Parameter cost: its estimated cost
         * Parameter tableFingerprints: the under-lock table fingerprint entries
         * Parameter bindFunctionDependencies: the pre-analysis function entries, or null
         * Parameter functionRegistryVersion: the pre-analysis UDF registry version, or null
         * Parameter partitionNames: table id to partition id to the ADMITTED name
         * Parameter concreteSessionSensitiveExpression: whether a concrete expression can fold using session settings
         */
        public OptimizeResult(PhysicalPlan physicalPlan, double cost,
                java.util.Map<String, String> tableFingerprints,
                java.util.Set<String> bindFunctionDependencies,
                Long functionRegistryVersion,
                java.util.Map<Long, java.util.Map<Long, SPMPlanTreeSupport.PartitionPin>> partitionNames,
                boolean concreteSessionSensitiveExpression) {
            this.physicalPlan = physicalPlan;
            this.cost = cost;
            this.concreteSessionSensitiveExpression = concreteSessionSensitiveExpression;
            this.tableFingerprints = tableFingerprints;
            this.bindFunctionDependencies = bindFunctionDependencies;
            this.functionRegistryVersion = functionRegistryVersion;
            this.partitionNames = partitionNames;
        }

        public PhysicalPlan getPhysicalPlan() {
            return physicalPlan;
        }

        public boolean hasConcreteSessionSensitiveExpression() {
            return concreteSessionSensitiveExpression;
        }

        /** The under-lock partition-name snapshot (see the field javadoc). */
        public java.util.Map<Long, java.util.Map<Long, SPMPlanTreeSupport.PartitionPin>> getPartitionNames() {
            return partitionNames;
        }

        public double getCost() {
            return cost;
        }

        /**
         * The table fingerprint entries hashed under the planning locks (see the field
         * javadoc).
         *
         * Returns: lowercase table name to the fingerprint entry
         */
        public java.util.Map<String, String> getTableFingerprints() {
            return tableFingerprints;
        }

        /**
         * The FUNCTION dependency entries captured before the nested plan was analyzed (see the
         * field javadoc), or null when the capture recorded none.
         *
         * Returns: the bind-side function entries captured at expansion time
         */
        public java.util.Set<String> getBindFunctionDependencies() {
            return bindFunctionDependencies;
        }

        /**
         * The UDF registry version read before analysis, after the function entry snapshot (see
         * the field javadoc), or null when the capture recorded none.
         *
         * Returns: the pre-analysis UDF version
         */
        public Long getFunctionRegistryVersion() {
            return functionRegistryVersion;
        }
    }

    /**
     * Nested capture context for planning-time metadata. Initial relation lookup only resolves tables;
     * fingerprint entries are recorded once after locking, with late relations locked before capture.
     * The analyze hook records the explicitly bound partition IDs separately.
     */
    private static final class CaptureStatementContext extends StatementContext {
        private final java.util.Map<String, String> tableFingerprints =
                new java.util.concurrent.ConcurrentHashMap<>();
        private final java.util.Map<org.apache.doris.catalog.TableIf, String> capturedTables =
                new java.util.IdentityHashMap<>();
        private boolean planningLocked;

        /** Explicitly bound partition IDs only, recorded by the afterAnalyze hook. */
        private final java.util.Map<Long, java.util.Map<Long, SPMPlanTreeSupport.PartitionPin>> partitionNames =
                new java.util.HashMap<>();

        CaptureStatementContext(ConnectContext ctx, String originSql) {
            super(ctx, new OriginStatement(originSql, 0));
        }

        @Override
        public org.apache.doris.catalog.TableIf getAndCacheTable(List<String> tableQualifier,
                TableFrom tableFrom,
                java.util.Optional<org.apache.doris.nereids.analyzer.UnboundRelation> unboundRelation) {
            org.apache.doris.catalog.TableIf table =
                    super.getAndCacheTable(tableQualifier, tableFrom, unboundRelation);
            if (planningLocked) {
                if (!capturedTables.containsKey(table)) {
                    // The initial ordered lock acquisition cannot cover a table first
                    // resolved during analysis. Do not wait out of lock order; fail the
                    // capture if a concurrent writer already owns the late table.
                    lockTableForPlanning(table, 0, java.util.concurrent.TimeUnit.MILLISECONDS);
                }
                SPMPlanTreeSupport.recordTableFingerprint(tableFingerprints, table, tableQualifier, capturedTables);
            }
            return table;
        }

        /** CollectRelation only resolves tables. Capture metadata once after acquiring the planning locks. */
        @Override
        public synchronized void lock() {
            if (!planningLocked) {
                capturedTables.clear();
                tableFingerprints.clear();
                super.lock();
                planningLocked = true;
                getTables().forEach((qualifier, table) ->
                        SPMPlanTreeSupport.recordTableFingerprint(tableFingerprints, table, qualifier, capturedTables));
            }
        }

        @Override
        public synchronized void releasePlannerResources() {
            planningLocked = false;
            super.releasePlannerResources();
        }

        java.util.Map<String, String> getTableFingerprints() {
            return tableFingerprints;
        }

        java.util.Map<Long, java.util.Map<Long, SPMPlanTreeSupport.PartitionPin>> getPartitionNames() {
            return partitionNames;
        }
    }

    /**
     * Whether the current thread is optimizing a BASELINE CAPTURE plan (see
     * SPMOptimizer.optimize): SimplifyCastRule keeps an EXPLICIT cast that it would
     * otherwise discard as redundant, because the placeholder it wraps reports the
     * CAPTURED literal's type - eliminating the conversion froze a plan that silently
     * dropped the user's cast, and a matching query whose literal has a WIDER type then
     * substituted it without any conversion.
     *
     * Returns: true while a capture optimization (or its replan probe) is running here
     */
    public static boolean isCaptureInProgress() {
        return Boolean.TRUE.equals(CAPTURE_IN_PROGRESS.get());
    }
}
