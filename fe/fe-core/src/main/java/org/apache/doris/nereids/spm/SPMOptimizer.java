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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.cost.Cost;
import org.apache.doris.nereids.memo.GroupExpression;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.properties.SelectHint;
import org.apache.doris.nereids.properties.SelectHintSetVar;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.Command;
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
 * SPMOptimizer - baseline-dedicated optimizer (M3).
 *
 * Corresponds to design doc section 6.7. When a baseline is created, a dedicated
 * optimizer is used that deliberately disables many "state-sensitive" optimization
 * rules (MV rewrite, table pruning, UKFK JOIN pruning, equivalence derivation,
 * structural rewrites, ...), so the baseline plan only depends on SQL semantics and the
 * general cost model - because a baseline is a cross-time promise that must remain
 * reproducible and semantically correct even after data, statistics or MVs change.
 *
 * Rule exclusion mechanism (WHITELIST mode): the session variable enable_nereids_rules
 * carries a comma-separated rule WHITELIST; when non-empty, the engine only applies the
 * listed rules (the statement-level rule mask forbids every rule outside the list, see
 * StatementContext#getOrCacheDisableRules). SPMOptimizer temporarily replaces that
 * variable with the SPM whitelist - every RuleType except the excluded set below, see
 * buildSpmEnabledRules - while a baseline is created, and restores the original value
 * after optimization completes (the original value is cached before calling and
 * restored in a finally block). disable_nereids_rules is NOT touched: the user's own
 * disable list keeps applying on top of the whitelist.
 *
 * The excluded set is:
 * - an explicit list of state-sensitive RuleType names (categories 1, 3, 4, 5, 6), and
 * - the whole RuleTypeClass.MATERIALIZE_VIEW rule family (category 2) enumerated
 *   programmatically via RuleType.isMaterializedViewRule(), so new MV rules are covered
 *   automatically.
 */
public class SPMOptimizer {

    /**
     * RuleType names excluded from the SPM whitelist (aligned with design doc 5.4).
     * All names must exist in RuleType (the whitelist is parsed with
     * RuleType.valueOf, which throws on unknown names).
     */
    public static final List<String> SPM_EXCLUDED_RULE_NAMES = List.of(
            // ===== category 1: data-state sensitive =====
            // simple aggregate to constant (wrong result on an empty table)
            "REWRITE_SIMPLE_AGG_TO_CONSTANT",
            // skewed JOIN salt splitting (skew pattern changes over time)
            "SALT_JOIN",
            // grouping sets decomposition (topology change, cardinality dependent)
            "DECOMPOSE_REPEAT",

            // ===== category 2: MV rewrite (all) =====
            // Excluded via the whole RuleTypeClass.MATERIALIZE_VIEW family (see
            // getMaterializedViewRuleNames()), not listed here.

            // ===== category 3: table pruning + UKFK =====
            "ELIMINATE_JOIN_BY_UK",              // UK constraint JOIN elimination
            "ELIMINATE_JOIN_BY_FK",              // FK constraint JOIN elimination
            "ELIMINATE_GROUP_BY_KEY",            // UKFK GROUP BY key elimination
            "ELIMINATE_GROUP_BY_KEY_BY_UNIFORM", // uniform-distribution GROUP BY key elimination
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

            // ===== category 4: equivalence derivation =====
            "INFER_PREDICATES",                  // predicate derivation
            "INFER_FILTER_NOT_NULL",             // Filter NOT NULL derivation
            "INFER_JOIN_NOT_NULL",               // JOIN NOT NULL derivation
            "CONSTANT_PROPAGATION",              // constant propagation

            // ===== category 5: structural rewrite =====
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
            // detail; freezing the single-phase limit keeps LIMIT ... OFFSET
            // semantics (see SPMPlan2SQLBuilder.visitPhysicalLimit for the
            // collapse of already-frozen pairs).
            "SPLIT_LIMIT",

            // ===== category 6: external sources / empty relations (data dependent) =====
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
            "PRUNE_EMPTY_PARTITION"
    );

    /**
     * SET_VAR keys that carry the SPM safety overrides (see optimize()): a plan-SQL hint
     * must never clear or weaken them for the nested statement, otherwise the frozen plan
     * could be produced with state-sensitive rewrites re-enabled (e.g.
     * SET_VAR(enable_nereids_rules='') re-enables FK join elimination, so a join could be
     * frozen away and return different rows once constraint state changes) or with TopN
     * lazy materialization / CTE inlining undoing the plan-serialization guards.
     */
    private static final Set<String> SPM_LOCKED_SET_VAR_KEYS = ImmutableSet.of(
            SessionVariable.ENABLE_NEREIDS_RULES,
            "topn_lazy_materialization_threshold",
            SessionVariable.ENABLE_CTE_MATERIALIZE,
            SessionVariable.INLINE_CTE_REFERENCED_THRESHOLD,
            SessionVariable.CTE_INLINE_MODE);

    private SPMOptimizer() {
    }

    /**
     * All materialized view rewrite rule names (RuleTypeClass.MATERIALIZE_VIEW),
     * enumerated programmatically so newly added MV rules are excluded from the SPM
     * whitelist automatically.
     *
     * @return the immutable list of MV rewrite RuleType names
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
     * @return the immutable list of all SPM-excluded rule names
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
     * the statement-level forbidden-rule mask (StatementContext#getOrCacheDisableRules),
     * so the excluded rules cannot apply during baseline creation.
     *
     * @param originalEnabled the previous raw enable_nereids_rules value (may be null /
     *                        empty)
     * @return the comma-separated whitelist (RuleType declaration order)
     * @throws AnalysisException when the original value names an unknown rule, or when
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
     * Optimizes a planSql in SPM mode: parse -> analyze -> rewrite -> CBO optimize with
     * the SPM rule whitelist installed in enable_nereids_rules, then return the best
     * physical plan and its estimated cost.
     *
     * The original enable_nereids_rules value is cached before the call and restored in
     * a finally block (disable_nereids_rules is not touched). A fresh StatementContext is
     * used so the per-statement forbidden-rule cache
     * (CascadesContext.getAndCacheDisableRules) reflects the whitelist.
     *
     * @param ctx    the connect context (provides session variables and catalog)
     * @param planSql the plan SQL to optimize (a SELECT statement, may contain SET_VAR
     *               hints)
     * @return the optimization result (best physical plan + estimated cost)
     * @throws UserException when the SQL cannot be parsed or planned
     */
    public static OptimizeResult optimize(ConnectContext ctx, String planSql) throws UserException {
        Plan parsed = new NereidsParser().parseSingle(planSql);
        // A SELECT statement is parsed as a logical plan; DDL/DML parse to a Command.
        if (!(parsed instanceof LogicalPlan) || parsed instanceof Command) {
            throw new AnalysisException("SPM only supports SELECT statements: " + planSql);
        }
        return optimize(ctx, (LogicalPlan) parsed, planSql);
    }

    /**
     * Optimizes an already-parsed (possibly parameterized) plan tree in SPM mode:
     * analyze -> rewrite -> CBO optimize with the SPM rule whitelist installed in
     * enable_nereids_rules, then return the best physical plan and its estimated cost.
     *
     * This is the entry used by CREATE BASELINE when the plan tree is first
     * parameterized (literals replaced by SpmConstVar / SpmConstList placeholders) and
     * then optimized: the placeholders travel through the optimizer and survive into
     * the physical plan, so the decompiled frozen planSql keeps the placeholder ids
     * (matching the SR model where the frozen SQL is re-parsed and user values are
     * substituted by id at rewrite time).
     *
     * @param ctx          the connect context (provides session variables and catalog)
     * @param logicalPlan  the (possibly parameterized) SELECT plan tree
     * @param originSql    the original SQL text (used for the statement context /
     *                     error messages)
     * @return the optimization result (best physical plan + estimated cost)
     * @throws UserException when the plan cannot be analyzed / planned
     */
    public static OptimizeResult optimize(ConnectContext ctx, LogicalPlan logicalPlan, String originSql)
            throws UserException {
        // Plan-SQL hints are applied DURING analysis (after the SPM overrides below are
        // installed): SelectHintSetVar writes the SAME SessionVariable object, so a hint
        // such as SET_VAR(enable_nereids_rules='') would silently clear the whitelist and
        // the TopN / CTE guards for the nested statement. Reject such hints up front -
        // CREATE then keeps the user's planSql text instead of freezing an unguarded plan.
        checkProtectedSetVarHints(logicalPlan);
        StatementContext statementContext = new StatementContext(ctx,
                new OriginStatement(originSql, 0));
        NereidsPlanner planner = new NereidsPlanner(statementContext);

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
            // referenced by alias), like StarRocks. Three settings are overridden while
            // the baseline plan is produced:
            // - enable_cte_materialize / inline_cte_referenced_threshold: by default the
            //   engine inlines a CTE with a single consumer (a common TPCDS shape), which
            //   deletes the anchor from the optimized plan before the decompiler can see it;
            // - cte_inline_mode = -1: mode 0 (the default) builds an alternative fully
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
            // statement context, and collectAndLockTable() then locks THAT object. When
            // the caller (StmtExecutor for a CREATE BASELINE statement) already installed
            // the outer command's context, leaving it active made collectRelation cache
            // into the outer object while lock() ran on the fresh (empty) one: the nested
            // plan was optimized / decompiled WITHOUT its metadata-stability locks and
            // could race concurrent ALTER / DROP. The exact original (possibly null) is
            // restored in the finally below.
            ctx.setStatementContext(statementContext);
            // planWithLock runs preprocess (SET_VAR hint) -> analyze -> rewrite ->
            // optimize -> postProcess; distribution planning is not needed for the
            // decompiler (the physical plan already carries distribution specs).
            // The root physical plan is the RETURN value of planWithLock: the planner's
            // physicalPlan field is only assigned through the lockCallback used by
            // plan(), so planner.getPhysicalPlan() would be null here.
            Plan resultPlan = planner.planWithLock(logicalPlan,
                    PhysicalProperties.ANY, ExplainLevel.NONE);
            if (!(resultPlan instanceof PhysicalPlan)) {
                throw new AnalysisException("SPM failed to plan SQL: " + originSql);
            }
            // Belt-and-braces for any hint path the pre-scan does not model: the protected
            // variables must still carry the SPM values after planning, otherwise the plan
            // may have been produced without a guard - fail the freeze instead of
            // publishing it.
            verifySpmOverridesIntact(sessionVar);
            return new OptimizeResult((PhysicalPlan) resultPlan,
                    extractCost((PhysicalPlan) resultPlan));
        } finally {
            sessionVar.setTopNLazyMaterializationThreshold(originalTopnLazyThreshold);
            sessionVar.enableCTEMaterialize = originalCteMaterialize;
            sessionVar.inlineCTEReferencedThreshold = originalInlineCteThreshold;
            sessionVar.cteInlineMode = originalCteInlineMode;
            ctx.setStatementContext(originalCtx);
        }
    }

    /**
     * Rejects SET_VAR hints that target an SPM-protected session variable. Public for
     * tests (the hint objects are constructed directly there).
     */
    public static void rejectProtectedSetVarHints(List<SelectHint> hints) throws AnalysisException {
        if (hints == null) {
            return;
        }
        for (SelectHint hint : hints) {
            if (!(hint instanceof SelectHintSetVar)) {
                continue;
            }
            for (String key : ((SelectHintSetVar) hint).getParameters().keySet()) {
                if (key != null && SPM_LOCKED_SET_VAR_KEYS.contains(key.toLowerCase(Locale.ROOT))) {
                    throw new AnalysisException("SPM cannot freeze a plan whose SET_VAR hint targets the"
                            + " SPM-protected session variable '" + key + "'");
                }
            }
        }
    }

    /**
     * Walks the WHOLE statement and rejects any SELECT hint targeting a protected
     * variable. The walk must follow the plans held outside children() (CTE bodies via
     * extraPlans(), IN / EXISTS / scalar subquery plans via SubqueryExpr.queryPlan and
     * through expression coercions): a hint inside a CTE definition or a subquery would
     * otherwise escape the pre-scan and EliminateLogicalSelectHint would apply it AFTER
     * the SPM CTE / TopN serialization overrides were installed, letting CTE inlining or
     * TopN materialization change the frozen plan.
     *
     * Package-visible for tests.
     */
    static void checkProtectedSetVarHints(Plan node) throws AnalysisException {
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
     * Extracts the estimated cost of the root physical plan.
     *
     * The cost is estimated over the PARAMETERIZED plan tree, whose literals are still
     * SpmConstVar / SpmConstList placeholder calls (not concrete values), so predicates
     * such as c > SpmConstVar(...) cannot be folded and their selectivity falls back to
     * the optimizer's default (e.g. 1/3). The absolute value is therefore not the cost
     * of any one concrete query; it is only used as a RELATIVE ranking key among the
     * baselines that share one bind digest (when several stored plans exist, the
     * cheapest frozen plan wins - see BaselineManager), exactly like StarRocks, which
     * also takes the CBO cost of the placeholder-carrying optimized plan
     * (optimizedPlan.getCost()) with no placeholder-specific adjustment.
     *
     * @param physicalPlan the root physical plan
     * @return the cost value (0 when the group cost is not available)
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

        public OptimizeResult(PhysicalPlan physicalPlan, double cost) {
            this.physicalPlan = physicalPlan;
            this.cost = cost;
        }

        public PhysicalPlan getPhysicalPlan() {
            return physicalPlan;
        }

        public double getCost() {
            return cost;
        }
    }
}
