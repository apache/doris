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

package org.apache.doris.nereids.rules.exploration.mv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;

import org.apache.commons.lang3.EnumUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Runs an early, isolated CBO pass when shape-changing RBO rules could otherwise hide a transparent
 * materialized view rewrite candidate.
 *
 * <p>The pre-rewrite flow is:
 * <pre>
 * analyzed plan
 *      |
 *      v
 * RecordPlanForMvPreRewrite saves a normalized plan before shape-changing RBO rules
 *      |
 *      v
 * RBO records every successfully applied RuleType
 *      |
 *      v
 * needPreRewrite: applied rules intersect NEED_PRE_REWRITE_RULE_TYPES, or strategy is FORCE_IN_RBO
 *      |
 *      v
 * initialize MV contexts from the saved plan
 *      |
 *      v
 * rewrite: explore MV alternatives -> choose the best physical plan -> recover its logical plan
 *      |
 *      v
 * continue the normal rewrite and optimization pipeline with the MV-based plan
 * </pre>
 */
public class PreMaterializedViewRewriter {
    /**
     * Rules whose successful application can materially change the plan shape seen by MV matching.
     *
     * <p>In {@link #needPreRewrite(CascadesContext)}, this mask is intersected with the rules actually applied
     * during RBO. A non-empty intersection means that matching only the post-RBO plan could miss an MV alternative,
     * so the saved pre-RBO plan is sent through the early CBO rewrite path.
     *
     * <p>When adding a shape-changing RBO rule that can move or transform joins, aggregates, limits, windows,
     * projections, or scan expressions relevant to MV matching, add its {@link RuleType} here as well.
     */
    public static BitSet NEED_PRE_REWRITE_RULE_TYPES = new BitSet();
    private static final Logger LOG = LogManager.getLogger(PreMaterializedViewRewriter.class);

    static {
        // TopN and limit pushdown rules change which operators belong to the query region matched with an MV.
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_PROJECT_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_TOP_N_DISTINCT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_LIMIT_DISTINCT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_PROJECT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_PROJECT_WINDOW.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_UNION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_LIMIT_THROUGH_WINDOW.ordinal());

        // Join and expression normalization rules change the graph edges or expression forms used for matching.
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_CONST_JOIN_CONDITION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.MERGE_PERCENTILE_TO_ARRAY.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.SUM_LITERAL_REWRITE.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.DISTINCT_AGG_STRATEGY_SELECTOR.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.CONSTANT_PROPAGATION.ordinal());

        // Scan, aggregate, join, and TopN rewrites below change structures consumed by specialized MV rules.
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PUSH_DOWN_VIRTUAL_COLUMNS_INTO_OLAP_SCAN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.DISTINCT_AGGREGATE_SPLIT.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PROCESS_SCALAR_AGG_MUST_USE_MULTI_DISTINCT.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_GROUP_BY_KEY_BY_UNIFORM.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_GROUP_BY_KEY.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.SALT_JOIN.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.PULL_UP_PROJECT_EXPR_UNDER_TOPN.ordinal());
    }

    /**
     * Optimize the saved pre-RBO plan with MV exploration and return the logical plan represented by the chosen
     * physical alternative.
     *
     * <p>Returning a logical plan is important: the caller still needs to run the remaining rewrite and CBO stages.
     * A null result means either pre-rewrite is disabled or the best alternative does not use a materialization.
     */
    public static Plan rewrite(CascadesContext cascadesContext) {
        if (cascadesContext.getMaterializationContexts().isEmpty()
                || !cascadesContext.getStatementContext().isNeedPreMvRewrite()) {
            return null;
        }
        // Step 1: explore and cost all alternatives, including MV alternatives registered for pre-rewrite.
        new Optimizer(cascadesContext).execute();
        // Step 2: choose the cheapest physical alternative from the isolated memo.
        Group root = cascadesContext.getMemo().getRoot();
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(root,
                cascadesContext.getCurrentJobContext().getRequiredProperties(), cascadesContext);
        Pair<Map<List<String>, MaterializationContext>, BitSet> chosenMaterializationAndUsedTable
                = MaterializedViewUtils.getChosenMaterializationAndUsedTable(physicalPlan,
                cascadesContext.getAllMaterializationContexts());
        // Step 3: use the chosen MV/table set to recover the corresponding logical expression from the memo.
        StructInfo structInfo = root.getStructInfoMap().getStructInfo(cascadesContext,
                chosenMaterializationAndUsedTable.value(), root, null, true, false);
        if (structInfo == null) {
            LOG.error("preMaterializedViewRewriter rewrite structInfo is null, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
        }
        if (structInfo != null && !chosenMaterializationAndUsedTable.key().isEmpty()) {
            return structInfo.getOriginalPlan();
        }
        return null;
    }

    public static BitSet getNeedPreRewriteRule() {
        return NEED_PRE_REWRITE_RULE_TYPES;
    }

    /**
     * Decide whether RBO should preserve a normalized plan before later rules change its shape.
     * Recording is allowed only when the strategy permits pre-rewrite and the statement has candidate MVs.
     */
    public static boolean needRecordTmpPlanForRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        PreRewriteStrategy preRewriteStrategy = PreRewriteStrategy.getEnum(
                cascadesContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy());
        if (statementContext.isForceRecordTmpPlan()) {
            return true;
        }
        if (PreRewriteStrategy.NOT_IN_RBO.equals(preRewriteStrategy)) {
            return false;
        }
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            // current statement context doesn't have hook, doesn't use pre RBO materialized view rewrite
            return false;
        }
        return !statementContext.getCandidateMVs().isEmpty() || !statementContext.getCandidateMTMVs().isEmpty();
    }

    /**
     * Decide after RBO whether to run the saved plan through pre-rewrite.
     *
     * <p>TRY_IN_RBO requires at least one applied rule from {@link #NEED_PRE_REWRITE_RULE_TYPES}; FORCE_IN_RBO
     * bypasses that rule-mask condition. Both strategies still require a recorded plan, an MV hook, and a supported
     * optimizer mode.
     */
    public static boolean needPreRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (!needRecordTmpPlanForRewrite(cascadesContext)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("needPreRewrite found not need record tmp plan, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        if (statementContext.getTmpPlanForMvRewrite().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because TmpPlanForMvRewrite is empty, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because no hook exists, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        if (Optimizer.isDpHyp(cascadesContext)) {
            // dp hyper only support one group expression in each group when init
            if (LOG.isDebugEnabled()) {
                LOG.debug("does not need pre rewrite, because is dp hyper optimize, query id is {}",
                        cascadesContext.getConnectContext().getQueryIdentifier());
            }
            return false;
        }
        // if rewrite success rule not in NeedPreRewriteRule, should not be written in rbo
        BitSet appliedRules = statementContext.getNeedPreMvRewriteRuleMasks();
        BitSet needPreRewriteRuleSet = (BitSet) getNeedPreRewriteRule().clone();
        needPreRewriteRuleSet.and(appliedRules);
        PreRewriteStrategy preRewriteStrategy = PreRewriteStrategy.getEnum(
                statementContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy());
        boolean shouldPreRewrite = !needPreRewriteRuleSet.isEmpty()
                || PreRewriteStrategy.FORCE_IN_RBO.equals(preRewriteStrategy);
        if (!shouldPreRewrite && LOG.isDebugEnabled()) {
            LOG.debug("does not need pre rewrite, because needPreRewriteRuleSet is empty or "
                            + "preRewriteStrategy is not FORCE_IN_RBO, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
        }
        return shouldPreRewrite;
    }

    /**
     * convert millis to ceiling seconds
     */
    public static int convertMillisToCeilingSeconds(long milliseconds) {
        if (milliseconds <= 0) {
            return 0;
        }
        double secondsAsDouble = (double) milliseconds / 1000.0;
        double ceilingSeconds = Math.ceil(secondsAsDouble);
        return (int) ceilingSeconds;
    }

    /**
     * PreRewriteStrategy from materialized view rewrite
     */
    public enum PreRewriteStrategy {
        // Force transparent rewriting in the RBO phase
        FORCE_IN_RBO,
        // Attempt transparent rewriting in the RBO phase
        TRY_IN_RBO,
        // Do not attempt rewriting in the RBO phase; apply only during the CBO phase
        NOT_IN_RBO;

        public static PreRewriteStrategy getEnum(String name) {
            return EnumUtils.getEnum(PreRewriteStrategy.class, name);
        }
    }
}
