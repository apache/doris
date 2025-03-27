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
 * Individual materialized view rewriter based CBO
 */
public class PreMaterializedViewRewriter {
    public static BitSet NEED_PRE_REWRITE_RULE_TYPES = new BitSet();
    private static final Logger LOG = LogManager.getLogger(PreMaterializedViewRewriter.class);

    static {
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
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.LIMIT_SORT_TO_TOP_N.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.LIMIT_AGG_TO_TOPN_AGG.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.ELIMINATE_CONST_JOIN_CONDITION.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.MERGE_PERCENTILE_TO_ARRAY.ordinal());
        NEED_PRE_REWRITE_RULE_TYPES.set(RuleType.SUM_LITERAL_REWRITE.ordinal());
    }

    /**
     * Materialize view pre rewrite
     */
    public static Plan rewrite(CascadesContext cascadesContext) {
        if (cascadesContext.getMaterializationContexts().isEmpty()
                || !cascadesContext.getStatementContext().isNeedPreRewrite()) {
            return null;
        }
        // Do optimize
        new Optimizer(cascadesContext).execute();
        // Chose the best physical plan
        Group root = cascadesContext.getMemo().getRoot();
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(root,
                cascadesContext.getCurrentJobContext().getRequiredProperties(), cascadesContext);
        Pair<Map<List<String>, MaterializationContext>, BitSet> chosenMaterializationAndUsedTable
                = MaterializedViewUtils.getChosenMaterializationAndUsedTable(physicalPlan,
                cascadesContext.getAllMaterializationContexts());
        // Calc the table id set which is used by physical plan
        cascadesContext.getMemo().incrementAndGetRefreshVersion();
        // Extract logical plan by table id set by the corresponding best physical plan
        StructInfo structInfo = root.getStructInfoMap().getStructInfo(cascadesContext,
                chosenMaterializationAndUsedTable.value(), root, null, true);
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
     * Calc need to record tmp plan for rewrite or not, this would be calculated in RBO phase
     * if needed should return true, or would return false
     */
    public static boolean needRecordTmpPlanForRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        PreRewriteStrategy preRewriteStrategy = PreRewriteStrategy.getEnum(
                cascadesContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy());
        if (PreRewriteStrategy.NOT_IN_RBO.equals(preRewriteStrategy)) {
            return false;
        }
        if (statementContext.isForceRecordTmpPlan()) {
            return true;
        }
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            // current statement context doesn't have hook, doesn't use pre RBO materialized view rewrite
            return false;
        }
        return !statementContext.getCandidateMVs().isEmpty() || !statementContext.getCandidateMTMVs().isEmpty();
    }

    /**
     * Calc need pre mv rewrite or not, this would be calculated after RBO phase
     */
    public static boolean needPreRewrite(CascadesContext cascadesContext) {
        StatementContext statementContext = cascadesContext.getStatementContext();
        if (statementContext.getTmpPlanForMvRewrite().isEmpty()) {
            LOG.debug("does not need pre rewrite, because TmpPlanForMvRewrite is empty, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return false;
        }
        if (!MaterializedViewUtils.containMaterializedViewHook(statementContext)) {
            LOG.debug("does not need pre rewrite, because no hook exists, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return false;
        }
        boolean outputAnyEquals = false;
        Plan finalRewritePlan = cascadesContext.getRewritePlan();
        for (Plan tmpPlanForRewrite : statementContext.getTmpPlanForMvRewrite()) {
            if (finalRewritePlan.getLogicalProperties().equals(tmpPlanForRewrite.getLogicalProperties())) {
                outputAnyEquals = true;
                break;
            }
        }
        if (!outputAnyEquals) {
            // if tmp plan has no same logical properties to the finalRewritePlan, should not be written in rbo
            LOG.debug("does not need pre rewrite, because outputAnyEquals is false, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
            return false;
        }
        if (Optimizer.isDpHyp(cascadesContext)) {
            // dp hyper only support one group expression in each group when init
            LOG.debug("does not need pre rewrite, because is dp hyper optimize, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
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
        if (!shouldPreRewrite) {
            LOG.debug("does not need pre rewrite, because needPreRewriteRuleSet is empty or "
                            + "preRewriteStrategy is not FORCE_IN_RBO, query id is {}",
                    cascadesContext.getConnectContext().getQueryIdentifier());
        }
        return shouldPreRewrite;
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
