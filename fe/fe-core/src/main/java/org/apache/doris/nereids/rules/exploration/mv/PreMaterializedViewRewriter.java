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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.PlannerHook;
import org.apache.doris.nereids.jobs.executor.Optimizer;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.trees.plans.physical.PhysicalCatalogRelation;
import org.apache.doris.nereids.trees.plans.physical.PhysicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

/**
 * Individual materialized view rewriter based CBO
 */
public class PreMaterializedViewRewriter {

    public static BitSet NEED_PRE_REWRITE_RULE_TYPES = new BitSet();

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
    }

    /**
     * Materialize view pre rewrite
     */
    public static void rewrite(CascadesContext cascadesContext) {
        for (PlannerHook hook : cascadesContext.getStatementContext().getPlannerHooks()) {
            // call hook manually to generate materialization context
            hook.afterAnalyze(cascadesContext);
        }
        if (cascadesContext.getMaterializationContexts().isEmpty()
                || PreRewriteStrategy.NOT_IN_RBO.toString().equals(
                cascadesContext.getConnectContext().getSessionVariable().getPreMaterializedViewRewriteStrategy())
                || !MaterializedViewUtils.containMaterializedViewHook(cascadesContext.getStatementContext())) {
            return;
        }
        // Do optimize
        new Optimizer(cascadesContext).execute();
        // Chose the best physical plan
        Group root = cascadesContext.getMemo().getRoot();
        PhysicalPlan physicalPlan = NereidsPlanner.chooseBestPlan(root,
                cascadesContext.getCurrentJobContext().getRequiredProperties(), cascadesContext);
        Pair<CascadesContext, BitSet> collectTableContext = Pair.of(cascadesContext, new BitSet());
        final Set<Boolean> usedMv = new HashSet<>();
        // If cte, how handle?
        physicalPlan.accept(new DefaultPlanVisitor<Void, Pair<CascadesContext, BitSet>>() {
            @Override
            public Void visitPhysicalCatalogRelation(PhysicalCatalogRelation catalogRelation,
                    Pair<CascadesContext, BitSet> ctx) {
                ctx.value().set(ctx.key().getStatementContext().getTableId(catalogRelation.getTable()).asInt());
                if (catalogRelation.getTable() instanceof MTMV) {
                    usedMv.add(true);
                }
                return null;
            }
        }, collectTableContext);
        // Calc the table id set which is used by physical plan
        boolean tmpEnableNestMaterializedViewRewrite =
                cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite;
        try {
            cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite = true;
            cascadesContext.getMemo().incrementAndGetRefreshVersion();
            root.getstructInfoMap().refresh(root, cascadesContext, new HashSet<>());
        } finally {
            cascadesContext.getConnectContext().getSessionVariable().enableMaterializedViewNestRewrite =
                    tmpEnableNestMaterializedViewRewrite;
        }
        // Extract logical plan by table id set by the corresponding best physical plan
        StructInfo structInfo = root.getstructInfoMap().getStructInfo(cascadesContext,
                collectTableContext.second, root, null);
        if (structInfo != null && !usedMv.isEmpty()) {
            cascadesContext.getStatementContext().addRewrittenPlanByMv(structInfo.getOriginalPlan());
        }
    }

    public static BitSet getNeedPreRewriteRule() {
        return NEED_PRE_REWRITE_RULE_TYPES;
    }

    public static boolean needPreRewrite(BitSet appliedRules, PreRewriteStrategy preRewriteStrategy) {
        BitSet needPreRewriteRuleTypes = (BitSet) getNeedPreRewriteRule().clone();
        needPreRewriteRuleTypes.and(appliedRules);
        return !needPreRewriteRuleTypes.isEmpty() || PreRewriteStrategy.FORCE_IN_ROB.equals(preRewriteStrategy);
    }

    /**
     * PreRewriteStrategy from materialized view rewrite
     */
    public enum PreRewriteStrategy {
        // Force transparent rewriting in the RBO phase
        FORCE_IN_ROB,
        // Attempt transparent rewriting in the RBO phase
        TRY_IN_RBO,
        // Do not attempt rewriting in the RBO phase; apply only during the CBO phase
        NOT_IN_RBO
    }
}
