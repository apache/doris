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

package org.apache.doris.mtmv;

import org.apache.doris.common.Pair;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.rules.rewrite.MergeProjectable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;

/**
 * The cache for materialized view cache
 */
public class MTMVCache {

    public static final Logger LOG = LogManager.getLogger(MTMVCache.class);

    // The materialized view plan which should be optimized by the same rules to query
    // and will remove top sink and unused sort
    private final Pair<Plan, StructInfo> allRulesRewrittenPlanAndStructInfo;
    // The original rewritten plan of mv def sql
    private final Plan originalFinalPlan;
    private final Statistics statistics;
    private final List<Pair<Plan, StructInfo>> partRulesRewrittenPlanAndStructInfos;

    public MTMVCache(Pair<Plan, StructInfo> allRulesRewrittenPlanAndStructInfo, Plan originalFinalPlan,
            Statistics statistics, List<Pair<Plan, StructInfo>> partRulesRewrittenPlanAndStructInfos) {
        this.allRulesRewrittenPlanAndStructInfo = allRulesRewrittenPlanAndStructInfo;
        this.originalFinalPlan = originalFinalPlan;
        this.statistics = statistics;
        this.partRulesRewrittenPlanAndStructInfos = partRulesRewrittenPlanAndStructInfos;
    }

    public Pair<Plan, StructInfo> getAllRulesRewrittenPlanAndStructInfo() {
        return allRulesRewrittenPlanAndStructInfo;
    }

    public Plan getOriginalFinalPlan() {
        return originalFinalPlan;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public List<Pair<Plan, StructInfo>> getPartRulesRewrittenPlanAndStructInfos() {
        return partRulesRewrittenPlanAndStructInfos;
    }

    /**
     * @param defSql the def sql of materialization
     * @param createCacheContext should create new createCacheContext use MTMVPlanUtil createMTMVContext
     *         or createBasicMvContext
     * @param needCost the plan from def sql should calc cost or not
     * @param needLock should lock when create mtmv cache
     * @param currentContext current context, after create cache,should setThreadLocalInfo
     */
    public static MTMVCache from(String defSql,
            ConnectContext createCacheContext,
            boolean needCost, boolean needLock,
            ConnectContext currentContext) throws AnalysisException {
        StatementContext mvSqlStatementContext = new StatementContext(createCacheContext,
                new OriginStatement(defSql, 0));
        if (!needLock) {
            mvSqlStatementContext.setNeedLockTables(false);
        }
        if (mvSqlStatementContext.getConnectContext().getStatementContext() == null) {
            mvSqlStatementContext.getConnectContext().setStatementContext(mvSqlStatementContext);
        }
        createCacheContext.getStatementContext().setForceRecordTmpPlan(true);
        mvSqlStatementContext.setForceRecordTmpPlan(true);
        boolean originalRewriteFlag = createCacheContext.getSessionVariable().enableMaterializedViewRewrite;
        createCacheContext.getSessionVariable().enableMaterializedViewRewrite = false;
        LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(defSql);
        NereidsPlanner planner = new NereidsPlanner(mvSqlStatementContext);
        try {
            // Can not convert to table sink, because use the same column from different table when self join
            // the out slot is wrong
            if (needCost) {
                // Only in mv rewrite, we need plan with eliminated cost which is used for mv chosen
                planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
            } else {
                // No need cost for performance
                planner.planWithLock(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
            }
            CascadesContext cascadesContext = planner.getCascadesContext();
            Pair<Plan, StructInfo> finalPlanStructInfoPair = constructPlanAndStructInfo(
                    cascadesContext.getRewritePlan(), cascadesContext);
            List<Pair<Plan, StructInfo>> tmpPlanUsedForRewrite = new ArrayList<>();
            for (Plan plan : cascadesContext.getStatementContext().getTmpPlanForMvRewrite()) {
                tmpPlanUsedForRewrite.add(constructPlanAndStructInfo(plan, cascadesContext));
            }
            return new MTMVCache(finalPlanStructInfoPair, cascadesContext.getRewritePlan(), needCost
                    ? cascadesContext.getMemo().getRoot().getStatistics() : null, tmpPlanUsedForRewrite);
        } finally {
            createCacheContext.getStatementContext().setForceRecordTmpPlan(false);
            mvSqlStatementContext.setForceRecordTmpPlan(false);
            createCacheContext.getSessionVariable().enableMaterializedViewRewrite = originalRewriteFlag;
            if (currentContext != null) {
                currentContext.setThreadLocalInfo();
            }
        }
    }

    // Eliminate result sink because sink operator is useless in query rewrite by materialized view
    // and the top sort can also be removed
    private static Pair<Plan, StructInfo> constructPlanAndStructInfo(Plan plan, CascadesContext cascadesContext) {
        Plan mvPlan = plan.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink,
                    Object context) {
                return new LogicalProject(logicalResultSink.getOutput(),
                        false, logicalResultSink.children());
            }
        }, null);
        // Optimize by rules to remove top sort
        mvPlan = MaterializedViewUtils.rewriteByRules(cascadesContext, childContext -> {
            Rewriter.getCteChildrenRewriter(childContext, ImmutableList.of(
                    Rewriter.custom(RuleType.ELIMINATE_SORT, EliminateSort::new),
                    Rewriter.bottomUp(new MergeProjectable()))).execute();
            return childContext.getRewritePlan();
        }, mvPlan, plan, false);
        // Construct structInfo once for use later
        Optional<StructInfo> structInfoOptional = MaterializationContext.constructStructInfo(mvPlan, plan,
                cascadesContext, new BitSet());
        return Pair.of(mvPlan, structInfoOptional.orElse(null));
    }
}
