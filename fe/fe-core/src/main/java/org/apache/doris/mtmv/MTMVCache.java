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

import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.exploration.mv.MaterializationContext;
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.exploration.mv.StructInfo;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.statistics.Statistics;

import com.google.common.collect.ImmutableList;

import java.util.BitSet;
import java.util.Optional;

/**
 * The cache for materialized view cache
 */
public class MTMVCache {

    // The materialized view plan which should be optimized by the same rules to query
    // and will remove top sink and unused sort
    private final Plan logicalPlan;
    // The original plan of mv def sql
    private final Plan originalPlan;
    private final Statistics statistics;
    private final StructInfo structInfo;

    public MTMVCache(Plan logicalPlan, Plan originalPlan, Statistics statistics, StructInfo structInfo) {
        this.logicalPlan = logicalPlan;
        this.originalPlan = originalPlan;
        this.statistics = statistics;
        this.structInfo = structInfo;
    }

    public Plan getLogicalPlan() {
        return logicalPlan;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
    }

    public Statistics getStatistics() {
        return statistics;
    }

    public StructInfo getStructInfo() {
        return structInfo;
    }

    public static MTMVCache from(MTMV mtmv, ConnectContext connectContext) {
        LogicalPlan unboundMvPlan = new NereidsParser().parseSingle(mtmv.getQuerySql());
        StatementContext mvSqlStatementContext = new StatementContext(connectContext,
                new OriginStatement(mtmv.getQuerySql(), 0));
        NereidsPlanner planner = new NereidsPlanner(mvSqlStatementContext);
        if (mvSqlStatementContext.getConnectContext().getStatementContext() == null) {
            mvSqlStatementContext.getConnectContext().setStatementContext(mvSqlStatementContext);
        }
        // Can not convert to table sink, because use the same column from different table when self join
        // the out slot is wrong
        planner.plan(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.ALL_PLAN);
        Plan originPlan = planner.getCascadesContext().getRewritePlan();
        // Eliminate result sink because sink operator is useless in query rewrite by materialized view
        // and the top sort can also be removed
        Plan mvPlan = originPlan.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, Object context) {
                return logicalResultSink.child().accept(this, context);
            }
        }, null);
        // Optimize by rules to remove top sort
        CascadesContext parentCascadesContext = CascadesContext.initContext(mvSqlStatementContext, mvPlan,
                PhysicalProperties.ANY);
        mvPlan = MaterializedViewUtils.rewriteByRules(parentCascadesContext, childContext -> {
            Rewriter.getCteChildrenRewriter(childContext,
                    ImmutableList.of(Rewriter.custom(RuleType.ELIMINATE_SORT, EliminateSort::new))).execute();
            return childContext.getRewritePlan();
        }, mvPlan, originPlan);
        // Construct structInfo once for use later
        Optional<StructInfo> structInfoOptional = MaterializationContext.constructStructInfo(mvPlan, originPlan,
                planner.getCascadesContext(),
                new BitSet());
        return new MTMVCache(mvPlan, originPlan, planner.getCascadesContext().getMemo().getRoot().getStatistics(),
                structInfoOptional.orElseGet(() -> null));
    }
}
