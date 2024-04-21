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
import org.apache.doris.nereids.rules.exploration.mv.MaterializedViewUtils;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.ImmutableList;

/**
 * The cache for materialized view cache
 */
public class MTMVCache {

    // the materialized view plan which should be optimized by the same rules to query
    private final Plan logicalPlan;
    // for stable output order, we should use original plan
    private final Plan originalPlan;

    public MTMVCache(Plan logicalPlan, Plan originalPlan) {
        this.logicalPlan = logicalPlan;
        this.originalPlan = originalPlan;
    }

    public Plan getLogicalPlan() {
        return logicalPlan;
    }

    public Plan getOriginalPlan() {
        return originalPlan;
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
        Plan originPlan = planner.plan(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
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
        return new MTMVCache(mvPlan, originPlan);
    }
}
