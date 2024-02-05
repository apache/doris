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

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.jobs.executor.AbstractBatchJobExecutor;
import org.apache.doris.nereids.jobs.executor.Rewriter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.rules.RuleType;
import org.apache.doris.nereids.rules.rewrite.EliminateSort;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;

import com.google.common.collect.Lists;

import java.util.stream.Collectors;

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
        Plan originPlan = planner.plan(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
        // change result sink to table sink for eliminate sort
        Plan mvPlan = originPlan.accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalResultSink(LogicalResultSink<? extends Plan> logicalResultSink, Object context) {
                return new LogicalOlapTableSink<>(new Database(), mtmv, mtmv.getBaseSchema(),
                        mtmv.getPartitionIds(),
                        logicalResultSink.getOutput()
                                .stream().map(NamedExpression.class::cast).collect(Collectors.toList()),
                        mtmv.isPartitionedTable(), DMLCommandType.NONE, logicalResultSink.child());
            }
        }, null);
        // eliminate sort under table sink, because sort is useless for materialized view
        planner.getCascadesContext().setRewritePlan(mvPlan);
        Rewriter rewriter = Rewriter.getCteChildrenRewriter(planner.getCascadesContext(),
                Lists.newArrayList(AbstractBatchJobExecutor.custom(RuleType.ELIMINATE_SORT, EliminateSort::new)));
        rewriter.execute();
        // eliminate logicalTableSink
        mvPlan = planner.getCascadesContext().getRewritePlan().accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalTableSink(LogicalTableSink<? extends Plan> logicalTableSink, Object context) {
                return logicalTableSink.child().accept(this, context);
            }
        }, null);
        return new MTMVCache(mvPlan, originPlan);
    }
}
