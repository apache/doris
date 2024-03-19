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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.Partition;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanRewriter;
import org.apache.doris.nereids.trees.plans.visitor.DefaultPlanVisitor;
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
        unboundMvPlan = unboundMvPlan.accept(new DefaultPlanVisitor<LogicalPlan, Void>() {
            // convert to table sink to eliminate sort under table sink, because sort under result sink can not be
            // eliminated
            @Override
            public LogicalPlan visitUnboundResultSink(UnboundResultSink<? extends Plan> unboundResultSink,
                    Void context) {
                return new UnboundTableSink<>(mtmv.getFullQualifiers(),
                        mtmv.getBaseSchema().stream().map(Column::getName).collect(Collectors.toList()),
                        Lists.newArrayList(),
                        mtmv.getPartitions().stream().map(Partition::getName).collect(Collectors.toList()),
                        unboundResultSink.child());
            }
        }, null);
        Plan originPlan = planner.plan(unboundMvPlan, PhysicalProperties.ANY, ExplainLevel.REWRITTEN_PLAN);
        // eliminate logicalTableSink because sink operator is useless in query rewrite by materialized view
        Plan mvPlan = planner.getCascadesContext().getRewritePlan().accept(new DefaultPlanRewriter<Object>() {
            @Override
            public Plan visitLogicalTableSink(LogicalTableSink<? extends Plan> logicalTableSink, Object context) {
                return logicalTableSink.child().accept(this, context);
            }
        }, null);
        return new MTMVCache(mvPlan, originPlan);
    }
}
