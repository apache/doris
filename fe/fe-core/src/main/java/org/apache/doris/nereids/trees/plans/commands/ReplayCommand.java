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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * replay command: PLAN REPLAYER DUMP query.
 */
public class ReplayCommand extends Command implements NoForward {

    private final LogicalPlan plan;

    public ReplayCommand(PlanType type, LogicalPlan plan) {
        super(type);
        this.plan = plan;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(plan, ctx.getStatementContext());
        MinidumpUtils.openDump();
        executor.setParsedStmt(logicalPlanAdapter);
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        if (ctx.getSessionVariable().isEnableMaterializedViewRewrite()) {
            ctx.getStatementContext().addPlannerHook(InitMaterializationContextHook.INSTANCE);
        }
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.setPlanner(planner);
        executor.checkBlockRules();
        executor.handleReplayStmt(MinidumpUtils.getHttpGetString());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitReplayCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REPLAY;
    }
}
