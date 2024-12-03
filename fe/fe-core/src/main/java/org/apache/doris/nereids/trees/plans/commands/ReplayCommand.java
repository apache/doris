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
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.minidump.Minidump;
import org.apache.doris.nereids.minidump.MinidumpUtils;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import org.json.JSONObject;

import java.util.List;

/**
 * replay command.
 */
public class ReplayCommand extends Command implements NoForward {

    private final String dumpFileFullPath;

    private final LogicalPlan plan;

    private final ReplayType replayType;

    public ReplayCommand(PlanType type, String dumpFileFullPath, LogicalPlan plan, ReplayType replayType) {
        super(type);
        this.dumpFileFullPath = dumpFileFullPath;
        this.plan = plan;
        this.replayType = replayType;
    }

    public String getDumpFileFullPath() {
        return dumpFileFullPath;
    }

    public ReplayType getReplayType() {
        return replayType;
    }

    /**
     * explain level.
     */
    public enum ReplayType {
        DUMP,
        PLAY
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (this.replayType == ReplayType.DUMP) {
            handleDump(ctx, executor);
        } else if (this.replayType == ReplayType.PLAY) {
            handleLoad();
        }
    }

    private void handleDump(ConnectContext ctx, StmtExecutor executor) throws Exception {
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

    private void handleLoad() throws Exception {
        // 1. check fe version, if not matched throw exception
        // 2. load every thing from minidump file and replace original ones
        Minidump minidump = MinidumpUtils.loadMinidumpInputs(dumpFileFullPath);
        // 3. run nereids planner with sql in minidump file
        StatementContext statementContext = new StatementContext(ConnectContext.get(),
                new OriginStatement(minidump.getSql(), 0));
        statementContext.setTables(minidump.getTables());
        ConnectContext.get().setStatementContext(statementContext);
        JSONObject resultPlan = MinidumpUtils.executeSql(minidump.getSql());
        JSONObject minidumpResult = new JSONObject(minidump.getResultPlanJson());

        List<String> differences = MinidumpUtils.compareJsonObjects(minidumpResult, resultPlan, "");
        assert (differences.isEmpty());
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
