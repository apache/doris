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

import org.apache.doris.analysis.ExplainOptions;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.rules.exploration.mv.InitMaterializationContextHook;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergDeleteSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalIcebergMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Optional;

/**
 * explain command.
 */
public class ExplainCommand extends Command implements NoForward {

    /**
     * explain level.
     */
    public enum ExplainLevel {
        NONE(false),
        NORMAL(false),
        VERBOSE(false),
        TREE(false),
        GRAPH(false),
        PARSED_PLAN(true),
        ANALYZED_PLAN(true),
        REWRITTEN_PLAN(true),
        OPTIMIZED_PLAN(true),
        SHAPE_PLAN(true),
        MEMO_PLAN(true),
        DISTRIBUTED_PLAN(true),
        ALL_PLAN(true)
        ;

        public final boolean isPlanLevel;

        ExplainLevel(boolean isPlanLevel) {
            this.isPlanLevel = isPlanLevel;
        }
    }

    private final ExplainLevel level;
    private final LogicalPlan logicalPlan;
    private final boolean showPlanProcess;

    public ExplainCommand(ExplainLevel level, LogicalPlan logicalPlan, boolean showPlanProcess) {
        super(PlanType.EXPLAIN_COMMAND);
        this.level = level;
        this.logicalPlan = logicalPlan;
        this.showPlanProcess = showPlanProcess;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!(logicalPlan instanceof Explainable)) {
            throw new AnalysisException(logicalPlan.getClass().getSimpleName() + " cannot be explained");
        }
        ConnectContext previousCtx = ConnectContext.get();
        Explainable explainable = (Explainable) logicalPlan;
        ConnectContext explainCtx = null;
        long previousTargetTableId = -1;
        boolean resetTargetTableId = false;
        try {
            explainCtx = explainable.getExplainConnectContext(ctx);
            if (explainable instanceof InsertIntoTableCommand
                    || explainable instanceof InsertOverwriteTableCommand
                    || explainable instanceof UpdateCommand) {
                explainCtx.getStatementContext().setIsInsert(true);
            }
            if (explainable instanceof DeleteFromCommand) {
                explainCtx.getStatementContext().setIsDelete(true);
            }
            LogicalPlan explainPlan = ((LogicalPlan) explainable.getExplainPlan(explainCtx));
            Optional<NereidsPlanner> explainPlanner =
                    explainable.getExplainPlanner(explainPlan, explainCtx.getStatementContext());
            NereidsPlanner planner = explainPlanner.isPresent()
                    ? explainPlanner.get()
                    : new NereidsPlanner(explainCtx.getStatementContext());

            previousTargetTableId = explainCtx.getIcebergRowIdTargetTableId();
            if (explainPlan instanceof LogicalIcebergDeleteSink) {
                if (previousTargetTableId < 0) {
                    explainCtx.setIcebergRowIdTargetTableId(
                            ((LogicalIcebergDeleteSink<?>) explainPlan).getTargetTable().getId());
                    resetTargetTableId = true;
                }
            } else if (explainPlan instanceof LogicalIcebergMergeSink) {
                if (previousTargetTableId < 0) {
                    explainCtx.setIcebergRowIdTargetTableId(
                            ((LogicalIcebergMergeSink<?>) explainPlan).getTargetTable().getId());
                    resetTargetTableId = true;
                }
            }
            LogicalPlanAdapter logicalPlanAdapter =
                    new LogicalPlanAdapter(explainPlan, explainCtx.getStatementContext());
            ExplainOptions explainOptions = new ExplainOptions(level, showPlanProcess);
            logicalPlanAdapter.setIsExplain(explainOptions);
            executor.setParsedStmt(logicalPlanAdapter);
            if (explainCtx.getSessionVariable().isEnableMaterializedViewRewrite()) {
                explainCtx.getStatementContext().addPlannerHook(InitMaterializationContextHook.INSTANCE);
            }
            planner.plan(logicalPlanAdapter, explainCtx.getSessionVariable().toThrift());
            executor.setPlanner(planner);
            // Skip SQL block rules check for EXPLAIN statements since they only show
            // the execution plan without actually executing the query
            if (showPlanProcess) {
                executor.handleExplainPlanProcessStmt(planner.getCascadesContext().getPlanProcesses());
            } else {
                executor.handleExplainStmt(planner.getExplainString(explainOptions), true);
            }
            for (ScanNode scanNode : planner.getScanNodes()) {
                scanNode.stop();
            }
        } finally {
            if (resetTargetTableId) {
                explainCtx.setIcebergRowIdTargetTableId(previousTargetTableId);
            }
            if (ConnectContext.get() != previousCtx) {
                ConnectContext.remove();
                if (previousCtx != null) {
                    previousCtx.setThreadLocalInfo();
                }
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExplainCommand(this, context);
    }

    public ExplainLevel getLevel() {
        return level;
    }

    public LogicalPlan getLogicalPlan() {
        return logicalPlan;
    }

    public boolean showPlanProcess() {
        return showPlanProcess;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXPLAIN;
    }

    @Override
    public String toDigest() {
        StringBuilder sb = new StringBuilder();
        sb.append("EXPLAIN ");
        sb.append(logicalPlan.toDigest());
        return sb.toString();
    }
}
