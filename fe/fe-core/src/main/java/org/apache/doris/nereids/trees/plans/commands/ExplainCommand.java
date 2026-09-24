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
import org.apache.doris.nereids.spm.SPMPlanner;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertOverwriteTableCommand;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRowLevelDeleteSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalExternalRowLevelMergeSink;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Optional;

/**
 * explain command.
 */
public class ExplainCommand extends Command implements NoForward {

    private static final Logger LOG = LogManager.getLogger(ExplainCommand.class);

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
            LogicalPlan originalPlan = explainPlan;
            // SPM (SQL Plan Management) rewrite for EXPLAIN: mirrors the query path
            // (StmtExecutor SPM integration point) so EXPLAIN reports the matched
            // baseline (id + bindSqlDigest) when enable_spm_rewrite is on.
            if (explainCtx.getSessionVariable().isEnableSpmRewrite() && explainPlan != null) {
                try {
                    long deadline = System.currentTimeMillis()
                            + explainCtx.getSessionVariable().getSpmRewriteTimeoutMs();
                    SPMPlanner spmPlanner = new SPMPlanner();
                    LogicalPlan rewrittenPlan = spmPlanner.tryRewritePlan(explainPlan, deadline);
                    if (rewrittenPlan != null) {
                        explainPlan = rewrittenPlan;
                        explainCtx.getStatementContext().setSpmBaselineApplied(true);
                        explainCtx.getStatementContext().setSpmUsedBaselineId(spmPlanner.getUsedBaselineId());
                    }
                } catch (Throwable e) {
                    LOG.warn("SPM rewrite failed for EXPLAIN, fallback to normal planning", e);
                }
            }
            Optional<NereidsPlanner> explainPlanner =
                    explainable.getExplainPlanner(explainPlan, explainCtx.getStatementContext());
            NereidsPlanner planner = explainPlanner.isPresent()
                    ? explainPlanner.get()
                    : new NereidsPlanner(explainCtx.getStatementContext());

            previousTargetTableId = explainCtx.getSyntheticWriteColTargetTableId();
            if (explainPlan instanceof LogicalExternalRowLevelDeleteSink) {
                if (previousTargetTableId < 0) {
                    explainCtx.setSyntheticWriteColTargetTableId(
                            ((LogicalExternalRowLevelDeleteSink<?>) explainPlan).getTargetTable().getId());
                    resetTargetTableId = true;
                }
            } else if (explainPlan instanceof LogicalExternalRowLevelMergeSink) {
                if (previousTargetTableId < 0) {
                    explainCtx.setSyntheticWriteColTargetTableId(
                            ((LogicalExternalRowLevelMergeSink<?>) explainPlan).getTargetTable().getId());
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
            try {
                planner.plan(logicalPlanAdapter, explainCtx.getSessionVariable().toThrift());
            } catch (Throwable t) {
                // A failed planning of the REPLACEMENT tree must degrade like the query
                // path (StmtExecutor): clear the applied flag and replan the ORIGINAL
                // tree, so EXPLAIN cannot fail for a query that succeeds. The rewritten
                // tree is validated again at execution time, so a broken frozen text
                // must not make EXPLAIN unusable.
                if (explainCtx.getStatementContext().isSpmBaselineApplied()
                        && originalPlan != null && originalPlan != explainPlan) {
                    LOG.warn("SPM EXPLAIN planning failed on the rewritten tree,"
                            + " retrying with the original plan", t);
                    explainCtx.getStatementContext().setSpmBaselineApplied(false);
                    explainCtx.getStatementContext().setSpmUsedBaselineId(-1);
                    explainPlan = originalPlan;
                    Optional<NereidsPlanner> retryPlanner =
                            explainable.getExplainPlanner(explainPlan,
                                    explainCtx.getStatementContext());
                    planner = retryPlanner.isPresent()
                            ? retryPlanner.get()
                            : new NereidsPlanner(explainCtx.getStatementContext());
                    logicalPlanAdapter =
                            new LogicalPlanAdapter(explainPlan, explainCtx.getStatementContext());
                    logicalPlanAdapter.setIsExplain(explainOptions);
                    executor.setParsedStmt(logicalPlanAdapter);
                    planner.plan(logicalPlanAdapter,
                            explainCtx.getSessionVariable().toThrift());
                } else {
                    LOG.warn("SPM EXPLAIN analyze/plan failed; rewritten tree:\n{}",
                            explainPlan.treeString(), t);
                    throw t;
                }
            }
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
                explainCtx.setSyntheticWriteColTargetTableId(previousTargetTableId);
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
