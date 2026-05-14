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
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.ivm.IvmRefreshExplainResult;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalResultSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;

/**
 * Explain IVM refresh dry-run plans.
 */
public class ExplainRefreshIvmCommand extends Command implements NoForward {
    private static final ShowResultSetMetaData OVERVIEW_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("Item", ScalarType.createVarchar(30)))
            .addColumn(new Column("Delta", ScalarType.createVarchar(-1)))
            .addColumn(new Column("Value", ScalarType.createVarchar(-1)))
            .build();
    private static final ShowResultSetMetaData DELTA_PLAN_META_DATA = ShowResultSetMetaData.builder()
            .addColumn(new Column("IVM Delta Plan", ScalarType.createVarchar(-1)))
            .build();

    private final RefreshMTMVInfo refreshMTMVInfo;
    private final ExplainLevel level;
    private final boolean showPlanProcess;
    private final Integer deltaId;

    /**
     * Creates an EXPLAIN REFRESH command for IVM refresh planning.
     */
    public ExplainRefreshIvmCommand(RefreshMTMVInfo refreshMTMVInfo, ExplainLevel level,
            boolean showPlanProcess, Integer deltaId) {
        super(PlanType.EXPLAIN_REFRESH_IVM_COMMAND);
        this.refreshMTMVInfo = Objects.requireNonNull(refreshMTMVInfo, "refreshMTMVInfo can not be null");
        this.level = Objects.requireNonNull(level, "level can not be null");
        this.showPlanProcess = showPlanProcess;
        this.deltaId = deltaId;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (refreshMTMVInfo.getRefreshMode() != RefreshMode.INCREMENTAL) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN REFRESH only supports IVM materialized views");
        }
        if (deltaId == null && level != ExplainLevel.NORMAL) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN " + level + " REFRESH requires FOR DELTA k");
        }
        if (showPlanProcess && deltaId == null) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN REFRESH PLAN PROCESS requires FOR DELTA k");
        }

        refreshMTMVInfo.analyze(ctx);
        MTMV mtmv = getMtmv();
        IvmRefreshExplainResult result;
        try {
            result = createIvmRefreshManager().explainRefresh(mtmv);
        } finally {
            ctx.setThreadLocalInfo();
        }
        if (deltaId == null) {
            executor.sendResultSet(new ShowResultSet(OVERVIEW_META_DATA, result.formatOverviewRows()));
        } else {
            explainDeltaPlan(ctx, executor, mtmv, result.getDeltaBundle(deltaId).getDeltaPlan());
        }
    }

    private void explainDeltaPlan(ConnectContext ctx, StmtExecutor executor,
            MTMV mtmv, Plan deltaPlan) throws Exception {
        if (!(deltaPlan instanceof LogicalPlan)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "IVM delta plan is not a logical plan: " + deltaPlan.getClass().getSimpleName());
        }
        if (level == ExplainLevel.ANALYZED_PLAN && !showPlanProcess) {
            executor.sendResultSet(new ShowResultSet(DELTA_PLAN_META_DATA,
                    IvmRefreshExplainResult.formatDeltaPlanRows(deltaPlan)));
            return;
        }

        LogicalPlan logicalPlan = removeNestedResultSink((LogicalPlan) deltaPlan);
        ConnectContext planCtx = MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        try (StatementContext deltaStatementContext = new StatementContext(planCtx, null)) {
            planCtx.setStatementContext(deltaStatementContext);
            NereidsPlanner planner = new NereidsPlanner(deltaStatementContext);
            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalPlan, deltaStatementContext);
            ExplainOptions explainOptions = new ExplainOptions(level, showPlanProcess);
            logicalPlanAdapter.setIsExplain(explainOptions);
            executor.setParsedStmt(logicalPlanAdapter);
            planner.plan(logicalPlanAdapter, planCtx.getSessionVariable().toThrift());
            executor.setPlanner(planner);
            if (showPlanProcess) {
                executor.handleExplainPlanProcessStmt(planner.getCascadesContext().getPlanProcesses());
            } else {
                executor.handleExplainStmt(planner.getExplainString(explainOptions), true);
            }
            for (ScanNode scanNode : planner.getScanNodes()) {
                scanNode.stop();
            }
        } finally {
            ctx.setThreadLocalInfo();
        }
    }

    private LogicalPlan removeNestedResultSink(LogicalPlan logicalPlan) {
        if (!(logicalPlan instanceof LogicalResultSink)) {
            return logicalPlan;
        }

        // IVM normalized plans can contain nested result sinks because the MV query is parsed as a
        // query statement with its own result sink, and MTMV analyze wraps the logical query with
        // another result sink before planning. For EXPLAIN ANALYZED PLAN we print the raw delta
        // rewriter output, but for fragment/logical/physical explain we continue planning the delta
        // plan. Collapse consecutive root result sinks first so Nereids sees a single root sink,
        // matching the shape of a normal query explain.
        LogicalResultSink<?> resultSink = (LogicalResultSink<?>) logicalPlan;
        Plan child = resultSink.child();
        if (!(child instanceof LogicalResultSink)) {
            return logicalPlan;
        }
        while (child instanceof LogicalResultSink) {
            child = ((LogicalResultSink<?>) child).child();
        }
        return resultSink.withGroupExprLogicalPropChildren(resultSink.getGroupExpression(),
                Optional.of(resultSink.getLogicalProperties()), Collections.singletonList(child));
    }

    private MTMV getMtmv() throws org.apache.doris.common.AnalysisException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(refreshMTMVInfo.getMvName().getDb());
        return (MTMV) db.getTableOrMetaException(refreshMTMVInfo.getMvName().getTbl(),
                TableType.MATERIALIZED_VIEW);
    }

    IvmRefreshManager createIvmRefreshManager() {
        return new IvmRefreshManager();
    }

    public RefreshMTMVInfo getRefreshMTMVInfo() {
        return refreshMTMVInfo;
    }

    public ExplainLevel getLevel() {
        return level;
    }

    public boolean showPlanProcess() {
        return showPlanProcess;
    }

    public Integer getDeltaId() {
        return deltaId;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExplainRefreshIvmCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXPLAIN;
    }
}
