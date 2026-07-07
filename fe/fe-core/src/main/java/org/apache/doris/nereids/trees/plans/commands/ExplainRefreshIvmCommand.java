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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.ivm.IvmRefreshManager;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.ExplainCommand.ExplainLevel;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo.RefreshMode;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;
import java.util.Optional;

/**
 * Explain IVM refresh dry-run plans.
 */
public class ExplainRefreshIvmCommand extends Command implements NoForward {
    private final RefreshMTMVInfo refreshMTMVInfo;
    private final ExplainLevel level;
    private final boolean showPlanProcess;

    /**
     * Creates an EXPLAIN REFRESH command for IVM refresh planning.
     */
    public ExplainRefreshIvmCommand(RefreshMTMVInfo refreshMTMVInfo, ExplainLevel level,
            boolean showPlanProcess) {
        super(PlanType.EXPLAIN_REFRESH_IVM_COMMAND);
        this.refreshMTMVInfo = Objects.requireNonNull(refreshMTMVInfo, "refreshMTMVInfo can not be null");
        this.level = Objects.requireNonNull(level, "level can not be null");
        this.showPlanProcess = showPlanProcess;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (refreshMTMVInfo.getRefreshMode() != RefreshMode.INCREMENTAL) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "EXPLAIN REFRESH only supports IVM materialized views");
        }

        refreshMTMVInfo.analyze(ctx);
        MTMV mtmv = getMtmv();
        ConnectContext planCtx = createExplainConnectContext(mtmv);
        try {
            StatementContext statementContext = new StatementContext(planCtx, null);
            statementContext.setIvmRewriteContext(Optional.of(
                    IvmRewriteContext.incremental(mtmv, true, true)));
            planCtx.setStatementContext(statementContext);
            InsertIntoTableCommand command = createIvmRefreshManager().buildInsertCommand(mtmv);
            runExplainCommand(planCtx, executor, command);
        } finally {
            ctx.setThreadLocalInfo();
        }
    }

    protected ConnectContext createExplainConnectContext(MTMV mtmv) {
        return MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
    }

    protected void runExplainCommand(ConnectContext planCtx, StmtExecutor executor,
            InsertIntoTableCommand command) throws Exception {
        new ExplainCommand(level, command, showPlanProcess).run(planCtx, executor);
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

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitExplainRefreshIvmCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.EXPLAIN;
    }
}
