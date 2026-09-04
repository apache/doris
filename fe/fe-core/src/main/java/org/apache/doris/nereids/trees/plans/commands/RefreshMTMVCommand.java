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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mtmv.ivm.IvmDryRunLimit;
import org.apache.doris.mtmv.ivm.IvmIncrRefreshManager;
import org.apache.doris.mtmv.ivm.IvmRewriteContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.RefreshMTMVInfo;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * refresh mtmv
 */
public class RefreshMTMVCommand extends Command implements Forward, Explainable {
    private final RefreshMTMVInfo refreshMTMVInfo;
    // Whether EXPLAIN REFRESH should include up-to-date streams.
    private final boolean includeExhaustedStreams;
    // Dry run computes the delta query and streams rows back without writing anything.
    private final boolean dryRun;
    // Only used when dryRun is true: optional offset/count cap for the returned delta rows.
    private final Optional<IvmDryRunLimit> dryRunLimit;
    private Plan explainPlan;
    private Optional<NereidsPlanner> explainPlanner = Optional.empty();

    public RefreshMTMVCommand(RefreshMTMVInfo refreshMTMVInfo) {
        this(refreshMTMVInfo, false, false, Optional.empty());
    }

    public RefreshMTMVCommand(RefreshMTMVInfo refreshMTMVInfo, boolean includeExhaustedStreams) {
        this(refreshMTMVInfo, includeExhaustedStreams, false, Optional.empty());
    }

    public RefreshMTMVCommand(RefreshMTMVInfo refreshMTMVInfo, boolean includeExhaustedStreams,
            boolean dryRun, Optional<IvmDryRunLimit> dryRunLimit) {
        super(PlanType.REFRESH_MTMV_COMMAND);
        this.refreshMTMVInfo = Objects.requireNonNull(refreshMTMVInfo, "require refreshMTMVInfo object");
        this.includeExhaustedStreams = includeExhaustedStreams;
        this.dryRun = dryRun;
        this.dryRunLimit = Objects.requireNonNull(dryRunLimit, "require dryRunLimit object");
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        refreshMTMVInfo.analyze(ctx);
        if (dryRun) {
            dryRunRefresh(ctx, executor);
        } else {
            Env.getCurrentEnv().getMtmvService().refreshMTMV(refreshMTMVInfo);
        }
    }

    // Real refresh forwards to master with sync; dry run is read-only and must run locally,
    // streaming rows to the client instead of materializing all delta rows into one RPC frame.
    @Override
    public RedirectStatus toRedirectStatus() {
        return dryRun ? RedirectStatus.NO_FORWARD : RedirectStatus.FORWARD_WITH_SYNC;
    }

    private void dryRunRefresh(ConnectContext ctx, StmtExecutor executor) throws Exception {
        MTMV mtmv = getMtmv();
        if (!mtmv.isIvm()) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "REFRESH MATERIALIZED VIEW ... INCREMENTAL WITH DRY RUN "
                            + "only supports IVM materialized views");
        }

        // createMTMVContext installs the internal ADMIN context as the thread-local
        // ConnectContext. Restore the caller's context afterwards, mirroring ExplainCommand:
        // the next statement of the same multi-statement request must not observe the
        // internal identity/session.
        ConnectContext previousCtx = ConnectContext.get();
        try {
            ConnectContext internalCtx = MTMVPlanUtil.createMTMVContext(
                    mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
            StatementContext stmtCtx = createDryRunStatementContext(mtmv, internalCtx);

            LogicalPlan queryPlan = new IvmIncrRefreshManager().buildQueryPlan(mtmv);
            LogicalPlanAdapter adapter = new LogicalPlanAdapter(queryPlan, stmtCtx);
            adapter.setOrigStmt(new OriginStatement(mtmv.getQuerySql(), 0));

            // Execute on a dedicated internal executor (admin identity, MV session variables)
            // and stream each batch to the client's real mysql channel. The internal executor
            // owns a fresh query id that KILL QUERY cannot reach, so forward cancellations of
            // the outer statement (Ctrl+C / KILL / timeout) to it while it runs.
            StmtExecutor internalExecutor = new StmtExecutor(internalCtx, adapter);
            internalCtx.setExecutor(internalExecutor);
            executor.setCancelDelegate(internalExecutor::cancel);
            internalExecutor.executeInternalQueryAndSend(adapter, ctx.getMysqlChannel());
            ctx.getState().setEof();
        } finally {
            executor.clearCancelDelegate();
            if (ConnectContext.get() != previousCtx) {
                ConnectContext.remove();
                if (previousCtx != null) {
                    previousCtx.setThreadLocalInfo();
                }
            }
        }
    }

    @VisibleForTesting
    StatementContext createDryRunStatementContext(MTMV mtmv, ConnectContext internalCtx) {
        StatementContext stmtCtx = new StatementContext(
                internalCtx, new OriginStatement(mtmv.getQuerySql(), 0));
        stmtCtx.setIvmRewriteContext(Optional.of(IvmRewriteContext.incrementalDryRun(mtmv, dryRunLimit)));
        // Excluded trigger tables must not be validated for binlog / key-type support.
        stmtCtx.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
        return stmtCtx;
    }

    @Override
    public ConnectContext getExplainConnectContext(ConnectContext ctx) throws Exception {
        refreshMTMVInfo.analyze(ctx);
        MTMV mtmv = getMtmv();
        explainPlan = null;
        explainPlanner = Optional.empty();
        ConnectContext explainCtx = createExplainConnectContext(mtmv);
        StatementContext statementContext = new StatementContext(explainCtx, null);
        explainCtx.setStatementContext(statementContext);
        statementContext.setConnectContext(explainCtx);
        return explainCtx;
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) throws Exception {
        initializeExplainPlanIfNeeded(getMtmv(), ctx.getStatementContext(), ctx);
        return explainPlan;
    }

    protected ConnectContext createExplainConnectContext(MTMV mtmv) {
        return MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
    }

    protected LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) throws Exception {
        switch (refreshMTMVInfo.getRefreshMode()) {
            case INCREMENTAL:
                if (!mtmv.isIvm()) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "EXPLAIN REFRESH INCREMENTAL only supports IVM materialized views");
                }
                statementContext.setIvmRewriteContext(Optional.of(
                        IvmRewriteContext.incremental(mtmv, includeExhaustedStreams)));
                // Excluded trigger tables must not be validated for binlog / key-type support.
                statementContext.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
                return createIvmIncrRefreshManager().buildInsertCommand(mtmv);
            case COMPLETE:
                if (mtmv.isIvm()) {
                    statementContext.setIvmRewriteContext(Optional.of(IvmRewriteContext.full(mtmv)));
                }
                statementContext.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
                return UpdateMvByPartitionCommand.from(
                        mtmv, getCompleteRefreshPartitions(mtmv), getIncrementalTableMap(mtmv), statementContext);
            default:
                throw new org.apache.doris.nereids.exceptions.AnalysisException(
                        "EXPLAIN REFRESH currently supports COMPLETE and INCREMENTAL only");
        }
    }

    @Override
    public Optional<NereidsPlanner> getExplainPlanner(
            LogicalPlan logicalPlan, StatementContext ctx) throws Exception {
        initializeExplainPlanIfNeeded(getMtmv(), ctx, ctx.getConnectContext());
        return explainPlanner;
    }

    private void initializeExplainPlanIfNeeded(
            MTMV mtmv, StatementContext statementContext, ConnectContext connectContext)
            throws Exception {
        if (explainPlan != null) {
            return;
        }
        LogicalPlan refreshCommand = createRefreshCommand(mtmv, statementContext);
        if (refreshCommand instanceof Explainable) {
            Explainable explainable = (Explainable) refreshCommand;
            explainPlan = explainable.getExplainPlan(connectContext);
            explainPlanner =
                    explainable.getExplainPlanner((LogicalPlan) explainPlan, statementContext);
            return;
        }
        explainPlan = refreshCommand;
        explainPlanner = Optional.empty();
    }

    IvmIncrRefreshManager createIvmIncrRefreshManager() {
        return new IvmIncrRefreshManager();
    }

    public RefreshMTMVInfo getRefreshMTMVInfo() {
        return refreshMTMVInfo;
    }

    public boolean isIncludeExhaustedStreams() {
        return includeExhaustedStreams;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public Optional<IvmDryRunLimit> getDryRunLimit() {
        return dryRunLimit;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshMTMVCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }

    private MTMV getMtmv() throws AnalysisException, MetaNotFoundException {
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(refreshMTMVInfo.getMvName().getDb());
        return (MTMV) db.getTableOrMetaException(refreshMTMVInfo.getMvName().getTbl(),
                TableType.MATERIALIZED_VIEW);
    }

    private Set<String> getCompleteRefreshPartitions(MTMV mtmv) {
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(mtmv.getPartitionNames());
    }

    private Map<TableIf, String> getIncrementalTableMap(MTMV mtmv)
            throws AnalysisException {
        Map<TableIf, String> tableWithPartKey = Maps.newHashMap();
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return tableWithPartKey;
        }
        for (BaseColInfo pctInfo : mtmv.getMvPartitionInfo().getPctInfos()) {
            tableWithPartKey.put(MTMVUtil.getTable(pctInfo.getTableInfo()), pctInfo.getColName());
        }
        return tableWithPartKey;
    }
}
