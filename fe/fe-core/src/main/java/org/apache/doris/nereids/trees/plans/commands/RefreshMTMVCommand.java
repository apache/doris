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

import org.apache.doris.analysis.PartitionKeyDesc;
import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.stream.StreamReadMode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.datasource.mvcc.MvccTable;
import org.apache.doris.datasource.mvcc.MvccTableInfo;
import org.apache.doris.mtmv.BaseColInfo;
import org.apache.doris.mtmv.BaseTableInfo;
import org.apache.doris.mtmv.MTMVPartitionInfo.MTMVPartitionType;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.mtmv.MTMVPlanUtil;
import org.apache.doris.mtmv.MTMVRefreshContext;
import org.apache.doris.mtmv.MTMVRelation;
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

import java.util.List;
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
            // and stream each batch to the client through this session's result sender. The
            // internal executor owns a fresh query id that KILL QUERY cannot reach, so forward
            // cancellations of the outer statement (Ctrl+C / KILL / timeout) to it while it runs.
            StmtExecutor internalExecutor = new StmtExecutor(internalCtx, adapter);
            internalCtx.setExecutor(internalExecutor);
            executor.setCancelDelegate(internalExecutor::cancel);
            internalExecutor.executeInternalQueryAndSend(adapter, ctx.getResultSender());
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
        // Excluded trigger tables must not be validated for changelog / key-type support.
        stmtCtx.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
        // The MV query is parsed before the internal executor is created. SET_VAR hints
        // need this context already installed on the internal session during parsing.
        internalCtx.setStatementContext(stmtCtx);
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
        ConnectContext ctx = MTMVPlanUtil.createMTMVContext(mtmv, MTMVPlanUtil.DISABLE_RULES_WHEN_RUN_MTMV_TASK);
        // Execution reads the base tables, never the MV's own rows: MTMVPlanUtil.executeCommand turns the
        // rewrite off the same way before it runs. Left on, the refresh query would be rewritten to the MV
        // it is about to rebuild, and the preview would show a read the refresh never makes.
        ctx.getSessionVariable().setEnableMaterializedViewRewrite(false);
        ctx.getSessionVariable().setEnableDmlMaterializedViewRewrite(false);
        return ctx;
    }

    protected LogicalPlan createRefreshCommand(MTMV mtmv, StatementContext statementContext) throws Exception {
        switch (refreshMTMVInfo.getRefreshMode()) {
            case INCREMENTAL:
                if (!mtmv.isIvm()) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "EXPLAIN REFRESH INCREMENTAL only supports IVM materialized views");
                }
                statementContext.setIvmRewriteContext(Optional.of(
                        IvmRewriteContext.incrementalExplain(mtmv, includeExhaustedStreams)));
                // Excluded trigger tables must not be validated for changelog / key-type support.
                statementContext.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
                return createIvmIncrRefreshManager().buildInsertCommand(mtmv);
            case COMPLETE:
                statementContext.setExcludedTriggerTables(mtmv.getExcludedTriggerTables());
                Set<String> completeRefreshPartitions = getCompleteRefreshPartitions(mtmv);
                Map<TableIf, String> tableWithPartKey = getIncrementalTableMap(mtmv);
                // Pinned before the context is built, the way a refresh task pins its inputs before its
                // own: the mapping, the read predicates and the scan binding then all resolve one
                // snapshot per input, instead of the mapping re-materializing the latest one per MV
                // partition and the scan possibly binding to a different one than the mapping read.
                Map<MvccTableInfo, MvccSnapshot> pinnedSnapshots =
                        pinBaseTableSnapshots(mtmv, statementContext);
                // The refresh aligns before it reads anything -- the partitions the base partitions no
                // longer describe are dropped, the ones they now produce are created, and only then is the
                // set to rebuild taken from the MV -- so the preview takes the same set. A dropped partition
                // left in it would be read through a scope the refresh never reads, and being undescribed
                // there it would also cost the retained partitions their scopes; see
                // MTMVPartitionUtil#mappedBasePartitionsByMvPartition.
                completeRefreshPartitions = partitionsAfterPendingAlignment(mtmv, pinnedSnapshots,
                        completeRefreshPartitions);
                MTMVRefreshContext refreshContext =
                        MTMVRefreshContext.buildContext(mtmv, Maps.newHashMap(), pinnedSnapshots);
                if (mtmv.isIvm()) {
                    // The explained refresh covers every MV partition in one plan, which is the refresh-all
                    // first batch a refresh would run: its PCT inputs are reset to the base partitions the
                    // mapping names -- a table it names as empty is read as no rows -- and a non-PCT input is
                    // read in RESET mode. Both are derived the way MTMVTask derives them, from the same
                    // context, so the preview's reads are the reads execution makes; see IvmFullRefreshMTMV.
                    statementContext.setIvmRewriteContext(Optional.of(IvmRewriteContext.fullExplain(mtmv,
                            MTMVPartitionUtil.collectPctResetPartitionIds(mtmv, refreshContext,
                                    completeRefreshPartitions),
                            StreamReadMode.RESET)));
                }
                // Explained through the base partitions the refresh reads for these MV partitions, from the
                // same mapping the refresh records them with and paired per MV partition. The MV partitions'
                // own key ranges are wider than that read: with a partition_sync_limit window, a base
                // partition it leaves out can lie inside a retained MV partition, and an explain written
                // from the ranges alone would show the expired partition as read -- a read the refresh does
                // not make. The pairing is what keeps one MV partition's recorded base partitions from being
                // read through another's range, which the refresh does not do either. An IVM MV passes no
                // predicate scope: its rewrite scopes the read through the reset partition ids instead.
                return UpdateMvByPartitionCommand.from(mtmv, completeRefreshPartitions, tableWithPartKey,
                        statementContext, mtmv.isIvm() ? null
                                : MTMVPartitionUtil.mappedBasePartitionsByMvPartition(tableWithPartKey,
                                        refreshContext, completeRefreshPartitions));
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
        // createRefreshCommand installs an EXPLAIN-kind rewrite context for both the
        // INCREMENTAL and COMPLETE branches, so guards that protect real execution (and
        // dry-run data reads) skip plan-only generation.
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

    /**
     * One engine snapshot per external input of this MV, also installed on {@code statementContext} so
     * that predicate construction and scan binding reuse the pins the mapping is built from, as
     * {@code MTMVTask#beforeMTMVRefresh} does for a refresh.
     */
    @VisibleForTesting
    Map<MvccTableInfo, MvccSnapshot> pinBaseTableSnapshots(MTMV mtmv, StatementContext statementContext)
            throws AnalysisException {
        Map<MvccTableInfo, MvccSnapshot> snapshots = Maps.newHashMap();
        MTMVRelation relation = mtmv.getRelation();
        if (relation == null) {
            // A legacy MV has no relation until its first refresh regenerates one.
            return snapshots;
        }
        for (BaseTableInfo tableInfo : relation.getBaseTablesOneLevelAndFromView()) {
            TableIf table = MTMVUtil.getTable(tableInfo);
            if (!(table instanceof MvccTable)) {
                continue;
            }
            MvccTableInfo mvccTableInfo = new MvccTableInfo(table);
            MvccSnapshot snapshot = ((MvccTable) table).loadSnapshot(Optional.empty(), Optional.empty());
            snapshots.put(mvccTableInfo, snapshot);
            statementContext.setSnapshot(mvccTableInfo, snapshot);
        }
        return snapshots;
    }

    private Set<String> getCompleteRefreshPartitions(MTMV mtmv) {
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return Sets.newHashSet();
        }
        return Sets.newHashSet(mtmv.getPartitionNames());
    }

    /**
     * The partitions a COMPLETE refresh will rebuild, once the alignment it runs before reading anything has
     * dropped the partitions the base partitions no longer describe.
     *
     * <p>Execution applies that alignment and then takes the set it rebuilds from the MV
     * (MTMVTask#executeCompleteAttempt), so a preview that kept a dropped partition would show a read the
     * refresh does not make: that partition's own key range, and -- while the partition is undescribed -- the
     * whole batch unscoped, taking every other partition's scope with it (see
     * MTMVPartitionUtil#mappedBasePartitionsByMvPartition). The alignment computed here is the one the next
     * refresh would run, against the snapshots the preview is pinned to, and nothing is applied.
     *
     * <p>Only names that exist stay in the preview. A partition the alignment has yet to create has no name
     * the sink can target and no rows until the refresh creates it, so it stays out; one the alignment drops
     * and recreates under its own name stays in, because that name is what the refresh rebuilds and leaving
     * it out would hide a read the refresh makes.
     */
    private static Set<String> partitionsAfterPendingAlignment(MTMV mtmv,
            Map<MvccTableInfo, MvccSnapshot> pinnedSnapshots, Set<String> partitionNames)
            throws AnalysisException {
        if (mtmv.getMvPartitionInfo().getPartitionType() == MTMVPartitionType.SELF_MANAGE) {
            return partitionNames;
        }
        Pair<List<String>, List<PartitionKeyDesc>> alignment =
                MTMVPartitionUtil.alignMvPartition(mtmv, pinnedSnapshots);
        Set<String> retained = Sets.newHashSet(partitionNames);
        retained.removeAll(alignment.first);
        for (PartitionKeyDesc partitionKeyDesc : alignment.second) {
            String recreatedName = MTMVPartitionUtil.generatePartitionName(partitionKeyDesc);
            if (partitionNames.contains(recreatedName)) {
                retained.add(recreatedName);
            }
        }
        // An alignment that replaces every partition leaves no name the preview can target, and one that
        // targets nothing reads every table whole; the stale names keep the preview about the partitions the
        // MV actually has.
        return retained.isEmpty() ? partitionNames : retained;
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
