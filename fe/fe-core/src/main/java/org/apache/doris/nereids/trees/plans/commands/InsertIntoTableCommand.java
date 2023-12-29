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

import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.planner.DataSink;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.SqlModeHelper;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * insert into select command implementation
 * insert into select command support the grammar: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private LogicalPlan logicalQuery;
    private Optional<String> labelName;
    /**
     * When source it's from job scheduler,it will be set.
     */
    private long jobId;
    private boolean allowAutoPartition;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        // only insert overwrite will disable it.
        this.allowAutoPartition = true;
    }

    public Optional<String> getLabelName() {
        return labelName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public void setAllowAutoPartition(boolean allowAutoPartition) {
        this.allowAutoPartition = allowAutoPartition;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        runInternal(ctx, executor);
    }

    public void runWithUpdateInfo(ConnectContext ctx, StmtExecutor executor,
                                  LoadStatistic loadStatistic) throws Exception {
        // TODO: add coordinator statistic
        runInternal(ctx, executor);
    }

    private void runInternal(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        PhysicalOlapTableSink<?> physicalOlapTableSink;
        DataSink sink;
        InsertExecutor insertExecutor;
        TableIf targetTableIf = InsertExecutor.getTargetTable(logicalQuery, ctx);
        // should lock target table until we begin transaction.
        targetTableIf.readLock();
        try {
            // 1. process inline table (default values, empty values)
            this.logicalQuery = (LogicalPlan) InsertExecutor.normalizePlan(logicalQuery, targetTableIf);

            LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
            NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
            planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
            executor.setPlanner(planner);
            executor.checkBlockRules();
            if (ctx.getMysqlChannel() != null) {
                ctx.getMysqlChannel().reset();
            }

            // TODO: support other type table insert into
            Optional<PhysicalOlapTableSink<?>> plan = (planner.getPhysicalPlan()
                    .<Set<PhysicalOlapTableSink<?>>>collect(PhysicalOlapTableSink.class::isInstance)).stream()
                    .findAny();
            Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
            physicalOlapTableSink = plan.get();

            Table targetTable = physicalOlapTableSink.getTargetTable();
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), targetTable.getQualifiedDbName(), targetTable.getName(),
                            PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        targetTable.getQualifiedDbName() + ": " + targetTable.getName());
            }
            sink = planner.getFragments().get(0).getSink();
            // group commit
            if (analyzeGroupCommit(ctx, sink, physicalOlapTableSink)) {
                // handleGroupCommit(ctx, sink, physicalOlapTableSink);
                // return;
                throw new AnalysisException("group commit is not supported in nereids now");
            }

            String label = this.labelName.orElse(String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));
            insertExecutor = new InsertExecutor(ctx,
                    physicalOlapTableSink.getDatabase(),
                    physicalOlapTableSink.getTargetTable(), label, planner);
            insertExecutor.beginTransaction();
            insertExecutor.finalizeSink(sink, physicalOlapTableSink.isPartialUpdate(),
                    physicalOlapTableSink.getDmlCommandType() == DMLCommandType.INSERT, this.allowAutoPartition);
        } finally {
            targetTableIf.readUnlock();
        }

        executor.setProfileType(ProfileType.LOAD);
        // We exposed @StmtExecutor#cancel as a unified entry point for statement interruption
        // so we need to set this here
        executor.setCoord(insertExecutor.getCoordinator());
        insertExecutor.executeSingleInsertTransaction(executor, jobId);
    }

    private void handleGroupCommit(ConnectContext ctx, DataSink sink,
                PhysicalOlapTableSink<?> physicalOlapTableSink)
                throws UserException, RpcException, TException, ExecutionException, InterruptedException {
        // TODO we should refactor this to remove rely on UnionNode
        List<InternalService.PDataRow> rows = new ArrayList<>();
        List<List<Expr>> materializedConstExprLists = ((UnionNode) sink.getFragment()
                .getPlanRoot()).getMaterializedConstExprLists();
        int filterSize = 0;
        for (Slot slot : physicalOlapTableSink.getOutput()) {
            if (slot.getName().contains(Column.DELETE_SIGN)
                    || slot.getName().contains(Column.VERSION_COL)) {
                filterSize += 1;
            }
        }
        for (List<Expr> list : materializedConstExprLists) {
            rows.add(GroupCommitPlanner.getRowStringValue(list, filterSize));
        }
        GroupCommitPlanner groupCommitPlanner = new GroupCommitPlanner(physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), null, ctx.queryId(),
                ConnectContext.get().getSessionVariable().getGroupCommit());
        PGroupCommitInsertResponse response = groupCommitPlanner.executeGroupCommitInsert(ctx, rows);
        TStatusCode code = TStatusCode.findByValue(response.getStatus().getStatusCode());
        if (code == TStatusCode.DATA_QUALITY_ERROR) {
            LOG.info("group commit insert failed. query id: {}, backend id: {}, status: {}, "
                    + "schema version: {}", ctx.queryId(),
                    groupCommitPlanner.getBackend(), response.getStatus(),
                    physicalOlapTableSink.getTargetTable().getBaseSchemaVersion());
        } else if (code != TStatusCode.OK) {
            String errMsg = "group commit insert failed. backend id: "
                    + groupCommitPlanner.getBackend().getId() + ", status: "
                    + response.getStatus();
            ErrorReport.reportDdlException(errMsg, ErrorCode.ERR_FAILED_WHEN_INSERT);
        }
        TransactionStatus txnStatus = TransactionStatus.PREPARE;
        String sb = "{'label':'" + response.getLabel() + "', 'status':'" + txnStatus.name()
                + "', 'txnId':'" + response.getTxnId() + "'"
                + "', 'optimizer':'" + "nereids" + "'"
                + "}";
        ctx.getState().setOk(response.getLoadedRows(), (int) response.getFilteredRows(), sb);
        ctx.setOrUpdateInsertResult(response.getTxnId(), response.getLabel(),
                physicalOlapTableSink.getDatabase().getFullName(), physicalOlapTableSink.getTargetTable().getName(),
                txnStatus, response.getLoadedRows(), (int) response.getFilteredRows());
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) response.getLoadedRows());
    }

    private boolean analyzeGroupCommit(ConnectContext ctx, DataSink sink,
            PhysicalOlapTableSink<?> physicalOlapTableSink) {
        if (!(sink instanceof OlapTableSink) || !ctx.getSessionVariable().isEnableInsertGroupCommit()
                || ctx.getSessionVariable().isEnableUniqueKeyPartialUpdate()) {
            return false;
        }
        return ConnectContext.get().getSessionVariable().getSqlMode() != SqlModeHelper.MODE_NO_BACKSLASH_ESCAPES
                && physicalOlapTableSink.getTargetTable() instanceof OlapTable && !ConnectContext.get().isTxnModel()
                && sink.getFragment().getPlanRoot() instanceof UnionNode && physicalOlapTableSink.getPartitionIds()
                .isEmpty() && physicalOlapTableSink.getTargetTable().getTableProperty().getUseSchemaLightChange();
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }
        return InsertExecutor.normalizePlan(this.logicalQuery, InsertExecutor.getTargetTable(this.logicalQuery, ctx));
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoTableCommand(this, context);
    }
}
