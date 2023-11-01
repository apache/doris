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

import org.apache.doris.analysis.AddPartitionLikeClause;
import org.apache.doris.analysis.AlterClause;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.DropPartitionClause;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.PartitionNames;
import org.apache.doris.analysis.ReplacePartitionClause;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ProfileManager.ProfileType;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.analyzer.UnboundOlapTableSink;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.nereids.txn.Transaction;
import org.apache.doris.planner.GroupCommitPlanner;
import org.apache.doris.planner.OlapTableSink;
import org.apache.doris.planner.UnionNode;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PGroupCommitInsertResponse;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.DdlExecutor;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.rpc.RpcException;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionStatus;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertIntoTableCommand extends Command implements ForwardWithSync, Explainable {

    public static final Logger LOG = LogManager.getLogger(InsertIntoTableCommand.class);

    private final LogicalPlan logicalQuery;
    private final Optional<String> labelName;
    private final boolean isOverwrite;
    private NereidsPlanner planner;
    private boolean isTxnBegin = false;

    /**
     * constructor
     */
    public InsertIntoTableCommand(LogicalPlan logicalQuery, Optional<String> labelName, boolean isOverwrite) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.logicalQuery = Objects.requireNonNull(logicalQuery,
                "logicalQuery cannot be null in InsertIntoTableCommand");
        this.labelName = labelName;
        this.isOverwrite = isOverwrite;
    }

    public NereidsPlanner getPlanner() {
        return planner;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!ctx.getSessionVariable().isEnableNereidsDML()) {
            try {
                ctx.getSessionVariable().enableFallbackToOriginalPlannerOnce();
            } catch (Exception e) {
                throw new AnalysisException("failed to set fallback to original planner to true", e);
            }
            throw new AnalysisException("Nereids DML is disabled, will try to fall back to the original planner");
        }

        if (ctx.isTxnModel()) {
            // in original planner, if is in txn model, insert into select command and tableRef >= 1 will be refused.
            // we can just run select a one-row-relation like select 1, 2, 3
            // in StmtExecutor#executeForTxn, select 1, 2, 3 means valueList is null, so the if-clause from line 1556
            // to 1580 will be skipped and effect rows will be always 0
            // in nereids, we just forbid it.
            throw new AnalysisException("insert into table command is not supported in txn model");
        }

        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.checkBlockRules();
        if (ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }
        String label = this.labelName.orElse(String.format("label_%x_%x", ctx.queryId().hi, ctx.queryId().lo));

        Optional<TreeNode<?>> plan = (planner.getPhysicalPlan()
                .<Set<TreeNode<?>>>collect(node -> node instanceof PhysicalOlapTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalOlapTableSink<?> physicalOlapTableSink = ((PhysicalOlapTableSink<?>) plan.get());

        if (isOverwrite) {
            dealOverwrite(ctx, executor, physicalOlapTableSink);
            return;
        }

        OlapTableSink sink = ((OlapTableSink) planner.getFragments().get(0).getSink());
        if (ctx.getSessionVariable().enableInsertGroupCommit) {
            // group commit
            if (!analyzeGroupCommit(sink, physicalOlapTableSink)) {
                throw new AnalysisException("Nereids does not support group_commit "
                        + "for this SQL statement, query id: " + ctx.queryId());
            }
            handleGroupCommit(ctx, sink, physicalOlapTableSink);
            return;
        }
        Preconditions.checkArgument(!isTxnBegin, "an insert command cannot create more than one txn");
        Transaction txn = new Transaction(ctx,
                physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), label, planner);
        isTxnBegin = true;
        boolean isStrictMode = (ctx.getSessionVariable().getEnableInsertStrict()
                && physicalOlapTableSink.isPartialUpdate()
                && physicalOlapTableSink.isFromNativeInsertStmt());
        sink.init(ctx.queryId(), txn.getTxnId(),
                physicalOlapTableSink.getDatabase().getId(),
                ctx.getExecTimeout(),
                ctx.getSessionVariable().getSendBatchParallelism(),
                false,
                isStrictMode);

        sink.complete(new Analyzer(Env.getCurrentEnv(), ctx));
        TransactionState state = Env.getCurrentGlobalTransactionMgr().getTransactionState(
                physicalOlapTableSink.getDatabase().getId(),
                txn.getTxnId());
        if (state == null) {
            throw new DdlException("txn does not exist: " + txn.getTxnId());
        }
        state.addTableIndexes(physicalOlapTableSink.getTargetTable());

        executor.setProfileType(ProfileType.LOAD);

        LOG.info("Nereids start to execute the insert command, query id: {}, txn id: {}",
                ctx.queryId(), txn.getTxnId());

        txn.executeInsertIntoTableCommand(executor);
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            try {
                String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
                Env.getCurrentGlobalTransactionMgr().abortTransaction(
                        physicalOlapTableSink.getDatabase().getId(), txn.getTxnId(),
                        (errMsg == null ? "unknown reason" : errMsg));
            } catch (Exception abortTxnException) {
                LOG.warn("errors when abort txn. {}", ctx.getQueryIdentifier(), abortTxnException);
            }
        }
    }

    /**
     * when `isOverwrite` is true, use this logic
     *
     * @param ctx ctx
     * @param executor executor
     * @param physicalOlapTableSink physicalOlapTableSink
     *
     * @throws Exception Exception
     */
    public void dealOverwrite(ConnectContext ctx, StmtExecutor executor,
            PhysicalOlapTableSink<?> physicalOlapTableSink) throws Exception {
        OlapTable targetTable = physicalOlapTableSink.getTargetTable();
        TableName tableName = new TableName(InternalCatalog.INTERNAL_CATALOG_NAME, targetTable.getQualifiedDbName(),
                targetTable.getName());
        List<String> partitionNames = ((UnboundOlapTableSink<?>) logicalQuery).getPartitions();
        if (CollectionUtils.isEmpty(partitionNames)) {
            partitionNames = Lists.newArrayList(targetTable.getPartitionNames());
        }
        List<String> tempPartitionNames = addTempPartition(ctx, tableName, partitionNames);
        boolean insertRes = insertInto(ctx, executor, tempPartitionNames, tableName);
        if (!insertRes) {
            return;
        }
        replacePartition(ctx, tableName, partitionNames, tempPartitionNames);
    }

    /**
     * replacing partitionNames with tempPartitionNames
     *
     * @param ctx ctx
     * @param tableName tableName
     * @param partitionNames partitionNames
     * @param tempPartitionNames tempPartitionNames
     * @throws UserException UserException
     */
    private void replacePartition(ConnectContext ctx, TableName tableName, List<String> partitionNames,
            List<String> tempPartitionNames)
            throws UserException {
        // overwrite old partition with tmp partition
        try {
            List<AlterClause> ops = new ArrayList<>();
            Map<String, String> properties = new HashMap<>();
            properties.put("use_temp_partition_name", "false");
            ops.add(new ReplacePartitionClause(new PartitionNames(false, partitionNames),
                    new PartitionNames(true, tempPartitionNames), properties));
            AlterTableStmt alterTableStmt = new AlterTableStmt(tableName, ops);
            Env.getCurrentEnv().alterTable(alterTableStmt);
        } catch (Exception e) {
            LOG.warn("IOT overwrite table partitions error", e);
            handleIotPartitionRollback(ctx, tableName, tempPartitionNames);
            throw e;
        }
    }

    /**
     * insert into select
     *
     * @param ctx ctx
     * @param executor executor
     * @param tempPartitionNames tempPartitionNames
     * @param tableName tableName
     */
    private boolean insertInto(ConnectContext ctx, StmtExecutor executor, List<String> tempPartitionNames,
            TableName tableName) {
        try {
            UnboundOlapTableSink<?> sink = (UnboundOlapTableSink<?>) logicalQuery;
            UnboundOlapTableSink<?> copySink = new UnboundOlapTableSink<>(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    tempPartitionNames,
                    sink.isPartialUpdate(),
                    sink.isFromNativeInsertStmt(),
                    (LogicalPlan) (sink.child(0)));
            new InsertIntoTableCommand(copySink, labelName, false).run(ctx, executor);
            if (ctx.getState().getStateType() == MysqlStateType.ERR) {
                String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
                LOG.warn("InsertInto state error:{}", errMsg);
                handleIotPartitionRollback(ctx, tableName, tempPartitionNames);
                return false;
            }
            return true;
        } catch (Exception e) {
            LOG.warn("InsertInto error", e);
            handleIotPartitionRollback(ctx, tableName, tempPartitionNames);
            return false;
        }
    }

    /**
     * add some tempPartitions
     *
     * @param ctx ctx
     * @param tableName tableName
     * @param partitionNames partitionNames
     * @return tempPartitionNames
     * @throws Exception Exception
     */
    private List<String> addTempPartition(ConnectContext ctx, TableName tableName, List<String> partitionNames)
            throws Exception {
        List<String> tempPartitionNames = new ArrayList<>();
        try {
            // create tmp partitions with uuid
            for (String partitionName : partitionNames) {
                UUID uuid = UUID.randomUUID();
                // to comply with naming rules
                String tempPartName = "tmp_partition_" + uuid.toString().replace('-', '_');
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new AddPartitionLikeClause(tempPartName, partitionName, true));

                AlterTableStmt alterTableStmt = new AlterTableStmt(tableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), ctx);
                alterTableStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(ctx.getEnv(), alterTableStmt);
                // only when execution succeeded, put the temp partition name into list
                tempPartitionNames.add(tempPartName);
            }
            return tempPartitionNames;
        } catch (Exception e) {
            LOG.warn("IOT create tmp table partitions error", e);
            handleIotPartitionRollback(ctx, tableName, tempPartitionNames);
            throw e;
        }
    }

    /**
     * delete temp partitions
     *
     * @param ctx ctx
     * @param targetTableName targetTableName
     * @param tempPartitionNames tempPartitionNames
     */
    private void handleIotPartitionRollback(ConnectContext ctx, TableName targetTableName,
            List<String> tempPartitionNames) {
        try {
            for (String partitionName : tempPartitionNames) {
                List<AlterClause> ops = new ArrayList<>();
                ops.add(new DropPartitionClause(true, partitionName, true, true));
                AlterTableStmt dropTablePartitionStmt = new AlterTableStmt(targetTableName, ops);
                Analyzer tempAnalyzer = new Analyzer(Env.getCurrentEnv(), ctx);
                dropTablePartitionStmt.analyze(tempAnalyzer);
                DdlExecutor.execute(ctx.getEnv(), dropTablePartitionStmt);
            }
        } catch (Exception ex) {
            LOG.warn("IOT drop partitions error", ex);
            ctx.getState().setError(ErrorCode.ERR_UNKNOWN_ERROR, "Unexpected exception: " + ex.getMessage());
        }
    }

    private void handleGroupCommit(ConnectContext ctx, OlapTableSink sink,
                PhysicalOlapTableSink<?> physicalOlapTableSink)
                throws UserException, RpcException, TException, ExecutionException, InterruptedException {

        List<InternalService.PDataRow> rows = new ArrayList<>();
        List<List<Expr>> materializedConstExprLists = ((UnionNode) sink.getFragment().getPlanRoot())
                .getMaterializedConstExprLists();
        for (List<Expr> list : materializedConstExprLists) {
            rows.add(GroupCommitPlanner.getRowValue(list));
        }

        GroupCommitPlanner groupCommitPlanner = new GroupCommitPlanner(physicalOlapTableSink.getDatabase(),
                physicalOlapTableSink.getTargetTable(), ctx.queryId());
        Future<PGroupCommitInsertResponse> future = groupCommitPlanner.prepareGroupCommitInsertRequest(ctx, rows);
        PGroupCommitInsertResponse response = future.get();
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
        StringBuilder sb = new StringBuilder();
        sb.append("{'label':'").append(response.getLabel()).append("', 'status':'").append(txnStatus.name());
        sb.append("', 'txnId':'").append(response.getTxnId()).append("'");
        sb.append("', 'optimizer':'").append("nereids").append("'");
        sb.append("}");

        ctx.getState().setOk(response.getLoadedRows(), (int) response.getFilteredRows(), sb.toString());
        ctx.setOrUpdateInsertResult(response.getTxnId(), response.getLabel(),
                physicalOlapTableSink.getDatabase().getFullName(), physicalOlapTableSink.getTargetTable().getName(),
                txnStatus, response.getLoadedRows(), (int) response.getFilteredRows());
        // update it, so that user can get loaded rows in fe.audit.log
        ctx.updateReturnRows((int) response.getLoadedRows());
    }

    private boolean analyzeGroupCommit(OlapTableSink sink, PhysicalOlapTableSink<?> physicalOlapTableSink) {
        return ConnectContext.get().getSessionVariable().enableInsertGroupCommit
            && physicalOlapTableSink.getTargetTable() instanceof OlapTable
            && !ConnectContext.get().isTxnModel()
            && sink.getFragment().getPlanRoot() instanceof UnionNode
            && physicalOlapTableSink.getPartitionIds().isEmpty();
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        return this.logicalQuery;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertIntoTableCommand(this, context);
    }
}
