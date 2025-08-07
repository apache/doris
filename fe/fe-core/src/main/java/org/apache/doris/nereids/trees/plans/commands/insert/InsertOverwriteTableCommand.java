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

package org.apache.doris.nereids.trees.plans.commands.insert;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.InternalDatabaseUtil;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.insertoverwrite.InsertOverwriteManager;
import org.apache.doris.insertoverwrite.InsertOverwriteUtil;
import org.apache.doris.mtmv.MTMVUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.CascadesContext;
import org.apache.doris.nereids.NereidsPlanner;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundHiveTableSink;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSink;
import org.apache.doris.nereids.analyzer.UnboundTableSinkCreator;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.properties.PhysicalProperties;
import org.apache.doris.nereids.trees.TreeNode;
import org.apache.doris.nereids.trees.plans.Explainable;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.algebra.TVFRelation;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.NeedAuditEncryption;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.UnboundLogicalSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalIcebergTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalOlapTableSink;
import org.apache.doris.nereids.trees.plans.physical.PhysicalTableSink;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ConnectContext.ConnectType;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TPartialUpdateNewRowPolicy;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.awaitility.Awaitility;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * insert into select command implementation
 * insert into select command support the grammer: explain? insert into table columns? partitions? hints? query
 * InsertIntoTableCommand is a command to represent insert the answer of a query into a table.
 * class structure's:
 * InsertIntoTableCommand(Query())
 * ExplainCommand(Query())
 */
public class InsertOverwriteTableCommand extends Command implements NeedAuditEncryption, ForwardWithSync, Explainable {

    private static final Logger LOG = LogManager.getLogger(InsertOverwriteTableCommand.class);

    private LogicalPlan originLogicalQuery;
    private Optional<LogicalPlan> logicalQuery;
    private Optional<String> labelName;
    private final Optional<LogicalPlan> cte;
    private AtomicBoolean isCancelled = new AtomicBoolean(false);
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private Optional<String> branchName;

    /**
     * constructor
     */
    public InsertOverwriteTableCommand(LogicalPlan logicalQuery, Optional<String> labelName,
            Optional<LogicalPlan> cte, Optional<String> branchName) {
        super(PlanType.INSERT_INTO_TABLE_COMMAND);
        this.originLogicalQuery = Objects.requireNonNull(logicalQuery, "logicalQuery should not be null");
        this.logicalQuery = Optional.empty();
        this.labelName = Objects.requireNonNull(labelName, "labelName should not be null");
        this.cte = cte;
        this.branchName = branchName;
    }

    public void setLabelName(Optional<String> labelName) {
        this.labelName = labelName;
    }

    public boolean isAutoDetectOverwrite(LogicalPlan logicalQuery) {
        return (logicalQuery instanceof UnboundTableSink)
                && ((UnboundTableSink<?>) logicalQuery).isAutoDetectPartition();
    }

    public LogicalPlan getLogicalQuery() {
        return logicalQuery.orElse(originLogicalQuery);
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        TableIf targetTableIf = InsertUtils.getTargetTable(originLogicalQuery, ctx);
        //check allow insert overwrite
        if (!allowInsertOverwrite(targetTableIf)) {
            String errMsg = "insert into overwrite only support OLAP and HMS/ICEBERG table."
                    + " But current table type is " + targetTableIf.getType();
            LOG.error(errMsg);
            throw new AnalysisException(errMsg);
        }
        //check allow modify MTMVData
        if (targetTableIf instanceof MTMV && !MTMVUtil.allowModifyMTMVData(ctx)) {
            throw new AnalysisException("Not allowed to perform current operation on async materialized view");
        }
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
        this.logicalQuery = Optional.of((LogicalPlan) InsertUtils.normalizePlan(
            originLogicalQuery, targetTableIf, analyzeContext, Optional.empty()));
        if (cte.isPresent()) {
            LogicalPlan logicalQuery = this.logicalQuery.get();
            this.logicalQuery = Optional.of(
                    (LogicalPlan) logicalQuery.withChildren(
                            cte.get().withChildren(logicalQuery.child(0))
                    )
            );
        }
        LogicalPlan logicalQuery = this.logicalQuery.get();
        LogicalPlanAdapter logicalPlanAdapter = new LogicalPlanAdapter(logicalQuery, ctx.getStatementContext());
        NereidsPlanner planner = new NereidsPlanner(ctx.getStatementContext());
        planner.plan(logicalPlanAdapter, ctx.getSessionVariable().toThrift());
        executor.checkBlockRules();
        if (ctx.getConnectType() == ConnectType.MYSQL && ctx.getMysqlChannel() != null) {
            ctx.getMysqlChannel().reset();
        }

        Optional<TreeNode<?>> plan = (planner.getPhysicalPlan()
                .<TreeNode<?>>collect(node -> node instanceof PhysicalTableSink)).stream().findAny();
        Preconditions.checkArgument(plan.isPresent(), "insert into command must contain OlapTableSinkNode");
        PhysicalTableSink<?> physicalTableSink = ((PhysicalTableSink<?>) plan.get());
        TableIf targetTable = physicalTableSink.getTargetTable();
        List<String> partitionNames;
        boolean wholeTable = false;
        if (physicalTableSink instanceof PhysicalOlapTableSink) {
            InternalDatabaseUtil
                    .checkDatabase(((OlapTable) targetTable).getQualifiedDbName(), ConnectContext.get());
            // check auth
            if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ConnectContext.get(), targetTable.getDatabase().getCatalog().getName(),
                            ((OlapTable) targetTable).getQualifiedDbName(),
                            targetTable.getName(), PrivPredicate.LOAD)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                        ((OlapTable) targetTable).getQualifiedDbName() + ": " + targetTable.getName());
            }
            ConnectContext.get().setSkipAuth(true);
            partitionNames = ((UnboundTableSink<?>) logicalQuery).getPartitions();
            // If not specific partition to overwrite, means it's a command to overwrite the table.
            // not we execute as overwrite every partitions.
            if (CollectionUtils.isEmpty(partitionNames)) {
                wholeTable = true;
                try { // avoid concurrent modification exception when get partition names
                    targetTable.readLock();
                    partitionNames = Lists.newArrayList(targetTable.getPartitionNames());
                } finally {
                    targetTable.readUnlock();
                }
            }
        } else {
            // Do not create temp partition on FE
            partitionNames = new ArrayList<>();
        }

        // check branch
        if (branchName.isPresent() && !(physicalTableSink instanceof PhysicalIcebergTableSink)) {
            throw new AnalysisException(
                    "Only support insert overwrite into iceberg table's branch");
        }

        InsertOverwriteManager insertOverwriteManager = Env.getCurrentEnv().getInsertOverwriteManager();
        insertOverwriteManager.recordRunningTableOrException(targetTable.getDatabase(), targetTable);
        isRunning.set(true);
        long taskId = 0;
        try {
            if (isAutoDetectOverwrite(getLogicalQuery())) {
                // taskId here is a group id. it contains all replace tasks made and registered in rpc process.
                taskId = insertOverwriteManager.registerTaskGroup();
                // When inserting, BE will call to replace partition by FrontendService. FE will register new temp
                // partitions and return. for transactional, the replacement will really occur when insert successed,
                // i.e. `insertInto` finished. then we call taskGroupSuccess to make replacement.
                insertIntoAutoDetect(ctx, executor, taskId);
                insertOverwriteManager.taskGroupSuccess(taskId, (OlapTable) targetTable);
            } else {
                // it's overwrite table(as all partitions) or specific partition(s)
                List<String> tempPartitionNames = InsertOverwriteUtil.generateTempPartitionNames(partitionNames);
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before registerTask, queryId: {}",
                            ctx.getQueryIdentifier());
                    return;
                }
                taskId = insertOverwriteManager
                        .registerTask(targetTable.getDatabase().getId(), targetTable.getId(), tempPartitionNames);
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before addTempPartitions, queryId: {}",
                            ctx.getQueryIdentifier());
                    // not need deal temp partition
                    insertOverwriteManager.taskSuccess(taskId);
                    return;
                }
                InsertOverwriteUtil.addTempPartitions(targetTable, partitionNames, tempPartitionNames);
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before insertInto, queryId: {}", ctx.getQueryIdentifier());
                    insertOverwriteManager.taskFail(taskId);
                    return;
                }
                insertIntoPartitions(ctx, executor, tempPartitionNames, wholeTable);
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before replacePartition, queryId: {}",
                            ctx.getQueryIdentifier());
                    insertOverwriteManager.taskFail(taskId);
                    return;
                }
                InsertOverwriteUtil.replacePartition(targetTable, partitionNames, tempPartitionNames,
                        isForceDropPartition());
                if (isCancelled.get()) {
                    LOG.info("insert overwrite is cancelled before taskSuccess, do nothing, queryId: {}",
                            ctx.getQueryIdentifier());
                }
                insertOverwriteManager.taskSuccess(taskId);
            }
        } catch (Exception e) {
            LOG.warn("insert into overwrite failed with task(or group) id " + taskId);
            if (isAutoDetectOverwrite(getLogicalQuery())) {
                insertOverwriteManager.taskGroupFail(taskId);
            } else {
                insertOverwriteManager.taskFail(taskId);
            }
            throw e;
        } finally {
            ConnectContext.get().setSkipAuth(false);
            insertOverwriteManager
                    .dropRunningRecord(targetTable.getDatabase().getId(), targetTable.getId());
            isRunning.set(false);
        }
    }

    /**
     * cancel insert overwrite
     */
    public void cancel() {
        this.isCancelled.set(true);
    }

    /**
     * wait insert overwrite not running
     */
    public void waitNotRunning() {
        long waitMaxTimeSecond = 10L;
        try {
            Awaitility.await().atMost(waitMaxTimeSecond, TimeUnit.SECONDS).untilFalse(isRunning);
        } catch (Exception e) {
            LOG.warn("waiting time exceeds {} second, stop wait, labelName: {}", waitMaxTimeSecond,
                    labelName.isPresent() ? labelName.get() : "", e);
        }
    }

    private boolean allowInsertOverwrite(TableIf targetTable) {
        if (targetTable instanceof OlapTable) {
            return true;
        } else {
            return targetTable instanceof HMSExternalTable || targetTable instanceof IcebergExternalTable;
        }
    }

    private void runInsertCommand(LogicalPlan logicalQuery, InsertCommandContext insertCtx,
            ConnectContext ctx, StmtExecutor executor) throws Exception {
        InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(logicalQuery, labelName,
                Optional.of(insertCtx), Optional.empty(), false, Optional.empty());
        insertCommand.run(ctx, executor);
        if (ctx.getState().getStateType() == MysqlStateType.ERR) {
            String errMsg = Strings.emptyToNull(ctx.getState().getErrorMessage());
            LOG.warn("InsertInto state error:{}", errMsg);
            throw new UserException(errMsg);
        }
    }

    /**
     * insert into select. for sepecified temp partitions or all partitions(table).
     *
     * @param ctx                ctx
     * @param executor           executor
     * @param tempPartitionNames tempPartitionNames
     * @param wholeTable         overwrite target is the whole table. not one by one by partitions(...)
     */
    private void insertIntoPartitions(ConnectContext ctx, StmtExecutor executor, List<String> tempPartitionNames,
            boolean wholeTable)
            throws Exception {
        // copy sink tot replace by tempPartitions
        UnboundLogicalSink<?> copySink;
        InsertCommandContext insertCtx;
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            UnboundTableSink<?> sink = (UnboundTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    true,
                    tempPartitionNames,
                    sink.isPartialUpdate(),
                    sink.getPartialUpdateNewRowPolicy(),
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            // 1. when overwrite table, allow auto partition or not is controlled by session variable.
            // 2. we save and pass overwrite auto detect by insertCtx
            boolean allowAutoPartition = wholeTable && ctx.getSessionVariable().isEnableAutoCreateWhenOverwrite();
            insertCtx = new OlapInsertCommandContext(allowAutoPartition, true);
        } else if (logicalQuery instanceof UnboundHiveTableSink) {
            UnboundHiveTableSink<?> sink = (UnboundHiveTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    false,
                    sink.getPartitions(),
                    false,
                    TPartialUpdateNewRowPolicy.APPEND,
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            insertCtx = new HiveInsertCommandContext();
            ((HiveInsertCommandContext) insertCtx).setOverwrite(true);
        } else if (logicalQuery instanceof UnboundIcebergTableSink) {
            UnboundIcebergTableSink<?> sink = (UnboundIcebergTableSink<?>) logicalQuery;
            copySink = (UnboundLogicalSink<?>) UnboundTableSinkCreator.createUnboundTableSink(
                    sink.getNameParts(),
                    sink.getColNames(),
                    sink.getHints(),
                    false,
                    sink.getPartitions(),
                    false,
                    TPartialUpdateNewRowPolicy.APPEND,
                    sink.getDMLCommandType(),
                    (LogicalPlan) (sink.child(0)));
            insertCtx = new IcebergInsertCommandContext();
            ((IcebergInsertCommandContext) insertCtx).setOverwrite(true);
            branchName.ifPresent(notUsed -> ((IcebergInsertCommandContext) insertCtx).setBranchName(branchName));
        } else {
            throw new UserException("Current catalog does not support insert overwrite yet.");
        }
        runInsertCommand(copySink, insertCtx, ctx, executor);
    }

    /**
     * insert into auto detect partition.
     *
     * @param ctx ctx
     * @param executor executor
     */
    private void insertIntoAutoDetect(ConnectContext ctx, StmtExecutor executor, long groupId) throws Exception {
        InsertCommandContext insertCtx;
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            // 1. when overwrite auto-detect, allow auto partition or not is controlled by session variable.
            // 2. we save and pass overwrite auto detect by insertCtx
            boolean allowAutoPartition = ctx.getSessionVariable().isEnableAutoCreateWhenOverwrite();
            insertCtx = new OlapInsertCommandContext(allowAutoPartition,
                    ((UnboundTableSink<?>) logicalQuery).isAutoDetectPartition(), groupId, true);
        } else if (logicalQuery instanceof UnboundHiveTableSink) {
            insertCtx = new HiveInsertCommandContext();
            ((HiveInsertCommandContext) insertCtx).setOverwrite(true);
        } else if (logicalQuery instanceof UnboundIcebergTableSink) {
            insertCtx = new IcebergInsertCommandContext();
            ((IcebergInsertCommandContext) insertCtx).setOverwrite(true);
            branchName.ifPresent(notUsed -> ((IcebergInsertCommandContext) insertCtx).setBranchName(branchName));
        } else {
            throw new UserException("Current catalog does not support insert overwrite yet.");
        }
        runInsertCommand(logicalQuery, insertCtx, ctx, executor);
    }

    @Override
    public Plan getExplainPlan(ConnectContext ctx) {
        Optional<CascadesContext> analyzeContext = Optional.of(
                CascadesContext.initContext(ctx.getStatementContext(), originLogicalQuery, PhysicalProperties.ANY)
        );
        return InsertUtils.getPlanForExplain(ctx, analyzeContext, getLogicalQuery());
    }

    @Override
    public Optional<NereidsPlanner> getExplainPlanner(LogicalPlan logicalPlan, StatementContext ctx) {
        LogicalPlan logicalQuery = getLogicalQuery();
        if (logicalQuery instanceof UnboundTableSink) {
            boolean allowAutoPartition = ctx.getConnectContext().getSessionVariable().isEnableAutoCreateWhenOverwrite();
            OlapInsertCommandContext insertCtx = new OlapInsertCommandContext(allowAutoPartition, true);
            InsertIntoTableCommand insertIntoTableCommand = new InsertIntoTableCommand(
                    logicalQuery, labelName, Optional.of(insertCtx), Optional.empty(), true, Optional.empty());
            return insertIntoTableCommand.getExplainPlanner(logicalPlan, ctx);
        }
        return Optional.empty();
    }

    public boolean isForceDropPartition() {
        return false;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitInsertOverwriteTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.INSERT;
    }

    @Override
    public boolean needAuditEncryption() {
        return originLogicalQuery.anyMatch(node -> node instanceof TVFRelation);
    }
}
