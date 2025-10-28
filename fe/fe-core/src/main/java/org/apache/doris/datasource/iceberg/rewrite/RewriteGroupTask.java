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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergRewriteExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.thrift.TStatusCode;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independent task executor for processing a single rewrite group.
 */
public class RewriteGroupTask implements TransientTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteGroupTask.class);

    private final RewriteDataGroup group;
    private final long transactionId;
    private final IcebergExternalTable dorisTable;
    private final ConnectContext connectContext;
    private final long targetFileSizeBytes;
    private final RewriteResultCallback resultCallback;
    private final Long taskId;
    private final AtomicBoolean isCanceled;
    private final AtomicBoolean isFinished;

    // for canceling the task
    private StmtExecutor stmtExecutor;

    public RewriteGroupTask(RewriteDataGroup group,
            long transactionId,
            IcebergExternalTable dorisTable,
            ConnectContext connectContext,
            long targetFileSizeBytes,
            RewriteResultCallback resultCallback) {
        this.group = group;
        this.transactionId = transactionId;
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
        this.targetFileSizeBytes = targetFileSizeBytes;
        this.resultCallback = resultCallback;
        this.taskId = UUID.randomUUID().getMostSignificantBits();
        this.isCanceled = new AtomicBoolean(false);
        this.isFinished = new AtomicBoolean(false);
    }

    @Override
    public Long getId() {
        return taskId;
    }

    @Override
    public void execute() throws JobException {
        LOG.debug("[Rewrite Task] taskId: {} starting execution for group with {} tasks",
                taskId, group.getTaskCount());

        if (isCanceled.get()) {
            LOG.debug("[Rewrite Task] taskId: {} was already canceled before execution", taskId);
            throw new JobException("Rewrite task has been canceled, task id: " + taskId);
        }

        if (isFinished.get()) {
            LOG.debug("[Rewrite Task] taskId: {} was already finished", taskId);
            return;
        }

        try {
            // Step 1: Create and customize a new ConnectContext for this task
            ConnectContext taskConnectContext = buildConnectContext();
            // Set target file size for Iceberg write
            taskConnectContext.getSessionVariable().setIcebergWriteTargetFileSizeBytes(targetFileSizeBytes);
            // Custom file scan tasks for rewrite operations
            taskConnectContext.getStatementContext().setIcebergRewriteFileScanTasks(group.getTasks());

            // Step 2: Build logical plan for this task
            InsertIntoTableCommand taskLogicalPlan = buildRewriteLogicalPlan();
            LogicalPlanAdapter taskParsedStmt = new LogicalPlanAdapter(
                    taskLogicalPlan,
                    taskConnectContext.getStatementContext());
            taskParsedStmt.setOrigStmt(new OriginStatement(taskLogicalPlan.toString(), 0));

            // Step 3: Execute the rewrite operation for this group
            executeGroup(taskConnectContext, taskLogicalPlan, taskParsedStmt);

            // Notify result callback
            if (resultCallback != null) {
                resultCallback.onTaskCompleted(taskId);
            }

            LOG.debug("[Rewrite Task] taskId: {} execution completed successfully", taskId);

        } catch (Exception e) {
            LOG.warn("Failed to execute rewrite group: {}", e.getMessage(), e);

            // Notify error callback
            if (resultCallback != null) {
                resultCallback.onTaskFailed(taskId, e);
            }

            throw new JobException("Rewrite group execution failed: " + e.getMessage(), e);
        } finally {
            isFinished.set(true);
        }
    }

    @Override
    public void cancel() throws JobException {
        if (isFinished.get()) {
            LOG.debug("[Rewrite Task] taskId: {} already finished, cannot cancel", taskId);
            return;
        }

        isCanceled.set(true);
        if (stmtExecutor != null) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "rewrite task cancelled"));
        }
        LOG.info("[Rewrite Task] taskId: {} cancelled", taskId);
    }

    /**
     * Execute rewrite group with task-specific logical plan and parsed statement
     */
    private void executeGroup(ConnectContext taskConnectContext,
            InsertIntoTableCommand taskLogicalPlan,
            StatementBase taskParsedStmt) throws Exception {
        // Step 1: Create stmt executor
        stmtExecutor = new StmtExecutor(taskConnectContext, taskParsedStmt);

        // Step 2: Create insert executor, but not to begin a transaction
        AbstractInsertExecutor insertExecutor = taskLogicalPlan.initPlan(taskConnectContext, stmtExecutor, false);
        Preconditions.checkState(insertExecutor instanceof IcebergRewriteExecutor,
                "Expected IcebergRewriteExecutor, got: " + insertExecutor.getClass());

        // Step 3: Set transaction id for updating CommitData
        insertExecutor.getCoordinator().setTxnId(transactionId);

        // Step 4: Execute insert operation
        insertExecutor.executeSingleInsert(stmtExecutor, System.currentTimeMillis());

        LOG.debug("[Rewrite Task] taskId: {} completed execution successfully", taskId);
    }

    /**
     * Build logical plan for rewrite operation (INSERT INTO ... SELECT ...)
     * Each task creates its own independent InsertIntoTableCommand instance
     */
    private InsertIntoTableCommand buildRewriteLogicalPlan() {
        // Build table name parts
        List<String> tableNameParts = ImmutableList.of(
                dorisTable.getCatalog().getName(),
                dorisTable.getDbName(),
                dorisTable.getName());

        // Create UnboundRelation for SELECT part (source table)
        UnboundRelation sourceRelation = new UnboundRelation(
                StatementScopeIdGenerator.newRelationId(),
                tableNameParts,
                ImmutableList.of(), // partitions
                false, // isTemporary
                ImmutableList.of(), // tabletIds
                ImmutableList.of(), // hints
                Optional.empty(), // orderKeys
                Optional.empty() // limit
        );

        // Create UnboundIcebergTableSink for INSERT part (target table)
        UnboundIcebergTableSink<?> tableSink = new UnboundIcebergTableSink<>(
                tableNameParts,
                ImmutableList.of(), // colNames (empty means all columns)
                ImmutableList.of(), // hints
                ImmutableList.of(), // partitions
                DMLCommandType.INSERT,
                Optional.empty(), // labelName
                Optional.empty(), // branchName
                sourceRelation);

        // Create InsertIntoTableCommand for rewrite operation
        InsertIntoTableCommand insertCommand = new InsertIntoTableCommand(
                tableSink,
                Optional.empty(), // labelName
                Optional.empty(), // insertCtx
                Optional.empty(), // cte
                true, // needNormalizePlan
                Optional.empty() // branchName
        );
        insertCommand.setRewriteOperation(true);
        return insertCommand;
    }

    /**
     * Build ConnectContext for this task
     */
    private ConnectContext buildConnectContext() {
        ConnectContext taskContext = new ConnectContext();

        // Clone session variables from parent
        taskContext.setSessionVariable(VariableMgr.cloneSessionVariable(connectContext.getSessionVariable()));

        // Set env and basic identities
        taskContext.setEnv(Env.getCurrentEnv());
        taskContext.setDatabase(connectContext.getDatabase());
        taskContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        taskContext.setRemoteIP(connectContext.getRemoteIP());

        // Assign unique query id and start time
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        taskContext.setQueryId(queryId);
        taskContext.setThreadLocalInfo();
        taskContext.setStartTime();

        // Initialize StatementContext for this task
        StatementContext statementContext = new StatementContext();
        statementContext.setConnectContext(taskContext);
        taskContext.setStatementContext(statementContext);

        return taskContext;
    }

    /**
     * Callback interface for task completion
     */
    public interface RewriteResultCallback {
        void onTaskCompleted(Long taskId);

        void onTaskFailed(Long taskId, Exception error);
    }
}
