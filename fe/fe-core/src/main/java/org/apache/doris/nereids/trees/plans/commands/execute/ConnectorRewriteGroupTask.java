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

package org.apache.doris.nereids.trees.plans.commands.execute;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.procedure.ConnectorRewriteGroup;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.analyzer.UnboundConnectorTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.ConnectorRewriteExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.RewriteTableCommand;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independent task executor for one bin-packed group of a distributed {@code rewrite_data_files} — the
 * engine-neutral counterpart of the legacy {@code RewriteGroupTask}. Runs the group's {@code INSERT-SELECT}
 * through the connector SPI: it scopes the source scan to the group's data files (the neutral
 * {@link StatementContext} raw-path stash, read by {@code PluginDrivenScanNode.pinRewriteFileScope}), builds a
 * rewrite-tagged {@link UnboundConnectorTableSink} (which forces GATHER distribution), binds the driver's
 * SHARED {@link ConnectorTransaction} onto this group's sink session (via the stash that
 * {@link ConnectorRewriteExecutor#finalizeSink} reads), and stamps the shared transaction id onto the BE
 * coordinator so every group's commit fragments flow into the one rewrite transaction.
 */
public class ConnectorRewriteGroupTask implements TransientTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(ConnectorRewriteGroupTask.class);

    private final ConnectorRewriteGroup group;
    private final long transactionId;
    private final ConnectorTransaction sharedTransaction;
    private final ExternalTable dorisTable;
    private final ConnectContext connectContext;
    private final RewriteResultCallback resultCallback;
    private final Long taskId;
    private final AtomicBoolean isCanceled;
    private final AtomicBoolean isFinished;

    // for canceling the task
    private StmtExecutor stmtExecutor;

    /**
     * Builds a task for one bin-packed rewrite group, sharing {@code transactionId} /
     * {@code sharedTransaction} with the driver's other groups.
     */
    public ConnectorRewriteGroupTask(ConnectorRewriteGroup group,
            long transactionId,
            ConnectorTransaction sharedTransaction,
            ExternalTable dorisTable,
            ConnectContext connectContext,
            RewriteResultCallback resultCallback) {
        this.group = group;
        this.transactionId = transactionId;
        this.sharedTransaction = sharedTransaction;
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
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
        if (isCanceled.get()) {
            throw new JobException("Rewrite task has been canceled, task id: " + taskId);
        }
        if (isFinished.get()) {
            return;
        }

        try {
            // Step 1: Build a fresh ConnectContext for this group and stash the per-group scan scope + the
            // shared connector transaction (read back during planning by pinRewriteFileScope / finalizeSink).
            ConnectContext taskConnectContext = buildConnectContext();
            StatementContext stmtCtx = taskConnectContext.getStatementContext();
            stmtCtx.setRewriteSourceFilePaths(new ArrayList<>(group.getDataFilePaths()));
            stmtCtx.setRewriteSharedTransaction(sharedTransaction);

            // Step 2: Build the INSERT-SELECT plan (rewrite-tagged connector sink) for this group.
            RewriteTableCommand taskLogicalPlan = buildRewriteLogicalPlan();
            LogicalPlanAdapter taskParsedStmt = new LogicalPlanAdapter(taskLogicalPlan, stmtCtx);
            taskParsedStmt.setOrigStmt(new OriginStatement(taskLogicalPlan.toString(), 0));

            // Step 3: Execute the rewrite write for this group.
            executeGroup(taskConnectContext, taskLogicalPlan, taskParsedStmt);

            if (resultCallback != null) {
                resultCallback.onTaskCompleted(taskId);
            }
        } catch (Exception e) {
            LOG.warn("Failed to execute connector rewrite group: {}", e.getMessage(), e);
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
            return;
        }
        isCanceled.set(true);
        if (stmtExecutor != null) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "rewrite task cancelled"));
        }
        LOG.info("[Connector Rewrite Task] taskId: {} cancelled", taskId);
    }

    private void executeGroup(ConnectContext taskConnectContext,
            RewriteTableCommand taskLogicalPlan,
            StatementBase taskParsedStmt) throws Exception {
        stmtExecutor = new StmtExecutor(taskConnectContext, taskParsedStmt);

        // initPlan finalizes the sink (ConnectorRewriteExecutor.finalizeSink binds the shared transaction
        // onto the sink session BEFORE planWrite reads it).
        AbstractInsertExecutor insertExecutor = taskLogicalPlan.initPlan(taskConnectContext, stmtExecutor);
        Preconditions.checkState(insertExecutor instanceof ConnectorRewriteExecutor,
                "Expected ConnectorRewriteExecutor, got: " + insertExecutor.getClass());

        // Stamp the shared transaction id onto the coordinator so the BE-reported commit fragments
        // accumulate on the one rewrite transaction (mirrors legacy RewriteGroupTask).
        insertExecutor.getCoordinator().setTxnId(transactionId);

        insertExecutor.executeSingleInsert(stmtExecutor);
    }

    private RewriteTableCommand buildRewriteLogicalPlan() {
        List<String> tableNameParts = ImmutableList.of(
                dorisTable.getCatalog().getName(),
                dorisTable.getDbName(),
                dorisTable.getName());

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

        // rewrite=true (last arg) -> PhysicalConnectorTableSink.isRewrite -> GATHER + WriteOperation.REWRITE.
        UnboundConnectorTableSink<?> tableSink = new UnboundConnectorTableSink<>(
                tableNameParts,
                ImmutableList.of(), // colNames (empty means all columns)
                ImmutableList.of(), // hints
                ImmutableList.of(), // partitions
                DMLCommandType.INSERT,
                Optional.empty(), // groupExpression
                Optional.empty(), // logicalProperties
                sourceRelation,
                null, // staticPartitionKeyValues
                true); // rewrite
        return new RewriteTableCommand(
                tableSink,
                Optional.empty(), // labelName
                Optional.empty(), // insertCtx
                Optional.empty(), // cte
                Optional.empty() // branchName
        );
    }

    private ConnectContext buildConnectContext() {
        ConnectContext taskContext = new ConnectContext();
        taskContext.setSessionVariable(VariableMgr.cloneSessionVariable(connectContext.getSessionVariable()));
        taskContext.setEnv(Env.getCurrentEnv());
        taskContext.setDatabase(connectContext.getDatabase());
        taskContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        taskContext.setRemoteIP(connectContext.getRemoteIP());

        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        taskContext.setQueryId(queryId);
        taskContext.setThreadLocalInfo();
        taskContext.setStartTime();

        StatementContext statementContext = new StatementContext();
        statementContext.setConnectContext(taskContext);
        taskContext.setStatementContext(statementContext);
        return taskContext;
    }

    /**
     * Callback interface for task completion.
     */
    public interface RewriteResultCallback {
        void onTaskCompleted(Long taskId);

        void onTaskFailed(Long taskId, Exception error);
    }
}
