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
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.source.IcebergScanNode;
import org.apache.doris.nereids.trees.plans.commands.insert.AbstractInsertExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.IcebergRewriteExecutor;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.Coordinator;
import org.apache.doris.qe.CoordinatorContext;
import org.apache.doris.qe.NereidsCoordinator;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.VariableMgr;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Independent task executor for processing a single rewrite group.
 */
@Slf4j
public class RewriteGroupTask implements TransientTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteGroupTask.class);

    private final RewriteDataGroup group;
    private final long transactionId;
    private final InsertIntoTableCommand preparedPlan;
    private final StatementBase parsedStmt;
    private final ConnectContext connectContext;
    private final RewriteResultCallback resultCallback;
    private final Long taskId;
    private final AtomicBoolean isCanceled;
    private final AtomicBoolean isFinished;

    public RewriteGroupTask(RewriteDataGroup group,
            long transactionId,
            InsertIntoTableCommand preparedPlan,
            StatementBase parsedStmt,
            ConnectContext connectContext,
            RewriteResultCallback resultCallback) {
        this.group = group;
        this.transactionId = transactionId;
        this.preparedPlan = preparedPlan;
        this.parsedStmt = parsedStmt;
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
            // Create a new ConnectContext for this task
            ConnectContext taskConnectContext = buildConnectContext();

            executeGroup(taskConnectContext);

            // Notify result callback
            if (resultCallback != null) {
                resultCallback.onTaskCompleted(taskId);
            }

            LOG.debug("[Rewrite Task] taskId: {} execution completed successfully", taskId);

        } catch (Exception e) {
            LOG.error("Failed to execute rewrite group: {}", e.getMessage(), e);

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
        LOG.info("[Rewrite Task] taskId: {} cancelled", taskId);
    }

    /**
     * Execute rewrite group and return RewriteResult
     */
    private void executeGroup(ConnectContext taskConnectContext) throws Exception {
        // Step 1: Create stmt executor
        StmtExecutor executor = new StmtExecutor(taskConnectContext, parsedStmt);

        // Step 2: Create insert executor, not to begin a transaction
        AbstractInsertExecutor insertExecutor = this.preparedPlan.initPlan(taskConnectContext, executor, false);
        Preconditions.checkState(insertExecutor instanceof IcebergRewriteExecutor,
                "Expected IcebergRewriteExecutor, got: " + insertExecutor.getClass());
        IcebergRewriteExecutor rewriteExecutor = (IcebergRewriteExecutor) insertExecutor;

        // Step 3: Customize insert executor for rewrite
        customizeInsertExecutorForRewrite(rewriteExecutor, group);

        // Step 4: Set transaction id for updating CommitData
        insertExecutor.getCoordinator().setTxnId(transactionId);

        // Step 5: Execute insert operation
        insertExecutor.executeSingleInsert(executor, System.currentTimeMillis());
    }

    /**
     * Build ConnectContext for this task executor
     */
    private ConnectContext buildConnectContext() {
        ConnectContext taskContext = new ConnectContext();
        // clone session variables from parent
        taskContext.setSessionVariable(VariableMgr.cloneSessionVariable(connectContext.getSessionVariable()));
        // set env and basic identities
        taskContext.setEnv(Env.getCurrentEnv());
        taskContext.setDatabase(connectContext.getDatabase());
        taskContext.setCurrentUserIdentity(connectContext.getCurrentUserIdentity());
        taskContext.setRemoteIP(connectContext.getRemoteIP());
        // assign query id and start time
        UUID uuid = UUID.randomUUID();
        TUniqueId queryId = new TUniqueId(uuid.getMostSignificantBits(), uuid.getLeastSignificantBits());
        taskContext.setQueryId(queryId);
        taskContext.setThreadLocalInfo();
        taskContext.setStartTime();
        return taskContext;
    }

    /**
     * Customize insert executor for Iceberg file rewrite
     */
    private void customizeInsertExecutorForRewrite(
            IcebergRewriteExecutor insertExecutor,
            RewriteDataGroup group) throws UserException {

        LOG.debug("Customizing insert executor for rewrite with {} tasks", group.getTaskCount());

        // Get the coordinator from the insert executor
        Coordinator coordinator = insertExecutor.getCoordinator();
        if (coordinator == null) {
            throw new UserException("No coordinator found in insert executor");
        }

        // Access coordinator context to get scan nodes
        Preconditions.checkState(coordinator instanceof NereidsCoordinator,
                "Expected NereidsCoordinator, got: " + coordinator.getClass());
        NereidsCoordinator nereidsCoordinator = (NereidsCoordinator) coordinator;
        CoordinatorContext context = nereidsCoordinator.getCoordinatorContext();
        Preconditions.checkState(context != null && context.scanNodes != null && context.scanNodes.size() == 1,
                "No scan nodes found in coordinator context");

        // Find and customize IcebergScanNode
        ScanNode scanNode = context.scanNodes.get(0);
        Preconditions.checkState(scanNode instanceof IcebergScanNode,
                "Expected IcebergScanNode, got: " + scanNode.getClass());
        ((IcebergScanNode) scanNode).resetFileScanTasks(group.getTasks());
    }

    /**
     * Callback interface for task completion
     */
    public interface RewriteResultCallback {
        void onTaskCompleted(Long taskId);

        void onTaskFailed(Long taskId, Exception error);
    }
}
