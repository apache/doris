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

import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task executor for processing rewrite groups concurrently.
 * Similar to ExportTaskExecutor, this class executes multiple rewrite groups
 * in a single task executor.
 */
@Slf4j
public class RewriteTaskExecutor implements TransientTaskExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteTaskExecutor.class);

    private final List<RewriteDataGroup> groups;
    private final RewriteJob rewriteJob;
    private final Long taskId;
    private final AtomicBoolean isCanceled;
    private final AtomicBoolean isFinished;

    public RewriteTaskExecutor(List<RewriteDataGroup> groups, RewriteJob rewriteJob) {
        this.groups = Lists.newArrayList(groups);
        this.rewriteJob = rewriteJob;
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
        LOG.debug("[Rewrite Task] taskId: {} starting execution with {} groups", taskId, groups.size());

        if (isCanceled.get()) {
            LOG.debug("[Rewrite Task] taskId: {} was already canceled before execution", taskId);
            throw new JobException("Rewrite executor has been canceled, task id: {}", taskId);
        }

        LOG.debug("[Rewrite Task] taskId: {} updating state to REWRITING", taskId);
        try {
            rewriteJob.updateJobState(RewriteJobState.REWRITING, taskId, null, null, null);
        } catch (JobException e) {
            LOG.error("Failed to update job state to REWRITING", e);
            throw e;
        }

        RewriteResult totalResult = new RewriteResult();

        // Process each group in this executor
        for (RewriteDataGroup group : groups) {
            if (isCanceled.get()) {
                LOG.debug("[Rewrite Task] taskId: {} canceled during execution", taskId);
                throw new JobException("Rewrite executor has been canceled, task id: {}", taskId);
            }

            try {
                LOG.info("[Rewrite Task] taskId: {} processing group with {} tasks, size: {} bytes",
                        taskId, group.getTaskCount(), group.getTotalSize());

                // Execute rewrite for this group
                RewriteResult groupResult = executeGroup(group);
                totalResult.merge(groupResult);

                LOG.info("[Rewrite Task] taskId: {} completed group, result: {}", taskId, groupResult);

            } catch (Exception e) {
                LOG.error("[Rewrite Task] taskId: {} failed to process group: {}", taskId, e.getMessage(), e);
                try {
                    rewriteJob.updateJobState(RewriteJobState.CANCELLED, taskId, null,
                            RewriteFailMsg.CancelType.RUN_FAIL, e.getMessage());
                } catch (JobException je) {
                    LOG.error("Failed to update job state to CANCELLED", je);
                }
                throw new JobException("Rewrite group execution failed: " + e.getMessage(), e);
            }
        }

        if (isCanceled.get()) {
            LOG.debug("[Rewrite Task] taskId: {} canceled after processing all groups", taskId);
            throw new JobException("Rewrite executor has been canceled, task id: {}", taskId);
        }

        LOG.debug("[Rewrite Task] taskId: {} completed successfully, updating state to FINISHED", taskId);
        try {
            rewriteJob.updateJobState(RewriteJobState.FINISHED, taskId, totalResult, null, null);
        } catch (JobException e) {
            LOG.error("Failed to update job state to FINISHED", e);
            throw e;
        }

        isFinished.getAndSet(true);
        LOG.debug("[Rewrite Task] taskId: {} execution completed", taskId);
    }

    /**
     * Execute rewrite for a single group
     */
    private RewriteResult executeGroup(RewriteDataGroup group) throws UserException {
        // Create a new ConnectContext for this task
        ConnectContext taskConnectContext = buildConnectContext();
        
        // Set ConnectContext to thread local for this task
        taskConnectContext.setThreadLocalInfo();
        
        try {
            // Create a new executor for this group
            RewriteDataFileExecutor executor = new RewriteDataFileExecutor(
                    rewriteJob.getIcebergTable(), taskConnectContext);

            // Initialize executor
            executor.initialize();

            // Execute the group
            return executor.executeGroup(group);

        } catch (Exception e) {
            LOG.error("Failed to execute rewrite group: {}", e.getMessage(), e);
            throw new UserException("Rewrite group execution failed: " + e.getMessage(), e);
        } finally {
            // Clean up thread local ConnectContext
            ConnectContext.remove();
        }
    }

    /**
     * Build ConnectContext for this task executor
     */
    private ConnectContext buildConnectContext() {
        ConnectContext connectContext = new ConnectContext();
        
        // Copy session variables from the original context
        if (rewriteJob.getConnectContext() != null) {
            connectContext.setSessionVariable(rewriteJob.getConnectContext().getSessionVariable());
            connectContext.setCurrentUserIdentity(rewriteJob.getConnectContext().getCurrentUserIdentity());
            connectContext.setDatabase(rewriteJob.getConnectContext().getDatabase());
        }
        
        // Set environment
        connectContext.setEnv(org.apache.doris.catalog.Env.getCurrentEnv());
        
        // Generate unique query ID for this task
        UUID uuid = UUID.randomUUID();
        connectContext.setQueryId(new org.apache.doris.thrift.TUniqueId(
                uuid.getMostSignificantBits(), uuid.getLeastSignificantBits()));
        connectContext.setStartTime();
        
        // Create and set StatementContext
        org.apache.doris.nereids.StatementContext statementContext = new org.apache.doris.nereids.StatementContext();
        statementContext.setConnectContext(connectContext);
        connectContext.setStatementContext(statementContext);
        
        return connectContext;
    }

    @Override
    public void cancel() throws JobException {
        if (isFinished.get()) {
            throw new JobException("Rewrite executor has finished, task id: {}", taskId);
        }

        isCanceled.getAndSet(true);
        LOG.info("[Rewrite Task] taskId: {} cancelled", taskId);
    }

    public List<RewriteDataGroup> getGroups() {
        return groups;
    }

    public RewriteJob getRewriteJob() {
        return rewriteJob;
    }

    public boolean isCanceled() {
        return isCanceled.get();
    }

    public boolean isFinished() {
        return isFinished.get();
    }
}
