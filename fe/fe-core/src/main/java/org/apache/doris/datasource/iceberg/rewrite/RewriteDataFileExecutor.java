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
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.nereids.analyzer.UnboundIcebergTableSink;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.plans.commands.info.DMLCommandType;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes INSERT-SELECT statements for Iceberg data file rewriting.
 *
 * Execution Flow:
 * 1. initialize() - Build rewrite logical plan
 * 2. executeGroup() - Execute rewrite for each file group
 * 3. Collect execution statistics and return results
 */
public class RewriteDataFileExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFileExecutor.class);

    private final IcebergExternalTable dorisTable;
    private final ConnectContext connectContext;

    // Pre-prepared execution context
    private boolean initialized = false;
    private InsertIntoTableCommand logicalPlan;
    private StatementBase parsedStmt;

    public RewriteDataFileExecutor(IcebergExternalTable dorisTable,
            ConnectContext connectContext) {
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
    }

    /**
     * Initialize the executor with manually constructed logical plan
     */
    public void initialize() {
        if (initialized) {
            return;
        }
        this.logicalPlan = buildRewriteLogicalPlan();

        LogicalPlanAdapter adapter = new LogicalPlanAdapter(logicalPlan, connectContext.getStatementContext());
        adapter.setOrigStmt(new OriginStatement(logicalPlan.toString(), 0));
        this.parsedStmt = adapter;
        this.initialized = true;

        LOG.debug("Initialized executor with manually constructed logical plan");
    }

    /**
     * Execute rewrite for multiple groups concurrently
     */
    public RewriteResult executeGroupsConcurrently(List<RewriteDataGroup> groups) throws UserException {
        if (!initialized) {
            throw new UserException("Executor not initialized. Call initialize() first.");
        }

        LOG.info("Starting concurrent rewrite with {} groups", groups.size());

        // Begin transaction
        long transactionId = dorisTable.getCatalog().getTransactionManager().begin();
        IcebergTransaction transaction = (IcebergTransaction) dorisTable.getCatalog().getTransactionManager()
                .getTransaction(transactionId);
        transaction.beginRewrite(dorisTable);

        // Create and submit tasks
        List<RewriteGroupTask> tasks = Lists.newArrayList();
        for (RewriteDataGroup group : groups) {
            transaction.updateRewriteFiles(Lists.newArrayList(group.getDataFiles()));
            RewriteGroupTask task = new RewriteGroupTask(group, transactionId, logicalPlan, parsedStmt, connectContext,
                    null); // Callback will be set after creating the collector
            tasks.add(task);
        }

        // Create result collector with tasks list for cancellation support
        RewriteResultCollector resultCollector = new RewriteResultCollector(groups.size(), tasks);

        // Update tasks with the result collector callback
        for (int i = 0; i < groups.size(); i++) {
            RewriteDataGroup group = groups.get(i);
            RewriteGroupTask task = new RewriteGroupTask(group, transactionId, logicalPlan, parsedStmt, connectContext,
                    new RewriteGroupTask.RewriteResultCallback() {
                        @Override
                        public void onTaskCompleted(Long taskId) {
                            resultCollector.onTaskCompleted(taskId);
                        }

                        @Override
                        public void onTaskFailed(Long taskId, Exception error) {
                            resultCollector.onTaskFailed(taskId, error);
                        }
                    });
            tasks.set(i, task);
        }

        // Submit tasks to TransientTaskManager
        try {
            for (TransientTaskExecutor task : tasks) {
                Env.getCurrentEnv().getTransientTaskManager().addMemoryTask(task);
            }
        } catch (JobException e) {
            throw new UserException("Failed to submit rewrite tasks: " + e.getMessage(), e);
        }

        // Wait for all tasks to complete
        waitForTasksCompletion(resultCollector, groups.size());

        // Finish rewrite operation
        transaction.finishRewrite();

        // Collect statistics from transaction after all tasks are completed
        int rewrittenDataFilesCount = groups.stream().mapToInt(group -> group.getDataFiles().size()).sum();
        // this should after finishRewrite
        int addedDataFilesCount = transaction.getFilesToAddCount();
        long rewrittenBytesCount = groups.stream().mapToLong(group -> group.getTotalSize()).sum();
        int removedDeleteFilesCount = groups.stream().mapToInt(group -> group.getDeleteFileCount()).sum();

        // Commit transaction
        transaction.commit();

        return new RewriteResult(rewrittenDataFilesCount, addedDataFilesCount,
                rewrittenBytesCount, removedDeleteFilesCount);
    }

    /**
     * Build logical plan for rewrite operation (INSERT INTO ... SELECT ...)
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
     * Wait for all tasks to complete using notification mechanism
     */
    private void waitForTasksCompletion(RewriteResultCollector collector, int totalTasks)
            throws UserException {
        LOG.info("Waiting for {} rewrite tasks to complete using notification mechanism", totalTasks);

        int maxWaitTime = 300; // 5 minutes

        try {
            boolean completed = collector.await(maxWaitTime, TimeUnit.SECONDS);

            if (!completed) {
                LOG.error("Rewrite tasks did not complete within timeout of {} seconds", maxWaitTime);
                throw new UserException("Rewrite tasks did not complete within timeout");
            }

            // Check if any task failed
            if (collector.getFirstError() != null) {
                LOG.error("Rewrite tasks failed: {}", collector.getFirstError().getMessage());
                throw new UserException("Some rewrite tasks failed: " + collector.getFirstError().getMessage(),
                        collector.getFirstError());
            }

            LOG.info("All {} rewrite tasks completed successfully", totalTasks);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Wait for tasks completion was interrupted", e);
            throw new UserException("Wait for tasks completion was interrupted", e);
        }
    }

    /**
     * Result collector for concurrent rewrite tasks
     */
    private static class RewriteResultCollector {
        private final int expectedTasks;
        private final AtomicInteger completedTasks = new AtomicInteger(0);
        private final AtomicInteger failedTasks = new AtomicInteger(0);
        private volatile Exception firstError = null;
        private final CountDownLatch completionLatch;
        private final List<RewriteGroupTask> allTasks;

        public RewriteResultCollector(int expectedTasks, List<RewriteGroupTask> tasks) {
            this.expectedTasks = expectedTasks;
            this.completionLatch = new CountDownLatch(expectedTasks);
            this.allTasks = tasks;
        }

        public synchronized void onTaskCompleted(Long taskId) {
            int completed = completedTasks.incrementAndGet();
            LOG.info("Task {} completed ({}/{})", taskId, completed, expectedTasks);
            completionLatch.countDown();
        }

        public synchronized void onTaskFailed(Long taskId, Exception error) {
            int failed = failedTasks.incrementAndGet();
            if (firstError == null) {
                firstError = error;

                // Cancel all other tasks immediately when first failure occurs
                LOG.warn("Task {} failed, cancelling all other tasks", taskId);
                cancelAllOtherTasks(taskId);

                // Count down remaining tasks to unblock waiting thread
                int remaining = expectedTasks - completedTasks.get() - failedTasks.get();
                for (int i = 0; i < remaining; i++) {
                    completionLatch.countDown();
                }
            }
            LOG.error("Task {} failed ({}/{}): {}", taskId, failed, expectedTasks, error.getMessage());
            completionLatch.countDown();
        }

        private void cancelAllOtherTasks(Long failedTaskId) {
            for (RewriteGroupTask task : allTasks) {
                if (!task.getId().equals(failedTaskId)) {
                    try {
                        task.cancel();
                        LOG.info("Cancelled task {}", task.getId());
                    } catch (Exception e) {
                        LOG.warn("Failed to cancel task {}: {}", task.getId(), e.getMessage());
                    }
                }
            }
        }

        public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
            return completionLatch.await(timeout, unit);
        }

        public Exception getFirstError() {
            return firstError;
        }
    }
}
