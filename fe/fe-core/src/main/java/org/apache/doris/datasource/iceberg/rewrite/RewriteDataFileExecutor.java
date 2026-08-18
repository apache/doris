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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.iceberg.IcebergExternalTable;
import org.apache.doris.datasource.iceberg.IcebergMvccSnapshot;
import org.apache.doris.datasource.iceberg.IcebergTransaction;
import org.apache.doris.datasource.mvcc.MvccSnapshot;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroup;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.scheduler.manager.TransientTaskManager;
import org.apache.doris.system.Backend;
import org.apache.doris.transaction.TransactionManager;

import com.google.common.collect.Lists;
// Keep third-party imports lexical to preserve the repository's CustomImportOrder invariant.
import org.apache.iceberg.Table;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Executes INSERT-SELECT statements for Iceberg data file rewriting.
 */
public class RewriteDataFileExecutor {
    private static final Logger LOG = LogManager.getLogger(RewriteDataFileExecutor.class);

    private final IcebergExternalTable dorisTable;
    private final ConnectContext connectContext;

    public RewriteDataFileExecutor(IcebergExternalTable dorisTable,
            ConnectContext connectContext) {
        this.dorisTable = dorisTable;
        this.connectContext = connectContext;
    }

    /**
     * Execute rewrite for multiple groups concurrently
     */
    public RewriteResult executeGroupsConcurrently(List<RewriteDataGroup> groups, long targetFileSizeBytes)
            throws UserException {
        TransactionManager transactionManager = dorisTable.getCatalog().getTransactionManager();
        long transactionId = transactionManager.begin();
        List<RewriteGroupTask> tasks = Lists.newArrayList();
        boolean committed = false;
        try {
            IcebergTransaction transaction = (IcebergTransaction) transactionManager
                    .getTransaction(transactionId);
            MvccSnapshot targetSnapshot = dorisTable.loadSnapshot(Optional.empty(), Optional.empty());
            Table targetIcebergTable = ((IcebergMvccSnapshot) targetSnapshot).getSnapshotCacheValue()
                    .getIcebergTable().orElseThrow(
                            () -> new UserException("Iceberg rewrite target metadata is not available"));
            transaction.beginRewrite(dorisTable, targetIcebergTable);

            for (RewriteDataGroup group : groups) {
                transaction.updateRewriteFiles(Lists.newArrayList(group.getDataFiles()));
            }

            RewriteResultCollector resultCollector = new RewriteResultCollector(groups.size(), tasks);
            int availableBeCount = getAvailableBeCount();
            for (RewriteDataGroup group : groups) {
                RewriteGroupTask task = new RewriteGroupTask(
                        group, transactionId, dorisTable, targetSnapshot, connectContext,
                        targetFileSizeBytes, availableBeCount,
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
                tasks.add(task);
            }

            try {
                for (TransientTaskExecutor task : tasks) {
                    Env.getCurrentEnv().getTransientTaskManager().addMemoryTask(task);
                }
            } catch (JobException e) {
                throw new UserException("Failed to submit rewrite tasks: " + e.getMessage(), e);
            }

            waitForTasksCompletion(resultCollector, groups.size());
            transaction.finishRewrite();

            int rewrittenDataFilesCount = groups.stream()
                    .mapToInt(group -> group.getDataFiles().size()).sum();
            int addedDataFilesCount = transaction.getFilesToAddCount();
            long rewrittenBytesCount = groups.stream().mapToLong(group -> group.getTotalSize()).sum();
            int removedDeleteFilesCount = groups.stream()
                    .mapToInt(group -> group.getDeleteFileCount()).sum();

            transactionManager.commit(transactionId);
            committed = true;
            invalidateTableCacheAfterCommit();
            return new RewriteResult(rewrittenDataFilesCount, addedDataFilesCount,
                    rewrittenBytesCount, removedDeleteFilesCount);
        } finally {
            if (!committed) {
                cancelAndQuiesce(tasks);
                // No task may update the transaction after rollback releases its rewrite fence.
                transactionManager.rollback(transactionId);
            }
        }
    }

    void invalidateTableCacheAfterCommit() {
        try {
            // Rewrite commits bypass the external-table DDL path, so evict the pre-rewrite snapshot before reuse.
            Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(dorisTable);
        } catch (Exception e) {
            // Cache refresh is outside the durable Iceberg commit boundary and cannot make it retryable.
            LOG.warn("Post-commit cache invalidation failed for rewritten table {}",
                    dorisTable.getName(), e);
        }
    }

    private void cancelAndQuiesce(List<RewriteGroupTask> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        TransientTaskManager taskManager = Env.getCurrentEnv().getTransientTaskManager();
        for (RewriteGroupTask task : tasks) {
            try {
                taskManager.cancelMemoryTask(task.getId());
            } catch (JobException e) {
                LOG.warn("Failed to remove rewrite task {} from the transient queue", task.getId(), e);
            }
            try {
                task.cancel();
            } catch (JobException e) {
                LOG.warn("Failed to cancel rewrite task {}", task.getId(), e);
            }
        }
        for (RewriteGroupTask task : tasks) {
            task.awaitCompletionUninterruptibly();
        }
    }

    /**
     * Wait for all tasks to complete using notification mechanism
     */
    private void waitForTasksCompletion(RewriteResultCollector collector, int totalTasks)
            throws UserException {
        LOG.info("Waiting for {} rewrite tasks to complete using notification mechanism", totalTasks);

        int maxWaitTime = connectContext.getSessionVariable().getInsertTimeoutS();

        try {
            boolean completed = collector.await(maxWaitTime, TimeUnit.SECONDS);

            if (!completed) {
                LOG.warn("Rewrite tasks did not complete within timeout of {} seconds", maxWaitTime);
                throw new UserException("Rewrite tasks did not complete within timeout");
            }

            // Check if any task failed
            if (collector.getFirstError() != null) {
                LOG.warn("Rewrite tasks failed: {}", collector.getFirstError().getMessage());
                throw new UserException("Some rewrite tasks failed: " + collector.getFirstError().getMessage(),
                        collector.getFirstError());
            }

            LOG.info("All {} rewrite tasks completed successfully", totalTasks);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Wait for tasks completion was interrupted", e);
            throw new UserException("Wait for tasks completion was interrupted", e);
        }
    }

    private int getAvailableBeCount() throws UserException {
        ComputeGroup computeGroup = connectContext.getComputeGroup();
        List<Backend> backends = computeGroup.getBackendList();
        int availableBeCount = 0;
        for (Backend backend : backends) {
            if (backend.isAlive()) {
                availableBeCount++;
            }
        }
        return availableBeCount;
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
            }
            LOG.warn("Task {} failed ({}/{}): {}", taskId, failed, expectedTasks, error.getMessage());
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
