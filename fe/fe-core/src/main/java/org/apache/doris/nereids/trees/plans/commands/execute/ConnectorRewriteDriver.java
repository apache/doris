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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.UserException;
import org.apache.doris.connector.api.ConnectorColumn;
import org.apache.doris.connector.api.ConnectorMetadata;
import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.connector.api.ConnectorType;
import org.apache.doris.connector.api.DorisConnectorException;
import org.apache.doris.connector.api.handle.ConnectorTableHandle;
import org.apache.doris.connector.api.handle.ConnectorTransaction;
import org.apache.doris.connector.api.procedure.ConnectorProcedureOps;
import org.apache.doris.connector.api.procedure.ConnectorProcedureResult;
import org.apache.doris.connector.api.procedure.ConnectorRewriteGroup;
import org.apache.doris.connector.api.pushdown.ConnectorPredicate;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.PluginDrivenExternalCatalog;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.TransientTaskExecutor;
import org.apache.doris.transaction.PluginDrivenTransactionManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Engine-neutral driver for a distributed {@code rewrite_data_files} (compaction), the post-flip
 * counterpart of the legacy {@code RewriteDataFileExecutor} + {@code IcebergRewriteDataFilesAction}. It
 * orchestrates the read/write distribution; the connector owns the file-selection / bin-pack / commit
 * decisions behind neutral SPIs (no {@code instanceof Iceberg}, no iceberg types).
 *
 * <p>Flow: (0) ask the connector to plan N bin-packed groups ({@link ConnectorProcedureOps#planRewrite}); (1)
 * open ONE shared connector transaction; (2) run one {@code INSERT-SELECT} per group concurrently, each
 * scoped to its files and bound to the shared transaction; (3) register the union of source files to remove
 * (AFTER the groups began the transaction so the table + OCC snapshot are loaded); (4) commit once; (5) read
 * the added-files count post-commit and emit the four-column result row.</p>
 *
 * <p><b>R6 scope.</b> No-WHERE rewrite only (WHERE lowering is a later step). Output file SIZING — the
 * per-group {@code target-file-size}/parallelism that the legacy task threaded via an iceberg session var —
 * is deferred (see the rewrite-output-sizing follow-up): every group GATHERs to a single writer via the
 * rewrite sink flag, which is correct but not size-tuned. This only affects real BE writes (exercised at the
 * flip rehearsal), not the dormant pre-flip / mock path.</p>
 */
public class ConnectorRewriteDriver {
    private static final Logger LOG = LogManager.getLogger(ConnectorRewriteDriver.class);

    private final ConnectContext ctx;
    private final ExternalTable table;
    private final PluginDrivenExternalCatalog catalog;
    private final ConnectorMetadata metadata;
    private final ConnectorProcedureOps procedureOps;
    private final ConnectorSession session;
    private final ConnectorTableHandle tableHandle;
    private final String procedureName;
    private final Map<String, String> properties;
    private final List<String> partitionNames;
    // The engine-lowered WHERE restricting which files to rewrite, or null when there is no WHERE. Passed
    // straight through to the connector's planRewrite (the connector scopes the rewrite to the matching files).
    private final ConnectorPredicate whereCondition;

    /**
     * Builds a driver bound to one {@code ALTER TABLE ... EXECUTE rewrite_data_files} invocation; all of
     * these are already resolved by {@code ConnectorExecuteAction} (the only caller).
     */
    public ConnectorRewriteDriver(ConnectContext ctx, ExternalTable table, PluginDrivenExternalCatalog catalog,
            ConnectorMetadata metadata, ConnectorProcedureOps procedureOps, ConnectorSession session,
            ConnectorTableHandle tableHandle, String procedureName, Map<String, String> properties,
            List<String> partitionNames, ConnectorPredicate whereCondition) {
        this.ctx = ctx;
        this.table = table;
        this.catalog = catalog;
        this.metadata = metadata;
        this.procedureOps = procedureOps;
        this.session = session;
        this.tableHandle = tableHandle;
        this.procedureName = procedureName;
        this.properties = properties;
        this.partitionNames = partitionNames;
        this.whereCondition = whereCondition;
    }

    /**
     * Runs the distributed rewrite and returns the single-row result the engine wraps into a ResultSet.
     */
    public ConnectorProcedureResult run() throws UserException {
        // STEP 0: ask the connector to plan the bin-packed groups, scoped by the lowered WHERE (null = no WHERE).
        List<ConnectorRewriteGroup> groups;
        try {
            groups = procedureOps.planRewrite(session, tableHandle, procedureName, properties, whereCondition,
                    partitionNames);
        } catch (DorisConnectorException e) {
            throw new UserException(e.getMessage(), e);
        }
        if (groups == null || groups.isEmpty()) {
            // Nothing to rewrite: skip the transaction entirely and return the all-zero row (legacy parity).
            return buildResult(0, 0, 0, 0);
        }

        // STEP 1: open ONE shared connector transaction for all groups.
        PluginDrivenTransactionManager txnManager =
                (PluginDrivenTransactionManager) catalog.getTransactionManager();
        ConnectorTransaction connectorTx;
        long txnId;
        try {
            connectorTx = metadata.beginTransaction(session);
            txnId = txnManager.begin(connectorTx);
        } catch (DorisConnectorException e) {
            throw new UserException(e.getMessage(), e);
        }

        try {
            // STEP 2: run one INSERT-SELECT per group concurrently, all sharing the transaction.
            runGroups(groups, txnId, connectorTx);

            // STEP 3: register the union of source data files to remove. AFTER the groups ran, so the first
            // group's write loaded the table + pinned the OCC snapshot that the connector re-derives against;
            // BEFORE commit, which consumes the registered files in the RewriteFiles op.
            for (ConnectorRewriteGroup group : groups) {
                connectorTx.registerRewriteSourceFiles(group.getDataFilePaths());
            }
        } catch (Exception e) {
            txnManager.rollback(txnId);
            if (e instanceof UserException) {
                throw (UserException) e;
            }
            throw new UserException("Failed to rewrite data files: " + e.getMessage(), e);
        }

        // STEP 4: commit once. The manager deregisters the transaction on both success and failure, so a
        // failed commit needs no rollback (it would find nothing) — surface it directly.
        txnManager.commit(txnId);

        // STEP 5: post-commit statistics. The added-files count is only valid after commit (it is
        // materialized from the BE commit fragments during commit); the other three are summed from the
        // planning groups (the connector exposes them on each ConnectorRewriteGroup).
        int addedDataFilesCount = connectorTx.getRewriteAddedDataFilesCount();
        int rewrittenDataFilesCount = groups.stream().mapToInt(ConnectorRewriteGroup::getDataFileCount).sum();
        long rewrittenBytesCount = groups.stream().mapToLong(ConnectorRewriteGroup::getTotalSizeBytes).sum();
        int removedDeleteFilesCount = groups.stream().mapToInt(ConnectorRewriteGroup::getDeleteFileCount).sum();

        return buildResult(rewrittenDataFilesCount, addedDataFilesCount, rewrittenBytesCount,
                removedDeleteFilesCount);
    }

    private void runGroups(List<ConnectorRewriteGroup> groups, long txnId, ConnectorTransaction connectorTx)
            throws UserException {
        List<ConnectorRewriteGroupTask> tasks = Lists.newArrayList();
        RewriteResultCollector collector = new RewriteResultCollector(groups.size(), tasks);

        for (ConnectorRewriteGroup group : groups) {
            ConnectorRewriteGroupTask task = new ConnectorRewriteGroupTask(group, txnId, connectorTx, table, ctx,
                    new ConnectorRewriteGroupTask.RewriteResultCallback() {
                        @Override
                        public void onTaskCompleted(Long taskId) {
                            collector.onTaskCompleted(taskId);
                        }

                        @Override
                        public void onTaskFailed(Long taskId, Exception error) {
                            collector.onTaskFailed(taskId, error);
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

        int maxWaitTime = ctx.getSessionVariable().getInsertTimeoutS();
        try {
            boolean completed = collector.await(maxWaitTime, TimeUnit.SECONDS);
            if (!completed) {
                throw new UserException("Rewrite tasks did not complete within timeout");
            }
            if (collector.getFirstError() != null) {
                throw new UserException("Some rewrite tasks failed: " + collector.getFirstError().getMessage(),
                        collector.getFirstError());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UserException("Wait for rewrite tasks completion was interrupted", e);
        }
    }

    private ConnectorProcedureResult buildResult(int rewrittenDataFilesCount, int addedDataFilesCount,
            long rewrittenBytesCount, int removedDeleteFilesCount) {
        // Four-column schema in the exact legacy order/types (IcebergRewriteDataFilesAction.getResultSchema);
        // rewritten_bytes_count is INT for byte-parity with legacy (a latent quirk kept on purpose).
        List<ConnectorColumn> schema = ImmutableList.of(
                new ConnectorColumn("rewritten_data_files_count", ConnectorType.of("INT"),
                        "Number of data which were re-written by this command", false, null),
                new ConnectorColumn("added_data_files_count", ConnectorType.of("INT"),
                        "Number of new data files which were written by this command", false, null),
                new ConnectorColumn("rewritten_bytes_count", ConnectorType.of("INT"),
                        "Number of bytes which were written by this command", false, null),
                new ConnectorColumn("removed_delete_files_count", ConnectorType.of("BIGINT"),
                        "Number of delete files removed by this command", false, null));
        List<String> row = ImmutableList.of(
                String.valueOf(rewrittenDataFilesCount),
                String.valueOf(addedDataFilesCount),
                String.valueOf(rewrittenBytesCount),
                String.valueOf(removedDeleteFilesCount));
        return new ConnectorProcedureResult(schema, ImmutableList.of(row));
    }

    /**
     * Collects concurrent group-task completions and cancels the rest on the first failure (copied from the
     * legacy {@code RewriteDataFileExecutor.RewriteResultCollector}).
     */
    private static class RewriteResultCollector {
        private final int expectedTasks;
        private final AtomicInteger completedTasks = new AtomicInteger(0);
        private final AtomicInteger failedTasks = new AtomicInteger(0);
        private volatile Exception firstError = null;
        private final CountDownLatch completionLatch;
        private final List<ConnectorRewriteGroupTask> allTasks;

        RewriteResultCollector(int expectedTasks, List<ConnectorRewriteGroupTask> tasks) {
            this.expectedTasks = expectedTasks;
            this.completionLatch = new CountDownLatch(expectedTasks);
            this.allTasks = tasks;
        }

        public synchronized void onTaskCompleted(Long taskId) {
            int completed = completedTasks.incrementAndGet();
            LOG.info("Connector rewrite task {} completed ({}/{})", taskId, completed, expectedTasks);
            completionLatch.countDown();
        }

        public synchronized void onTaskFailed(Long taskId, Exception error) {
            int failed = failedTasks.incrementAndGet();
            if (firstError == null) {
                firstError = error;
                cancelAllOtherTasks(taskId);
            }
            LOG.warn("Connector rewrite task {} failed ({}/{}): {}", taskId, failed, expectedTasks,
                    error.getMessage());
            completionLatch.countDown();
        }

        private void cancelAllOtherTasks(Long failedTaskId) {
            for (ConnectorRewriteGroupTask task : allTasks) {
                if (!task.getId().equals(failedTaskId)) {
                    try {
                        task.cancel();
                    } catch (Exception e) {
                        LOG.warn("Failed to cancel rewrite task {}: {}", task.getId(), e.getMessage());
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
