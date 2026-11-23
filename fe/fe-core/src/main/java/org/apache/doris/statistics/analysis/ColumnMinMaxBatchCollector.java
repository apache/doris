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

package org.apache.doris.statistics.analysis;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.MaterializedIndexMeta;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.common.Status;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.repository.ResultRow;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CancellationException;
import java.util.stream.Collectors;

/** Shares bounds only between columns selected for table sampling at job creation. */
final class ColumnMinMaxBatchCollector {
    private static final Logger LOG = LogManager.getLogger(ColumnMinMaxBatchCollector.class);

    private enum State {
        PENDING, RUNNING, SUCCEEDED, FALLBACK, CANCELLED
    }

    private record Target(long jobId, long indexId, String qualifiedTable, String indexSql, boolean aggregated) {
    }

    private final List<Batch> batches = new ArrayList<>();
    // Guarded by this collector's monitor. Never execute or cancel SQL under the monitor.
    private boolean heavyBatchRunning;

    ColumnMinMaxBatchCollector(Collection<? extends BaseAnalysisTask> tasks, int batchSize) {
        if (batchSize <= 1) {
            return;
        }
        Map<Target, Map<String, List<OlapAnalysisTask>>> groups = new HashMap<>();
        for (BaseAnalysisTask task : tasks) {
            if (!(task instanceof OlapAnalysisTask olapTask) || task.tableSample == null || task.info.rowCount == 0) {
                continue;
            }
            OlapTable table = (OlapTable) task.tbl;
            long indexId = task.info.indexId;
            MaterializedIndexMeta meta = table.getIndexMetaByIndexId(indexId == -1 ? table.getBaseIndexId() : indexId);
            Column column = meta == null ? null : meta.getColumnByName(task.info.colName);
            // DDL can remove a column, and getColumnByName also strips embedded backticks.
            // Only batch a physical column that matches the SQL identifier the task would query.
            if (column != null && column.getName().equalsIgnoreCase(task.info.colName)) {
                Target target = new Target(task.info.jobId, indexId, olapTask.qualifiedTableName(),
                        olapTask.getIndex(), column.isAggregated());
                groups.computeIfAbsent(target, key -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                        .computeIfAbsent(column.getName(), key -> new ArrayList<>()).add(olapTask);
            }
        }
        groups.forEach((target, byName) -> {
            for (List<String> names : Lists.partition(new ArrayList<>(byName.keySet()), batchSize)) {
                List<OlapAnalysisTask> members = names.stream().flatMap(name -> byName.get(name).stream())
                        .collect(Collectors.toList());
                Batch batch = new Batch(target, names, members);
                batches.add(batch);
                members.forEach(task -> task.minMaxBatch = batch);
            }
        });
    }

    final class Batch {
        private final Target target;
        private final List<String> columns;
        private final List<OlapAnalysisTask> memberTasks;
        // All mutable fields are guarded by the collector's monitor.
        private State state = State.PENDING;
        private ResultRow result;
        private StmtExecutor activeExecutor;

        private Batch(Target target, List<String> columns, List<OlapAnalysisTask> memberTasks) {
            this.target = target;
            this.columns = List.copyOf(columns);
            this.memberTasks = List.copyOf(memberTasks);
        }

        ResultRow get(OlapAnalysisTask caller) throws InterruptedException {
            while (true) {
                synchronized (ColumnMinMaxBatchCollector.this) {
                    checkCaller(caller);
                    if (state == State.FALLBACK) {
                        return null;
                    }
                    if (state == State.SUCCEEDED) {
                        int offset = 2 * Collections.binarySearch(columns, caller.info.colName,
                                String.CASE_INSENSITIVE_ORDER);
                        return new ResultRow(result.getValues().subList(offset, offset + 2));
                    }
                    if (state == State.RUNNING || (target.aggregated && heavyBatchRunning)) {
                        ColumnMinMaxBatchCollector.this.wait();
                        continue;
                    }
                    // Claim the batch and its scan slot together. A worker waiting for a slot owns neither.
                    state = State.RUNNING;
                    if (target.aggregated) {
                        heavyBatchRunning = true;
                    }
                }
                execute(caller);
            }
        }

        // Called under the collector monitor, including immediately before registering the executor.
        private void checkCaller(OlapAnalysisTask caller) throws InterruptedException {
            caller.checkMinMaxCancelled();
            if (state == State.CANCELLED) {
                throw new CancellationException("Analysis bounds cancelled");
            }
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Analysis bounds interrupted");
            }
        }

        private void execute(OlapAnalysisTask caller) throws InterruptedException {
            State next = State.PENDING;
            ResultRow row = null;
            try (AutoCloseConnectContext ctx = StatisticsUtil.buildConnectContext(false)) {
                String sql = OlapAnalysisTask.buildMinMaxSql(target.qualifiedTable, target.indexSql, columns);
                StmtExecutor executor = new StmtExecutor(ctx.connectContext, sql);
                synchronized (ColumnMinMaxBatchCollector.this) {
                    checkCaller(caller);
                    activeExecutor = executor;
                }
                row = OlapAnalysisTask.boundsRow(executor.executeInternalQuery(), columns.size());
                next = State.SUCCEEDED;
            } catch (RuntimeException e) {
                // An aborted owner leaves PENDING for a live sibling. Only an ordinary SQL failure
                // switches all remaining columns to individual collection.
                synchronized (ColumnMinMaxBatchCollector.this) {
                    checkCaller(caller);
                }
                if (Throwables.getRootCause(e) instanceof InterruptedException interrupted) {
                    throw interrupted;
                }
                LOG.warn("Batch min/max failed for job {}, index {}, columns {}. Collecting individually.",
                        target.jobId, target.indexId, columns, e);
                next = State.FALLBACK;
            } finally {
                synchronized (ColumnMinMaxBatchCollector.this) {
                    activeExecutor = null;
                    if (target.aggregated) {
                        heavyBatchRunning = false;
                    }
                    if (state == State.RUNNING) {
                        result = row;
                        state = next;
                    }
                    ColumnMinMaxBatchCollector.this.notifyAll();
                }
            }
        }

        void onTaskCancelled() {
            StmtExecutor executor = null;
            synchronized (ColumnMinMaxBatchCollector.this) {
                if (state != State.PENDING && state != State.RUNNING) {
                    return;
                }
                if (memberTasks.stream().allMatch(t -> t.killed)) {
                    executor = cancelLocked();
                }
                ColumnMinMaxBatchCollector.this.notifyAll();
            }
            if (executor != null) {
                cancelQuery(executor);
            }
        }

        private StmtExecutor cancelLocked() {
            if (state == State.PENDING || state == State.RUNNING) {
                state = State.CANCELLED;
                StmtExecutor executor = activeExecutor;
                activeExecutor = null;
                return executor;
            }
            return null;
        }
    }

    void cancelAll() {
        List<StmtExecutor> toCancel = new ArrayList<>();
        synchronized (this) {
            for (Batch batch : batches) {
                StmtExecutor executor = batch.cancelLocked();
                if (executor != null) {
                    toCancel.add(executor);
                }
            }
            notifyAll();
        }
        toCancel.forEach(ColumnMinMaxBatchCollector::cancelQuery);
    }

    private static void cancelQuery(StmtExecutor executor) {
        try {
            executor.cancel(Status.CANCELLED);
        } catch (RuntimeException e) {
            LOG.warn("Failed to cancel an analysis bounds query", e);
        }
    }
}
