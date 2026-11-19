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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.analysis.AnalysisInfo.AnalysisMethod;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TStatusCode;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;

public class AnalysisJob {

    private final Set<BaseAnalysisTask> queryingTasks;
    private List<BaseAnalysisTask> tasksAwaitingFlush = new ArrayList<>();
    private List<ColStatsData> bufferedStats = new ArrayList<>();

    @VisibleForTesting
    volatile StmtExecutor stmtExecutor;
    private volatile AnalysisState state = AnalysisState.RUNNING;
    private final List<BaseAnalysisTask> tasks;

    // Guarded by this. A writer relinquishes ownership under the same lock used to append results.
    private boolean flushing;

    private volatile AnalysisInfo jobInfo;
    private boolean historyDropped;
    private final AnalysisManager analysisManager;

    public AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask) {
        this(jobInfo, queryingTask, Env.getCurrentEnv().getAnalysisManager());
    }

    AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask,
            AnalysisManager analysisManager) {
        for (BaseAnalysisTask task : queryingTask) {
            task.job = this;
        }
        this.tasks = new ArrayList<>(queryingTask);
        this.queryingTasks = new HashSet<>(queryingTask);
        this.jobInfo = jobInfo;
        this.analysisManager = analysisManager;
    }

    public void taskDoneWithoutData(BaseAnalysisTask task) {
        appendBuf(task, List.of());
    }

    void taskDoneWithoutData(BaseAnalysisTask task, String message) {
        appendBuf(task, List.of(), message);
    }

    public void appendBuf(BaseAnalysisTask task, List<ColStatsData> statsData) {
        appendBuf(task, statsData, "");
    }

    private void appendBuf(BaseAnalysisTask task, List<ColStatsData> statsData, String message) {
        synchronized (this) {
            if (isTerminated() || !queryingTasks.remove(task)) {
                return;
            }
            if (!message.isEmpty()) {
                task.info.message = message;
            }
            bufferedStats.addAll(statsData);
            tasksAwaitingFlush.add(task);
            if (flushing) {
                return;
            }
            flushing = true;
        }
        flushReadyBatches();
    }

    private void flushReadyBatches() {
        try {
            while (true) {
                List<ColStatsData> rows;
                List<BaseAnalysisTask> batch;
                synchronized (this) {
                    if (isTerminated()) {
                        return;
                    }
                    if (tasksAwaitingFlush.isEmpty() || (!queryingTasks.isEmpty()
                            && bufferedStats.size() < StatisticsUtil.getInsertMergeCount())) {
                        flushing = false;
                        return;
                    }
                    rows = bufferedStats;
                    batch = tasksAwaitingFlush;
                    bufferedStats = new ArrayList<>();
                    tasksAwaitingFlush = new ArrayList<>();
                }

                if (!rows.isEmpty()) {
                    insertColumnStats(rows);
                }

                synchronized (this) {
                    if (isTerminated()) {
                        return;
                    }
                    updateTaskStates(batch, AnalysisState.FINISHED, "");
                    if (queryingTasks.isEmpty() && tasksAwaitingFlush.isEmpty()) {
                        state = AnalysisState.FINISHED;
                        break;
                    }
                }
            }
        } catch (RuntimeException | Error e) {
            try {
                fail(StatisticsUtil.analyzeFailureMessage(e));
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != e) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw e;
        }
        complete(null);
    }

    private void updateTaskStates(List<BaseAnalysisTask> batch, AnalysisState state, String message) {
        long time = System.currentTimeMillis();
        RuntimeException failure = null;
        for (BaseAnalysisTask task : batch) {
            try {
                analysisManager.updateTaskStatus(task.info, state, message, time);
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private void insertColumnStats(List<ColStatsData> rows) {
        StringJoiner values = new StringJoiner(",");
        for (ColStatsData row : rows) {
            values.add(row.toSQL(true));
        }
        String insertStmt = StatisticConstants.INSERT_INTO_COLUMN_STATS_PREFIX + values;
        try (AutoCloseConnectContext context = StatisticsUtil.buildConnectContext(false)) {
            ConnectContext ctx = context.connectContext;
            ctx.setSqlHash(DigestUtils.md5Hex(insertStmt));
            StmtExecutor executor = new StmtExecutor(ctx, insertStmt);
            stmtExecutor = executor;
            if (!isTerminated()) {
                executeInsert(executor);
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to write column statistics", e);
        } finally {
            // Keep the cancellation handle available until execution and audit logging finish.
            stmtExecutor = null;
        }
    }

    @VisibleForTesting
    void executeInsert(StmtExecutor executor) throws Exception {
        StatisticsUtil.executeAnalyzeInsert(executor);
    }

    public void fail(String reason) {
        synchronized (this) {
            if (isTerminated()) {
                return;
            }
            state = AnalysisState.FAILED;
        }

        try {
            RuntimeException failure = cancelExecutors(reason);
            try {
                updateTaskStates(tasks, AnalysisState.FAILED, reason);
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            complete(reason);
        }
    }

    private RuntimeException cancelExecutors(String reason) {
        RuntimeException failure = null;
        StmtExecutor executor = stmtExecutor;
        try {
            if (executor != null) {
                executor.cancel(new Status(TStatusCode.CANCELLED, reason));
            }
        } catch (RuntimeException e) {
            failure = e;
        }
        for (BaseAnalysisTask task : tasks) {
            try {
                task.cancel();
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
        }
        return failure;
    }

    private static RuntimeException combineFailures(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        if (failure != next) {
            failure.addSuppressed(next);
        }
        return failure;
    }

    synchronized void putPartitionUpdateRowsIfAbsent(long partitionId, long rows) {
        if (!isTerminated()) {
            jobInfo.partitionUpdateRows.putIfAbsent(partitionId, rows);
        }
    }

    synchronized void recordIndexRowCount(long indexId, long rowCount) {
        if (!isTerminated()) {
            jobInfo.addIndexRowCount(indexId, rowCount);
        }
    }

    synchronized void markSampleFallback() {
        if (isTerminated() || !analysisManager.isAsyncJob(jobInfo.jobId)
                || jobInfo.analysisMethod == AnalysisMethod.SAMPLE) {
            return;
        }
        AnalysisInfo sample = new AnalysisInfoBuilder(jobInfo).setAnalysisMethod(AnalysisMethod.SAMPLE).build();
        sample.markStartTime(jobInfo.startTime);
        sample.indexesRowCount.putAll(jobInfo.indexesRowCount);
        jobInfo = sample;
        if (!historyDropped) {
            analysisManager.replayCreateAnalysisJob(sample);
        }
    }

    public void cancel() {
        fail("analysis job cancelled");
    }

    private void complete(String reason) {
        try {
            analysisManager.finishJob(this, state, reason);
        } finally {
            analysisManager.removeJob(jobInfo.jobId);
            synchronized (this) {
                // Synchronous callers consume this metadata after the workers return.
                if (state == AnalysisState.FAILED) {
                    // Task records share these collections with the job record.
                    if (jobInfo.jobColumns != null) {
                        jobInfo.jobColumns.clear();
                    }
                    if (jobInfo.partitionNames != null) {
                        jobInfo.partitionNames.clear();
                    }
                    jobInfo.partitionUpdateRows.clear();
                }
                tasks.clear();
                queryingTasks.clear();
                tasksAwaitingFlush.clear();
                bufferedStats.clear();
            }
        }
    }

    boolean isTerminated() {
        return state != AnalysisState.RUNNING;
    }

    // Called under this monitor, which serializes history deletion and publication.
    void markHistoryDropped() {
        historyDropped = true;
    }

    boolean isHistoryDropped() {
        return historyDropped;
    }

    public AnalysisInfo getJobInfo() {
        return jobInfo;
    }
}
