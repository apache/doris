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
import org.apache.doris.common.util.Util;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.util.StatisticsUtil;
import org.apache.doris.thrift.TStatusCode;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;

public class AnalysisJob {

    public static final Logger LOG = LogManager.getLogger(AnalysisJob.class);

    protected Set<BaseAnalysisTask> queryingTask;

    protected Set<BaseAnalysisTask> queryFinished;

    protected List<ColStatsData> buf;

    protected volatile StmtExecutor stmtExecutor;

    protected volatile boolean killed;

    private final AtomicBoolean terminated = new AtomicBoolean();

    private final List<BaseAnalysisTask> tasks;

    // Protected by this; SQL execution itself must not hold the job monitor.
    private boolean flushing;

    protected long start;

    protected AnalysisInfo jobInfo;

    protected AnalysisManager analysisManager;

    public AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask) {
        this(jobInfo, queryingTask, Env.getCurrentEnv().getAnalysisManager());
    }

    AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask,
            AnalysisManager analysisManager) {
        for (BaseAnalysisTask task : queryingTask) {
            task.job = this;
        }
        this.tasks = new ArrayList<>(queryingTask);
        this.queryingTask = new HashSet<>(queryingTask);
        this.queryFinished = new HashSet<>();
        this.buf = new ArrayList<>();
        start = System.currentTimeMillis();
        this.jobInfo = jobInfo;
        this.analysisManager = analysisManager;
    }

    public void taskDoneWithoutData(BaseAnalysisTask task) {
        appendBuf(task, List.of());
    }

    public void appendBuf(BaseAnalysisTask task, List<ColStatsData> statsData) {
        synchronized (this) {
            // A cancelled query can still return a result. Never resurrect a terminated job.
            if (terminated.get()) {
                return;
            }
            queryingTask.remove(task);
            buf.addAll(statsData);
            queryFinished.add(task);
        }
        markOneTaskDone();
    }

    protected void markOneTaskDone() {
        synchronized (this) {
            if (terminated.get() || flushing
                    || (!queryingTask.isEmpty() && buf.size() < StatisticsUtil.getInsertMergeCount())) {
                return;
            }
            flushing = true;
        }
        boolean ownsFlush = true;
        try {
            while (!terminated.get()) {
                flushBuffer();
                boolean finished;
                synchronized (this) {
                    finished = queryingTask.isEmpty() && queryFinished.isEmpty();
                    if (!finished && !queryingTask.isEmpty()
                            && buf.size() < StatisticsUtil.getInsertMergeCount()) {
                        // Release ownership before another task appends the final batch.
                        flushing = false;
                        ownsFlush = false;
                        return;
                    }
                    if (finished && !terminated.compareAndSet(false, true)) {
                        return;
                    }
                }
                if (finished) {
                    deregisterJob();
                    return;
                }
            }
        } catch (RuntimeException | Error e) {
            try {
                taskFailed(null, Util.getRootCauseMessage(e));
            } catch (RuntimeException cleanupFailure) {
                if (cleanupFailure != e) {
                    e.addSuppressed(cleanupFailure);
                }
            }
            throw e;
        } finally {
            if (ownsFlush) {
                synchronized (this) {
                    flushing = false;
                }
            }
        }
    }

    private void updateTaskState(List<BaseAnalysisTask> finishedTasks, AnalysisState state, String msg) {
        long time = System.currentTimeMillis();
        RuntimeException failure = null;
        for (BaseAnalysisTask task : finishedTasks) {
            String taskMsg = msg;
            if ((taskMsg == null || taskMsg.isEmpty()) && task.info != null) {
                taskMsg = task.info.message;
            }
            try {
                analysisManager.updateTaskStatus(task.info, state, taskMsg == null ? "" : taskMsg, time);
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    protected void flushBuffer() {
        List<ColStatsData> data;
        List<BaseAnalysisTask> finishedTasks;
        synchronized (this) {
            if (terminated.get()) {
                return;
            }
            data = new ArrayList<>(buf);
            finishedTasks = new ArrayList<>(queryFinished);
            buf.clear();
            queryFinished.clear();
        }
        if (!data.isEmpty()) {
            StringJoiner values = new StringJoiner(",");
            for (ColStatsData row : data) {
                values.add(row.toSQL(true));
            }
            String insertStmt = StatisticConstants.INSERT_INTO_COLUMN_STATS_PREFIX + values;
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
                ConnectContext context = r.connectContext;
                context.setSqlHash(DigestUtils.md5Hex(insertStmt));
                StmtExecutor executor = new StmtExecutor(context, insertStmt);
                stmtExecutor = executor;
                executeWithExceptionOnFail(executor);
            } catch (Exception e) {
                throw new RuntimeException("Failed to analyze: " + e.getMessage(), e);
            } finally {
                // Keep the cancellation handle until execution and audit logging have finished.
                stmtExecutor = null;
            }
        }
        if (!terminated.get()) {
            updateTaskState(finishedTasks, AnalysisState.FINISHED, "");
        }
    }

    protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
        if (terminated.get()) {
            return;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("execute internal sql: {}", stmtExecutor.getOriginStmt());
        }
        try {
            stmtExecutor.execute();
            QueryState queryState = stmtExecutor.getContext().getState();
            if (queryState.getStateType().equals(MysqlStateType.ERR)) {
                throw new RuntimeException(
                        "Failed to insert : " + stmtExecutor.getOriginStmt().originStmt + "Error msg: "
                                + queryState.getErrorMessage());
            }
        } finally {
            AuditLogHelper.logAuditLog(stmtExecutor.getContext(), stmtExecutor.getOriginStmt().originStmt,
                    stmtExecutor.getParsedStmt(), stmtExecutor.getQueryStatisticsForAuditLog(),
                    true);
        }
    }

    public void taskFailed(BaseAnalysisTask task, String reason) {
        boolean finished;
        synchronized (jobInfo) {
            if (!terminated.compareAndSet(false, true)) {
                return;
            }
            finished = jobInfo.state == AnalysisState.FINISHED;
            killed = !finished;
        }
        // The final status callback may have completed before the flushing worker unregisters.
        if (finished) {
            deregisterJob();
            return;
        }
        List<BaseAnalysisTask> snapshot;
        synchronized (this) {
            snapshot = new ArrayList<>(tasks);
        }
        try {
            // Cancel before taking any status/cleanup locks: a flush may be waiting on this SQL.
            RuntimeException failure = null;
            StmtExecutor executor = stmtExecutor;
            try {
                if (executor != null) {
                    executor.cancel(new Status(TStatusCode.CANCELLED, reason));
                }
            } catch (RuntimeException e) {
                failure = e;
            }
            for (BaseAnalysisTask runningTask : snapshot) {
                try {
                    runningTask.cancel();
                } catch (RuntimeException e) {
                    failure = combineFailures(failure, e);
                }
            }
            synchronized (jobInfo) {
                jobInfo.markFailed();
            }
            try {
                updateTaskState(snapshot, AnalysisState.FAILED, reason);
            } catch (RuntimeException e) {
                failure = combineFailures(failure, e);
            }
            if (failure != null) {
                throw failure;
            }
        } finally {
            deregisterJob();
        }
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

    synchronized void putPartitionUpdateRowsIfAbsent(AnalysisInfo info, long partitionId, long rows) {
        if (!terminated.get()) {
            info.partitionUpdateRows.putIfAbsent(partitionId, rows);
        }
    }

    public void cancel() {
        taskFailed(null, "analysis job cancelled");
    }

    public void deregisterJob() {
        try {
            analysisManager.removeJob(jobInfo.jobId);
        } finally {
            synchronized (this) {
                // Successful synchronous analyze consumes these maps in updateTableStats after
                // the workers return. Failure has no such consumer and must release them here.
                if (killed) {
                    for (BaseAnalysisTask task : tasks) {
                        if (task.info.jobColumns != null) {
                            task.info.jobColumns.clear();
                        }
                        if (task.info.partitionNames != null) {
                            task.info.partitionNames.clear();
                        }
                    }
                    if (jobInfo.partitionUpdateRows != null) {
                        jobInfo.partitionUpdateRows.clear();
                    }
                }
                tasks.clear();
                queryingTask.clear();
                queryFinished.clear();
                buf.clear();
            }
        }
    }

    boolean isTerminated() {
        return terminated.get();
    }

    public AnalysisInfo getJobInfo() {
        return jobInfo;
    }
}
