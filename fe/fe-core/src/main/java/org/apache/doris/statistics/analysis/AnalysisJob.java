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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.StatisticConstants;
import org.apache.doris.statistics.repository.ColStatsData;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot;
import org.apache.doris.statistics.sampling.IndexSamplingSnapshot.ReportState;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.base.Suppliers;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

public class AnalysisJob {

    public static final Logger LOG = LogManager.getLogger(AnalysisJob.class);

    private final long samplingSeed = ThreadLocalRandom.current().nextLong() & Long.MAX_VALUE;
    private final Map<Long, Long> reportedRows = new ConcurrentHashMap<>();
    private final Map<Long, Supplier<Boolean>> keyRangeDecisions = new ConcurrentHashMap<>();
    private final Map<Long, List<String>> sampleColumns = new HashMap<>();
    private final Map<Long, Long> indexRows = new ConcurrentHashMap<>();
    private final Map<Long, ReportState> reportStates = new ConcurrentHashMap<>();
    private final Map<Long, Supplier<IndexSamplingSnapshot>> samplingFrames = new ConcurrentHashMap<>();

    long reportedIndexRows(OlapTable table, long indexId) {
        return reportedRows.computeIfAbsent(indexId, id -> IndexSamplingSnapshot.reportedIndexRows(table, id, false));
    }

    void requireIndexReports(OlapTable table, long indexId) {
        ReportState state = reportStates.computeIfAbsent(indexId, id -> IndexSamplingSnapshot.reportState(table, id));
        if (state == ReportState.PENDING || (state == ReportState.READY && reportedIndexRows(table, indexId) <= 0)) {
            throw new AnalyzeSkipException("Written partitions have no usable row count for index " + indexId);
        }
    }

    boolean useKeyRange(long indexId, Supplier<Boolean> eligible) {
        return keyRangeDecisions.computeIfAbsent(indexId, ignored -> Suppliers.memoize(eligible::get)).get();
    }

    List<String> sampleColumns(long indexId, long baseIndexId) {
        List<String> columns = new ArrayList<>(sampleColumns.getOrDefault(indexId, List.of()));
        if (indexId == baseIndexId) {
            columns.addAll(sampleColumns.getOrDefault(-1L, List.of()));
        }
        return columns;
    }

    long samplingSeed(long seek) {
        return seek >= 0 ? seek : samplingSeed;
    }

    long freezeIndexRows(long indexId, LongSupplier rows) {
        return indexRows.computeIfAbsent(indexId, ignored -> rows.getAsLong());
    }

    IndexSamplingSnapshot samplingFrame(OlapTable table, long indexId) {
        return samplingFrames.computeIfAbsent(indexId,
                id -> Suppliers.memoize(() -> IndexSamplingSnapshot.read(table, id))).get();
    }

    private final ColumnMinMaxBatchCollector minMaxCollector;

    protected Set<BaseAnalysisTask> queryingTask;

    protected Set<BaseAnalysisTask> queryFinished;

    protected List<ColStatsData> buf;

    protected StmtExecutor stmtExecutor;

    private final AtomicBoolean killed = new AtomicBoolean();

    protected long start;

    protected AnalysisInfo jobInfo;

    protected AnalysisManager analysisManager;

    public AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask) {
        minMaxCollector = new ColumnMinMaxBatchCollector(queryingTask, StatisticsUtil.getMinMaxBatchColumns());
        for (BaseAnalysisTask task : queryingTask) {
            task.job = this;
            if (task instanceof OlapAnalysisTask && task.tableSample != null && task.info != null) {
                sampleColumns.computeIfAbsent(task.info.indexId, ignored -> new ArrayList<>()).add(task.info.colName);
            }
        }
        this.queryingTask = Collections.synchronizedSet(new HashSet<>(queryingTask));
        this.queryFinished = Collections.synchronizedSet(new HashSet<>());
        this.buf = new ArrayList<>();
        start = System.currentTimeMillis();
        this.jobInfo = jobInfo;
        this.analysisManager = Env.getCurrentEnv().getAnalysisManager();
    }

    public synchronized void taskDoneWithoutData(BaseAnalysisTask task) {
        queryingTask.remove(task);
        queryFinished.add(task);
        markOneTaskDone();
    }

    public synchronized void appendBuf(BaseAnalysisTask task, List<ColStatsData> statsData) {
        queryingTask.remove(task);
        buf.addAll(statsData);
        queryFinished.add(task);
        markOneTaskDone();
    }

    protected void markOneTaskDone() {
        if (queryingTask.isEmpty()) {
            try {
                flushBuffer();
            } finally {
                deregisterJob();
            }
        } else if (buf.size() >= StatisticsUtil.getInsertMergeCount()) {
            flushBuffer();
        }
    }

    // CHECKSTYLE OFF
    // fallthrough here is expected
    public void updateTaskState(AnalysisState state, String msg) {
        long time = System.currentTimeMillis();
        switch (state) {
            case FAILED:
                markKilled();
                for (BaseAnalysisTask task : new ArrayList<>(queryingTask)) {
                    analysisManager.updateTaskStatus(task.info, state, msg, time);
                    task.cancel();
                }
            case FINISHED:
                for (BaseAnalysisTask task : new ArrayList<>(queryFinished)) {
                    // When flushBuffer passes an empty msg, fall back to the
                    // task's own info.message. This propagates a skip reason
                    // previously stashed by BaseAnalysisTask.handleSkip into
                    // the single FINISHED update for this task, so job.message
                    // accumulation in AnalysisManager sees it and SHOW ANALYZE
                    // surfaces the skip reason at the job level.
                    String taskMsg = msg;
                    if ((taskMsg == null || taskMsg.isEmpty()) && task.info != null) {
                        taskMsg = task.info.message;
                    }
                    if (taskMsg == null) {
                        taskMsg = "";
                    }
                    analysisManager.updateTaskStatus(task.info, state, taskMsg, time);
                }
            default:
                // DO NOTHING
        }
    }

    protected void flushBuffer() {
        if (isKilled()) {
            return;
        }
        // buf could be empty when nothing need to do,r for example user submit an analysis task for table with no data
        // change
        if (!buf.isEmpty())  {
            String insertStmt = StatisticConstants.INSERT_INTO_COLUMN_STATS_PREFIX;
            StringJoiner values = new StringJoiner(",");
            for (ColStatsData data : buf) {
                values.add(data.toSQL(true));
            }
            insertStmt += values.toString();
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
                ConnectContext context = r.connectContext;
                context.setSqlHash(DigestUtils.md5Hex(insertStmt));
                stmtExecutor = new StmtExecutor(context, insertStmt);
                executeWithExceptionOnFail(stmtExecutor);
            } catch (Exception t) {
                throw new RuntimeException("Failed to analyze: " + t.getMessage());
            }
        }
        updateTaskState(AnalysisState.FINISHED, "");
        queryFinished.clear();
        buf.clear();
    }

    protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
        if (isKilled()) {
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
        // A cancelled shared query can fail several wrappers at once. Only one reports the job failure.
        if (!killed.compareAndSet(false, true)) {
            return;
        }
        try {
            updateTaskState(AnalysisState.FAILED, reason);
            cancel();
        } finally {
            // All task threads have been marked killed by cancel(), so the guarded
            // partitionUpdateRows writers have stopped. Clear the shared map here, after
            // the cancels (the terminal-state clear inside updateTaskStatus runs while
            // sibling threads may still be executing and can miss in-flight writes), so
            // the job and all its task records release the map at once.
            if (jobInfo.partitionUpdateRows != null) {
                jobInfo.partitionUpdateRows.clear();
            }
            deregisterJob();
        }
    }

    void markKilled() {
        killed.set(true);
        minMaxCollector.cancelAll();
    }

    public boolean isKilled() {
        return killed.get();
    }

    public void cancel() {
        markKilled();
        for (BaseAnalysisTask task : new ArrayList<>(queryingTask)) {
            task.cancel();
        }
    }

    public void deregisterJob() {
        analysisManager.removeJob(jobInfo.jobId);
        for (BaseAnalysisTask task : new ArrayList<>(queryingTask)) {
            task.info.jobColumns.clear();
            if (task.info.partitionNames != null) {
                task.info.partitionNames.clear();
            }
        }
        for (BaseAnalysisTask task : new ArrayList<>(queryFinished)) {
            task.info.jobColumns.clear();
            if (task.info.partitionNames != null) {
                task.info.partitionNames.clear();
            }
        }
    }

    public AnalysisInfo getJobInfo() {
        return jobInfo;
    }
}
