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

package org.apache.doris.statistics;

import org.apache.doris.catalog.Env;
import org.apache.doris.qe.AuditLogHelper;
import org.apache.doris.qe.AutoCloseConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.QueryState.MysqlStateType;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.util.StatisticsUtil;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;

public class AnalysisJob {

    public static final Logger LOG = LogManager.getLogger(AnalysisJob.class);

    protected Map<Long, BaseAnalysisTask> idToTask = new ConcurrentHashMap<>();

    protected Set<BaseAnalysisTask> queryingTask;

    protected Set<BaseAnalysisTask> queryFinished;

    protected List<ColStatsData> buf;

    protected StmtExecutor stmtExecutor;

    protected boolean killed;

    protected long start;

    protected AnalysisInfo jobInfo;

    protected AnalysisManager analysisManager;

    public AnalysisJob(AnalysisInfo jobInfo, Collection<? extends BaseAnalysisTask> queryingTask) {
        for (BaseAnalysisTask task : queryingTask) {
            task.job = this;
            idToTask.put(task.info.taskId, task);
        }
        this.queryingTask = Collections.synchronizedSet(new HashSet<>(queryingTask));
        this.queryFinished = Collections.synchronizedSet(new HashSet<>());
        this.buf = new ArrayList<>();
        start = System.currentTimeMillis();
        this.jobInfo = jobInfo;
        this.analysisManager = Env.getCurrentEnv().getAnalysisManager();
    }

    public synchronized void appendBuf(BaseAnalysisTask task, List<ColStatsData> statsData) {
        queryingTask.remove(task);
        buf.addAll(statsData);
        queryFinished.add(task);
        markOneTaskDone();
    }

    public synchronized void rowCountDone(BaseAnalysisTask task) {
        queryingTask.remove(task);
        queryFinished.add(task);
        markOneTaskDone();
    }

    protected void markOneTaskDone() {
        if (queryingTask.isEmpty()) {
            try {
                writeBuf();
            } finally {
                deregisterJob();
            }
        } else if (buf.size() >= StatisticsUtil.getInsertMergeCount()) {
            writeBuf();
        }
    }

    // CHECKSTYLE OFF
    // fallthrough here is expected
    public void updateTaskState(AnalysisState state, String msg) {
        switch (state) {
            case FAILED:
                for (BaseAnalysisTask task : queryingTask) {
                    task.info.state = state;
                    task.info.message = msg;
                    analysisManager.logCreateAnalysisTask(task.info);
                    task.cancel();
                }
                killed = true;
            case FINISHED:
                for (BaseAnalysisTask task : queryFinished) {
                    task.info.state = state;
                    task.info.message = msg;
                    analysisManager.logCreateAnalysisTask(task.info);
                }
            default:
                // DO NOTHING
        }
    }

    protected void writeBuf() {
        if (killed) {
            return;
        }
        // buf could be empty when nothing need to do,r for example user submit an analysis task for table with no data
        // change
        if (!buf.isEmpty())  {
            String insertStmt = "INSERT INTO " + StatisticConstants.FULL_QUALIFIED_STATS_TBL_NAME + " VALUES ";
            StringJoiner values = new StringJoiner(",");
            for (ColStatsData data : buf) {
                values.add(data.toSQL(true));
            }
            insertStmt += values.toString();
            try (AutoCloseConnectContext r = StatisticsUtil.buildConnectContext(false)) {
                stmtExecutor = new StmtExecutor(r.connectContext, insertStmt);
                executeWithExceptionOnFail(stmtExecutor);
            } catch (Exception t) {
                throw new RuntimeException("Failed to analyze: " + t.getMessage());
            }
        }
        updateTaskState(AnalysisState.FINISHED, "");
        syncLoadStats();
        queryFinished.clear();
        buf.clear();
    }

    protected void executeWithExceptionOnFail(StmtExecutor stmtExecutor) throws Exception {
        if (killed) {
            return;
        }
        LOG.debug("execute internal sql: {}", stmtExecutor.getOriginStmt());
        try {
            stmtExecutor.execute();
            QueryState queryState = stmtExecutor.getContext().getState();
            if (queryState.getStateType().equals(MysqlStateType.ERR)) {
                throw new RuntimeException(
                        "Failed to insert : " + stmtExecutor.getOriginStmt().originStmt + "Error msg: "
                                + queryState.getErrorMessage());
            }
        } finally {
            AuditLogHelper.logAuditLog(stmtExecutor.getContext(), stmtExecutor.getOriginStmt().toString(),
                    stmtExecutor.getParsedStmt(), stmtExecutor.getQueryStatisticsForAuditLog(),
                    true);
        }
    }

    public void taskFailed(BaseAnalysisTask task, String reason) {
        try {
            updateTaskState(AnalysisState.FAILED, reason);
            cancel();
        } finally {
            deregisterJob();
        }
    }

    public void cancel() {
        for (BaseAnalysisTask task : queryingTask) {
            task.cancel();
        }
    }

    public void deregisterJob() {
        analysisManager.removeJob(jobInfo.jobId);
        for (BaseAnalysisTask task : queryingTask) {
            task.info.colToPartitions.clear();
            if (task.info.partitionNames != null) {
                task.info.partitionNames.clear();
            }
        }
        for (BaseAnalysisTask task : queryFinished) {
            task.info.colToPartitions.clear();
            if (task.info.partitionNames != null) {
                task.info.partitionNames.clear();
            }
        }
        if (queryingTask.isEmpty()) {
            jobInfo.state = AnalysisState.FINISHED;
            analysisManager.updateTableStats(jobInfo);
        } else {
            jobInfo.state = AnalysisState.FAILED;
        }
        jobInfo.endTime = System.currentTimeMillis();
        if (jobInfo.jobType.equals(AnalysisInfo.JobType.MANUAL)) {
            analysisManager.logCreateAnalysisJob(jobInfo);
        } else {
            analysisManager.autoJobs.offer(jobInfo);
        }
    }

    protected void syncLoadStats() {
        long tblId = jobInfo.tblId;
        for (BaseAnalysisTask task : queryFinished) {
            if (task.info.externalTableLevelTask) {
                continue;
            }
            String colName = task.col.getName();
            if (!Env.getCurrentEnv().getStatisticsCache().syncLoadColStats(tblId, -1, colName)) {
                LOG.warn("Failed to load col stats after analyze");
                analysisManager.removeColStatsStatus(tblId, colName);
            }
        }
    }

    public void kill() {
        for (BaseAnalysisTask task : queryingTask) {
            try {
                task.cancel();
            } catch (Exception e) {
                LOG.warn("Exception when cancel task: {}", task.info, e);
            }
        }
    }

    public Collection<BaseAnalysisTask> tasks() {
        return idToTask.values();
    }

}
