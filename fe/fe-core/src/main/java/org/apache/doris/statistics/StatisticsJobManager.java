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

import org.apache.doris.analysis.AnalyzeStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * For unified management of statistics job,
 * including job addition, cancellation, scheduling, etc.
 */
public class StatisticsJobManager {
    private static final Logger LOG = LogManager.getLogger(StatisticsJobManager.class);

    /**
     * save statistics job status information
     */
    private final Map<Long, StatisticsJob> idToStatisticsJob = Maps.newConcurrentMap();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    public void readLock() {
        this.lock.readLock().lock();
    }

    public void readUnlock() {
        this.lock.readLock().unlock();
    }

    private void writeLock() {
        this.lock.writeLock().lock();
    }

    private void writeUnlock() {
        this.lock.writeLock().unlock();
    }

    public Map<Long, StatisticsJob> getIdToStatisticsJob() {
        return this.idToStatisticsJob;
    }

    public void createStatisticsJob(AnalyzeStmt analyzeStmt) throws UserException {
        // step1: init statistics job by analyzeStmt
        StatisticsJob statisticsJob = StatisticsJob.fromAnalyzeStmt(analyzeStmt);
        writeLock();
        try {
            // step2: check restrict
            this.checkRestrict(analyzeStmt.getDb(), statisticsJob.getTblIds());
            // step3: create it
            this.createStatisticsJob(statisticsJob);
        } finally {
            writeUnlock();
        }
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) throws DdlException {
        // assign the id when the job is ready to run
        statisticsJob.setId(Catalog.getCurrentCatalog().getNextId());
        this.idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        try {
            Catalog.getCurrentCatalog().getStatisticsJobScheduler().addPendingJob(statisticsJob);
        } catch (IllegalStateException e) {
            LOG.info("The pending statistics job is full. Please submit it again later.");
            throw new DdlException("The pending statistics job is full, Please submit it again later.");
        }
    }

    /**
     * The statistical job has the following restrict:
     * - Rule1: The same table cannot have two unfinished statistics jobs
     * - Rule2: The unfinished statistics job could not more then Config.max_statistics_job_num
     * - Rule3: The job for external table is not supported
     */
    private void checkRestrict(Database db, Set<Long> tableIds) throws AnalysisException {
        // check table type
        for (Long tableId : tableIds) {
            Table table = db.getTableOrAnalysisException(tableId);
            if (table.getType() != Table.TableType.OLAP) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, db.getFullName(), table.getName(), "ANALYZE");
            }
        }

        int unfinishedJobs = 0;

        // check table unfinished job
        for (StatisticsJob statisticsJob : this.idToStatisticsJob.values()) {
            StatisticsJob.JobState jobState = statisticsJob.getJobState();
            Set<Long> tblIds = statisticsJob.getTblIds();
            if (jobState == StatisticsJob.JobState.PENDING
                    || jobState == StatisticsJob.JobState.SCHEDULING
                    || jobState == StatisticsJob.JobState.RUNNING) {
                for (Long tableId : tableIds) {
                    if (tblIds.contains(tableId)) {
                        throw new AnalysisException("The table(id=" + tableId + ") have unfinished statistics jobs");
                    }
                }
                unfinishedJobs++;
            }
        }

        // check the number of unfinished tasks
        if (unfinishedJobs > Config.cbo_max_statistics_job_num) {
            throw new AnalysisException("The unfinished statistics job could not more than cbo_max_statistics_job_num: " +
                    Config.cbo_max_statistics_job_num);
        }
    }

    public void alterStatisticsJobInfo(Long jobId, Long taskId, String errorMsg)  {
        StatisticsJob statisticsJob = this.idToStatisticsJob.get(jobId);
        if (statisticsJob == null) {
            return;
        }
        writeLock();
        try {
            List<StatisticsTask> tasks = statisticsJob.getTasks();
            for (StatisticsTask task : tasks) {
                if (taskId == task.getId()) {
                    if (Strings.isNullOrEmpty(errorMsg)) {
                        int progress = statisticsJob.getProgress() + 1;
                        statisticsJob.setProgress(progress);
                        if (progress == statisticsJob.getTasks().size()) {
                            statisticsJob.setFinishTime(System.currentTimeMillis());
                            statisticsJob.setJobState(StatisticsJob.JobState.FINISHED);
                        }
                        task.setFinishTime(System.currentTimeMillis());
                        task.setTaskState(StatisticsTask.TaskState.FINISHED);
                    } else {
                        statisticsJob.getErrorMsgs().add(errorMsg);
                        task.setTaskState(StatisticsTask.TaskState.FAILED);
                        statisticsJob.setJobState(StatisticsJob.JobState.FAILED);
                    }
                    return;
                }
            }
        } finally {
            writeUnlock();
        }
    }
}
