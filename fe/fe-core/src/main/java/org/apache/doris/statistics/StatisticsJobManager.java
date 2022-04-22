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

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
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
            this.checkRestrict(analyzeStmt.getDbId(), statisticsJob.getTblIds());
            // step3: create it
            this.createStatisticsJob(statisticsJob);
        } finally {
            writeUnlock();
        }
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) throws DdlException {
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
     * - Rule2: The unfinished statistics job could not more than Config.max_statistics_job_num
     * - Rule3: The job for external table is not supported
     */
    private void checkRestrict(long dbId, Set<Long> tableIds) throws AnalysisException {
        Database db = Catalog.getCurrentCatalog().getDbOrAnalysisException(dbId);
        db.readLock();
        try {
            // check table type
            for (Long tableId : tableIds) {
                Table table = db.getTableOrAnalysisException(tableId);
                if (table.getType() != Table.TableType.OLAP) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, db.getFullName(), table.getName(), "ANALYZE");
                }
            }
        } finally {
            db.readUnlock();
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
}
