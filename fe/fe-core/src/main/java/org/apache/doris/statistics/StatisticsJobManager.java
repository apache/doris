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
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.statistics.StatisticsJob.JobState;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

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

    public Map<Long, StatisticsJob> getIdToStatisticsJob() {
        return idToStatisticsJob;
    }

    public void createStatisticsJob(AnalyzeStmt analyzeStmt) throws DdlException {
        // step0: init statistics job by analyzeStmt
        StatisticsJob statisticsJob = StatisticsJob.fromAnalyzeStmt(analyzeStmt);

        // step1: get statistical db&tbl to be analyzed
        long dbId = statisticsJob.getDbId();
        Set<Long> tableIds = statisticsJob.relatedTableId();

        // step2: check restrict
        this.checkRestrict(dbId, tableIds);

        // step3: check permission
        UserIdentity userInfo = analyzeStmt.getUserInfo();
        this.checkPermission(dbId, tableIds, userInfo);

        // step4: create it
        this.createStatisticsJob(statisticsJob);
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) {
        idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        try {
            Catalog.getCurrentCatalog().getStatisticsJobScheduler().addPendingJob(statisticsJob);
        } catch (IllegalStateException e) {
            LOG.warn("The pending statistics job is full. Please submit it again later.");
        }
    }

    /**
     * Rule1: The same table cannot have two unfinished statistics jobs
     * Rule2: The unfinished statistics job could not more then Config.max_statistics_job_num
     * Rule3: The job for external table is not supported
     */
    private void checkRestrict(long dbId, Set<Long> tableIds) throws DdlException {
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);

        // check table type
        for (Long tableId : tableIds) {
            Table table = db.getTableOrDdlException(tableId);
            if (table.getType() != Table.TableType.OLAP) {
                throw new DdlException("The external table(" + table.getName() + ") is not supported.");
            }
        }

        int unfinishedJobs = 0;
        StatisticsJobScheduler jobScheduler = Catalog.getCurrentCatalog().getStatisticsJobScheduler();
        Queue<StatisticsJob> statisticsJobs = jobScheduler.pendingJobQueue;

        // check table unfinished job
        for (StatisticsJob statisticsJob : statisticsJobs) {
            JobState jobState = statisticsJob.getJobState();
            List<Long> tableIdList = statisticsJob.getTableIdList();
            if (jobState == JobState.PENDING || jobState == JobState.SCHEDULING || jobState == JobState.RUNNING) {
                for (Long tableId : tableIds) {
                    if (tableIdList.contains(tableId)) {
                        throw new DdlException("The same table(id=" + tableId + ") cannot have two unfinished statistics jobs.");
                    }
                }
                unfinishedJobs++;
            }
        }

        // check the number of unfinished tasks
        if (unfinishedJobs > Config.cbo_max_statistics_job_num) {
            throw new DdlException("The unfinished statistics job could not more then Config.cbo_max_statistics_job_num.");
        }
    }

    private void checkPermission(long dbId, Set<Long> tableIds, UserIdentity userInfo) throws DdlException {
        PaloAuth auth = Catalog.getCurrentCatalog().getAuth();
        Database db = Catalog.getCurrentCatalog().getDbOrDdlException(dbId);

        // check the db permission
        String dbName = db.getFullName();
        boolean dbPermission = auth.checkDbPriv(userInfo, dbName, PrivPredicate.SELECT);
        if (!dbPermission) {
            throw new DdlException("You do not have permissions to analyze the database(" + dbName + ").");
        }

        // check the tables permission
        for (Long tableId : tableIds) {
            Table tbl = db.getTableOrDdlException(tableId);
            boolean tblPermission = auth.checkTblPriv(userInfo, dbName, tbl.getName(), PrivPredicate.SELECT);
            if (!tblPermission) {
                throw new DdlException("You do not have permissions to analyze the table(" + tbl.getName() + ").");
            }
        }
    }
}
