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
import org.apache.doris.analysis.ShowAnalyzeStmt;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.ListComparator;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

    public void createStatisticsJob(AnalyzeStmt analyzeStmt) throws UserException {
        // The current statistics are only used for CBO test,
        // and are not available to users. (work in progress)
        // TODO(wzt): Further tests are needed
        boolean enableCboStatistics = ConnectContext.get()
                .getSessionVariable().getEnableCboStatistics();
        if (enableCboStatistics) {
            // step1: init statistics job by analyzeStmt
            StatisticsJob statisticsJob = StatisticsJob.fromAnalyzeStmt(analyzeStmt);
            synchronized (this) {
                // step2: check restrict
                checkRestrict(analyzeStmt.getDbId(), statisticsJob.getTblIds());
                // step3: create it
                createStatisticsJob(statisticsJob);
            }
        } else {
            throw new UserException("Statistics are not yet stable, if you want to enable statistics,"
                    + " use 'set enable_cbo_statistics=true' to enable it.");
        }
    }

    public void createStatisticsJob(StatisticsJob statisticsJob) throws DdlException {
        idToStatisticsJob.put(statisticsJob.getId(), statisticsJob);
        try {
            Env.getCurrentEnv().getStatisticsJobScheduler().addPendingJob(statisticsJob);
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
        Database db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbId);
        db.readLock();
        try {
            // check table type
            for (Long tableId : tableIds) {
                Table table = db.getTableOrAnalysisException(tableId);
                if (table.getType() != Table.TableType.OLAP) {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_NOT_OLAP_TABLE, db.getFullName(),
                            table.getName(), "ANALYZE");
                }
            }
        } finally {
            db.readUnlock();
        }

        int unfinishedJobs = 0;

        // check table unfinished job
        for (StatisticsJob statisticsJob : idToStatisticsJob.values()) {
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
            throw new AnalysisException("The unfinished statistics job could not more than cbo_max_statistics_job_num: "
                    + Config.cbo_max_statistics_job_num);
        }
    }

    public List<List<String>> getAnalyzeJobInfos(ShowAnalyzeStmt showStmt) throws AnalysisException {
        List<List<Comparable>> results = Lists.newArrayList();

        String stateValue = showStmt.getStateValue();
        StatisticsJob.JobState jobState = null;
        if (!Strings.isNullOrEmpty(stateValue)) {
            jobState = StatisticsJob.JobState.valueOf(stateValue);
        }

        // step 1: get job infos
        List<Long> jobIds = showStmt.getJobIds();
        if (jobIds != null && !jobIds.isEmpty()) {
            for (Long jobId : jobIds) {
                StatisticsJob statisticsJob = idToStatisticsJob.get(jobId);
                if (statisticsJob == null) {
                    throw new AnalysisException("No such job id: " + jobId);
                }
                if (jobState == null || jobState == statisticsJob.getJobState()) {
                    List<Comparable> showInfo = statisticsJob.getShowInfo(null);
                    if (showInfo == null || showInfo.isEmpty()) {
                        continue;
                    }
                    results.add(showInfo);
                }
            }
        } else {
            long dbId = showStmt.getDbId();
            Set<Long> tblIds = showStmt.getTblIds();
            for (StatisticsJob statisticsJob : idToStatisticsJob.values()) {
                long jobDbId = statisticsJob.getDbId();
                if (jobDbId == dbId) {
                    // check the state
                    if (jobState == null || jobState == statisticsJob.getJobState()) {
                        Set<Long> jobTblIds = statisticsJob.getTblIds();
                        // get the intersection of two sets
                        Set<Long> set = Sets.newHashSet();
                        set.addAll(jobTblIds);
                        set.retainAll(tblIds);
                        for (long tblId : set) {
                            List<Comparable> showInfo = statisticsJob.getShowInfo(tblId);
                            if (showInfo == null || showInfo.isEmpty()) {
                                continue;
                            }
                            results.add(showInfo);
                        }
                    }
                }
            }
        }

        // step2: order the result
        ListComparator<List<Comparable>> comparator;
        List<OrderByPair> orderByPairs = showStmt.getOrderByPairs();
        if (orderByPairs == null) {
            // sort by id asc
            comparator = new ListComparator<>(0);
        } else {
            OrderByPair[] orderByPairArr = new OrderByPair[orderByPairs.size()];
            comparator = new ListComparator<>(orderByPairs.toArray(orderByPairArr));
        }
        results.sort(comparator);

        // step3: filter by limit
        long limit = showStmt.getLimit();
        long offset = showStmt.getOffset() == -1L ? 0 : showStmt.getOffset();
        if (offset >= results.size()) {
            results = Collections.emptyList();
        } else if (limit != -1L) {
            if ((limit + offset) >= results.size()) {
                results = results.subList((int) offset, results.size());
            } else {
                results = results.subList((int) offset, (int) (limit + offset));
            }
        }

        // step4: convert to result and return it
        List<List<String>> rows = Lists.newArrayList();
        for (List<Comparable> result : results) {
            List<String> row = result.stream().map(Object::toString)
                    .collect(Collectors.toList());
            rows.add(row);
        }

        return rows;
    }
}
