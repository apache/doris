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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/***
 * Used to store statistics job info,
 * including job status, progress, etc.
 */
public class StatisticsJob {
    private static final Logger LOG = LogManager.getLogger(StatisticsJob.class);

    public enum JobState {
        PENDING,
        SCHEDULING,
        RUNNING,
        FINISHED,
        FAILED,
        CANCELLED
    }

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    private final long id = Catalog.getCurrentCatalog().getNextId();

    /**
     * to be collected database stats.
     */
    private final long dbId;

    /**
     * to be collected table stats.
     */
    private final Set<Long> tblIds;

    /**
     * to be collected column stats.
     */
    private final Map<Long, List<String>> tableIdToColumnName;

    private final Map<String, String> properties;

    /**
     * to be executed tasks.
     */
    private final List<StatisticsTask> tasks = Lists.newArrayList();

    private JobState jobState = JobState.PENDING;
    private final List<String> errorMsgs = Lists.newArrayList();

    private final long createTime = System.currentTimeMillis();
    private long startTime = -1L;
    private long finishTime = -1L;
    private int progress = 0;

    public StatisticsJob(Long dbId,
                         Set<Long> tblIds,
                         Map<Long, List<String>> tableIdToColumnName,
                         Map<String, String> properties) {
        this.dbId = dbId;
        this.tblIds = tblIds;
        this.tableIdToColumnName = tableIdToColumnName;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

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

    public long getId() {
        return id;
    }

    public long getDbId() {
        return dbId;
    }

    public Set<Long> getTblIds() {
        return tblIds;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        return tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public List<StatisticsTask> getTasks() {
        return tasks;
    }

    public List<String> getErrorMsgs() {
        return errorMsgs;
    }

    public JobState getJobState() {
        return jobState;
    }

    public long getCreateTime() {
        return createTime;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getFinishTime() {
        return finishTime;
    }

    public int getProgress() {
        return progress;
    }

    public void updateJobState(JobState newState) throws DdlException {
        LOG.info("To change statistics job(id={}) state from {} to {}", id, jobState, newState);
        writeLock();
        JobState fromState = jobState;
        try {
            unprotectedUpdateJobState(newState);
        } catch (DdlException e) {
            LOG.warn(e.getMessage(), e);
            throw e;
        } finally {
            writeUnlock();
        }
        LOG.info("Statistics job(id={}) state changed from {} to {}", id, fromState, jobState);
    }

    private void unprotectedUpdateJobState(JobState newState) throws DdlException {
        // PENDING -> PENDING/SCHEDULING/FAILED/CANCELLED
        if (jobState == JobState.PENDING) {
            switch (newState) {
                case PENDING:
                case SCHEDULING:
                    break;
                case FAILED:
                case CANCELLED:
                    finishTime = System.currentTimeMillis();
                    break;
                default:
                    throw new DdlException("Invalid job state transition from " + jobState + " to " + newState);
            }
        }
        // SCHEDULING -> RUNNING/FAILED/CANCELLED
        else if (jobState == JobState.SCHEDULING) {
            switch (newState) {
                case RUNNING:
                    startTime = System.currentTimeMillis();
                    break;
                case FAILED:
                case CANCELLED:
                    finishTime = System.currentTimeMillis();
                    break;
                default:
                    throw new DdlException("Invalid job state transition from " + jobState + " to " + newState);
            }
        }
        // RUNNING -> FINISHED/FAILED/CANCELLED
        else if (jobState == JobState.RUNNING) {
            switch (newState) {
                case FINISHED:
                case FAILED:
                case CANCELLED:
                    // set finish time
                    finishTime = System.currentTimeMillis();
                    break;
                default:
                    throw new DdlException("Invalid job state transition from " + jobState + " to " + newState);
            }
        } else {
            // TODO
            throw new DdlException("Invalid job state transition from " + jobState + " to " + newState);
        }
        jobState = newState;
    }

    public void updateJobInfoByTaskId(Long taskId, String errorMsg) throws DdlException {
        writeLock();
        try {
            for (StatisticsTask task : tasks) {
                if (taskId == task.getId()) {
                    if (Strings.isNullOrEmpty(errorMsg)) {
                        progress += 1;
                        if (progress == tasks.size()) {
                            unprotectedUpdateJobState(StatisticsJob.JobState.FINISHED);
                        }
                        task.updateTaskState(StatisticsTask.TaskState.FINISHED);
                    } else {
                        errorMsgs.add(errorMsg);
                        task.updateTaskState(StatisticsTask.TaskState.FAILED);
                        unprotectedUpdateJobState(StatisticsJob.JobState.FAILED);
                    }
                    return;
                }
            }
        } finally {
            writeUnlock();
        }
    }

    /**
     * get statisticsJob from analyzeStmt.
     * AnalyzeStmt: analyze t1(c1,c2,c3)
     * tableId: [t1]
     * tableIdToColumnName <t1, [c1,c2,c3]>
     */
    public static StatisticsJob fromAnalyzeStmt(AnalyzeStmt analyzeStmt) throws AnalysisException {
        long dbId = analyzeStmt.getDbId();
        Map<Long, List<String>> tableIdToColumnName = analyzeStmt.getTableIdToColumnName();
        Set<Long> tblIds = analyzeStmt.getTblIds();
        Map<String, String> properties = analyzeStmt.getProperties();
        return new StatisticsJob(dbId, tblIds, tableIdToColumnName, properties);
    }
}
