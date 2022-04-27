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

import org.apache.doris.catalog.Catalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * The StatisticsTask belongs to one StatisticsJob.
 * A job may be split into multiple tasks but a task can only belong to one job.
 *
 * @granularityDesc, @categoryDesc, @statsTypeList
 * These three attributes indicate which statistics this task is responsible for collecting.
 * In general, a task will collect more than one @StatsType at the same time
 * while all of types belong to the same @granularityDesc and @categoryDesc.
 * For example: the task is responsible for collecting min, max, ndv of t1.c1 in partition p1.
 * @granularityDesc: StatsGranularity=partition
 */
public abstract class StatisticsTask implements Callable<StatisticsTaskResult> {
    protected static final Logger LOG = LogManager.getLogger(StatisticsTask.class);

    public enum TaskState {
        PENDING,
        RUNNING,
        FINISHED,
        FAILED
    }

    protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

    protected long id = Catalog.getCurrentCatalog().getNextId();
    protected long jobId;
    protected StatsGranularityDesc granularityDesc;
    protected StatsCategoryDesc categoryDesc;
    protected List<StatsType> statsTypeList;
    protected TaskState taskState = TaskState.PENDING;

    protected final long createTime = System.currentTimeMillis();
    protected long startTime = -1L;
    protected long finishTime = -1L;

    public StatisticsTask(long jobId,
                          StatsGranularityDesc granularityDesc,
                          StatsCategoryDesc categoryDesc,
                          List<StatsType> statsTypeList) {
        this.jobId = jobId;
        this.granularityDesc = granularityDesc;
        this.categoryDesc = categoryDesc;
        this.statsTypeList = statsTypeList;
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    protected void writeLock() {
        lock.writeLock().lock();
    }

    protected void writeUnlock() {
        lock.writeLock().unlock();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getJobId() {
        return jobId;
    }

    public StatsGranularityDesc getGranularityDesc() {
        return granularityDesc;
    }

    public StatsCategoryDesc getCategoryDesc() {
        return categoryDesc;
    }

    public List<StatsType> getStatsTypeList() {
        return statsTypeList;
    }

    public TaskState getTaskState() {
        return taskState;
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

    /**
     * Different statistics implement different collection methods.
     *
     * @return true if this task is finished, false otherwise
     * @throws Exception
     */
    @Override
    public abstract StatisticsTaskResult call() throws Exception;

    public void updateTaskState(TaskState newState) throws IllegalStateException{
        LOG.info("To change statistics task(id={}) state from {} to {}", id, taskState, newState);
        writeLock();

        try {
            // PENDING -> RUNNING/FAILED
            if (taskState == TaskState.PENDING) {
                if (newState == TaskState.RUNNING) {
                    taskState = newState;
                    // task start running, set start time
                    startTime = System.currentTimeMillis();
                    LOG.info("Statistics task(id={}) state changed from {} to {}", id, taskState, newState);
                } else if (newState == TaskState.FAILED) {
                    taskState = newState;
                    LOG.info("Statistics task(id={}) state changed from {} to {}", id, taskState, newState);
                } else {
                    LOG.info("Invalid task(id={}) state transition from {} to {}", id, taskState, newState);
                    throw new IllegalStateException("Invalid task state transition from PENDING to " + newState);
                }
                return;
            }

            // RUNNING -> FINISHED/FAILED
            if (taskState == TaskState.RUNNING) {
                if (newState == TaskState.FINISHED) {
                    // set finish time
                    finishTime = System.currentTimeMillis();
                    taskState = newState;
                    LOG.info("Statistics task(id={}) state changed from {} to {}", id, taskState, newState);
                } else if (newState == TaskState.FAILED) {
                    taskState = newState;
                    LOG.info("Statistics task(id={}) state changed from {} to {}", id, taskState, newState);
                } else {
                    LOG.info("Invalid task(id={}) state transition from {} to {}", id, taskState, newState);
                    throw new IllegalStateException("Invalid task state transition from RUNNING to " + newState);
                }
            }

            LOG.info("Invalid task(id={}) state transition from {} to {}", id, taskState, newState);
            throw new IllegalStateException("Invalid task state transition from " + taskState + " to " + newState);
        } finally {
            writeUnlock();
            LOG.info("Statistics task(id={}) current state is {}", id, taskState);
        }
    }
}
