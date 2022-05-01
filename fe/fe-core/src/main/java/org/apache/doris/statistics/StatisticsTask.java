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
import org.apache.doris.common.DdlException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.Callable;

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

    // please retain job lock firstly
    public void updateTaskState(TaskState newState) throws DdlException {
        LOG.info("To change statistics task(id={}) state from {} to {}", id, taskState, newState);
        String errorMsg = "Invalid statistics task state transition from ";

        // PENDING -> RUNNING/FAILED
        if (taskState == TaskState.PENDING) {
            switch (newState) {
                case RUNNING:
                    startTime = System.currentTimeMillis();
                    break;
                case FAILED:
                    finishTime = System.currentTimeMillis();
                    break;
                default:
                    throw new DdlException(errorMsg + taskState + " to " + newState);
            }
        }
        // RUNNING -> FINISHED/FAILED
        else if (taskState == TaskState.RUNNING) {
            switch (newState) {
                case FINISHED:
                case FAILED:
                    finishTime = System.currentTimeMillis();
                    break;
                default:
                    throw new DdlException(errorMsg + taskState + " to " + newState);
            }
        }
        // unsupported state transition
        else {
            throw new DdlException(errorMsg + taskState + " to " + newState);
        }

        LOG.info("Statistics job(id={}) state changed from {} to {}", id, taskState, newState);
        taskState = newState;
    }
}
