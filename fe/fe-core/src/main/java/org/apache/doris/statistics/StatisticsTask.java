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
public class StatisticsTask implements Callable<StatisticsTaskResult> {
    protected static final Logger LOG = LogManager.getLogger(StatisticsTask.class);

    public enum TaskState {
        PENDING,
        RUNNING,
        FINISHED,
        FAILED
    }

    protected long id = -1;
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
        return this.id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getJobId() {
        return this.jobId;
    }

    public StatsGranularityDesc getGranularityDesc() {
        return this.granularityDesc;
    }

    public StatsCategoryDesc getCategoryDesc() {
        return this.categoryDesc;
    }

    public List<StatsType> getStatsTypeList() {
        return this.statsTypeList;
    }

    public TaskState getTaskState() {
        return this.taskState;
    }

    public void setTaskState(TaskState taskState) {
        this.taskState = taskState;
    }

    public long getCreateTime() {
        return this.createTime;
    }

    public long getStartTime() {
        return this.startTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public long getFinishTime() {
        return this.finishTime;
    }

    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    @Override
    public StatisticsTaskResult call() throws Exception {
        LOG.warn("execute invalid statistics task.");
        return null;
    }
}
