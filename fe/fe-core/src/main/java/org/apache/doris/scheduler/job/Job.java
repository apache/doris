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

package org.apache.doris.scheduler.job;

import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.constants.JobStatus;
import org.apache.doris.scheduler.executor.JobExecutor;

import lombok.Data;

import java.util.UUID;

/**
 * Job is the core of the scheduler module, which is used to store the Job information of the job module.
 * We can use the job to uniquely identify a Job.
 * The jobName is used to identify the job, which is not unique.
 * The jobStatus is used to identify the status of the Job, which is used to control the execution of the
 * job.
 */
@Data
public class Job {

    public Job(String jobName, Long intervalMilliSeconds, Long startTimeMs, Long endTimeMs,
               JobExecutor executor) {
        this.jobName = jobName;
        this.executor = executor;
        this.intervalMs = intervalMilliSeconds;
        this.startTimeMs = null == startTimeMs ? 0L : startTimeMs;
        this.endTimeMs = null == endTimeMs ? 0L : endTimeMs;
    }

    public Job() {
    }

    private Long jobId = UUID.randomUUID().getMostSignificantBits();

    private String jobName;

    /**
     * The status of the job, which is used to control the execution of the job.
     *
     * @see JobStatus
     */
    private JobStatus jobStatus = JobStatus.RUNNING;

    /**
     * The executor of the job.
     *
     * @see JobExecutor
     */
    private JobExecutor executor;

    private String user;

    private String errMsg;

    private IntervalUnit intervalUnit;

    private boolean isCycleJob = false;

    private Long intervalMs;

    private Long updateTime;

    private Long nextExecuteTimestamp;
    private Long startTimeMs = 0L;

    private Long endTimeMs = 0L;

    private Long firstExecuteTimestamp = 0L;

    private Long latestStartExecuteTimestamp = 0L;
    private Long latestCompleteExecuteTimestamp = 0L;

    public boolean isRunning() {
        return jobStatus == JobStatus.RUNNING;
    }

    public boolean isStopped() {
        return jobStatus == JobStatus.STOPPED;
    }

    public boolean isExpired(long nextExecuteTimestamp) {
        if (endTimeMs == 0L) {
            return false;
        }
        return nextExecuteTimestamp > endTimeMs;
    }

    public boolean isTaskTimeExceeded() {
        if (endTimeMs == 0L) {
            return false;
        }
        return System.currentTimeMillis() >= endTimeMs || nextExecuteTimestamp > endTimeMs;
    }

    public boolean isExpired() {
        if (endTimeMs == 0L) {
            return false;
        }
        return System.currentTimeMillis() >= endTimeMs;
    }

    public Long getExecuteTimestampAndGeneratorNext() {
        this.latestStartExecuteTimestamp = nextExecuteTimestamp;
        // todo The problem of delay should be considered. If it is greater than the ten-minute time window,
        //  should the task be lost or executed on a new time window?
        this.nextExecuteTimestamp = latestStartExecuteTimestamp + intervalMs;
        return nextExecuteTimestamp;
    }

    public void pause() {
        this.jobStatus = JobStatus.PAUSED;
    }

    public void pause(String errMsg) {
        this.jobStatus = JobStatus.PAUSED;
        this.errMsg = errMsg;
    }

    public void finish() {
        this.jobStatus = JobStatus.FINISHED;
    }

    public void resume() {
        this.jobStatus = JobStatus.RUNNING;
    }

    public void stop() {
        this.jobStatus = JobStatus.STOPPED;
    }

    public boolean checkJobParam() {
        if (startTimeMs != 0L && startTimeMs < System.currentTimeMillis()) {
            return false;
        }
        if (endTimeMs != 0L && endTimeMs < System.currentTimeMillis()) {
            return false;
        }
        
        if (isCycleJob && (intervalMs == null || intervalMs <= 0L)) {
            return false;
        }
        return null != executor;
    }
    
}
