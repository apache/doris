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

    public Job(String jobName, Long intervalMilliSeconds, Long startTimestamp, Long endTimestamp,
               JobExecutor executor) {
        this.jobName = jobName;
        this.executor = executor;
        this.intervalMilliSeconds = intervalMilliSeconds;
        this.startTimestamp = null == startTimestamp ? 0L : startTimestamp;
        this.endTimestamp = null == endTimestamp ? 0L : endTimestamp;
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

    private Long intervalMilliSeconds;

    private Long updateTime;

    private Long nextExecuteTimestamp;
    private Long startTimestamp = 0L;

    private Long endTimestamp = 0L;

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
        if (endTimestamp == 0L) {
            return false;
        }
        return nextExecuteTimestamp > endTimestamp;
    }

    public boolean isTaskTimeExceeded() {
        if (endTimestamp == 0L) {
            return false;
        }
        return System.currentTimeMillis() >= endTimestamp || nextExecuteTimestamp > endTimestamp;
    }

    public boolean isExpired() {
        if (endTimestamp == 0L) {
            return false;
        }
        return System.currentTimeMillis() >= endTimestamp;
    }

    public Long getExecuteTimestampAndGeneratorNext() {
        this.latestStartExecuteTimestamp = nextExecuteTimestamp;
        // todo The problem of delay should be considered. If it is greater than the ten-minute time window,
        //  should the task be lost or executed on a new time window?
        this.nextExecuteTimestamp = latestStartExecuteTimestamp + intervalMilliSeconds;
        return nextExecuteTimestamp;
    }

    public void pause() {
        this.jobStatus = JobStatus.PAUSED;
    }

    public void pause(String errMsg) {
        this.jobStatus = JobStatus.PAUSED;
        this.errMsg = errMsg;
    }

    public void resume() {
        this.jobStatus = JobStatus.RUNNING;
    }

    public void stop() {
        this.jobStatus = JobStatus.STOPPED;
    }

    public boolean checkJobParam() {
        if (startTimestamp != 0L && startTimestamp < System.currentTimeMillis()) {
            return false;
        }
        if (endTimestamp != 0L && endTimestamp < System.currentTimeMillis()) {
            return false;
        }
        if (intervalMilliSeconds == null || intervalMilliSeconds <= 0L) {
            return false;
        }
        return null != executor;
    }
}
