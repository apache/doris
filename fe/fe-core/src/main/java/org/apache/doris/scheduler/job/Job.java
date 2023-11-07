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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.constants.JobCategory;
import org.apache.doris.scheduler.constants.JobStatus;
import org.apache.doris.scheduler.constants.JobType;
import org.apache.doris.scheduler.executor.JobExecutor;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * Job is the core of the scheduler module, which is used to store the Job information of the job module.
 * We can use the job to uniquely identify a Job.
 * The jobName is used to identify the job, which is not unique.
 * The jobStatus is used to identify the status of the Job, which is used to control the execution of the
 * job.
 */
@Data
public class Job implements Writable {

    public Job(String jobName, Long intervalMilliSeconds, Long startTimeMs, Long endTimeMs,
               JobExecutor executor) {
        this.jobName = jobName;
        this.executor = executor;
        this.intervalMs = intervalMilliSeconds;
        this.startTimeMs = null == startTimeMs ? 0L : startTimeMs;
        this.endTimeMs = null == endTimeMs ? 0L : endTimeMs;
        this.jobStatus = JobStatus.RUNNING;
        this.jobId = Env.getCurrentEnv().getNextId();
    }

    public Job() {
        this.jobId = Env.getCurrentEnv().getNextId();
    }

    @SerializedName("jobId")
    private Long jobId;

    @SerializedName("jobName")
    private String jobName;

    @SerializedName("dbName")
    private String dbName;

    /**
     * The status of the job, which is used to control the execution of the job.
     *
     * @see JobStatus
     */
    @SerializedName("jobStatus")
    private JobStatus jobStatus;

    @SerializedName("jobType")
    private JobType jobType = JobType.RECURRING;

    /**
     * The executor of the job.
     *
     * @see JobExecutor
     */
    @SerializedName("executor")
    private JobExecutor executor;
    @SerializedName("baseName")
    private String baseName;

    @SerializedName("user")
    private String user;

    @SerializedName("intervalMs")
    private Long intervalMs = 0L;
    @SerializedName("startTimeMs")
    private Long startTimeMs = 0L;

    @SerializedName("endTimeMs")
    private Long endTimeMs = 0L;

    @SerializedName("timezone")
    private String timezone;

    @SerializedName("jobCategory")
    private JobCategory jobCategory;


    @SerializedName("latestStartExecuteTimeMs")
    private Long latestStartExecuteTimeMs = 0L;
    @SerializedName("latestCompleteExecuteTimeMs")
    private Long latestCompleteExecuteTimeMs = 0L;

    @SerializedName("intervalUnit")
    private IntervalUnit intervalUnit;
    @SerializedName("originInterval")
    private Long originInterval;
    @SerializedName("nextExecuteTimeMs")
    private Long nextExecuteTimeMs = 0L;

    @SerializedName("createTimeMs")
    private Long createTimeMs = System.currentTimeMillis();

    private Boolean lastExecuteTaskStatus;

    @SerializedName("comment")
    private String comment;

    @SerializedName("errMsg")
    private String errMsg;

    /**
     * if we want to start the job immediately, we can set this flag to true.
     * The default value is false.
     * when we set this flag to true, the start time will be set to current time.
     * we don't need to serialize this field.
     */
    private boolean immediatelyStart = false;

    public boolean isRunning() {
        return jobStatus == JobStatus.RUNNING;
    }

    public boolean isStopped() {
        return jobStatus == JobStatus.STOPPED;
    }

    public boolean isFinished() {
        return jobStatus == JobStatus.FINISHED;
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
        return System.currentTimeMillis() >= endTimeMs || nextExecuteTimeMs > endTimeMs;
    }

    public boolean isExpired() {
        if (endTimeMs == 0L) {
            return false;
        }
        return System.currentTimeMillis() >= endTimeMs;
    }

    public Long getExecuteTimestampAndGeneratorNext() {
        this.latestStartExecuteTimeMs = nextExecuteTimeMs;
        //  todo The problem of delay should be considered. If it is greater than the ten-minute time window,
        //  should the task be lost or executed on a new time window?
        this.nextExecuteTimeMs = latestStartExecuteTimeMs + intervalMs;
        return nextExecuteTimeMs;
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

    public void checkJobParam() throws DdlException {
        if (null == jobCategory) {
            throw new DdlException("jobCategory must be set");
        }
        if (null == executor) {
            throw new DdlException("Job executor must be set");
        }
        if (null == jobType) {
            throw new DdlException("Job type must be set");
        }
        if (jobType.equals(JobType.MANUAL)) {
            return;
        }
        if (startTimeMs != 0L && startTimeMs < System.currentTimeMillis()) {
            throw new DdlException("startTimeMs must be greater than current time");
        }
        if (immediatelyStart && startTimeMs != 0L) {
            throw new DdlException("immediately start and startTimeMs can't be set at the same time");
        }
        if (immediatelyStart) {
            startTimeMs = System.currentTimeMillis();
        }
        if (endTimeMs != 0L && endTimeMs < System.currentTimeMillis()) {
            throw new DdlException("endTimeMs must be greater than current time");
        }
        if (null != intervalUnit && null != originInterval) {
            this.intervalMs = intervalUnit.getParameterValue(originInterval);
        }
        if (jobType.equals(JobType.RECURRING) && (intervalMs == null || intervalMs <= 0L)) {
            throw new DdlException("cycle job must set intervalMs");
        }

    }


    @Override
    public void write(DataOutput out) throws IOException {
        String jobData = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jobData);
    }

    public static Job readFields(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), Job.class);
    }

    public List<String> getShowInfo() {
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(jobId));
        row.add(jobName);
        row.add(user);
        row.add(jobType.name());

        row.add(convertRecurringStrategyToString());
        row.add(jobStatus.name());
        row.add(null == lastExecuteTaskStatus ? "null" : lastExecuteTaskStatus.toString());
        row.add(createTimeMs <= 0L ? "null" : TimeUtils.longToTimeString(createTimeMs));
        row.add(comment == null ? "null" : comment);
        return row;
    }

    private String convertRecurringStrategyToString() {
        if (jobType.equals(JobType.MANUAL)) {
            return "MANUAL TRIGGER";
        }
        switch (jobType) {
            case ONE_TIME:
                return "AT " + TimeUtils.longToTimeString(startTimeMs);
            case RECURRING:
                String result = "EVERY " + originInterval + " " + intervalUnit.name();
                if (startTimeMs > 0) {
                    result += " STARTS " + TimeUtils.longToTimeString(startTimeMs);
                }
                if (endTimeMs > 0) {
                    result += " ENDS " + TimeUtils.longToTimeString(endTimeMs);
                }
                return result;
            case STREAMING:
                return "STREAMING" + (startTimeMs > 0 ? " AT " + TimeUtils.longToTimeString(startTimeMs) : "");
            case MANUAL:
                return "MANUAL TRIGGER";
            default:
                return "UNKNOWN";
        }
    }

}
