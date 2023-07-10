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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.scheduler.common.IntervalUnit;
import org.apache.doris.scheduler.constants.JobStatus;
import org.apache.doris.scheduler.executor.JobExecutor;

import lombok.Data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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
    }

    public Job() {
    }

    private Long jobId = Env.getCurrentEnv().getNextId();

    private String jobName;

    private String dbName;
    
    private String comment;

    /**
     * The status of the job, which is used to control the execution of the job.
     *
     * @see JobStatus
     */
    private JobStatus jobStatus;

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

    private Long intervalMs = 0L;
    
    private Long originInterval = 0L;

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
        //  todo The problem of delay should be considered. If it is greater than the ten-minute time window,
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

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(jobId);
        Text.writeString(out, user);
        Text.writeString(out, jobName);
        Text.writeString(out, dbName);
        out.writeInt(jobStatus.ordinal());
        out.writeBoolean(isCycleJob);
        out.writeLong(intervalMs);
        out.writeLong(startTimeMs);
        out.writeLong(endTimeMs);
        out.writeLong(firstExecuteTimestamp);
        out.writeLong(latestStartExecuteTimestamp);
        out.writeLong(latestCompleteExecuteTimestamp);
        out.writeLong(nextExecuteTimestamp);
        Text.writeString(out, errMsg);
        Text.writeString(out, intervalUnit.name());
    }

    protected void readFields(DataInput in) throws IOException {
        jobId = in.readLong();
        user = Text.readString(in);
        jobName = Text.readString(in);
        dbName = Text.readString(in);
        jobStatus = JobStatus.values()[in.readInt()];
        isCycleJob = in.readBoolean();
        intervalMs = in.readLong();
        startTimeMs = in.readLong();
        endTimeMs = in.readLong();
        firstExecuteTimestamp = in.readLong();
        latestStartExecuteTimestamp = in.readLong();
        latestCompleteExecuteTimestamp = in.readLong();
        nextExecuteTimestamp = in.readLong();
        errMsg = Text.readString(in);
        intervalUnit = IntervalUnit.valueOf(Text.readString(in));
    }

    public List<String> getShowInfo() {
       
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(jobId));
        row.add(jobName);
        row.add(dbName);
        row.add(user);
        row.add(jobStatus.name());
        row.add(isCycleJob ? "true" : "false");
        if(isCycleJob){
            row.add(String.valueOf(originInterval));
            row.add(intervalUnit.name());
            if (startTimeMs != 0L) {
                row.add(TimeUtils.longToTimeString(startTimeMs));
            } else {
                row.add("null");
            }
        }
        
        row.add(String.valueOf(intervalMs));
        row.add(String.valueOf(startTimeMs));
        row.add(String.valueOf(endTimeMs));
        row.add(String.valueOf(firstExecuteTimestamp));
        row.add(String.valueOf(latestStartExecuteTimestamp));
        row.add(String.valueOf(latestCompleteExecuteTimestamp));
        row.add(String.valueOf(nextExecuteTimestamp));
        row.add(comment);
        return row;

    }
}
