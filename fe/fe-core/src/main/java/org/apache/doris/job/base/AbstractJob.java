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

package org.apache.doris.job.base;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.common.JobType;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertJob;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.job.task.Task;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Data
public abstract class AbstractJob<T extends AbstractTask> implements Job<T>, Writable {

    @SerializedName(value = "jobId")
    private Long jobId;

    @SerializedName(value = "jobName")
    private String jobName;

    @SerializedName(value = "jobStatus")
    private JobStatus jobStatus;

    @SerializedName(value = "currentDbName")
    private String currentDbName;

    @SerializedName(value = "comment")
    private String comment;

    @SerializedName(value = "jobType")
    private String createUser;

    @SerializedName(value = "jobConfig")
    private JobExecutionConfiguration jobConfig;

    @SerializedName(value = "createTimeMs")
    private Long createTimeMs;

    private List<? extends AbstractTask> runningTasks = new ArrayList<>();

    @Override
    public void cancel() throws JobException {
        if (CollectionUtils.isEmpty(runningTasks)) {
            return;
        }
        runningTasks.forEach(Task::cancel);
    }

    public void checkJobParams() {
        if (null == jobConfig) {
            throw new IllegalArgumentException("jobConfig cannot be null");
        }
        jobConfig.checkParams(createTimeMs);
        checkJobParamsInternal();
    }

    public void updateJobStatus(JobStatus newJobStatus) {
        if (null == newJobStatus) {
            throw new IllegalArgumentException("jobStatus cannot be null");
        }
        if (jobStatus == newJobStatus) {
            throw new IllegalArgumentException(String.format("Can't update job %s status to the %s status",
                    jobStatus.name(), this.jobStatus.name()));
        }
        // check other status
    }

    public void resumeJob() {
        if (jobStatus != JobStatus.PAUSED) {
            throw new IllegalArgumentException(String.format("Can't resume job %s status to the %s status",
                    jobStatus.name(), this.jobStatus.name()));
        }
        jobStatus = JobStatus.RUNNING;
    }

    protected abstract void checkJobParamsInternal();

    public static AbstractJob readFields(DataInput in) throws IOException {
        JobType jobType = JobType.valueOf(Text.readString(in));
        switch (jobType) {
            case INSERT:
                return InsertJob.readFields(in);
            case MTMV:
                // return MTMVJob.readFields(in);
                break;
            default:
                throw new IllegalArgumentException("unknown job type");
        }
        throw new IllegalArgumentException("unknown job type");
    }

    @Override
    public void onTaskFail(long taskId) {
        updateJobStatusIfEnd();
    }

    @Override
    public void onTaskSuccess(long taskId) {
        updateJobStatusIfEnd();

    }

    private void updateJobStatusIfEnd() {
        JobExecuteType executeType = getJobConfig().getExecuteType();
        if (executeType.equals(JobExecuteType.MANUAL)) {
            return;
        }
        switch (executeType) {
            case ONE_TIME:
            case INSTANT:
                jobStatus = JobStatus.FINISHED;
                Env.getCurrentEnv().getJobManager().getJob(jobId).updateJobStatus(jobStatus);
                break;
            case RECURRING:
                if (null != getJobConfig().getTimerDefinition().getEndTimeMs()
                        && getJobConfig().getTimerDefinition().getEndTimeMs() < System.currentTimeMillis()) {
                    jobStatus = JobStatus.FINISHED;
                    Env.getCurrentEnv().getJobManager().getJob(jobId).updateJobStatus(jobStatus);
                }
                break;
            default:
                break;
        }
    }
}
