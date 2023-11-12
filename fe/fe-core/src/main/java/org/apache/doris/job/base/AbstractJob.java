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

import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.job.common.JobStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.job.task.Task;
import org.apache.doris.persist.gson.GsonUtils;

import lombok.Data;
import lombok.Getter;
import org.apache.commons.collections.CollectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Data
public abstract class AbstractJob<T extends AbstractTask> implements Job<T>, Writable {

    @Getter
    private Long jobId;

    private String jobName;

    private JobStatus jobStatus;

    private String currentDbName;

    private String comment;

    private String currentUser;

    private JobExecutionConfiguration jobConfig;

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
        jobConfig.checkParams();
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

    @Override
    public void write(DataOutput out) throws IOException {
        String jobData = GsonUtils.GSON.toJson(this);
        Text.writeString(out, jobData);
    }

    public static AbstractJob readFields(DataInput in) throws IOException {
        return GsonUtils.GSON.fromJson(Text.readString(in), AbstractJob.class);
    }

    @Override
    public void onTaskFail(long taskId) {
        // AbstractTask task=runningTasks.stream().findFirst();
    }

    @Override
    public void onTaskSuccess(long taskId) {

    }
}
