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

package org.apache.doris.job.task;

import org.apache.doris.catalog.Env;
import org.apache.doris.job.base.AbstractJob;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;
import org.apache.doris.job.exception.JobException;

import com.google.gson.annotations.SerializedName;
import lombok.Data;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.RandomUtils;

@Data
@Log4j2
public abstract class AbstractTask implements Task {

    @SerializedName(value = "jid")
    private Long jobId;
    @SerializedName(value = "tid")
    private Long taskId;

    @SerializedName(value = "st")
    private TaskStatus status;
    @SerializedName(value = "ctm")
    private Long createTimeMs;
    @SerializedName(value = "stm")
    private Long startTimeMs;
    @SerializedName(value = "ftm")
    private Long finishTimeMs;

    @SerializedName(value = "tt")
    private TaskType taskType;

    @SerializedName(value = "emg")
    private String errMsg;

    public AbstractTask() {
        taskId = getNextTaskId();
    }

    private static long getNextTaskId() {
        // do not use Env.getNextId(), just generate id without logging
        return System.nanoTime() + RandomUtils.nextInt();
    }

    @Override
    public void onFail(String msg) throws JobException {
        status = TaskStatus.FAILED;
        if (!isCallable()) {
            return;
        }
        Env.getCurrentEnv().getJobManager().getJob(jobId).onTaskFail(this);
    }

    @Override
    public void onFail() throws JobException {
        if (TaskStatus.CANCELED.equals(status)) {
            return;
        }
        status = TaskStatus.FAILED;
        setFinishTimeMs(System.currentTimeMillis());
        if (!isCallable()) {
            return;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        job.onTaskFail(this);
    }

    private boolean isCallable() {
        if (status.equals(TaskStatus.CANCELED)) {
            return false;
        }
        if (null != Env.getCurrentEnv().getJobManager().getJob(jobId)) {
            return true;
        }
        return false;
    }

    @Override
    public void onSuccess() throws JobException {
        if (TaskStatus.CANCELED.equals(status)) {
            return;
        }
        status = TaskStatus.SUCCESS;
        setFinishTimeMs(System.currentTimeMillis());
        if (!isCallable()) {
            return;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        if (null == job) {
            log.info("job is null, job id is {}", jobId);
            return;
        }
        job.onTaskSuccess(this);
    }

    @Override
    public void cancel() throws JobException {
        status = TaskStatus.CANCELED;
    }

    @Override
    public void before() throws JobException {
        status = TaskStatus.RUNNING;
        setStartTimeMs(System.currentTimeMillis());
    }

    public void runTask() throws JobException {
        try {
            before();
            run();
            onSuccess();
        } catch (Exception e) {
            this.errMsg = e.getMessage();
            onFail();
            log.warn("execute task error, job id is {}, task id is {}", jobId, taskId, e);
        }
    }

    public boolean isCancelled() {
        return status.equals(TaskStatus.CANCELED);
    }

    public String getJobName() {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(jobId);
        return job == null ? "" : job.getJobName();
    }

    public Job getJobOrJobException() throws JobException {
        AbstractJob job = Env.getCurrentEnv().getJobManager().getJob(jobId);
        if (job == null) {
            throw new JobException("job not exist, jobId:" + jobId);
        }
        return job;
    }

}
