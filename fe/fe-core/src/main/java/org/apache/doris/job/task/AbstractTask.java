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
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.common.TaskType;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

@Data
@Slf4j
public abstract class AbstractTask implements Task {

    private Long jobId;

    private Long taskId;

    private TaskStatus status;

    private Long createTimeMs;

    private Long startTimeMs;

    private Long finishTimeMs;

    private TaskType taskType;

    @Override
    public void onFail(String msg) {
        Env.getCurrentEnv().getJobManager().getJob(jobId).onTaskFail(taskId);
        status = TaskStatus.FAILD;
    }

    @Override
    public void onFail() {
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        job.onTaskFail(getTaskId());
        setFinishTimeMs(System.currentTimeMillis());
    }

    @Override
    public void onSuccess() {
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        job.onTaskSuccess(getTaskId());
        status = TaskStatus.SUCCESS;
        setFinishTimeMs(System.currentTimeMillis());
    }

    @Override
    public void cancel() {
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        job.onTaskCancel(getTaskId());
        status = TaskStatus.CANCEL;
    }

    public void runTask() {
        try {
            before();
            run();
            onSuccess();
        } catch (Exception e) {
            onFail();
            log.warn("execute task error, job id is {},task id is {}", jobId, taskId, e);
        }
    }


}
