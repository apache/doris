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

package org.apache.doris.manager.agent.task;

import org.apache.doris.manager.agent.common.AgentConstants;
import org.apache.doris.manager.common.domain.TaskResult;
import org.apache.doris.manager.common.domain.TaskState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.UUID;

public abstract class Task<T extends TaskDesc> {
    private static final Logger TASKLOG = LoggerFactory.getLogger(AgentConstants.LOG_TYPE_TASK);

    protected T taskDesc;

    private TaskResult taskResult;

    private TaskHook taskHook;

    protected Task(T taskDesc) {
        this.taskDesc = taskDesc;
        this.taskResult = new TaskResult();
        this.taskResult.setTaskId(UUID.randomUUID().toString().replace("-", ""));
    }

    public Task(T taskDesc, TaskHook taskHook) {
        this(taskDesc);
        this.taskHook = taskHook;
    }

    public String getTaskId() {
        return this.taskResult.getTaskId();
    }

    public TaskResult getTaskResult() {
        return taskResult;
    }

    public TaskResult executeTask() {
        this.taskResult.setStartTime(new Date());
        this.taskResult.setTaskState(TaskState.RUNNING);
        int code = AgentConstants.TASK_ERROR_CODE_DEFAULT;
        try {
            code = execute();
            if (0 == code && taskHook != null) {
                taskHook.onSuccess(taskDesc);
            }
        } catch (Throwable e) {
            code = AgentConstants.TASK_ERROR_CODE_EXCEPTION;
            e.printStackTrace();
            TASKLOG.info(e.getMessage());
        } finally {
            this.taskResult.setEndTime(new Date());
            this.taskResult.setRetCode(code);
            this.taskResult.setTaskState(TaskState.FINISHED);
        }
        return this.taskResult;
    }

    public T getTaskDesc() {
        return taskDesc;
    }

    protected abstract int execute() throws IOException, InterruptedException;

}
