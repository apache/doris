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

import java.util.Date;
import java.util.UUID;

public abstract class Task<T extends TaskDesc> {
    protected T taskDesc;
    private TaskResult taskResult;
    private ITaskLog tasklog;
    private TaskHook taskHook;


    private Task(T taskDesc) {
        this.taskDesc = taskDesc;
        this.taskResult = new TaskResult();
        this.taskResult.setTaskId(UUID.randomUUID().toString().replace("-", ""));
    }

    public Task(T taskDesc, ITaskLog taskLruLog) {
        this(taskDesc);
        this.tasklog = taskLruLog;
    }

    public Task(T taskDesc, ITaskLog taskLruLog, TaskHook taskHook) {
        this(taskDesc);
        this.tasklog = taskLruLog;
        this.taskHook = taskHook;
    }


    public ITaskLog getTasklog() {
        return tasklog;
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
        int code = -501;
        try {
            code = execute();
            if (0 == code && taskHook != null) {
                taskHook.onSuccess(taskDesc);
            }
        } catch (Throwable e) {
            e.printStackTrace();
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

    protected abstract int execute();

}
