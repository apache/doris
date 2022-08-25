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

package org.apache.doris.scheduler;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.scheduler.metadata.Job;
import org.apache.doris.scheduler.metadata.Task;

import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Future;

public class TaskExecutor implements Comparable<TaskExecutor> {
    private static final Logger LOG = LogManager.getLogger(TaskExecutor.class);

    private long jobId;

    private Map<String, String> properties;

    private Future<?> future;

    private Job job;

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    private ConnectContext ctx;

    private TaskProcessor processor;

    private Task task;

    public long getJobId() {
        return jobId;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    public Future<?> getFuture() {
        return future;
    }

    public void setFuture(Future<?> future) {
        this.future = future;
    }


    public TaskProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(TaskProcessor processor) {
        this.processor = processor;
    }

    public boolean executeTask() throws Exception {
        TaskContext taskContext = new TaskContext();
        taskContext.setDefinition(task.getDefinition());
        ctx = new ConnectContext();
        ctx.setDatabase(job.getDbName());
        ctx.setQualifiedUser(task.getUser());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getCreateUser(), "%"));
        ctx.getState().reset();
        ctx.setQueryId(Utils.genTUniqueId(UUID.fromString(task.getTaskId())));
        Map<String, String> taskContextProperties = Maps.newHashMap();

        taskContext.setCtx(ctx);
        taskContext.setRemoteIp(ctx.getMysqlChannel().getRemoteHostPortString());
        taskContext.setProperties(taskContextProperties);
        processor.process(taskContext);
        QueryState queryState = ctx.getState();
        if (ctx.getState().getStateType() == QueryState.MysqlStateType.ERR) {
            task.setErrorMessage(queryState.getErrorMessage());
            int errorCode = -1;
            if (queryState.getErrorCode() != null) {
                errorCode = queryState.getErrorCode().getCode();
            }
            task.setErrorCode(errorCode);
            return false;
        }
        return true;
    }

    public ConnectContext getCtx() {
        return ctx;
    }

    public Task getTask() {
        return task;
    }

    public Task initTask(String taskId, Long createTime) {
        Task task = new Task();
        task.setTaskId(taskId);
        task.setJobName(job.getName());
        if (createTime == null) {
            task.setCreateTime(System.currentTimeMillis());
        } else {
            task.setCreateTime(createTime);
        }
        task.setUser(job.getCreateUser());
        task.setDbName(job.getDbName());
        task.setDefinition(job.getDefinition());
        task.setExpireTime(System.currentTimeMillis() + Config.scheduler_task_expire_ms);
        this.task = task;
        return task;
    }

    @Override
    public int compareTo(@NotNull TaskExecutor task) {
        if (this.getTask().getPriority() != task.getTask().getPriority()) {
            return task.getTask().getPriority() - this.getTask().getPriority();
        } else {
            return this.getTask().getCreateTime() > task.getTask().getCreateTime() ? 1 : -1;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskExecutor task = (TaskExecutor) o;
        return this.task.getDefinition().equals(task.getTask().getDefinition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(task);
    }
}
