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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TStatusCode;

import lombok.Getter;
import lombok.extern.log4j.Log4j2;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
@Getter
public class StreamingInsertTask {
    private static final String LABEL_SPLITTER = "_";
    private static final int MAX_RETRY = 3;
    private long jobId;
    private long taskId;
    private String labelName;
    private TaskStatus status;
    private String errMsg;
    private Long createTimeMs;
    private Long startTimeMs;
    private Long finishTimeMs;
    private InsertIntoTableCommand command;
    private StmtExecutor stmtExecutor;
    private String currentDb;
    private UserIdentity userIdentity;
    private ConnectContext ctx;
    private Offset offset;
    private AtomicBoolean isCanceled = new AtomicBoolean(false);
    private StreamingJobProperties jobProperties;

    public StreamingInsertTask(long jobId,
                               long taskId,
                               InsertIntoTableCommand command,
                               String currentDb,
                               Offset offset,
                               StreamingJobProperties jobProperties) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.command = command;
        this.userIdentity = ctx.getCurrentUserIdentity();
        this.currentDb = currentDb;
        this.offset = offset;
        this.jobProperties = jobProperties;
        this.labelName = getJobId() + LABEL_SPLITTER + getTaskId();
        this.createTimeMs = System.currentTimeMillis();
    }

    public void execute() throws JobException {
        try {
            before();
            run();
            onSuccess();
        } catch (Exception e) {
            if (TaskStatus.CANCELED.equals(status)) {
                return;
            }
            onFail(e.getMessage());
            log.warn("execute task error, job id is {}, task id is {}", jobId, taskId, e);
        } finally {
            // The cancel logic will call the closeOrReleased Resources method by itself.
            // If it is also called here,
            // it may result in the inability to obtain relevant information when canceling the task
            if (!TaskStatus.CANCELED.equals(status)) {
                closeOrReleaseResources();
            }
        }
    }

    private void before() throws JobException {
        this.startTimeMs = System.currentTimeMillis();
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", getTaskId());
        }
        ctx = InsertTask.makeConnectContext(userIdentity, currentDb);
        ctx.setSessionVariable(jobProperties.getSessionVariable());
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
        this.command.setLabelName(Optional.of(this.labelName));
        this.command.setJobId(getTaskId());
        stmtExecutor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
    }

    private void run() throws JobException {
        String errMsg = null;
        int retry = 0;
        while (retry <= MAX_RETRY) {
            try {
                if (isCanceled.get()) {
                    log.info("task has been canceled, task id is {}", getTaskId());
                    return;
                }
                command.runWithUpdateInfo(ctx, stmtExecutor, null);
                if (ctx.getState().getStateType() == QueryState.MysqlStateType.OK) {
                    return;
                } else {
                    errMsg = ctx.getState().getErrorMessage();
                }
                log.error(
                        "streaming insert failed with {}, reason {}, to retry",
                        command.getLabelName(),
                        errMsg);
                if (retry == MAX_RETRY) {
                    errMsg = "reached max retry times, failed with" + errMsg;
                }
            } catch (Exception e) {
                log.warn("execute insert task error, label is {},offset is {}", command.getLabelName(),
                         offset.toJson(), e);
                errMsg = Util.getRootCauseMessage(e);
            }
            retry++;
        }
        log.error("streaming insert task failed, job id is {}, task id is {}, offset is {}, errMsg is {}",
                getJobId(), getTaskId(), offset.toJson(), errMsg);
        throw new JobException(errMsg);
    }

    public boolean onSuccess() throws JobException {
        if (TaskStatus.CANCELED.equals(status)) {
            return false;
        }
        this.status = TaskStatus.SUCCESS;
        this.finishTimeMs = System.currentTimeMillis();
        if (!isCallable()) {
            return false;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        if (null == job) {
            log.info("job is null, job id is {}", jobId);
            return false;
        }

        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        streamingInsertJob.onStreamTaskSuccess(this);
        return true;
    }

    public void onFail(String errMsg) throws JobException {
        this.errMsg = errMsg;
        if (TaskStatus.CANCELED.equals(status)) {
            return;
        }
        this.status = TaskStatus.FAILED;
        this.finishTimeMs = System.currentTimeMillis();
        if (!isCallable()) {
            return;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        streamingInsertJob.onStreamTaskFail(this);
    }

    public void cancel(boolean needWaitCancelComplete) throws Exception {
        if (isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        if (null != stmtExecutor) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "streaming insert task cancelled"),
                    needWaitCancelComplete);
        }
    }

    public void closeOrReleaseResources() {
        if (null != stmtExecutor) {
            stmtExecutor = null;
        }
        if (null != command) {
            command = null;
        }
        if (null != ctx) {
            ctx = null;
        }
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
}
