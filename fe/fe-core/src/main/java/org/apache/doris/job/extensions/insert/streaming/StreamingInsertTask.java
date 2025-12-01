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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.base.Job;
import org.apache.doris.job.common.TaskStatus;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.offset.SourceOffsetProvider;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
    @Setter
    private volatile TaskStatus status;
    private String errMsg;
    private Long createTimeMs;
    private Long startTimeMs;
    private Long finishTimeMs;
    private String sql;
    private StmtExecutor stmtExecutor;
    private InsertIntoTableCommand taskCommand;
    private String currentDb;
    private UserIdentity userIdentity;
    private ConnectContext ctx;
    private Offset runningOffset;
    @Getter
    private AtomicBoolean isCanceled = new AtomicBoolean(false);
    private StreamingJobProperties jobProperties;
    private Map<String, String> originTvfProps;
    SourceOffsetProvider offsetProvider;
    private int retryCount = 0;

    public StreamingInsertTask(long jobId,
                               long taskId,
                               String sql,
                               SourceOffsetProvider offsetProvider,
                               String currentDb,
                               StreamingJobProperties jobProperties,
                               Map<String, String> originTvfProps,
                               UserIdentity userIdentity) {
        this.jobId = jobId;
        this.taskId = taskId;
        this.sql = sql;
        this.userIdentity = userIdentity;
        this.currentDb = currentDb;
        this.offsetProvider = offsetProvider;
        this.jobProperties = jobProperties;
        this.originTvfProps = originTvfProps;
        this.labelName = getJobId() + LABEL_SPLITTER + getTaskId();
        this.createTimeMs = System.currentTimeMillis();
    }

    public void execute() throws JobException {
        while (retryCount <= MAX_RETRY) {
            try {
                before();
                run();
                onSuccess();
                return;
            } catch (Exception e) {
                if (TaskStatus.CANCELED.equals(status)) {
                    return;
                }
                this.errMsg = e.getMessage();
                retryCount++;
                if (retryCount > MAX_RETRY) {
                    log.error("Task execution failed after {} retries.", MAX_RETRY, e);
                    onFail(e.getMessage());
                    return;
                }
                log.warn("execute streaming task error, job id is {}, task id is {}, retrying {}/{}: {}",
                        jobId, taskId, retryCount, MAX_RETRY, e.getMessage());
            } finally {
                // The cancel logic will call the closeOrReleased Resources method by itself.
                // If it is also called here,
                // it may result in the inability to obtain relevant information when canceling the task
                if (!TaskStatus.CANCELED.equals(status)) {
                    closeOrReleaseResources();
                }
            }
        }
    }

    private void before() throws Exception {
        if (isCanceled.get()) {
            log.info("streaming insert task has been canceled, task id is {}", getTaskId());
            return;
        }
        this.status = TaskStatus.RUNNING;
        this.startTimeMs = System.currentTimeMillis();
        ctx = InsertTask.makeConnectContext(userIdentity, currentDb);
        ctx.setSessionVariable(jobProperties.getSessionVariable(ctx.getSessionVariable()));
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);

        this.runningOffset = offsetProvider.getNextOffset(jobProperties, originTvfProps);
        log.info("streaming insert task {} get running offset: {}", taskId, runningOffset.toString());
        InsertIntoTableCommand baseCommand = (InsertIntoTableCommand) new NereidsParser().parseSingle(sql);
        baseCommand.setJobId(getTaskId());
        StmtExecutor baseStmtExecutor =
                new StmtExecutor(ctx, new LogicalPlanAdapter(baseCommand, ctx.getStatementContext()));
        baseCommand.initPlan(ctx, baseStmtExecutor, false);
        if (!baseCommand.getParsedPlan().isPresent()) {
            throw new JobException("Can not get Parsed plan");
        }
        this.taskCommand = offsetProvider.rewriteTvfParams(baseCommand, runningOffset);
        this.taskCommand.setLabelName(Optional.of(labelName));
        this.stmtExecutor = new StmtExecutor(ctx, new LogicalPlanAdapter(taskCommand, ctx.getStatementContext()));
    }

    private void run() throws JobException {
        if (isCanceled.get()) {
            log.info("task has been canceled, task id is {}", getTaskId());
            return;
        }
        log.info("start to run streaming insert task, label {}, offset is {}", labelName, runningOffset.toString());
        String errMsg = null;
        try {
            taskCommand.run(ctx, stmtExecutor);
            if (ctx.getState().getStateType() == QueryState.MysqlStateType.OK) {
                return;
            } else {
                errMsg = ctx.getState().getErrorMessage();
            }
            throw new JobException(errMsg);
        } catch (Exception e) {
            log.warn("execute insert task error, label is {},offset is {}", taskCommand.getLabelName(),
                    runningOffset.toString(), e);
            throw new JobException(Util.getRootCauseMessage(e));
        }
    }

    public boolean onSuccess() throws JobException {
        if (isCanceled.get()) {
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
        if (isCanceled.get()) {
            return;
        }
        this.errMsg = errMsg;
        this.status = TaskStatus.FAILED;
        this.finishTimeMs = System.currentTimeMillis();
        if (!isCallable()) {
            return;
        }
        Job job = Env.getCurrentEnv().getJobManager().getJob(getJobId());
        StreamingInsertJob streamingInsertJob = (StreamingInsertJob) job;
        streamingInsertJob.onStreamTaskFail(this);
    }

    public void cancel(boolean needWaitCancelComplete) {
        if (TaskStatus.SUCCESS.equals(status) || TaskStatus.FAILED.equals(status)
                || TaskStatus.CANCELED.equals(status)) {
            return;
        }
        status = TaskStatus.CANCELED;
        if (isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        this.errMsg = "task cancelled";
        if (null != stmtExecutor) {
            log.info("cancelling streaming insert task, job id is {}, task id is {}",
                    getJobId(), getTaskId());
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "streaming insert task cancelled"),
                    needWaitCancelComplete);
        }
    }

    public void closeOrReleaseResources() {
        if (null != stmtExecutor) {
            stmtExecutor = null;
        }
        if (null != taskCommand) {
            taskCommand = null;
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

    /**
     * show streaming insert task info detail
     */
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(this.getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(this.getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(this.getLabelName()));
        trow.addToColumnValue(new TCell().setStringVal(this.getStatus().name()));
        // err msg
        trow.addToColumnValue(new TCell().setStringVal(StringUtils.isNotBlank(errMsg)
                ? errMsg : FeConstants.null_string));

        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(this.getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? FeConstants.null_string
                : TimeUtils.longToTimeString(this.getStartTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(this.getFinishTimeMs())));

        List<LoadJob> loadJobs = Env.getCurrentEnv().getLoadManager()
                .queryLoadJobsByJobIds(Arrays.asList(this.getTaskId()));
        if (!loadJobs.isEmpty()) {
            LoadJob loadJob = loadJobs.get(0);
            if (loadJob.getLoadingStatus() != null && loadJob.getLoadingStatus().getTrackingUrl() != null) {
                trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadingStatus().getTrackingUrl()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            }

            if (loadJob.getLoadStatistic() != null) {
                trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadStatistic().toJson()));
            } else {
                trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            }
        } else {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        }

        if (this.getUserIdentity() == null) {
            trow.addToColumnValue(new TCell().setStringVal(FeConstants.null_string));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(this.getUserIdentity().getQualifiedUser()));
        }
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(runningOffset == null
                ? FeConstants.null_string : runningOffset.showRange()));
        return trow;
    }
}
