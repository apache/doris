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
import org.apache.doris.common.Status;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.common.util.Util;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.extensions.insert.InsertTask;
import org.apache.doris.job.offset.Offset;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.glue.LogicalPlanAdapter;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.QueryState;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TStatusCode;

import lombok.extern.log4j.Log4j2;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

@Log4j2
public class StreamingInsertTask extends AbstractTask {
    private static final String LABEL_SPLITTER = "_";
    private static final int MAX_RETRY = 3;
    private InsertIntoTableCommand command;
    private StmtExecutor stmtExecutor;
    private String currentDb;
    private UserIdentity userIdentity;
    private ConnectContext ctx;
    private LoadStatistic loadStatistic;
    private Offset offset;
    private AtomicBoolean isCanceled = new AtomicBoolean(false);
    private AtomicBoolean isFinished = new AtomicBoolean(false);
    private StreamingJobProperties jobProperties;

    public StreamingInsertTask(InsertIntoTableCommand command,
                               LoadStatistic loadStatistic,
                               String currentDb,
                               Offset offset,
                               StreamingJobProperties jobProperties) {
        this.command = command;
        this.loadStatistic = loadStatistic;
        this.userIdentity = ctx.getCurrentUserIdentity();
        this.currentDb = currentDb;
        this.offset = offset;
        this.jobProperties = jobProperties;
    }

    @Override
    public void before() throws JobException {
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", getTaskId());
        }
        ctx = InsertTask.makeConnectContext(userIdentity, currentDb);
        ctx.setSessionVariable(jobProperties.getSessionVariable());
        StatementContext statementContext = new StatementContext();
        ctx.setStatementContext(statementContext);
        this.command.setLabelName(Optional.of(getJobId() + LABEL_SPLITTER + getTaskId()));
        this.command.setJobId(getTaskId());
        stmtExecutor = new StmtExecutor(ctx, new LogicalPlanAdapter(command, ctx.getStatementContext()));
        super.before();
    }


    @Override
    public void run() throws JobException {
        String errMsg = null;
        int retry = 0;
        while (retry <= MAX_RETRY) {
            try {
                if (isCanceled.get()) {
                    log.info("task has been canceled, task id is {}", getTaskId());
                    return;
                }
                command.runWithUpdateInfo(ctx, stmtExecutor, loadStatistic);
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

    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(getStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(getErrMsg()));
        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getStartTimeMs() ? ""
                : TimeUtils.longToTimeString(getStartTimeMs())));
        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getFinishTimeMs())));
        // tracking url
        trow.addToColumnValue(new TCell().setStringVal("trackingUrl"));
        trow.addToColumnValue(new TCell().setStringVal("getLoadStatistic"));
        if (userIdentity == null) {
            trow.addToColumnValue(new TCell().setStringVal(""));
        } else {
            trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        }
        trow.addToColumnValue(new TCell().setStringVal(""));
        trow.addToColumnValue(new TCell().setStringVal(offset.toJson()));
        return trow;
    }

    @Override
    protected void closeOrReleaseResources() {
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

    @Override
    protected void executeCancelLogic(boolean needWaitCancelComplete) throws Exception {
        if (isFinished.get() || isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        if (null != stmtExecutor) {
            stmtExecutor.cancel(new Status(TStatusCode.CANCELLED, "streaming insert task cancelled"));
        }
    }

    @Override
    public boolean onFail() throws JobException {
        if (isCanceled.get()) {
            return false;
        }
        isFinished.set(true);
        return super.onFail();
    }

    @Override
    public boolean onSuccess() throws JobException {
        isFinished.set(true);
        return super.onSuccess();
    }
}
