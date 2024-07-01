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

package org.apache.doris.job.extensions.insert;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.loadv2.LoadStatistic;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.insert.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.ImmutableList;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The AbstractInsertTask class extends AbstractTask and is designed to handle tasks specific to insertion operations.
 * Acting as an abstract base class for insertion tasks, it provides a general framework with specific insertion logic
 * to be implemented by its subclasses.
 */
@Log4j2
public class AbstractInsertTask extends AbstractTask {
    public static final ImmutableList<Column> BASE_SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("JobId", ScalarType.createStringType()),
            new Column("JobName", ScalarType.createStringType()),
            new Column("Label", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("Sql", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTime", ScalarType.createStringType()),
            new Column("FinishTime", ScalarType.createStringType()),
            new Column("TrackingUrl", ScalarType.createStringType()),
            new Column("LoadStatistic", ScalarType.createStringType()),
            new Column("User", ScalarType.createStringType()));
    protected String labelName;
    protected InsertIntoTableCommand command;
    protected StmtExecutor stmtExecutor;
    protected ConnectContext ctx;
    @SerializedName(value = "sql")
    protected String sql;
    protected String currentDb;
    @SerializedName(value = "uif")
    protected UserIdentity userIdentity;
    protected LoadStatistic loadStatistic;
    protected AtomicBoolean isCanceled = new AtomicBoolean(false);
    protected AtomicBoolean isFinished = new AtomicBoolean(false);
    protected static final String LABEL_SPLITTER = "_";

    protected FailMsg failMsg;
    @Getter
    protected String trackingUrl;

    @Getter
    @Setter
    protected LoadJob jobInfo;


    @Override
    public void before() throws JobException {
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", getTaskId());
        }
        ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setQualifiedUser(userIdentity.getQualifiedUser());
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        if (StringUtils.isNotEmpty(currentDb)) {
            ctx.setDatabase(currentDb);
        }
        TUniqueId queryId = generateQueryId(UUID.randomUUID().toString());
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        ctx.getSessionVariable().enableNereidsDML = true;
        stmtExecutor = new StmtExecutor(ctx, (String) null);
        ctx.setQueryId(queryId);
        if (StringUtils.isNotEmpty(sql)) {
            NereidsParser parser = new NereidsParser();
            this.command = (InsertIntoTableCommand) parser.parseSingle(sql);
            this.command.setLabelName(Optional.of(getJobId() + LABEL_SPLITTER + getTaskId()));
            this.command.setJobId(getTaskId());
        }

        super.before();
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

    protected TUniqueId generateQueryId(String taskIdString) {
        UUID taskId = UUID.fromString(taskIdString);
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    @Override
    public void run() throws JobException {
        try {
            if (isCanceled.get()) {
                log.info("task has been canceled, task id is {}", getTaskId());
                return;
            }
            command.runWithUpdateInfo(ctx, stmtExecutor, loadStatistic);
        } catch (Exception e) {
            throw new JobException(e);
        }
    }

    @Override
    public void onFail() throws JobException {
        isFinished.set(true);
        super.onFail();
    }

    @Override
    public void onSuccess() throws JobException {
        isFinished.set(true);
        super.onSuccess();
    }

    @Override
    protected void executeCancelLogic() {
        if (isFinished.get() || isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        if (null != stmtExecutor) {
            stmtExecutor.cancel();
        }
    }


    @Override
    public TRow getTvfInfo(String jobName) {
        TRow trow = new TRow();
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getTaskId())));
        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(getJobId())));
        trow.addToColumnValue(new TCell().setStringVal(jobName));
        trow.addToColumnValue(new TCell().setStringVal(getJobId() + LABEL_SPLITTER + getTaskId()));
        trow.addToColumnValue(new TCell().setStringVal(getStatus().name()));
        trow.addToColumnValue(new TCell().setStringVal(sql));
        trow.addToColumnValue(new TCell().setStringVal(null == failMsg ? "" : failMsg.getMsg()));
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(getCreateTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == getFinishTimeMs() ? ""
                : TimeUtils.longToTimeString(getFinishTimeMs())));
        trow.addToColumnValue(new TCell().setStringVal(null == trackingUrl ? "" : trackingUrl));
        trow.addToColumnValue(new TCell().setStringVal(null == loadStatistic ? "" : loadStatistic.toJson()));
        trow.addToColumnValue(new TCell().setStringVal(userIdentity.getQualifiedUser()));
        return trow;
    }

}
