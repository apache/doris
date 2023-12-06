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
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j2;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * todo implement this later
 */
@Log4j2
public class InsertTask extends AbstractTask {

    public static final ImmutableList<Column> SCHEMA = ImmutableList.of(
            new Column("TaskId", ScalarType.createStringType()),
            new Column("Label", ScalarType.createStringType()),
            new Column("Status", ScalarType.createStringType()),
            new Column("EtlInfo", ScalarType.createStringType()),
            new Column("TaskInfo", ScalarType.createStringType()),
            new Column("ErrorMsg", ScalarType.createStringType()),
            new Column("CreateTimeMs", ScalarType.createStringType()),
            new Column("FinishTimeMs", ScalarType.createStringType()),
            new Column("TrackingUrl", ScalarType.createStringType()),
            new Column("LoadStatistic", ScalarType.createStringType()),
            new Column("User", ScalarType.createStringType()));

    public static final ImmutableMap<String, Integer> COLUMN_TO_INDEX;

    static {
        ImmutableMap.Builder<String, Integer> builder = new ImmutableMap.Builder();
        for (int i = 0; i < SCHEMA.size(); i++) {
            builder.put(SCHEMA.get(i).getName().toLowerCase(), i);
        }
        COLUMN_TO_INDEX = builder.build();
    }

    private String labelName;

    private InsertIntoTableCommand command;

    private StmtExecutor stmtExecutor;

    private ConnectContext ctx;

    private String sql;

    private String currentDb;

    private UserIdentity userIdentity;

    private AtomicBoolean isCanceled = new AtomicBoolean(false);

    private AtomicBoolean isFinished = new AtomicBoolean(false);


    @Getter
    @Setter
    private LoadJob loadJob;


    @Override
    public void before() throws JobException {
        if (isCanceled.get()) {
            throw new JobException("Export executor has been canceled, task id: {}", getTaskId());
        }
        ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setQualifiedUser(userIdentity.getQualifiedUser());
        ctx.setCurrentUserIdentity(userIdentity);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        ctx.setDatabase(currentDb);
        TUniqueId queryId = generateQueryId(UUID.randomUUID().toString());
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        stmtExecutor = new StmtExecutor(ctx, (String) null);
        ctx.setQueryId(queryId);
        NereidsParser parser = new NereidsParser();
        this.command = (InsertIntoTableCommand) parser.parseSingle(sql);
        this.command.setLabelName(Optional.of(getJobId() + "_" + getTaskId()));
        this.command.setJobId(getTaskId());

        super.before();

    }

    protected TUniqueId generateQueryId(String taskIdString) {
        UUID taskId = UUID.fromString(taskIdString);
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    public InsertTask(String labelName, String currentDb, String sql, UserIdentity userIdentity) {
        this.labelName = labelName;
        this.sql = sql;
        this.currentDb = currentDb;
        this.userIdentity = userIdentity;

    }

    @Override
    public void run() throws JobException {
        try {
            if (isCanceled.get()) {
                log.info("task has been canceled, task id is {}", getTaskId());
                return;
            }
            command.run(ctx, stmtExecutor);
        } catch (Exception e) {
            log.warn("execute insert task error, job id is {}, task id is {},sql is {}", getJobId(),
                    getTaskId(), sql, e);
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
    public void cancel() throws JobException {
        if (isFinished.get() || isCanceled.get()) {
            return;
        }
        isCanceled.getAndSet(true);
        if (null != stmtExecutor) {
            stmtExecutor.cancel();
        }
        super.cancel();
    }

    @Override
    public List<String> getShowInfo() {
        if (null == loadJob) {
            return getPendingTaskShowInfo();
        }
        List<String> jobInfo = Lists.newArrayList();
        // jobId
        jobInfo.add(String.valueOf(loadJob.getId()));
        // label
        jobInfo.add(loadJob.getLabel());
        // state
        jobInfo.add(loadJob.getState().name());

        // etl info
        if (loadJob.getLoadingStatus().getCounters().isEmpty()) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add(Joiner.on("; ").withKeyValueSeparator("=").join(loadJob.getLoadingStatus().getCounters()));
        }

        // task info
        jobInfo.add("cluster:" + loadJob.getResourceName() + "; timeout(s):" + loadJob.getTimeout()
                + "; max_filter_ratio:" + loadJob.getMaxFilterRatio() + "; priority:" + loadJob.getPriority());
        // error msg
        if (loadJob.getFailMsg() == null) {
            jobInfo.add(FeConstants.null_string);
        } else {
            jobInfo.add("type:" + loadJob.getFailMsg().getCancelType() + "; msg:" + loadJob.getFailMsg().getMsg());
        }

        // create time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getCreateTimestamp()));

        // load end time
        jobInfo.add(TimeUtils.longToTimeString(loadJob.getFinishTimestamp()));
        // tracking url
        jobInfo.add(loadJob.getLoadingStatus().getTrackingUrl());
        jobInfo.add(loadJob.getLoadStatistic().toJson());
        // user
        jobInfo.add(loadJob.getUserInfo().getQualifiedUser());
        return jobInfo;
    }

    @Override
    public TRow getTvfInfo() {
        TRow trow = new TRow();
        if (loadJob == null) {
            return trow;
        }

        trow.addToColumnValue(new TCell().setStringVal(String.valueOf(loadJob.getId())));
        trow.addToColumnValue(new TCell().setStringVal(loadJob.getLabel()));
        trow.addToColumnValue(new TCell().setStringVal(loadJob.getState().name()));
        // etl info
        String etlInfo = FeConstants.null_string;
        if (!loadJob.getLoadingStatus().getCounters().isEmpty()) {
            etlInfo = Joiner.on("; ").withKeyValueSeparator("=").join(loadJob.getLoadingStatus().getCounters());
        }
        trow.addToColumnValue(new TCell().setStringVal(etlInfo));

        // task info
        String taskInfo = "cluster:" + loadJob.getResourceName() + "; timeout(s):" + loadJob.getTimeout()
                + "; max_filter_ratio:" + loadJob.getMaxFilterRatio() + "; priority:" + loadJob.getPriority();
        trow.addToColumnValue(new TCell().setStringVal(taskInfo));

        // err msg
        String errMsg = FeConstants.null_string;
        if (loadJob.getFailMsg() != null) {
            errMsg = "type:" + loadJob.getFailMsg().getCancelType() + "; msg:" + loadJob.getFailMsg().getMsg();
        }
        trow.addToColumnValue(new TCell().setStringVal(errMsg));

        // create time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(loadJob.getCreateTimestamp())));

        // load end time
        trow.addToColumnValue(new TCell().setStringVal(TimeUtils.longToTimeString(loadJob.getFinishTimestamp())));
        // tracking url
        trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadingStatus().getTrackingUrl()));
        trow.addToColumnValue(new TCell().setStringVal(loadJob.getLoadStatistic().toJson()));
        trow.addToColumnValue(new TCell().setStringVal(loadJob.getUserInfo().getQualifiedUser()));
        return trow;
    }

    // if task not start, load job is null,return pending task show info
    private List<String> getPendingTaskShowInfo() {
        List<String> datas = new ArrayList<>();

        datas.add(String.valueOf(getTaskId()));
        datas.add(getJobId() + "_" + getTaskId());
        datas.add(getStatus().name());
        datas.add(FeConstants.null_string);
        datas.add(FeConstants.null_string);
        datas.add(FeConstants.null_string);
        datas.add(TimeUtils.longToTimeString(getCreateTimeMs()));
        datas.add(FeConstants.null_string);
        datas.add(FeConstants.null_string);
        datas.add(FeConstants.null_string);
        datas.add(userIdentity.getQualifiedUser());
        return datas;
    }

}
