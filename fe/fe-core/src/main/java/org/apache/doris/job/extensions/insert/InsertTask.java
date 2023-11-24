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
import org.apache.doris.catalog.Env;
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
import org.apache.doris.thrift.TUniqueId;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * todo implement this later
 */
@Slf4j
public class InsertTask extends AbstractTask {

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
            command.run(ctx, stmtExecutor);
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
            return new ArrayList<>();
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

}
