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
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.parser.NereidsParser;
import org.apache.doris.nereids.trees.plans.commands.InsertIntoTableCommand;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

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


    private LoadJob loadJob;


    @Override
    public void before() {

        ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setQualifiedUser(Auth.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
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

    public InsertTask(String labelName, String currentDb, String sql) {
        this.labelName = labelName;
        this.sql = sql;
        this.currentDb = currentDb;

    }

    @Override
    public void run() throws Exception {
        if (getJobId().equals(888L)) {
            System.out.println("InsertTask run" + getJobId() + TimeUtils.longToTimeString(System.currentTimeMillis()));
        }
        //just for test

        command.run(ctx, stmtExecutor);
        log.info(getJobId() + "InsertTask run" + TimeUtils.longToTimeString(System.currentTimeMillis()));
    }

    @Override
    public void onFail() {
        super.onFail();
    }

    @Override
    public void onSuccess() {
        super.onSuccess();
    }

    @Override
    public void cancel() {
        super.cancel();
    }

    @Override
    public List<String> getShowInfo() {
        if (null == loadJob) {
            return new ArrayList<>();
        }
        List<String> taskInfos = new ArrayList<>();
        taskInfos.add(loadJob.getLabel());
        taskInfos.add(loadJob.getState().name());
        // create time
        taskInfos.add(TimeUtils.longToTimeString(loadJob.getCreateTimestamp()));
        // etl end time
        taskInfos.add(TimeUtils.longToTimeString(loadJob.getFinishTimestamp()));

        if (loadJob.getFailMsg() == null) {
            taskInfos.add(FeConstants.null_string);
        } else {
            taskInfos.add("type:" + loadJob.getFailMsg().getCancelType() + "; msg:" + loadJob.getFailMsg().getMsg());
        }

        // tracking url
        taskInfos.add(loadJob.getLoadingStatus().getTrackingUrl());
        taskInfos.add(loadJob.getLoadStatistic().toJson());
        // transaction id
        taskInfos.add(String.valueOf(loadJob.getTransactionId()));
        // error tablets
        return taskInfos;
    }

    private static final ShowResultSetMetaData TASK_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("JobId", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Label", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Status", ScalarType.createVarchar(20)))
                    .addColumn(new Column("CreateTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("FinishTimeMs", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Duration", ScalarType.createVarchar(20)))
                    .addColumn(new Column("ExecuteSql", ScalarType.createVarchar(20)))
                    .build();

}
