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

package org.apache.doris.scheduler.executor;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * we use this executor to execute sql job
 */
@Slf4j
public class SqlJobExecutor implements JobExecutor {

    @Getter
    @Setter
    @SerializedName(value = "sql")
    private String sql;

    public SqlJobExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public ExecutorResult<String> execute(Job job) throws JobException {
        long taskStartTime = System.currentTimeMillis();
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(ClusterNamespace.getClusterNameFromFullName(job.getDbName()));
        ctx.setDatabase(job.getDbName());
        ctx.setQualifiedUser(job.getUser());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getUser(), "%"));
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        // run insert overwrite table need this var
        ctx.getSessionVariable().setEnableNereidsDML(true);
        String taskIdString = UUID.randomUUID().toString();
        UUID taskId = UUID.fromString(taskIdString);
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        ctx.setQueryId(queryId);
        ExecutorResult executorResult;
        try {
            StmtExecutor executor = new StmtExecutor(ctx, sql);
            executor.execute(queryId);
            String result = convertExecuteResult(ctx, taskIdString);

            executorResult = new ExecutorResult<>(result, true, null, sql);
        } catch (Exception e) {
            executorResult = new ExecutorResult<>(null, false, e.getMessage(), sql);
            log.warn("execute sql job failed, job id :{}, sql: {}, error: {}", job.getJobId(), sql, e);
            return new ExecutorResult<>(null, false, e.getMessage(), sql);
        }
        long lastRefreshFinishedTime = System.currentTimeMillis();
        afterExecute(executorResult, taskStartTime, lastRefreshFinishedTime, taskIdString);
        return executorResult;
    }

    private String convertExecuteResult(ConnectContext ctx, String queryId) throws JobException {
        if (null == ctx.getState()) {
            throw new JobException("execute sql job failed, sql: " + sql + ", error:  response state is null");
        }
        if (null != ctx.getState().getErrorCode()) {
            throw new JobException("error code: " + ctx.getState().getErrorCode() + ", error msg: "
                    + ctx.getState().getErrorMessage());
        }

        return "queryId:" + queryId + ",affectedRows : " + ctx.getState().getAffectedRows() + ", warningRows: "
                + ctx.getState().getWarningRows() + ",infoMsg" + ctx.getState().getInfoMessage();
    }

    protected void afterExecute(ExecutorResult result, long taskStartTime, long lastRefreshFinishedTime,
            String taskId) {
    }
}
