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

package org.apache.doris.mtmv;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.scheduler.executor.AbstractJobExecutor;
import org.apache.doris.scheduler.job.ExecutorResult;
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.UUID;

public class MTMVJobExecutor extends AbstractJobExecutor<MTMVTaskResult, MTMVTaskParams> {
    private static final Logger LOG = LogManager.getLogger(MTMVJobExecutor.class);

    @Getter
    @Setter
    @SerializedName(value = "dn")
    private String dbName;

    @Getter
    @Setter
    @SerializedName(value = "mi")
    private long mtmvId;

    public MTMVJobExecutor(String dbName, long mtmvId) {
        this.dbName = dbName;
        this.mtmvId = mtmvId;
    }

    protected void afterExecute(ExecutorResult result, long taskStartTime, long lastRefreshFinishedTime,
            String taskId) {
        try {
            MTMVTaskResult taskResult = new MTMVTaskResult(result.getErrorMsg(), result.getExecutorSql(),
                    result.isSuccess(),
                    taskStartTime, lastRefreshFinishedTime, taskId);
            System.out.println(taskResult);
            // Env.getCurrentEnv().alterMTMVStatus(new TableNameInfo(dbName, tableName), taskResult);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("afterExecute failed: {} ", e.getMessage());
        }
    }

    @Override
    public ExecutorResult<MTMVTaskResult> execute(Job job, MTMVTaskParams dataContext) throws JobException {
        MTMV mtmv;
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
            mtmv = (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
        } catch (MetaNotFoundException | DdlException e) {
            throw new JobException(e);
        }

        long taskStartTime = System.currentTimeMillis();

        ConnectContext ctx = createContext(job,mtmv);


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
            LOG.warn("execute sql job failed, job id :{}, sql: {}, error: {}", job.getJobId(), sql, e);
            return new ExecutorResult<>(null, false, e.getMessage(), sql);
        }
        long lastRefreshFinishedTime = System.currentTimeMillis();
        afterExecute(executorResult, taskStartTime, lastRefreshFinishedTime, taskIdString);
        return executorResult;
    }

    private ConnectContext createContext(Job job, MTMV mtmv) {
        ConnectContext context = super.createContext(job);
        context.changeDefaultCatalog(mtmv.getEnvInfo().getCtlName());
        context.setDatabase(mtmv.getEnvInfo().getDbName());
        return context;
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
}
