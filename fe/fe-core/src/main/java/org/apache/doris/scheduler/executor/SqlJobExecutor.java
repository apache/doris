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

import java.util.Map;

/**
 * we use this executor to execute sql job
 */
@Getter
@Slf4j
public class SqlJobExecutor extends AbstractJobExecutor<String, Map<String, Object>> {

    @Setter
    @SerializedName(value = "sql")
    private String sql;

    public SqlJobExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public ExecutorResult<String> execute(Job job, Map<String, Object> dataContext) throws JobException {
        ConnectContext ctx = createContext(job);
        String taskIdString = generateTaskId();
        TUniqueId queryId = generateQueryId(taskIdString);
        try {
            StmtExecutor executor = new StmtExecutor(ctx, sql);
            executor.execute(queryId);
            String result = convertExecuteResult(ctx, taskIdString);
            return new ExecutorResult<>(result, true, null, sql);
        } catch (Exception e) {
            log.warn("execute sql job failed, job id :{}, sql: {}, error: {}", job.getJobId(), sql, e);
            return new ExecutorResult<>(null, false, e.getMessage(), sql);
        }

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
