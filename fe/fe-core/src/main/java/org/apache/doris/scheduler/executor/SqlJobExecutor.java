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
import org.apache.doris.scheduler.job.Job;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * we use this executor to execute sql job
 * @param <QueryState> the state of sql job, we can record the state of sql job
 */
@Slf4j
public class SqlJobExecutor<QueryState> implements JobExecutor {

    @Getter
    @Setter
    @SerializedName(value = "sql")
    private String sql;

    public SqlJobExecutor(String sql) {
        this.sql = sql;
    }

    @Override
    public QueryState execute(Job job) throws JobException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(ClusterNamespace.getClusterNameFromFullName(job.getDbName()));
        ctx.setDatabase(job.getDbName());
        ctx.setQualifiedUser(job.getUser());
        ctx.setCurrentUserIdentity(UserIdentity.createAnalyzedUserIdentWithIp(job.getUser(), "%"));
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        String taskIdString = UUID.randomUUID().toString();
        UUID taskId = UUID.fromString(taskIdString);
        TUniqueId queryId = new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
        ctx.setQueryId(queryId);
        try {
            StmtExecutor executor = new StmtExecutor(ctx, sql);
            executor.execute(queryId);
            log.debug("execute sql job success, sql: {}, state is: {}", sql, ctx.getState());
            return (QueryState) ctx.getState();
        } catch (Exception e) {
            log.warn("execute sql job failed, sql: {}, error: {}", sql, e);
            throw new JobException("execute sql job failed, sql: " + sql + ", error: " + e.getMessage());
        }

    }
}
