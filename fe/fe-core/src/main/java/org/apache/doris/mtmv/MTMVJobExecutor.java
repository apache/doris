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

import org.apache.doris.nereids.trees.plans.commands.info.MTMVTaskResult;
import org.apache.doris.scheduler.executor.SqlJobExecutor;
import org.apache.doris.scheduler.job.ExecutorResult;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MTMVJobExecutor extends SqlJobExecutor {
    private static final Logger LOG = LogManager.getLogger(MTMVJobExecutor.class);

    @Getter
    @Setter
    @SerializedName(value = "dn")
    private String dbName;

    @Getter
    @Setter
    @SerializedName(value = "tn")
    private String tableName;

    public MTMVJobExecutor(String dbName, String tableName, String sql) {
        super(sql);
        this.dbName = dbName;
        this.tableName = tableName;
    }

    @Override
    protected void afterExecute(ExecutorResult result, long taskStartTime, long lastRefreshFinishedTime, String taskId) {
        try {
            MTMVTaskResult taskResult = new MTMVTaskResult(result.getErrorMsg(), result.getExecutorSql(), result.isSuccess(),
                    taskStartTime, lastRefreshFinishedTime, taskId);
            // Env.getCurrentEnv().alterMTMVStatus(new TableNameInfo(dbName, tableName), taskResult);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.warn("afterExecute failed: {} ", e.getMessage());
        }
    }
}
