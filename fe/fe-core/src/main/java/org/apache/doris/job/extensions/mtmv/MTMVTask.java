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

package org.apache.doris.job.extensions.mtmv;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mtmv.MTMVTaskResult;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.scheduler.exception.JobException;
import org.apache.doris.thrift.TUniqueId;

import com.google.gson.annotations.SerializedName;
import lombok.extern.slf4j.Slf4j;

/**
 * mtmv task
 */
@Slf4j
public class MTMVTask extends AbstractTask {
    public static final Long MAX_HISTORY_TASKS_NUM = 100L;

    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "mi")
    private long mtmvId;

    private MTMV mtmv;
    private String sql;
    private MTMVCache cache;

    @Override
    public void before() {
        super.before();
    }

    public MTMVTask(String dbName, long mtmvId) {
        this.dbName = dbName;
        this.mtmvId = mtmvId;
    }

    @Override
    public void run() {
        beforeExecute();
        long taskStartTime = System.currentTimeMillis();
        ConnectContext ctx = createContext(job, mtmv);
        String taskIdString = generateTaskId();
        TUniqueId queryId = generateQueryId(taskIdString);

        cache = MTMVCacheManager.generateMTMVCache(mtmv);
        StmtExecutor executor = new StmtExecutor(ctx, sql);
        executor.execute(queryId);
    }

    @Override
    public void onFail() {
        super.onFail();
        try {
            Env.getCurrentEnv()
                    .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this,
                            cache);
        } catch (UserException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onSuccess() {
        super.onSuccess();
        try {
            Env.getCurrentEnv()
                    .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this,
                            cache);
        } catch (UserException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cancel() {
        super.cancel();
    }

    private void beforeExecute() {
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbName);
            mtmv = (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
            sql = generateSql(mtmv);
            MTMVStatus status = new MTMVStatus(MTMVRefreshState.REFRESHING);
            Env.getCurrentEnv().alterMTMVStatus(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), status);
        } catch (UserException e) {
            e.printStackTrace();
        }
    }

    private ConnectContext createContext(Job job, MTMV mtmv) {
        ConnectContext context = super.createContext(job);
        context.changeDefaultCatalog(mtmv.getEnvInfo().getCtlName());
        context.setDatabase(mtmv.getEnvInfo().getDbName());
        context.getSessionVariable().enableFallbackToOriginalPlanner = false;
        return context;
    }

    private static String generateSql(MTMV mtmv) {
        StringBuilder builder = new StringBuilder();
        builder.append("INSERT OVERWRITE TABLE ");
        builder.append(mtmv.getDatabase().getCatalog().getName());
        builder.append(".");
        builder.append(ClusterNamespace.getNameFromFullName(mtmv.getQualifiedDbName()));
        builder.append(".");
        builder.append(mtmv.getName());
        builder.append(" ");
        builder.append(mtmv.getQuerySql());
        return builder.toString();
    }

}
