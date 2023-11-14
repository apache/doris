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

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.UserException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVCache;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVRefreshEnum.MTMVRefreshState;
import org.apache.doris.mtmv.MTMVStatus;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.List;
import java.util.UUID;

public class MTMVTask extends AbstractTask {
    public static final Long MAX_HISTORY_TASKS_NUM = 100L;

    @SerializedName(value = "dn")
    private String dbName;
    @SerializedName(value = "mi")
    private long mtmvId;
    @SerializedName("sql")
    private String sql;

    private MTMV mtmv;
    private MTMVCache cache;

    public MTMVTask(String dbName, long mtmvId) {
        this.dbName = dbName;
        this.mtmvId = mtmvId;
    }

    @Override
    public void run() {
        ConnectContext ctx = createContext();
        TUniqueId queryId = generateQueryId();

        cache = MTMVCacheManager.generateMTMVCache(mtmv);
        StmtExecutor executor = new StmtExecutor(ctx, sql);
        try {
            executor.execute(queryId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void onFail() {
        super.onFail();
        addTaskResult();
    }

    @Override
    public void onSuccess() {
        super.onSuccess();
        addTaskResult();
    }

    @Override
    public void cancel() {
        super.cancel();
        addTaskResult();
    }

    @Override
    public void before() {
        super.before();
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

    @Override
    public List<String> getShowInfo() {
        List<String> data = Lists.newArrayList();
        data.add(super.getJobId() + "");
        data.add(super.getTaskId() + "");
        return data;
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

    private ConnectContext createContext() {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setQualifiedUser(Auth.ROOT_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ROOT);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        ctx.changeDefaultCatalog(mtmv.getEnvInfo().getCtlName());
        ctx.setDatabase(mtmv.getEnvInfo().getDbName());
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        return ctx;
    }

    private TUniqueId generateQueryId() {
        UUID taskId = UUID.randomUUID();
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    private void addTaskResult() {
        try {
            Env.getCurrentEnv()
                    .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this,
                            cache);
        } catch (UserException e) {
            e.printStackTrace();
        }
    }
}
