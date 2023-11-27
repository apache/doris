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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.job.task.AbstractTask;
import org.apache.doris.mtmv.MTMVCacheManager;
import org.apache.doris.mtmv.MTMVRelation;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.TUniqueId;

import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.UUID;

public class MTMVTask extends AbstractTask {
    private static final Logger LOG = LogManager.getLogger(MTMVTask.class);
    public static final Long MAX_HISTORY_TASKS_NUM = 100L;

    @SerializedName(value = "di")
    private long dbId;
    @SerializedName(value = "mi")
    private long mtmvId;
    @SerializedName("sql")
    private String sql;

    private MTMV mtmv;
    private MTMVRelation relation;
    private StmtExecutor executor;

    public MTMVTask(long dbId, long mtmvId) {
        this.dbId = dbId;
        this.mtmvId = mtmvId;
    }

    @Override
    public void run() throws JobException {
        try {
            ConnectContext ctx = createContext();
            TUniqueId queryId = generateQueryId();
            // Every time a task is run, the relation is regenerated because baseTables and baseViews may change,
            // such as deleting a table and creating a view with the same name
            relation = MTMVCacheManager.generateMTMVRelation(mtmv, ctx);
            executor = new StmtExecutor(ctx, sql);
            executor.execute(queryId);
        } catch (Throwable e) {
            LOG.warn(e);
            throw new JobException(e);
        }
    }

    @Override
    public synchronized void onFail() throws JobException {
        super.onFail();
        after();
    }

    @Override
    public synchronized void onSuccess() throws JobException {
        super.onSuccess();
        after();
    }

    @Override
    public synchronized void cancel() throws JobException {
        super.cancel();
        if (executor != null) {
            executor.cancel();
        }
        after();
    }

    @Override
    public void before() throws JobException {
        super.before();
        try {
            Database db = Env.getCurrentInternalCatalog().getDbOrDdlException(dbId);
            mtmv = (MTMV) db.getTableOrMetaException(mtmvId, TableType.MATERIALIZED_VIEW);
            sql = generateSql(mtmv);
        } catch (UserException e) {
            LOG.warn(e);
            throw new JobException(e);
        }
    }

    @Override
    public List<String> getShowInfo() {
        List<String> data = Lists.newArrayList();
        data.add(super.getJobId() + "");
        data.add(super.getTaskId() + "");
        data.add(super.getStatus() + "");
        data.add(TimeUtils.longToTimeString(super.getCreateTimeMs()));
        data.add(TimeUtils.longToTimeString(super.getStartTimeMs()));
        data.add(TimeUtils.longToTimeString(super.getFinishTimeMs()));
        data.add(String.valueOf(super.getFinishTimeMs() - super.getStartTimeMs()));
        data.add(sql);
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

    private ConnectContext createContext() throws AnalysisException {
        ConnectContext ctx = new ConnectContext();
        ctx.setEnv(Env.getCurrentEnv());
        ctx.setCluster(SystemInfoService.DEFAULT_CLUSTER);
        ctx.setQualifiedUser(Auth.ADMIN_USER);
        ctx.setCurrentUserIdentity(UserIdentity.ADMIN);
        ctx.getState().reset();
        ctx.setThreadLocalInfo();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(mtmv.getEnvInfo().getCtlId());
        ctx.changeDefaultCatalog(catalog.getName());
        ctx.setDatabase(catalog.getDbOrAnalysisException(mtmv.getEnvInfo().getDbId()).getFullName());
        ctx.getSessionVariable().enableFallbackToOriginalPlanner = false;
        return ctx;
    }

    private TUniqueId generateQueryId() {
        UUID taskId = UUID.randomUUID();
        return new TUniqueId(taskId.getMostSignificantBits(), taskId.getLeastSignificantBits());
    }

    private void after() {
        Env.getCurrentEnv()
                .addMTMVTaskResult(new TableNameInfo(mtmv.getQualifiedDbName(), mtmv.getName()), this, relation);
        mtmv = null;
        relation = null;
        executor = null;
    }
}
