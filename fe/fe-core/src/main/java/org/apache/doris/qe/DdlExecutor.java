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

package org.apache.doris.qe;

import org.apache.doris.analysis.AdminSetPartitionVersionStmt;
import org.apache.doris.analysis.AlterJobStatusStmt;
import org.apache.doris.analysis.AlterRepositoryStmt;
import org.apache.doris.analysis.AlterRoleStmt;
import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterWorkloadGroupStmt;
import org.apache.doris.analysis.AlterWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.CancelExportStmt;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateEncryptKeyStmt;
import org.apache.doris.analysis.CreateIndexPolicyStmt;
import org.apache.doris.analysis.CreateJobStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.analysis.DropIndexPolicyStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.DropWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.SyncStmt;
import org.apache.doris.analysis.UninstallPluginStmt;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.cloud.load.CopyJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Use for execute ddl.
 **/
public class DdlExecutor {
    private static final Logger LOG = LogManager.getLogger(DdlExecutor.class);

    /**
     * Execute ddl.
     **/
    public static void execute(Env env, DdlStmt ddlStmt) throws Exception {
        checkDdlStmtSupported(ddlStmt);
        if (ddlStmt instanceof DropFunctionStmt) {
            env.dropFunction((DropFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateEncryptKeyStmt) {
            EncryptKeyHelper.createEncryptKey((CreateEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            env.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            env.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMaterializedViewStmt) {
            env.createMaterializedView((CreateMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            env.alterTable((AlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelExportStmt) {
            env.getExportMgr().cancelExportJob((CancelExportStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelLoadStmt) {
            CancelLoadStmt cs = (CancelLoadStmt) ddlStmt;
            // cancel all
            try {
                env.getJobManager().cancelLoadJob(cs);
            } catch (JobException e) {
                env.getLoadManager().cancelLoadJob(cs);
            }
        } else if (ddlStmt instanceof CreateRoutineLoadStmt) {
            env.getRoutineLoadManager().createRoutineLoadJob((CreateRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateJobStmt) {
            try {
                env.getJobManager().registerJob(((CreateJobStmt) ddlStmt).getJobInstance());
            } catch (Exception e) {
                throw new DdlException(e.getMessage());
            }
        } else if (ddlStmt instanceof AlterJobStatusStmt) {
            AlterJobStatusStmt stmt = (AlterJobStatusStmt) ddlStmt;
            try {
                // drop job
                if (stmt.isDrop()) {
                    env.getJobManager().unregisterJob(stmt.getJobName(), stmt.isIfExists());
                    return;
                }
                // alter job status
                env.getJobManager().alterJobStatus(stmt.getJobName(), stmt.getJobStatus());
            } catch (Exception e) {
                throw new DdlException(e.getMessage());
            }
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            env.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof AlterRoleStmt) {
            env.getAuth().alterRole((AlterRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            env.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);

        } else if (ddlStmt instanceof RecoverDbStmt) {
            env.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            env.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            env.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return;
        } else if (ddlStmt instanceof InstallPluginStmt) {
            env.installPlugin((InstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            env.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetPartitionVersionStmt) {
            env.setPartitionVersion((AdminSetPartitionVersionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().createWorkloadSchedPolicy((CreateWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().alterWorkloadSchedPolicy((AlterWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().dropWorkloadSchedPolicy((DropWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().createSqlBlockRule((CreateSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().alterSqlBlockRule((AlterSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().dropSqlBlockRule((DropSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshTableStmt) {
            RefreshTableStmt refreshTableStmt = (RefreshTableStmt) ddlStmt;
            env.getRefreshManager().handleRefreshTable(refreshTableStmt.getCtl(), refreshTableStmt.getDbName(),
                    refreshTableStmt.getTblName(), false);
        } else if (ddlStmt instanceof RefreshDbStmt) {
            RefreshDbStmt refreshDbStmt = (RefreshDbStmt) ddlStmt;
            env.getRefreshManager().handleRefreshDb(refreshDbStmt.getCatalogName(), refreshDbStmt.getDbName());
        } else if (ddlStmt instanceof AlterWorkloadGroupStmt) {
            env.getWorkloadGroupMgr().alterWorkloadGroup((AlterWorkloadGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateIndexPolicyStmt) {
            env.getIndexPolicyMgr().createIndexPolicy((CreateIndexPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropIndexPolicyStmt) {
            env.getIndexPolicyMgr().dropIndexPolicy((DropIndexPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateCatalogStmt) {
            env.getCatalogMgr().createCatalog((CreateCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof DropCatalogStmt) {
            env.getCatalogMgr().dropCatalog((DropCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshCatalogStmt) {
            RefreshCatalogStmt refreshCatalogStmt = (RefreshCatalogStmt) ddlStmt;
            env.getRefreshManager()
                    .handleRefreshCatalog(refreshCatalogStmt.getCatalogName(), refreshCatalogStmt.isInvalidCache());
        } else if (ddlStmt instanceof AlterRepositoryStmt) {
            AlterRepositoryStmt alterRepositoryStmt = (AlterRepositoryStmt) ddlStmt;
            env.getBackupHandler().alterRepository(alterRepositoryStmt.getName(), alterRepositoryStmt.getProperties(),
                    false);
        } else if (ddlStmt instanceof CopyStmt) {
            executeCopyStmt(env, (CopyStmt) ddlStmt);
        } else {
            LOG.warn("Unkown statement " + ddlStmt.getClass());
            throw new DdlException("Unknown statement.");
        }
    }

    public static void executeCopyStmt(Env env, CopyStmt copyStmt) throws Exception {
        CopyJob job = (CopyJob) (((CloudLoadManager) env.getLoadManager()).createLoadJobFromStmt(copyStmt));
        if (!copyStmt.isAsync()) {
            // wait for execute finished
            waitJobCompleted(job);
            if (job.getState() == JobState.UNKNOWN || job.getState() == JobState.CANCELLED) {
                QueryState queryState = new QueryState();
                FailMsg failMsg = job.getFailMsg();
                EtlStatus loadingStatus = job.getLoadingStatus();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add(failMsg == null ? "" : failMsg.getCancelType().toString());
                entry.add(failMsg == null ? "" : failMsg.getMsg());
                entry.add("");
                entry.add("");
                entry.add("");
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
                ConnectContext.get().setState(queryState);
                return;
            } else if (job.getState() == JobState.FINISHED) {
                EtlStatus loadingStatus = job.getLoadingStatus();
                Map<String, String> counters = loadingStatus.getCounters();
                QueryState queryState = new QueryState();
                List<List<String>> result = Lists.newArrayList();
                List<String> entry = Lists.newArrayList();
                entry.add(job.getCopyId());
                entry.add(job.getState().toString());
                entry.add("");
                entry.add("");
                entry.add(counters.getOrDefault(LoadJob.DPP_NORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.DPP_ABNORMAL_ALL, "0"));
                entry.add(counters.getOrDefault(LoadJob.UNSELECTED_ROWS, "0"));
                entry.add(loadingStatus.getTrackingUrl());
                result.add(entry);
                queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
                ConnectContext.get().setState(queryState);
                return;
            }
        }
        QueryState queryState = new QueryState();
        List<List<String>> result = Lists.newArrayList();
        List<String> entry = Lists.newArrayList();
        entry.add(job.getCopyId());
        entry.add(job.getState().toString());
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        entry.add("");
        result.add(entry);
        queryState.setResultSet(new ShowResultSet(copyStmt.getMetaData(), result));
        ConnectContext.get().setState(queryState);
    }

    private static void waitJobCompleted(CopyJob job) throws InterruptedException {
        // check the job is completed or not.
        // sleep 10ms, 1000 times(10s)
        // sleep 100ms, 1000 times(100s + 10s = 110s)
        // sleep 1000ms, 1000 times(1000s + 110s = 1110s)
        // sleep 5000ms...
        long retry = 0;
        long currentInterval = 10;
        while (!job.isCompleted()) {
            Thread.sleep(currentInterval);
            if (retry > 3010) {
                continue;
            }
            retry++;
            if (retry > 3000) {
                currentInterval = 5000;
            } else if (retry > 2000) {
                currentInterval = 1000;
            } else if (retry > 1000) {
                currentInterval = 100;
            }
        }
    }

    private static void checkDdlStmtSupported(DdlStmt ddlStmt) throws DdlException {
        // check stmt has been supported in cloud mode
        if (Config.isNotCloudMode()) {
            return;
        }
    }
}
