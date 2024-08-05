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

import org.apache.doris.analysis.AdminCancelRebalanceDiskStmt;
import org.apache.doris.analysis.AdminCancelRepairTableStmt;
import org.apache.doris.analysis.AdminCheckTabletsStmt;
import org.apache.doris.analysis.AdminCleanTrashStmt;
import org.apache.doris.analysis.AdminCompactTableStmt;
import org.apache.doris.analysis.AdminRebalanceDiskStmt;
import org.apache.doris.analysis.AdminRepairTableStmt;
import org.apache.doris.analysis.AdminSetConfigStmt;
import org.apache.doris.analysis.AdminSetPartitionVersionStmt;
import org.apache.doris.analysis.AdminSetReplicaStatusStmt;
import org.apache.doris.analysis.AdminSetReplicaVersionStmt;
import org.apache.doris.analysis.AdminSetTableStatusStmt;
import org.apache.doris.analysis.AlterCatalogCommentStmt;
import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.AlterColocateGroupStmt;
import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterDatabasePropertyStmt;
import org.apache.doris.analysis.AlterDatabaseQuotaStmt;
import org.apache.doris.analysis.AlterDatabaseRename;
import org.apache.doris.analysis.AlterJobStatusStmt;
import org.apache.doris.analysis.AlterPolicyStmt;
import org.apache.doris.analysis.AlterRepositoryStmt;
import org.apache.doris.analysis.AlterResourceStmt;
import org.apache.doris.analysis.AlterRoleStmt;
import org.apache.doris.analysis.AlterRoutineLoadStmt;
import org.apache.doris.analysis.AlterSqlBlockRuleStmt;
import org.apache.doris.analysis.AlterSystemStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.AlterUserStmt;
import org.apache.doris.analysis.AlterViewStmt;
import org.apache.doris.analysis.AlterWorkloadGroupStmt;
import org.apache.doris.analysis.AlterWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.BackupStmt;
import org.apache.doris.analysis.CancelAlterSystemStmt;
import org.apache.doris.analysis.CancelAlterTableStmt;
import org.apache.doris.analysis.CancelBackupStmt;
import org.apache.doris.analysis.CancelCloudWarmUpStmt;
import org.apache.doris.analysis.CancelExportStmt;
import org.apache.doris.analysis.CancelJobTaskStmt;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CleanLabelStmt;
import org.apache.doris.analysis.CleanProfileStmt;
import org.apache.doris.analysis.CleanQueryStatsStmt;
import org.apache.doris.analysis.CopyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.CreateDataSyncJobStmt;
import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateEncryptKeyStmt;
import org.apache.doris.analysis.CreateFileStmt;
import org.apache.doris.analysis.CreateFunctionStmt;
import org.apache.doris.analysis.CreateJobStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreatePolicyStmt;
import org.apache.doris.analysis.CreateRepositoryStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.CreateRoleStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.CreateSqlBlockRuleStmt;
import org.apache.doris.analysis.CreateStageStmt;
import org.apache.doris.analysis.CreateStorageVaultStmt;
import org.apache.doris.analysis.CreateTableAsSelectStmt;
import org.apache.doris.analysis.CreateTableLikeStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.CreateUserStmt;
import org.apache.doris.analysis.CreateViewStmt;
import org.apache.doris.analysis.CreateWorkloadGroupStmt;
import org.apache.doris.analysis.CreateWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DropAnalyzeJobStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropEncryptKeyStmt;
import org.apache.doris.analysis.DropFileStmt;
import org.apache.doris.analysis.DropFunctionStmt;
import org.apache.doris.analysis.DropMaterializedViewStmt;
import org.apache.doris.analysis.DropPolicyStmt;
import org.apache.doris.analysis.DropRepositoryStmt;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.analysis.DropRoleStmt;
import org.apache.doris.analysis.DropSqlBlockRuleStmt;
import org.apache.doris.analysis.DropStageStmt;
import org.apache.doris.analysis.DropStatsStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.DropWorkloadGroupStmt;
import org.apache.doris.analysis.DropWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.InstallPluginStmt;
import org.apache.doris.analysis.KillAnalysisJobStmt;
import org.apache.doris.analysis.PauseRoutineLoadStmt;
import org.apache.doris.analysis.PauseSyncJobStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.RefreshCatalogStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshLdapStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.RestoreStmt;
import org.apache.doris.analysis.ResumeRoutineLoadStmt;
import org.apache.doris.analysis.ResumeSyncJobStmt;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.SetDefaultStorageVaultStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.StopRoutineLoadStmt;
import org.apache.doris.analysis.StopSyncJobStmt;
import org.apache.doris.analysis.SyncStmt;
import org.apache.doris.analysis.TruncateTableStmt;
import org.apache.doris.analysis.UninstallPluginStmt;
import org.apache.doris.analysis.UnsetDefaultStorageVaultStmt;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.Env;
import org.apache.doris.cloud.catalog.CloudEnv;
import org.apache.doris.cloud.load.CloudLoadManager;
import org.apache.doris.cloud.load.CopyJob;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.util.ProfileManager;
import org.apache.doris.job.exception.JobException;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.JobState;
import org.apache.doris.load.loadv2.LoadJob;
import org.apache.doris.load.sync.SyncJobManager;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.persist.CleanQueryStatsInfo;
import org.apache.doris.statistics.StatisticsRepository;

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
        if (ddlStmt instanceof CreateDbStmt) {
            env.createDb((CreateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof DropDbStmt) {
            env.dropDb((DropDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFunctionStmt) {
            env.createFunction((CreateFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFunctionStmt) {
            env.dropFunction((DropFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateEncryptKeyStmt) {
            EncryptKeyHelper.createEncryptKey((CreateEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropEncryptKeyStmt) {
            EncryptKeyHelper.dropEncryptKey((DropEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            env.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableLikeStmt) {
            env.createTableLike((CreateTableLikeStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableAsSelectStmt) {
            env.createTableAsSelect((CreateTableAsSelectStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            env.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMaterializedViewStmt) {
            env.createMaterializedView((CreateMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            env.alterTable((AlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterColumnStatsStmt) {
            StatisticsRepository.alterColumnStatistics((AlterColumnStatsStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterViewStmt) {
            env.alterView((AlterViewStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelAlterTableStmt) {
            env.cancelAlter((CancelAlterTableStmt) ddlStmt);
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
        } else if (ddlStmt instanceof PauseRoutineLoadStmt) {
            env.getRoutineLoadManager().pauseRoutineLoadJob((PauseRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof ResumeRoutineLoadStmt) {
            env.getRoutineLoadManager().resumeRoutineLoadJob((ResumeRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof StopRoutineLoadStmt) {
            env.getRoutineLoadManager().stopRoutineLoadJob((StopRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterRoutineLoadStmt) {
            env.getRoutineLoadManager().alterRoutineLoadJob((AlterRoutineLoadStmt) ddlStmt);
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
        } else if (ddlStmt instanceof CancelJobTaskStmt) {
            CancelJobTaskStmt stmt = (CancelJobTaskStmt) ddlStmt;
            try {
                env.getJobManager().cancelTaskById(stmt.getJobName(), stmt.getTaskId());
            } catch (Exception e) {
                throw new DdlException(e.getMessage());
            }
        } else if (ddlStmt instanceof CreateUserStmt) {
            CreateUserStmt stmt = (CreateUserStmt) ddlStmt;
            env.getAuth().createUser(stmt);
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            env.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof GrantStmt) {
            GrantStmt stmt = (GrantStmt) ddlStmt;
            env.getAuth().grant(stmt);
        } else if (ddlStmt instanceof RevokeStmt) {
            RevokeStmt stmt = (RevokeStmt) ddlStmt;
            env.getAuth().revoke(stmt);
        } else if (ddlStmt instanceof CreateRoleStmt) {
            env.getAuth().createRole((CreateRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterRoleStmt) {
            env.getAuth().alterRole((AlterRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRoleStmt) {
            env.getAuth().dropRole((DropRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            env.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSystemStmt) {
            AlterSystemStmt stmt = (AlterSystemStmt) ddlStmt;
            env.alterCluster(stmt);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            env.cancelAlterCluster(stmt);
        } else if (ddlStmt instanceof AlterDatabaseQuotaStmt) {
            env.alterDatabaseQuota((AlterDatabaseQuotaStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabaseRename) {
            env.renameDatabase((AlterDatabaseRename) ddlStmt);
        } else if (ddlStmt instanceof RecoverDbStmt) {
            env.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            env.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            env.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateViewStmt) {
            env.createView((CreateViewStmt) ddlStmt);
        } else if (ddlStmt instanceof BackupStmt) {
            env.backup((BackupStmt) ddlStmt);
        } else if (ddlStmt instanceof RestoreStmt) {
            env.restore((RestoreStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelBackupStmt) {
            env.cancelBackup((CancelBackupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRepositoryStmt) {
            env.getBackupHandler().createRepository((CreateRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRepositoryStmt) {
            env.getBackupHandler().dropRepository((DropRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return;
        } else if (ddlStmt instanceof TruncateTableStmt) {
            env.truncateTable((TruncateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRepairTableStmt) {
            env.getTabletChecker().repairTable((AdminRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRepairTableStmt) {
            env.getTabletChecker().cancelRepairTable((AdminCancelRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCompactTableStmt) {
            env.compactTable((AdminCompactTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetConfigStmt) {
            env.setConfig((AdminSetConfigStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetTableStatusStmt) {
            env.setTableStatus((AdminSetTableStatusStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFileStmt) {
            env.getSmallFileMgr().createFile((CreateFileStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFileStmt) {
            env.getSmallFileMgr().dropFile((DropFileStmt) ddlStmt);
        } else if (ddlStmt instanceof InstallPluginStmt) {
            env.installPlugin((InstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            env.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCheckTabletsStmt) {
            env.checkTablets((AdminCheckTabletsStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetReplicaStatusStmt) {
            env.setReplicaStatus((AdminSetReplicaStatusStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetReplicaVersionStmt) {
            env.setReplicaVersion((AdminSetReplicaVersionStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetPartitionVersionStmt) {
            env.setPartitionVersion((AdminSetPartitionVersionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateResourceStmt) {
            env.getResourceMgr().createResource((CreateResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof DropResourceStmt) {
            env.getResourceMgr().dropResource((DropResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateWorkloadGroupStmt) {
            env.getWorkloadGroupMgr().createWorkloadGroup((CreateWorkloadGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkloadGroupStmt) {
            env.getWorkloadGroupMgr().dropWorkloadGroup((DropWorkloadGroupStmt) ddlStmt);
        }  else if (ddlStmt instanceof CreateWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().createWorkloadSchedPolicy((CreateWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().alterWorkloadSchedPolicy((AlterWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().dropWorkloadSchedPolicy((DropWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateDataSyncJobStmt) {
            CreateDataSyncJobStmt createSyncJobStmt = (CreateDataSyncJobStmt) ddlStmt;
            SyncJobManager syncJobMgr = env.getSyncJobManager();
            if (!syncJobMgr.isJobNameExist(createSyncJobStmt.getDbName(), createSyncJobStmt.getJobName())) {
                syncJobMgr.addDataSyncJob((CreateDataSyncJobStmt) ddlStmt);
            } else {
                throw new DdlException("The syncJob with jobName '" + createSyncJobStmt.getJobName() + "' in database ["
                        + createSyncJobStmt.getDbName() + "] is already exists.");
            }
        } else if (ddlStmt instanceof ResumeSyncJobStmt) {
            env.getSyncJobManager().resumeSyncJob((ResumeSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof PauseSyncJobStmt) {
            env.getSyncJobManager().pauseSyncJob((PauseSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof StopSyncJobStmt) {
            env.getSyncJobManager().stopSyncJob((StopSyncJobStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCleanTrashStmt) {
            env.cleanTrash((AdminCleanTrashStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRebalanceDiskStmt) {
            env.getTabletScheduler().rebalanceDisk((AdminRebalanceDiskStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRebalanceDiskStmt) {
            env.getTabletScheduler().cancelRebalanceDisk((AdminCancelRebalanceDiskStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().createSqlBlockRule((CreateSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().alterSqlBlockRule((AlterSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropSqlBlockRuleStmt) {
            env.getSqlBlockRuleMgr().dropSqlBlockRule((DropSqlBlockRuleStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabasePropertyStmt) {
            env.alterDatabaseProperty((AlterDatabasePropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshTableStmt) {
            env.getRefreshManager().handleRefreshTable((RefreshTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshDbStmt) {
            env.getRefreshManager().handleRefreshDb((RefreshDbStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterResourceStmt) {
            env.getResourceMgr().alterResource((AlterResourceStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterColocateGroupStmt) {
            env.getColocateTableIndex().alterColocateGroup((AlterColocateGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterWorkloadGroupStmt) {
            env.getWorkloadGroupMgr().alterWorkloadGroup((AlterWorkloadGroupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreatePolicyStmt) {
            env.getPolicyMgr().createPolicy((CreatePolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof DropPolicyStmt) {
            env.getPolicyMgr().dropPolicy((DropPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterPolicyStmt) {
            env.getPolicyMgr().alterPolicy((AlterPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateCatalogStmt) {
            env.getCatalogMgr().createCatalog((CreateCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof DropCatalogStmt) {
            env.getCatalogMgr().dropCatalog((DropCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterCatalogNameStmt) {
            env.getCatalogMgr().alterCatalogName((AlterCatalogNameStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterCatalogCommentStmt) {
            env.getCatalogMgr().alterCatalogComment((AlterCatalogCommentStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterCatalogPropertyStmt) {
            env.getCatalogMgr().alterCatalogProps((AlterCatalogPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof CleanLabelStmt) {
            env.getLoadManager().cleanLabel((CleanLabelStmt) ddlStmt);
        } else if (ddlStmt instanceof DropMaterializedViewStmt) {
            env.dropMaterializedView((DropMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshCatalogStmt) {
            env.getRefreshManager().handleRefreshCatalog((RefreshCatalogStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshLdapStmt) {
            env.getAuth().refreshLdap((RefreshLdapStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterUserStmt) {
            env.getAuth().alterUser((AlterUserStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelCloudWarmUpStmt) {
            if (Config.isCloudMode()) {
                CancelCloudWarmUpStmt stmt = (CancelCloudWarmUpStmt) ddlStmt;
                ((CloudEnv) env).cancelCloudWarmUp(stmt);
            }
        } else if (ddlStmt instanceof CleanProfileStmt) {
            ProfileManager.getInstance().cleanProfile();
        } else if (ddlStmt instanceof DropStatsStmt) {
            env.getAnalysisManager().dropStats((DropStatsStmt) ddlStmt);
        } else if (ddlStmt instanceof KillAnalysisJobStmt) {
            env.getAnalysisManager().handleKillAnalyzeStmt((KillAnalysisJobStmt) ddlStmt);
        } else if (ddlStmt instanceof CleanQueryStatsStmt) {
            CleanQueryStatsStmt stmt = (CleanQueryStatsStmt) ddlStmt;
            CleanQueryStatsInfo cleanQueryStatsInfo = null;
            switch (stmt.getScope()) {
                case ALL:
                    cleanQueryStatsInfo = new CleanQueryStatsInfo(
                            CleanQueryStatsStmt.Scope.ALL, env.getCurrentCatalog().getName(), null, null);
                    break;
                case DB:
                    cleanQueryStatsInfo = new CleanQueryStatsInfo(CleanQueryStatsStmt.Scope.DB,
                            env.getCurrentCatalog().getName(), stmt.getDbName(), null);
                    break;
                case TABLE:
                    cleanQueryStatsInfo = new CleanQueryStatsInfo(CleanQueryStatsStmt.Scope.TABLE,
                            env.getCurrentCatalog().getName(), stmt.getDbName(), stmt.getTableName().getTbl());
                    break;
                default:
                    throw new DdlException("Unknown scope: " + stmt.getScope());
            }
            env.cleanQueryStats(cleanQueryStatsInfo);
        } else if (ddlStmt instanceof DropAnalyzeJobStmt) {
            DropAnalyzeJobStmt analyzeJobStmt = (DropAnalyzeJobStmt) ddlStmt;
            Env.getCurrentEnv().getAnalysisManager().dropAnalyzeJob(analyzeJobStmt);
        } else if (ddlStmt instanceof AlterRepositoryStmt) {
            env.getBackupHandler().alterRepository((AlterRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateStorageVaultStmt) {
            env.getStorageVaultMgr().createStorageVaultResource((CreateStorageVaultStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateStageStmt) {
            ((CloudEnv) env).createStage((CreateStageStmt) ddlStmt);
        } else if (ddlStmt instanceof DropStageStmt) {
            ((CloudEnv) env).dropStage((DropStageStmt) ddlStmt);
        } else if (ddlStmt instanceof CopyStmt) {
            executeCopyStmt(env, (CopyStmt) ddlStmt);
        } else if (ddlStmt instanceof SetDefaultStorageVaultStmt) {
            env.getStorageVaultMgr().setDefaultStorageVault((SetDefaultStorageVaultStmt) ddlStmt);
        } else if (ddlStmt instanceof UnsetDefaultStorageVaultStmt) {
            env.getStorageVaultMgr().unsetDefaultStorageVault();
        } else {
            LOG.warn("Unkown statement " + ddlStmt.getClass());
            throw new DdlException("Unknown statement.");
        }
    }

    private static void executeCopyStmt(Env env, CopyStmt copyStmt) throws Exception {
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
                copyStmt.getAnalyzer().getContext().setState(queryState);
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
                copyStmt.getAnalyzer().getContext().setState(queryState);
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
        copyStmt.getAnalyzer().getContext().setState(queryState);
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

        if (ddlStmt instanceof AdminSetConfigStmt) {
            if (!ConnectContext.get().getCurrentUserIdentity().getUser().equals(Auth.ROOT_USER)) {
                LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
                throw new DdlException("Unsupported operation");
            }
        }

        if (ddlStmt instanceof BackupStmt
                || ddlStmt instanceof RestoreStmt
                || ddlStmt instanceof CancelBackupStmt
                || ddlStmt instanceof CreateRepositoryStmt
                || ddlStmt instanceof DropRepositoryStmt
                || ddlStmt instanceof AdminRepairTableStmt
                || ddlStmt instanceof AdminCancelRepairTableStmt
                || ddlStmt instanceof AdminCompactTableStmt
                || ddlStmt instanceof AdminCheckTabletsStmt
                || ddlStmt instanceof AdminSetReplicaStatusStmt
                || ddlStmt instanceof AdminCleanTrashStmt
                || ddlStmt instanceof AdminRebalanceDiskStmt
                || ddlStmt instanceof AdminCancelRebalanceDiskStmt
                || ddlStmt instanceof AlterResourceStmt
                || ddlStmt instanceof AlterPolicyStmt
                || ddlStmt instanceof AlterSystemStmt) {
            LOG.info("stmt={}, not supported in cloud mode", ddlStmt.toString());
            throw new DdlException("Unsupported operation");
        }
    }
}
