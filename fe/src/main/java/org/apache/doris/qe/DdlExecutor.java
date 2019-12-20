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

import org.apache.doris.analysis.*;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.Load;

public class DdlExecutor {
    public static void execute(Catalog catalog, DdlStmt ddlStmt, String origStmt) throws DdlException, Exception {
        if (ddlStmt instanceof CreateClusterStmt) {
            CreateClusterStmt stmt = (CreateClusterStmt) ddlStmt;
            catalog.createCluster(stmt);
        } else if (ddlStmt instanceof AlterClusterStmt) {
            catalog.processModifyCluster((AlterClusterStmt) ddlStmt);
        } else if (ddlStmt instanceof DropClusterStmt) {
            catalog.dropCluster((DropClusterStmt) ddlStmt);
        } else if (ddlStmt instanceof MigrateDbStmt) {
            catalog.migrateDb((MigrateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof LinkDbStmt) {
            catalog.linkDb((LinkDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateDbStmt) {
            catalog.createDb((CreateDbStmt) ddlStmt);
        } else if (ddlStmt instanceof DropDbStmt) {
            catalog.dropDb((DropDbStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFunctionStmt) {
            catalog.createFunction((CreateFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFunctionStmt) {
            catalog.dropFunction((DropFunctionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            catalog.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            catalog.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            catalog.alterTable((AlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterViewStmt) {
            catalog.alterView((AlterViewStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelAlterTableStmt) {
            catalog.cancelAlter((CancelAlterTableStmt) ddlStmt);
        } else if (ddlStmt instanceof LoadStmt) {
            LoadStmt loadStmt = (LoadStmt) ddlStmt;
            EtlJobType jobType;
            if (loadStmt.getBrokerDesc() != null) {
                jobType = EtlJobType.BROKER;
            } else {
                if (Config.disable_hadoop_load) {
                    throw new DdlException("Load job by hadoop cluster is disabled."
                            + " Try using broker load. See 'help broker load;'");
                }
                jobType = EtlJobType.HADOOP;
            }
            if (loadStmt.getVersion().equals(Load.VERSION) || jobType == EtlJobType.HADOOP) {
                catalog.getLoadManager().createLoadJobV1FromStmt(loadStmt, jobType, System.currentTimeMillis());
            } else {
                catalog.getLoadManager().createLoadJobFromStmt(loadStmt, origStmt);
            }
        } else if (ddlStmt instanceof CancelLoadStmt) {
            if (catalog.getLoadInstance().isLabelExist(
                    ((CancelLoadStmt) ddlStmt).getDbName(), ((CancelLoadStmt) ddlStmt).getLabel())) {
                catalog.getLoadInstance().cancelLoadJob((CancelLoadStmt) ddlStmt);
            } else {
                catalog.getLoadManager().cancelLoadJob((CancelLoadStmt) ddlStmt);
            }
        } else if (ddlStmt instanceof CreateRoutineLoadStmt) {
            catalog.getRoutineLoadManager().createRoutineLoadJob((CreateRoutineLoadStmt) ddlStmt, origStmt);
        } else if (ddlStmt instanceof PauseRoutineLoadStmt) {
            catalog.getRoutineLoadManager().pauseRoutineLoadJob((PauseRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof ResumeRoutineLoadStmt) {
            catalog.getRoutineLoadManager().resumeRoutineLoadJob((ResumeRoutineLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof StopRoutineLoadStmt) {
            catalog.getRoutineLoadManager().stopRoutineLoadJob((StopRoutineLoadStmt) ddlStmt);
        }  else if (ddlStmt instanceof DeleteStmt) {
            catalog.getLoadInstance().delete((DeleteStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateUserStmt) {
            CreateUserStmt stmt = (CreateUserStmt) ddlStmt;
            catalog.getAuth().createUser(stmt);
        } else if (ddlStmt instanceof DropUserStmt) {
            DropUserStmt stmt = (DropUserStmt) ddlStmt;
            catalog.getAuth().dropUser(stmt);
        } else if (ddlStmt instanceof GrantStmt) {
            GrantStmt stmt = (GrantStmt) ddlStmt;
            catalog.getAuth().grant(stmt);
        } else if (ddlStmt instanceof RevokeStmt) {
            RevokeStmt stmt = (RevokeStmt) ddlStmt;
            catalog.getAuth().revoke(stmt);
        } else if (ddlStmt instanceof CreateRoleStmt) {
            catalog.getAuth().createRole((CreateRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRoleStmt) {
            catalog.getAuth().dropRole((DropRoleStmt) ddlStmt);
        } else if (ddlStmt instanceof SetUserPropertyStmt) {
            catalog.getAuth().updateUserProperty((SetUserPropertyStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterSystemStmt) {
            AlterSystemStmt stmt = (AlterSystemStmt) ddlStmt;
            catalog.alterCluster(stmt);
        } else if (ddlStmt instanceof CancelAlterSystemStmt) {
            CancelAlterSystemStmt stmt = (CancelAlterSystemStmt) ddlStmt;
            catalog.cancelAlterCluster(stmt);
        } else if (ddlStmt instanceof AlterDatabaseQuotaStmt) {
            catalog.alterDatabaseQuota((AlterDatabaseQuotaStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterDatabaseRename) {
            catalog.renameDatabase((AlterDatabaseRename) ddlStmt);
        } else if (ddlStmt instanceof RecoverDbStmt) {
            catalog.recoverDatabase((RecoverDbStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverTableStmt) {
            catalog.recoverTable((RecoverTableStmt) ddlStmt);
        } else if (ddlStmt instanceof RecoverPartitionStmt) {
            catalog.recoverPartition((RecoverPartitionStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateViewStmt) {
            catalog.createView((CreateViewStmt) ddlStmt);
        } else if (ddlStmt instanceof BackupStmt) {
            catalog.backup((BackupStmt) ddlStmt);
        } else if (ddlStmt instanceof RestoreStmt) {
            catalog.restore((RestoreStmt) ddlStmt);
        } else if (ddlStmt instanceof CancelBackupStmt) {
            catalog.cancelBackup((CancelBackupStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateRepositoryStmt) {
            catalog.getBackupHandler().createRepository((CreateRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof DropRepositoryStmt) {
            catalog.getBackupHandler().dropRepository((DropRepositoryStmt) ddlStmt);
        } else if (ddlStmt instanceof SyncStmt) {
            return;
        } else if (ddlStmt instanceof TruncateTableStmt) {
            catalog.truncateTable((TruncateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminRepairTableStmt) {
            catalog.getTabletChecker().repairTable((AdminRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminCancelRepairTableStmt) {
            catalog.getTabletChecker().cancelRepairTable((AdminCancelRepairTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetConfigStmt) {
            catalog.setConfig((AdminSetConfigStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateFileStmt) {
            catalog.getSmallFileMgr().createFile((CreateFileStmt) ddlStmt);
        } else if (ddlStmt instanceof DropFileStmt) {
            catalog.getSmallFileMgr().dropFile((DropFileStmt) ddlStmt);
        } else {
            throw new DdlException("Unknown statement.");

        }
    }
}
