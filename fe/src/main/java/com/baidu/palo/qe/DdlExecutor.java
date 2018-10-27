// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.qe;

import com.baidu.palo.analysis.AlterClusterStmt;
import com.baidu.palo.analysis.AlterDatabaseQuotaStmt;
import com.baidu.palo.analysis.AlterDatabaseRename;
import com.baidu.palo.analysis.AlterSystemStmt;
import com.baidu.palo.analysis.AlterTableStmt;
import com.baidu.palo.analysis.BackupStmt;
import com.baidu.palo.analysis.CancelAlterSystemStmt;
import com.baidu.palo.analysis.CancelAlterTableStmt;
import com.baidu.palo.analysis.CancelBackupStmt;
import com.baidu.palo.analysis.CancelLoadStmt;
import com.baidu.palo.analysis.CreateClusterStmt;
import com.baidu.palo.analysis.CreateDbStmt;
import com.baidu.palo.analysis.CreateRepositoryStmt;
import com.baidu.palo.analysis.CreateRoleStmt;
import com.baidu.palo.analysis.CreateTableStmt;
import com.baidu.palo.analysis.CreateUserStmt;
import com.baidu.palo.analysis.CreateViewStmt;
import com.baidu.palo.analysis.DdlStmt;
import com.baidu.palo.analysis.DeleteStmt;
import com.baidu.palo.analysis.DropClusterStmt;
import com.baidu.palo.analysis.DropDbStmt;
import com.baidu.palo.analysis.DropRepositoryStmt;
import com.baidu.palo.analysis.DropRoleStmt;
import com.baidu.palo.analysis.DropTableStmt;
import com.baidu.palo.analysis.DropUserStmt;
import com.baidu.palo.analysis.GrantStmt;
import com.baidu.palo.analysis.LinkDbStmt;
import com.baidu.palo.analysis.LoadStmt;
import com.baidu.palo.analysis.MigrateDbStmt;
import com.baidu.palo.analysis.RecoverDbStmt;
import com.baidu.palo.analysis.RecoverPartitionStmt;
import com.baidu.palo.analysis.RecoverTableStmt;
import com.baidu.palo.analysis.RestoreStmt;
import com.baidu.palo.analysis.RevokeStmt;
import com.baidu.palo.analysis.SetUserPropertyStmt;
import com.baidu.palo.analysis.SyncStmt;
import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.Config;
import com.baidu.palo.common.DdlException;
import com.baidu.palo.load.LoadJob.EtlJobType;

/**
 * Created by zhaochun on 14/11/10.
 */
public class DdlExecutor {
    public static void execute(Catalog catalog, DdlStmt ddlStmt) throws DdlException, Exception {
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
        } else if (ddlStmt instanceof CreateTableStmt) {
            catalog.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            catalog.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            catalog.alterTable((AlterTableStmt) ddlStmt);
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
                            + " Try use broker load. See 'help broker load;'");
                }
                jobType = EtlJobType.HADOOP;
            }
            catalog.getLoadInstance().addLoadJob(loadStmt, jobType, System.currentTimeMillis());
        } else if (ddlStmt instanceof CancelLoadStmt) {
            catalog.getLoadInstance().cancelLoadJob((CancelLoadStmt) ddlStmt);
        } else if (ddlStmt instanceof DeleteStmt) {
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
        } else {
            throw new DdlException("Unknown statement.");
        }
    }
}
