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
import org.apache.doris.analysis.AlterRoleStmt;
import org.apache.doris.analysis.AlterTableStmt;
import org.apache.doris.analysis.CancelLoadStmt;
import org.apache.doris.analysis.CreateEncryptKeyStmt;
import org.apache.doris.analysis.CreateMaterializedViewStmt;
import org.apache.doris.analysis.CreateRoutineLoadStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DdlStmt;
import org.apache.doris.analysis.DropIndexPolicyStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.DropUserStmt;
import org.apache.doris.analysis.DropWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.RecoverDbStmt;
import org.apache.doris.analysis.RecoverPartitionStmt;
import org.apache.doris.analysis.RecoverTableStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.SetUserPropertyStmt;
import org.apache.doris.analysis.SyncStmt;
import org.apache.doris.analysis.UninstallPluginStmt;
import org.apache.doris.catalog.EncryptKeyHelper;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.job.exception.JobException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        if (ddlStmt instanceof CreateEncryptKeyStmt) {
            EncryptKeyHelper.createEncryptKey((CreateEncryptKeyStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateTableStmt) {
            env.createTable((CreateTableStmt) ddlStmt);
        } else if (ddlStmt instanceof DropTableStmt) {
            env.dropTable((DropTableStmt) ddlStmt);
        } else if (ddlStmt instanceof CreateMaterializedViewStmt) {
            env.createMaterializedView((CreateMaterializedViewStmt) ddlStmt);
        } else if (ddlStmt instanceof AlterTableStmt) {
            env.alterTable((AlterTableStmt) ddlStmt);
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
        } else if (ddlStmt instanceof UninstallPluginStmt) {
            env.uninstallPlugin((UninstallPluginStmt) ddlStmt);
        } else if (ddlStmt instanceof AdminSetPartitionVersionStmt) {
            env.setPartitionVersion((AdminSetPartitionVersionStmt) ddlStmt);
        } else if (ddlStmt instanceof DropWorkloadSchedPolicyStmt) {
            env.getWorkloadSchedPolicyMgr().dropWorkloadSchedPolicy((DropWorkloadSchedPolicyStmt) ddlStmt);
        } else if (ddlStmt instanceof RefreshTableStmt) {
            RefreshTableStmt refreshTableStmt = (RefreshTableStmt) ddlStmt;
            env.getRefreshManager().handleRefreshTable(refreshTableStmt.getCtl(), refreshTableStmt.getDbName(),
                    refreshTableStmt.getTblName(), false);
        } else if (ddlStmt instanceof RefreshDbStmt) {
            RefreshDbStmt refreshDbStmt = (RefreshDbStmt) ddlStmt;
            env.getRefreshManager().handleRefreshDb(refreshDbStmt.getCatalogName(), refreshDbStmt.getDbName());
        } else if (ddlStmt instanceof DropIndexPolicyStmt) {
            env.getIndexPolicyMgr().dropIndexPolicy((DropIndexPolicyStmt) ddlStmt);
        } else {
            LOG.warn("Unkown statement " + ddlStmt.getClass());
            throw new DdlException("Unknown statement.");
        }
    }

    private static void checkDdlStmtSupported(DdlStmt ddlStmt) throws DdlException {
        // check stmt has been supported in cloud mode
        if (Config.isNotCloudMode()) {
            return;
        }
    }
}
