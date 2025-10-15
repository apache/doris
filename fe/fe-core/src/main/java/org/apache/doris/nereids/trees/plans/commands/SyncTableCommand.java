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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.util.Util;
import org.apache.doris.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

/**
 * sync table command
 */
public class SyncTableCommand extends Command implements Redirect {
    private TableNameInfo tableNameInfo;

    public SyncTableCommand(TableNameInfo table) {
        super(PlanType.SYNC_TABLE_COMMAND);
        this.tableNameInfo = table;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        System.out.println("sync table " + tableNameInfo);
        tableNameInfo.analyze(ctx);

        String catalogName = tableNameInfo.getCtl();
        Util.prohibitExternalCatalog(catalogName, this.getClass().getSimpleName());

        String dbName = tableNameInfo.getDb();
        String tblName = tableNameInfo.getTbl();

        Env.getCurrentEnv().syncTable(dbName, tblName);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitSyncTableCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.FORWARD_WITH_SYNC;
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SYNC;
    }

    public TableNameInfo getTableNameInfo() {
        return tableNameInfo;
    }
}
