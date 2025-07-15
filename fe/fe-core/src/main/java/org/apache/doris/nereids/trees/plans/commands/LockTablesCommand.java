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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.LockTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.List;

/**
 * Command for LOCK TABLES.
 */
public class LockTablesCommand extends Command implements NoForward {

    private List<LockTableInfo> lockTables;

    public LockTablesCommand(List<LockTableInfo> lockTables) {
        super(PlanType.LOCK_TABLES_COMMAND);
        this.lockTables = lockTables;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        for (LockTableInfo lockTable : lockTables) {
            TableNameInfo tableNameInfo = lockTable.getTableNameInfo();
            tableNameInfo.analyze(ctx);
            Database db = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(tableNameInfo.getDb());
            db.getTableOrAnalysisException(tableNameInfo.getTbl());

            // check auth
            if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                    ConnectContext.get(), tableNameInfo, PrivPredicate.SELECT)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SELECT",
                        ConnectContext.get().getQualifiedUser(),
                        ConnectContext.get().getRemoteIP(),
                        tableNameInfo.toString());
            }
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitLockTablesCommand(this, context);
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}

