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

package org.apache.doris.nereids.trees.plans.commands.refresh;

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Refresh table command.
 */
public class RefreshTableCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(RefreshTableCommand.class);
    private TableNameInfo tableNameInfo;

    public RefreshTableCommand(TableNameInfo tableName) {
        super(PlanType.REFRESH_TABLE_COMMAND);
        this.tableNameInfo = tableName;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        tableNameInfo.analyze(ctx);
        checkRefreshTableAccess();
        // refresh table execute logic
        Env.getCurrentEnv().getRefreshManager()
                .handleRefreshTable(tableNameInfo.getCtl(), tableNameInfo.getDb(), tableNameInfo.getTbl(), false);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshTableCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }

    /**
     * check access.
     */
    private void checkRefreshTableAccess() throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                tableNameInfo.getCtl(), tableNameInfo.getDb(),
                tableNameInfo.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), tableNameInfo.getTbl());
        }
    }

    public String toSql() {
        return "REFRESH TABLE " + tableNameInfo.toSql();
    }

}
