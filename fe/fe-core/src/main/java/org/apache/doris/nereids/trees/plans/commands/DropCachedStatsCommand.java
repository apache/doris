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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import java.util.Objects;

/**
 * Manually drop cached statistics for table and its mv.
 * <p>
 * syntax:
 * DROP CACHED STATS TableName;
 */
public class DropCachedStatsCommand extends DropCommand {
    private final TableNameInfo tableNameInfo;
    private long catalogId;
    private long dbId;
    private long tblId;

    public DropCachedStatsCommand(TableNameInfo tableNameInfo) {
        super(PlanType.DROP_CACHED_STATS_COMMAND);
        Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        this.tableNameInfo = tableNameInfo;
    }

    @Override
    public void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        ctx.getEnv().getAnalysisManager().dropCachedStats(catalogId, dbId, tblId);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        if (!ConnectContext.get().getSessionVariable().enableStats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                + " in your FE conf file");
        }

        tableNameInfo.analyze(ctx);
        String catalogName = tableNameInfo.getCtl();
        String dbName = tableNameInfo.getDb();
        String tblName = tableNameInfo.getTbl();
        CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(tableNameInfo.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrAnalysisException(tblName);
        tblId = table.getId();
        dbId = db.getId();
        catalogId = catalog.getId();
        // check permission
        checkAnalyzePriv(catalogName, dbName, tblName);
    }

    private void checkAnalyzePriv(String catalogName, String dbName, String tblName) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), catalogName, dbName, tblName,
                PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "DROP",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + "." + tblName);
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitDropCachedStatsCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DROP;
    }
}
