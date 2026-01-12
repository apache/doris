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
import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.catalog.MysqlDb;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.Command;
import org.apache.doris.nereids.trees.plans.commands.ForwardWithSync;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Strings;

import java.util.Map;

/**
 * Refresh database.
 */
public class RefreshDatabaseCommand extends Command implements ForwardWithSync {
    private String catalogName;
    private String dbName;
    private Map<String, String> properties;

    public RefreshDatabaseCommand(String dbName, Map<String, String> properties) {
        super(PlanType.REFRESH_DATABASE_COMMAND);
        this.dbName = dbName;
        this.properties = properties;
    }

    public RefreshDatabaseCommand(String catalogName, String dbName, Map<String, String> properties) {
        super(PlanType.REFRESH_DATABASE_COMMAND);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.properties = properties;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(catalogName)) {
            catalogName = ConnectContext.get().getCurrentCatalog().getName();
        }
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_DB_NAME, dbName);
        }

        // Don't allow dropping 'information_schema' database
        if (dbName.equalsIgnoreCase(InfoSchemaDb.DATABASE_NAME)) {

            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), dbName);
        }
        // Don't allow dropping 'mysql' database
        if (dbName.equalsIgnoreCase(MysqlDb.DATABASE_NAME)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_DBACCESS_DENIED_ERROR, ctx.getQualifiedUser(), dbName);
        }
        // check access
        if (!Env.getCurrentEnv().getAccessManager().checkDbPriv(ConnectContext.get(), catalogName,
                dbName, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED_ERROR,
                    PrivPredicate.SHOW.getPrivs().toString(), dbName);
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        Env.getCurrentEnv().getRefreshManager().handleRefreshDb(catalogName, dbName);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitRefreshDatabaseCommand(this, context);
    }

    /**
     * refresh database statement.
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH DATABASE ");
        if (catalogName != null) {
            sb.append("`").append(catalogName).append("`.");
        }
        sb.append("`").append(dbName).append("`");
        return sb.toString();
    }

    @Override
    public StmtType stmtType() {
        return StmtType.REFRESH;
    }
}
