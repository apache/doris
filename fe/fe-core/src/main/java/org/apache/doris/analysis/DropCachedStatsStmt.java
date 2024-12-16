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

package org.apache.doris.analysis;

import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

/**
 * Manually drop cached statistics for table and its mv.
 * <p>
 * syntax:
 * DROP CACHED STATS TableName;
 */
public class DropCachedStatsStmt extends DdlStmt implements NotFallbackInParser {

    private final TableName tableName;

    private long catalogId;
    private long dbId;
    private long tblId;

    public DropCachedStatsStmt(TableName tableName) {
        this.tableName = tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (tableName == null) {
            throw new UserException("Should specify a valid table name.");
        }
        tableName.analyze(analyzer);
        String catalogName = tableName.getCtl();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        CatalogIf catalog = analyzer.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalogName);
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        TableIf table = db.getTableOrAnalysisException(tblName);
        tblId = table.getId();
        dbId = db.getId();
        catalogId = catalog.getId();
        // check permission
        checkAnalyzePriv(catalogName, db.getFullName(), table.getName());
    }

    public long getTblId() {
        return tblId;
    }

    public long getDbId() {
        return dbId;
    }

    public long getCatalogIdId() {
        return catalogId;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP CACHED STATS ");

        if (tableName != null) {
            sb.append(tableName.toSql());
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
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
    public StmtType stmtType() {
        return StmtType.DROP;
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }
}
