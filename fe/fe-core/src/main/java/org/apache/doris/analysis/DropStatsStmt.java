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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Manually drop statistics for tables or partitions.
 * Table or partition can be specified, if neither is specified,
 * all statistics under the current database will be deleted.
 * <p>
 * syntax:
 * DROP [EXPIRED] STATS [TableName [PARTITIONS(partitionNames)]];
 */
public class DropStatsStmt extends DdlStmt {

    public final boolean dropExpired;

    private final TableName tableName;
    private Set<String> columnNames;

    private long tblId;

    public DropStatsStmt(boolean dropExpired) {
        this.dropExpired = dropExpired;
        this.tableName = null;
        this.columnNames = null;
    }

    public DropStatsStmt(TableName tableName,
            List<String> columnNames) {
        this.tableName = tableName;
        this.columnNames = new HashSet<>(columnNames);
        dropExpired = false;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Config.enable_stats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
        super.analyze(analyzer);
        if (dropExpired) {
            return;
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
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(),
                this.getClass().getSimpleName());
        // check permission
        checkAnalyzePriv(db.getFullName(), table.getName());
        // check columnNames
        if (columnNames != null) {
            for (String cName : columnNames) {
                if (table.getColumn(cName) == null) {
                    ErrorReport.reportAnalysisException(
                            ErrorCode.ERR_WRONG_COLUMN_NAME,
                            "DROP",
                            ConnectContext.get().getQualifiedUser(),
                            ConnectContext.get().getRemoteIP(),
                            cName);
                }
            }
        } else {
            columnNames = table.getColumns().stream().map(Column::getName).collect(Collectors.toSet());
        }
    }

    public long getTblId() {
        return tblId;
    }

    public Set<String> getColumnNames() {
        return columnNames;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP STATS ");

        if (tableName != null) {
            sb.append(tableName.toSql());
        }

        if (columnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(columnNames, ","));
            sb.append(")");
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.DROP)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "DROP",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + "." + tblName);
        }
    }
}
