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
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import java.util.List;

// Alter view statement
@Deprecated
public class AlterViewStmt extends BaseViewStmt implements NotFallbackInParser {

    public AlterViewStmt(TableName tbl, List<ColWithComment> cols, QueryStmt queryStmt) {
        super(tbl, cols, queryStmt);
    }

    public TableName getTbl() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (tableName == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tableName.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());

        DatabaseIf db = Env.getCurrentInternalCatalog().getDbOrAnalysisException(tableName.getDb());
        TableIf table = db.getTableOrAnalysisException(tableName.getTbl());
        if (!(table instanceof View)) {
            throw new AnalysisException(
                    String.format("ALTER VIEW not allowed on a table:%s.%s", getDbName(), getTable()));
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.ALTER)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLE_ACCESS_DENIED_ERROR,
                    PrivPredicate.ALTER.getPrivs().toString(), tableName.getTbl());
        }

        if (cols != null) {
            cloneStmt = viewDefStmt.clone();
        }

        viewDefStmt.setNeedToSql(true);
        Analyzer viewAnalyzer = new Analyzer(analyzer);
        viewDefStmt.analyze(viewAnalyzer);
        checkQueryAuth();
        createColumnAndViewDefs(analyzer);
    }

    public void setInlineViewDef(String querySql) {
        inlineViewDef = querySql;
    }

    public void setFinalColumns(List<Column> columns) {
        finalCols.addAll(columns);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER VIEW ");
        sb.append(tableName.toSql()).append("\n");
        if (cols != null) {
            sb.append("(\n");
            for (int i = 0; i < cols.size(); i++) {
                if (i != 0) {
                    sb.append(",\n");
                }
                sb.append("  ").append(cols.get(i).getColName());
            }
            sb.append("\n)");
        }
        sb.append("\n");
        sb.append("AS ").append(viewDefStmt.toSql()).append("\n");
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
