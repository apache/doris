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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import static org.apache.doris.system.SystemInfoService.DEFAULT_CLUSTER;

// Show rollup statement, used to show rollup information of one table.
//
// Syntax:
//      SHOW ROLLUP { FROM | IN } table [ FROM db ]
public class ShowRollupStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("id", ScalarType.createVarchar(50)))
                    .addColumn(new Column("name", ScalarType.createVarchar(50)))
                    .addColumn(new Column("database_name", ScalarType.createVarchar(20)))
                    .addColumn(new Column("text", ScalarType.createVarchar(1024)))
                    .addColumn(new Column("rows", ScalarType.createVarchar(50)))
                    .build();
    private String db;

    public ShowRollupStmt(String db) {
        this.db = DEFAULT_CLUSTER + ":" + db;
    }

    public String getDb() {
        return db;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        // if both `db` and `table` have database have database info, use `db` information.
        // 1. use `db` database info
        // 2. use `table` database info
        // 3. use default database info in analyzer.
        /*
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        if (!Strings.isNullOrEmpty(db)) {
            // overwrite database in tbl.
            tbl.setDb(db);
        }
        tbl.analyze(analyzer);
        */

        if (!Catalog.getCurrentCatalog().getAuth().checkDbPriv(ConnectContext.get(), db, PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_DB_ACCESS_DENIED, "SHOW MATERIALIZED VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    db);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("SHOW MATERIALIZED VIEW ON ").append(db);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }
    
    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
