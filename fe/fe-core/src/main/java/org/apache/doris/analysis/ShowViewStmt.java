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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;

// Show view statement, used to show view information of one table.
//
// Syntax:
//      SHOW VIEW { FROM | IN } table [ FROM db ]
public class ShowViewStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("View", ScalarType.createVarchar(30)))
                    .addColumn(new Column("Create View", ScalarType.createVarchar(65535)))
                    .build();

    private String db;
    private TableName tbl;

    private List<View> matchViews = Lists.newArrayList();

    public ShowViewStmt(String db, TableName tbl) {
        this.db = db;
        this.tbl = tbl;
    }

    public String getDb() {
        return tbl.getDb();
    }

    public String getTbl() {
        return tbl.getTbl();
    }

    public List<View> getMatchViews() {
        return matchViews;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        if (!Strings.isNullOrEmpty(db)) {
            // if user specify the `from db`, overwrite the db in `tbl` with this db.
            // for example:
            //      show view from db1.tbl1 from db2;
            // will be rewrote to:
            //      show view from db2.tbl1;
            // this act same as in MySQL
            tbl.setDb(db);
        }
        tbl.analyze(analyzer);
        // disallow external catalog
        Util.prohibitExternalCatalog(tbl.getCtl(), this.getClass().getSimpleName());

        String dbName = tbl.getDb();
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                ConnectContext.get(), tbl.getCtl(), dbName, getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW VIEW",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + getTbl());
        }

        Database database = Env.getCurrentInternalCatalog().getDbOrAnalysisException(dbName);
        database.getOlapTableOrAnalysisException(tbl.getTbl());
        for (Table table : database.getViews()) {
            View view = (View) table;
            List<TableRef> tblRefs = Lists.newArrayList();
            // get table refs instead of get tables because it don't need to check table's validity
            getTableRefs(analyzer, view, tblRefs);
            for (TableRef tblRef : tblRefs) {
                tblRef.getName().analyze(analyzer);
                if (tblRef.getName().equals(tbl)) {
                    matchViews.add(view);
                }
            }
        }
    }

    private void getTableRefs(Analyzer analyzer, View view, List<TableRef> tblRefs) {
        Set<String> parentViewNameSet = Sets.newHashSet();
        QueryStmt queryStmt = view.getQueryStmt();
        queryStmt.getTableRefs(analyzer, tblRefs, parentViewNameSet);
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder("SHOW VIEW FROM ");
        sb.append(tbl.toSql());
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
}
