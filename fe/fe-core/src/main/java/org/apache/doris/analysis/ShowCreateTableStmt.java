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
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSetMetaData;

// SHOW CREATE TABLE statement.
public class ShowCreateTableStmt extends ShowStmt {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Table", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData VIEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("View", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create View", ScalarType.createVarchar(30)))
                    .addColumn(new Column("character_set_client", ScalarType.createVarchar(30)))
                    .addColumn(new Column("collation_connection", ScalarType.createVarchar(30)))
                    .build();

    private static final ShowResultSetMetaData MATERIALIZED_VIEW_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Materialized View", ScalarType.createVarchar(20)))
                    .addColumn(new Column("Create Materialized View", ScalarType.createVarchar(30)))
                    .build();

    private TableName tbl;
    private boolean isView;
    private boolean needBriefDdl;

    public ShowCreateTableStmt(TableName tbl) {
        this(tbl, false, false);
    }

    public ShowCreateTableStmt(TableName tbl, boolean needBriefDdl) {
        this(tbl, false, needBriefDdl);
    }

    public ShowCreateTableStmt(TableName tbl, boolean isView, boolean needBriefDdl) {
        this.tbl = tbl;
        this.isView = isView;
        this.needBriefDdl = needBriefDdl;
    }


    public String getCtl() {
        return tbl.getCtl();
    }

    public String getDb() {
        return tbl.getDb();
    }

    public String getTable() {
        return tbl.getTbl();
    }

    public boolean isView() {
        return isView;
    }

    public boolean isNeedBriefDdl() {
        return needBriefDdl;
    }

    public static ShowResultSetMetaData getViewMetaData() {
        return VIEW_META_DATA;
    }

    public static ShowResultSetMetaData getMaterializedViewMetaData() {
        return MATERIALIZED_VIEW_META_DATA;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (tbl == null) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_TABLES_USED);
        }
        tbl.analyze(analyzer);

        TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(tbl.getCtl())
                .getDbOrAnalysisException(tbl.getDb()).getTableOrAnalysisException(tbl.getTbl());

        if (tableIf instanceof MTMV) {
            ErrorReport.reportAnalysisException("not support async materialized view, "
                    + "please use `show create materialized view`");
        }

        PrivPredicate wanted;
        if (tableIf instanceof View) {
            wanted = PrivPredicate.SHOW_VIEW;
        } else {
            wanted = PrivPredicate.SHOW;
        }

        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tbl.getCtl(), tbl.getDb(),
                tbl.getTbl(), wanted)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tbl.getDb() + ": " + tbl.getTbl());
        }
    }

    @Override
    public String toSql() {
        return "SHOW CREATE TABLE " + tbl;
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
