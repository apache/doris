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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.datasource.hive.HiveMetaStoreClientHelper;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;

/**
 * Represents the command for SHOW CREATE TABLE.
 */
public class ShowCreateTableCommand extends ShowCommand {
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

    private final TableNameInfo tblNameInfo;
    private final boolean isBrief;

    public ShowCreateTableCommand(TableNameInfo tableNameInfo, boolean isBrief) {
        super(PlanType.SHOW_CREATE_TABLE_COMMAND);
        this.tblNameInfo = tableNameInfo;
        this.isBrief = isBrief;
    }

    private void validate(ConnectContext ctx) throws AnalysisException {
        tblNameInfo.analyze(ctx);

        TableIf tableIf = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(tblNameInfo.getCtl())
                .getDbOrAnalysisException(tblNameInfo.getDb()).getTableOrAnalysisException(tblNameInfo.getTbl());

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

        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                tblNameInfo.getCtl(), tblNameInfo.getDb(), tblNameInfo.getTbl(), wanted)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW CREATE TABLE",
                                                ConnectContext.get().getQualifiedUser(),
                                                ConnectContext.get().getRemoteIP(),
                                                tblNameInfo.getDb() + ": " + tblNameInfo.getTbl());
        }
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowCreateTableCommand(this, context);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);

        // Fetch the catalog, database, and table metadata
        DatabaseIf db = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(tblNameInfo.getCtl())
                            .getDbOrMetaException(tblNameInfo.getDb());
        TableIf table = db.getTableOrMetaException(tblNameInfo.getTbl());

        List<List<String>> rows = Lists.newArrayList();

        table.readLock();
        try {
            if (table.getType() == Table.TableType.HMS_EXTERNAL_TABLE) {
                rows.add(Arrays.asList(table.getName(),
                        HiveMetaStoreClientHelper.showCreateTable(((HMSExternalTable) table).getRemoteTable())));
                return new ShowResultSet(META_DATA, rows);
            }
            List<String> createTableStmt = Lists.newArrayList();
            Env.getDdlStmt(null, null, table, createTableStmt, null, null, false,
                    true /* hide password */, false, -1L, isBrief, false);
            if (createTableStmt.isEmpty()) {
                return new ShowResultSet(META_DATA, rows);
            }

            if (table instanceof View) {
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0), "utf8mb4", "utf8mb4_0900_bin"));
                return new ShowResultSet(VIEW_META_DATA, rows);
            } else {
                rows.add(Lists.newArrayList(table.getName(), createTableStmt.get(0)));
                return (table.getType() != Table.TableType.MATERIALIZED_VIEW
                        ? new ShowResultSet(META_DATA, rows)
                        : new ShowResultSet(MATERIALIZED_VIEW_META_DATA, rows));
            }
        } finally {
            table.readUnlock();
        }
    }

}
