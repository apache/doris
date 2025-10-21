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

import org.apache.doris.analysis.RedirectStatus;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Index;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * show index command
 */
public class ShowIndexCommand extends ShowCommand {
    private static final ShowResultSetMetaData META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("Table", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Non_unique", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Key_name", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Seq_in_index", ScalarType.createVarchar(128)))
                    .addColumn(new Column("Column_name", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Collation", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Cardinality", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Sub_part", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Packed", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Null", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Index_type", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Comment", ScalarType.createVarchar(160)))
                    .addColumn(new Column("Properties", ScalarType.createVarchar(400)))
                    .build();
    private TableNameInfo tableNameInfo;

    /**
     * constructor for show index
     */
    public ShowIndexCommand(TableNameInfo tableNameInfo) {
        super(PlanType.SHOW_INDEX_COMMAND);
        this.tableNameInfo = tableNameInfo;
    }

    @VisibleForTesting
    protected void analyze(ConnectContext ctx) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableNameInfo.getCtl())) {
            String ctl = ctx.getCurrentCatalog().getName();
            if (Strings.isNullOrEmpty(ctl)) {
                ctl = InternalCatalog.INTERNAL_CATALOG_NAME;
            }
            tableNameInfo.setCtl(ctl);
        }
        if (Strings.isNullOrEmpty(tableNameInfo.getDb())) {
            String dbName = ctx.getDatabase();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            tableNameInfo.setDb(dbName);
        }

        if (Strings.isNullOrEmpty(tableNameInfo.getTbl())) {
            throw new AnalysisException("Table name is null");
        }

        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(
                ConnectContext.get(), tableNameInfo.getCtl(),
                tableNameInfo.getDb(), tableNameInfo.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "SHOW INDEX",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(), tableNameInfo.toSql());
        }
    }

    private ShowResultSet handleShowIndex(ConnectContext ctx, StmtExecutor executor) throws Exception {
        analyze(ctx);

        List<List<String>> rows = Lists.newArrayList();
        // in show index, only support internal catalog
        DatabaseIf db = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(tableNameInfo.getCtl())
                .getDbOrAnalysisException(tableNameInfo.getDb());
        if (db instanceof Database) {
            TableIf table = db.getTableOrAnalysisException(tableNameInfo.getTbl());
            if (table instanceof OlapTable) {
                OlapTable olapTable = (OlapTable) table;
                olapTable.readLock();
                try {
                    List<Index> indexes = olapTable.getIndexes();
                    for (Index index : indexes) {
                        rows.add(Lists.newArrayList(tableNameInfo.getTbl(), "", index.getIndexName(),
                                "", String.join(",", index.getColumns()), "", "", "", "",
                                "", index.getIndexType().name(), index.getComment(), index.getPropertiesString()));
                    }
                } finally {
                    olapTable.readUnlock();
                }
            }
        }
        return new ShowResultSet(getMetaData(), rows);
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        return handleShowIndex(ctx, executor);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return META_DATA;
    }

    @Override
    public RedirectStatus toRedirectStatus() {
        return RedirectStatus.NO_FORWARD;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowIndexCommand(this, context);
    }
}
