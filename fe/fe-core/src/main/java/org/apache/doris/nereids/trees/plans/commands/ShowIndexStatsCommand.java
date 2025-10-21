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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * ShowIndexStatsCommand
 */
public class ShowIndexStatsCommand extends ShowCommand {
    private static final ImmutableList<String> INDEX_TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("table_name")
                    .add("index_name")
                    .add("analyze_row_count")
                    .add("report_row_count")
                    .add("report_row_count_for_nereids")
                    .build();
    private final TableNameInfo tableNameInfo;
    private final String indexName;
    private TableIf table;

    /**
     * Constructor
     */
    public ShowIndexStatsCommand(TableNameInfo tableNameInfo, String indexName) {
        super(PlanType.SHOW_INDEX_STATS_COMMAND);
        this.tableNameInfo = tableNameInfo;
        this.indexName = indexName;
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws AnalysisException {
        tableNameInfo.analyze(ctx);

        CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableNameInfo.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException(String.format("Catalog: %s not exists", tableNameInfo.getCtl()));
        }
        DatabaseIf<TableIf> db = catalog.getDb(tableNameInfo.getDb()).orElse(null);
        if (db == null) {
            ErrorReport.reportAnalysisException(String.format("DB: %s not exists", tableNameInfo.getDb()));
        }
        table = db.getTable(tableNameInfo.getTbl()).orElse(null);
        if (table == null) {
            ErrorReport.reportAnalysisException(String.format("Table: %s not exists", tableNameInfo.getTbl()));
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(),
                tableNameInfo.getCtl(), tableNameInfo.getDb(), tableNameInfo.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        return constructIndexResultSet(tableStats, table);
    }

    /**
     * constructIndexResultSet
     */
    public ShowResultSet constructIndexResultSet(TableStatsMeta tableStatistic, TableIf table) {
        List<List<String>> result = Lists.newArrayList();
        if (!(table instanceof OlapTable)) {
            return new ShowResultSet(getMetaData(), result);
        }
        OlapTable olapTable = (OlapTable) table;
        Long indexId = olapTable.getIndexIdByName(indexName);
        if (indexId == null) {
            throw new RuntimeException(String.format("Index %s not exist.", indexName));
        }
        long rowCount = tableStatistic == null ? -1 : tableStatistic.getRowCount(olapTable.getIndexIdByName(indexName));
        List<String> row = Lists.newArrayList();
        row.add(table.getName());
        row.add(indexName);
        row.add(String.valueOf(rowCount));
        row.add(String.valueOf(olapTable.getRowCountForIndex(indexId, false)));
        row.add(String.valueOf(olapTable.getRowCountForIndex(indexId, true)));
        result.add(row);
        return new ShowResultSet(getMetaData(), result);
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        for (String title : INDEX_TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowIndexStatsCommand(this, context);
    }
}
