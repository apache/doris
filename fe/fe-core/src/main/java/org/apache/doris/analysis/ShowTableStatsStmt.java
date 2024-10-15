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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

public class ShowTableStatsStmt extends ShowStmt {

    // TODO add more columns
    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("updated_rows")
                    .add("query_times")
                    .add("row_count")
                    .add("updated_time")
                    .add("columns")
                    .add("trigger")
                    .add("new_partition")
                    .add("user_inject")
                    .add("last_analyze_time")
                    .build();

    private static final ImmutableList<String> INDEX_TITLE_NAMES =
            new ImmutableList.Builder<String>()
            .add("table_name")
            .add("index_name")
            .add("analyze_row_count")
            .add("report_row_count")
            .add("report_row_count_for_nereids")
            .build();

    private final TableName tableName;
    private final PartitionNames partitionNames;
    private final String indexName;
    private final long tableId;
    private final boolean useTableId;

    private TableIf table;

    public ShowTableStatsStmt(long tableId) {
        this.tableName = null;
        this.partitionNames = null;
        this.indexName = null;
        this.tableId = tableId;
        this.useTableId = true;
    }

    public ShowTableStatsStmt(TableName tableName, PartitionNames partitionNames, String indexName) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
        this.indexName = indexName;
        this.tableId = -1;
        this.useTableId = false;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (useTableId) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP());
            }
            return;
        }
        tableName.analyze(analyzer);
        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.getPartitionNames().size() > 1) {
                throw new AnalysisException("Only one partition name could be specified");
            }
        }
        CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException(String.format("Catalog: %s not exists", tableName.getCtl()));
        }
        DatabaseIf<TableIf> db = catalog.getDb(tableName.getDb()).orElse(null);
        if (db == null) {
            ErrorReport.reportAnalysisException(String.format("DB: %s not exists", tableName.getDb()));
        }
        table = db.getTable(tableName.getTbl()).orElse(null);
        if (table == null) {
            ErrorReport.reportAnalysisException(String.format("Table: %s not exists", tableName.getTbl()));
        }
        if (partitionNames != null) {
            String partitionName = partitionNames.getPartitionNames().get(0);
            Partition partition = table.getPartition(partitionName);
            if (partition == null) {
                ErrorReport.reportAnalysisException(String.format("Partition: %s not exists", partitionName));
            }
        }
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getCtl(), tableName.getDb(), tableName.getTbl(),
                        PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        ImmutableList<String> titles;
        if (indexName != null) {
            titles = INDEX_TITLE_NAMES;
        } else {
            titles = TITLE_NAMES;
        }
        for (String title : titles) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public TableIf getTable() {
        return table;
    }

    public boolean isUseTableId() {
        return useTableId;
    }

    public long getTableId() {
        return tableId;
    }

    public ShowResultSet constructResultSet(TableStatsMeta tableStatistic, TableIf table) {
        if (indexName != null) {
            return constructIndexResultSet(tableStatistic, table);
        }
        return constructTableResultSet(tableStatistic, table);
    }

    public ShowResultSet constructEmptyResultSet() {
        return new ShowResultSet(getMetaData(), new ArrayList<>());
    }

    public ShowResultSet constructTableResultSet(TableStatsMeta tableStatistic, TableIf table) {
        if (tableStatistic == null) {
            List<List<String>> result = Lists.newArrayList();
            List<String> row = Lists.newArrayList();
            row.add("");
            row.add("");
            row.add(String.valueOf(table.getCachedRowCount()));
            row.add("");
            row.add("");
            row.add("");
            row.add("");
            row.add("");
            row.add("");
            result.add(row);
            return new ShowResultSet(getMetaData(), result);
        }
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        List<List<String>> result = Lists.newArrayList();
        List<String> row = Lists.newArrayList();
        row.add(String.valueOf(tableStatistic.updatedRows));
        row.add(String.valueOf(tableStatistic.queriedTimes.get()));
        row.add(String.valueOf(tableStatistic.rowCount));
        LocalDateTime tableUpdateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tableStatistic.updatedTime),
                java.time.ZoneId.systemDefault());
        LocalDateTime lastAnalyzeTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tableStatistic.lastAnalyzeTime),
                        java.time.ZoneId.systemDefault());
        row.add(tableUpdateTime.format(formatter));
        row.add(tableStatistic.analyzeColumns().toString());
        row.add(tableStatistic.jobType.toString());
        row.add(String.valueOf(tableStatistic.newPartitionLoaded.get()));
        row.add(String.valueOf(tableStatistic.userInjected));
        row.add(lastAnalyzeTime.format(formatter));
        result.add(row);
        return new ShowResultSet(getMetaData(), result);
    }

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
}
