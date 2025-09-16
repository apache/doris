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

import org.apache.doris.analysis.StmtType;
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
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.TableStatsMeta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ShowTableStatsCommand
 */
public class ShowTableStatsCommand extends ShowCommand {

    private static final ImmutableList<String> TABLE_TITLE_NAMES =
            new ImmutableList.Builder<String>()
                .add("updated_rows")
                .add("query_times")
                .add("row_count")
                .add("updated_time")
                .add("columns")
                .add("trigger")
                .add("new_partition")
                .add("user_inject")
                .add("enable_auto_analyze")
                .add("last_analyze_time")
                .build();

    private static final ImmutableList<String> PARTITION_TITLE_NAMES =
            new ImmutableList.Builder<String>()
                .add("partition_name")
                .add("updated_rows")
                .add("row_count")
                .build();

    private static final ImmutableList<String> COLUMN_PARTITION_TITLE_NAMES =
            new ImmutableList.Builder<String>()
                .add("index_name")
                .add("column_name")
                .add("partition_name")
                .add("updated_rows")
                .build();

    private final TableNameInfo tableNameInfo;
    private final List<String> columnNames;
    private final PartitionNamesInfo partitionNamesInfo;
    private final long tableId;
    private final boolean useTableId;
    private TableIf table;

    /**
     * ShowTableStatsCommand
     */
    public ShowTableStatsCommand(long tableId) {
        super(PlanType.SHOW_TABLE_STATS_COMMAND);
        this.tableNameInfo = null;
        this.columnNames = null;
        this.partitionNamesInfo = null;
        this.tableId = tableId;
        this.useTableId = true;
    }

    /**
     * ShowTableStatsCommand
     */
    public ShowTableStatsCommand(TableNameInfo tableNameInfo,
                                 List<String> columnNames,
                                 PartitionNamesInfo partitionNamesInfo) {
        super(PlanType.SHOW_TABLE_STATS_COMMAND);
        this.tableNameInfo = tableNameInfo;
        this.columnNames = columnNames;
        this.partitionNamesInfo = partitionNamesInfo;
        this.tableId = -1;
        this.useTableId = false;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        validate(ctx);
        return handleShowTableStatsCommand(ctx);
    }

    /**
     * validate
     */
    public void validate(ConnectContext ctx) throws UserException {
        if (useTableId) {
            if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.SHOW)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                        ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(), tableId);
            }
            return;
        }
        assert tableNameInfo != null;
        tableNameInfo.analyze(ctx);
        if (partitionNamesInfo != null) {
            partitionNamesInfo.validate();
        }
        if (columnNames != null && !columnNames.isEmpty() && partitionNamesInfo == null) {
            ErrorReport.reportAnalysisException(String.format("Must specify partitions when columns are specified."));
        }
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
        if (partitionNamesInfo != null && !partitionNamesInfo.isStar()) {
            for (String partitionName : partitionNamesInfo.getPartitionNames()) {
                Partition partition = table.getPartition(partitionName);
                if (partition == null) {
                    ErrorReport.reportAnalysisException(String.format("Partition: %s not exists", partitionName));
                }
            }
        }
        if (!Env.getCurrentEnv().getAccessManager().checkTblPriv(ConnectContext.get(), tableNameInfo.getCtl(),
                    tableNameInfo.getDb(), tableNameInfo.getTbl(),
                PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableNameInfo.getDb() + ": " + tableNameInfo.getTbl());
        }
    }

    private ShowResultSet handleShowTableStatsCommand(ConnectContext ctx) throws AnalysisException {
        ShowResultSet resultSet;
        // Handle use table id to show table stats. Mainly for online debug.
        if (useTableId) {
            TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(tableId);
            if (tableStats == null) {
                resultSet = constructEmptyResultSet();
            } else {
                resultSet = constructResultSet(tableStats, table);
            }
            return resultSet;
        }
        CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(tableNameInfo.getCtl());
        DatabaseIf db = catalog.getDbOrAnalysisException(tableNameInfo.getDb());
        TableIf table = db.getTableOrAnalysisException(tableNameInfo.getTbl());
        TableStatsMeta tableStats = Env.getCurrentEnv().getAnalysisManager().findTableStatsStatus(table.getId());
        resultSet = constructResultSet(tableStats, table);
        return resultSet;
    }

    /**
     * constructResultSet
     */
    public ShowResultSet constructResultSet(TableStatsMeta tableStatistic, TableIf table) {
        if (partitionNamesInfo == null) {
            return constructTableResultSet(tableStatistic, table);
        }
        if (columnNames == null || columnNames.isEmpty()) {
            return constructPartitionResultSet(tableStatistic, table);
        } else {
            return constructColumnPartitionResultSet(tableStatistic, table);
        }
    }

    public ShowResultSet constructEmptyResultSet() {
        return new ShowResultSet(getMetaData(), new ArrayList<>());
    }

    /**
     * constructTableResultSet
     */
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
            row.add(String.valueOf(table.autoAnalyzeEnabled()));
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
        LocalDateTime dateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tableStatistic.updatedTime),
                java.time.ZoneId.systemDefault());
        LocalDateTime lastAnalyzeTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(tableStatistic.lastAnalyzeTime),
                java.time.ZoneId.systemDefault());
        row.add(dateTime.format(formatter));
        Set<Pair<String, String>> columnsSet = tableStatistic.analyzeColumns();
        Set<Pair<String, String>> newColumnsSet = new HashSet<>();
        for (Pair<String, String> pair : columnsSet) {
            newColumnsSet.add(Pair.of(Util.getTempTableDisplayName(pair.first), pair.second));
        }
        row.add(newColumnsSet.toString());
        row.add(tableStatistic.jobType.toString());
        row.add(String.valueOf(tableStatistic.partitionChanged.get()));
        row.add(String.valueOf(tableStatistic.userInjected));
        row.add(table == null ? "N/A" : String.valueOf(table.autoAnalyzeEnabled()));
        row.add(lastAnalyzeTime.format(formatter));
        result.add(row);
        return new ShowResultSet(getMetaData(), result);
    }

    /**
     * constructPartitionResultSet
     */
    public ShowResultSet constructPartitionResultSet(TableStatsMeta tableStatistic, TableIf table) {
        List<List<String>> result = Lists.newArrayList();
        if (!(table instanceof OlapTable) || tableStatistic == null) {
            return new ShowResultSet(getMetaData(), result);
        }
        Collection<String> partitions = partitionNamesInfo.isStar()
                ? table.getPartitionNames()
                : partitionNamesInfo.getPartitionNames();
        for (String part : partitions) {
            Partition partition = table.getPartition(part);
            if (partition == null) {
                continue;
            }
            Long updateRows = tableStatistic.partitionUpdateRows.get(partition.getId());
            if (updateRows == null) {
                continue;
            }
            List<String> row = Lists.newArrayList();
            row.add(part);
            row.add(String.valueOf(updateRows.longValue()));
            row.add(String.valueOf(partition.getBaseIndex().getRowCount()));
            result.add(row);
        }
        return new ShowResultSet(getMetaData(), result);
    }

    /**
     * constructColumnPartitionResultSet
     */
    public ShowResultSet constructColumnPartitionResultSet(TableStatsMeta tableStatistic, TableIf table) {
        List<List<String>> result = Lists.newArrayList();
        if (!(table instanceof OlapTable) || tableStatistic == null) {
            return new ShowResultSet(getMetaData(), result);
        }
        OlapTable olapTable = (OlapTable) table;
        Collection<String> partitions = partitionNamesInfo.isStar()
                ? table.getPartitionNames() : partitionNamesInfo.getPartitionNames();
        if (partitions.size() > 100) {
            throw new RuntimeException("Too many partitions, show at most 100 partitions each time.");
        }
        Set<Pair<String, String>> columnIndexPairs = olapTable.getColumnIndexPairs(new HashSet<>(columnNames));
        for (Pair<String, String> pair : columnIndexPairs) {
            ColStatsMeta columnStatsMeta = tableStatistic.findColumnStatsMeta(pair.first, pair.second);
            if (columnStatsMeta != null && columnStatsMeta.partitionUpdateRows != null) {
                for (Map.Entry<Long, Long> entry : columnStatsMeta.partitionUpdateRows.entrySet()) {
                    Partition partition = olapTable.getPartition(entry.getKey());
                    if (partition != null && !partitions.contains(partition.getName())) {
                        continue;
                    }
                    List<String> row = Lists.newArrayList();
                    row.add(pair.first);
                    row.add(pair.second);
                    if (partition == null) {
                        row.add("Partition " + entry.getKey() + " Not Exist");
                    } else {
                        row.add(partition.getName());
                    }
                    row.add(String.valueOf(entry.getValue()));
                    result.add(row);
                }
            }
        }
        return new ShowResultSet(getMetaData(), result);
    }

    /**
     * getMetaData
     */
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();
        ImmutableList<String> titles;
        if (columnNames != null && !columnNames.isEmpty()) {
            // If columnNames != null, partitionNames is also not null. Guaranteed in analyze()
            titles = COLUMN_PARTITION_TITLE_NAMES;
        } else if (partitionNamesInfo != null) {
            titles = PARTITION_TITLE_NAMES;
        } else {
            titles = TABLE_TITLE_NAMES;
        }
        for (String title : titles) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitShowTableStatsCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.SHOW;
    }
}
