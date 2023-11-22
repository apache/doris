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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.ColStatsMeta;
import org.apache.doris.statistics.ColumnStatistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ShowColumnStatsStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("column_name")
                    .add("count")
                    .add("ndv")
                    .add("num_null")
                    .add("data_size")
                    .add("avg_size_byte")
                    .add("min")
                    .add("max")
                    .add("method")
                    .add("type")
                    .add("trigger")
                    .add("query_times")
                    .add("updated_time")
                    .build();

    private final TableName tableName;

    private final List<String> columnNames;
    private final PartitionNames partitionNames;
    private final boolean cached;

    private TableIf table;

    public ShowColumnStatsStmt(TableName tableName, List<String> columnNames,
                               PartitionNames partitionNames, boolean cached) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.partitionNames = partitionNames;
        this.cached = cached;
    }

    public TableName getTableName() {
        return tableName;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        tableName.analyze(analyzer);
        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
            if (partitionNames.getPartitionNames().size() > 1) {
                throw new AnalysisException("Only one partition name could be specified");
            }
        }
        CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException("Catalog: %s not exists", tableName.getCtl());
        }
        DatabaseIf<TableIf> db = catalog.getDb(tableName.getDb()).orElse(null);
        if (db == null) {
            ErrorReport.reportAnalysisException("DB: %s not exists", tableName.getDb());
        }
        table = db.getTable(tableName.getTbl()).orElse(null);
        if (table == null) {
            ErrorReport.reportAnalysisException("Table: %s not exists", tableName.getTbl());
        }

        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), tableName.getDb(), tableName.getTbl(), PrivPredicate.SHOW)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "Permission denied",
                    ConnectContext.get().getQualifiedUser(), ConnectContext.get().getRemoteIP(),
                    tableName.getDb() + ": " + tableName.getTbl());
        }

        if (columnNames != null) {
            for (String name : columnNames) {
                if (table.getColumn(name) == null) {
                    ErrorReport.reportAnalysisException("Column: %s not exists", name);
                }
            }
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    public TableIf getTable() {
        return table;
    }

    public ShowResultSet constructResultSet(List<Pair<String, ColumnStatistic>> columnStatistics) {
        List<List<String>> result = Lists.newArrayList();
        columnStatistics.forEach(p -> {
            if (p.second.isUnKnown) {
                return;
            }

            List<String> row = Lists.newArrayList();
            row.add(p.first);
            row.add(String.valueOf(p.second.count));
            row.add(String.valueOf(p.second.ndv));
            row.add(String.valueOf(p.second.numNulls));
            row.add(String.valueOf(p.second.dataSize));
            row.add(String.valueOf(p.second.avgSizeByte));
            row.add(String.valueOf(p.second.minExpr == null ? "N/A" : p.second.minExpr.toSql()));
            row.add(String.valueOf(p.second.maxExpr == null ? "N/A" : p.second.maxExpr.toSql()));
            ColStatsMeta colStatsMeta = Env.getCurrentEnv().getAnalysisManager().findColStatsMeta(table.getId(),
                    p.first);
            row.add(String.valueOf(colStatsMeta == null ? "N/A" : colStatsMeta.analysisMethod));
            row.add(String.valueOf(colStatsMeta == null ? "N/A" : colStatsMeta.analysisType));
            row.add(String.valueOf(colStatsMeta == null ? "N/A" : colStatsMeta.jobType));
            row.add(String.valueOf(colStatsMeta == null ? "N/A" : colStatsMeta.queriedTimes));
            row.add(String.valueOf(p.second.updatedTime));
            result.add(row);
        });
        return new ShowResultSet(getMetaData(), result);
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public Set<String> getColumnNames() {
        if (columnNames != null) {
            return  Sets.newHashSet(columnNames);
        }
        return table.getColumns().stream()
                .map(Column::getName).collect(Collectors.toSet());
    }

    public boolean isCached() {
        return cached;
    }
}
