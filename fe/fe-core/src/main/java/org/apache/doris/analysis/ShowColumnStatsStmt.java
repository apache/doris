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
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.ColumnStatistic;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

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
                    .add("min_expr")
                    .add("max_expr")
                    .build();

    private final TableName tableName;

    private final PartitionNames partitionNames;

    private TableIf table;

    public ShowColumnStatsStmt(TableName tableName, PartitionNames partitionNames) {
        this.tableName = tableName;
        this.partitionNames = partitionNames;
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
        // disallow external catalog
        Util.prohibitExternalCatalog(tableName.getCtl(), this.getClass().getSimpleName());
        CatalogIf<DatabaseIf> catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(tableName.getCtl());
        if (catalog == null) {
            ErrorReport.reportAnalysisException("Catalog: {} not exists", tableName.getCtl());
        }
        DatabaseIf<TableIf> db = catalog.getDb(tableName.getDb()).orElse(null);
        if (db == null) {
            ErrorReport.reportAnalysisException("DB: {} not exists", tableName.getDb());
        }
        table = db.getTable(tableName.getTbl()).orElse(null);
        if (table == null) {
            ErrorReport.reportAnalysisException("Table: {} not exists", tableName.getTbl());
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
            if (p.second == ColumnStatistic.DEFAULT) {
                return;
            }
            List<String> row = Lists.newArrayList();
            row.add(p.first);
            row.add(String.valueOf(p.second.count));
            row.add(String.valueOf(p.second.ndv));
            row.add(String.valueOf(p.second.numNulls));
            row.add(String.valueOf(p.second.dataSize));
            row.add(String.valueOf(p.second.avgSizeByte));
            row.add(String.valueOf(p.second.minValue));
            row.add(String.valueOf(p.second.maxValue));
            row.add(String.valueOf(p.second.minExpr == null ? "N/A" : p.second.minExpr.toSql()));
            row.add(String.valueOf(p.second.maxExpr == null ? "N/A" : p.second.maxExpr.toSql()));
            result.add(row);
        });
        return new ShowResultSet(getMetaData(), result);
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }
}
