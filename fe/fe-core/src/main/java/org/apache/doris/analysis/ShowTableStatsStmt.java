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
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.statistics.TableStats;

import com.google.common.collect.ImmutableList;

import org.apache.parquet.Preconditions;
import org.apache.parquet.Strings;

public class ShowTableStatsStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("table_name")
                    .add(TableStats.ROW_COUNT.getValue())
                    .add(TableStats.DATA_SIZE.getValue())
                    .build();

    private TableName tableName;

    // after analyzed
    // There is only on attribute for both @tableName and @dbName at the same time.
    private String dbName;

    public ShowTableStatsStmt(TableName tableName) {
        this.tableName = tableName;
    }

    public String getTableName() {
        Preconditions.checkArgument(isAnalyzed(), "The db name must be obtained after the parsing is complete");
        if (tableName == null) {
            return null;
        }
        return tableName.getTbl();
    }

    public String getDbName() {
        Preconditions.checkArgument(isAnalyzed(), "The db name must be obtained after the parsing is complete");
        if (tableName == null) {
            return dbName;
        }
        return tableName.getDb();
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        if (tableName == null) {
            dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            return;
        }
        tableName.analyze(analyzer);
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
