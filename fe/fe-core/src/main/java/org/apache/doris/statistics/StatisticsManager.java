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

package org.apache.doris.statistics;

import org.apache.doris.analysis.AlterColumnStatsStmt;
import org.apache.doris.analysis.AlterTableStatsStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

public class StatisticsManager {
    private Statistics statistics;

    public StatisticsManager() {
        statistics = new Statistics();
    }

    public void alterTableStatistics(AlterTableStatsStmt stmt)
            throws DdlException, AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        statistics.updateTableStats(table.getId(), stmt.getProperties());
    }

    public void alterColumnStatistics(AlterColumnStatsStmt stmt) throws DdlException, AnalysisException {
        Table table = validateTableName(stmt.getTableName());
        String columnName = stmt.getColumnName();
        Column column = table.getColumn(columnName);
        if (column == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_FIELD_ERROR, columnName);
        }
        // match type and column value
        statistics.updateColumnStats(table.getId(), columnName, column.getType(), stmt.getProperties());
    }

    private Table validateTableName(TableName dbTableName) throws DdlException {
        String dbName = dbTableName.getDb();
        String tableName = dbTableName.getTbl();

        Database db = Catalog.getCurrentCatalog().getDb(dbName);
        if (db == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_DB_ERROR, dbName);
        }
        Table table = db.getTable(tableName);
        if (table == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_BAD_TABLE_ERROR, tableName);
        }
        return table;
    }
}
