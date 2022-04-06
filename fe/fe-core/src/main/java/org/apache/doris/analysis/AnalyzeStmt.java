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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.clearspring.analytics.util.Lists;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Collect statistics about a database
 * <p>
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 * <p>
 * db_name.tb_name: collect table and column statistics from tb_name
 * <p>
 * column_name: collect column statistics from column_name
 * <p>
 * properties: properties of statistics jobs
 */
public class AnalyzeStmt extends DdlStmt {
    private final TableName dbTableName;
    private final List<String> columnNames;
    private Map<String, String> properties;

    // after analyzed
    private Database db;
    private List<Table> tables;
    private final Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();

    public AnalyzeStmt(TableName dbTableName, List<String> columns, Map<String, String> properties) {
        this.dbTableName = dbTableName;
        this.columnNames = columns;
        this.properties = properties;
    }

    public Database getDb() {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        return this.db;
    }

    public List<Table> getTables() {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        return this.tables;
    }

    public Map<Long, List<String>> getTableIdToColumnName() {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        return this.tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // step1: analyze database and table
        if (this.dbTableName != null) {
            String dbName;
            if (Strings.isNullOrEmpty(this.dbTableName.getDb())) {
                dbName = analyzer.getDefaultDb();
            } else {
                dbName = ClusterNamespace.getFullName(analyzer.getClusterName(), this.dbTableName.getDb());
            }

            // check db
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            this.db = analyzer.getCatalog().getDbOrAnalysisException(dbName);

            // check table
            String tblName = this.dbTableName.getTbl();
            if (Strings.isNullOrEmpty(tblName)) {
                this.tables = this.db.getTables();
                for (Table table : this.tables) {
                    checkAnalyzePriv(dbName, table.getName());
                }
            } else {
                Table table = this.db.getTableOrAnalysisException(tblName);
                this.tables = Collections.singletonList(table);
                checkAnalyzePriv(dbName, table.getName());
            }

            // check column
            if (this.columnNames == null || this.columnNames.isEmpty()) {
                setTableIdToColumnName();
            } else {
                Table table = this.db.getTableOrAnalysisException(tblName);
                for (String columnName : this.columnNames) {
                    Column column = table.getColumn(columnName);
                    if (column == null) {
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
                    }
                }
                this.tableIdToColumnName.put(table.getId(), this.columnNames);
            }
        } else {
            // analyze the current default db
            String dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            this.db = analyzer.getCatalog().getDbOrAnalysisException(dbName);
            this.tables = this.db.getTables();
            for (Table table : this.tables) {
                checkAnalyzePriv(dbName, table.getName());
            }
            setTableIdToColumnName();
        }

        // step2: analyze properties
        if (this.properties != null) {
            for (Map.Entry<String, String> pros : this.properties.entrySet()) {
                if (!"cbo_statistics_task_timeout_sec".equals(pros.getKey())) {
                    throw new AnalysisException("Unsupported property: " + pros.getKey());
                }
                if (!StringUtils.isNumeric(pros.getValue()) || Integer.parseInt(pros.getValue()) <= 0) {
                    throw new AnalysisException("Invalid property value: " + pros.getValue());
                }
            }
        } else {
            this.properties = Maps.newHashMap();
            this.properties.put("cbo_statistics_task_timeout_sec", String.valueOf(Config.cbo_statistics_task_timeout_sec));
        }
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Catalog.getCurrentCatalog().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void setTableIdToColumnName() {
        for (Table table : this.tables) {
            long tableId = table.getId();
            List<Column> baseSchema = table.getBaseSchema();
            List<String> colNames = Lists.newArrayList();
            baseSchema.stream().map(Column::getName).forEach(colNames::add);
            this.tableIdToColumnName.put(tableId, colNames);
        }
    }
}

