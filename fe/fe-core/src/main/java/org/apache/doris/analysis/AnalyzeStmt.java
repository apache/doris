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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.Util;
import org.apache.doris.mysql.privilege.PaloAuth;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Collect statistics about a database
 *
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 *
 *      db_name.tb_name: collect table and column statistics from tb_name
 *
 *      column_name: collect column statistics from column_name
 *
 *      properties: properties of statistics jobs
 */
public class AnalyzeStmt extends DdlStmt {
    private static final Logger LOG = LogManager.getLogger(AnalyzeStmt.class);

    // time to wait for collect  statistics
    public static final String CBO_STATISTICS_TASK_TIMEOUT_SEC = "cbo_statistics_task_timeout_sec";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CBO_STATISTICS_TASK_TIMEOUT_SEC)
            .build();

    public static final Predicate<Long> DESIRED_TASK_TIMEOUT_SEC = (v) -> v > 0L;

    private final TableName dbTableName;
    private final List<String> columnNames;
    private final Map<String, String> properties;

    // after analyzed
    private long dbId;
    private final Set<Long> tblIds = Sets.newHashSet();

    public AnalyzeStmt(TableName dbTableName, List<String> columns, Map<String, String> properties) {
        this.dbTableName = dbTableName;
        this.columnNames = columns;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public long getDbId() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbId must be obtained after the parsing is complete");
        return this.dbId;
    }

    public Set<Long> getTblIds() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tblIds must be obtained after the parsing is complete");
        return this.tblIds;
    }

    public Database getDb() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The db must be obtained after the parsing is complete");
        return this.analyzer.getCatalog().getDbOrAnalysisException(this.dbId);
    }

    public List<Table> getTables() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The tables must be obtained after the parsing is complete");
        Database db = getDb();
        List<Table> tables = Lists.newArrayList();

        db.readLock();
        try {
            for (Long tblId : this.tblIds) {
                Table table = db.getTableOrAnalysisException(tblId);
                tables.add(table);
            }
        } finally {
            db.readUnlock();
        }

        return tables;
    }

    public Map<Long, List<String>> getTableIdToColumnName() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        List<Table> tables = getTables();
        if (this.columnNames == null || this.columnNames.isEmpty()) {
            for (Table table : tables) {
                table.readLock();
                try {
                    long tblId = table.getId();
                    List<Column> baseSchema = table.getBaseSchema();
                    List<String> colNames = Lists.newArrayList();
                    baseSchema.stream().map(Column::getName).forEach(colNames::add);
                    tableIdToColumnName.put(tblId, colNames);
                } finally {
                    table.readUnlock();
                }
            }
        } else {
            for (Long tblId : this.tblIds) {
                tableIdToColumnName.put(tblId, this.columnNames);
            }
        }

        return tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // step1: analyze db, table and column
        if (this.dbTableName != null) {
            this.dbTableName.analyze(analyzer);
            String dbName = this.dbTableName.getDb();
            String tblName = this.dbTableName.getTbl();
            checkAnalyzePriv(dbName, tblName);

            Database db = analyzer.getCatalog().getDbOrAnalysisException(dbName);
            Table table = db.getTableOrAnalysisException(tblName);

            if (this.columnNames != null && !this.columnNames.isEmpty()) {
                table.readLock();
                try {
                    List<String> baseSchema = table.getBaseSchema(false)
                            .stream().map(Column::getName).collect(Collectors.toList());
                    Optional<String> optional = this.columnNames.stream()
                            .filter(entity -> !baseSchema.contains(entity)).findFirst();
                    if (optional.isPresent()) {
                        String columnName = optional.get();
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
                    }
                } finally {
                    table.readUnlock();
                }
            }

            this.dbId = db.getId();
            this.tblIds.add(table.getId());
        } else {
            // analyze the current default db
            String dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            Database db = analyzer.getCatalog().getDbOrAnalysisException(dbName);

            db.readLock();
            try {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    checkAnalyzePriv(dbName, table.getName());
                }

                this.dbId = db.getId();
                for (Table table : tables) {
                    long tblId = table.getId();
                    this.tblIds.add(tblId);
                }
            } finally {
                db.readUnlock();
            }
        }

        // step2: analyze properties
        checkProperties();
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

    private void checkProperties() throws UserException {
        Optional<String> optional = this.properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        long taskTimeout = ((Long) Util.getLongPropertyOrDefault(this.properties.get(CBO_STATISTICS_TASK_TIMEOUT_SEC),
                Config.max_cbo_statistics_task_timeout_sec, DESIRED_TASK_TIMEOUT_SEC,
                CBO_STATISTICS_TASK_TIMEOUT_SEC + " should > 0")).intValue();
        this.properties.put(CBO_STATISTICS_TASK_TIMEOUT_SEC, String.valueOf(taskTimeout));
    }
}

