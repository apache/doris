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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
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
import org.apache.commons.lang.StringUtils;

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
    // time to wait for collect  statistics
    public static final String CBO_STATISTICS_TASK_TIMEOUT_SEC = "cbo_statistics_task_timeout_sec";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CBO_STATISTICS_TASK_TIMEOUT_SEC)
            .build();

    private static final Predicate<Long> DESIRED_TASK_TIMEOUT_SEC = (v) -> v > 0L;

    private final TableName dbTableName;
    private final PartitionNames partitionNames;
    private final List<String> columnNames;
    private final Map<String, String> properties;

    // after analyzed
    private long dbId;
    private final Set<Long> tblIds = Sets.newHashSet();

    public AnalyzeStmt(TableName dbTableName,
            List<String> columns,
            PartitionNames partitionNames,
            Map<String, String> properties) {
        this.dbTableName = dbTableName;
        this.columnNames = columns;
        this.partitionNames = partitionNames;
        this.properties = properties == null ? Maps.newHashMap() : properties;
    }

    public long getDbId() {
        Preconditions.checkArgument(isAnalyzed(),
                "The dbId must be obtained after the parsing is complete");
        return dbId;
    }

    public Set<Long> getTblIds() {
        Preconditions.checkArgument(isAnalyzed(),
                "The tblIds must be obtained after the parsing is complete");
        return tblIds;
    }

    public Database getDb() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The db must be obtained after the parsing is complete");
        return analyzer.getCatalog().getInternalDataSource().getDbOrAnalysisException(dbId);
    }

    public List<Table> getTables() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The tables must be obtained after the parsing is complete");
        Database db = getDb();
        List<Table> tables = Lists.newArrayList();

        db.readLock();
        try {
            for (Long tblId : tblIds) {
                Table table = db.getTableOrAnalysisException(tblId);
                tables.add(table);
            }
        } finally {
            db.readUnlock();
        }

        return tables;
    }

    public List<String> getPartitionNames() {
        if (partitionNames == null) {
            return Lists.newArrayList();
        }
        return partitionNames.getPartitionNames();
    }

    public Map<Long, List<String>> getTableIdToPartitionName() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The partitionIds must be obtained after the parsing is complete");
        Map<Long, List<String>> tableIdToPartitionName = Maps.newHashMap();

        for (Table table : getTables()) {
            table.readLock();
            try {
                OlapTable olapTable = (OlapTable) table;
                List<String> partitionNames = getPartitionNames();
                if (partitionNames.isEmpty() && olapTable.isPartitioned()) {
                    partitionNames.addAll(olapTable.getPartitionNames());
                }
                tableIdToPartitionName.put(table.getId(), partitionNames);
            } finally {
                table.readUnlock();
            }
        }
        return tableIdToPartitionName;
    }

    public Map<Long, List<String>> getTableIdToColumnName() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The db name must be obtained after the parsing is complete");
        Map<Long, List<String>> tableIdToColumnName = Maps.newHashMap();
        List<Table> tables = getTables();
        if (columnNames == null || columnNames.isEmpty()) {
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
            for (Long tblId : tblIds) {
                tableIdToColumnName.put(tblId, columnNames);
            }
        }

        return tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // step1: analyze db, table and column
        if (dbTableName != null) {
            dbTableName.analyze(analyzer);

            // disallow external catalog
            Util.prohibitExternalCatalog(dbTableName.getCtl(),
                    this.getClass().getSimpleName());

            String dbName = dbTableName.getDb();
            String tblName = dbTableName.getTbl();
            checkAnalyzePriv(dbName, tblName);

            Database db = analyzer.getCatalog().getInternalDataSource()
                    .getDbOrAnalysisException(dbName);
            Table table = db.getTableOrAnalysisException(tblName);

            if (columnNames != null && !columnNames.isEmpty()) {
                table.readLock();
                try {
                    List<String> baseSchema = table.getBaseSchema(false)
                            .stream().map(Column::getName).collect(Collectors.toList());
                    Optional<String> optional = columnNames.stream()
                            .filter(entity -> !baseSchema.contains(entity)).findFirst();
                    if (optional.isPresent()) {
                        String columnName = optional.get();
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME, columnName);
                    }
                } finally {
                    table.readUnlock();
                }
            }

            dbId = db.getId();
            tblIds.add(table.getId());
        } else {
            // analyze the current default db
            String dbName = analyzer.getDefaultDb();
            if (Strings.isNullOrEmpty(dbName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
            Database db = analyzer.getCatalog().getInternalDataSource()
                    .getDbOrAnalysisException(dbName);

            db.readLock();
            try {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    checkAnalyzePriv(dbName, table.getName());
                }

                dbId = db.getId();
                for (Table table : tables) {
                    long tblId = table.getId();
                    tblIds.add(tblId);
                }
            } finally {
                db.readUnlock();
            }
        }

        // step2: check partition
        if (partitionNames != null) {
            partitionNames.analyze(analyzer);
        }

        // step3: analyze properties
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
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }

        long taskTimeout = ((Long) Util.getLongPropertyOrDefault(properties.get(CBO_STATISTICS_TASK_TIMEOUT_SEC),
                Config.max_cbo_statistics_task_timeout_sec, DESIRED_TASK_TIMEOUT_SEC,
                CBO_STATISTICS_TASK_TIMEOUT_SEC + " should > 0")).intValue();
        properties.put(CBO_STATISTICS_TASK_TIMEOUT_SEC, String.valueOf(taskTimeout));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE");

        if (dbTableName != null) {
            sb.append(" ");
            sb.append(dbTableName.toSql());
        }

        if  (columnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(columnNames, ","));
            sb.append(")");
        }

        if (partitionNames != null) {
            sb.append(" ");
            sb.append(partitionNames.toSql());
        }

        if (properties != null) {
            sb.append(" ");
            sb.append("PROPERTIES(");
            sb.append(new PrintableMap<>(properties, " = ",
                    true,
                    false));
            sb.append(")");
        }

        return sb.toString();
    }
}
