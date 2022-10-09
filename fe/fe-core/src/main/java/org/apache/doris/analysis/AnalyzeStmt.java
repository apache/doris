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
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Collect statistics.
 *
 * syntax:
 * ANALYZE [[ db_name.tb_name ] [( column_name [, ...] )], ...] [ PROPERTIES(...) ]
 *     db_name.tb_name: collect table and column statistics from tb_name
 *     column_name: collect column statistics from column_name
 *     properties: properties of statistics jobs
 */
public class AnalyzeStmt extends DdlStmt {
    // time to wait for collect  statistics
    public static final String CBO_STATISTICS_TASK_TIMEOUT_SEC = "cbo_statistics_task_timeout_sec";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(CBO_STATISTICS_TASK_TIMEOUT_SEC)
            .build();

    private static final Predicate<Long> DESIRED_TASK_TIMEOUT_SEC = (v) -> v > 0L;

    private final TableName optTableName;
    private final PartitionNames optPartitionNames;
    private final List<String> optColumnNames;
    private Map<String, String> optProperties;

    // after analyzed
    private long dbId;
    private final Set<Long> tblIds = Sets.newHashSet();
    private final List<String> partitionNames = Lists.newArrayList();

    // TODO(wzt): support multiple tables
    public AnalyzeStmt(TableName optTableName,
            List<String> optColumnNames,
            PartitionNames optPartitionNames,
            Map<String, String> optProperties) {
        this.optTableName = optTableName;
        this.optColumnNames = optColumnNames;
        this.optPartitionNames = optPartitionNames;
        this.optProperties = optProperties;
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
        return analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbId);
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
        Preconditions.checkArgument(isAnalyzed(),
                "The partitionNames must be obtained after the parsing is complete");
        return partitionNames;
    }

    /**
     * The statistics task obtains partitions and then collects partition statistics,
     * we need to filter out partitions that do not have data.
     *
     * @return map of tableId and partitionName
     * @throws AnalysisException not analyzed
     */
    public Map<Long, List<String>> getTableIdToPartitionName() throws AnalysisException {
        Preconditions.checkArgument(isAnalyzed(),
                "The partitionIds must be obtained after the parsing is complete");
        Map<Long, List<String>> tableIdToPartitionName = Maps.newHashMap();

        for (Table table : getTables()) {
            table.readLock();
            try {
                OlapTable olapTable = (OlapTable) table;
                List<String> partitionNames = getPartitionNames();
                List<String> newPartitionNames = new ArrayList<>(partitionNames);
                if (newPartitionNames.isEmpty() && olapTable.isPartitioned()) {
                    newPartitionNames.addAll(olapTable.getPartitionNames());
                }
                tableIdToPartitionName.put(table.getId(), newPartitionNames);
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
        if (optColumnNames == null || optColumnNames.isEmpty()) {
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
                tableIdToColumnName.put(tblId, optColumnNames);
            }
        }

        return tableIdToColumnName;
    }

    public Map<String, String> getProperties() {
        return optProperties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        // step1: analyze db, table and column
        if (optTableName != null) {
            optTableName.analyze(analyzer);

            // disallow external catalog
            Util.prohibitExternalCatalog(optTableName.getCtl(),
                    this.getClass().getSimpleName());

            String dbName = optTableName.getDb();
            String tblName = optTableName.getTbl();
            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);
            Table table = db.getTableOrAnalysisException(tblName);

            // external table is not supported
            checkAnalyzeType(table);
            checkAnalyzePriv(dbName, tblName);

            if (optColumnNames != null && !optColumnNames.isEmpty()) {
                table.readLock();
                try {
                    List<String> baseSchema = table.getBaseSchema(false)
                            .stream().map(Column::getName).collect(Collectors.toList());
                    Optional<String> optional = optColumnNames.stream()
                            .filter(entity -> !baseSchema.contains(entity)).findFirst();
                    if (optional.isPresent()) {
                        String columnName = optional.get();
                        ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                                columnName, FeNameFormat.getColumnNameRegex());
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
            Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbName);

            db.readLock();
            try {
                List<Table> tables = db.getTables();
                for (Table table : tables) {
                    checkAnalyzeType(table);
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

        // step2: analyze partition
        checkPartitionNames();

        // step3: analyze properties
        checkProperties();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        PaloAuth auth = Env.getCurrentEnv().getAuth();
        if (!auth.checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void checkAnalyzeType(Table table) throws AnalysisException {
        if (table.getType() != Table.TableType.OLAP) {
            throw new AnalysisException("Only OLAP table statistics are supported");
        }
    }

    private void checkPartitionNames() throws AnalysisException {
        if (optPartitionNames != null) {
            optPartitionNames.analyze(analyzer);
            if (optTableName != null) {
                Database db = analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(optTableName.getDb());
                OlapTable olapTable = (OlapTable) db.getTableOrAnalysisException(optTableName.getTbl());
                if (!olapTable.isPartitioned()) {
                    throw new AnalysisException("Not a partitioned table: " + olapTable.getName());
                }
                List<String> names = optPartitionNames.getPartitionNames();
                Set<String> olapPartitionNames = olapTable.getPartitionNames();
                List<String> tempPartitionNames = olapTable.getTempPartitions().stream()
                        .map(Partition::getName).collect(Collectors.toList());
                Optional<String> optional = names.stream()
                        .filter(name -> (tempPartitionNames.contains(name)
                                || !olapPartitionNames.contains(name)))
                        .findFirst();
                if (optional.isPresent()) {
                    throw new AnalysisException("Temporary partition or partition does not exist");
                }
            } else {
                throw new AnalysisException("Specify partition should specify table name as well");
            }
            partitionNames.addAll(optPartitionNames.getPartitionNames());
        }
    }

    private void checkProperties() throws UserException {
        if (optProperties == null) {
            optProperties = Maps.newHashMap();
        } else {
            Optional<String> optional = optProperties.keySet().stream().filter(
                    entity -> !PROPERTIES_SET.contains(entity)).findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException(optional.get() + " is invalid property");
            }
        }
        long taskTimeout = ((Long) Util.getLongPropertyOrDefault(optProperties.get(CBO_STATISTICS_TASK_TIMEOUT_SEC),
                Config.max_cbo_statistics_task_timeout_sec, DESIRED_TASK_TIMEOUT_SEC,
                CBO_STATISTICS_TASK_TIMEOUT_SEC + " should > 0")).intValue();
        optProperties.put(CBO_STATISTICS_TASK_TIMEOUT_SEC, String.valueOf(taskTimeout));
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE");

        if (optTableName != null) {
            sb.append(" ");
            sb.append(optTableName.toSql());
        }

        if (optColumnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(optColumnNames, ","));
            sb.append(")");
        }

        if (optPartitionNames != null) {
            sb.append(" ");
            sb.append(optPartitionNames.toSql());
        }

        if (optProperties != null) {
            sb.append(" ");
            sb.append("PROPERTIES(");
            sb.append(new PrintableMap<>(optProperties, " = ",
                    true,
                    false));
            sb.append(")");
        }

        return sb.toString();
    }
}
