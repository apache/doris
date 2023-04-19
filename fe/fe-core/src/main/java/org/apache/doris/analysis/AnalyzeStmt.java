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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

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

    public boolean isWholeTbl;
    public boolean isHistogram;
    public boolean isIncrement;

    private final TableName tableName;

    private final boolean sync;
    private final List<String> columnNames;
    private final Map<String, String> properties;

    // after analyzed
    private long dbId;
    private TableIf table;

    public AnalyzeStmt(TableName tableName,
                       boolean sync,
                       List<String> columnNames,
                       Map<String, String> properties,
                       Boolean isWholeTbl,
                       Boolean isHistogram,
                       Boolean isIncrement) {
        this.tableName = tableName;
        this.sync = sync;
        this.columnNames = columnNames;
        this.properties = properties;
        this.isWholeTbl = isWholeTbl;
        this.isHistogram = isHistogram;
        this.isIncrement = isIncrement;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);

        tableName.analyze(analyzer);

        String catalogName = tableName.getCtl();
        String dbName = tableName.getDb();
        String tblName = tableName.getTbl();
        CatalogIf catalog = analyzer.getEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalogName);
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        dbId = db.getId();
        table = db.getTableOrAnalysisException(tblName);
        if (table instanceof View) {
            throw new AnalysisException("Analyze view is not allowed");
        }
        checkAnalyzePriv(dbName, tblName);

        if (columnNames != null && !columnNames.isEmpty()) {
            table.readLock();
            try {
                List<String> baseSchema = table.getBaseSchema(false)
                        .stream().map(Column::getName).collect(Collectors.toList());
                Optional<String> optional = columnNames.stream()
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

        checkProperties();
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }

    private void checkAnalyzePriv(String dbName, String tblName) throws AnalysisException {
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), dbName, tblName, PrivPredicate.SELECT)) {
            ErrorReport.reportAnalysisException(
                    ErrorCode.ERR_TABLEACCESS_DENIED_ERROR,
                    "ANALYZE",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(),
                    dbName + ": " + tblName);
        }
    }

    private void checkProperties() throws UserException {
        if (properties != null) {
            Optional<String> optional = properties.keySet().stream().filter(
                    entity -> !PROPERTIES_SET.contains(entity)).findFirst();
            if (optional.isPresent()) {
                throw new AnalysisException(optional.get() + " is invalid property");
            }

            long taskTimeout = ((Long) Util.getLongPropertyOrDefault(
                    properties.get(CBO_STATISTICS_TASK_TIMEOUT_SEC),
                    Config.max_cbo_statistics_task_timeout_sec, DESIRED_TASK_TIMEOUT_SEC,
                    CBO_STATISTICS_TASK_TIMEOUT_SEC + " should > 0")).intValue();
            properties.put(CBO_STATISTICS_TASK_TIMEOUT_SEC, String.valueOf(taskTimeout));
        }
    }

    public String getCatalogName() {
        return tableName.getCtl();
    }

    public long getDbId() {
        return dbId;
    }

    public String getDBName() {
        return tableName.getDb();
    }

    public Database getDb() throws AnalysisException {
        return analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbId);
    }

    public TableIf getTable() {
        return table;
    }

    public TableName getTblName() {
        return tableName;
    }

    public Set<String> getColumnNames() {
        return columnNames == null ? table.getBaseSchema(false)
                .stream().map(Column::getName).collect(Collectors.toSet()) : Sets.newHashSet(columnNames);
    }

    public Map<String, String> getProperties() {
        // TODO add default properties
        return properties != null ? properties : Maps.newHashMap();
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE");

        if (isIncrement) {
            sb.append(" ");
            sb.append("INCREMENTAL");
        }

        if (tableName != null) {
            sb.append(" ");
            sb.append(tableName.toSql());
        }

        if (isHistogram) {
            sb.append(" ");
            sb.append("UPDATE HISTOGRAM ON");
            sb.append(" ");
            sb.append(StringUtils.join(columnNames, ","));
        } else if (columnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(columnNames, ","));
            sb.append(")");
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

    public boolean isSync() {
        return sync;
    }
}
