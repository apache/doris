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
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.catalog.external.HMSExternalTable;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisInfo.AnalysisType;
import org.apache.doris.statistics.ColumnStatistic;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Column Statistics Collection Syntax:
 * ANALYZE [ SYNC ] TABLE table_name
 * [ PARTITIONS (partition_name [, ...])]
 * [ (column_name [, ...]) ]
 * [ [WITH SYNC] | [WITH INCREMENTAL] | [WITH SAMPLE PERCENT | ROWS ] ]
 * [ PROPERTIES ('key' = 'value', ...) ];
 * <p>
 * Column histogram collection syntax:
 * ANALYZE [ SYNC ] TABLE table_name
 * [ partitions (partition_name [, ...])]
 * [ (column_name [, ...]) ]
 * UPDATE HISTOGRAM
 * [ [ WITH SYNC ][ WITH INCREMENTAL ][ WITH SAMPLE PERCENT | ROWS ][ WITH BUCKETS ] ]
 * [ PROPERTIES ('key' = 'value', ...) ];
 * <p>
 * Illustrate：
 * - sync：Collect statistics synchronously. Return after collecting.
 * - incremental：Collect statistics incrementally. Incremental collection of histogram statistics is not supported.
 * - sample percent | rows：Collect statistics by sampling. Scale and number of rows can be sampled.
 * - buckets：Specifies the maximum number of buckets generated when collecting histogram statistics.
 * - table_name: The purpose table for collecting statistics. Can be of the form `db_name.table_name`.
 * - partition_name: The specified destination partition must be a partition that exists in `table_name`,
 * - column_name: The specified destination column must be a column that exists in `table_name`,
 * and multiple column names are separated by commas.
 * - properties：Properties used to set statistics tasks. Currently only the following configurations
 * are supported (equivalent to the with statement)
 * - 'sync' = 'true'
 * - 'incremental' = 'true'
 * - 'sample.percent' = '50'
 * - 'sample.rows' = '1000'
 * - 'num.buckets' = 10
 */
public class AnalyzeTblStmt extends AnalyzeStmt {
    // The properties passed in by the user through "with" or "properties('K', 'V')"

    private final TableName tableName;
    private List<String> columnNames;
    private List<String> partitionNames;
    private boolean isAllColumns;

    // after analyzed
    private long dbId;
    private TableIf table;

    public AnalyzeTblStmt(TableName tableName,
            PartitionNames partitionNames,
            List<String> columnNames,
            AnalyzeProperties properties) {
        super(properties);
        this.tableName = tableName;
        this.partitionNames = partitionNames == null ? null : partitionNames.getPartitionNames();
        this.columnNames = columnNames;
        this.analyzeProperties = properties;
        this.isAllColumns = columnNames == null;
    }

    public AnalyzeTblStmt(AnalyzeProperties analyzeProperties, TableName tableName, List<String> columnNames, long dbId,
            TableIf table) {
        super(analyzeProperties);
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.dbId = dbId;
        this.table = table;
        this.isAllColumns = columnNames == null;
    }

    @Override
    @SuppressWarnings({"rawtypes"})
    public void analyze(Analyzer analyzer) throws UserException {
        if (!Config.enable_stats) {
            throw new UserException("Analyze function is forbidden, you should add `enable_stats=true`"
                    + "in your FE conf file");
        }
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
        isAllColumns = columnNames == null;
        check();
    }

    public void check() throws AnalysisException {
        if (table instanceof View) {
            throw new AnalysisException("Analyze view is not allowed");
        }
        checkAnalyzePriv(tableName.getDb(), tableName.getTbl());
        if (columnNames == null) {
            // Filter unsupported type columns.
            columnNames = table.getBaseSchema(false).stream()
                .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                .map(Column::getName)
                .collect(Collectors.toList());
        }
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
            checkColumn();
        } finally {
            table.readUnlock();
        }
        analyzeProperties.check();

        // TODO support external table
        if (analyzeProperties.isSample()) {
            if (!(table instanceof OlapTable)) {
                throw new AnalysisException("Sampling statistics "
                        + "collection of external tables is not supported");
            }
        }
        if (analyzeProperties.isSync()
                && (analyzeProperties.isAutomatic() || analyzeProperties.getPeriodTimeInMs() != 0)) {
            throw new AnalysisException("Automatic/Period statistics collection "
                    + "and synchronous statistics collection cannot be set at same time");
        }
        if (analyzeProperties.isAutomatic() && analyzeProperties.getPeriodTimeInMs() != 0) {
            throw new AnalysisException("Automatic collection "
                    + "and period statistics collection cannot be set at same time");
        }
    }

    private void checkColumn() throws AnalysisException {
        boolean containsUnsupportedTytpe = false;
        for (String colName : columnNames) {
            Column column = table.getColumn(colName);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                        colName, FeNameFormat.getColumnNameRegex());
            }
            if (ColumnStatistic.UNSUPPORTED_TYPE.contains(column.getType())) {
                containsUnsupportedTytpe = true;
            }
        }
        if (containsUnsupportedTytpe) {
            if (!ConnectContext.get().getSessionVariable().enableAnalyzeComplexTypeColumn) {
                columnNames = columnNames.stream()
                        .filter(c -> !StatisticsUtil.isUnsupportedType(table.getColumn(c).getType()))
                        .collect(Collectors.toList());
            } else {
                throw new AnalysisException(
                        "Contains unsupported column type"
                                + "if you want to ignore them and analyze rest"
                                + "columns, please set session variable "
                                + "`ignore_column_with_complex_type` to true");
            }
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

    public Set<String> getPartitionNames() {
        Set<String> partitions = partitionNames == null ? table.getPartitionNames() : Sets.newHashSet(partitionNames);
        if (isSamplingPartition()) {
            int partNum = ConnectContext.get().getSessionVariable().getExternalTableAnalyzePartNum();
            partitions = partitions.stream().limit(partNum).collect(Collectors.toSet());
        }
        return partitions;
    }

    public boolean isPartitionOnly() {
        return partitionNames != null;
    }

    public boolean isSamplingPartition() {
        if (!(table instanceof HMSExternalTable) || partitionNames != null) {
            return false;
        }
        int partNum = ConnectContext.get().getSessionVariable().getExternalTableAnalyzePartNum();
        if (partNum == -1 || partitionNames != null) {
            return false;
        }
        return table instanceof HMSExternalTable && table.getPartitionNames().size() > partNum;
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

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE TABLE ");

        if (tableName != null) {
            sb.append(" ");
            sb.append(tableName.toSql());
        }

        if (columnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(columnNames, ","));
            sb.append(")");
        }

        if (getAnalysisType().equals(AnalysisType.HISTOGRAM)) {
            sb.append(" ");
            sb.append("UPDATE HISTOGRAM");
        }

        if (analyzeProperties != null) {
            sb.append(" ");
            sb.append(analyzeProperties.toSQL());
        }

        return sb.toString();
    }

    public Database getDb() throws AnalysisException {
        return analyzer.getEnv().getInternalCatalog().getDbOrAnalysisException(dbId);
    }

    public boolean isAllColumns() {
        return isAllColumns;
    }
}
