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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisMethod;
import org.apache.doris.statistics.AnalysisTaskInfo.AnalysisType;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
  * Column Statistics Collection Syntax:
  *   ANALYZE [ SYNC ] TABLE table_name
  *   [ (column_name [, ...]) ]
  *   [ [WITH SYNC] | [WITH INCREMENTAL] | [WITH SAMPLE PERCENT | ROWS ] ]
  *   [ PROPERTIES ('key' = 'value', ...) ];
  *
  * Column histogram collection syntax:
  *   ANALYZE [ SYNC ] TABLE table_name
  *   [ (column_name [, ...]) ]
  *   UPDATE HISTOGRAM
  *   [ [ WITH SYNC ][ WITH INCREMENTAL ][ WITH SAMPLE PERCENT | ROWS ][ WITH BUCKETS ] ]
  *   [ PROPERTIES ('key' = 'value', ...) ];
  *
  * Illustrate：
  * - sync：Collect statistics synchronously. Return after collecting.
  * - incremental：Collect statistics incrementally. Incremental collection of histogram statistics is not supported.
  * - sample percent | rows：Collect statistics by sampling. Scale and number of rows can be sampled.
  * - buckets：Specifies the maximum number of buckets generated when collecting histogram statistics.
  * - table_name: The purpose table for collecting statistics. Can be of the form `db_name.table_name`.
  * - column_name: The specified destination column must be a column that exists in `table_name`,
 *    and multiple column names are separated by commas.
  * - properties：Properties used to set statistics tasks. Currently only the following configurations
 *    are supported (equivalent to the with statement)
  *    - 'sync' = 'true'
  *    - 'incremental' = 'true'
  *    - 'sample.percent' = '50'
  *    - 'sample.rows' = '1000'
  *    - 'num.buckets' = 10
 */
public class AnalyzeStmt extends DdlStmt {
    // The properties passed in by the user through "with" or "properties('K', 'V')"
    public static final String PROPERTY_SYNC = "sync";
    public static final String PROPERTY_INCREMENTAL = "incremental";
    public static final String PROPERTY_SAMPLE_PERCENT = "sample.percent";
    public static final String PROPERTY_SAMPLE_ROWS = "sample.rows";
    public static final String PROPERTY_NUM_BUCKETS = "num.buckets";
    public static final String PROPERTY_ANALYSIS_TYPE = "analysis.type";

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>()
            .add(PROPERTY_SYNC)
            .add(PROPERTY_INCREMENTAL)
            .add(PROPERTY_SAMPLE_PERCENT)
            .add(PROPERTY_SAMPLE_ROWS)
            .add(PROPERTY_NUM_BUCKETS)
            .add(PROPERTY_ANALYSIS_TYPE)
            .build();

    private final TableName tableName;
    private final List<String> columnNames;
    private final Map<String, String> properties;

    // after analyzed
    private long dbId;
    private TableIf table;

    public AnalyzeStmt(TableName tableName,
            List<String> columnNames,
            Map<String, String> properties) {
        this.tableName = tableName;
        this.columnNames = columnNames;
        this.properties = properties;
    }

    @Override
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

        // TODO support external table
        if (properties.containsKey(PROPERTY_SAMPLE_PERCENT)
                || properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            if (!(table instanceof OlapTable)) {
                throw new AnalysisException("Sampling statistics "
                        + "collection of external tables is not supported");
            }
        }
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
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("analysis properties should not be empty");
        }

        String msgTemplate = "%s = %s is invalid property";
        Optional<String> optional = properties.keySet().stream().filter(
                entity -> !PROPERTIES_SET.contains(entity)).findFirst();

        if (optional.isPresent()) {
            String msg = String.format(msgTemplate, optional.get(), properties.get(optional.get()));
            throw new AnalysisException(msg);
        }

        if (properties.containsKey(PROPERTY_SYNC)) {
            try {
                Boolean.valueOf(properties.get(PROPERTY_SYNC));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_SYNC, properties.get(PROPERTY_SYNC));
                throw new AnalysisException(msg);
            }
        }

        if (properties.containsKey(PROPERTY_INCREMENTAL)) {
            try {
                Boolean.valueOf(properties.get(PROPERTY_INCREMENTAL));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_INCREMENTAL, properties.get(PROPERTY_INCREMENTAL));
                throw new AnalysisException(msg);
            }
        }

        if (properties.containsKey(PROPERTY_SAMPLE_PERCENT)
                && properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            throw new AnalysisException("only one sampling parameter can be specified simultaneously");
        }

        if (properties.containsKey(PROPERTY_SAMPLE_PERCENT)) {
            checkNumericProperty(PROPERTY_SAMPLE_PERCENT, properties.get(PROPERTY_SAMPLE_PERCENT),
                    1, 100, true, "should be >= 1 and <= 100");
        }

        if (properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            checkNumericProperty(PROPERTY_SAMPLE_ROWS, properties.get(PROPERTY_SAMPLE_ROWS),
                    0, Integer.MAX_VALUE, false, "needs at least 1 row");
        }

        if (properties.containsKey(PROPERTY_NUM_BUCKETS)) {
            checkNumericProperty(PROPERTY_NUM_BUCKETS, properties.get(PROPERTY_NUM_BUCKETS),
                    1, Integer.MAX_VALUE, true, "needs at least 1 buckets");
        }

        if (properties.containsKey(PROPERTY_ANALYSIS_TYPE)) {
            try {
                AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE));
            } catch (NumberFormatException e) {
                String msg = String.format(msgTemplate, PROPERTY_ANALYSIS_TYPE, properties.get(PROPERTY_ANALYSIS_TYPE));
                throw new AnalysisException(msg);
            }
        }

        if (properties.containsKey(PROPERTY_INCREMENTAL)
                && AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE)) == AnalysisType.HISTOGRAM) {
            throw new AnalysisException(PROPERTY_INCREMENTAL + " collection of histograms is not supported");
        }

        if (properties.containsKey(PROPERTY_NUM_BUCKETS)
                && AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE)) != AnalysisType.HISTOGRAM) {
            throw new AnalysisException(PROPERTY_NUM_BUCKETS + " can only be specified when collecting histograms");
        }
    }

    private void checkNumericProperty(String key, String value, int lowerBound, int upperBound,
            boolean includeBoundary, String errorMsg) throws AnalysisException {
        if (!StringUtils.isNumeric(value)) {
            String msg = String.format("%s = %s is an invalid property.", key, value);
            throw new AnalysisException(msg);
        }
        int intValue = Integer.parseInt(value);
        boolean isOutOfBounds = (includeBoundary && (intValue < lowerBound || intValue > upperBound))
                || (!includeBoundary && (intValue <= lowerBound || intValue >= upperBound));
        if (isOutOfBounds) {
            throw new AnalysisException(key + " " + errorMsg);
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
        return properties;
    }

    public boolean isSync() {
        return Boolean.parseBoolean(properties.get(PROPERTY_SYNC));
    }

    public boolean isIncremental() {
        return Boolean.parseBoolean(properties.get(PROPERTY_INCREMENTAL));
    }

    public int getSamplePercent() {
        if (!properties.containsKey(PROPERTY_SAMPLE_PERCENT)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_SAMPLE_PERCENT));
    }

    public int getSampleRows() {
        if (!properties.containsKey(PROPERTY_SAMPLE_ROWS)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_SAMPLE_ROWS));
    }

    public int getNumBuckets() {
        if (!properties.containsKey(PROPERTY_NUM_BUCKETS)) {
            return 0;
        }
        return Integer.parseInt(properties.get(PROPERTY_NUM_BUCKETS));
    }

    public AnalysisType getAnalysisType() {
        return AnalysisType.valueOf(properties.get(PROPERTY_ANALYSIS_TYPE));
    }

    public AnalysisMethod getAnalysisMethod() {
        if (getSamplePercent() > 0 || getSampleRows() > 0) {
            return AnalysisMethod.SAMPLE;
        }
        return AnalysisMethod.FULL;
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
