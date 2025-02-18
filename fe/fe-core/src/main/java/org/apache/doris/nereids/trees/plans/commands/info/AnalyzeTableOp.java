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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.analysis.AnalyzeProperties;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.View;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.hive.HMSExternalTable;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.statistics.AnalysisInfo;
import org.apache.doris.statistics.util.StatisticsUtil;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * AnalyzeTableOp
 */
public class AnalyzeTableOp extends AnalyzeOp {
    private final TableNameInfo tableNameInfo;
    private List<String> columnNames;
    private boolean isAllColumns;
    private PartitionNamesInfo partitionNames;

    // after analyzed
    private long catalogId;
    private long dbId;
    private TableIf table;
    private Database database;

    /**
     * AnalyzeTableOp
     */
    public AnalyzeTableOp(TableNameInfo tableNameInfo,
                          PartitionNamesInfo partitionNames,
                          List<String> columnNames,
                          AnalyzeProperties properties) {
        super(properties, PlanType.ANALYZE_TABLE);
        this.tableNameInfo = tableNameInfo;
        this.partitionNames = partitionNames;
        this.columnNames = columnNames;
        this.analyzeProperties = properties;
        this.isAllColumns = columnNames == null;
    }

    /**
     * AnalyzeTableOp
     */
    public AnalyzeTableOp(AnalyzeProperties analyzeProperties, TableNameInfo tableNameInfo,
                          List<String> columnNames, long dbId, TableIf table) throws AnalysisException {
        super(analyzeProperties, PlanType.ANALYZE_TABLE);
        this.tableNameInfo = tableNameInfo;
        this.columnNames = columnNames;
        this.dbId = dbId;
        this.table = table;
        this.isAllColumns = columnNames == null;
        String catalogName = tableNameInfo.getCtl();
        CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr()
                .getCatalogOrAnalysisException(catalogName);
        this.catalogId = catalog.getId();
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        super.validate(ctx);
        tableNameInfo.analyze(ctx);

        String catalogName = tableNameInfo.getCtl();
        String dbName = tableNameInfo.getDb();
        String tblName = tableNameInfo.getTbl();
        CatalogIf catalog = ctx.getEnv().getCatalogMgr().getCatalogOrAnalysisException(catalogName);
        this.catalogId = catalog.getId();
        DatabaseIf db = catalog.getDbOrAnalysisException(dbName);
        dbId = db.getId();
        table = db.getTableOrAnalysisException(tblName);
        isAllColumns = columnNames == null;
        database = ctx.getEnv().getInternalCatalog().getDbOrAnalysisException(dbId);
        check();
    }

    /**
     * check
     */
    public void check() throws AnalysisException {
        if (table instanceof View) {
            throw new AnalysisException("Analyze view is not allowed");
        }
        checkAnalyzePriv(tableNameInfo.getCtl(), tableNameInfo.getDb(), tableNameInfo.getTbl());
        if (columnNames == null) {
            columnNames = table.getSchemaAllIndexes(false).stream()
                    // Filter unsupported type columns.
                    .filter(c -> !StatisticsUtil.isUnsupportedType(c.getType()))
                    .map(Column::getName)
                    .collect(Collectors.toList());
        } else {
            table.readLock();
            try {
                List<String> baseSchema = table.getSchemaAllIndexes(false)
                        .stream().map(Column::getName).collect(Collectors.toList());
                Optional<String> optional = columnNames.stream()
                        .filter(entity -> !baseSchema.contains(entity)).findFirst();
                if (optional.isPresent()) {
                    String columnName = optional.get();
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_FIELD_ERROR,
                            columnName, tableNameInfo.getTbl());
                }
                checkColumn();
            } finally {
                table.readUnlock();
            }
        }
        analyzeProperties.check();

        if (analyzeProperties.isSync()
                && (analyzeProperties.isAutomatic() || analyzeProperties.getPeriodTimeInMs() != 0)) {
            throw new AnalysisException("Automatic/Period statistics collection "
                    + "and synchronous statistics collection cannot be set at same time");
        }
        if (analyzeProperties.isAutomatic() && analyzeProperties.getPeriodTimeInMs() != 0) {
            throw new AnalysisException("Automatic collection "
                    + "and period statistics collection cannot be set at same time");
        }
        if (analyzeProperties.isSample() && analyzeProperties.forceFull()) {
            throw new AnalysisException("Impossible to analyze with sample and full simultaneously");
        }
    }

    private void checkColumn() throws AnalysisException {
        boolean containsUnsupportedTytpe = false;
        for (String colName : columnNames) {
            Column column = table instanceof OlapTable
                    ? ((OlapTable) table).getVisibleColumn(colName)
                    : table.getColumn(colName);
            if (column == null) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_COLUMN_NAME,
                        colName, FeNameFormat.getColumnNameRegex());
            }
            if (StatisticsUtil.isUnsupportedType(column.getType())) {
                containsUnsupportedTytpe = true;
            }
        }
        if (containsUnsupportedTytpe) {
            if (ConnectContext.get() == null
                    || !ConnectContext.get().getSessionVariable().enableAnalyzeComplexTypeColumn) {
                columnNames = columnNames.stream()
                        .filter(c -> !StatisticsUtil.isUnsupportedType(table instanceof OlapTable
                                ? ((OlapTable) table).getVisibleColumn(c).getType()
                                : table.getColumn(c).getType()))
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
        return tableNameInfo.getCtl();
    }

    public long getDbId() {
        return dbId;
    }

    public String getDBName() {
        return tableNameInfo.getDb();
    }

    public TableIf getTable() {
        return table;
    }

    public TableNameInfo getTblName() {
        return tableNameInfo;
    }

    public Set<String> getColumnNames() {
        return Sets.newHashSet(columnNames);
    }

    /**
     * getPartitionNames
     */
    public Set<String> getPartitionNames() {
        if (partitionNames == null || partitionNames.getPartitionNames() == null || partitionNames.isStar()) {
            return Collections.emptySet();
        }
        Set<String> partitions = Sets.newHashSet();
        partitions.addAll(partitionNames.getPartitionNames());
        return partitions;
    }

    /**
     * isStarPartition
     * @return for OLAP table, only in overwrite situation, overwrite auto detect partition
     *         for External table, all partitions.
     */
    public boolean isStarPartition() {
        if (partitionNames == null) {
            return false;
        }
        return partitionNames.isStar();
    }

    public long getPartitionCount() {
        if (partitionNames == null) {
            return 0;
        }
        return partitionNames.getCount();
    }

    public boolean isPartitionOnly() {
        return partitionNames != null;
    }

    /**
     * isSamplingPartition
     */
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

    private void checkAnalyzePriv(String ctlName, String dbName, String tblName) throws AnalysisException {
        ConnectContext ctx = ConnectContext.get();
        // means it a system analyze
        if (ctx == null) {
            return;
        }
        if (!Env.getCurrentEnv().getAccessManager()
                    .checkTblPriv(ctx, ctlName, dbName, tblName, PrivPredicate.SELECT)) {
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

        if (tableNameInfo != null) {
            sb.append(" ");
            sb.append(tableNameInfo.toSql());
        }

        if (columnNames != null) {
            sb.append("(");
            sb.append(StringUtils.join(columnNames, ","));
            sb.append(")");
        }

        if (getAnalysisType().equals(AnalysisInfo.AnalysisType.HISTOGRAM)) {
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
        return database;
    }

    public boolean isAllColumns() {
        return isAllColumns;
    }

    public long getCatalogId() {
        return catalogId;
    }

    public PlanType getType() {
        return PlanType.ANALYZE_TABLE;
    }
}
