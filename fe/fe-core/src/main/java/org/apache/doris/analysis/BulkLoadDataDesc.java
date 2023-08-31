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
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.util.PrintableMap;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.tablefunction.HdfsTableValuedFunction;
import org.apache.doris.tablefunction.S3TableValuedFunction;

import com.google.common.base.Joiner;
import com.google.common.base.VerifyException;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

// used to describe data info which is needed to import.
//
//      data_desc:
//          DATA INFILE ('file_path', ...)
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [COLUMNS TERMINATED BY separator]
//          [FORMAT AS format]
//          [(tmp_col1, tmp_col2, col3, ...)]
//          [COLUMNS FROM PATH AS (col1, ...)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]
//
//          DATA FROM TABLE external_hive_tbl_name
//          [NEGATIVE]
//          INTO TABLE tbl_name
//          [PARTITION (p1, p2)]
//          [SET (k1=f1(xx), k2=f2(xxx))]
//          [where_clause]

/**
 * The transform of columns should be added after the keyword named COLUMNS.
 * The transform after the keyword named SET is the old ways which only supports the hadoop function.
 * It old way of transform will be removed gradually. It
 */
public class BulkLoadDataDesc {
    private static final Logger LOG = LogManager.getLogger(BulkLoadDataDesc.class);
    public static final String EXPECT_MERGE_DELETE_ON = "not support DELETE ON clause when merge type is not MERGE.";
    public static final String EXPECT_DELETE_ON = "Excepted DELETE ON clause when merge type is MERGE.";
    private final String tableName;
    private final String dbName;
    private final List<String> partitionNames;
    private final List<String> filePaths;
    private final boolean isNegative;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final List<Expression> columnMappingList;
    private final Expression precedingFilterExpr;
    private final Expression whereExpr;
    private final LoadTask.MergeType mergeType;
    private final String srcTableName;
    // column names of source files
    private final List<String> fileFieldNames;
    private final String sequenceCol;
    private final FileFormatDesc formatDesc;
    // Merged from fileFieldNames, columnsFromPath and columnMappingList
    // ImportColumnDesc: column name to (expr or null)
    private final Expression deleteCondition;
    private final Map<String, String> dataProperties;
    private boolean isMysqlLoad = false;

    public BulkLoadDataDesc(List<String> fullTableName,
                            List<String> partitionNames,
                            List<String> filePaths,
                            List<String> columns,
                            List<String> columnsFromPath,
                            List<Expression> columnMappingList,
                            FileFormatDesc formatDesc,
                            boolean isNegative,
                            Expression fileFilterExpr,
                            Expression whereExpr,
                            LoadTask.MergeType mergeType,
                            Expression deleteCondition,
                            String sequenceColName,
                            Map<String, String> dataProperties) {
        this.dbName = Objects.requireNonNull(fullTableName.get(1), "dbName should not null");
        this.tableName = Objects.requireNonNull(fullTableName.get(2), "tableName should not null");
        this.partitionNames = Objects.requireNonNull(partitionNames, "partitionNames should not null");
        this.filePaths = Objects.requireNonNull(filePaths, "filePaths should not null");
        this.formatDesc = Objects.requireNonNull(formatDesc, "formatDesc should not null");
        this.fileFieldNames = columnsNameToLowerCase(Objects.requireNonNull(columns, "columns should not null"));
        this.columnsFromPath = columnsNameToLowerCase(columnsFromPath);
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.mergeType = mergeType;
        // maybe from tvf or table
        this.srcTableName = null;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceColName;
        this.dataProperties = dataProperties;
    }

    public static class FileFormatDesc {
        private final Separator lineDelimiter;
        private final Separator columnSeparator;
        private final String fileFormat;

        public FileFormatDesc(String fileFormat) {
            this(null, null, fileFormat);
        }

        public FileFormatDesc(String lineDelimiter, String columnSeparator) {
            this(lineDelimiter, columnSeparator, null);
        }

        public FileFormatDesc(String lineDelimiter, String columnSeparator, String fileFormat) {
            this.lineDelimiter = new Separator(lineDelimiter);
            this.columnSeparator = new Separator(columnSeparator);
            try {
                if (!StringUtils.isEmpty(this.lineDelimiter.getOriSeparator())) {
                    this.lineDelimiter.analyze();
                }
                if (!StringUtils.isEmpty(this.columnSeparator.getOriSeparator())) {
                    this.columnSeparator.analyze();
                }
            } catch (AnalysisException e) {
                throw new RuntimeException("Fail to parse separator. ", e);
            }
            this.fileFormat = fileFormat;
        }

        public Optional<Separator> getLineDelimiter() {
            if (lineDelimiter == null || lineDelimiter.getOriSeparator() == null) {
                return Optional.empty();
            }
            return Optional.of(lineDelimiter);
        }

        public Optional<Separator> getColumnSeparator() {
            if (columnSeparator == null || columnSeparator.getOriSeparator() == null) {
                return Optional.empty();
            }
            return Optional.of(columnSeparator);
        }

        public String getFileFormat() {
            return fileFormat;
        }
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public List<String> getPartitionNames() {
        return partitionNames;
    }

    public FileFormatDesc getFormatDesc() {
        return formatDesc;
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public List<String> getColumnsFromPath() {
        return columnsFromPath;
    }

    public Map<String, String> getProperties() {
        return dataProperties;
    }

    // Change all the columns name to lower case, because Doris column is case-insensitive.
    private List<String> columnsNameToLowerCase(List<String> columns) {
        if (columns == null || columns.isEmpty() || "json".equals(this.formatDesc.fileFormat)) {
            return columns;
        }
        List<String> lowerCaseColumns = new ArrayList<>();
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.get(i);
            lowerCaseColumns.add(i, column.toLowerCase());
        }
        return lowerCaseColumns;
    }

    public String toInsertSql(BulkStorageDesc.StorageType storageType, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append("`").append(dbName).append("`");
        sb.append(".");
        sb.append("`").append(tableName).append("`");
        sb.append("(");

        List<String> targetColumns = getTargetColumnsFromTable(dbName, tableName);
        verifyLoadColumns(fileFieldNames, targetColumns);

        List<String> mappingExpressions = new ArrayList<>();
        if (!columnMappingList.isEmpty()) {
            for (Expression mappingExpr : columnMappingList) {
                String targetColumn = mappingExpr.child(0).toSql();
                if (targetColumns.contains(targetColumn)) {
                    mappingExpressions.add(mappingExpr.child(1).toSql());
                }
                // check key is in target columns
            }
        }
        // create target columns for olap sink table
        appendColumnFromPathColumns(targetColumns);
        appendVirtualColumnForDeleteAndSeq(targetColumns);
        Joiner.on(", ").appendTo(sb, targetColumns);
        sb.append(")");

        // create src columns from data input file
        sb.append(" SELECT ");
        List<String> mappingSrcColumns = setMappingExpressionsToSrc(fileFieldNames, mappingExpressions);
        appendColumnValuesFromPathColumns(mappingSrcColumns);
        appendDeleteOnConditions(mappingSrcColumns);
        Joiner.on(", ").appendTo(sb, mappingSrcColumns);
        sb.append(" FROM ");

        // create tvf clause
        if (BulkStorageDesc.StorageType.S3 == storageType) {
            appendS3TvfTable(sb, properties);
        } else if (BulkStorageDesc.StorageType.HDFS == storageType) {
            appendHdfsTvfTable(sb, properties);
        } else {
            throw new UnsupportedOperationException("Unsupported storage type " + storageType + " for nereids load. ");
        }
        Expression rewrittenWhere = rewriteByColumnMappingSet(whereExpr);
        String rewrittenWhereClause = rewriteByPrecedingFilter(rewrittenWhere, precedingFilterExpr);
        sb.append(" WHERE ").append(rewrittenWhereClause);
        return sb.toString();
    }

    private void appendHdfsTvfTable(StringBuilder sb, Map<String, String> properties) {
        sb.append(HdfsTableValuedFunction.NAME);
        sb.append("( ");
        PrintableMap<String, String> printableMap = new PrintableMap<>(properties, "=", true, false, ",");
        sb.append(printableMap);
        sb.append(")");
    }

    private static void appendS3TvfTable(StringBuilder sb, Map<String, String> properties) {
        sb.append(S3TableValuedFunction.NAME);
        sb.append("( ");
        PrintableMap<String, String> printableMap = new PrintableMap<>(properties, "=", true, false, ",");
        sb.append(printableMap);
        sb.append(")");
    }

    private void verifyLoadColumns(List<String> fileFieldNames, List<String> targetColumns) {
        if (targetColumns.size() > fileFieldNames.size()) {
            throw new VerifyException("Sink columns size is less than source columns size.");
        }
    }

    private String rewriteByPrecedingFilter(Expression whereExpr, Expression precedingFilterExpr) {
        return whereExpr.toSql();
    }

    private Expression rewriteByColumnMappingSet(Expression whereExpr) {
        return whereExpr;
    }

    private void appendDeleteOnConditions(List<String> mappingTargetColumns) {
        if (mergeType != LoadTask.MergeType.MERGE && deleteCondition != null) {
            throw new IllegalArgumentException(EXPECT_MERGE_DELETE_ON);
        }
        if (mergeType == LoadTask.MergeType.MERGE && deleteCondition == null) {
            throw new IllegalArgumentException(EXPECT_DELETE_ON);
        }
    }

    private void appendColumnValuesFromPathColumns(List<String> mappingTargetColumns) {
    }

    private List<String> setMappingExpressionsToSrc(List<String> fileFieldNames, List<String> mappingExpressions) {
        if (mappingExpressions.isEmpty()) {
            return fileFieldNames;
        }
        // process set clause
        List<String> mappedColumns = new ArrayList<>();
        for (String fileField : fileFieldNames) {
            // TODO: replace source column when it is in mapping expressions.
            mappedColumns.add(fileField);
        }
        return mappedColumns;
    }

    private void appendVirtualColumnForDeleteAndSeq(List<String> mappingSrcColumns) {}

    private void appendColumnFromPathColumns(List<String> mappingSrcColumns) {
        mappingSrcColumns.addAll(columnsFromPath);
    }

    private List<String> getTargetColumnsFromTable(String dbName, String tableName) {
        List<String> columnNames = new ArrayList<>();
        String fullDbName = ClusterNamespace.getFullName(SystemInfoService.DEFAULT_CLUSTER, dbName);
        try {
            DatabaseIf<?> databaseIf = Env.getCurrentEnv().getInternalCatalog().getDbNullable(fullDbName);
            Table targetTable = (Table) databaseIf.getTableOrAnalysisException(tableName);
            List<Column> columns = targetTable.getColumns();
            columns.stream()
                    .filter(c -> !c.isVersionColumn())
                    .filter(c -> !c.isDeleteSignColumn())
                    .forEach(e -> columnNames.add(e.getName()));
        } catch (AnalysisException e) {
            throw new RuntimeException(e);
        }
        return columnNames;
    }

    @Override
    public String toString() {
        return toSql();
    }

    public String toSql() {
        StringBuilder sb = new StringBuilder();
        if (isMysqlLoad) {
            sb.append("DATA ").append(isClientLocal() ? "LOCAL " : "");
            sb.append("INFILE '").append(filePaths.get(0)).append("'");
        } else if (isLoadFromTable()) {
            sb.append(mergeType.toString());
            sb.append(" DATA FROM TABLE ").append(srcTableName);
        } else {
            sb.append(mergeType.toString());
            sb.append(" DATA INFILE (");
            Joiner.on(", ").appendTo(sb, filePaths.stream()
                    .map(s -> "'" + s + "'").collect(Collectors.toList())).append(")");
        }
        if (isNegative) {
            sb.append(" NEGATIVE");
        }
        sb.append(" INTO TABLE ");
        sb.append(isMysqlLoad ? ClusterNamespace.getNameFromFullName(dbName) + "." + tableName : tableName);
        if (partitionNames != null && !partitionNames.isEmpty()) {
            sb.append(" (");
            Joiner.on(", ").appendTo(sb, partitionNames).append(")");
        }
        if (formatDesc.columnSeparator != null) {
            sb.append(" COLUMNS TERMINATED BY ").append(formatDesc.columnSeparator.toSql());
        }
        if (formatDesc.lineDelimiter != null && isMysqlLoad) {
            sb.append(" LINES TERMINATED BY ").append(formatDesc.lineDelimiter.toSql());
        }
        if (formatDesc.fileFormat != null && !formatDesc.fileFormat.isEmpty()) {
            sb.append(" FORMAT AS '" + formatDesc.fileFormat + "'");
        }
        if (fileFieldNames != null && !fileFieldNames.isEmpty()) {
            sb.append(" (");
            Joiner.on(", ").appendTo(sb, fileFieldNames).append(")");
        }
        if (columnsFromPath != null && !columnsFromPath.isEmpty()) {
            sb.append(" COLUMNS FROM PATH AS (");
            Joiner.on(", ").appendTo(sb, columnsFromPath).append(")");
        }
        if (columnMappingList != null && !columnMappingList.isEmpty()) {
            sb.append(" SET (");
            Joiner.on(", ").appendTo(sb, columnMappingList.stream()
                    .map(Expression::toSql).collect(Collectors.toList())).append(")");
        }
        if (whereExpr != null) {
            sb.append(" WHERE ").append(whereExpr.toSql());
        }
        if (deleteCondition != null && mergeType == LoadTask.MergeType.MERGE) {
            sb.append(" DELETE ON ").append(deleteCondition.toSql());
        }
        return sb.toString();
    }

    private boolean isLoadFromTable() {
        return false;
    }

    private boolean isClientLocal() {
        return false;
    }
}

