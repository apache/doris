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

import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
    private final String tableName;
    private String dbName;
    private final PartitionNames partitionNames;
    private final List<String> filePaths;
    private final Separator columnSeparator;
    private String fileFormat;
    private final boolean isNegative;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final List<Expression> columnMappingList;
    private final Expression precedingFilterExpr;
    private final Expression whereExpr;
    private LoadTask.MergeType mergeType;
    private final String srcTableName;
    // column names of source files
    private List<String> fileFieldNames;
    private Separator lineDelimiter;
    private String sequenceCol;

    // Merged from fileFieldNames, columnsFromPath and columnMappingList
    // ImportColumnDesc: column name to (expr or null)
    private List<ImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
    private final Expression deleteCondition;
    private final Map<String, String> properties;
    private boolean isMysqlLoad = false;

    public BulkLoadDataDesc(List<String> fullTableName,
                            PartitionNames partitionNames,
                            List<String> filePaths,
                            List<String> columns,
                            Separator columnSeparator,
                            Separator lineDelimiter,
                            String fileFormat,
                            List<String> columnsFromPath,
                            boolean isNegative,
                            List<Expression> columnMappingList,
                            Expression fileFilterExpr,
                            Expression whereExpr,
                            LoadTask.MergeType mergeType,
                            Expression deleteCondition,
                            String sequenceColName,
                            Map<String, String> properties) {
        this.dbName = Objects.requireNonNull(fullTableName.get(1), "Database name should not null");
        this.tableName = Objects.requireNonNull(fullTableName.get(2), "Table name should not null");
        this.partitionNames = partitionNames;
        this.filePaths = filePaths;
        this.fileFieldNames = columns;
        this.columnSeparator = columnSeparator;
        this.lineDelimiter = lineDelimiter;
        this.fileFormat = fileFormat;
        this.columnsFromPath = columnsFromPath;
        this.isNegative = isNegative;
        this.columnMappingList = columnMappingList;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.mergeType = mergeType;
        this.srcTableName = null;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceColName;
        this.properties = properties;
        columnsNameToLowerCase(fileFieldNames);
        columnsNameToLowerCase(columnsFromPath);
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    // Change all the columns name to lower case, because Doris column is case-insensitive.
    private void columnsNameToLowerCase(List<String> columns) {
        if (columns == null || columns.isEmpty() || "json".equals(this.fileFormat)) {
            return;
        }
        for (int i = 0; i < columns.size(); i++) {
            String column = columns.remove(i);
            columns.add(i, column.toLowerCase());
        }
    }

    public String toInsertSql(Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ");
        sb.append(dbName).append(".").append(tableName);
        sb.append("(");
        List<String> srcColumns = getSrcColumnsFromTable(tableName);
        List<String> mappingTargetColumns = tryToTransFormToMappingColumns(srcColumns);
        appendColumnFromPathColumns(mappingTargetColumns);
        appendVirtualColumnForDeleteAndSeq(mappingTargetColumns);
        Joiner.on(", ").appendTo(sb, mappingTargetColumns);
        sb.append(")");

        sb.append("SELECT ");
        List<String> mappingSrcColumns = tryToSetMappingColumns(fileFieldNames);
        appendColumnValuesFromPathColumns(mappingTargetColumns);
        appendDeleteOnConditions(mappingTargetColumns);
        Joiner.on(", ").appendTo(sb, mappingSrcColumns);
        sb.append("FROM ");

        // TODO: check if s3
        // if is s3 data desc type
        sb.append("s3( ");
        // properties.isEmpty();
        // append s3 properties
        sb.append(")");
        Expression rewrittenWhere = rewriteByColumnMappingSet(whereExpr);
        String rewrittenWhereClause = rewriteByPrecedingFilter(rewrittenWhere, precedingFilterExpr);
        sb.append(" WHERE ").append(rewrittenWhereClause);
        return sb.toString();
    }

    private String rewriteByPrecedingFilter(Expression whereExpr, Expression precedingFilterExpr) {
        return whereExpr.toSql();
    }

    private Expression rewriteByColumnMappingSet(Expression whereExpr) {
        return whereExpr;
    }

    private void appendDeleteOnConditions(List<String> mappingTargetColumns) {
    }

    private void appendColumnValuesFromPathColumns(List<String> mappingTargetColumns) {
    }

    private List<String> tryToSetMappingColumns(List<String> fileFieldNames) {
        // process set clause
        return new ArrayList<>();
    }

    private void appendVirtualColumnForDeleteAndSeq(List<String> mappingSrcColumns) {}

    private void appendColumnFromPathColumns(List<String> mappingSrcColumns) {
        mappingSrcColumns.addAll(columnsFromPath);
    }

    private List<String> tryToTransFormToMappingColumns(List<String> srcColumns) {
        return new ArrayList<>();
    }

    private List<String> getSrcColumnsFromTable(String tableName) {
        return new ArrayList<>();
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
        if (partitionNames != null) {
            sb.append(" ");
            sb.append(partitionNames.toSql());
        }
        if (columnSeparator != null) {
            sb.append(" COLUMNS TERMINATED BY ").append(columnSeparator.toSql());
        }
        if (lineDelimiter != null && isMysqlLoad) {
            sb.append(" LINES TERMINATED BY ").append(lineDelimiter.toSql());
        }
        if (fileFormat != null && !fileFormat.isEmpty()) {
            sb.append(" FORMAT AS '" + fileFormat + "'");
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

