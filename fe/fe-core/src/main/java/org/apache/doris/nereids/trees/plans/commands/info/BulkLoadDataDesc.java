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

import org.apache.doris.analysis.Separator;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.load.loadv2.LoadTask;
import org.apache.doris.nereids.trees.expressions.Expression;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * used to describe data info which is needed to import.
 * The transform of columns should be added after the keyword named COLUMNS.
 * The transform after the keyword named SET is the old ways which only supports the hadoop function.
 * It old way of transform will be removed gradually.
 *       data_desc:
 *           DATA INFILE ('file_path', ...)
 *           [NEGATIVE]
 *           INTO TABLE tbl_name
 *           [PARTITION (p1, p2)]
 *           [COLUMNS TERMINATED BY separator]
 *           [FORMAT AS format]
 *           [(tmp_col1, tmp_col2, col3, ...)]
 *           [COLUMNS FROM PATH AS (col1, ...)]
 *           [SET (k1=f1(xx), k2=f2(xxx))]
 *           [where_clause]
 *           DATA FROM TABLE external_hive_tbl_name
 *           [NEGATIVE]
 *           INTO TABLE tbl_name
 *           [PARTITION (p1, p2)]
 *           [SET (k1=f1(xx), k2=f2(xxx))]
 *           [where_clause]
 */
public class BulkLoadDataDesc {

    public static final String EXPECT_MERGE_DELETE_ON = "not support DELETE ON clause when merge type is not MERGE.";
    public static final String EXPECT_DELETE_ON = "Excepted DELETE ON clause when merge type is MERGE.";
    private static final Logger LOG = LogManager.getLogger(BulkLoadDataDesc.class);

    private final List<String> nameParts;
    private final String tableName;
    private final String dbName;
    private final List<String> partitionNames;
    private final List<String> filePaths;
    private final boolean isNegative;
    // column names in the path
    private final List<String> columnsFromPath;
    // save column mapping in SET(xxx = xxx) clause
    private final Map<String, Expression> columnMappings;
    private final Optional<Expression> precedingFilterExpr;
    private final Optional<Expression> whereExpr;
    private final LoadTask.MergeType mergeType;
    private final String srcTableName;
    // column names of source files
    private final List<String> fileFieldNames;
    private final Optional<String> sequenceCol;
    private final FileFormatDesc formatDesc;
    // Merged from fileFieldNames, columnsFromPath and columnMappingList
    // ImportColumnDesc: column name to (expr or null)
    private final Optional<Expression> deleteCondition;
    private final Map<String, String> dataProperties;
    private boolean isMysqlLoad = false;

    /**
     * bulk load desc
     */
    public BulkLoadDataDesc(List<String> fullTableName,
                            List<String> partitionNames,
                            List<String> filePaths,
                            List<String> columns,
                            List<String> columnsFromPath,
                            Map<String, Expression> columnMappings,
                            FileFormatDesc formatDesc,
                            boolean isNegative,
                            Optional<Expression> fileFilterExpr,
                            Optional<Expression> whereExpr,
                            LoadTask.MergeType mergeType,
                            Optional<Expression> deleteCondition,
                            Optional<String> sequenceColName,
                            Map<String, String> dataProperties) {
        this.nameParts = Objects.requireNonNull(fullTableName, "nameParts should not null");
        this.dbName = Objects.requireNonNull(fullTableName.get(1), "dbName should not null");
        this.tableName = Objects.requireNonNull(fullTableName.get(2), "tableName should not null");
        this.partitionNames = Objects.requireNonNull(partitionNames, "partitionNames should not null");
        this.filePaths = Objects.requireNonNull(filePaths, "filePaths should not null");
        this.formatDesc = Objects.requireNonNull(formatDesc, "formatDesc should not null");
        this.fileFieldNames = columnsNameToLowerCase(Objects.requireNonNull(columns, "columns should not null"));
        this.columnsFromPath = columnsNameToLowerCase(columnsFromPath);
        this.isNegative = isNegative;
        this.columnMappings = columnMappings;
        this.precedingFilterExpr = fileFilterExpr;
        this.whereExpr = whereExpr;
        this.mergeType = mergeType;
        // maybe from tvf or table
        this.srcTableName = null;
        this.deleteCondition = deleteCondition;
        this.sequenceCol = sequenceColName;
        this.dataProperties = dataProperties;
    }

    /**
     * bulk load file format desc
     */
    public static class FileFormatDesc {
        private final Separator lineDelimiter;
        private final Separator columnSeparator;
        private final String fileFormat;

        public FileFormatDesc(Optional<String> fileFormat) {
            this(Optional.empty(), Optional.empty(), fileFormat);
        }

        public FileFormatDesc(Optional<String> lineDelimiter, Optional<String> columnSeparator) {
            this(lineDelimiter, columnSeparator, Optional.empty());
        }

        /**
         * build bulk load format desc and check valid
         * @param lineDelimiter text format line delimiter
         * @param columnSeparator text format column separator
         * @param fileFormat file format
         */
        public FileFormatDesc(Optional<String> lineDelimiter, Optional<String> columnSeparator,
                              Optional<String> fileFormat) {
            this.lineDelimiter = new Separator(lineDelimiter.orElse(null));
            this.columnSeparator = new Separator(columnSeparator.orElse(null));
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
            this.fileFormat = fileFormat.orElse(null);
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

        public Optional<String> getFileFormat() {
            return Optional.ofNullable(fileFormat);
        }
    }

    public List<String> getNameParts() {
        return nameParts;
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

    public Map<String, Expression> getColumnMappings() {
        return columnMappings;
    }

    public Optional<Expression> getPrecedingFilterExpr() {
        return precedingFilterExpr;
    }

    public Optional<Expression> getWhereExpr() {
        return whereExpr;
    }

    public List<String> getFileFieldNames() {
        return fileFieldNames;
    }

    public Optional<String> getSequenceCol() {
        if (sequenceCol.isPresent() && StringUtils.isBlank(sequenceCol.get())) {
            return Optional.empty();
        }
        return sequenceCol;
    }

    public Optional<Expression> getDeleteCondition() {
        return deleteCondition;
    }

    public LoadTask.MergeType getMergeType() {
        return mergeType;
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

    @Override
    public String toString() {
        return toSql();
    }

    /**
     * print data desc load info
     * @return bulk load sql
     */
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
        if (columnMappings != null && !columnMappings.isEmpty()) {
            sb.append(" SET (");
            Joiner.on(", ").appendTo(sb, columnMappings.entrySet().stream()
                    .map(e -> e.getKey() + "=" + e.getValue().toSql()).collect(Collectors.toList())).append(")");
        }
        whereExpr.ifPresent(e -> sb.append(" WHERE ").append(e.toSql()));
        deleteCondition.ifPresent(e -> {
            if (mergeType == LoadTask.MergeType.MERGE) {
                sb.append(" DELETE ON ").append(e.toSql());
            }
        });
        return sb.toString();
    }

    private boolean isLoadFromTable() {
        return false;
    }

    private boolean isClientLocal() {
        return false;
    }
}

