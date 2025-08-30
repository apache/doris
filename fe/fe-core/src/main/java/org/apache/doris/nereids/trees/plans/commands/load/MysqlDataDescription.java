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

package org.apache.doris.nereids.trees.plans.commands.load;

import org.apache.doris.analysis.ImportColumnDesc;
import org.apache.doris.analysis.Separator;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.datasource.property.fileformat.FileFormatProperties;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.plans.commands.info.PartitionNamesInfo;
import org.apache.doris.nereids.trees.plans.commands.info.TableNameInfo;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * MysqlDataDescription
 */
public class MysqlDataDescription {
    private final List<String> filePaths;
    private String dbName;
    private String tableName;
    private final PartitionNamesInfo partitionNamesInfo;
    private final Separator columnSeparator;
    private Separator lineDelimiter;
    private int skipLines = 0;
    private List<String> columns;
    private final List<Expression> columnMappingList;
    private final Map<String, String> properties;
    private boolean clientLocal;
    private List<ImportColumnDesc> parsedColumnExprList = Lists.newArrayList();
    private FileFormatProperties fileFormatProperties;

    // This map is used to collect information of file format properties.
    // The map should be only used in `constructor` and `analyzeWithoutCheckPriv` method.
    private Map<String, String> analysisMap = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);

    /**
     * MysqlDataDescription
     */
    public MysqlDataDescription(List<String> filePaths,
                                TableNameInfo tableNameInfo,
                                boolean clientLocal,
                                PartitionNamesInfo partitionNamesInfo,
                                Optional<String> columnSeparator,
                                Optional<String> lineDelimiter,
                                int skipLines,
                                List<String> columns,
                                List<Expression> columnMappingList,
                                Map<String, String> properties) {
        Objects.requireNonNull(filePaths, "filePaths is null");
        Objects.requireNonNull(tableNameInfo, "tableNameInfo is null");
        Objects.requireNonNull(clientLocal, "clientLocal is null");
        Objects.requireNonNull(partitionNamesInfo, "partitionNamesInfo is null");
        Objects.requireNonNull(columnSeparator, "columnSeparator is null");
        Objects.requireNonNull(lineDelimiter, "lineDelimiter is null");
        Objects.requireNonNull(skipLines, "skipLines is null");
        Objects.requireNonNull(columns, "columns is null");
        Objects.requireNonNull(columnMappingList, "columnMappingList is null");
        Objects.requireNonNull(properties, "properties is null");

        this.filePaths = filePaths;
        this.dbName = tableNameInfo.getDb();
        this.tableName = tableNameInfo.getTbl();
        this.clientLocal = clientLocal;
        this.partitionNamesInfo = partitionNamesInfo;
        this.columnSeparator = new Separator(columnSeparator.orElse(null));
        this.lineDelimiter = new Separator(lineDelimiter.orElse(null));
        this.skipLines = skipLines;
        this.columns = columns;
        this.columnMappingList = columnMappingList;
        this.properties = properties;
        this.analysisMap.putAll(properties);
    }

    public List<String> getFilePaths() {
        return filePaths;
    }

    public String getDbName() {
        return dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public PartitionNamesInfo getPartitionNamesInfo() {
        return partitionNamesInfo;
    }

    public String getColumnSeparator() {
        if (columnSeparator == null) {
            return null;
        }
        return columnSeparator.getSeparator();
    }

    public String getLineDelimiter() {
        if (lineDelimiter == null) {
            return null;
        }
        return lineDelimiter.getSeparator();
    }

    public int getSkipLines() {
        return skipLines;
    }

    public List<String> getColumns() {
        return columns;
    }

    public List<Expression> getColumnMappingList() {
        return columnMappingList;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public boolean isClientLocal() {
        return clientLocal;
    }

    /**
     * analyzeFullDbName
     */
    public String analyzeFullDbName(ConnectContext ctx) throws AnalysisException {
        String dbName = Strings.isNullOrEmpty(getDbName()) ? ctx.getDatabase() : getDbName();
        if (Strings.isNullOrEmpty(dbName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
        }
        this.dbName = dbName;
        return this.dbName;
    }

    /**
     * analyze
     */
    public void analyze(String fullDbName) throws UserException {
        checkLoadPriv(fullDbName);
        analyzeWithoutCheckPriv(fullDbName);
    }

    private void checkLoadPriv(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(tableName)) {
            throw new AnalysisException("No table name in load statement.");
        }

        // check auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, tableName,
                    PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), fullDbName + ": " + tableName);
        }
    }

    /**
     * analyzeWithoutCheckPriv
     */
    public void analyzeWithoutCheckPriv(String fullDbName) throws UserException {
        analyzeFilePaths();
        analyzeLoadAttributes();
        analyzeColumns();

        fileFormatProperties = FileFormatProperties.createFileFormatPropertiesOrDeferred(
                analysisMap.getOrDefault(FileFormatProperties.PROP_FORMAT, ""));
        fileFormatProperties.analyzeFileFormatProperties(analysisMap, false);
    }

    private void analyzeFilePaths() throws AnalysisException {
        if (filePaths.isEmpty()) {
            throw new AnalysisException("No file path in load command.");
        }
        filePaths.replaceAll(String::trim);
    }

    private void analyzeLoadAttributes() throws UserException {
        if (columnSeparator.getOriSeparator() != null) {
            columnSeparator.analyze(false);
        }

        if (lineDelimiter.getOriSeparator() != null) {
            lineDelimiter.analyze(true);
        }

        if (partitionNamesInfo.getPartitionNames() != null && !partitionNamesInfo.getPartitionNames().isEmpty()) {
            partitionNamesInfo.validate();
        }
    }

    private void analyzeColumns() throws AnalysisException {
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        // analyze columns
        for (String columnName : columns) {
            if (!columnNames.add(columnName)) {
                throw new AnalysisException("Duplicate column: " + columnName);
            }
            ImportColumnDesc importColumnDesc = new ImportColumnDesc(columnName, null);
            parsedColumnExprList.add(importColumnDesc);
        }
    }

    public FileFormatProperties getFileFormatProperties() {
        return fileFormatProperties;
    }

    /**
     * toSql
     */
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DATA ").append(isClientLocal() ? "LOCAL " : "");
        sb.append("INFILE '").append(filePaths.get(0)).append("'");
        sb.append(" INTO TABLE ");
        sb.append(ClusterNamespace.getNameFromFullName(dbName) + "." + tableName);
        sb.append(" ");
        sb.append(partitionNamesInfo.toSql());

        if (columnSeparator != null) {
            sb.append(" COLUMNS TERMINATED BY ").append(columnSeparator.toSql());
        }
        if (lineDelimiter != null) {
            sb.append(" LINES TERMINATED BY ").append(lineDelimiter.toSql());
        }
        if (!columns.isEmpty()) {
            sb.append(" (");
            Joiner.on(", ").appendTo(sb, columns).append(")");
        }

        if (!columnMappingList.isEmpty()) {
            sb.append(" SET (");
            Joiner.on(", ").appendTo(sb, Lists.transform(columnMappingList, new Function<Expression, Object>() {
                @Override
                public Object apply(Expression expr) {
                    return expr.toSql();
                }
            })).append(")");
        }
        return sb.toString();
    }
}
