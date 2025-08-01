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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Table;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.dictionary.LayoutType;
import org.apache.doris.mtmv.MTMVRelatedTableIf;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Dictionary info in creating dictionary.
 */
public class CreateDictionaryInfo {
    private final boolean ifNotExists;

    // only support for internal catalog now. need enhance.
    private String dbName;
    private final String dictName;

    private String sourceCtlName;

    private String sourceDbName;
    private final String sourceTableName;

    private final List<DictionaryColumnDefinition> columns;

    private final Map<String, String> properties;
    private final LayoutType layout;
    private long dataLifetime;
    private boolean skipNullKey = false;

    private long memoryLimit = 2 * 1024 * 1024 * 1024L; // 2GB for default

    /**
     * Constructor.
     */
    public CreateDictionaryInfo(boolean ifNotExists, String dbName, String dictName, String sourceCtlName,
            String sourceDbName, String sourceTableName, List<DictionaryColumnDefinition> columns,
            Map<String, String> properties, LayoutType layout) {
        this.ifNotExists = ifNotExists;
        this.dbName = dbName;
        this.dictName = Objects.requireNonNull(dictName, "Dictionary name cannot be null");
        this.sourceCtlName = sourceCtlName;
        this.sourceDbName = sourceDbName;
        this.sourceTableName = Objects.requireNonNull(sourceTableName, "Source table name cannot be null");
        this.columns = Objects.requireNonNull(columns, "Columns cannot be null");
        this.properties = Objects.requireNonNull(properties, "Properties cannot be null");
        this.layout = layout;
    }

    /**
     * Get dictionary name parts.
     *
     * @return list of dictionary name parts
     */
    public List<String> getDictionaryNameParts() {
        List<String> parts = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(dbName)) {
            parts.add(dbName);
        }
        parts.add(dictName);
        return parts;
    }

    /**
     * Get source table name parts.
     *
     * @return list of source table name parts
     */
    public List<String> getSourceTableNameParts() {
        List<String> parts = Lists.newArrayList();
        if (!Strings.isNullOrEmpty(sourceCtlName)) {
            parts.add(sourceCtlName);
        }
        if (!Strings.isNullOrEmpty(sourceDbName)) {
            parts.add(sourceDbName);
        }
        parts.add(sourceTableName);
        return parts;
    }

    /**
     * Validate create dictionary info. about dictionary name, source table legal.
     *
     * @throws DdlException if not found db
     */
    public void validateAndSet(ConnectContext ctx) throws org.apache.doris.common.AnalysisException, DdlException {
        // Check dictionary name format
        FeNameFormat.checkTableName(dictName);

        // Check database of dictionary exists
        if (Strings.isNullOrEmpty(dbName)) {
            dbName = ctx.getDatabase(); // not specified, use current database
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException(ErrorCode.ERR_NO_DB_ERROR.formatErrorMsg());
            }
        } else {
            if (Strings.isNullOrEmpty(dbName)) {
                throw new AnalysisException(ErrorCode.ERR_NO_DB_ERROR.formatErrorMsg());
            }
            InternalCatalog catalog = Env.getCurrentEnv().getInternalCatalog();
            catalog.getDbOrDdlException(dbName);
        }

        // Check source table legal
        CatalogIf<?> catalog;
        DatabaseIf<?> sourceDb;
        if (Strings.isNullOrEmpty(sourceCtlName)) {
            catalog = ctx.getCurrentCatalog();
            sourceCtlName = catalog.getName(); // set from null to current catalog name
        } else {
            catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(sourceCtlName);
            if (catalog == null) {
                throw new AnalysisException("Catalog " + sourceCtlName + " does not exist");
            }
        }
        if (Strings.isNullOrEmpty(sourceDbName)) {
            sourceDbName = ctx.getDatabase(); // not specified, use current database
            if (Strings.isNullOrEmpty(sourceDbName)) {
                throw new AnalysisException(ErrorCode.ERR_NO_DB_ERROR.formatErrorMsg());
            }
        } else {
            if (Strings.isNullOrEmpty(sourceDbName)) {
                throw new AnalysisException(ErrorCode.ERR_NO_DB_ERROR.formatErrorMsg());
            }
        }
        sourceDb = catalog.getDbOrDdlException(sourceDbName);
        // Check the column existance. check and set their types
        Table table = (Table) sourceDb.getTableOrDdlException(sourceTableName);
        validateAndSetColumns(table);
        validateAndSetProperties();
    }

    private void validateAndSetColumns(Table table) throws DdlException {
        if (!(table instanceof OlapTable || table instanceof MTMVRelatedTableIf)) {
            throw new DdlException("Source table " + table.getName() + " is not a valid table type. "
                    + "Only OlapTable and MTMVRelatedTableIf are supported");
        }
        List<Column> schema = table.getFullSchema();
        // Build a map of source table columns for quick lookup
        Map<String, Column> sourceColumns = new HashMap<>();
        Set<String> usedColNames = new HashSet<>();
        for (Column column : schema) {
            sourceColumns.put(column.getName().toLowerCase(), column);
        }

        // Validate at least one Key/Value column
        if (columns.stream().filter(DictionaryColumnDefinition::isKey).count() < 1) {
            throw new DdlException("Need at least one key column");
        }
        if (columns.stream().filter(c -> !c.isKey()).count() < 1) {
            throw new DdlException("Need at least one value column");
        }
        if (getLayout() == LayoutType.IP_TRIE && columns.stream().filter(c -> c.isKey()).count() != 1) {
            throw new DdlException("IP_TRIE layout requires exactly one key column");
        }

        // Validate each dictionary column exists in source table and set its type
        for (DictionaryColumnDefinition columnDef : columns) {
            Column sourceColumn = sourceColumns.get(columnDef.getName().toLowerCase());
            if (sourceColumn == null) {
                throw new DdlException(
                        "Column " + columnDef.getName() + " not found in source table " + table.getName());
            }
            if (usedColNames.contains(columnDef.getName().toLowerCase())) {
                throw new DdlException("Column " + columnDef.getName() + " is used more than once");
            }
            if (columnDef.isKey()) {
                validateKeyColumn(sourceColumn);
            }
            columnDef.setType(sourceColumn.getType());
            columnDef.setNullable(sourceColumn.isAllowNull());
            columnDef.setOriginColumn(new Column(sourceColumn)); // copy to avoid changing. TODO: consider SC
            usedColNames.add(sourceColumn.getName().toLowerCase());
        }
    }

    private void validateKeyColumn(Column source) throws DdlException {
        if (source.getType().isComplexType()) {
            throw new DdlException("Key column " + source.getName() + " cannot be complex type");
        }
        if (getLayout() == LayoutType.IP_TRIE) {
            if (!source.getType().isVarcharOrStringType()) {
                throw new DdlException("Key column " + source.getName() + " must be String type for IP_TRIE layout");
            }
        }
    }

    private void validateAndSetProperties() throws DdlException {
        if (!properties.containsKey("data_lifetime")) {
            throw new DdlException("Property 'data_lifetime' is required");
        }
        try {
            dataLifetime = Long.parseLong(properties.get("data_lifetime"));
        } catch (NumberFormatException e) {
            throw new DdlException("Property 'data_lifetime' must be a number");
        }

        if (properties.containsKey("skip_null_key")) {
            skipNullKey = Boolean.parseBoolean(properties.get("skip_null_key"));
        }

        if (properties.containsKey("memory_limit")) {
            memoryLimit = Long.parseLong(properties.get("data_lifetime"));
        }
    }

    // Getters
    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getDbName() {
        return dbName;
    }

    public String getDictName() {
        return dictName;
    }

    public String getSourceCtlName() {
        return sourceCtlName;
    }

    public String getSourceDbName() {
        return sourceDbName;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public List<DictionaryColumnDefinition> getColumns() {
        return columns;
    }

    public long getDataLifetime() {
        return dataLifetime;
    }

    public boolean skipNullKey() {
        return skipNullKey;
    }

    public long getMemoryLimit() {
        return memoryLimit;
    }

    public LayoutType getLayout() {
        return layout;
    }
}
