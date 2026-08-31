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

package org.apache.doris.datasource.paimon;

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.DorisTypeVisitor;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseNotEmptyException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableAlreadyExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PaimonMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(PaimonMetadataOps.class);
    protected Catalog catalog;
    protected ExternalCatalog dorisCatalog;
    private ExecutionAuthenticator executionAuthenticator;
    private static final String PRIMARY_KEY_IDENTIFIER = "primary-key";
    private static final String PROP_COMMENT = "comment";
    private static final String PROP_LOCATION = "location";

    public PaimonMetadataOps(ExternalCatalog dorisCatalog, Catalog catalog) {
        this.dorisCatalog = dorisCatalog;
        this.catalog = catalog;
        this.executionAuthenticator = dorisCatalog.getExecutionAuthenticator();
    }


    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        try {
            return executionAuthenticator.execute(() -> performCreateDb(dbName, ifNotExists, properties));
        } catch (Exception e) {
            throw new DdlException("Failed to create database: "
                + dbName + ": " + Util.getRootCauseMessage(e), e);
        }
    }

    private boolean performCreateDb(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException, Catalog.DatabaseAlreadyExistException {
        if (databaseExist(dbName)) {
            if (ifNotExists) {
                LOG.info("create database[{}] which already exists", dbName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
        }

        if (!properties.isEmpty() && dorisCatalog instanceof PaimonExternalCatalog) {
            String catalogType = ((PaimonExternalCatalog) dorisCatalog).getCatalogType();
            validateDatabaseProperties(catalogType, properties);
        }

        catalog.createDatabase(dbName, ifNotExists, properties);
        return false;
    }

    private boolean supportsDatabaseProperties(String catalogType) {
        return PaimonExternalCatalog.PAIMON_HMS.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_JDBC.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_REST.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_DLF.equals(catalogType);
    }

    private boolean supportsDatabaseLocation(String catalogType) {
        return PaimonExternalCatalog.PAIMON_HMS.equals(catalogType)
                || PaimonExternalCatalog.PAIMON_DLF.equals(catalogType);
    }

    private void validateDatabaseProperties(String catalogType, Map<String, String> properties) throws DdlException {
        if (!supportsDatabaseProperties(catalogType)) {
            throw new DdlException(
                    "Not supported: create database with properties for paimon catalog type: " + catalogType);
        }
        if (properties.containsKey(PROP_LOCATION) && !supportsDatabaseLocation(catalogType)) {
            throw new DdlException("Not supported: database property 'location' for paimon catalog type: "
                    + catalogType + " because it does not determine the default table location");
        }
    }

    @Override
    public void afterCreateDb() {
        dorisCatalog.resetMetaCacheNames();
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        try {
            executionAuthenticator.execute(() -> {
                performDropDb(dbName, ifExists, force);
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                "Failed to drop database: " + dbName + ", error message is:" + e.getMessage(), e);
        }
    }

    private void performDropDb(String dbName, boolean ifExists, boolean force) throws DdlException {
        ExternalDatabase dorisDb = dorisCatalog.getDbNullable(dbName);
        if (dorisDb == null) {
            if (ifExists) {
                LOG.info("drop database[{}] which does not exist", dbName);
                // Database does not exist and IF EXISTS is specified; treat as no-op.
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                // ErrorReport.reportDdlException is expected to throw DdlException.
                return;
            }
        }

        if (force) {
            List<String> tableNames = listTableNames(dbName);
            if (!tableNames.isEmpty()) {
                LOG.info("drop database[{}] with force, drop all tables, num: {}", dbName, tableNames.size());
            }
            for (String tableName : tableNames) {
                performDropTable(dbName, tableName, true);
            }
        }

        try {
            catalog.dropDatabase(dbName, ifExists, force);
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException("database " + dbName + " does not exist!");
        } catch (DatabaseNotEmptyException e) {
            throw new RuntimeException("database " + dbName + " is not empty! please check!");
        }
    }

    @Override
    public void afterDropDb(String dbName) {
        dorisCatalog.unregisterDatabase(dbName);
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        try {
            return executionAuthenticator.execute(() -> performCreateTable(createTableInfo));
        } catch (Exception e) {
            throw new DdlException(
                "Failed to create table: " + createTableInfo.getTableName() + ", error message is:" + e.getMessage(),
                e);
        }
    }

    public boolean performCreateTable(CreateTableInfo createTableInfo) throws UserException {
        String dbName = createTableInfo.getDbName();
        ExternalDatabase<?> db = dorisCatalog.getDbNullable(dbName);
        if (db == null) {
            throw new UserException("Failed to get database: '" + dbName + "' in catalog: " + dorisCatalog.getName());
        }
        String tableName = createTableInfo.getTableName();
        // 1. first, check if table exist in remote
        if (tableExist(db.getRemoteName(), tableName)) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                // Existing-table success skips the normal post-create hook, so refresh names here.
                resetTableNameCache(dbName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }

        // 2. second, check if table exist in local.
        // This is because case sensibility issue, eg:
        // 1. lower_case_table_name = 1
        // 2. create table tbl1;
        // 3. create table TBL1;  TBL1 does not exist in remote because the remote system is case-sensitive.
        //    but because lower_case_table_name = 1, the table can not be created in Doris because it is conflict with
        //    tbl1
        ExternalTable dorisTable = db.getTableNullable(tableName);
        if (dorisTable != null) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                // Every successful no-op bypasses the normal post-create hook and must refresh names.
                resetTableNameCache(dbName);
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        List<Column> columns = createTableInfo.getColumnDefinitions().stream()
                .map(ColumnDefinition::translateToCatalogStyle)
                .collect(Collectors.toList());
        Schema schema = toPaimonSchema(columns, createTableInfo.getPartitionDesc(),
                createTableInfo.getProperties());
        try {
            // Let Paimon report a concurrent winner so callers can distinguish an existing table
            // from the table created by this statement before deciding whether rollback is owned.
            catalog.createTable(new Identifier(createTableInfo.getDbName(), createTableInfo.getTableName()),
                    schema, false);
        } catch (TableAlreadyExistException e) {
            if (createTableInfo.isIfNotExists()) {
                LOG.info("create table[{}] which already exists", tableName);
                // A concurrent remote creator also bypasses the normal post-create hook.
                resetTableNameCache(dbName);
                return true;
            }
            throw new RuntimeException(e);
        } catch (DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private Schema toPaimonSchema(List<Column> columns, PartitionDesc partitionDesc, Map<String, String> properties) {
        Map<String, String> normalizedProperties = new HashMap<>(properties);
        normalizedProperties.remove(PRIMARY_KEY_IDENTIFIER);
        normalizedProperties.remove(PROP_COMMENT);
        if (normalizedProperties.containsKey(PROP_LOCATION)) {
            String path = normalizedProperties.remove(PROP_LOCATION);
            normalizedProperties.put(CoreOptions.PATH.key(), path);
        }

        String pkAsString = properties.get(PRIMARY_KEY_IDENTIFIER);
        List<String> primaryKeys = pkAsString == null ? Collections.emptyList() : Arrays.stream(pkAsString.split(","))
                .map(String::trim)
                .collect(Collectors.toList());
        List<String> partitionKeys = partitionDesc == null ? new ArrayList<>() : partitionDesc.getPartitionColNames();
        List<String> rootFieldNames = columns.stream().map(Column::getName).collect(Collectors.toList());
        primaryKeys = getPaimonColumnNames(rootFieldNames, primaryKeys);
        partitionKeys = getPaimonColumnNames(rootFieldNames, partitionKeys);
        Schema.Builder schemaBuilder = Schema.newBuilder()
                .options(normalizedProperties)
                .primaryKey(primaryKeys)
                .partitionKeys(partitionKeys)
                .comment(properties.getOrDefault(PROP_COMMENT, null));
        for (Column column : columns) {
            schemaBuilder.column(column.getName(),
                    toPaimonType(column.getType()).copy(column.isAllowNull()),
                    column.getComment(),
                    column.getDefaultValue());
        }
        return schemaBuilder.build();
    }

    private List<String> getPaimonColumnNames(List<String> paimonColumnNames, List<String> dorisColumnNames) {
        Map<String, String> paimonColumnNameMap = paimonColumnNames.stream()
                .collect(Collectors.toMap(name -> name.toLowerCase(Locale.ROOT), name -> name));
        return dorisColumnNames.stream()
                .map(name -> paimonColumnNameMap.getOrDefault(name.toLowerCase(Locale.ROOT), name))
                .collect(Collectors.toList());
    }

    private DataType toPaimonType(Type type) {
        return DorisTypeVisitor.visit(type, new DorisToPaimonTypeVisitor());
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = resetTableNameCache(dbName);
        LOG.info("after create table {}.{}.{}, is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    private Optional<ExternalDatabase<?>> resetTableNameCache(String dbName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().resetMetaCacheNames();
        }
        return db;
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        try {
            executionAuthenticator.execute(() -> {
                performDropTable(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(), ifExists);
                return null;
            });
        } catch (Exception e) {
            throw new DdlException(
                "Failed to drop table: " + dorisTable.getName() + ", error message is:" + e.getMessage(), e);
        }
    }

    private void performDropTable(String dBName, String tableName, boolean ifExists) throws DdlException {
        if (!tableExist(dBName, tableName)) {
            if (ifExists) {
                LOG.info("drop table[{}] which does not exist", tableName);
                return;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dBName);
            }
        }
        try {
            catalog.dropTable(Identifier.create(dBName, tableName), ifExists);
        } catch (TableNotExistException e) {
            throw new RuntimeException("table " + tableName + " does not exist");
        }
    }

    @Override
    public void afterDropTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        db.ifPresent(externalDatabase -> externalDatabase.unregisterTable(tblName));
        LOG.info("after drop table {}.{}.{}. is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException {
        throw new UnsupportedOperationException("truncate table is not a supported operation!");
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {
        throw new UnsupportedOperationException("create or replace branch is not a supported operation!");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo) throws UserException {
        throw new UnsupportedOperationException("create or replace tag is not a supported operation!");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {
        throw new UnsupportedOperationException("drop tag is not a supported operation!");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {
        throw new UnsupportedOperationException("drop branch is not a supported operation!");
    }

    @Override
    public List<String> listDatabaseNames() {
        try {
            return executionAuthenticator.execute(() -> new ArrayList<>(catalog.listDatabases()));
        } catch (Exception e) {
            throw new RuntimeException("Failed to list databases names, catalog name: " + dorisCatalog.getName(), e);
        }
    }

    @Override
    public List<String> listTableNames(String db) {
        try {
            return executionAuthenticator.execute(() -> {
                List<String> tableNames = new ArrayList<>();
                try {
                    tableNames.addAll(catalog.listTables(db));
                } catch (DatabaseNotExistException e) {
                    LOG.warn("DatabaseNotExistException", e);
                }
                return tableNames;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to list table names, catalog name: " + dorisCatalog.getName(), e);
        }
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        try {
            return executionAuthenticator.execute(() -> {
                try {
                    catalog.getTable(Identifier.create(dbName, tblName));
                    return true;
                } catch (TableNotExistException e) {
                    return false;
                }
            });

        } catch (Exception e) {
            throw new RuntimeException("Failed to check table existence, catalog name: " + dorisCatalog.getName()
                + "error message is:" + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    @Override
    public boolean databaseExist(String dbName) {
        try {
            return executionAuthenticator.execute(() -> {
                try {
                    catalog.getDatabase(dbName);
                    return true;
                } catch (DatabaseNotExistException e) {
                    return false;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to check database exist, error message is:" + e.getMessage(), e);
        }
    }

    private Identifier tableIdentifier(ExternalTable dorisTable) {
        return Identifier.create(dorisTable.getRemoteDbName(), dorisTable.getRemoteName());
    }

    private List<DataField> loadRemoteFields(ExternalTable dorisTable) throws UserException {
        try {
            return executionAuthenticator.execute(
                    () -> new ArrayList<>(catalog.getTable(tableIdentifier(dorisTable)).rowType().getFields()));
        } catch (Exception e) {
            throw new UserException("Failed to load schema for Paimon table " + dorisTable.getName()
                    + ": " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private Map<String, DataField> indexFieldsByDorisName(List<DataField> fields) throws UserException {
        Map<String, DataField> fieldsByLowerCase = new HashMap<>();
        for (DataField field : fields) {
            DataField previous = fieldsByLowerCase.put(field.name().toLowerCase(Locale.ROOT), field);
            if (previous != null) {
                throw new UserException("Paimon table contains columns which differ only by case: "
                        + previous.name() + " and " + field.name());
            }
        }
        return fieldsByLowerCase;
    }

    private DataField resolveRemoteField(Map<String, DataField> fieldsByDorisName, String columnName)
            throws UserException {
        DataField field = fieldsByDorisName.get(columnName.toLowerCase(Locale.ROOT));
        if (field == null) {
            throw new UserException("Column " + columnName + " does not exist in Paimon table");
        }
        return field;
    }

    private DataType toPaimonColumnType(Column column) throws UserException {
        try {
            return toPaimonType(column.getType()).copy(column.isAllowNull());
        } catch (RuntimeException e) {
            throw new UserException("Unsupported Paimon type for column " + column.getName()
                    + ": " + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    private void registerDorisColumnName(Set<String> columnNames, String columnName) throws UserException {
        if (!columnNames.add(columnName.toLowerCase(Locale.ROOT))) {
            throw new UserException("Column " + columnName
                    + " conflicts with an existing Paimon column (case-insensitive)");
        }
    }

    private void checkUnsupportedColumnAttributes(Column column) throws UserException {
        if (column.isAggregated()) {
            throw new UserException("Paimon column does not support aggregation method: " + column.getName());
        }
        if (column.isAutoInc()) {
            throw new UserException("Paimon column does not support AUTO_INCREMENT: " + column.getName());
        }
        if (column.isGeneratedColumn()) {
            throw new UserException("Column " + column.getName()
                    + " cannot be a generated column in a Paimon table");
        }
    }

    private void appendAddColumnChanges(List<SchemaChange> changes, Column column, SchemaChange.Move move)
            throws UserException {
        changes.add(SchemaChange.addColumn(
                column.getName(), toPaimonColumnType(column), column.getComment(), move));
        if (column.getDefaultValue() != null) {
            changes.add(SchemaChange.updateColumnDefaultValue(
                    new String[] {column.getName()}, column.getDefaultValue()));
        }
    }

    private void alterTable(ExternalTable dorisTable, List<SchemaChange> changes, String operation,
            long updateTime) throws UserException {
        if (changes.isEmpty()) {
            return;
        }
        try {
            executionAuthenticator.execute(() -> {
                catalog.alterTable(tableIdentifier(dorisTable), changes, false);
                return null;
            });
        } catch (Exception e) {
            throw new UserException("Failed to " + operation + " for Paimon table " + dorisTable.getName()
                    + ": " + ExceptionUtils.getRootCauseMessage(e), e);
        }
        refreshTable(dorisTable, updateTime);
    }

    private void refreshTable(ExternalTable dorisTable, long updateTime) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dorisTable.getDbName());
        if (db.isPresent()) {
            Optional<?> table = db.get().getTableForReplay(dorisTable.getName());
            if (table.isPresent()) {
                Env.getCurrentEnv().getRefreshManager()
                        .refreshTableInternal(db.get(), (ExternalTable) table.get(), updateTime);
            }
        }
    }

    @Override
    public void updateTableProperties(ExternalTable dorisTable, Map<String, String> properties, long updateTime)
            throws UserException {
        try {
            properties.keySet().forEach(SchemaManager::checkAlterTablePath);
        } catch (UnsupportedOperationException e) {
            throw new UserException("Failed to set properties for Paimon table " + dorisTable.getName()
                    + ": " + ExceptionUtils.getRootCauseMessage(e), e);
        }

        List<SchemaChange> changes = new ArrayList<>(properties.size());
        properties.forEach((key, value) -> changes.add(SchemaChange.setOption(key, value)));
        alterTable(dorisTable, changes, "set properties", updateTime);
    }

    @Override
    public void addColumn(ExternalTable dorisTable, Column column, ColumnPosition position, long updateTime)
            throws UserException {
        List<DataField> fields = loadRemoteFields(dorisTable);
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(fields);
        registerDorisColumnName(new HashSet<>(fieldsByDorisName.keySet()), column.getName());
        checkUnsupportedColumnAttributes(column);

        SchemaChange.Move move = null;
        if (position != null) {
            move = position.isFirst()
                    ? SchemaChange.Move.first(column.getName())
                    : SchemaChange.Move.after(column.getName(),
                            resolveRemoteField(fieldsByDorisName, position.getLastCol()).name());
        }

        List<SchemaChange> changes = new ArrayList<>();
        appendAddColumnChanges(changes, column, move);
        alterTable(dorisTable, changes, "add column " + column.getName(), updateTime);
    }

    @Override
    public void addColumns(ExternalTable dorisTable, List<Column> columns, long updateTime) throws UserException {
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(loadRemoteFields(dorisTable));
        Set<String> columnNames = new HashSet<>(fieldsByDorisName.keySet());
        List<SchemaChange> changes = new ArrayList<>();
        for (Column column : columns) {
            registerDorisColumnName(columnNames, column.getName());
            checkUnsupportedColumnAttributes(column);
            appendAddColumnChanges(changes, column, null);
        }
        alterTable(dorisTable, changes, "add columns", updateTime);
    }

    @Override
    public void dropColumn(ExternalTable dorisTable, String columnName, long updateTime) throws UserException {
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(loadRemoteFields(dorisTable));
        String remoteColumnName = resolveRemoteField(fieldsByDorisName, columnName).name();
        alterTable(dorisTable, Collections.singletonList(SchemaChange.dropColumn(remoteColumnName)),
                "drop column " + remoteColumnName, updateTime);
    }

    @Override
    public void renameColumn(ExternalTable dorisTable, String oldName, String newName, long updateTime)
            throws UserException {
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(loadRemoteFields(dorisTable));
        DataField oldField = resolveRemoteField(fieldsByDorisName, oldName);
        DataField conflictingField = fieldsByDorisName.get(newName.toLowerCase(Locale.ROOT));
        if (conflictingField != null && conflictingField != oldField) {
            throw new UserException("Column " + newName
                    + " conflicts with an existing Paimon column (case-insensitive)");
        }
        if (oldField.name().equals(newName)) {
            return;
        }
        alterTable(dorisTable,
                Collections.singletonList(SchemaChange.renameColumn(oldField.name(), newName)),
                "rename column " + oldField.name() + " to " + newName, updateTime);
    }

    @Override
    public void modifyColumn(ExternalTable dorisTable, Column column, ColumnPosition position, long updateTime)
            throws UserException {
        checkUnsupportedColumnAttributes(column);
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(loadRemoteFields(dorisTable));
        DataField currentField = resolveRemoteField(fieldsByDorisName, column.getName());
        DataType requestedType = requestedColumnType(column, currentField);
        List<SchemaChange> changes = new ArrayList<>();

        DataType requestedTypeWithCurrentNullability =
                requestedType.copy(currentField.type().isNullable());
        if (!currentField.type().equalsIgnoreFieldId(requestedTypeWithCurrentNullability)) {
            changes.add(SchemaChange.updateColumnType(
                    currentField.name(), requestedTypeWithCurrentNullability, true));
        }
        if (currentField.type().isNullable() != requestedType.isNullable()) {
            changes.add(SchemaChange.updateColumnNullability(
                    currentField.name(), requestedType.isNullable()));
        }
        if (!Objects.equals(currentField.description(), column.getComment())) {
            changes.add(SchemaChange.updateColumnComment(currentField.name(), column.getComment()));
        }
        if (!Objects.equals(currentField.defaultValue(), column.getDefaultValue())) {
            changes.add(SchemaChange.updateColumnDefaultValue(
                    new String[] {currentField.name()}, column.getDefaultValue()));
        }
        if (position != null) {
            SchemaChange.Move move = position.isFirst()
                    ? SchemaChange.Move.first(currentField.name())
                    : SchemaChange.Move.after(currentField.name(),
                            resolveRemoteField(fieldsByDorisName, position.getLastCol()).name());
            changes.add(SchemaChange.updateColumnPosition(move));
        }

        alterTable(dorisTable, changes, "modify column " + currentField.name(), updateTime);
    }

    DataType requestedColumnType(Column column, DataField currentField)
            throws UserException {
        Type currentDorisType = PaimonUtil.paimonTypeToDorisType(
                currentField.type(),
                dorisCatalog.getEnableMappingVarbinary(),
                dorisCatalog.getEnableMappingTimestampTz());
        // Doris external-table types are a projection of the remote schema. The projection can
        // lose Paimon timestamp precision, binary/string length, LTZ identity and nested
        // nullability. If ALTER did not change that projected type, retain the exact remote type
        // and apply only the independently requested attributes below.
        return currentDorisType.equals(column.getType())
                ? currentField.type().copy(column.isAllowNull())
                : toPaimonColumnType(column);
    }

    @Override
    public void reorderColumns(ExternalTable dorisTable, List<String> newOrder, long updateTime)
            throws UserException {
        List<DataField> fields = loadRemoteFields(dorisTable);
        Map<String, DataField> fieldsByDorisName = indexFieldsByDorisName(fields);
        if (newOrder.size() != fields.size()) {
            throw new UserException("Reorder columns must contain every Paimon column exactly once");
        }

        List<String> remoteOrder = new ArrayList<>(newOrder.size());
        Set<String> seen = new HashSet<>();
        for (String columnName : newOrder) {
            DataField field = resolveRemoteField(fieldsByDorisName, columnName);
            if (!seen.add(field.name().toLowerCase(Locale.ROOT))) {
                throw new UserException("Duplicate column in reorder columns: " + columnName);
            }
            remoteOrder.add(field.name());
        }
        List<String> currentOrder = fields.stream().map(DataField::name).collect(Collectors.toList());
        if (currentOrder.equals(remoteOrder)) {
            return;
        }

        List<SchemaChange> changes = new ArrayList<>();
        changes.add(SchemaChange.updateColumnPosition(SchemaChange.Move.first(remoteOrder.get(0))));
        for (int i = 1; i < remoteOrder.size(); i++) {
            changes.add(SchemaChange.updateColumnPosition(
                    SchemaChange.Move.after(remoteOrder.get(i), remoteOrder.get(i - 1))));
        }
        alterTable(dorisTable, changes, "reorder columns", updateTime);
    }

    public Catalog getCatalog() {
        return catalog;
    }

    @Override
    public void close() {
        if (catalog != null) {
            catalog = null;
        }
    }
}
