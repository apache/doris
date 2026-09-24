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

package org.apache.doris.datasource.lance;

import org.apache.doris.analysis.ColumnPosition;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.lance.metadata.LanceTypeConverter;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.lance.namespace.errors.NamespaceAlreadyExistsException;
import org.lance.namespace.errors.NamespaceNotFoundException;
import org.lance.namespace.errors.TableAlreadyExistsException;
import org.lance.namespace.errors.TableNotFoundException;
import org.lance.namespace.model.AddColumnsEntry;
import org.lance.namespace.model.AlterColumnsEntry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/** Doris external metadata operations backed by the Lance Namespace API. */
public class LanceMetadataOps implements ExternalMetadataOps {
    private static final Logger LOG = LogManager.getLogger(LanceMetadataOps.class);
    private static final String TABLE_COMMENT_PROPERTY = "comment";

    private final LanceExternalCatalog catalog;

    public LanceMetadataOps(LanceExternalCatalog catalog) {
        this.catalog = catalog;
    }

    @Override
    public boolean createDbImpl(String dbName, boolean ifNotExists, Map<String, String> properties)
            throws DdlException {
        return execute("Failed to create Lance database " + dbName, client -> {
            if (client.databaseExists(dbName)) {
                if (ifNotExists) {
                    catalog.resetMetaCacheNames();
                    return true;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
            }
            try {
                client.createDatabase(dbName, new HashMap<>(
                        Optional.ofNullable(properties).orElse(Collections.emptyMap())));
                return false;
            } catch (NamespaceAlreadyExistsException e) {
                if (ifNotExists) {
                    catalog.resetMetaCacheNames();
                    return true;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_CREATE_EXISTS, dbName);
                throw new IllegalStateException("unreachable");
            }
        });
    }

    @Override
    public void afterCreateDb() {
        catalog.resetMetaCacheNames();
    }

    @Override
    public void dropDbImpl(String dbName, boolean ifExists, boolean force) throws DdlException {
        execute("Failed to drop Lance database " + dbName, client -> {
            if (client.isRootDatabase(dbName)) {
                throw new DdlException("Cannot drop the configured Lance root database: " + dbName);
            }
            if (!client.databaseExists(dbName)) {
                if (ifExists) {
                    return null;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
            }
            try {
                client.dropDatabase(dbName, ifExists, force);
            } catch (NamespaceNotFoundException e) {
                if (!ifExists) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
                }
            }
            return null;
        });
    }

    @Override
    public void afterDropDb(String dbName) {
        catalog.unregisterDatabase(dbName);
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        String dbName = createTableInfo.getDbName();
        String tableName = createTableInfo.getTableName();
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName
                    + "' in catalog: " + catalog.getName());
        }
        List<Column> columns = createTableInfo.getColumns();
        validateCreateColumns(columns);
        Schema schema = LanceTypeConverter.toArrowSchema(columns);
        Map<String, String> properties = new HashMap<>(
                Optional.ofNullable(createTableInfo.getProperties()).orElse(Collections.emptyMap()));
        if (StringUtils.isNotBlank(createTableInfo.getComment())) {
            properties.put(TABLE_COMMENT_PROPERTY, createTableInfo.getComment());
        }

        return execute("Failed to create Lance table " + dbName + "." + tableName, client -> {
            if (client.tableExists(db.getRemoteName(), tableName)) {
                if (createTableInfo.isIfNotExists()) {
                    resetTableNameCache(dbName);
                    return true;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
            if (db.getTableNullable(tableName) != null) {
                resetTableNameCache(dbName);
                if (db.getTableNullable(tableName) != null) {
                    if (createTableInfo.isIfNotExists()) {
                        return true;
                    }
                    ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                }
            }
            try {
                client.createTable(db.getRemoteName(), tableName, schema, properties);
                return false;
            } catch (TableAlreadyExistsException e) {
                if (createTableInfo.isIfNotExists()) {
                    resetTableNameCache(dbName);
                    return true;
                }
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
                throw new IllegalStateException("unreachable");
            }
        });
    }

    private static void validateCreateColumns(List<Column> columns) throws UserException {
        for (Column column : columns) {
            if (column.isAggregated()) {
                throw new UserException("Lance columns do not support aggregation: " + column.getName());
            }
            if (column.isAutoInc()) {
                throw new UserException("Lance columns do not support AUTO_INCREMENT: " + column.getName());
            }
            if (column.isGeneratedColumn()) {
                throw new UserException("Lance columns do not support generated columns: " + column.getName());
            }
            if (column.getDefaultValue() != null) {
                throw new UserException("Lance table creation does not support column defaults: "
                        + column.getName());
            }
        }
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        catalog.invalidateTableAccessCache();
        resetTableNameCache(dbName);
    }

    @Override
    public void dropTableImpl(ExternalTable dorisTable, boolean ifExists) throws DdlException {
        String dbName = dorisTable.getRemoteDbName();
        String tableName = dorisTable.getRemoteName();
        execute("Failed to drop Lance table " + dbName + "." + tableName, client -> {
            try {
                client.dropTable(dbName, tableName);
            } catch (TableNotFoundException e) {
                if (!ifExists) {
                    ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, tableName, dbName);
                }
            }
            return null;
        });
    }

    @Override
    public void afterDropTable(String dbName, String tblName) {
        catalog.invalidateTableAccessCache();
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
        db.ifPresent(externalDatabase -> externalDatabase.unregisterTable(tblName));
    }

    @Override
    public void renameTableImpl(String dbName, String oldName, String newName) throws DdlException {
        ExternalDatabase<?> db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Failed to get database: '" + dbName
                    + "' in catalog: " + catalog.getName());
        }
        ExternalTable oldTable = db.getTableNullable(oldName);
        if (oldTable == null) {
            ErrorReport.reportDdlException(ErrorCode.ERR_UNKNOWN_TABLE, oldName, dbName);
            throw new IllegalStateException("unreachable");
        }
        String remoteOldName = oldTable.getRemoteName();
        execute("Failed to rename Lance table " + dbName + "." + oldName + " to " + newName,
                client -> {
                    client.renameTable(db.getRemoteName(), remoteOldName, newName);
                    return null;
                });
    }

    @Override
    public void afterRenameTable(String dbName, String oldName, String newName) {
        catalog.invalidateTableAccessCache();
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().unregisterTable(oldName);
            db.get().resetMetaCacheNames();
        }
    }

    @Override
    public void addColumn(ExternalTable dorisTable, Column column, ColumnPosition position, long updateTime)
            throws UserException {
        validateAddColumn(column, position);
        ensureNewColumnNames(dorisTable, Collections.singletonList(column));
        AddColumnsEntry entry = new AddColumnsEntry()
                .name(column.getName())
                .expression(LanceTypeConverter.toAddColumnExpression(column.getType()));
        execute("Failed to add column " + column.getName() + " to Lance table "
                        + tableName(dorisTable),
                client -> {
                    client.addColumns(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(),
                            Collections.singletonList(entry));
                    return null;
                });
        refreshTable(dorisTable, updateTime);
    }

    @Override
    public void addColumns(ExternalTable dorisTable, List<Column> columns, long updateTime)
            throws UserException {
        if (columns.isEmpty()) {
            return;
        }
        List<AddColumnsEntry> entries = new ArrayList<>(columns.size());
        for (Column column : columns) {
            validateAddColumn(column, null);
            entries.add(new AddColumnsEntry()
                    .name(column.getName())
                    .expression(LanceTypeConverter.toAddColumnExpression(column.getType())));
        }
        ensureNewColumnNames(dorisTable, columns);
        execute("Failed to add columns to Lance table " + tableName(dorisTable), client -> {
            client.addColumns(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(), entries);
            return null;
        });
        refreshTable(dorisTable, updateTime);
    }

    @Override
    public void dropColumn(ExternalTable dorisTable, String columnName, long updateTime)
            throws UserException {
        Column currentColumn = requireColumn(dorisTable, columnName);
        execute("Failed to drop column " + currentColumn.getName() + " from Lance table "
                        + tableName(dorisTable),
                client -> {
                    client.dropColumns(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(),
                            Collections.singletonList(currentColumn.getName()));
                    return null;
                });
        refreshTable(dorisTable, updateTime);
    }

    @Override
    public void renameColumn(ExternalTable dorisTable, String oldName, String newName, long updateTime)
            throws UserException {
        Column currentColumn = requireColumn(dorisTable, oldName);
        Column conflictingColumn = dorisTable.getColumn(newName);
        if (conflictingColumn != null && conflictingColumn != currentColumn) {
            throw new UserException("Column " + newName
                    + " conflicts with an existing Lance column (case-insensitive)");
        }
        if (currentColumn.getName().equals(newName)) {
            return;
        }
        AlterColumnsEntry alteration = new AlterColumnsEntry()
                .path(currentColumn.getName())
                .rename(newName);
        execute("Failed to rename column " + currentColumn.getName() + " to " + newName
                        + " in Lance table " + tableName(dorisTable),
                client -> {
                    client.alterColumns(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(),
                            Collections.singletonList(alteration));
                    return null;
                });
        refreshTable(dorisTable, updateTime);
    }

    @Override
    public void modifyColumn(ExternalTable dorisTable, Column column, ColumnPosition position, long updateTime)
            throws UserException {
        validateModifyColumn(column, position);
        Column currentColumn = requireColumn(dorisTable, column.getName());
        AlterColumnsEntry alteration = new AlterColumnsEntry().path(currentColumn.getName());
        boolean changed = false;
        if (!currentColumn.getType().equals(column.getType())) {
            alteration.dataType(LanceTypeConverter.toAlterColumnType(column.getType()));
            changed = true;
        }
        if (column.isNullableSpecified()
                && currentColumn.isAllowNull() != column.isAllowNull()) {
            alteration.nullable(column.isAllowNull());
            changed = true;
        }
        if (!changed) {
            return;
        }

        execute("Failed to modify column " + currentColumn.getName() + " in Lance table "
                        + tableName(dorisTable),
                client -> {
                    client.alterColumns(dorisTable.getRemoteDbName(), dorisTable.getRemoteName(),
                            Collections.singletonList(alteration));
                    return null;
                });
        refreshTable(dorisTable, updateTime);
    }

    static void validateAddColumn(Column column, ColumnPosition position) throws UserException {
        validateColumnAttributes(column, "ADD COLUMN");
        if (column.hasDefaultValue() || column.hasOnUpdateDefaultValue()) {
            throw new UserException("Lance ADD COLUMN does not support default values");
        }
        if (!column.isAllowNull()) {
            throw new UserException("Lance ADD COLUMN only supports nullable columns");
        }
        if (column.isCommentSpecified()) {
            throw new UserException("Lance ADD COLUMN does not support column comments");
        }
        if (position != null) {
            throw new UserException("Lance ADD COLUMN does not support column positions");
        }
    }

    static void validateModifyColumn(Column column, ColumnPosition position) throws UserException {
        validateColumnAttributes(column, "MODIFY COLUMN");
        if (column.hasDefaultValue() || column.hasOnUpdateDefaultValue()) {
            throw new UserException("Lance MODIFY COLUMN does not support default values");
        }
        if (column.isCommentSpecified()) {
            throw new UserException("Lance MODIFY COLUMN does not support column comments");
        }
        if (position != null) {
            throw new UserException("Lance MODIFY COLUMN does not support column positions");
        }
    }

    private static void validateColumnAttributes(Column column, String operation) throws UserException {
        if (column.isKey()) {
            throw new UserException("Lance " + operation + " does not support key columns");
        }
        if (column.isAggregated()) {
            throw new UserException("Lance " + operation + " does not support aggregation: "
                    + column.getName());
        }
        if (column.isAutoInc()) {
            throw new UserException("Lance " + operation + " does not support AUTO_INCREMENT: "
                    + column.getName());
        }
        if (column.isGeneratedColumn()) {
            throw new UserException("Lance " + operation + " does not support generated columns: "
                    + column.getName());
        }
    }

    private static void ensureNewColumnNames(ExternalTable dorisTable, List<Column> columns)
            throws UserException {
        Set<String> names = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (Column column : dorisTable.getFullSchema()) {
            names.add(column.getName());
        }
        for (Column column : columns) {
            if (!names.add(column.getName())) {
                throw new UserException("Column " + column.getName()
                        + " conflicts with an existing Lance column (case-insensitive)");
            }
        }
    }

    private static Column requireColumn(ExternalTable dorisTable, String columnName)
            throws UserException {
        Column column = dorisTable.getColumn(columnName);
        if (column == null) {
            throw new UserException("Column " + columnName + " does not exist in Lance table "
                    + tableName(dorisTable));
        }
        return column;
    }

    private static String tableName(ExternalTable dorisTable) {
        return dorisTable.getRemoteDbName() + "." + dorisTable.getRemoteName();
    }

    private void refreshTable(ExternalTable dorisTable, long updateTime) {
        catalog.invalidateTableAccessCache();
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dorisTable.getDbName());
        if (db.isPresent()) {
            Optional<?> table = db.get().getTableForReplay(dorisTable.getName());
            if (table.isPresent()) {
                Env.getCurrentEnv().getRefreshManager()
                        .refreshTableInternal(db.get(), (ExternalTable) table.get(), updateTime);
            }
        }
    }

    private Optional<ExternalDatabase<?>> resetTableNameCache(String dbName) {
        Optional<ExternalDatabase<?>> db = catalog.getDbForReplay(dbName);
        db.ifPresent(ExternalDatabase::resetMetaCacheNames);
        return db;
    }

    @Override
    public List<String> listDatabaseNames() {
        return executeUnchecked("Failed to list Lance databases", LanceCatalogClient::listDatabaseNames);
    }

    @Override
    public List<String> listTableNames(String db) {
        return executeUnchecked("Failed to list Lance tables in " + db,
                client -> client.listTableNames(db));
    }

    @Override
    public boolean tableExist(String dbName, String tblName) {
        return executeUnchecked("Failed to check Lance table " + dbName + "." + tblName,
                client -> client.tableExists(dbName, tblName));
    }

    @Override
    public boolean databaseExist(String dbName) {
        return executeUnchecked("Failed to check Lance database " + dbName,
                client -> client.databaseExists(dbName));
    }

    @Override
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) {
        throw new UnsupportedOperationException("TRUNCATE TABLE is not supported for Lance tables");
    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo) {
        throw new UnsupportedOperationException("Branches are not supported for Lance tables");
    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo) {
        throw new UnsupportedOperationException("Tags are not supported for Lance tables");
    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) {
        throw new UnsupportedOperationException("Tags are not supported for Lance tables");
    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) {
        throw new UnsupportedOperationException("Branches are not supported for Lance tables");
    }

    @Override
    public void close() {
        // LanceExternalCatalog owns the leased client generations and their native resources.
    }

    private <T> T execute(String action, ClientOperation<T> operation) throws DdlException {
        try (LanceCatalogClient.Lease lease = catalog.acquireClient()) {
            try {
                return operation.execute(lease.client());
            } catch (DdlException e) {
                throw e;
            } catch (RuntimeException e) {
                throw lease.client().ddlFailure(action, e);
            }
        }
    }

    private <T> T executeUnchecked(String action, ClientOperation<T> operation) {
        try {
            return execute(action, operation);
        } catch (DdlException e) {
            LOG.warn(action);
            throw new RuntimeException(e);
        }
    }

    @FunctionalInterface
    private interface ClientOperation<T> {
        T execute(LanceCatalogClient client) throws DdlException;
    }
}
