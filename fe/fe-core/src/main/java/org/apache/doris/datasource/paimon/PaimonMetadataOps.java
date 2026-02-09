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

import org.apache.doris.analysis.PartitionDesc;
import org.apache.doris.catalog.StructField;
import org.apache.doris.catalog.StructType;
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
import org.apache.paimon.types.DataType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
            if (!PaimonExternalCatalog.PAIMON_HMS.equals(catalogType)) {
                throw new DdlException(
                    "Not supported: create database with properties for paimon catalog type: " + catalogType);
            }
        }

        catalog.createDatabase(dbName, ifNotExists, properties);
        return false;
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
                return true;
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
            }
        }
        List<ColumnDefinition> columns = createTableInfo.getColumnDefinitions();
        List<StructField> collect = columns.stream()
                .map(col -> new StructField(col.getName(), col.getType().toCatalogDataType(),
                    col.getComment(), col.isNullable()))
                .collect(Collectors.toList());
        StructType structType = new StructType(new ArrayList<>(collect));
        Schema schema = toPaimonSchema(structType, createTableInfo.getPartitionDesc(), createTableInfo.getProperties());
        try {
            catalog.createTable(new Identifier(createTableInfo.getDbName(), createTableInfo.getTableName()),
                    schema, createTableInfo.isIfNotExists());
        } catch (TableAlreadyExistException | DatabaseNotExistException e) {
            throw new RuntimeException(e);
        }
        return false;
    }

    private Schema toPaimonSchema(StructType structType, PartitionDesc partitionDesc, Map<String, String> properties) {
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
        Schema.Builder schemaBuilder = Schema.newBuilder()
                .options(normalizedProperties)
                .primaryKey(primaryKeys)
                .partitionKeys(partitionKeys)
                .comment(properties.getOrDefault(PROP_COMMENT, null));
        for (StructField field : structType.getFields()) {
            schemaBuilder.column(field.getName(),
                    toPaimontype(field.getType()).copy(field.getContainsNull()),
                    field.getComment());
        }
        return schemaBuilder.build();
    }

    private DataType toPaimontype(Type type) {
        return DorisTypeVisitor.visit(type, new DorisToPaimonTypeVisitor());
    }

    @Override
    public void afterCreateTable(String dbName, String tblName) {
        Optional<ExternalDatabase<?>> db = dorisCatalog.getDbForReplay(dbName);
        if (db.isPresent()) {
            db.get().resetMetaCacheNames();
        }
        LOG.info("after create table {}.{}.{}, is db exists: {}",
                dorisCatalog.getName(), dbName, tblName, db.isPresent());
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
