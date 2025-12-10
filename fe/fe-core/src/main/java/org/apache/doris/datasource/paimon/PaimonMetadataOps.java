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

import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.security.authentication.ExecutionAuthenticator;
import org.apache.doris.common.util.Util;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalDatabase;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.datasource.operations.ExternalMetadataOps;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateOrReplaceTagInfo;
import org.apache.doris.nereids.trees.plans.commands.info.CreateTableInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropBranchInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DropTagInfo;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Catalog.DatabaseNotEmptyException;
import org.apache.paimon.catalog.Catalog.DatabaseNotExistException;
import org.apache.paimon.catalog.Catalog.TableNotExistException;
import org.apache.paimon.catalog.Identifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PaimonMetadataOps implements ExternalMetadataOps {

    private static final Logger LOG = LogManager.getLogger(PaimonMetadataOps.class);
    protected Catalog catalog;
    protected ExternalCatalog dorisCatalog;
    private ExecutionAuthenticator executionAuthenticator;

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
            } else {
                ErrorReport.reportDdlException(ErrorCode.ERR_DB_DROP_EXISTS, dbName);
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
            throw new RuntimeException("database " + dbName + " does not empty! please check!");
        }
    }

    @Override
    public void afterDropDb(String dbName) {
        dorisCatalog.unregisterDatabase(dbName);
    }

    @Override
    public boolean createTableImpl(CreateTableInfo createTableInfo) throws UserException {
        return false;
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
    public void truncateTableImpl(ExternalTable dorisTable, List<String> partitions) throws DdlException {

    }

    @Override
    public void createOrReplaceBranchImpl(ExternalTable dorisTable, CreateOrReplaceBranchInfo branchInfo)
            throws UserException {

    }

    @Override
    public void createOrReplaceTagImpl(ExternalTable dorisTable, CreateOrReplaceTagInfo tagInfo) throws UserException {

    }

    @Override
    public void dropTagImpl(ExternalTable dorisTable, DropTagInfo tagInfo) throws UserException {

    }

    @Override
    public void dropBranchImpl(ExternalTable dorisTable, DropBranchInfo branchInfo) throws UserException {

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

    @Override
    public void close() {

    }
}
