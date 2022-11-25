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

package org.apache.doris.catalog;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.analysis.TableName;
import org.apache.doris.catalog.external.ExternalDatabase;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalCatalog;
import org.apache.doris.datasource.ExternalObjectLog;
import org.apache.doris.datasource.InternalCatalog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// Manager for refresh database and table action
public class RefreshManager {
    private static final Logger LOG = LogManager.getLogger(RefreshManager.class);

    public void handleRefreshTable(RefreshTableStmt stmt) throws UserException {
        String catalogName = stmt.getCtl();
        String dbName = stmt.getDbName();
        String tableName = stmt.getTblName();
        Env env = Env.getCurrentEnv();

        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();

        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }

        if (catalog.getName().equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            // Process internal catalog iceberg external table refresh.
            refreshInternalCtlIcebergTable(stmt, env);
        } else {
            // Process external catalog table refresh
            refreshExternalCtlTable(dbName, tableName, catalog);
        }
        LOG.info("Successfully refresh table: {} from db: {}", tableName, dbName);
    }

    public void handleRefreshDb(RefreshDbStmt stmt) throws DdlException {
        String catalogName = stmt.getCatalogName();
        String dbName = stmt.getDbName();
        Env env = Env.getCurrentEnv();
        CatalogIf catalog = catalogName != null ? env.getCatalogMgr().getCatalog(catalogName) : env.getCurrentCatalog();

        if (catalog == null) {
            throw new DdlException("Catalog " + catalogName + " doesn't exist.");
        }

        if (catalog.getName().equals(InternalCatalog.INTERNAL_CATALOG_NAME)) {
            // Process internal catalog iceberg external db refresh.
            refreshInternalCtlIcebergDb(dbName, env);
        } else {
            // Process external catalog db refresh
            refreshExternalCtlDb(dbName, catalog);
        }
        LOG.info("Successfully refresh db: {}", dbName);
    }

    private void refreshInternalCtlIcebergDb(String dbName, Env env) throws DdlException {
        Database db = env.getInternalCatalog().getDbOrDdlException(dbName);

        // 0. build iceberg property
        // Since we have only persisted database properties with key-value format in DatabaseProperty,
        // we build IcebergProperty here, before checking database type.
        db.getDbProperties().checkAndBuildProperties();
        // 1. check database type
        if (!db.getDbProperties().getIcebergProperty().isExist()) {
            throw new DdlException("Only support refresh Iceberg database.");
        }

        // 2. only drop iceberg table in the database
        // Current database may have other types of table, which is not allowed to drop.
        for (Table table : db.getTables()) {
            if (table instanceof IcebergTable) {
                DropTableStmt dropTableStmt =
                        new DropTableStmt(true, new TableName(null, dbName, table.getName()), true);
                env.dropTable(dropTableStmt);
            }
        }

        // 3. register iceberg database to recreate iceberg table
        env.getIcebergTableCreationRecordMgr().registerDb(db);
    }

    private void refreshExternalCtlDb(String dbName, CatalogIf catalog) throws DdlException {
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Database");
        }

        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
        }
        ((ExternalDatabase) db).setUnInitialized();
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        Env.getCurrentEnv().getEditLog().logRefreshExternalDb(log);
    }

    private void refreshInternalCtlIcebergTable(RefreshTableStmt stmt, Env env) throws UserException {
        // 0. check table type
        Database db = env.getInternalCatalog().getDbOrDdlException(stmt.getDbName());
        Table table = db.getTableNullable(stmt.getTblName());
        if (!(table instanceof IcebergTable)) {
            throw new DdlException("Only support refresh Iceberg table.");
        }

        // 1. get iceberg properties
        Map<String, String> icebergProperties = ((IcebergTable) table).getIcebergProperties();
        icebergProperties.put(IcebergProperty.ICEBERG_TABLE, ((IcebergTable) table).getIcebergTbl());
        icebergProperties.put(IcebergProperty.ICEBERG_DATABASE, ((IcebergTable) table).getIcebergDb());

        // 2. drop old table
        DropTableStmt dropTableStmt = new DropTableStmt(true, stmt.getTableName(), true);
        env.dropTable(dropTableStmt);

        // 3. create new table
        CreateTableStmt createTableStmt = new CreateTableStmt(true, true,
                stmt.getTableName(), "ICEBERG", icebergProperties, "");
        env.createTable(createTableStmt);
    }

    private void refreshExternalCtlTable(String dbName, String tableName, CatalogIf catalog) throws DdlException {
        if (!(catalog instanceof ExternalCatalog)) {
            throw new DdlException("Only support refresh ExternalCatalog Tables");
        }
        DatabaseIf db = catalog.getDbNullable(dbName);
        if (db == null) {
            throw new DdlException("Database " + dbName + " does not exist in catalog " + catalog.getName());
        }

        TableIf table = db.getTableNullable(tableName);
        if (table == null) {
            throw new DdlException("Table " + tableName + " does not exist in db " + dbName);
        }
        Env.getCurrentEnv().getExtMetaCacheMgr().invalidateTableCache(catalog.getId(), dbName, tableName);
        ExternalObjectLog log = new ExternalObjectLog();
        log.setCatalogId(catalog.getId());
        log.setDbId(db.getId());
        log.setTableId(table.getId());
        Env.getCurrentEnv().getEditLog().logRefreshExternalTable(log);
    }
}
