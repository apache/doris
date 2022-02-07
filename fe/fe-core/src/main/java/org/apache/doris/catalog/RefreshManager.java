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

import org.apache.doris.analysis.CreateDbStmt;
import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.analysis.DropDbStmt;
import org.apache.doris.analysis.DropTableStmt;
import org.apache.doris.analysis.RefreshDbStmt;
import org.apache.doris.analysis.RefreshTableStmt;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

// Manager for refresh database and table action
public class RefreshManager {
    private static final Logger LOG = LogManager.getLogger(RefreshManager.class);

    public void handleRefreshTable(RefreshTableStmt stmt) throws UserException {
        String dbName = stmt.getDbName();
        String tableName = stmt.getTblName();
        Catalog catalog = Catalog.getCurrentCatalog();

        // 0. check table type
        Database db = catalog.getDbOrDdlException(dbName);
        Table table = db.getTableNullable(tableName);
        if (!(table instanceof IcebergTable)) {
            throw new DdlException("Only support refresh Iceberg table.");
        }

        // 1. get iceberg properties
        Map<String, String> icebergProperties = ((IcebergTable) table).getIcebergProperties();
        icebergProperties.put(IcebergProperty.ICEBERG_TABLE, ((IcebergTable) table).getIcebergTbl());
        icebergProperties.put(IcebergProperty.ICEBERG_DATABASE, ((IcebergTable) table).getIcebergDb());

        // 2. drop old table
        DropTableStmt dropTableStmt = new DropTableStmt(true, stmt.getTableName(), true);
        catalog.dropTable(dropTableStmt);

        // 3. create new table
        CreateTableStmt createTableStmt = new CreateTableStmt(true, true,
                stmt.getTableName(), "ICEBERG", icebergProperties, "");
        catalog.createTable(createTableStmt);

        LOG.info("Successfully refresh table: {} from db: {}", tableName, dbName);
    }

    public void handleRefreshDb(RefreshDbStmt stmt) throws DdlException {
        String dbName = stmt.getDbName();
        Catalog catalog = Catalog.getCurrentCatalog();

        Database db = catalog.getDbOrDdlException(dbName);
        // 0. get db properties
        Map<String, String> dbProperties = db.getDbProperties().getProperties();
        // build iceberg properties
        db.getDbProperties().addAndBuildProperties(dbProperties);
        // 1. check database type
        if (!db.getDbProperties().getIcebergProperty().isExist()) {
            throw new DdlException("Only support refresh Iceberg database.");
        }

        // 2. drop database
        DropDbStmt dropDbStmt = new DropDbStmt(true, dbName, true);
        catalog.dropDb(dropDbStmt);

        // 3. create database
        CreateDbStmt createDbStmt = new CreateDbStmt(true, dbName, dbProperties);
        createDbStmt.setClusterName(db.getClusterName());
        catalog.createDb(createDbStmt);

        LOG.info("Successfully refresh db: {}", dbName);
    }
}
