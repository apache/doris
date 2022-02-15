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
                DropTableStmt dropTableStmt = new DropTableStmt(true, new TableName(dbName, table.getName()), true);
                catalog.dropTable(dropTableStmt);
            }
        }

        // 3. register iceberg database to recreate iceberg table
        catalog.getIcebergTableCreationRecordMgr().registerDb(db);

        LOG.info("Successfully refresh db: {}", dbName);
    }
}
