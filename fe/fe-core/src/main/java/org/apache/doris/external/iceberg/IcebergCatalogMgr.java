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

package org.apache.doris.external.iceberg;

import org.apache.doris.analysis.CreateTableStmt;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.IcebergProperty;
import org.apache.doris.catalog.IcebergTable;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.external.iceberg.util.IcebergUtils;

import com.google.common.base.Enums;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.doris.catalog.IcebergProperty.ICEBERG_CATALOG_TYPE;
import static org.apache.doris.catalog.IcebergProperty.ICEBERG_DATABASE;
import static org.apache.doris.catalog.IcebergProperty.ICEBERG_HIVE_METASTORE_URIS;
import static org.apache.doris.catalog.IcebergProperty.ICEBERG_TABLE;
import static org.apache.doris.common.SystemIdGenerator.getNextId;

/**
 * Iceberg catalog manager
 */
public class IcebergCatalogMgr {
    private static final Logger LOG = LogManager.getLogger(IcebergCatalogMgr.class);

    private static final String PROPERTY_MISSING_MSG = "Iceberg %s is null. " +
            "Please add properties('%s'='xxx') when create iceberg database.";

    // hive metastore uri -> iceberg catalog
    // used to cache iceberg catalogs
    private static final ConcurrentHashMap<String, IcebergCatalog> metastoreUriToCatalog = new ConcurrentHashMap();

    // TODO:(qjl) We'll support more types of Iceberg catalog.
    public enum CatalogType {
        HIVE_CATALOG
    }

    public static IcebergCatalog getCatalog(IcebergProperty icebergProperty) throws DdlException {
        String uri = icebergProperty.getHiveMetastoreUris();
        if (!metastoreUriToCatalog.containsKey(uri)) {
            metastoreUriToCatalog.put(uri, createCatalog(icebergProperty));
        }
        return metastoreUriToCatalog.get(uri);
    }

    private static IcebergCatalog createCatalog(IcebergProperty icebergProperty) throws DdlException {
        CatalogType type = CatalogType.valueOf(icebergProperty.getCatalogType());
        IcebergCatalog catalog;
        switch (type) {
            case HIVE_CATALOG:
                catalog = new HiveCatalog();
                break;
            default:
                throw new DdlException("Unsupported catalog type: " + type);
        }
        catalog.initialize(icebergProperty);
        return catalog;
    }

    public static void validateProperties(Map<String, String> properties, boolean isTable) throws DdlException {
        if (properties.size() == 0) {
            throw new DdlException("Please set properties of iceberg, "
                    + "they are: iceberg.database and 'iceberg.hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        String icebergDb = copiedProps.get(ICEBERG_DATABASE);
        if (Strings.isNullOrEmpty(icebergDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, ICEBERG_DATABASE, ICEBERG_DATABASE));
        }
        copiedProps.remove(ICEBERG_DATABASE);

        // check hive properties
        // hive.metastore.uris
        String hiveMetastoreUris = copiedProps.get(ICEBERG_HIVE_METASTORE_URIS);
        if (Strings.isNullOrEmpty(hiveMetastoreUris)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, ICEBERG_HIVE_METASTORE_URIS, ICEBERG_HIVE_METASTORE_URIS));
        }
        copiedProps.remove(ICEBERG_HIVE_METASTORE_URIS);

        // check iceberg catalog type
        String icebergCatalogType = copiedProps.get(ICEBERG_CATALOG_TYPE);
        if (Strings.isNullOrEmpty(icebergCatalogType)) {
            icebergCatalogType = IcebergCatalogMgr.CatalogType.HIVE_CATALOG.name();
            properties.put(ICEBERG_CATALOG_TYPE, icebergCatalogType);
        } else {
            copiedProps.remove(ICEBERG_CATALOG_TYPE);
        }

        if (!Enums.getIfPresent(IcebergCatalogMgr.CatalogType.class, icebergCatalogType).isPresent()) {
            throw new DdlException("Unknown catalog type: " + icebergCatalogType + ". Current only support HiveCatalog.");
        }

        // only check table property when it's an iceberg table
        if (isTable) {
            String icebergTbl = copiedProps.get(ICEBERG_TABLE);
            if (Strings.isNullOrEmpty(icebergTbl)) {
                throw new DdlException(String.format(PROPERTY_MISSING_MSG, ICEBERG_TABLE, ICEBERG_TABLE));
            }
            copiedProps.remove(ICEBERG_TABLE);
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }

    /**
     * Get Doris IcebergTable from remote Iceberg by database and table
     * @param tableId table id in Doris
     * @param tableName table name in Doris
     * @param icebergProperty Iceberg property
     * @param identifier Iceberg table identifier
     * @param isTable
     * @return IcebergTable in Doris
     * @throws DdlException
     */
    public static IcebergTable getTableFromIceberg(long tableId, String tableName, IcebergProperty icebergProperty,
                                            TableIdentifier identifier,
                                            boolean isTable) throws DdlException {
        IcebergCatalog icebergCatalog = IcebergCatalogMgr.getCatalog(icebergProperty);

        if (isTable && !icebergCatalog.tableExists(identifier)) {
            throw new DdlException(String.format("Table [%s] dose not exist in Iceberg.", identifier.toString()));
        }

        // get iceberg table schema
        org.apache.iceberg.Table icebergTable = icebergCatalog.loadTable(identifier);

        // covert iceberg table schema to Doris's
        List<Column> columns = IcebergUtils.createSchemaFromIcebergSchema(icebergTable.schema());

        // create new iceberg table in doris
        IcebergTable table = new IcebergTable(tableId, tableName, columns, icebergProperty, icebergTable);

        return table;

    }

    /**
     * create iceberg table in Doris
     *
     * 1. check table existence in Iceberg
     * 2. get table schema from Iceberg
     * 3. convert Iceberg table schema to Doris table schema
     * 4. create associate table in Doris
     *
     * @param db
     * @param stmt
     * @throws DdlException
     */
    public static void createIcebergTable(Database db, CreateTableStmt stmt) throws DdlException {
        String tableName = stmt.getTableName();
        Map<String, String> properties = stmt.getProperties();

        // validate iceberg table properties
        validateProperties(properties, true);
        IcebergProperty icebergProperty = new IcebergProperty(properties);

        String icebergDb = icebergProperty.getDatabase();
        String icebergTbl = icebergProperty.getTable();

        // create iceberg table struct
        // 1. Already set column def in Create Stmt, just create table
        // 2. No column def in Create Stmt, get it from remote Iceberg schema.
        IcebergTable table;
        long tableId = getNextId();
        if (stmt.getColumns().size() > 0) {
            // set column def in CREATE TABLE
            table = new IcebergTable(tableId, tableName, stmt.getColumns(), icebergProperty, null);
        } else {
            // get column def from remote Iceberg
            table = getTableFromIceberg(tableId, tableName, icebergProperty,
                    TableIdentifier.of(icebergDb, icebergTbl), true);
        }

        // check iceberg table if exists in doris database
        if (!db.createTableWithLock(table, false, stmt.isSetIfNotExists()).first) {
            ErrorReport.reportDdlException(ErrorCode.ERR_TABLE_EXISTS_ERROR, tableName);
        }
        LOG.info("successfully create table[{}-{}]", tableName, table.getId());
    }
}
