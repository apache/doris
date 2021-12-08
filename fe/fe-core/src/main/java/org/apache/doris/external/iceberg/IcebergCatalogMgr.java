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

import org.apache.doris.common.DdlException;

import com.google.common.base.Enums;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * Iceberg catalog manager
 */
public class IcebergCatalogMgr {
    private static final Logger LOG = LogManager.getLogger(IcebergCatalogMgr.class);

    private static final String PROPERTY_MISSING_MSG = "Iceberg %s is null. " +
            "Please add properties('%s'='xxx') when create iceberg database.";
    public static final String DATABASE = "database";
    public static final String TABLE = "table";
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    public static final String CATALOG_TYPE = "catalog.type";

    // TODO:(qjl) We'll support more types of Iceberg catalog.
    public enum CatalogType {
        HIVE_CATALOG
    }

    public static IcebergCatalog getCatalog(Map<String, String> properties) throws DdlException {
        CatalogType type = CatalogType.valueOf(properties.get(CATALOG_TYPE));
        IcebergCatalog catalog;
        switch (type) {
            case HIVE_CATALOG:
                catalog = new HiveCatalog();
                break;
            default:
                throw new DdlException("Unsupported catalog type: " + type);
        }
        catalog.initialize(properties);
        return catalog;
    }

    public static void validateProperties(Map<String, String> properties, boolean isTable) throws DdlException {
        if (properties.size() == 0) {
            throw new DdlException("Please set properties of hive table, "
                    + "they are: database and 'hive.metastore.uris'");
        }

        Map<String, String> copiedProps = Maps.newHashMap(properties);
        String icebergDb = copiedProps.get(DATABASE);
        if (Strings.isNullOrEmpty(icebergDb)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, DATABASE, DATABASE));
        }
        copiedProps.remove(DATABASE);

        // check hive properties
        // hive.metastore.uris
        String hiveMetastoreUris = copiedProps.get(HIVE_METASTORE_URIS);
        if (Strings.isNullOrEmpty(hiveMetastoreUris)) {
            throw new DdlException(String.format(PROPERTY_MISSING_MSG, HIVE_METASTORE_URIS, HIVE_METASTORE_URIS));
        }
        copiedProps.remove(HIVE_METASTORE_URIS);

        // check iceberg catalog type
        String icebergCatalogType = copiedProps.get(CATALOG_TYPE);
        if (Strings.isNullOrEmpty(icebergCatalogType)) {
            icebergCatalogType = IcebergCatalogMgr.CatalogType.HIVE_CATALOG.name();
            properties.put(CATALOG_TYPE, icebergCatalogType);
        } else {
            copiedProps.remove(CATALOG_TYPE);
        }

        if (!Enums.getIfPresent(IcebergCatalogMgr.CatalogType.class, icebergCatalogType).isPresent()) {
            throw new DdlException("Unknown catalog type: " + icebergCatalogType + ". Current only support HiveCatalog.");
        }

        // only check table property when it's an iceberg table
        if (isTable) {
            String icebergTbl = copiedProps.get(TABLE);
            if (Strings.isNullOrEmpty(icebergTbl)) {
                throw new DdlException(String.format(PROPERTY_MISSING_MSG, TABLE, TABLE));
            }
            copiedProps.remove(TABLE);
        }

        if (!copiedProps.isEmpty()) {
            throw new DdlException("Unknown table properties: " + copiedProps.toString());
        }
    }
}
