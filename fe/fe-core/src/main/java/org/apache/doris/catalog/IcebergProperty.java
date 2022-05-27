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

import java.util.Map;

/**
 * Iceberg property contains information to connect a remote iceberg db or table.
 */
public class IcebergProperty {
    public static final String ICEBERG_DATABASE = "iceberg.database";
    public static final String ICEBERG_TABLE = "iceberg.table";
    public static final String ICEBERG_HIVE_METASTORE_URIS = "iceberg.hive.metastore.uris";
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";

    private boolean exist;

    private String database;
    private String table;
    private String hiveMetastoreUris;
    private String catalogType;

    public IcebergProperty(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.database = properties.get(ICEBERG_DATABASE);
            this.table = properties.get(ICEBERG_TABLE);
            this.hiveMetastoreUris = properties.get(ICEBERG_HIVE_METASTORE_URIS);
            this.catalogType = properties.get(ICEBERG_CATALOG_TYPE);
        } else {
            this.exist = false;
        }
    }

    // Create a new Iceberg property from other property
    public IcebergProperty(IcebergProperty otherProperty) {
        this.exist = otherProperty.exist;
        this.database = otherProperty.database;
        this.table = otherProperty.table;
        this.hiveMetastoreUris = otherProperty.hiveMetastoreUris;
        this.catalogType = otherProperty.catalogType;
    }

    public boolean isExist() {
        return exist;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public String getHiveMetastoreUris() {
        return hiveMetastoreUris;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public String getProperties() {
        return "";
    }

    public void setTable(String table) {
        this.table = table;
    }
}
