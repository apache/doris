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

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Objects;

/**
 * Iceberg property contains information to connect a remote iceberg db or table.
 */
public class IcebergProperty {
    public static final String ICEBERG_DATABASE = "iceberg.database";
    public static final String ICEBERG_TABLE = "iceberg.table";
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_CATALOG_IMPL = "iceberg.catalog.catalog-impl";
    public static final String ICEBERG_HIVE_METASTORE_URIS = "iceberg.hive.metastore.uris";

    private static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";

    private final boolean exist;
    private final String database;
    private final String catalogType;
    private final String catalogImpl;
    private final Map<String, String> catalogProperties;

    private String table;

    public IcebergProperty(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.database = properties.get(ICEBERG_DATABASE);
            this.table = properties.get(ICEBERG_TABLE);
            this.catalogType = properties.get(ICEBERG_CATALOG_TYPE);
            this.catalogImpl = properties.get(ICEBERG_CATALOG_IMPL);

            // catalog properties
            ImmutableMap.Builder<String, String> catalogPropertiesBuilder = ImmutableMap.builder();
            properties.forEach((k, v) -> {
                if (k.startsWith(ICEBERG_CATALOG_PREFIX)) {
                    catalogPropertiesBuilder.put(k.replace(ICEBERG_CATALOG_PREFIX, ""), v);
                }
            });
            this.catalogProperties = catalogPropertiesBuilder.build();
        } else {
            this.exist = false;
            this.database = null;
            this.catalogType = null;
            this.catalogImpl = null;
            this.catalogProperties = ImmutableMap.of();
        }
    }

    // Create a new Iceberg property from other property
    public IcebergProperty(IcebergProperty otherProperty) {
        this.exist = otherProperty.exist;
        this.database = otherProperty.database;
        this.table = otherProperty.table;
        this.catalogType = otherProperty.catalogType;
        this.catalogImpl = otherProperty.catalogImpl;
        this.catalogProperties = otherProperty.catalogProperties;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getTable() {
        return table;
    }

    public boolean isExist() {
        return exist;
    }

    public String getDatabase() {
        return database;
    }

    public String getCatalogType() {
        return catalogType;
    }

    public String getCatalogImpl() {
        return catalogImpl;
    }

    public Map<String, String> getCatalogProperties() {
        return catalogProperties;
    }

    /**
     * Return catalog type or catalog impl. Because we do not allow to have both catalog type and catalog impl.
     * So we will return one of them which is not null.
     */
    public String getCatalogTypeOrImpl() {
        return catalogType != null ? catalogType : catalogImpl;
    }

    /**
     * Return the catalog properties, and the key of the map is prefixed with "iceberg.catalog.".
     */
    public Map<String, String> getCatalogPropertiesWithPrefix() {
        Map<String, String> map = Maps.newHashMap();
        // catalog type and catalog-impl also existed in catalogProperties
        catalogProperties.forEach((k, v) -> map.put(ICEBERG_CATALOG_PREFIX + k, v));
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(exist, database, table, catalogType, catalogImpl, catalogProperties);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        IcebergProperty that = (IcebergProperty) other;
        return this.exist == that.exist
            && Objects.equals(database, that.database)
            && Objects.equals(table, that.table)
            && Objects.equals(catalogType, that.catalogType)
            && Objects.equals(catalogImpl, that.catalogImpl)
            && Objects.equals(catalogProperties, that.catalogProperties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("database", database)
            .add("table", table)
            .add("catalogProperties", Joiner.on(",").withKeyValueSeparator(":").join(catalogProperties))
            .toString();
    }
}
