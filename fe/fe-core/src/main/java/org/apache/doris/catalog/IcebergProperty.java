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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Objects;

/**
 * Iceberg property contains information to connect a remote iceberg db or table.
 */
public class IcebergProperty {
    private static final Logger LOG = LogManager.getLogger(IcebergProperty.class);
    private static final String ICEBERG_CATALOG_PREFIX = "iceberg.catalog.";

    public static final String ICEBERG_DATABASE = "iceberg.database";
    public static final String ICEBERG_TABLE = "iceberg.table";
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";
    public static final String ICEBERG_HIVE_METASTORE_URIS = "iceberg.hive.metastore.uris";

    private final boolean exist;
    private final String database;
    private final String catalogType;
    private final Map<String, String> extraProperties;

    private String table;

    public IcebergProperty(Map<String, String> properties) {
        if (properties != null && !properties.isEmpty()) {
            this.exist = true;
            this.database = properties.get(ICEBERG_DATABASE);
            this.table = properties.get(ICEBERG_TABLE);
            this.catalogType = properties.get(ICEBERG_CATALOG_TYPE);

            // extra properties
            ImmutableMap.Builder<String, String> extraPropertiesBuilder = ImmutableMap.builder();
            properties.forEach((k, v) -> {
                if (k.startsWith(ICEBERG_CATALOG_PREFIX)) {
                    extraPropertiesBuilder.put(k.replace(ICEBERG_CATALOG_PREFIX, ""), v);
                }
            });
            this.extraProperties = extraPropertiesBuilder.build();
        } else {
            this.exist = false;
            this.database = null;
            this.catalogType = null;
            this.extraProperties = ImmutableMap.of();
        }
    }

    // Create a new Iceberg property from other property
    public IcebergProperty(IcebergProperty otherProperty) {
        this.exist = otherProperty.exist;
        this.database = otherProperty.database;
        this.table = otherProperty.table;
        this.catalogType = otherProperty.catalogType;
        this.extraProperties = otherProperty.extraProperties;
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

    public Map<String, String> getExtraProperties() {
        return extraProperties;
    }

    public Map<String, String> toMap() {
        Map<String, String> map = Maps.newHashMap();
        map.put(ICEBERG_DATABASE, database);
        map.put(ICEBERG_TABLE, table);
        map.put(ICEBERG_CATALOG_TYPE, catalogType);
        extraProperties.forEach((k, v) -> map.put(ICEBERG_CATALOG_PREFIX + k, v));
        return map;
    }

    @Override
    public int hashCode() {
        return Objects.hash(exist, database, table, catalogType, extraProperties);
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
        return this.exist == that.exist &&
            Objects.equals(database, that.database) &&
            Objects.equals(table, that.table) &&
            Objects.equals(catalogType, that.catalogType) &&
            Objects.equals(extraProperties, that.extraProperties);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("database", database)
            .add("table", table)
            .add("catalogType", catalogType)
            .add("extraProperties", Joiner.on(",").withKeyValueSeparator(":").join(extraProperties))
            .toString();
    }
}
