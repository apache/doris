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

package org.apache.doris.connector.iceberg;

/**
 * Property constants for Iceberg connector configuration.
 * Mirrors keys from fe-core's Iceberg property classes without taking
 * a compile-time dependency on fe-core.
 */
public final class IcebergConnectorProperties {

    private IcebergConnectorProperties() {
    }

    // -- Catalog type (second-level dispatch) --
    public static final String ICEBERG_CATALOG_TYPE = "iceberg.catalog.type";

    // -- Supported catalog type values --
    public static final String TYPE_REST = "rest";
    public static final String TYPE_HMS = "hms";
    public static final String TYPE_GLUE = "glue";
    public static final String TYPE_DLF = "dlf";
    public static final String TYPE_JDBC = "jdbc";
    public static final String TYPE_HADOOP = "hadoop";
    public static final String TYPE_S3_TABLES = "s3tables";

    // -- Warehouse --
    public static final String WAREHOUSE = "warehouse";

    // -- Type mapping options --
    public static final String ENABLE_MAPPING_VARBINARY = "enable_mapping_varbinary";
    public static final String ENABLE_MAPPING_TIMESTAMP_TZ = "enable_mapping_timestamp_tz";

    // -- REST catalog options --
    public static final String REST_NESTED_NAMESPACE_ENABLED = "iceberg.rest.nested-namespace-enabled";

    // -- Cache configuration --
    public static final String TABLE_CACHE_ENABLE = "meta.cache.iceberg.table.enable";
    public static final String TABLE_CACHE_TTL = "meta.cache.iceberg.table.ttl-second";
    public static final String TABLE_CACHE_CAPACITY = "meta.cache.iceberg.table.capacity";
    public static final String MANIFEST_CACHE_ENABLE = "meta.cache.iceberg.manifest.enable";
    public static final String MANIFEST_CACHE_TTL = "meta.cache.iceberg.manifest.ttl-second";
    public static final String MANIFEST_CACHE_CAPACITY = "meta.cache.iceberg.manifest.capacity";
}
