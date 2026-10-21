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

suite("test_iceberg_migrated_nested_without_name_mapping", "p0,external") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String catalog_name = "test_iceberg_migrated_nested_without_name_mapping"

    sql """drop catalog if exists ${catalog_name}"""
    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'uri' = 'http://${externalEnvIp}:${rest_port}',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.region' = 'us-east-1'
        )
    """
    sql """switch ${catalog_name}"""
    sql """use multi_catalog"""
    sql """set enable_fallback_to_original_planner=false"""

    // The physical Parquet fields predate migration and carry no Iceberg IDs, and this registered
    // metadata has no `schema.name-mapping.default`. Iceberg's Parquet reader binds such a file by
    // position (ParquetSchemaUtil.addFallbackIds + pruneColumnsFallback, see DORIS-29685): the k-th
    // physical top-level field is field id k, so `id` (field id 1) must read its real value instead
    // of NULL. Fallback ids are stamped on top-level fields only, so the nested children stay NULL
    // until a name mapping is restored.
    qt_idless_top_level_field_uses_positional_id """
        SELECT id, nested_struct.value_a, nested_struct.value_b
        FROM migrated_nested_without_name_mapping
        ORDER BY id
    """

    // The same positional binding must feed predicate/row-group filtering, otherwise the row would
    // be readable but unfilterable.
    qt_idless_top_level_field_filter_uses_positional_id """
        SELECT count(*) FROM migrated_nested_without_name_mapping WHERE id = 1
    """

    sql """drop catalog if exists ${catalog_name}"""
}
