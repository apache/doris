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

suite("test_iceberg_manifest_cache", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalogNameWithCache = "test_iceberg_manifest_cache_enabled"
        String catalogNameWithoutCache = "test_iceberg_manifest_cache_disabled"
        String tableName = "tb_ts_filter"

        try {
            sql """set enable_external_table_batch_mode=false"""
            
            // Create catalog with manifest cache enabled
            sql """drop catalog if exists ${catalogNameWithCache}"""
            sql """
                CREATE CATALOG ${catalogNameWithCache} PROPERTIES (
                    'type'='iceberg',
                    'iceberg.catalog.type'='rest',
                    'uri' = 'http://${externalEnvIp}:${restPort}',
                    "s3.access_key" = "admin",
                    "s3.secret_key" = "password",
                    "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                    "s3.region" = "us-east-1",
                    "meta.cache.iceberg.manifest.enable" = "true"
                );
            """

            // Create catalog with manifest cache disabled
            sql """drop catalog if exists ${catalogNameWithoutCache}"""
            sql """
                CREATE CATALOG ${catalogNameWithoutCache} PROPERTIES (
                    'type'='iceberg',
                    'iceberg.catalog.type'='rest',
                    'uri' = 'http://${externalEnvIp}:${restPort}',
                    "s3.access_key" = "admin",
                    "s3.secret_key" = "password",
                    "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                    "s3.region" = "us-east-1",
                    "meta.cache.iceberg.manifest.enable" = "false"
                );
            """

            // Test with cache enabled
            sql """switch ${catalogNameWithCache}"""
            sql """use multi_catalog"""

            // First explain should populate cache - check manifest cache info exists
            explain {
                sql("verbose select * from ${tableName} where id < 5")
                contains "manifest cache:"
                contains "hits=0"
                contains "misses=7"
                contains "failures=0"
            }

            // Test table structure with order_qt (should be same regardless of cache)
            order_qt_desc_with_cache """desc ${tableName}"""

            // Test table data with order_qt
            order_qt_select_with_cache """select * from ${tableName} where id < 5"""

            // Second explain should hit cache - verify cache metrics are present
            explain {
                sql("verbose select * from ${tableName} where id < 5")
                contains "manifest cache:"
                contains "hits=7"
                contains "misses=0"
                contains "failures=0"
            }

            // Test refresh catalog, the cache should be invalidated
            sql """ refresh catalog ${catalogNameWithCache} """
            explain {
                sql("verbose select * from ${tableName} where id < 5")
                contains "manifest cache:"
                contains "hits=0"
                contains "misses=7"
                contains "failures=0"
            }

            // Test with cache disabled
            sql """switch ${catalogNameWithoutCache}"""
            sql """use multi_catalog"""

            // Test table structure with order_qt (should be same as with cache)
            order_qt_desc_without_cache """desc ${tableName}"""

            // Test table data with order_qt
            order_qt_select_without_cache """select * from ${tableName} where id < 5"""

            // Explain should not contain manifest cache info when cache is disabled
            explain {
                sql("verbose select * from ${tableName} where id < 5")
                notContains "manifest cache:"
            }
        } finally {
            sql """set enable_external_table_batch_mode=true"""
        }
    }
}
