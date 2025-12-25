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

suite("test_iceberg_manifest_cache_explain", "p0,external,doris,external_docker,external_docker_doris") {
    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String restPort = context.config.otherConfigs.get("iceberg_rest_uri_port")
        String minioPort = context.config.otherConfigs.get("iceberg_minio_port")
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String catalogName = "test_iceberg_manifest_cache_explain"

        try {
            sql """set enable_external_table_batch_mode=false"""
            sql """drop catalog if exists ${catalogName}"""
            sql """
                CREATE CATALOG ${catalogName} PROPERTIES (
                    'type'='iceberg',
                    'iceberg.catalog.type'='rest',
                    'uri' = 'http://${externalEnvIp}:${restPort}',
                    "s3.access_key" = "admin",
                    "s3.secret_key" = "password",
                    "s3.endpoint" = "http://${externalEnvIp}:${minioPort}",
                    "s3.region" = "us-east-1",
                    "iceberg.manifest.cache.enable" = "true"
                );
            """

            sql """switch ${catalogName}"""
            sql """use multi_catalog"""

            String tableName = "tb_ts_filter"

            def extractManifestCacheMetrics = {
                def result = sql """explain verbose select * from ${tableName} where id < 5"""
                String text = result.toString()
                def matcher = (text =~ /manifest cache: hits=(\\d+), misses=(\\d+), failures=(\\d+)/)
                assertTrue(matcher.find(), "Manifest cache line should exist in explain")
                return [matcher.group(1).toLong(), matcher.group(2).toLong(), matcher.group(3).toLong()]
            }

            def first = extractManifestCacheMetrics()
            assertTrue(first[0] == 0, "First explain should have zero manifest cache hits")
            assertTrue(first[1] > 0, "First explain should populate manifest cache (misses > 0)")
            assertTrue(first[2] == 0, "Manifest cache failures should be zero")

            def second = extractManifestCacheMetrics()
            assertTrue(second[0] > 0, "Second explain should hit manifest cache")
            assertTrue(second[2] == 0, "Manifest cache failures should remain zero")
        } finally {
            sql """switch internal"""
            sql """set enable_external_table_batch_mode=true"""
            sql """drop catalog if exists ${catalogName}"""
        }
    }
}
