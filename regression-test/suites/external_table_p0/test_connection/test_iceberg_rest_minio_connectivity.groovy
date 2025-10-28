// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("test_iceberg_rest_minio_connectivity", "p0,external,iceberg,external_docker,external_docker_polaris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        return
    }

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String polaris_port = context.config.otherConfigs.get("polaris_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("polaris_minio_port")

    // ========== Test Meta Connectivity Failure ==========
    // Test with invalid REST URI
    def test_meta_fail_catalog = "test_meta_connectivity_fail"
    sql """DROP CATALOG IF EXISTS ${test_meta_fail_catalog}"""

    test {
        sql """
            CREATE CATALOG ${test_meta_fail_catalog} PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'iceberg.rest.uri' = 'http://${externalEnvIp}:9999/invalid/path',
                'warehouse' = 'test_warehouse',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.region' = 'us-east-1',
                'test_connection' = 'true'
            );
        """
        exception "connectivity test failed"
        exception "Iceberg REST"
    }

    // ========== Test Storage Connectivity Failure ==========
    // Test with invalid MinIO credentials
    def test_storage_fail_catalog = "test_storage_connectivity_fail"
    sql """DROP CATALOG IF EXISTS ${test_storage_fail_catalog}"""

    test {
        sql """
            CREATE CATALOG ${test_storage_fail_catalog} PROPERTIES (
                'type'='iceberg',
                'iceberg.catalog.type'='rest',
                'iceberg.rest.uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog',
                'iceberg.rest.security.type' = 'oauth2',
                'iceberg.rest.oauth2.credential' = 'root:secret123',
                'iceberg.rest.oauth2.server-uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog/v1/oauth/tokens',
                'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:ALL',
                'warehouse' = 'doris_test',
                's3.access_key' = 'wrong_key',
                's3.secret_key' = 'wrong_secret',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.region' = 'us-east-1',
                'test_connection' = 'true'
            );
        """
        exception "connectivity test failed"
    }

    // ========== Test Successful Connectivity ==========
    // Test with valid credentials
    def test_success_catalog = "test_connectivity_success"
    sql """DROP CATALOG IF EXISTS ${test_success_catalog}"""

    sql """
        CREATE CATALOG ${test_success_catalog} PROPERTIES (
            'type'='iceberg',
            'iceberg.catalog.type'='rest',
            'iceberg.rest.uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog',
            'iceberg.rest.security.type' = 'oauth2',
            'iceberg.rest.oauth2.credential' = 'root:secret123',
            'iceberg.rest.oauth2.server-uri' = 'http://${externalEnvIp}:${polaris_port}/api/catalog/v1/oauth/tokens',
            'iceberg.rest.oauth2.scope' = 'PRINCIPAL_ROLE:ALL',
            'warehouse' = 'doris_test',
            's3.access_key' = 'admin',
            's3.secret_key' = 'password',
            's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
            's3.region' = 'us-east-1',
            'test_connection' = 'true'
        );
    """

    logger.info("Connectivity test passed successfully")

    // Cleanup
    sql """DROP CATALOG IF EXISTS ${test_success_catalog}"""
}
