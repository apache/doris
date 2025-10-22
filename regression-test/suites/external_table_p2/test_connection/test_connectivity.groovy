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

suite("test_connectivity", "p2,external,hive,iceberg,external_docker,external_docker_hive,new_catalog_property") {

    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

    // ========== S3 Configuration ==========
    String s3_ak = context.config.otherConfigs.get("AWSAK")
    String s3_sk = context.config.otherConfigs.get("AWSSK")
    String s3_endpoint = context.config.otherConfigs.get("AWSEndpoint")
    String s3_region = context.config.otherConfigs.get("AWSRegion")
    String s3_bucket = "selectdb-qa-datalake-test-hk"

    String s3_storage_properties = """
        's3.access_key' = '${s3_ak}',
        's3.secret_key' = '${s3_sk}',
        's3.endpoint' = 'http://${s3_endpoint}',
        's3.region' = '${s3_region}'
    """

    // ========== HMS Configuration ==========
    String hms_port = context.config.otherConfigs.get("hms_port") ?: "9083"
    String hms_uri = "thrift://${externalEnvIp}:${hms_port}"

    // ========== Iceberg REST Configuration ==========
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port_s3") ?: "8181"
    String rest_uri = "http://${externalEnvIp}:${rest_port}"

    // ========== Test 1: Hive HMS + S3 Connectivity ==========
    logger.info("========== Testing Hive HMS + S3 Connectivity ==========")

    // Test 1.1: HMS connectivity failure (invalid HMS URI)
    def hive_meta_fail_catalog = "test_hive_meta_fail"
    sql """DROP CATALOG IF EXISTS ${hive_meta_fail_catalog}"""

    def hive_meta_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${hive_meta_fail_catalog} PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:9999',
                ${s3_storage_properties},
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        hive_meta_fail_caught = true
        logger.info("Hive HMS connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("HMS"),
            "Expected 'HMS' in error message, but got: " + e.getMessage()
    }
    assert hive_meta_fail_caught, "Hive HMS connectivity test should have failed but didn't"

    // Test 1.2: S3 storage connectivity failure (invalid S3 credentials)
    def hive_storage_fail_catalog = "test_hive_storage_fail"
    sql """DROP CATALOG IF EXISTS ${hive_storage_fail_catalog}"""

    def hive_storage_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${hive_storage_fail_catalog} PROPERTIES (
                'type' = 'hms',
                'hive.metastore.uris' = '${hms_uri}',
                's3.access_key' = 'wrong_key',
                's3.secret_key' = 'wrong_secret',
                's3.endpoint' = 'http://${s3_endpoint}',
                's3.region' = '${s3_region}',
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        hive_storage_fail_caught = true
        logger.info("Hive S3 storage connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("S3"),
            "Expected 'S3' in error message, but got: " + e.getMessage()
    }
    assert hive_storage_fail_caught, "Hive S3 storage connectivity test should have failed but didn't"

    // Test 1.3: Successful Hive HMS + S3 connectivity
    def hive_success_catalog = "test_hive_success"
    sql """DROP CATALOG IF EXISTS ${hive_success_catalog}"""

    sql """
        CREATE CATALOG ${hive_success_catalog} PROPERTIES (
            'type' = 'hms',
            'hive.metastore.uris' = '${hms_uri}',
            ${s3_storage_properties},
            'test_connection' = 'true'
        );
    """
    logger.info("Hive HMS + S3 connectivity test passed successfully")
    sql """DROP CATALOG IF EXISTS ${hive_success_catalog}"""

    // ========== Test 2: Iceberg HMS + S3 Connectivity ==========
    logger.info("========== Testing Iceberg HMS + S3 Connectivity ==========")

    // Test 2.1: HMS connectivity failure (invalid HMS URI)
    def iceberg_hms_meta_fail_catalog = "test_iceberg_hms_meta_fail"
    sql """DROP CATALOG IF EXISTS ${iceberg_hms_meta_fail_catalog}"""

    def iceberg_hms_meta_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${iceberg_hms_meta_fail_catalog} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:9999',
                'warehouse' = 's3a://${s3_bucket}/iceberg_hms_warehouse',
                ${s3_storage_properties},
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        iceberg_hms_meta_fail_caught = true
        logger.info("Iceberg HMS connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("HMS") || e.getMessage().contains("Iceberg HMS"),
            "Expected 'HMS' or 'Iceberg HMS' in error message, but got: " + e.getMessage()
    }
    assert iceberg_hms_meta_fail_caught, "Iceberg HMS connectivity test should have failed but didn't"

    // Test 2.2: S3 storage connectivity failure (invalid S3 credentials)
    def iceberg_hms_storage_fail_catalog = "test_iceberg_hms_storage_fail"
    sql """DROP CATALOG IF EXISTS ${iceberg_hms_storage_fail_catalog}"""

    def iceberg_hms_storage_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${iceberg_hms_storage_fail_catalog} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'hms',
                'hive.metastore.uris' = '${hms_uri}',
                'warehouse' = 's3a://${s3_bucket}/iceberg_hms_warehouse',
                's3.access_key' = 'wrong_key',
                's3.secret_key' = 'wrong_secret',
                's3.endpoint' = 'http://${s3_endpoint}',
                's3.region' = '${s3_region}',
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        iceberg_hms_storage_fail_caught = true
        logger.info("Iceberg HMS S3 storage connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("S3"),
            "Expected 'S3' in error message, but got: " + e.getMessage()
    }
    assert iceberg_hms_storage_fail_caught, "Iceberg HMS S3 storage connectivity test should have failed but didn't"

    // Test 2.3: Successful Iceberg HMS + S3 connectivity
    def iceberg_hms_success_catalog = "test_iceberg_hms_success"
    sql """DROP CATALOG IF EXISTS ${iceberg_hms_success_catalog}"""

    sql """
        CREATE CATALOG ${iceberg_hms_success_catalog} PROPERTIES (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'hms',
            'hive.metastore.uris' = '${hms_uri}',
            'warehouse' = 's3a://${s3_bucket}/iceberg_hms_warehouse',
            ${s3_storage_properties},
            'test_connection' = 'true'
        );
    """
    logger.info("Iceberg HMS + S3 connectivity test passed successfully")
    sql """DROP CATALOG IF EXISTS ${iceberg_hms_success_catalog}"""

    // ========== Test 3: Iceberg REST + S3 Connectivity ==========
    logger.info("========== Testing Iceberg REST + S3 Connectivity ==========")

    // Test 3.1: REST connectivity failure (invalid REST URI)
    def iceberg_rest_meta_fail_catalog = "test_iceberg_rest_meta_fail"
    sql """DROP CATALOG IF EXISTS ${iceberg_rest_meta_fail_catalog}"""

    def iceberg_rest_meta_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${iceberg_rest_meta_fail_catalog} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'rest',
                'iceberg.rest.uri' = 'http://${externalEnvIp}:9999/invalid',
                'warehouse' = 's3a://${s3_bucket}/iceberg_rest_warehouse',
                ${s3_storage_properties},
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        iceberg_rest_meta_fail_caught = true
        logger.info("Iceberg REST connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("Iceberg REST"),
            "Expected 'Iceberg REST' in error message, but got: " + e.getMessage()
    }
    assert iceberg_rest_meta_fail_caught, "Iceberg REST connectivity test should have failed but didn't"

    // Test 3.2: S3 storage connectivity failure (invalid S3 credentials)
    def iceberg_rest_storage_fail_catalog = "test_iceberg_rest_storage_fail"
    sql """DROP CATALOG IF EXISTS ${iceberg_rest_storage_fail_catalog}"""

    def iceberg_rest_storage_fail_caught = false
    try {
        sql """
            CREATE CATALOG ${iceberg_rest_storage_fail_catalog} PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'rest',
                'iceberg.rest.uri' = '${rest_uri}',
                'warehouse' = 's3a://${s3_bucket}/iceberg_rest_warehouse',
                's3.access_key' = 'wrong_key',
                's3.secret_key' = 'wrong_secret',
                's3.endpoint' = 'http://${s3_endpoint}',
                's3.region' = '${s3_region}',
                'test_connection' = 'true'
            );
        """
    } catch (Exception e) {
        iceberg_rest_storage_fail_caught = true
        logger.info("Iceberg REST S3 storage connectivity test failed as expected: " + e.getMessage())
        assert e.getMessage().contains("connectivity test failed"),
            "Expected 'connectivity test failed' in error message, but got: " + e.getMessage()
        assert e.getMessage().contains("S3"),
            "Expected 'S3' in error message, but got: " + e.getMessage()
    }
    assert iceberg_rest_storage_fail_caught, "Iceberg REST S3 storage connectivity test should have failed but didn't"

    // Test 3.3: Successful Iceberg REST + S3 connectivity
    def iceberg_rest_success_catalog = "test_iceberg_rest_success"
    sql """DROP CATALOG IF EXISTS ${iceberg_rest_success_catalog}"""

    sql """
        CREATE CATALOG ${iceberg_rest_success_catalog} PROPERTIES (
            'type' = 'iceberg',
            'iceberg.catalog.type' = 'rest',
            'iceberg.rest.uri' = '${rest_uri}',
            'warehouse' = 's3a://${s3_bucket}/iceberg_rest_warehouse',
            ${s3_storage_properties},
            'test_connection' = 'true'
        );
    """
    logger.info("Iceberg REST + S3 connectivity test passed successfully")
    sql """DROP CATALOG IF EXISTS ${iceberg_rest_success_catalog}"""

    logger.info("========== All connectivity tests completed ==========")
}
