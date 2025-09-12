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

suite("test_iceberg_cast_predicate_partition_pruning", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enableIcebergTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable iceberg test.")
        return
    }

    String catalog_name = "test_iceberg_cast_predicate_partition_pruning"
    String db_name = "partition_db"
    String rest_port = context.config.otherConfigs.get("iceberg_rest_uri_port")
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""
    sql """
    CREATE CATALOG ${catalog_name} PROPERTIES (
        'type'='iceberg',
        'iceberg.catalog.type'='rest',
        'uri' = 'http://${externalEnvIp}:${rest_port}',
        "s3.access_key" = "admin",
        "s3.secret_key" = "password",
        "s3.endpoint" = "http://${externalEnvIp}:${minio_port}",
        "s3.region" = "us-east-1"
    );"""

    sql """switch ${catalog_name}"""
    sql """use ${db_name}"""

    // Test CAST expression predicate correctness for Iceberg partition pruning
    // This test ensures that CAST expressions are NOT pushed down to Iceberg
    // and are properly handled by Doris to avoid type conversion inconsistencies
    def test_cast_predicate_partition_pruning = {
        
        // ==============================================
        // Core CAST scenarios - original issue #55804 
        // ==============================================

        // Test 1: CAST timestamp partition to date
        qt_cast_timestamp_to_date """
            select count(*) from timestamp_partitioned 
            where CAST(partition_key AS DATE) = DATE '2024-01-15';
        """
        
        // Test 2: CAST int partition to string with IN predicate
        qt_cast_int_to_string """
            select count(*) from int_partitioned 
            where CAST(partition_key AS STRING) IN ('1', '2');
        """
        
        // Test 3: CAST float partition to int
        qt_cast_float_to_int """
            select count(*) from float_partitioned 
            where CAST(partition_key AS INT) BETWEEN 10 AND 30;
        """
        
        // ==============================================
        // Mixed predicates and edge cases
        // ==============================================
        
        // Test 4: Nested CAST expressions
        qt_nested_cast """
            select count(*) from int_partitioned 
            where CAST(CAST(partition_key AS STRING) AS INT) = 1;
        """
        
        // Test 5: CAST with NULL handling
        qt_cast_null_check """
            select count(*) from string_partitioned 
            where CAST(partition_key AS DATE) IS NOT NULL;
        """
        
        // ==============================================
        // Regression test for original issue #55804
        // ==============================================
        
        // Create test table to reproduce the exact problem scenario
        sql """ DROP TABLE IF EXISTS test_string_datetime_cast """
        sql """
            CREATE TABLE IF NOT EXISTS test_string_datetime_cast (
                id BIGINT,
                event_name STRING,
                partition_date STRING
            ) PARTITION BY LIST (partition_date) ();
        """
        
        sql """
            INSERT INTO test_string_datetime_cast VALUES
            (1, 'Event A', '2025-06-10'),
            (2, 'Event B', '2025-06-09'), 
            (3, 'Event C', '2025-06-11')
        """
        
        // This exact query was failing before the fix
        qt_original_issue_regression """
            select count(*) from test_string_datetime_cast
            where CAST(partition_date AS DATETIME) >= '2025-06-10 00:00:00';
        """
        
        // ==============================================
        // Validation: Normal vs CAST predicates
        // ==============================================

        // Test 6: Verify normal predicates still work (baseline)
        qt_normal_predicate_baseline """
            select count(*) from int_partitioned where partition_key = 1;
        """

        // Test 7: CAST equivalent should return same logical result
        qt_cast_predicate_equivalent """
            select count(*) from int_partitioned where CAST(partition_key AS INT) = 1;
        """
    }

    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        
        logger.info("Testing CAST predicate partition pruning correctness...")
        test_cast_predicate_partition_pruning()
        logger.info("CAST predicate partition pruning tests completed.")

    } finally {
        sql """ unset variable time_zone; """
    }
}