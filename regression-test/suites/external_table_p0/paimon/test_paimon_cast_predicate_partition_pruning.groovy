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

suite("test_paimon_cast_predicate_partition_pruning", "p0,external,doris,external_docker,external_docker_doris") {

    String enabled = context.config.otherConfigs.get("enablePaimonTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable paimon test.")
        return
    }

    String catalog_name = "test_paimon_cast_predicate_partition_pruning"
    String db_name = "partition_db"
    String minio_port = context.config.otherConfigs.get("iceberg_minio_port")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    sql """drop catalog if exists ${catalog_name}"""

    sql """
        CREATE CATALOG ${catalog_name} PROPERTIES (
                'type' = 'paimon',
                'warehouse' = 's3://warehouse/wh',
                's3.endpoint' = 'http://${externalEnvIp}:${minio_port}',
                's3.access_key' = 'admin',
                's3.secret_key' = 'password',
                's3.path.style.access' = 'true'
        );
    """
    sql """use `${catalog_name}`.`${db_name}`;"""

    // Test CAST expression predicate correctness for Paimon partition pruning
    // This test ensures that CAST expressions are NOT pushed down to Paimon
    // and are properly handled by Doris to avoid type conversion inconsistencies
    def test_cast_predicate_partition_pruning = {
        
        // ==============================================
        // Core CAST scenarios for Paimon
        // ==============================================
        
        // Test 1: CAST timestamp partition to date
        qt_cast_timestamp_to_date """
            select count(*) from timestamp_partitioned 
            where CAST(partition_key AS DATE) = DATE '2024-01-15';
        """
        
        // Test 2: CAST int partition to string
        qt_cast_int_to_string """
            select count(*) from int_partitioned 
            where CAST(partition_key AS STRING) IN ('1', '2');
        """
        
        // Test 3: CAST bigint partition to string (Paimon specific)
        qt_cast_bigint_to_string """
            select count(*) from bigint_partitioned 
            where CAST(partition_key AS STRING) >= '100';
        """
        
        // Test 4: CAST decimal partition to int (Paimon specific)
        qt_cast_decimal_to_int """
            select count(*) from decimal_partitioned 
            where CAST(partition_key AS INT) IN (10, 25);
        """
        
        // Test 5: CAST boolean partition to string (Paimon specific)
        qt_cast_boolean_to_string """
            select count(*) from boolean_partitioned 
            where CAST(partition_key AS STRING) = '1';
        """
        
        // ==============================================
        // Mixed predicates and edge cases
        // ==============================================
        
        // Test 6: CAST with normal predicate (should push normal part only)
        qt_cast_and_normal """
            select count(*) from string_partitioned 
            where id > 2 AND CAST(partition_key AS STRING) LIKE 'A%';
        """
        
        // Test 7: Nested CAST expressions
        qt_nested_cast """
            select count(*) from int_partitioned 
            where CAST(CAST(partition_key AS STRING) AS INT) = 1;
        """
        
        // Test 8: CAST with NULL handling
        qt_cast_null_check """
            select count(*) from decimal_partitioned 
            where CAST(partition_key AS INT) IS NOT NULL;
        """
        
        // ==============================================
        // Validation: Normal vs CAST predicates
        // ==============================================
        
        // Test 9: Verify normal predicates still work (baseline)
        qt_normal_predicate_baseline """
            select count(*) from int_partitioned where partition_key = 1;
        """
        
        // Test 10: CAST equivalent should return same logical result
        qt_cast_predicate_equivalent """
            select count(*) from int_partitioned where CAST(partition_key AS INT) = 1;
        """
        
        // Test 11: Normal vs CAST string predicates
        qt_normal_string_predicate """
            select count(*) from string_partitioned where partition_key = 'Europe';
        """
        
        qt_cast_string_predicate_equivalent """
            select count(*) from string_partitioned where CAST(partition_key AS STRING) = 'Europe';
        """
    }

    try {
        sql """ set time_zone = 'Asia/Shanghai'; """
        
        logger.info("Testing Paimon CAST predicate partition pruning correctness...")
        test_cast_predicate_partition_pruning()
        logger.info("Paimon CAST predicate partition pruning tests completed.")

    } finally {
        sql """ unset variable time_zone; """
    }
}