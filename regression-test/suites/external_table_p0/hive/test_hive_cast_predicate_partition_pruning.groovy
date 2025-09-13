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

suite("test_hive_cast_predicate_partition_pruning", "p0,external,hive,external_docker,external_docker_hive") {
    
    // Test CAST expression predicate correctness for Hive partition pruning
    // This test ensures that CAST expressions are NOT pushed down to Hive
    // and are properly handled by Doris to avoid type conversion inconsistencies
    def test_cast_predicate_partition_pruning = {

        // ==============================================
        // Core CAST scenarios for Hive
        // ==============================================
        
        // Test 1: CAST date partition to string
        qt_cast_date_to_string """
            select count(*) from date_partition_table 
            where CAST(partition_col AS STRING) >= '2024-01-01';
        """
        
        // Test 2: CAST int partition to string
        qt_cast_int_to_string """
            select count(*) from int_partition_table 
            where CAST(partition_col AS STRING) IN ('1', '2');
        """
        
        // Test 3: CAST string partition to date
        qt_cast_string_to_date """
            select count(*) from string_partition_table 
            where CAST(partition_col AS DATE) >= DATE '2024-01-01';
        """
        
        // Test 4: CAST decimal partition to int
        qt_cast_decimal_to_int """
            select count(*) from decimal_partition_table 
            where CAST(partition_col AS INT) IN (100, 300);
        """

        // ==============================================
        // Mixed predicates and edge cases
        // ==============================================
        
        // Test 5: Nested CAST expressions
        qt_nested_cast """
            select count(*) from decimal_partition_table 
            where CAST(CAST(partition_col AS STRING) AS DECIMAL(10,2)) > 10.0;
        """
        
        // Test 6: CAST with NULL handling
        qt_cast_null_check """
            select count(*) from string_partition_table 
            where CAST(partition_col AS DATE) IS NULL;
        """
        
        // ==============================================
        // Validation: Normal vs CAST predicates
        // ==============================================
        
        // Test 7: Verify normal predicates still work (baseline)
        qt_normal_predicate_baseline """
            select count(*) from int_partition_table where partition_col = 1;
        """
        
        // Test 8: CAST equivalent should return same logical result
        qt_cast_predicate_equivalent """
            select count(*) from int_partition_table where CAST(partition_col AS INT) = 1;
        """
        
        // Test 9: Normal vs CAST string predicates
        qt_normal_string_predicate """
            select count(*) from string_partition_table where partition_col = 'A';
        """
        
        qt_cast_string_predicate_equivalent """
            select count(*) from string_partition_table where CAST(partition_col AS STRING) = 'A';
        """
    }

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String catalog_name = "${hivePrefix}_test_hive_cast_predicate_partition_pruning"
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            sql """drop catalog if exists ${catalog_name}"""
            sql """create catalog if not exists ${catalog_name} properties (
                "type"="hms",
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
            );"""
            sql """use `${catalog_name}`.`partition_tables`"""

            sql """ set time_zone = 'Asia/Shanghai'; """
            
            logger.info("Testing Hive CAST predicate partition pruning correctness for ${hivePrefix}...")
            test_cast_predicate_partition_pruning()
            logger.info("Hive CAST predicate partition pruning tests completed for ${hivePrefix}.")
        
        } finally {
            sql """ unset variable time_zone; """
        }
    }
}