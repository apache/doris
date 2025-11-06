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

suite("test_hive_metadata_refresh_interval", "p0,external,hive,external_docker,external_docker_hive") {

    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        setHivePrefix(hivePrefix)
        try {
            String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
            String hdfs_port = context.config.otherConfigs.get(hivePrefix + "HdfsPort")
            String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

            String catalog_name = "test_${hivePrefix}_refresh_interval"
            String test_db = "test_refresh_interval_db"
            String table1 = "test_refresh_table_1"
            String table2 = "test_refresh_table_2"

            sql """drop catalog if exists ${catalog_name}"""
            hive_docker "drop database if exists ${test_db} cascade"
            hive_docker "create database ${test_db}"

            // Create catalog with 10s refresh interval
            sql """create catalog if not exists ${catalog_name} properties (
                'type'='hms',
                'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}',
                'fs.defaultFS' = 'hdfs://${externalEnvIp}:${hdfs_port}',
                'use_meta_cache' = 'true',
                'metadata_refresh_interval_sec' = '10'
            )"""

            sql "switch ${catalog_name}"
            sql "use ${test_db}"

            // Show tables - should be empty
            def result1 = sql "show tables from ${test_db}"
            assertEquals(0, result1.size())

            // Create table1 via hive_docker
            hive_docker "create table ${test_db}.${table1} (id int, name string)"

            // Wait 10s for automatic refresh
            sleep(10000)

            // Show tables - should have table1
            def result2 = sql "show tables from ${test_db}"
            assertEquals(1, result2.size())
            assertTrue(result2.toString().contains(table1))

            // ALTER catalog to 60s refresh interval
            sql """alter catalog ${catalog_name} set properties (
                'metadata_refresh_interval_sec' = '60'
            )"""

            // Show tables - should still have only table1
            def result3 = sql "show tables from ${test_db}"
            assertEquals(1, result3.size())
            assertTrue(result3.toString().contains(table1))
            assertFalse(result3.toString().contains(table2))

            // Create table2 via hive_docker
            hive_docker "create table ${test_db}.${table2} (id int, value string)"

            // Wait 60s for automatic refresh
            sleep(60000)

            // Show tables - should have both table1 and table2
            def result4 = sql "show tables from ${test_db}"
            assertEquals(2, result4.size())
            assertTrue(result4.toString().contains(table1))
            assertTrue(result4.toString().contains(table2))

            // Cleanup
            hive_docker "drop database ${test_db} cascade"
            sql """drop catalog ${catalog_name}"""

        } finally {
            sql """drop catalog if exists test_${hivePrefix}_refresh_interval"""
            hive_docker "drop database if exists test_refresh_interval_db cascade"
        }
    }
}
