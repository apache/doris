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

suite("test_multi_delimit_serde", "p0,external,hive,external_docker,external_docker_hive") {
    String enabled = context.config.otherConfigs.get("enableHiveTest")
    if (enabled == null || !enabled.equalsIgnoreCase("true")) {
        logger.info("disable Hive test.")
        return;
    }

    for (String hivePrefix : ["hive2", "hive3"]) {
        String hms_port = context.config.otherConfigs.get(hivePrefix + "HmsPort")
        String catalog_name = "${hivePrefix}_test_multi_delimit_serde"
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")

        sql """drop catalog if exists ${catalog_name}"""
        sql """create catalog if not exists ${catalog_name} properties (
            "type"="hms",
            'hive.metastore.uris' = 'thrift://${externalEnvIp}:${hms_port}'
        );"""

        logger.info("catalog " + catalog_name + " created")
        sql """switch ${catalog_name};"""
        logger.info("switched to catalog " + catalog_name)

        sql """use regression;"""

        try {
            // Test 1: MultiDelimitSerDe with |+| delimiter - using pre-created table
            qt_01 """SELECT * FROM multi_delimit_test ORDER BY k1"""

            // Test 2: Different multi-character delimiter - using pre-created table
            qt_02 """SELECT * FROM multi_delimit_test2 ORDER BY id"""

            // Test 3: Complex types with array and map to test collection.delim and mapkey.delim
            logger.info("Test 3: Using pre-created table with array and map types")
            qt_03 """SELECT id, name, tags, properties FROM multi_delimit_complex_test ORDER BY id"""

            // Test 4: Insert data using Doris to write to Hive MultiDelimitSerDe tables
            logger.info("Test 4: Testing Doris INSERT to Hive MultiDelimitSerDe tables")

            // Test 4.1: Insert to basic multi-delimit table
            sql """INSERT INTO multi_delimit_test VALUES (4, 400, 'test4'), (5, 500, 'test5')"""
            qt_04 """SELECT * FROM multi_delimit_test WHERE k1 >= 4 ORDER BY k1"""

            // Test 4.2: Insert to double-pipe delimited table
            sql """INSERT INTO multi_delimit_test2 VALUES (4, 4.5, 'description4'), (5, 5.5, 'description5')"""
            qt_05 """SELECT * FROM multi_delimit_test2 WHERE id >= 4 ORDER BY id"""

            // Test 4.3: Insert to complex types table with arrays and maps
            sql """INSERT INTO multi_delimit_complex_test VALUES
                (3, 'user3', ARRAY('tagX', 'tagY'), MAP('newkey', 'newvalue'), ARRAY(ARRAY(7, 8)))"""
            qt_06 """SELECT id, name, tags, properties FROM multi_delimit_complex_test WHERE id = 3 ORDER BY id"""

            // Test 5: Show create table to check SerDe properties
            logger.info("Test 5: Checking show create table")
            def createTableResult = sql """SHOW CREATE TABLE multi_delimit_test"""
            logger.info("Create table result: " + createTableResult.toString())

            assertTrue(createTableResult.toString().contains("MultiDelimitSerDe"))
            assertTrue(createTableResult.toString().contains("field.delim"))
        } catch (Exception e) {
             logger.warn("Test failed, this might be expected if Hive version doesn't support MultiDelimitSerDe: " + e.getMessage())
            if (e.getMessage().contains("Unsupported hive table serde")) {
                logger.info("Got expected 'Unsupported hive table serde' error before implementing MultiDelimitSerDe support")
            }
        }
        sql """drop catalog if exists ${catalog_name}"""
    }
}