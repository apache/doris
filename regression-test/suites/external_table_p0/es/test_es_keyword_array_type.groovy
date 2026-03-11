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

// Test for https://github.com/apache/doris/issues/XXXXX
// When ES mapping defines a field as keyword/text, but the actual data stored
// is an array, Doris should not throw an error. Instead, it should serialize
// the array to a JSON string representation.

suite("test_es_keyword_array_type", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists test_es_keyword_array_es7;"""
        sql """drop catalog if exists test_es_keyword_array_es8;"""

        // Create ES7 catalog
        sql """create catalog if not exists test_es_keyword_array_es7 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        // Create ES8 catalog
        sql """create catalog if not exists test_es_keyword_array_es8 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true"
        );
        """

        // Test ES7
        sql """switch test_es_keyword_array_es7"""
        // Wait for metadata sync
        def maxRetries = 30
        def retryCount = 0
        def success = false
        while (!success && retryCount < maxRetries) {
            try {
                sql """select * from test_keyword_array"""
                success = true
            } catch (Exception e) {
                if (e.getMessage().contains("EsTable metadata has not been synced, Try it later")) {
                    logger.info("Waiting for ES metadata sync... Attempt ${retryCount + 1}")
                    retryCount++
                    sleep(1000)
                } else {
                    throw e
                }
            }
        }
        if (!success) {
            throw new RuntimeException("Failed to sync ES metadata after ${maxRetries} attempts")
        }

        // Test: Query keyword fields that contain array data should not throw error
        // Previously this would fail with: "Expected value of type: STRING; but found type: Array"
        order_qt_es7_keyword_array_01 """select id, name, tags, emails, single_value from test_keyword_array order by id"""
        order_qt_es7_keyword_array_02 """select id, name, description from test_keyword_array order by id"""
        order_qt_es7_keyword_array_03 """select id, tags from test_keyword_array where id = 1"""
        order_qt_es7_keyword_array_04 """select id, single_value from test_keyword_array where id = 3"""

        // Test ES8
        sql """switch test_es_keyword_array_es8"""
        retryCount = 0
        success = false
        while (!success && retryCount < maxRetries) {
            try {
                sql """select * from test_keyword_array"""
                success = true
            } catch (Exception e) {
                if (e.getMessage().contains("EsTable metadata has not been synced, Try it later")) {
                    logger.info("Waiting for ES metadata sync... Attempt ${retryCount + 1}")
                    retryCount++
                    sleep(1000)
                } else {
                    throw e
                }
            }
        }
        if (!success) {
            throw new RuntimeException("Failed to sync ES metadata after ${maxRetries} attempts")
        }

        order_qt_es8_keyword_array_01 """select id, name, tags, emails, single_value from test_keyword_array order by id"""
        order_qt_es8_keyword_array_02 """select id, name, description from test_keyword_array order by id"""
        order_qt_es8_keyword_array_03 """select id, tags from test_keyword_array where id = 1"""
        order_qt_es8_keyword_array_04 """select id, single_value from test_keyword_array where id = 3"""

        // Cleanup
        sql """drop catalog if exists test_es_keyword_array_es7;"""
        sql """drop catalog if exists test_es_keyword_array_es8;"""
    }
}
