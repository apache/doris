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

suite("test_es_datetime_type_conversion", "p0,external,es,external_docker,external_docker_es") {
    String enabled = context.config.otherConfigs.get("enableEsTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String es_7_port = context.config.otherConfigs.get("es_7_port")
        String es_8_port = context.config.otherConfigs.get("es_8_port")

        sql """drop catalog if exists test_es_datetime_fix_7;"""
        sql """drop catalog if exists test_es_datetime_fix_8;"""

        // Create ES catalog for testing datetime type conversion fix
        sql """create catalog if not exists test_es_datetime_fix_7 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_7_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "mapping_es_id"="true"
        );
        """

        sql """create catalog if not exists test_es_datetime_fix_8 properties(
            "type"="es",
            "hosts"="http://${externalEnvIp}:$es_8_port",
            "nodes_discovery"="false",
            "enable_keyword_sniff"="true",
            "mapping_es_id"="true"
        );
        """

        def executeWithRetry = { query, queryName, maxRetries ->
            def retryCount = 0
            def success = false

            while (!success && retryCount < maxRetries) {
                try {
                    sql query
                    success = true
                } catch (Exception e) {
                    if (e.getMessage().contains("EsTable metadata has not been synced, Try it later")) {
                        logger.error("Failed to execute ${queryName}: ${e.getMessage()}")
                        logger.info("Retrying... Attempt ${retryCount + 1}")
                        retryCount++
                        sleep(1000)
                    } else {
                        throw e
                    }
                }
            }

            if (!success) {
                throw new RuntimeException("Failed to execute ${queryName} after ${maxRetries} attempts")
            }
        }

        def test_datetime_timezone_format_fix = { catalog_name ->
            sql """switch ${catalog_name}"""

            // Test 1: select * should work after fix
            try {
                executeWithRetry("""select * from test1 where test1='string1' limit 1""", "datetime_select_all_with_timezone", 30)
                logger.info("select * with datetime timezone format - SUCCESS")
            } catch (Exception e) {
                if (e.getMessage().contains("Expected value of type") && e.getMessage().contains("DATETIMEV2") && e.getMessage().contains("Varchar/Char")) {
                    throw new RuntimeException("BUG NOT FIXED: select * still fails with datetime timezone format: " + e.getMessage())
                } else {
                    throw e
                }
            }

            // Test 2: Individual datetime field selection should continue working
            executeWithRetry("""select test6 from test1 where test1='string1' limit 1""", "datetime_individual_field", 30)
            logger.info("select individual datetime field - SUCCESS")

            // Test 3: Multiple fields including datetime should work
            executeWithRetry("""select test1, test6, test7, test8 from test1 where test1='string1' limit 1""", "datetime_multiple_fields_with_timezone", 30)
            logger.info("select multiple datetime fields - SUCCESS")
        }

        test_datetime_timezone_format_fix("test_es_datetime_fix_7")
        test_datetime_timezone_format_fix("test_es_datetime_fix_8")

        sql """drop catalog if exists test_es_datetime_fix_7;"""
        sql """drop catalog if exists test_es_datetime_fix_8;"""
    }
}
