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

            // Test 1: select * should work after fix for existing data
            try {
                executeWithRetry("""select * from test1 where test1='string1' limit 1""", "datetime_select_all_existing", 30)
                logger.info("select * with existing datetime data - SUCCESS")
            } catch (Exception e) {
                if (e.getMessage().contains("Expected value of type") && e.getMessage().contains("DATETIMEV2") && e.getMessage().contains("Varchar/Char")) {
                    throw new RuntimeException("BUG NOT FIXED: select * still fails with existing datetime format: " + e.getMessage())
                } else {
                    throw e
                }
            }

            // Test 2: Individual datetime field selection should continue working
            executeWithRetry("""select test6 from test1 where test1='string1' limit 1""", "datetime_individual_field", 30)
            logger.info("select individual datetime field - SUCCESS")

            // Test 3: Multiple fields including datetime should work
            executeWithRetry("""select test1, test6, test7, test8 from test1 where test1='string1' limit 1""", "datetime_multiple_fields", 30)
            logger.info("select multiple datetime fields - SUCCESS")

            // Test 4: Test existing timezone formats (backward compatibility)
            try {
                executeWithRetry("""select test1, test8, test10 from test1 where test1='string1' limit 1""", "datetime_timezone_z_format", 30)
                logger.info("select datetime fields with Z timezone format - SUCCESS")
            } catch (Exception e) {
                if (e.getMessage().contains("Expected value of type") && e.getMessage().contains("DATETIMEV2") && e.getMessage().contains("Varchar/Char")) {
                    throw new RuntimeException("BUG NOT FIXED: Z timezone format parsing still fails: " + e.getMessage())
                } else {
                    throw e
                }
            }

            // Test 5: Test specific timezone formats that the fix addresses
            try {
                executeWithRetry("""select test1, test11, test12 from test1 where test1='string1' limit 1""", "datetime_timezone_no_colon_format", 30)
                logger.info("select datetime fields with timezone format without colon - SUCCESS")
            } catch (Exception e) {
                if (e.getMessage().contains("Expected value of type") && e.getMessage().contains("DATETIMEV2") && e.getMessage().contains("Varchar/Char")) {
                    throw new RuntimeException("BUG NOT FIXED: Timezone format without colon parsing still fails: " + e.getMessage())
                } else {
                    throw e
                }
            }
            // Test 6: Comprehensive select * test to ensure all datetime fields work together
            try {
                executeWithRetry("""select * from test1 limit 1""", "datetime_comprehensive_select_all", 30)
                logger.info("Comprehensive select * test - SUCCESS")
            } catch (Exception e) {
                if (e.getMessage().contains("Expected value of type") && e.getMessage().contains("DATETIMEV2") && e.getMessage().contains("Varchar/Char")) {
                    throw new RuntimeException("BUG NOT FIXED: Comprehensive select * still fails with datetime parsing: " + e.getMessage())
                } else {
                    throw e
                }
            }


        }
        test_datetime_timezone_format_fix("test_es_datetime_fix_7")
        test_datetime_timezone_format_fix("test_es_datetime_fix_8")

        sql """drop catalog if exists test_es_datetime_fix_7;"""
        sql """drop catalog if exists test_es_datetime_fix_8;"""
    }
}
