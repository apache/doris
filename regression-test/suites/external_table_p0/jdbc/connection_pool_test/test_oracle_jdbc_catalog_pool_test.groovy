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

suite("test_oracle_jdbc_catalog_pool_test", "p0,external,oracle,external_docker,external_docker_oracle") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"
    String driver6_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc6.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ex_db_name = "DORIS_TEST";
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";
        String test_all_types = "TEST_ALL_TYPES";

        def poolOptions = [true, false]

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "pool_true" : "pool_false"
            String catalog_name = "oracle_catalog_${poolState}";

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                        "type"="jdbc",
                        "user"="doris_test",
                        "password"="123456",
                        "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "oracle.jdbc.driver.OracleDriver",
                        "enable_connection_pool" = "${poolEnabled}"
            );"""

            def tasks = (1..5).collect {
                Thread.start {
                    sql """switch ${catalog_name}"""
                    sql """ use ${ex_db_name}"""
                    order_qt_select_all_types """select * from ${test_all_types}; """
                }
            }
            tasks*.join()

            sql """refresh catalog ${catalog_name}"""

            def refreshTasks = (1..5).collect {
                Thread.start {
                    sql """switch ${catalog_name}"""
                    sql """ use ${ex_db_name}"""
                    order_qt_select_all_types_refresh """select * from ${test_all_types}; """
                }
            }
            refreshTasks*.join()

            sql """drop catalog if exists ${catalog_name} """
        }

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "pool_true" : "pool_false"
            String catalog_name = "oracle_ojdbc6_${poolState}";

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                        "type"="jdbc",
                        "user"="doris_test",
                        "password"="123456",
                        "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                        "driver_url" = "${driver6_url}",
                        "driver_class" = "oracle.jdbc.OracleDriver",
                        "enable_connection_pool" = "${poolEnabled}"
            );"""


            def tasks6 = (1..5).collect {
                Thread.start {
                    sql """ use ${catalog_name}.DORIS_TEST; """
                    qt_query_ojdbc6_all_types """ select * from ${catalog_name}.DORIS_TEST.TEST_ALL_TYPES order by 1; """
                }
            }
            tasks6*.join()

            sql """refresh catalog ${catalog_name};"""

            def refreshTasks6 = (1..5).collect {
                Thread.start {
                    sql """ use ${catalog_name}.DORIS_TEST; """
                    qt_query_ojdbc6_all_types_refresh """ select * from ${catalog_name}.DORIS_TEST.TEST_ALL_TYPES order by 1; """
                }
            }
            refreshTasks6*.join()

            sql """drop catalog if exists ${catalog_name}; """
        }
    }
}
