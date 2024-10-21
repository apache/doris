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

suite("test_clickhouse_jdbc_catalog_pool_test", "p0,external,clickhouse,external_docker,external_docker_clickhouse") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ex_db_name = "doris_test";
        String clickhouse_port = context.config.otherConfigs.get("clickhouse_22_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/clickhouse-jdbc-0.4.2-all.jar"

        def poolOptions = [true, false]

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "true" : "false"
            String catalog_name = "clickhouse_catalog_pool_${poolState}";

            sql """ drop catalog if exists ${catalog_name} """
            sql """ create catalog if not exists ${catalog_name} properties(
                        "type"="jdbc",
                        "user"="default",
                        "password"="123456",
                        "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver",
                        "enable_connection_pool" = "${poolState}"
            );"""

            def tasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use ${ex_db_name} """
                    order_qt_type  """ select * from type order by k1; """
                }
            }

            tasks*.join()

            sql """refresh catalog ${catalog_name}"""

            def refreshTasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use ${ex_db_name} """
                    order_qt_type_refresh """ select * from type order by k1; """
                }
            }

            refreshTasks*.join()

            sql """ drop catalog if exists ${catalog_name} """
        }
    }
}

