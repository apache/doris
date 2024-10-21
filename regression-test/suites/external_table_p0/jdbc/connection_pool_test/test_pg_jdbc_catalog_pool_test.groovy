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

suite("test_pg_jdbc_catalog_pool_test", "p0,external,pg,external_docker,external_docker_pg") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/postgresql-42.5.0.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ex_schema_name = "catalog_pg_test";
        String pg_port = context.config.otherConfigs.get("pg_14_port");
        String test_all_types = "test_all_types";

        def poolOptions = [true, false]

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "pool_true" : "pool_false"
            String catalog_name = "pg_jdbc_catalog_${poolState}";

            sql """drop catalog if exists ${catalog_name} """

            sql """create catalog if not exists ${catalog_name} properties(
                "type"="jdbc",
                "user"="postgres",
                "password"="123456",
                "jdbc_url" = "jdbc:postgresql://${externalEnvIp}:${pg_port}/postgres?currentSchema=doris_test&useSSL=false",
                "driver_url" = "${driver_url}",
                "driver_class" = "org.postgresql.Driver",
                "enable_connection_pool" = "${poolEnabled}"
            );"""

            def tasks = (1..5).collect {
                Thread.start {
                    sql """switch ${catalog_name}"""
                    sql """ use ${ex_schema_name}"""
                    order_qt_select_all_types """select * from ${test_all_types}; """
                }
            }
            tasks*.join()

            sql """refresh catalog ${catalog_name}"""

            def refreshTasks = (1..5).collect {
                Thread.start {
                    sql """switch ${catalog_name}"""
                    sql """ use ${ex_schema_name}"""
                    order_qt_select_all_types_refresh """select * from ${test_all_types}; """
                }
            }
            refreshTasks*.join()

            sql """ drop catalog if exists ${catalog_name} """
        }
    }
}
