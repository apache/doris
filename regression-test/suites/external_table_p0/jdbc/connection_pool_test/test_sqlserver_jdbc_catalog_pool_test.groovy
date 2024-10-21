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

suite("test_sqlserver_jdbc_catalog_pool_test", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest");
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"

    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String ex_db_name = "dbo";
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");

        def poolOptions = [true, false]

        poolOptions.each { poolEnabled ->
            String poolState = poolEnabled ? "pool_true" : "pool_false"
            String catalog_name = "sqlserver_catalog_${poolState}";

            sql """ drop catalog if exists ${catalog_name} """

            sql """ create catalog if not exists ${catalog_name} properties(
                        "type"="jdbc",
                        "user"="sa",
                        "password"="Doris123456",
                        "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                        "driver_url" = "${driver_url}",
                        "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        "enable_connection_pool" = "${poolEnabled}"
            );"""

            def tasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use ${ex_db_name} """
                    order_qt_all_type """ select * from all_type order by id; """
                }
            }
            tasks*.join()

            sql """refresh catalog ${catalog_name}"""

            def refreshTasks = (1..5).collect {
                Thread.start {
                    sql """ switch ${catalog_name} """
                    sql """ use ${ex_db_name} """
                    order_qt_all_type_refresh """ select * from all_type order by id; """
                }
            }
            refreshTasks*.join()

            sql """ drop catalog if exists ${catalog_name} """
        }
    }
}
