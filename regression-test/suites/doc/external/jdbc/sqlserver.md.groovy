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

suite("sqlserver.md", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "sqlserver_catalog_md";
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"

        sql """ drop catalog if exists ${catalog_name} """

        sql """ create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};databaseName=doris_test;encrypt=false;",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""

        sql """drop catalog if exists ${catalog_name} """
    }
}
