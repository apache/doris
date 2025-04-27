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

suite("test_sqlserver_all_types_select", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port");

        sql """drop catalog if exists sqlserver_all_type_test """
        sql """create catalog if not exists sqlserver_all_type_test properties(
                    "type"="jdbc",
                    "user"="sa",
                    "password"="Doris123456",
                    "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};encrypt=false;databaseName=doris_test;",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        );"""

        sql """use sqlserver_all_type_test.dbo"""

        qt_desc_all_types_null """desc dbo.extreme_test;"""

        qt_select_all_types_null """select * from dbo.extreme_test order by 1;"""

        qt_select_all_types_multi_block """select count(*) from dbo.extreme_test_multi_block;"""

        sql """drop catalog if exists sqlserver_all_type_test """
    }
}
