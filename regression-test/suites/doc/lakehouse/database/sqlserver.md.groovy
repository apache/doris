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

import org.junit.jupiter.api.Assertions;

suite("docs/lakehouse/database/sqlserver.md", "p0,external,sqlserver,external_docker,external_docker_sqlserver") {
    try {
        String enable = context.config.otherConfigs.get("enableJdbcTest")
        if(enable == null || !enable.equalsIgnoreCase("true")) {
            return
        }

        def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def sqlserver_port = context.config.otherConfigs.get("sqlserver_2022_port")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "http://${bucket}.${s3_endpoint}/regression/jdbc_driver/mssql-jdbc-11.2.3.jre8.jar"
        String driver_class = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
        String databaseName = "doris_test"

        sql """ DROP CATALOG IF EXISTS sqlserver; """
        sql """
            CREATE CATALOG sqlserver PROPERTIES (
                "type"="jdbc",
                "user"="sa",
                "password"="Doris123456",
                "jdbc_url" = "jdbc:sqlserver://${externalEnvIp}:${sqlserver_port};databaseName=${databaseName};encrypt=false",
                "driver_url" = "${driver_url}",
                "driver_class" = "${driver_class}"
            )
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/lakehouse/database/sqlserver.md failed to exec, please fix it", t)
    }
}
