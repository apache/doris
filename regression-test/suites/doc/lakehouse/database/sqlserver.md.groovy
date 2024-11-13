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

suite("docs/lakehouse/database/sqlserver.md") {
    try {
        String enable = context.config.otherConfigs.get("enableJdbcTest")
        if(enable == null || !enable.equalsIgnoreCase("true")) {
            return
        }

        def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def oracle_port = context.config.otherConfigs.get("oracle_11_port")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "http://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"
        String driver_class = "oracle.jdbc.driver.OracleDriver"
        String sid = "XE"

        sql """ DROP CATALOG IF EXISTS oracle; """
        sql """
            CREATE CATALOG oracle PROPERTIES (
                "type"="jdbc",
                "user"="doris_test",
                "password"="123456",
                "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${sid}",
                "driver_url" = "${driver_url}",
                "driver_class" = "${driver_class}"
            )
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/lakehouse/database/sqlserver.md failed to exec, please fix it", t)
    }
}
