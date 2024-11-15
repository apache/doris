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

suite("docs/lakehouse/database/oceanbase.md", "p0,external,oceanbase,external_docker,external_docker_oceanbase") {
    try {
        String enable = context.config.otherConfigs.get("enableJdbcTest")
        if(enable == null || !enable.equalsIgnoreCase("true")) {
            return
        }

        def externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        def ob_port = context.config.otherConfigs.get("oceanbase_port")
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "http://${bucket}.${s3_endpoint}/regression/jdbc_driver/oceanbase-client-2.4.8.jar"
        String driver_class = "com.oceanbase.jdbc.Driver"
        String database = "doris_test"

        sql """ DROP CATALOG IF EXISTS oceanbase; """
        sql """
            CREATE CATALOG oceanbase PROPERTIES (
                "type"="jdbc",
                "user"="root@test",
                "password"="",
                "jdbc_url" = "jdbc:oceanbase://${externalEnvIp}:${ob_port}/${database}",
                "driver_url" = "${driver_url}",
                "driver_class" = "${driver_class}"
            )
        """
    } catch (Throwable t) {
        Assertions.fail("examples in docs/lakehouse/database/oceanbase.md failed to exec, please fix it", t)
    }
}
