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

suite("oracle.md", "p0,external,oracle,external_docker,external_docker_oracle") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String catalog_name = "oracle_catalog_md";
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
        String SID = "XE";
        String s3_endpoint = getS3Endpoint()
        String bucket = getS3BucketName()
        String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"

        sql """drop catalog if exists ${catalog_name} """

        sql """create catalog if not exists ${catalog_name} properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """drop catalog if exists ${catalog_name} """
    }
}
