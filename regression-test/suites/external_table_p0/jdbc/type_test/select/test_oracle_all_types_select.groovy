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

suite("test_oracle_all_types_select", "p0,external,oracle,external_docker,external_docker_oracle") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/ojdbc8.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String oracle_port = context.config.otherConfigs.get("oracle_11_port");
        String SID = "XE";

        sql """drop catalog if exists oracle_all_type_test """
        sql """create catalog if not exists oracle_all_type_test properties(
                    "type"="jdbc",
                    "user"="doris_test",
                    "password"="123456",
                    "jdbc_url" = "jdbc:oracle:thin:@${externalEnvIp}:${oracle_port}:${SID}",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "oracle.jdbc.driver.OracleDriver"
        );"""

        sql """use oracle_all_type_test.DORIS_TEST"""

        qt_desc_all_types_null """desc DORIS_TEST.EXTREME_TEST;"""
        qt_select_all_types_null """select * from DORIS_TEST.EXTREME_TEST order by 1;"""

        qt_select_all_types_multi_block """select count(*) from DORIS_TEST.EXTREME_TEST_MULTI_BLOCK;"""

        sql """drop catalog if exists oracle_all_type_test """
    }
}
