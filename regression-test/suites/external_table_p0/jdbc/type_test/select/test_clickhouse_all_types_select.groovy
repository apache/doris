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

suite("test_clickhouse_all_types_select", "p0,external,clickhouse,external_docker,external_docker_clickhouse") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/clickhouse-jdbc-0.7.1-patch1-all.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String clickhouse_port = context.config.otherConfigs.get("clickhouse_22_port");

        sql """drop catalog if exists clickhouse_all_type_test """
        sql """create catalog if not exists clickhouse_all_type_test properties(
                    "type"="jdbc",
                    "user"="default",
                    "password"="123456",
                    "jdbc_url" = "jdbc:clickhouse://${externalEnvIp}:${clickhouse_port}/doris_test",
                    "driver_url" = "${driver_url}",
                    "driver_class" = "com.clickhouse.jdbc.ClickHouseDriver"
        );"""

        sql """use clickhouse_all_type_test.doris_test"""

        qt_desc_all_types_null """desc doris_test.extreme_test;"""
        qt_select_all_types_null """select * from doris_test.extreme_test order by 1;"""

        qt_select_all_types_multi_block """select count(*) from doris_test.extreme_test_multi_block;"""

        sql """drop catalog if exists clickhouse_all_type_test """
    }
}
