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

suite("test_mysql_all_types_select", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop catalog if exists mysql_all_type_test """
        sql """create catalog if not exists mysql_all_type_test properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use mysql_all_type_test.doris_test"""

        qt_desc_all_types_null """desc all_types_nullable;"""
        qt_select_all_types_null """select * from all_types_nullable order by 1;"""

        qt_desc_all_types_non_null """desc all_types_non_nullable;"""
        qt_select_all_types_non_null """select * from all_types_non_nullable order by 1;"""

        qt_select_varchar """select * from t_varchar order by 1;"""
        qt_select_char """select * from t_char order by 1;"""

        qt_select_all_types_multi_block """select count(`int`),count(`varchar`) from all_types_multi_block;"""

        sql """drop catalog if exists mysql_all_type_test """
    }
}
