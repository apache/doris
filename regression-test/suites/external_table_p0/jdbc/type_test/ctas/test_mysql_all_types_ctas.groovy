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

suite("test_mysql_all_types_ctas", "p0,external,mysql,external_docker,external_docker_mysql") {
    String enabled = context.config.otherConfigs.get("enableJdbcTest")
    String externalEnvIp = context.config.otherConfigs.get("externalEnvIp")
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"
    if (enabled != null && enabled.equalsIgnoreCase("true")) {
        String mysql_port = context.config.otherConfigs.get("mysql_57_port");

        sql """drop database if exists test_mysql_all_types_ctas;"""

        sql """create database if not exists test_mysql_all_types_ctas;"""

        sql """drop catalog if exists mysql_all_type_ctas_test """
        sql """create catalog if not exists mysql_all_type_ctas_test properties(
            "type"="jdbc",
            "user"="root",
            "password"="123456",
            "jdbc_url" = "jdbc:mysql://${externalEnvIp}:${mysql_port}/doris_test?useSSL=false",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver"
        );"""

        sql """use internal.test_mysql_all_types_ctas;"""

        sql """create table all_types_nullable properties("replication_num" = "1") as select * from mysql_all_type_ctas_test.doris_test.all_types_nullable;"""

        qt_select_all_types_nullable """select * from internal.test_mysql_all_types_ctas.all_types_nullable order by 1;"""

        sql """create table all_types_non_nullable properties("replication_num" = "1") as select * from mysql_all_type_ctas_test.doris_test.all_types_non_nullable;"""

        qt_select_all_types_non_nullable """select * from internal.test_mysql_all_types_ctas.all_types_non_nullable order by 1;"""

        sql """create table t_varchar properties("replication_num" = "1") as select * from mysql_all_type_ctas_test.doris_test.t_varchar;"""

        qt_select_varchar """select * from internal.test_mysql_all_types_ctas.t_varchar order by 1;"""

        sql """create table t_char properties("replication_num" = "1") as select * from mysql_all_type_ctas_test.doris_test.t_char;"""

        qt_select_char """select * from internal.test_mysql_all_types_ctas.t_char order by 1;"""

        sql """drop database if exists test_mysql_all_types_ctas;"""
    }
}
