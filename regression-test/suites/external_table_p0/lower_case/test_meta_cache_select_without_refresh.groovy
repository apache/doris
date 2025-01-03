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

suite("test_meta_cache_select_without_refresh", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_meta_cache_select_without_refresh_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    sql """ drop database if exists internal.external_lower_select_without_refresh; """
    sql """create database if not exists internal.external_lower_select_without_refresh;"""

    // Test include
    sql """drop catalog if exists test_meta_cache_lower_true_select_without_refresh """

    sql """ CREATE CATALOG `test_meta_cache_lower_true_select_without_refresh` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_lower_select_without_refresh"
        )"""

    sql """drop catalog if exists test_meta_cache_lower_false_select_without_refresh """

    sql """ CREATE CATALOG `test_meta_cache_lower_false_select_without_refresh` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_lower_select_without_refresh"
        )"""

    sql """create table if not exists internal.external_lower_select_without_refresh.table1
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_lower_select_without_refresh.table1 values(1, 'table1')"""

    qt_test_meta_cache_lower_true_select_without_refresh_select_table1 "select * from test_meta_cache_lower_true_select_without_refresh.external_lower_select_without_refresh.table1;"

    qt_test_meta_cache_lower_false_select_without_refresh_select_table1 "select * from test_meta_cache_lower_false_select_without_refresh.external_lower_select_without_refresh.table1;"

    sql """create table if not exists internal.external_lower_select_without_refresh.TABLE2
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_lower_select_without_refresh.TABLE2 values(1, 'TABLE2')"""

    test {
        sql """select * from test_meta_cache_lower_true_select_without_refresh.external_lower_select_without_refresh.table2;"""

        exception "Unknown table 'table2'"
    }

    qt_test_meta_cache_lower_false_select_without_refresh_select_table2 "select * from test_meta_cache_lower_false_select_without_refresh.external_lower_select_without_refresh.TABLE2;"

    sql """drop catalog if exists test_meta_cache_lower_true_select_without_refresh """
    sql """drop catalog if exists test_meta_cache_lower_false_select_without_refresh """
    sql """drop database if exists internal.external_lower_select_without_refresh; """

    try_sql """drop user ${jdbcUser}"""
}
