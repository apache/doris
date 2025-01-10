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

suite("test_conflict_name", "p0,external,doris,meta_names_mapping") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_conflict_name_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    sql """drop database if exists internal.external_conflict_name; """
    sql """drop database if exists internal.EXTERNAL_CONFLICT_NAME; """
    sql """create database if not exists internal.external_conflict_name; """
    sql """create database if not exists internal.EXTERNAL_CONFLICT_NAME; """
    sql """create table if not exists internal.external_conflict_name.table_test
         (id int, name varchar(20), column_test int)
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """drop catalog if exists test_conflict_name """
    sql """ CREATE CATALOG `test_conflict_name` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_conflict_name,EXTERNAL_CONFLICT_NAME"
        )"""

    test {
        sql """show databases from test_conflict_name"""
        exception """Found conflicting database names under case-insensitive conditions. Conflicting remote database names: EXTERNAL_CONFLICT_NAME, external_conflict_name in catalog test_conflict_name. Please use meta_names_mapping to handle name mapping."""
    }

    sql """refresh catalog test_conflict_name"""

    test {
        sql """select * from test_conflict_name.external_conflict_name.table_test"""
        exception """Found conflicting database names under case-insensitive conditions. Conflicting remote database names: EXTERNAL_CONFLICT_NAME, external_conflict_name in catalog test_conflict_name. Please use meta_names_mapping to handle name mapping."""
    }

    sql """drop database if exists internal.EXTERNAL_CONFLICT_NAME; """

    sql """refresh catalog test_conflict_name"""

    qt_select_external_conflict_name_db "select * from test_conflict_name.external_conflict_name.table_test"

    sql """create table if not exists internal.external_conflict_name.TABLE_TEST
         (id int, name varchar(20), column_test int)
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """refresh catalog test_conflict_name"""

    test {
        sql """show tables from test_conflict_name.external_conflict_name"""
        exception """Found conflicting table names under case-insensitive conditions. Conflicting remote table names: TABLE_TEST, table_test in remote database 'external_conflict_name' under catalog 'test_conflict_name'. Please use meta_names_mapping to handle name mapping."""
    }

    sql """refresh catalog test_conflict_name"""

    test {
        sql """select * from test_conflict_name.external_conflict_name.TABLE_TEST"""
        exception """Found conflicting table names under case-insensitive conditions. Conflicting remote table names: TABLE_TEST, table_test in remote database 'external_conflict_name' under catalog 'test_conflict_name'. Please use meta_names_mapping to handle name mapping."""
    }

    sql """drop table if exists internal.external_conflict_name.TABLE_TEST; """

    qt_select_external_conflict_name_tbl "select * from test_conflict_name.external_conflict_name.table_test"

    sql """drop database if exists internal.external_conflict_name; """
    sql """drop database if exists internal.EXTERNAL_CONFLICT_NAME; """

    try_sql """drop user ${jdbcUser}"""
}