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

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_lower_case_meta_with_lower_table_conf_show_and_select", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_lower_with_conf"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    def wait_table_sync = { String db ->
        Awaitility.await().atMost(10, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS).until{
            try {
                def res = sql "show tables from ${db}"
                return res.size() > 0;
            } catch (Exception e) {
                e.printStackTrace();
                return false;
            }
        }
    }

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    sql """drop database if exists internal.external_test_lower_with_conf; """
    sql """create database if not exists internal.external_test_lower_with_conf; """
    sql """create table if not exists internal.external_test_lower_with_conf.lower_with_conf
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_lower_with_conf.UPPER_with_conf
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_test_lower_with_conf.lower_with_conf values(1, 'lower')"""
    sql """insert into internal.external_test_lower_with_conf.UPPER_with_conf values(1, 'UPPER')"""

    sql """create table if not exists internal.external_test_lower_with_conf.with_conf_insert
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    // Test for cache false and lower false and lower conf 1
    sql """drop catalog if exists test_cache_false_lower_false_with_conf1 """

    sql """ CREATE CATALOG `test_cache_false_lower_false_with_conf1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "1"
        )"""

    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_1 "select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_2 "select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_3 "select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_4 "select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf1"
    wait_table_sync("test_cache_false_lower_false_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf1_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf.upper_with_conf"


    test {
        sql """show tables from test_cache_false_lower_false_with_conf1.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_false_with_conf1 """

    // Test for cache false and lower false and lower conf 2
    sql """drop catalog if exists test_cache_false_lower_false_with_conf2 """

    sql """ CREATE CATALOG `test_cache_false_lower_false_with_conf2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "2"
        )"""

    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_1 "select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_2 "select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_3 "select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_4 "select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf2"
    wait_table_sync("test_cache_false_lower_false_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf2_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_false_lower_false_with_conf2.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "UPPER_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_false_with_conf2 """



    // Test for cache true and lower false and lower conf 1
    sql """drop catalog if exists test_cache_true_lower_false_with_conf1 """

    sql """ CREATE CATALOG `test_cache_true_lower_false_with_conf1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "1"
        )"""

    qt_sql_test_cache_true_lower_false_with_conf1_1 "select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_2 "select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_3 "select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_4 "select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf1"
    qt_sql_test_cache_true_lower_false_with_conf1_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_true_lower_false_with_conf1.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_false_with_conf1 """

    // Test for cache true and lower false and lower conf 2
    sql """drop catalog if exists test_cache_true_lower_false_with_conf2 """

    sql """ CREATE CATALOG `test_cache_true_lower_false_with_conf2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "2"
        )"""

    qt_sql_test_cache_true_lower_false_with_conf2_1 "select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_2 "select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_3 "select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_4 "select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf2"
    qt_sql_test_cache_true_lower_false_with_conf2_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_true_lower_false_with_conf2.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "UPPER_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_false_with_conf2 """

    // Test for cache false and lower true and lower conf 1
    sql """drop catalog if exists test_cache_false_lower_true_with_conf1 """

    sql """ CREATE CATALOG `test_cache_false_lower_true_with_conf1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "1"
        )"""

    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_1 "select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_2 "select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_3 "select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_4 "select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf1"
    wait_table_sync("test_cache_false_lower_true_with_conf1.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf1_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_false_lower_true_with_conf1.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_true_with_conf1 """

    // Test for cache false and lower true and lower conf 2
    sql """drop catalog if exists test_cache_false_lower_true_with_conf2 """

    sql """ CREATE CATALOG `test_cache_false_lower_true_with_conf2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "2"
        )"""

    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_1 "select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_2 "select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_3 "select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_4 "select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf2"
    wait_table_sync("test_cache_false_lower_true_with_conf2.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf2_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_false_lower_true_with_conf2.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_true_with_conf2 """


    // Test for cache true and lower true and lower conf 1
    sql """drop catalog if exists test_cache_true_lower_true_with_conf1 """

    sql """ CREATE CATALOG `test_cache_true_lower_true_with_conf1` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "1"
        )"""

    qt_sql_test_cache_true_lower_true_with_conf1_1 "select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_2 "select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_3 "select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_4 "select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf1"
    qt_sql_test_cache_true_lower_true_with_conf1_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_true_lower_true_with_conf1.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_true_with_conf1 """

    // Test for cache true and lower true and lower conf 2
    sql """drop catalog if exists test_cache_true_lower_true_with_conf2 """

    sql """ CREATE CATALOG `test_cache_true_lower_true_with_conf2` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "2"
        )"""

    qt_sql_test_cache_true_lower_true_with_conf2_1 "select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_2 "select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_3 "select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_4 "select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.LOWER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_3_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.UPPER_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf2"
    qt_sql_test_cache_true_lower_true_with_conf2_4_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf.upper_with_conf"

    test {
        sql """show tables from test_cache_true_lower_true_with_conf2.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_true_with_conf2 """


    // Test for cache false and lower false and lower conf 0
    sql """drop catalog if exists test_cache_false_lower_false_with_conf0 """

    sql """ CREATE CATALOG `test_cache_false_lower_false_with_conf0` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "0"
        )"""

    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf0_1 "select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf0_2 "select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.UPPER_with_conf"

    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf0_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_false_with_conf0_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.UPPER_with_conf"

    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    test {
        sql "select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    test {
        sql "select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.upper_with_conf"
        exception "Unknown table 'upper_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_false_with_conf0"
    wait_table_sync("test_cache_false_lower_false_with_conf0.external_test_lower_with_conf");
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf.upper_with_conf"
        exception "Unknown table 'upper_with_conf'"
    }

    test {
        sql """show tables from test_cache_false_lower_false_with_conf0.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "UPPER_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_false_with_conf0 """


    // Test for cache true and lower false and lower conf 0
    sql """drop catalog if exists test_cache_true_lower_false_with_conf0 """

    sql """ CREATE CATALOG `test_cache_true_lower_false_with_conf0` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "0"
        )"""

    qt_sql_test_cache_true_lower_false_with_conf0_1 "select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    qt_sql_test_cache_true_lower_false_with_conf0_2 "select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.UPPER_with_conf"

    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    qt_sql_test_cache_true_lower_false_with_conf0_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    qt_sql_test_cache_true_lower_false_with_conf0_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.UPPER_with_conf"


    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    test {
        sql "select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    test {
        sql "select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.upper_with_conf"
        exception "Unknown table 'upper_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_false_with_conf0"
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf.upper_with_conf"
        exception "Unknown table 'upper_with_conf'"
    }

    test {
        sql """show tables from test_cache_true_lower_false_with_conf0.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "UPPER_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_false_with_conf0 """


    // Test for cache false and lower true and lower conf 0
    sql """drop catalog if exists test_cache_false_lower_true_with_conf0 """

    sql """ CREATE CATALOG `test_cache_false_lower_true_with_conf0` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "0"
        )"""

    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf0_1 "select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf0_2 "select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf0_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    qt_sql_test_cache_false_lower_true_with_conf0_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    test {
        sql "select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    test {
        sql "select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.UPPER_with_conf"
        exception "Unknown table 'UPPER_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_false_lower_true_with_conf0"
    wait_table_sync("test_cache_false_lower_true_with_conf0.external_test_lower_with_conf");
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf.UPPER_with_conf"
        exception "Unknown table 'UPPER_with_conf'"
    }

    test {
        sql """show tables from test_cache_false_lower_true_with_conf0.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_false_lower_true_with_conf0 """


    // Test for cache true and lower true and lower conf 0
    sql """drop catalog if exists test_cache_true_lower_true_with_conf0 """

    sql """ CREATE CATALOG `test_cache_true_lower_true_with_conf0` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower_with_conf",
            "only_test_lower_case_table_names" = "0"
        )"""

    qt_sql_test_cache_true_lower_true_with_conf0_1 "select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    qt_sql_test_cache_true_lower_true_with_conf0_2 "select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.upper_with_conf"

    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    qt_sql_test_cache_true_lower_true_with_conf0_1_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.lower_with_conf"
    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    qt_sql_test_cache_true_lower_true_with_conf0_2_insert "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.upper_with_conf"


    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    test {
        sql "select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    test {
        sql "select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.UPPER_with_conf"
        exception "Unknown table 'UPPER_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.Lower_with_conf"
        exception "Unknown table 'Lower_with_conf'"
    }

    sql "refresh catalog test_cache_true_lower_true_with_conf0"
    test {
        sql "insert into internal.external_test_lower_with_conf.with_conf_insert select * from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf.UPPER_with_conf"
        exception "Unknown table 'UPPER_with_conf'"
    }

    test {
        sql """show tables from test_cache_true_lower_true_with_conf0.external_test_lower_with_conf"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower_with_conf", "upper_with_conf"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_cache_true_lower_true_with_conf0 """

    sql """drop database if exists internal.external_test_lower_with_conf; """

    try_sql """drop user ${jdbcUser}"""
}
