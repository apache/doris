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

suite("test_lower_case_meta_show_and_select", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_lower_without_conf_user"
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

    sql """drop database if exists internal.external_test_lower; """
    sql """drop database if exists internal.external_test_UPPER; """
    sql """create database if not exists internal.external_test_lower; """
    sql """create database if not exists internal.external_test_UPPER; """
    sql """create table if not exists internal.external_test_lower.lower
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_lower.UPPER
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_test_lower.lower values(1, 'lower')"""
    sql """insert into internal.external_test_lower.UPPER values(1, 'UPPER')"""

    sql """create table if not exists internal.external_test_lower.lower_insert
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.external_test_lower.UPPER_insert
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    // Test for cache false and lower false
    sql """drop catalog if exists test_cache_false_lower_false """

    sql """ CREATE CATALOG `test_cache_false_lower_false` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower,external_test_UPPER"
        )"""

    wait_table_sync("test_cache_false_lower_false.external_test_lower")
    test {
        sql """show databases from test_cache_false_lower_false"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_test_lower", "external_test_UPPER"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_cache_false_lower_false.external_test_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "UPPER"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_cache_false_lower_false1 "select * from test_cache_false_lower_false.external_test_lower.lower"
    qt_sql_test_cache_false_lower_false2 "select * from test_cache_false_lower_false.external_test_lower.UPPER"

    qt_sql_test_cache_false_lower_false1_insert "insert into internal.external_test_lower.lower_insert select * from test_cache_false_lower_false.external_test_lower.lower"
    qt_sql_test_cache_false_lower_false2_insert "insert into internal.external_test_lower.UPPER_insert select * from test_cache_false_lower_false.external_test_lower.UPPER"

    sql """drop catalog if exists test_cache_false_lower_false """

    // Test for cache true and lower false
    sql """drop catalog if exists test_cache_true_lower_false """

    sql """ CREATE CATALOG `test_cache_true_lower_false` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "false",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower,external_test_UPPER"
        )"""

    test {
        sql """show databases from test_cache_true_lower_false"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_test_lower", "external_test_UPPER"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_cache_true_lower_false.external_test_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "UPPER"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_cache_false_lower_false1 "select * from test_cache_true_lower_false.external_test_lower.lower"
    qt_sql_test_cache_false_lower_false2 "select * from test_cache_true_lower_false.external_test_lower.UPPER"

    qt_sql_test_cache_false_lower_false1_insert "insert into internal.external_test_lower.lower_insert select * from test_cache_true_lower_false.external_test_lower.lower"
    qt_sql_test_cache_false_lower_false2_insert "insert into internal.external_test_lower.UPPER_insert select * from test_cache_true_lower_false.external_test_lower.UPPER"


    sql """drop catalog if exists test_cache_true_lower_false """

    // Test for cache false and lower true
    sql """drop catalog if exists test_cache_false_lower_true """

    sql """ CREATE CATALOG `test_cache_false_lower_true` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower,external_test_UPPER"
        )"""

    wait_table_sync("test_cache_false_lower_true.external_test_lower")
    test {
        sql """show databases from test_cache_false_lower_true"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_test_lower", "external_test_upper"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_cache_false_lower_true.external_test_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_cache_false_lower_true1 "select * from test_cache_false_lower_true.external_test_lower.lower"
    qt_sql_test_cache_false_lower_true2 "select * from test_cache_false_lower_true.external_test_lower.upper"

    qt_sql_test_cache_false_lower_true1_insert "insert into internal.external_test_lower.lower_insert select * from test_cache_false_lower_true.external_test_lower.lower"
    qt_sql_test_cache_false_lower_true2_insert "insert into internal.external_test_lower.UPPER_insert select * from test_cache_false_lower_true.external_test_lower.upper"

    sql """drop catalog if exists test_cache_false_lower_true """

    // Test for cache true and lower true
    sql """drop catalog if exists test_cache_true_lower_true """

    sql """ CREATE CATALOG `test_cache_true_lower_true` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "true",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_test_lower,external_test_UPPER"
        )"""

    test {
        sql """show databases from test_cache_true_lower_true"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_test_lower", "external_test_upper"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_cache_true_lower_true.external_test_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_cache_true_lower_true1 "select * from test_cache_true_lower_true.external_test_lower.lower"
    qt_sql_test_cache_true_lower_true2 "select * from test_cache_true_lower_true.external_test_lower.upper"

    qt_sql_test_cache_true_lower_true1_insert "insert into internal.external_test_lower.lower_insert select * from test_cache_true_lower_true.external_test_lower.lower"
    qt_sql_test_cache_true_lower_true2_insert "insert into internal.external_test_lower.UPPER_insert select * from test_cache_true_lower_true.external_test_lower.upper"

    sql """drop catalog if exists test_cache_true_lower_true """


    sql """drop database if exists internal.external_test_lower; """
    sql """drop database if exists internal.external_test_UPPER; """

    try_sql """drop user ${jdbcUser}"""
}
