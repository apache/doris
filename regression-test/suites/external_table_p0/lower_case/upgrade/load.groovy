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

suite("test_upgrade_lower_case_catalog_prepare", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_upgrade_lower_case_catalog_user"
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

    sql """drop database if exists internal.upgrade_lower_case_catalog_lower; """
    sql """drop database if exists internal.upgrade_lower_case_catalog_UPPER; """
    sql """create database if not exists internal.upgrade_lower_case_catalog_lower; """
    sql """create database if not exists internal.upgrade_lower_case_catalog_UPPER; """
    sql """create table if not exists internal.upgrade_lower_case_catalog_lower.lower
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """create table if not exists internal.upgrade_lower_case_catalog_lower.UPPER
         (id int, name varchar(20))
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.upgrade_lower_case_catalog_lower.lower values(1, 'lower')"""
    sql """insert into internal.upgrade_lower_case_catalog_lower.UPPER values(1, 'UPPER')"""

    // Test for cache false and lower false
    sql """drop catalog if exists test_upgrade_lower_case_catalog """

    sql """ CREATE CATALOG `test_upgrade_lower_case_catalog` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "use_meta_cache" = "false",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "upgrade_lower_case_catalog_lower,upgrade_lower_case_catalog_UPPER"
        )"""

    wait_table_sync("test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower")
    test {
        sql """show databases from test_upgrade_lower_case_catalog"""

        // Verification results include external_test_lower and external_test_UPPER
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["upgrade_lower_case_catalog_lower", "upgrade_lower_case_catalog_upper"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower"""

        // Verification results include lower and UPPER
        check { result, ex, startTime, endTime ->
            def expectedTables = ["lower", "upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_test_upgrade_lower_case_catalog_1 "select * from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower.lower"
    qt_sql_test_upgrade_lower_case_catalog_2 "select * from test_upgrade_lower_case_catalog.upgrade_lower_case_catalog_lower.upper"

}
