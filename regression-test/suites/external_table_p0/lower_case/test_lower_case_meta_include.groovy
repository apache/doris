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

suite("test_lower_case_meta_include", "p0,external,doris,external_docker,external_docker_doris") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_lower_include_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    String mapping_db = """
    {
        "databases": [
            {"remoteDatabase": "external_INCLUDE", "mapping": "external_include_test"},
            {"remoteDatabase": "external_EXCLUDE", "mapping": "external_exclude_test"}
        ]
    }
    """

    sql """drop database if exists internal.external_INCLUDE; """
    sql """drop database if exists internal.external_EXCLUDE; """
    sql """create database if not exists internal.external_INCLUDE; """
    sql """create database if not exists internal.external_EXCLUDE; """

    // Test include
    sql """drop catalog if exists test_lower_case_include """

    sql """ CREATE CATALOG `test_lower_case_include` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_INCLUDE"
        )"""

    test {
        sql """show databases from test_lower_case_include"""

        rowNum 3

        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_include"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_lower_case_include """

    // Test exclude
    sql """drop catalog if exists test_lower_case_exclude """

    sql """ CREATE CATALOG `test_lower_case_exclude` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "exclude_database_list" = "external_EXCLUDE"
        )"""

    test {
        sql """show databases from test_lower_case_exclude"""

        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_exclude"]
            expectedDatabases.each { dbName ->
                assertFalse(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    // Test include with mapping
    sql """drop catalog if exists test_lower_case_include """

    sql """ CREATE CATALOG `test_lower_case_include` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            'meta_names_mapping' = '${mapping_db}',
            "include_database_list" = "external_INCLUDE"
        )"""

    test {
        sql """show databases from test_lower_case_include"""

        rowNum 3

        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_include_test"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_lower_case_include """

    // Test exclude with mapping
    sql """drop catalog if exists test_lower_case_exclude """

    sql """ CREATE CATALOG `test_lower_case_exclude` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            'meta_names_mapping' = '${mapping_db}',
            "exclude_database_list" = "external_EXCLUDE"
        )"""

    test {
        sql """show databases from test_lower_case_exclude"""

        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_exclude_test"]
            expectedDatabases.each { dbName ->
                assertFalse(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    sql """drop catalog if exists test_lower_case_exclude """
    sql """drop database if exists internal.external_INCLUDE; """
    sql """drop database if exists internal.external_EXCLUDE; """

    try_sql """drop user ${jdbcUser}"""
}
