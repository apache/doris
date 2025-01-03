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

suite("test_meta_names_mapping", "p0,external,doris,meta_names_mapping") {

    String jdbcUrl = context.config.jdbcUrl
    String jdbcUser = "test_meta_names_mapping_user"
    String jdbcPassword = "C123_567p"
    String s3_endpoint = getS3Endpoint()
    String bucket = getS3BucketName()
    String driver_url = "https://${bucket}.${s3_endpoint}/regression/jdbc_driver/mysql-connector-j-8.3.0.jar"

    try_sql """drop user ${jdbcUser}"""
    sql """create user ${jdbcUser} identified by '${jdbcPassword}'"""
    sql """grant all on *.*.* to ${jdbcUser}"""

    String validMetaNamesMapping = """
    {
        "databases": [
            {"remoteDatabase": "EXTERNAL_META_NAMES_MAPPING", "mapping": "external_meta_names_mapping_upper"},
            {"remoteDatabase": "external_meta_names_mapping", "mapping": "external_meta_names_mapping_lower"}
        ],
        "tables": [
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "table_test", "mapping": "table_test_lower"},
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "TABLE_TEST", "mapping": "table_test_upper"}
        ],
        "columns": [
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "TABLE_TEST", "remoteColumn": "column_test", "mapping": "column_test_local"}
        ]
    }
    """

    sql """drop database if exists internal.external_meta_names_mapping; """
    sql """drop database if exists internal.EXTERNAL_META_NAMES_MAPPING; """
    sql """create database if not exists internal.external_meta_names_mapping; """
    sql """create database if not exists internal.EXTERNAL_META_NAMES_MAPPING; """

    sql """create table if not exists internal.external_meta_names_mapping.table_test
         (id int, name varchar(20), column_test int)
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """
    sql """create table if not exists internal.external_meta_names_mapping.TABLE_TEST
         (id int, name varchar(20), column_test int)
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """
    sql """create table if not exists internal.EXTERNAL_META_NAMES_MAPPING.table_test
         (id int, name varchar(20), column_test int)
         distributed by hash(id) buckets 10
         properties('replication_num' = '1'); 
         """

    sql """insert into internal.external_meta_names_mapping.table_test values(1, 'lowercase', 100);"""
    sql """insert into internal.external_meta_names_mapping.TABLE_TEST values(2, 'UPPERCASE', 200);"""
    sql """insert into internal.EXTERNAL_META_NAMES_MAPPING.table_test values(3, 'MIXEDCASE', 300);"""

    sql """drop catalog if exists test_valid_meta_names_mapping """
    sql """ CREATE CATALOG `test_valid_meta_names_mapping` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "external_meta_names_mapping,EXTERNAL_META_NAMES_MAPPING",
            "meta_names_mapping" = '${validMetaNamesMapping}'
        )"""

    test {
        sql """show databases from test_valid_meta_names_mapping"""
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_meta_names_mapping_upper", "external_meta_names_mapping_lower"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_valid_meta_names_mapping.external_meta_names_mapping_lower"""
        check { result, ex, startTime, endTime ->
            def expectedTables = ["table_test_lower", "table_test_upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    test {
        sql """describe test_valid_meta_names_mapping.external_meta_names_mapping_lower.table_test_upper"""
        check { result, ex, startTime, endTime ->
            def expectedColumns = ["column_test_local"]
            expectedColumns.each { columnName ->
                assertTrue(result.collect { it[0] }.contains(columnName), "Expected column '${columnName}' not found in result")
            }
        }
    }

    qt_sql_meta_mapping_select_lower "select * from test_valid_meta_names_mapping.external_meta_names_mapping_lower.table_test_lower"
    qt_sql_meta_mapping_select_upper "select * from test_valid_meta_names_mapping.external_meta_names_mapping_lower.table_test_upper"

    sql """drop catalog if exists test_valid_meta_names_mapping """

    sql """ drop catalog if exists test_conflict_meta_names """
    sql """ CREATE CATALOG `test_conflict_meta_names` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "lower_case_meta_names" = "true",
            "only_specified_database" = "true",
            "include_database_list" = "external_meta_names_mapping,EXTERNAL_META_NAMES_MAPPING"
        )"""

    test {
        sql """show databases from test_conflict_meta_names"""
        exception """Found conflicting database names under case-insensitive conditions. Conflicting remote database names: EXTERNAL_META_NAMES_MAPPING, external_meta_names_mapping in catalog test_conflict_meta_names. Please use meta_names_mapping to handle name mapping."""
    }

    sql """refresh catalog test_conflict_meta_names"""

    test {
        sql """select * from test_conflict_meta_names.external_meta_names_mapping.table_test"""
        exception """Found conflicting database names under case-insensitive conditions. Conflicting remote database names: EXTERNAL_META_NAMES_MAPPING, external_meta_names_mapping in catalog test_conflict_meta_names. Please use meta_names_mapping to handle name mapping."""
    }

    String validMetaNamesMapping2 = """
    {
        "databases": [
            {"remoteDatabase": "EXTERNAL_META_NAMES_MAPPING", "mapping": "external_meta_names_mapping_upper"},
            {"remoteDatabase": "external_meta_names_mapping", "mapping": "external_meta_names_mapping_lower"}
        ]
    }
    """

    sql """alter catalog test_conflict_meta_names set properties('meta_names_mapping' = '${validMetaNamesMapping2}')"""

    test {
        sql """show tables from test_conflict_meta_names.external_meta_names_mapping_lower"""
        exception """Found conflicting table names under case-insensitive conditions. Conflicting remote table names: TABLE_TEST, table_test in remote database 'external_meta_names_mapping' under catalog 'test_conflict_meta_names'. Please use meta_names_mapping to handle name mapping."""
    }

    sql """refresh catalog test_conflict_meta_names"""

    test {
        sql """select * from test_conflict_meta_names.external_meta_names_mapping_lower.table_test"""
        exception """Found conflicting table names under case-insensitive conditions. Conflicting remote table names: TABLE_TEST, table_test in remote database 'external_meta_names_mapping' under catalog 'test_conflict_meta_names'. Please use meta_names_mapping to handle name mapping."""
    }

    String validMetaNamesMapping3 = """
    {
        "databases": [
            {"remoteDatabase": "EXTERNAL_META_NAMES_MAPPING", "mapping": "external_meta_names_mapping_upper"},
            {"remoteDatabase": "external_meta_names_mapping", "mapping": "external_meta_names_mapping_lower"}
        ],
        "tables": [
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "TABLE_TEST", "mapping": "table_test_upper"},
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "table_test", "mapping": "table_test_lower"}
        ]
    }
    """

    sql """alter catalog test_conflict_meta_names set properties('meta_names_mapping' = '${validMetaNamesMapping3}')"""

    test {
        sql """show databases from test_conflict_meta_names"""
        check { result, ex, startTime, endTime ->
            def expectedDatabases = ["external_meta_names_mapping_upper", "external_meta_names_mapping_lower"]
            expectedDatabases.each { dbName ->
                assertTrue(result.collect { it[0] }.contains(dbName), "Expected database '${dbName}' not found in result")
            }
        }
    }

    test {
        sql """show tables from test_conflict_meta_names.external_meta_names_mapping_lower"""
        check { result, ex, startTime, endTime ->
            def expectedTables = ["table_test_lower", "table_test_upper"]
            expectedTables.each { tableName ->
                assertTrue(result.collect { it[0] }.contains(tableName), "Expected table '${tableName}' not found in result")
            }
        }
    }

    qt_sql_meta_mapping_select_lower_lower_case_true "select * from test_conflict_meta_names.external_meta_names_mapping_lower.table_test_lower"
    qt_sql_meta_mapping_select_upper_lower_case_true "select * from test_conflict_meta_names.external_meta_names_mapping_lower.table_test_upper"

    sql """drop catalog if exists test_conflict_meta_names """

    String error_mapping_db = """
    {
        "databases": [
            {"remoteDatabase": "EXTERNAL_META_NAMES_MAPPING", "mapping": "external_meta_names_mapping_upper"},
            {"remoteDatabase": "EXTERNAL_META_NAMES_MAPPING", "mapping": "external_meta_names_mapping_lower"}
        ]
    }
    """

    sql """drop catalog if exists test_error_mapping_db """

    test {
        sql """ CREATE CATALOG `test_error_mapping_db` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "external_meta_names_mapping,EXTERNAL_META_NAMES_MAPPING",
            "meta_names_mapping" = '${error_mapping_db}'
        )"""

        exception "Duplicate remoteDatabase found: EXTERNAL_META_NAMES_MAPPING"
    }

    sql """drop catalog if exists test_error_mapping_db """

    String error_mapping_tbl = """
    {
        "tables": [
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "TABLE_TEST", "mapping": "table_test_upper"},
            {"remoteDatabase": "external_meta_names_mapping", "remoteTable": "TABLE_TEST", "mapping": "table_test_lower"}
        ]
    }
    """

    sql """drop catalog if exists test_error_mapping_tbl """

    test {
        sql """ CREATE CATALOG `test_error_mapping_tbl` PROPERTIES (
            "user" = "${jdbcUser}",
            "type" = "jdbc",
            "password" = "${jdbcPassword}",
            "jdbc_url" = "${jdbcUrl}",
            "driver_url" = "${driver_url}",
            "driver_class" = "com.mysql.cj.jdbc.Driver",
            "only_specified_database" = "true",
            "include_database_list" = "external_meta_names_mapping,EXTERNAL_META_NAMES_MAPPING",
            "meta_names_mapping" = '${error_mapping_tbl}'
        )"""

        exception "Duplicate remoteTable found in database external_meta_names_mapping: TABLE_TEST"
    }

    sql """drop catalog if exists test_error_mapping_tbl """

    sql """drop catalog if exists test_alter_error_mapping """

    sql """ CREATE CATALOG `test_alter_error_mapping` PROPERTIES (
        "user" = "${jdbcUser}",
        "type" = "jdbc",
        "password" = "${jdbcPassword}",
        "jdbc_url" = "${jdbcUrl}",
        "driver_url" = "${driver_url}",
        "driver_class" = "com.mysql.cj.jdbc.Driver",
        "only_specified_database" = "true",
        "include_database_list" = "external_meta_names_mapping,EXTERNAL_META_NAMES_MAPPING"
    )"""

    test {
        sql """alter catalog test_alter_error_mapping set properties('meta_names_mapping' = '${error_mapping_db}')"""
        exception "Duplicate remoteDatabase found: EXTERNAL_META_NAMES_MAPPING"
    }

    test {
        sql """alter catalog test_alter_error_mapping set properties('meta_names_mapping' = '${error_mapping_tbl}')"""
        exception "Duplicate remoteTable found in database external_meta_names_mapping: TABLE_TEST"
    }

    sql """drop catalog if exists test_alter_error_mapping """

    sql """drop database if exists internal.external_meta_names_mapping; """
    sql """drop database if exists internal.EXTERNAL_META_NAMES_MAPPING; """

    try_sql """drop user ${jdbcUser}"""
}