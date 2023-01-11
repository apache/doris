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

suite("test_query_sys_tables", "query,p0") {
    def dbName1 = "test_query_sys_db_1"
    def dbName2 = "test_query_sys_db_2"
    def dbName3 = "test_query_sys_db_3"
    def tbName1 = "test_query_sys_tb_1"
    def tbName2 = "test_query_sys_tb_2"
    def tbName3 = "test_query_sys_tb_3"

    // test backends
    sql("use information_schema")
    qt_backends("select count(*) from backends")
    
    // test charsets
    sql("use information_schema")
    qt_charsets("select * from character_sets")

    // test collations
    sql("use information_schema")
    qt_collations("select * from collations")

    // test columns
    // create test dbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")    
    // create test tbs
    sql("use ${dbName1}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName1}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql("use ${dbName2}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName2}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql("use ${dbName3}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName3}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql("use information_schema")
    qt_columns("select * from columns where TABLE_SCHEMA = '${dbName1}' or TABLE_SCHEMA = '${dbName2}' or TABLE_SCHEMA = '${dbName3}'")

    // test files
    sql("use information_schema")
    qt_files("select * from files")

    // test partitions
    sql("use information_schema")
    qt_partitions("select * from partitions")

    // test rowsets
    sql("use information_schema")
    qt_rowsets1("select count(*) from rowsets");
    qt_rowsets2("select ROWSET_ID, TABLET_ID, DATA_DISK_SIZE, CREATION_TIME from rowsets")

    // test schema privileges
    sql("use information_schema")
    qt_schema_privileges("select * from schema_privileges")

    // test schemata
    // create test dbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")    

    sql("use information_schema")
    qt_schemata("select CATALOG_NAME, DEFAULT_CHARACTER_SET_NAME, SQL_PATH from schemata where SCHEMA_NAME = '${dbName1}' or SCHEMA_NAME = '${dbName2}' or SCHEMA_NAME = '${dbName3}'");
    
    // test statistics
    sql("use information_schema")
    qt_statistics("select * from statistics")

    // test table privileges
    sql("use information_schema")
    qt_table_privileges("select * from table_privileges")

    // test tables
    // create test dbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")    
    // create test tbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")    
    // create test tbs
    sql("use ${dbName1}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName1}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql("use ${dbName2}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName2}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql("use ${dbName3}")
    sql """
        CREATE TABLE IF NOT EXISTS `${tbName3}` (
            `aaa` varchar(170) NOT NULL COMMENT "",
            `bbb` varchar(20) NOT NULL COMMENT "",
            `ccc` INT NULL COMMENT "",
            `ddd` SMALLINT NULL COMMENT ""
        )
        DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1"
        );
    """

    sql("use information_schema")
    qt_tables("select TABLE_CATALOG, TABLE_NAME, TABLE_TYPE, AVG_ROW_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH, CREATE_TIME from tables where TABLE_SCHEMA = '${dbName1}' or TABLE_SCHEMA = '${dbName2}' or TABLE_SCHEMA = '${dbName3}'");
    
    // test user privileges
    sql("use information_schema")
    qt_user_privileges("select * from user_privileges")
    
    // test variables
    // session_variables
    sql("use information_schema")
    qt_session_variables("select count(*) from session_variables")
    // global_variables
    sql("use information_schema")
    qt_session_variables("select count(*) from global_variables")
    
    // test views
    sql("use information_schema")
    qt_views("select * from views")
}
