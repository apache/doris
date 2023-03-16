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
    sql("drop database IF EXISTS ${dbName1}")
    sql("drop database IF EXISTS ${dbName2}")
    sql("drop database IF EXISTS ${dbName3}")

    // test backends
    sql("use information_schema")
    qt_backends("select count(*) >= 1 from backends")

    // test charsets
    sql("use information_schema")
    qt_charsets("select count(*) >= 1 from character_sets")

    // test collations
    sql("use information_schema")
    qt_collations("select count(*) >= 1 from collations")

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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """
    sql("use information_schema")
    qt_columns("select TABLE_CATALOG, COLUMN_NAME, ORDINAL_POSITION, DATA_TYPE, COLUMN_TYPE, COLUMN_SIZE from columns where TABLE_SCHEMA = '${dbName1}' or TABLE_SCHEMA = '${dbName2}' or TABLE_SCHEMA = '${dbName3}'")

    // test files
    // have no impl

    // test partitions
    // have no impl

    // test rowsets
    // have no tablet system table, add this later 

    // test schemata
    // create test dbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")

    sql("use information_schema")
    qt_schemata("select CATALOG_NAME, SCHEMA_NAME, SQL_PATH from schemata where SCHEMA_NAME = '${dbName1}' or SCHEMA_NAME = '${dbName2}' or SCHEMA_NAME = '${dbName3}'");

    // test statistics
    // have no impl

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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
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
            "replication_num" = "1",
            "disable_auto_compaction" = "true"
        );
    """

    sql("use information_schema")
    qt_tables("select TABLE_CATALOG, TABLE_NAME, TABLE_TYPE, AVG_ROW_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH from tables where TABLE_SCHEMA = '${dbName1}' or TABLE_SCHEMA = '${dbName2}' or TABLE_SCHEMA = '${dbName3}'");

    // test variables
    // session_variables
    sql("use information_schema")
    sql("SET wait_timeout = 30000")
    qt_session_variables("select VARIABLE_NAME, VARIABLE_VALUE from session_variables where VARIABLE_NAME = 'wait_timeout'")
    
    // global_variables
    sql("use information_schema")
    sql("SET GLOBAL wait_timeout = 31000")
    qt_global_variables("select VARIABLE_NAME, VARIABLE_VALUE from global_variables where VARIABLE_NAME = 'wait_timeout'")

    // test user_privileges
    sql("CREATE USER 'test_sys_tables'")
    sql("GRANT SELECT_PRIV ON *.*.* TO 'test_sys_tables'")
    sql("use information_schema")
    qt_user_privileges """
        select GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE from user_privileges where GRANTEE regexp '^\\'test'
    """
    sql("DROP USER 'test_sys_tables'")

    // test views
    sql("use ${dbName1}")
    sql """
        CREATE VIEW IF NOT EXISTS ${dbName1}.test_view (a)
        AS
        SELECT ccc as a FROM ${tbName1}
    """
    sql("use information_schema")
    qt_views("select TABLE_NAME, VIEW_DEFINITION from views where TABLE_SCHEMA = '${dbName1}'")

    // test a large amount of data
    def dbPrefix = "db_query_sys_tables_with_lots_of_tables_"
    def tablePrefix = "tb_query_sys_tables_with_lots_of_tables_"

    // create lots of dbs and tables to make rows in `information_schema.columns`
    for (int i = 1; i <= 10; i++) {
        def dbName = dbPrefix + i.toString()
        sql "CREATE DATABASE IF NOT EXISTS `${dbName}`"
        sql "USE `${dbName}`"
        for (int j = 1; j <= 1000; j++) {
            def tableName = tablePrefix + j.toString();
            sql """
                CREATE TABLE IF NOT EXISTS `${tableName}` (
                `aaa` varchar(170) NOT NULL COMMENT "",
                `bbb` varchar(100) NOT NULL COMMENT "",
                `ccc` varchar(170) NULL COMMENT "",
                `ddd` varchar(120) NULL COMMENT "",
                `eee` varchar(120) NULL COMMENT "",
                `fff` varchar(130) NULL COMMENT "",
                `ggg` varchar(170) NULL COMMENT "",
                `hhh` varchar(170) NULL COMMENT "",
                `jjj` varchar(170) NULL COMMENT "",
                `kkk` varchar(170) NULL COMMENT "",
                `lll` varchar(170) NULL COMMENT "",
                `mmm` varchar(170) NULL COMMENT "",
                `nnn` varchar(70) NULL COMMENT "",
                `ooo` varchar(140) NULL COMMENT "",
                `ppp` varchar(70) NULL COMMENT "",
                `qqq` varchar(130) NULL COMMENT "",
                `rrr` bigint(20) NULL COMMENT "",
                `sss` bigint(20) NULL COMMENT "",
                `ttt` decimal(24, 2) NULL COMMENT "",
                `uuu` decimal(24, 2) NULL COMMENT "",
                `vvv` decimal(24, 2) NULL COMMENT "",
                `www` varchar(50) NULL COMMENT "",
                `xxx` varchar(190) NULL COMMENT "",
                `yyy` varchar(190) NULL COMMENT "",
                `zzz` varchar(100) NULL COMMENT "",
                `aa` bigint(20) NULL COMMENT "",
                `bb` bigint(20) NULL COMMENT "",
                `cc` bigint(20) NULL COMMENT "",
                `dd` varchar(60) NULL COMMENT "",
                `ee` varchar(60) NULL COMMENT "",
                `ff` varchar(60) NULL COMMENT "",
                `gg` varchar(50) NULL COMMENT "",
                `hh` bigint(20) NULL COMMENT "",
                `ii` bigint(20) NULL COMMENT ""
                ) ENGINE=OLAP
                DUPLICATE KEY(`aaa`)
                COMMENT "OLAP"
                DISTRIBUTED BY HASH(`aaa`) BUCKETS 1
                PROPERTIES (
                "replication_allocation" = "tag.location.default: 1",
                "in_memory" = "false",
                "storage_format" = "V2"
                )
            """
        }
    }

    for (int i = 1; i <= 10; i++) {
        def dbName = dbPrefix + i.toString()
        sql "USE information_schema"
        qt_sql "SELECT COUNT(*) FROM `columns` WHERE TABLE_SCHEMA='${dbName}'"
    }

    sql "USE information_schema"
    qt_sql "SELECT COLUMN_KEY FROM `columns` WHERE TABLE_SCHEMA='db_query_sys_tables_with_lots_of_tables_1' and TABLE_NAME='tb_query_sys_tables_with_lots_of_tables_1' and COLUMN_NAME='aaa'"

    for (int i = 1; i <= 10; i++) {
        def dbName = dbPrefix + i.toString()
        sql "DROP DATABASE `${dbName}`"
    }

    // test no impl schema table
    sql "USE information_schema"
    qt_sql "select * from column_privileges"
    qt_sql "select * from engines"
    qt_sql "select * from events"
    qt_sql "select * from routines"
    qt_sql "select * from referential_constraints"
    qt_sql "select * from key_column_usage"
    qt_sql "select * from triggers"
}