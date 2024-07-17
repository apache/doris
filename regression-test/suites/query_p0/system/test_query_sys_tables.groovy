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
    qt_desc_files """desc `information_schema`.`files` """
    order_qt_query_files """ select * from `information_schema`.`files` """

    //test information_schema.statistics
    // have no impl
    qt_desc_statistics """desc `information_schema`.`statistics` """
    order_qt_query_statistics """ select * from `information_schema`.`statistics` """

    //test information_schema.table_constraints 
    // have no impl
    qt_desc_statistics """desc `information_schema`.`table_constraints` """
    order_qt_query_table_constraints """ select * from `information_schema`.`table_constraints` """
    

    // test schema_privileges
    sql """  DROP USER if exists 'cyw'; """   
    qt_desc_schema_privileges """desc `information_schema`.`schema_privileges` """
    order_qt_schema_privileges1 """  select * from information_schema.schema_privileges where GRANTEE = "'root'@'%'" ; """    
    sql """  CREATE USER 'cyw'; """
    order_qt_schema_privileges2 """  select * from information_schema.schema_privileges where GRANTEE = "'cyw'@'%'" ;  """  
    sql """  DROP USER 'cyw'; """
    order_qt_schema_privileges3 """  select * from information_schema.schema_privileges where GRANTEE = "'cyw'@'%'" ;  """  

    
    // test table_privileges
    sql """  DROP USER if exists 'cywtable'; """   
    qt_desc_table_privileges """desc `information_schema`.`table_privileges` """
    order_qt_table_privileges """  select * from information_schema.table_privileges where GRANTEE = "'cywtable'@'%'" ;  """  
    sql """  CREATE USER 'cywtable'; """
    sql """ CREATE DATABASE IF NOT EXISTS table_privileges_demo  """
    sql """ create table IF NOT EXISTS table_privileges_demo.test_table_privileges( 
            a int , 
            b boolean , 
            c string ) 
        DISTRIBUTED BY HASH(`a`) BUCKETS 1 
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_single_replica_compaction"="true"
        );"""
    
    sql """ GRANT SELECT_PRIV,ALTER_PRIV,LOAD_PRIV ON table_privileges_demo.test_table_privileges  TO 'cywtable'@'%'; """
    order_qt_table_privileges2  """  select * from information_schema.table_privileges where GRANTEE = "'cywtable'@'%'" order by PRIVILEGE_TYPE ; """
    sql """ REVOKE SELECT_PRIV ON table_privileges_demo.test_table_privileges FROM 'cywtable'@'%'; """ 
    order_qt_table_privileges3  """  select * from information_schema.table_privileges where GRANTEE = "'cywtable'@'%'" order by PRIVILEGE_TYPE ; """


    // test partitions
    // have no impl
    qt_desc_partitions """ desc `information_schema`.`partitions` """ 
    order_qt_select_partitions """ select * from  `information_schema`.`partitions`; """ 

    // test schemata
    // create test dbs
    sql("CREATE DATABASE IF NOT EXISTS ${dbName1}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName2}")
    sql("CREATE DATABASE IF NOT EXISTS ${dbName3}")

    sql("use information_schema")
    qt_schemata("select CATALOG_NAME, SCHEMA_NAME, SQL_PATH from schemata where SCHEMA_NAME = '${dbName1}' or SCHEMA_NAME = '${dbName2}' or SCHEMA_NAME = '${dbName3}' order by SCHEMA_NAME");

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
    qt_tables("select TABLE_CATALOG, TABLE_NAME, TABLE_TYPE, AVG_ROW_LENGTH, MAX_DATA_LENGTH, INDEX_LENGTH from tables where TABLE_SCHEMA = '${dbName1}' or TABLE_SCHEMA = '${dbName2}' or TABLE_SCHEMA = '${dbName3}' order by TABLE_NAME");

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
    try {
        sql("CREATE USER 'original_test_sys_tables'")
    } catch (Exception e) {
        assertTrue(e.getMessage().contains("already exist"), e.getMessage())
    }
    sql("GRANT SELECT_PRIV ON *.*.* TO 'original_test_sys_tables'")
    sql("use information_schema")
    qt_user_privileges """
        select GRANTEE, PRIVILEGE_TYPE, IS_GRANTABLE from user_privileges where GRANTEE regexp '^\\'original_test_sys_tables'
    """
    sql("DROP USER 'original_test_sys_tables'")

    // test views
    sql("use ${dbName1}")
    sql """
        CREATE VIEW IF NOT EXISTS ${dbName1}.test_view (a)
        AS
        SELECT ccc as a FROM ${tbName1}
    """

    sql("use information_schema")
    qt_views("select TABLE_NAME, VIEW_DEFINITION from views where TABLE_SCHEMA = '${dbName1}'")

    // test no impl schema table
    sql "USE information_schema"
    qt_sql "select * from column_privileges"
    qt_sql "select * from engines"
    qt_sql "select * from events"
    qt_sql "select * from routines"
    qt_sql "select * from referential_constraints"
    qt_sql "select * from key_column_usage"
    qt_sql "select * from triggers"
    qt_sql "select * from parameters"
    qt_sql "select * from profiling"
}