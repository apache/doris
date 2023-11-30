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

suite("nereids_test_create_blocked") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'

    try {
        sql """ 
        CREATE TABLE test_create_blocked (
            c1 all
        )
        DISTRIBUTED BY HASH(c1)
        PROPERTIES(
            'replication_num'='1'
        ); 
        """
        assertTrue(false, "should not be able to execute")
    } catch (Exception ex) {
        assertTrue(ex.getMessage().contains("Disable to create table with `ALL` type columns"))
    } finally {
        sql """ DROP TABLE IF EXISTS test_create_blocked"""
    }

    def testTable = "test_time_range_table"

    sql "DROP TABLE IF EXISTS ${testTable}"

    // multi-line sql
    def result1 = sql """
    create table ${testTable} (
        `actorid` varchar(128),
        `gameid` varchar(128),
        `eventtime` datetimev2(3)
    )
    engine=olap
    duplicate key(actorid, gameid, eventtime)
    partition by range(eventtime)(
        from ("2000-01-01") to ("2021-01-01") interval 1 year,
        from ("2021-01-01") to ("2022-01-01") interval 1 MONth,
        from ("2022-01-01") to ("2023-01-01") interval 1 WEEK,
        from ("2023-01-01") TO ("2023-02-01") interval 1 DAY
    )
    distributed by hash(actorid) buckets 1
    properties(
        "replication_num"="1",
        "light_schema_change"="true",
        "compression"="zstd"
    );
    """

    // DDL/DML return 1 row and 1 column, the only value is update row count
    assertTrue(result1.size() == 1)
    assertTrue(result1[0].size() == 1)
    assertTrue(result1[0][0] == 0, "Create table should update 0 rows")

    def table = "test_ct_strlen"
    try {
        sql """
        create table ${table} (
            k1 CHAR, 
            K2 CHAR(10) ,  
            K3 VARCHAR , 
            K4 VARCHAR(10)
        ) 
        duplicate key (k1) 
        distributed by hash(k1) buckets 1 
        properties(
            'replication_num' = '1'
        );
        """

        qt_create """ desc ${table}; """

    } finally {
        sql """ DROP TABLE IF EXISTS ${table}; """
    }

    try {
        sql """
        CREATE TABLE IF NOT EXISTS `test_fnf` (
            `test_varchar` varchar(1) NULL,
            `0test_varchar` varchar(1) NULL,
            `@test_varchar` varchar(1) NULL,
            `_test_varchar` varchar(1) NULL,
            `_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL,
            `@test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL,
            `0test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_var` varchar(1) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`test_varchar`)
        DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
        """
    } catch (java.sql.SQLException t){
        assertTrue(false)
    }

    try {
        sql """
        CREATE TABLE IF NOT EXISTS `test_fnf_error_1` (
            `test@_varchar` varchar(1) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`test_varchar`)
        DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
        """
    } catch (java.sql.SQLException t){
        assertTrue(true)
    }

    try {
        sql """
        CREATE TABLE IF NOT EXISTS `test_fnf_error_2` (
            `test_varchar` varchar(1) NULL,
            `_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varchar_test_varc` varchar(1) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`test_varchar`)
        DISTRIBUTED BY HASH(`test_varchar`) BUCKETS 3
        PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "in_memory" = "false",
            "storage_format" = "V2"
        )
        """
    } catch (java.sql.SQLException t){
        assertTrue(true)
    } finally {
        sql """ DROP TABLE IF EXISTS test_fnf """
        sql """ DROP TABLE IF EXISTS test_fnf_error_1 """

        sql """ DROP TABLE IF EXISTS test_fnf_error_2 """
    }
}