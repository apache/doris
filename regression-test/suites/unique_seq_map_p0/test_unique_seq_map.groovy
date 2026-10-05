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


suite("test_unique_seq_map") {
    def dbName = "test_unique_seq_map_db"
    sql """DROP DATABASE IF EXISTS ${dbName}"""
    sql """CREATE DATABASE IF NOT EXISTS ${dbName}"""

    def tableName = "test_unique_seq_map"
    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """

    sql "insert into ${dbName}.$tableName(a, b, c, d, s1) values (1,1,4,4,4), (1,1,2,2,2), (1,1,3,3,3);"
    qt_sql "select * from ${dbName}.$tableName;"
    qt_sql "select a, b, c from ${dbName}.$tableName;"
    qt_sql "select a, b, c, s1 from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, b, c, d, s1) values (1,1,1,1,1);"
    qt_sql "select * from ${dbName}.$tableName;"
    qt_sql "select a, b, c, s1 from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, b, e, s2) values (1,1,2,2);"
    qt_sql "select * from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, b, e, s2) values (1,1,3,3);"
    qt_sql "select * from ${dbName}.$tableName;"
    qt_sql "select a, b, e from ${dbName}.$tableName;"
    qt_sql "select a, b, e, s2 from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, b, c, d, e, s1, s2) values (2,2,2,2,2,4,4);"
    sql "insert into ${dbName}.$tableName(a, b, c, d, e, s1, s2) values (2,2,3,3,3,4,4);"
    qt_sql "select * from ${dbName}.$tableName;"
    qt_sql "select a, b, c, d, e from ${dbName}.$tableName;"

    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """

    // test sequence column before value column
    tableName = "test_unique_seq_map_seq_before_value"
    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    sql "insert into ${dbName}.$tableName(a, b, c, d, e, s1, s2) values (2,2,2,2,2,4,4);"
    qt_sql "select a, b, c, d, e, s1, s2 from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, b, c, d, e, s1, s2) values (2,2,3,3,3,4,4);"
    qt_sql "select a, b, c, d, e, s1, s2 from ${dbName}.$tableName;"

    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """

    // test sequence column without value column
    tableName = "test_unique_seq_map_seq_without_value"
    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `s1` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = ""
            );
    """
    sql "insert into ${dbName}.$tableName(a, s1) values (1,10);"
    qt_sql "select a, s1 from ${dbName}.$tableName;"
    sql "insert into ${dbName}.$tableName(a, s1) values (1,5);"
    qt_sql "select a, s1 from ${dbName}.$tableName;"
    // this line just used to trigger some test pass
    sql "ALTER TABLE ${dbName}.$tableName SET('disable_auto_compaction' = 'true');"
    sql """ DROP TABLE IF EXISTS ${dbName}.$tableName """

    // test create not unique table will failed
    def errorMessage = 'column sequence mapping only be supported in unique table'
    tableName = 'test_unique_seq_map_duplicate'
    def createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    errorMessage = 'column sequence mapping only be supported in unique table'
    tableName = 'test_unique_seq_map_agg'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) SUM NULL COMMENT "",
            `d` int(11) SUM NULL COMMENT "",
            `e` int(11) SUM NULL COMMENT "",
            `s1` int(11) SUM NULL COMMENT "",
            `s2` int(11) SUM NULL COMMENT ""
            ) ENGINE=OLAP
            AGGREGATE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence mapping only support light schema change
    errorMessage = 'sequence mapping rely on light schema change'
    tableName = 'test_unique_seq_map_light_schema_change_false'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="false",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test enable_unique_key_merge_on_write is true
    errorMessage = 'sequence mapping do not support merge on write'
    tableName = 'test_unique_seq_map_enable_unique_key_merge_on_write_true'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test both seq mapping and sequence col
    errorMessage = "column group sequence and sequence column can't apply to both"
    tableName = 'test_unique_seq_map_both_seq_mapping_and_seq_col'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "function_column.sequence_type" = "Date",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence column not specified for sequence_mapping property
    errorMessage = "sequence column of column group should be specified"
    tableName = 'test_unique_seq_map_seq_mapping_without_seq_col'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping." = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence column type not supperted
    errorMessage = 'Unsupported data type in sequence column'
    tableName = 'test_unique_seq_map_seq_col_type_not_supported'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` text NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence column not exists in schema
    errorMessage = 'sequence column [s5] in column_group does not exist in schema'
    tableName = 'test_unique_seq_map_seq_col_not_exists'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s5" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence column can not be key column
    errorMessage = "sequence column [a] in column_group can't be key column"
    tableName = 'test_unique_seq_map_seq_col_key'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.a" = "s2,e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test value column not exist in schema
    errorMessage = "value column [f] in column_group [s2] does not exist in schema"
    tableName = 'test_unique_seq_map_value_not_exists'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e,f"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test value column can not be key column
    errorMessage = "value column [a] in column_group [s2] can't be key column"
    tableName = 'test_unique_seq_map_value_key'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e,a"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test value column overlap
    errorMessage = "The columns are overlapping"
    tableName = 'test_unique_seq_map_value_overlap'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e,c"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test sequence mapping info not cover all value column
    errorMessage = "The columns are not equal between the schema and prop"
    tableName = 'test_unique_seq_map_not_all_value_column'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c",
            "sequence_mapping.s2" = "e"
            );
    """
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test create table with rollup not support
    tableName = 'test_unique_seq_map_rollup'
    createTableStmt = """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            ROLLUP(r1(b,a,c,d,e,s1,s2))
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    errorMessage = 'sequence mapping not support rollup yet'
    expectExceptionLike({
        sql createTableStmt
    }, errorMessage
    )

    // test add rollup after table created
    tableName = 'test_unique_seq_map_rollup_after_create'
    sql """
        CREATE TABLE ${dbName}.$tableName (
            `a` bigint(20) NULL COMMENT "",
            `b` int(11) NULL COMMENT "",
            `c` int(11) NULL COMMENT "",
            `d` int(11) NULL COMMENT "",
            `e` int(11) NULL COMMENT "",
            `s1` int(11) NULL COMMENT "",
            `s2` int(11) NULL COMMENT ""
            ) ENGINE=OLAP
            UNIQUE KEY(`a`, `b`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`a`, `b`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "false",
            "light_schema_change"="true",
            "replication_num" = "1",
            "sequence_mapping.s1" = "c,d",
            "sequence_mapping.s2" = "e"
            );
    """
    errorMessage = 'Can not add rollup when table use column sequence mapping'
    expectExceptionLike({
        sql "ALTER TABLE ${dbName}.$tableName ADD ROLLUP r1 (b,a,c,d,e,s1,s2);"
    }, errorMessage
    )
    errorMessage = 'Can not add materialized view when table use column sequence mapping'
    expectExceptionLike({
        sql "create materialized view mv1 as select b, a, c, d, e, s1, s2 from ${dbName}.$tableName;"
    }, errorMessage
    )

    sql """DROP DATABASE IF EXISTS ${dbName}"""
}

