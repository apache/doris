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

suite("test_delete_predicate_on_value") {

    def tableName = "test_delete_on_value"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """ CREATE TABLE ${tableName} (
            `x` BIGINT NOT NULL,
            `y` BIGINT NULL,
            `z` BIGINT NULL)
            ENGINE=OLAP
            UNIQUE KEY(`x`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`x`) BUCKETS 4
            PROPERTIES (
                "replication_num" = "1",
                "disable_auto_compaction" = "true",
                "enable_unique_key_merge_on_write" = "true",
                "enable_mow_light_delete" = "true"
            );"""
    sql """ insert into ${tableName} values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9); """
    qt_sql_1 "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where y=4;"
    qt_sql_1 "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where z>=3 and z<=7;"
    qt_sql_1 "select * from ${tableName} order by x,y,z;"
    sql "set skip_delete_predicate=true;"
    qt_skip_delete_predicate_sql_1 "select x,y,z,__DORIS_DELETE_SIGN__ from ${tableName} order by x,y,z,__DORIS_DELETE_SIGN__;"
    sql "set skip_delete_predicate=false;"
    sql "insert into ${tableName} values(4,4,4),(5,5,5);"
    qt_sql_1 "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where y=5;"
    qt_sql_1 "select * from ${tableName} order by x,y,z;"

    sql "set skip_delete_predicate=true;"
    qt_skip_delete_predicate_sql_1 "select x,y,z,__DORIS_DELETE_SIGN__ from ${tableName} order by x,y,z,__DORIS_DELETE_SIGN__;"
    sql "set skip_storage_engine_merge=false;"

    sql "DROP TABLE IF EXISTS ${tableName};"


    def tableName2 = "test_delete_on_value2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """ CREATE TABLE ${tableName2} (
            `x` BIGINT NOT NULL,
            `y` BIGINT REPLACE_IF_NOT_NULL NULL,
            `z` BIGINT REPLACE_IF_NOT_NULL NULL)
            ENGINE=OLAP
            AGGREGATE KEY(`x`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`x`) BUCKETS 4
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1"
            );"""
    sql """ insert into ${tableName2} values(1,1,1); """
    test {
        sql "delete from ${tableName2} where y=4;"
        exception "delete predicate on value column only supports Unique table with merge-on-write enabled and Duplicate table, but Table[test_delete_on_value2] is an Aggregate table."
    }

    def tableName3 = "test_delete_on_value_with_seq_col"
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """ CREATE TABLE ${tableName3} (
            `x` BIGINT NOT NULL,
            `y` BIGINT NULL,
            `z` BIGINT NULL)
            ENGINE=OLAP
            UNIQUE KEY(`x`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`x`) BUCKETS 4
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true",
                "enable_mow_light_delete" = "true",
                "function_column.sequence_col" = "z"
            );"""
    sql "insert into ${tableName3} values(1,1,10);"
    sql "insert into ${tableName3} values(1,1,5);"
    qt_sql_3 "select * from ${tableName3} order by x,y,z;"

    sql "set skip_storage_engine_merge=true;"
    sql "set skip_delete_bitmap=true;"
    sql "set skip_delete_predicate=true;"
    qt_skip_delete_predicate_sql_3 "select * from ${tableName3} order by x,y,z;"
    sql "set skip_storage_engine_merge=false;"
    sql "set skip_delete_bitmap=false;"
    sql "set skip_delete_predicate=false;"

    sql "delete from ${tableName3} where z>=10;"
    qt_sql_3 "select * from ${tableName3} order by x,y,z;"

    sql "set skip_storage_engine_merge=true;"
    sql "set skip_delete_bitmap=true;"
    sql "set skip_delete_predicate=true;"
    qt_skip_delete_predicate_sql_3 "select * from ${tableName3} order by x,y,z;"
    sql "set skip_storage_engine_merge=false;"
    sql "set skip_delete_bitmap=false;"
    sql "set skip_delete_predicate=false;"
    sql "DROP TABLE IF EXISTS ${tableName3}"


    def tableName4 = "test_delete_on_value_with_seq_col_mor"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """ CREATE TABLE ${tableName4} (
            `x` BIGINT NOT NULL,
            `y` BIGINT NULL,
            `z` BIGINT NULL)
            ENGINE=OLAP
            UNIQUE KEY(`x`)
            COMMENT 'OLAP'
            DISTRIBUTED BY HASH(`x`) BUCKETS 4
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "false",
                "function_column.sequence_col" = "z"
            );"""
    // test mor table
    sql "insert into ${tableName4} values(1,1,10);"
    sql "insert into ${tableName4} values(1,1,5);"
    qt_sql_4 "select * from ${tableName4} order by x,y,z;"
    sql "delete from ${tableName4} where z>=10;"
    qt_sql_4 "select * from ${tableName4} order by x,y,z;"

    sql "set skip_storage_engine_merge=true;"
    sql "set skip_delete_bitmap=true;"
    sql "set skip_delete_predicate=true;"
    qt_skip_delete_predicate_sql_4 "select * from ${tableName4} order by x,y,z;"
    sql "set skip_storage_engine_merge=false;"
    sql "set skip_delete_bitmap=false;"
    sql "set skip_delete_predicate=false;"

    sql "DROP TABLE IF EXISTS ${tableName4};"
}
