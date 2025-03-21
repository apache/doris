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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility

suite("test_partial_update_row_store", "nonConcurrent") {

    def table1 = "test_partial_update_row_store"
    sql "DROP TABLE IF EXISTS ${table1} FORCE;"
    sql """ CREATE TABLE IF NOT EXISTS ${table1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            c3 int
        )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_mow_light_delete" = "false",
            "disable_auto_compaction" = "true",
            "replication_num" = "1",
            "store_row_column" = "false"); """

    sql "insert into ${table1} values(1,1,1,1),(2,2,2,2),(3,3,3,3);"
    sql "insert into ${table1} values(4,4,4,4),(5,5,5,5),(6,6,6,6);"
    sql "sync;"
    qt_1 "select * from ${table1} order by k1;"

    def doSchemaChange = { cmd ->
        sql cmd
        waitForSchemaChangeDone {
            sql """SHOW ALTER TABLE COLUMN WHERE IndexName='${table1}' ORDER BY createtime DESC LIMIT 1"""
            time 2000
        }
    }

    // turn on row_store_column, but only store part of columns
    doSchemaChange """alter table ${table1} set ("store_row_column" = "true")"""
    doSchemaChange """alter table ${table1} set ("row_store_columns" = "k1,c2")"""
    sql "insert into ${table1} values(7,7,7,7),(8,8,8,8),(9,9,9,9);"

    sql "set enable_unique_key_partial_update=true;"
    sql "set enable_insert_strict=false;"
    sql "sync;"
    sql "insert into ${table1}(k1,c1,c2) values(1,10,10),(2,20,20),(5,50,50),(7,70,70),(100,100,100);"
    qt_2 "select *, LENGTH(__DORIS_ROW_STORE_COL__) from ${table1} order by k1;"
    sql "insert into ${table1}(k1,c3) values(1,99),(3,99),(6,99),(8,99),(200,200);"
    qt_2 "select *, LENGTH(__DORIS_ROW_STORE_COL__) from ${table1} order by k1;"
    sql "set enable_unique_key_partial_update=false;"
    sql "set enable_insert_strict=true;"
    sql "sync;"


    sql "truncate table ${table1};"
    sql "insert into ${table1} values(1,1,1,1),(2,2,2,2),(3,3,3,3);"
    sql "insert into ${table1} values(4,4,4,4),(5,5,5,5),(6,6,6,6);"
    sql "sync;"
    qt_3 "select * from ${table1} order by k1;"


    // turn on full row store column
    doSchemaChange """alter table ${table1} set ("store_row_column" = "true")"""
    sql "insert into ${table1} values(7,7,7,7),(8,8,8,8),(9,9,9,9);"

    sql "set enable_unique_key_partial_update=true;"
    sql "set enable_insert_strict=false;"
    sql "sync;"
    sql "insert into ${table1}(k1,c2) values(2,777),(3,777),(10,777),(21,777),(8,777);"
    qt_4 "select *,LENGTH(__DORIS_ROW_STORE_COL__) from ${table1} order by k1;"
    sql "set enable_unique_key_partial_update=false;"
    sql "set enable_insert_strict=true;"
    sql "sync;"

    // from row store to part columns row store
    doSchemaChange """alter table ${table1} set ("row_store_columns" = "k1,c2")"""
    sql "insert into ${table1} values(11,11,11,11),(20,20,20,20);"

    sql "set enable_unique_key_partial_update=true;"
    sql "set enable_insert_strict=false;"
    sql "sync;"
    sql "insert into ${table1}(k1,c3) values(1,987),(2,987),(11,987),(22,987);"
    qt_4 "select *,LENGTH(__DORIS_ROW_STORE_COL__) from ${table1} order by k1;"
    sql "set enable_unique_key_partial_update=false;"
    sql "set enable_insert_strict=true;"
    sql "sync;"

    // Can not alter store_row_column from true to false currently, should add related case if supported
}
