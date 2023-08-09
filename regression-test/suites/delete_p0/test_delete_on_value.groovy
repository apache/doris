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

suite("test_delete_on_value") {
    sql 'set enable_nereids_planner=false'
    sql "set experimental_enable_nereids_planner=false;"
    sql 'set enable_nereids_dml=false'

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
                "enable_unique_key_merge_on_write" = "true"
            );"""
    sql """ insert into ${tableName} values(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6),(7,7,7),(8,8,8),(9,9,9); """
    sql "set skip_delete_sign=true;"
    qt_sql "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where y=4;"
    qt_sql "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where z>=3 and z<=7;"
    qt_sql "select * from ${tableName} order by x,y,z;"
    sql "set skip_delete_predicate=true;"
    qt_sql "select x,y,z,__DORIS_DELETE_SIGN__ from ${tableName} order by x,y,z,__DORIS_DELETE_SIGN__;"
    sql "set skip_delete_predicate=false;"
    sql "insert into ${tableName} values(4,4,4),(5,5,5);"
    qt_sql "select * from ${tableName} order by x,y,z;"
    sql "delete from ${tableName} where y=5;"
    qt_sql "select * from ${tableName} order by x,y,z;"
    sql "set skip_storage_engine_merge=true;"
    sql "set skip_delete_bitmap=true;"
    sql "set skip_delete_predicate=true;"
    qt_sql "select x,y,z,__DORIS_DELETE_SIGN__ from ${tableName} order by x,y,z,__DORIS_DELETE_SIGN__;"
}
