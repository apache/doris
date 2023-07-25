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

suite('nereids_delete_mow_partial_update') {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set experimental_enable_nereids_planner=true;"
    sql 'set enable_nereids_dml=true'

    def tableName1 = "nereids_delete_mow_partial_update1"
    sql "DROP TABLE IF EXISTS ${tableName1};"

    sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
                `uid` BIGINT NULL,
                `v1` BIGINT NULL 
            )UNIQUE KEY(uid)
        DISTRIBUTED BY HASH(uid) BUCKETS 3
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        );"""
    def tableName2 = "nereids_delete_mow_partial_update2"
    sql "DROP TABLE IF EXISTS ${tableName2};"

    sql """ CREATE TABLE IF NOT EXISTS ${tableName2} (
                `uid` BIGINT NULL
            ) UNIQUE KEY(uid)
        DISTRIBUTED BY HASH(uid) BUCKETS 3
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        );"""
    
    sql "insert into ${tableName1} values(1,1),(2,2),(3,3),(4,4),(5,5);"
    qt_sql "select * from ${tableName1} order by uid;"
    sql "insert into ${tableName2} values(1),(2),(3);"
    sql "delete from ${tableName1} A using ${tableName2} B where A.uid=B.uid;"
    qt_sql "select * from ${tableName1} order by uid;"
    sql "set skip_delete_predicate=true;"
    // if not use partial update insert stmt for delete stmt, it will use delete pedicate to "delete" the rows
    // when using parital update insert stmt for delete stmt, it will use delete bitmap or delete sign rather than 
    // delete predicate to "delete" the rows
    qt_sql_skip_delete_predicate "select * from ${tableName1} order by uid;"
}
