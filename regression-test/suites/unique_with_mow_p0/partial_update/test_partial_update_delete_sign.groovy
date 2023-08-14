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

suite('test_partial_update_delete_sign') {
    sql 'set enable_nereids_planner=false'
    sql "set experimental_enable_nereids_planner=false;"
    sql 'set enable_nereids_dml=false'

    def tableName1 = "test_partial_update_delete_sign1"
    sql "DROP TABLE IF EXISTS ${tableName1};"
    sql """ CREATE TABLE IF NOT EXISTS ${tableName1} (
            `k1` int NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int,
            `c4` int
            )UNIQUE KEY(k1)
        DISTRIBUTED BY HASH(k1) BUCKETS 1
        PROPERTIES (
            "enable_unique_key_merge_on_write" = "true",
            "disable_auto_compaction" = "true",
            "replication_num" = "1"
        );"""

    sql "insert into ${tableName1} values(1,1,1,1,1),(2,2,2,2,2),(3,3,3,3,3),(4,4,4,4,4),(5,5,5,5,5);"
    qt_sql "select * from ${tableName1} order by k1,c1,c2,c3,c4;"
    streamLoad {
        table "${tableName1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'k1,__DORIS_DELETE_SIGN__'

        file 'delete_sign.csv'
        time 10000 // limit inflight 10s
    }
    sql "sync"
    qt_after_delete "select * from ${tableName1} order by k1,c1,c2,c3,c4;"
    sql "set skip_delete_sign=true;"
    sql "set skip_storage_engine_merge=true;"
    sql "set skip_delete_bitmap=true;"
    sql "sync"
    qt_with_delete_sign "select k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__ from ${tableName1} order by k1,c1,c2,c3,c4,__DORIS_DELETE_SIGN__;"
    sql "drop table if exists ${tableName1};"
}
