
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

suite("test_mow_update_on_current_timestamp", "p0") {
    sql 'set experimental_enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql 'set enable_nereids_dml=true'
    sql "sync;"


    def t1 = "test_mow_update_on_current_timestamp1"
    sql """ DROP TABLE IF EXISTS ${t1};"""
    sql """ CREATE TABLE ${t1} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) DEFAULT "unknown" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321",
                `update_time` datetime default current_timestamp on update current_timestamp)
            UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"
            );"""
    
    // don't set partial_columns header, it's a row update
    streamLoad {
        table "${t1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'id,name,score,test,dft'

        file 'update_on_current_timestamp1.csv'
        time 10000 // limit inflight 10s
    }
    sql """ sync; """
    qt_sql "select id,name,score,test,dft from ${t1} order by id;"
    qt_sql "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';"


    // set partial columns header, it's a partial update
    // don't specify the `update_time` column
    // it will be automatically updated to current_timestamp()
    streamLoad {
        table "${t1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'update_on_current_timestamp2.csv'
        time 10000 // limit inflight 10s
    }
    sql """ sync; """
    qt_sql "select id,name,score,test,dft from ${t1} order by id;"
    qt_sql "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';"

    // when user specify that column, it will be filled with the input value
    streamLoad {
        table "${t1}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,update_time'

        file 'update_on_current_timestamp3.csv'
        time 10000 // limit inflight 10s
    }
    sql """ sync; """
    qt_sql "select id,name,score,test,dft from ${t1} order by id;"
    qt_sql "select count(distinct update_time) from ${t1} where update_time > '2023-10-01 00:00:00';"
    qt_sql "select count(distinct update_time) from ${t1};"
}
