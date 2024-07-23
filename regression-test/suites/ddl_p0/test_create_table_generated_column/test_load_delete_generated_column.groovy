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

suite("test_load_delete_generated_column") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"

    multi_sql"""
        drop table if exists test_stream_delete;
        create table test_stream_delete(a int, b int, c int as (a + b) , d int as (c + 1) )
        unique key(a, b, c) distributed by hash(a) properties("replication_num"="1");;
        insert into test_stream_delete(a, b) values(1, 2) , (3, 5 ), (2, 9 );
    """

    streamLoad {
        table 'test_stream_delete'
        set 'column_separator', ','
        file 'gen_col_data_delete.csv'
        set 'columns', 'a,b'
        time 10000 // limit inflight 10s
        set 'merge_type', 'DELETE'
    }
    sql "sync"
    qt_test_stream_load_delete "select * from test_stream_delete order by 1,2,3,4"

    streamLoad {
        table 'test_stream_delete'
        set 'column_separator', ','
        file 'gen_col_data.csv'
        set 'columns', 'a,b'
        time 10000 // limit inflight 10s
        set 'merge_type', 'MERGE'
        set 'delete','a=3'
    }
    sql "sync"
    qt_test_stream_load_merge "select * from test_stream_delete order by 1,2,3,4"

}