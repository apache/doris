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

suite("test_partial_update_generated_column") {
    sql "SET enable_nereids_planner=true;"
    sql "SET enable_fallback_to_original_planner=false;"
    sql "set enable_unique_key_partial_update=true"
    sql "set enable_insert_strict=false"

    sql "drop table if exists test_partial_column_unique_gen_col"
    sql """create table test_partial_column_unique_gen_col (a int, b int, c int as(a+b), d int as(c+1), e int)
           unique key(a) distributed by hash(a) properties(
             "enable_unique_key_merge_on_write" = "true",
             "replication_num"="1"
           );"""

    multi_sql """
    insert into test_partial_column_unique_gen_col(a,b,e) values(1,2,7);
    insert into test_partial_column_unique_gen_col(a,b) select 2,3;
    insert into test_partial_column_unique_gen_col(a,b) select 2,4;
    insert into test_partial_column_unique_gen_col(a,b) values(1,6),(3,6);
    insert into test_partial_column_unique_gen_col(a,b) values(1,5);"""
    qt_partial_update_select "select * from test_partial_column_unique_gen_col order by 1,2,3,4,5"

    test {
        sql "insert into test_partial_column_unique_gen_col(a) values(3);"
        exception "Partial update should include all ordinary columns referenced by generated columns, missing: b"
    }

    // test stream load partial update
    multi_sql """
        truncate table test_partial_column_unique_gen_col;
        insert into test_partial_column_unique_gen_col(a,b,e) values(1,4,0),(3,2,8),(2,9,null);
    """

    streamLoad {
        table 'test_partial_column_unique_gen_col'
        set 'column_separator', ','
        file 'gen_col_data.csv'
        set 'columns', 'a,b'
        time 10000 // limit inflight 10s
        set 'partial_columns', 'true'
    }
    sql "sync"
    qt_test_stream_load_refer_gencol "select * from test_partial_column_unique_gen_col order by 1,2,3,4"

    streamLoad {
        table 'test_partial_column_unique_gen_col'
        set 'column_separator', ','
        file 'gen_col_data.csv'
        set 'columns', 'a,e'
        time 10000 // limit inflight 10s
        set 'partial_columns', 'true'
        check {result, exception, startTime, endTime ->
            if (exception != null) {
                throw exception
            }
            log.info("Stream load result: ${result}".toString())
            def json = parseJson(result)
            mustContain(json.Message, "Partial update should include all ordinary columns referenced by generated columns, missing: b")
        }
    }

}