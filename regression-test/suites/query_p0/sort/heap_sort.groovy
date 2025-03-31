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

// The cases is copied from https://github.com/trinodb/trino/tree/master
// /testing/trino-product-tests/src/main/resources/sql-tests/testcases/aggregate
// and modified by Doris.

suite("heap_sort") {
    sql """
        drop table if exists test_heap_sort;
    """
    sql """
        create table if not exists test_heap_sort (
            f1 int,
            f2 varchar(65533)
        ) properties("replication_num"="1", "disable_auto_compaction"="true");
    """
    streamLoad {
        table "test_heap_sort"

        set 'column_separator', ';'

        file 'heap_sort.csv'

        time 10000 // limit inflight 10s
    }
    sql """ set parallel_pipeline_task_num = 1; """
    sql """ set batch_size = 3; """
    sql """ set force_sort_algorithm = "heap"; """
    qt_select_1 "select * from test_heap_sort order by f1, f2;"
    qt_select_2 "select * from test_heap_sort order by f2 limit 3;"
}