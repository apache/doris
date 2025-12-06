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
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
// OF ANY KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite('window_with_sort') {
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "drop table if exists tbl_window_with_sort force"
    sql "create table tbl_window_with_sort (a int, b int) distributed by hash(a) buckets 1 properties ('replication_num' = '1')"
    sql "insert into tbl_window_with_sort values (1, 2), (1, 1), (1, 3), (2, 1), (2, 3)"
    explainAndResult 'select_1', '''
        select a, b
        from tbl_window_with_sort
        order by sum(a + b) over (partition by b), rank() over(), a, b
        '''
    explainAndResult 'select_2', '''
        select a, b, sum(a + b) over (partition by b), rank() over(), max(a) over()
        from tbl_window_with_sort
        order by sum(a + b) over (partition by b), rank() over(), a, b
        '''
    sql "drop table if exists tbl_window_with_sort force"
}
