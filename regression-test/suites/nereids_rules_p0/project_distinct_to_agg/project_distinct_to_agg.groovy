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

suite('project_distinct_to_agg') {
    def tbl = 'tbl_project_distinct_to_agg'
    sql "SET ignore_shape_nodes='PhysicalDistribute'"
    sql "drop table if exists ${tbl} force"
    sql "create table ${tbl} (a int) distributed by hash(a) buckets 10 properties ('replication_num' = '1')"
    sql "insert into ${tbl} values (1), (1), (1), (2), (2)"

    explainAndOrderResult 'with_windows_1',  "select distinct a, max(a + 10) over() from ${tbl}"
    explainAndOrderResult 'with_windows_2',  'select distinct sum(value) over(partition by id) from (select 100 value, 1 id union all select 100, 2)a'
    explainAndResult 'order_by',  'select distinct value+1 from (select 100 value, 1 id union all select 100, 2)a order by value+1'
    explainAndOrderResult 'constant',  "select distinct 1, 2, 3 from ${tbl}"
    explainAndOrderResult 'agg',  "select distinct sum(a) from ${tbl}"

    sql "drop table if exists ${tbl} force"
}
