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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

suite('rf_merge_context_reuse') {
    sql 'drop table if exists rf_merge_reuse_edges'
    sql '''create table rf_merge_reuse_edges (src int, dst int) duplicate key(src)
            distributed by hash(src) buckets 4 properties ("replication_num" = "1")'''
    sql 'insert into rf_merge_reuse_edges values (1, 2), (2, 3), (3, 4)'

    String plan = sql('''explain with recursive numbers(n) as (
                            select cast(1 as int)
                            union all
                            select cast(e.dst as int) from rf_merge_reuse_edges e
                            inner join [shuffle] numbers r on e.src = r.n where r.n < 4
                         ) select n from numbers order by n''').toString()
    // The recursive join must publish RFs across an Exchange on every round;
    // a constants-only recursive query never exercises the merge-context reset.
    assertTrue(plan.contains('INNER JOIN(PARTITIONED)'))
    assertTrue(plan.contains('runtime filters: RF'))
    assertTrue(plan.contains('-> src'))
    assertTrue(plan.contains('VEXCHANGE'))

    order_qt_recursive '''with recursive numbers(n) as (
                            select cast(1 as int)
                            union all
                            select cast(e.dst as int) from rf_merge_reuse_edges e
                            inner join [shuffle] numbers r on e.src = r.n where r.n < 4
                          ) select n from numbers order by n'''
}
