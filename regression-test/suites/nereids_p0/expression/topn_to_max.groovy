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

suite("test_topn_to_max") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'

    sql 'drop table if exists test_topn_to_max;'

    sql '''create table test_topn_to_max (k1 int, k2 string) distributed by hash(k1) buckets 3 properties('replication_num' = '1');'''
    sql '''insert into test_topn_to_max values (1, "1"),  (2, "2");'''


    order_qt_sql '''
    select k1, topn(k2, 1)
    from test_topn_to_max
    group by k1;
    '''
    res = sql '''
    explain rewritten plan select k1, topn(k2, 1)
    from test_topn_to_max
    group by k1;
    '''
    assertTrue(res.toString().contains("max"), res.toString() + " should contain max")

    order_qt_sql '''
    select topn(k2, 1)
    from test_topn_to_max;
    '''
    res = sql '''
    explain rewritten plan select topn(k2, 1)
    from test_topn_to_max;
    '''
    assertTrue(res.toString().contains("max"), res.toString() + " should contain max")
}
