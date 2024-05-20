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

suite("test_case_when_to_if") {
    sql 'set enable_nereids_planner=true'
    sql 'set enable_fallback_to_original_planner=false'
    sql "set disable_nereids_rules=PRUNE_EMPTY_PARTITION"


    sql 'drop table if exists test_case_when_to_if;'

    sql '''create table test_case_when_to_if (k1 int, k2 int) distributed by hash(k1) buckets 3 properties('replication_num' = '1');'''

    // else is empty
    sql '''
    select k2,
        sum(case when (k1=1) then 1 end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    def res = sql '''
    explain rewritten plan select k2,
        sum(case when (k1=1) then 1 end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    assertTrue(res.toString().contains("if"))

    // else is null
    sql '''
    select k2,
        sum(case when (k1=1) then 1 else null end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    def res1 = sql '''
    explain rewritten plan select k2,
        sum(case when (k1=1) then 1 else null end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    assertTrue(res1.toString().contains("if"))

    sql '''
    select k2,
        sum(case when (k1>0) then k1 else abs(k1) end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    def res2 = sql '''
    explain rewritten plan select k2,
        sum(case when (k1>0) then k1 else abs(k1) end) sum1
    from test_case_when_to_if
    group by k2;
    '''
    assertTrue(res2.toString().contains("if"))
}
