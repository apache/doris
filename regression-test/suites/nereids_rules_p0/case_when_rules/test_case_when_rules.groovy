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

suite("test_case_when_rules") {

    // this test will invoke many rules, including:
    // PUSH_INTO_CASE_WHEN_BRNACH
    // CASE_WHEN_TO_COMPOUND_PREDICATE
    // NULL_SAFE_EQUAL_TO_EQUAL
    // SIMPLIFY_RANGE

    sql """
        SET ignore_shape_nodes='PhysicalDistribute';
        SET runtime_filter_mode=OFF;
        SET disable_join_reorder=true;
        drop table if exists tbl_test_case_when_rules force;
        create table tbl_test_case_when_rules(a bigint, b bigint) properties('replication_num' = '1');
        insert into tbl_test_case_when_rules values(null, 0), (1, 10), (2, 20), (3, 30), (4, 40), (5, 50), (6, 60)
        """

    explainAndOrderResult 'sql_1', '''
        select * 
        from (
            select a, b, case when a = 1 then 101 when a = 2 then 102 when a = 3 then 103 when a = 4 then 104 end as k
            from tbl_test_case_when_rules
        ) s 
        where k = 101 or k = 103 or k = 105
        '''

    explainAndOrderResult 'sql_2', '''
        select * 
        from (
            select a, b, case when a = 1 then 101 when a = 2 then 102 when a = 3 then 103 when a = 4 then 104 end as k
            from tbl_test_case_when_rules
        ) s 
        where k > 103
        '''

    explainAndOrderResult 'sql_3', '''
        select * 
        from (
            select a, b, case when a = 1 then 101 when a = 2 then 102 when a = 3 then 103 when a = 4 then 104 else 107 end as k
            from tbl_test_case_when_rules
        ) s 
        where k > 103
        '''

    explainAndOrderResult 'sql_4', '''
        select * 
        from (
            select a, b, case when a = 1 then 101 when a = 2 then 102 when a = 3 then 103 when a = 4 then 104 else 107 end as k
            from tbl_test_case_when_rules
        ) s 
        where k != 103
        '''

    explainAndOrderResult 'sql_5', '''
        select * 
        from (
            select a, b, case when a = 1 then 101 when a = 2 then 102 when a = 3 then 103 when a = 4 then 104 else 107 end as k
            from tbl_test_case_when_rules
        ) s 
        where k < 103
        '''
}
