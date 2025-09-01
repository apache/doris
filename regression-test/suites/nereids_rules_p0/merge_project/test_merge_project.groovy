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

suite('test_merge_project', 'nonConcurrent') {
    setFeConfigTemporary([expr_children_limit : 200]) {
        def tbl = 'tbl_test_merge_project'
        multi_sql """
            SET ignore_shape_nodes='PhysicalDistribute';
            drop table if exists ${tbl} force;
            create table ${tbl} (a int, b int) properties('replication_num' = '1');
            insert into ${tbl} values (2, 1), (4, 3);
            """

        explainAndOrderResult 'exceeds_expression_limit', """
            select
                case
                  when k15 > k16 then k15 + k16 + 1
                  when k15 > k16 - 1 then k15 + k16 - 1
                  when k15 > k16 - 2 then k15 + k16 - 2
                  else 0
                end as k17,
                k15 + k16 as k18
            from
            (select
                case
                  when k13 > k14 then k13 + k14 + 1
                  when k13 > k14 - 1 then k13 + k14 - 1
                  when k13 > k14 - 2 then k13 + k14 - 2
                  else 0
                end as k15,
                k13 + k14 as k16
            from
            (select
                case
                  when k11 > k12 then k11 + k12 + 1
                  when k11 > k12 - 1 then k11 + k12 - 1
                  when k11 > k12 - 2 then k11 + k12 - 2
                  else 0
                end as k13,
                k11 + k12 as k14
            from
            (select
                case
                  when k9 > k10 then k9 + k10 + 1
                  when k9 > k10 - 1 then k9 + k10 - 1
                  when k9 > k10 - 2 then k9 + k10 - 2
                  else 0
                end as k11,
                k9 + k10 as k12
            from
            (select
                case
                  when k7 > k8 then k7 + k8 + 1
                  when k7 > k8 - 1 then k7 + k8 - 1
                  when k7 > k8 - 2 then k7 + k8 - 2
                  else 0
                end as k9,
                k7 + k8 as k10
            from
            (select
               case
                 when k5 > k6 then k5 + k6 + 1
                 when k5 > k6 - 1 then k5 + k6 - 1
                 when k5 > k6 - 2 then k5 + k6 - 2
                 else 0
               end as k7,
               k5 + k6 as k8
            from
            (select
               case
                 when k3 > k4 then k3 + k4 + 1
                 when k3 > k4 - 1 then k3 + k4 - 1
                 when k3 > k4 - 2 then k3 + k4 - 2
                 else 0
               end as k5,
               k3 + k4 as k6
            from
            (select
                case
                  when k1 > k2 then k1 + k2 + 1
                  when k1 > k2 - 1 then k1 + k2 - 1
                  when k1 > k2 - 2 then k1 + k2 - 2
                  else 0
                end as k3,
                k1 + k2 as k4
            from
            (select a as k1, b as k2
            from ${tbl}) t1) t2) t3) t4) t5) t6) t7) t8;
            """
    }
}
