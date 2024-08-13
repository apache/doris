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

suite("test_agg_state_nereids") {
    sql "set enable_agg_state=true"
    sql "set enable_nereids_planner=true;"
    sql "set enable_fallback_to_original_planner=false;"

    sql """ DROP TABLE IF EXISTS d_table; """
    sql """
            create table d_table(
                k1 int null,
                k2 int not null,
                k3 bigint null,
                k4 varchar(100) null
            )
            duplicate key (k1,k2,k3)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    sql "insert into d_table select 1,1,1,'a';"
    sql "insert into d_table select 2,2,2,'b';"
    sql "insert into d_table select 3,3,null,'c';"

    qt_sum """ select sum_merge(sum_state(k1)) from d_table; """
    qt_avg """ select avg_merge(avg_state(k1)) from d_table; """
    qt_max_by """ select max_by_merge(max_by_state(k1,k3)),min_by_merge(min_by_state(k1,k3)) from d_table; """

    qt_sum_const """ select sum_merge(sum_state(1)) from d_table; """
    qt_sum_null """ select sum_merge(sum_state(null)) from d_table; """

    sql """ DROP TABLE IF EXISTS a_table; """
    sql """
            create table a_table(
                k1 int null,
                k2 agg_state<max_by(int not null, int)> generic
            )
            aggregate key (k1)
            distributed BY hash(k1) buckets 3
            properties("replication_num" = "1");
        """

    qt_desc "desc a_table;"

    sql "explain insert into a_table select 1,max_by_state(1,3);"

    sql "insert into a_table select 1,max_by_state(1,3);"
    sql "insert into a_table select 1,max_by_state(2,2);"
    sql "insert into a_table select 1,max_by_state(3,1);"
    sql 'set enable_fallback_to_original_planner=false'

    qt_length1 """select k1,length(k2) from a_table order by k1;"""
    qt_group1 """select k1,max_by_merge(k2) from a_table group by k1 order by k1;"""
    qt_merge1 """select max_by_merge(k2) from a_table;"""

    sql 'set enable_fallback_to_original_planner=true'
    sql "insert into a_table select k1+1, max_by_state(k2,k1+10) from d_table;"
    sql 'set enable_fallback_to_original_planner=false'

    qt_length2 """select k1,length(k2) from a_table order by k1;"""
    qt_group2 """select k1,max_by_merge(k2) from a_table group by k1 order by k1;"""
    qt_merge2 """select max_by_merge(k2) from a_table;"""
    
    qt_union """ select max_by_merge(kstate) from (select k1,max_by_union(k2) kstate from a_table group by k1 order by k1) t; """
    qt_max_by_null """ select max_by_merge(max_by_state(k1,null)),min_by_merge(min_by_state(null,k3)) from d_table; """
}
