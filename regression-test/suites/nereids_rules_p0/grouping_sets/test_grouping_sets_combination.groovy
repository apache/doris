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
suite("test_grouping_sets_combination") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql """
         DROP TABLE IF EXISTS mal_test1
        """

    sql """
        create table mal_test1(pk int, a int, b int) distributed by hash(pk) buckets 10
        properties('replication_num' = '1'); 
        """

    sql """
        insert into mal_test1 values(2,1,3),(1,1,2),(3,5,6),(6,null,6),(4,5,6);
     """


    sql """drop table if exists test_sql;"""
    sql """
        CREATE TABLE `test_sql` (
        `user_id` varchar(10) NULL,
        `dt` date NULL,
        `city` varchar(20) NULL,
        `age` int(11) NULL
        ) ENGINE=OLAP
        UNIQUE KEY(`user_id`)
        COMMENT 'test'
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """ insert into test_sql values (1,'2020-09-09',2,3);"""

    sql "DROP TABLE IF EXISTS agg_test_table_t;"
    sql """
        CREATE TABLE `agg_test_table_t` (
        `k1` varchar(65533) NULL,
        `k2` text NULL,
        `k3` text null,
        `k4` text null
        ) ENGINE=OLAP
        DUPLICATE KEY(`k1`)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(`k1`) BUCKETS 10
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "is_being_synced" = "false",
        "storage_format" = "V2",
        "light_schema_change" = "true",
        "disable_auto_compaction" = "false",
        "enable_single_replica_compaction" = "false"
        );
    """

    sql """insert into agg_test_table_t(`k1`,`k2`,`k3`) values('20231026221524','PA','adigu1bububud');"""

    sql "sync"

    qt_sum_distinct """
        select sum(distinct a) col1 from mal_test1 group by grouping sets ((b),(pk),()) order by col1;
    """
    qt_count_distinct """
        select count(distinct a) col1 from mal_test1 group by grouping sets ((b),(pk),()) order by col1;
     """

    qt_count_distinct_multi """
          select count(distinct a) col1,count(distinct b) col2 from mal_test1 
          group by grouping sets ((b),(pk),()) order by col1, col2;
     """

    // group_concat's result is not stable, disable for now
    // qt_group_concat_distinct """
    //       select pk, group_concat(distinct cast(a as varchar(10))) col1 from mal_test1 
    //       group by grouping sets ((b),(pk),()) order by 1,2;
    // """

    qt_agg_subquery_expression """
        select sum(a+(select sum(a) from mal_test1)) col1 from mal_test1 group by grouping sets ((b),(pk),()) order by col1;
    """

    qt_agg_subquery_expression_distinct """
        select sum(distinct a+(select sum(a) from mal_test1)) col1 from mal_test1 group by grouping sets ((b),(pk),()) order by col1;
    """

    qt_expression_after_agg_func1 """
        select a,sum(b)+1 from mal_test1 group by grouping sets((a)) order by 1,2;
    """

    qt_expression_before_agg_func1 """
        select a,sum(b+1) from mal_test1  group by grouping sets((a)) order by 1,2;
    """
    qt_expression_before_agg_func2 """
        select a,sum(abs(b)) from mal_test1 group by grouping sets((a)) order by 1,2;
    """

    qt_expression_after_agg_func2 """
        select a, abs(sum(b)) from mal_test1 group by grouping sets((a))order by 1,2;
    """

    qt_agg_func_window_func_rank """
       select sum(rank() over (partition by a order by pk)) from mal_test1 group by grouping sets((a)) order by 1;
    """

    qt_agg_func_window_func_sum """
        select sum(sum(a) over (partition by a order by pk)) from mal_test1 group by grouping sets((a)) order by 1;
    """

    qt_window_func_expression_subquery """
        select sum(age + (select sum(age) from test_sql)) over() from test_sql group by grouping sets ((dt,age)) order by 1;
    """
    qt_window_func_subquery """
        Select sum( (select sum(age) from test_sql)) over() from test_sql group by grouping sets ((city)) order by 1;
    """
    qt_agg_func_distinct_case_when """
         select count(distinct case when t.k2='PA' and loan_date=to_date(substr(t.k1,1,8)) then t.k2 end )
         from (select substr(k1,1,8) loan_date,k3,k2,k1 from agg_test_table_t) t group by grouping sets((substr(t.k1,1,8)))
         order by 1;
    """

    qt_expression_after_agg_func_multi_grouping_sets1 """
        select a,sum(b)+1 col1 from mal_test1 group by grouping sets((a),(b),(a,b)) order by a,col1;
    """

    qt_expression_before_agg_func_multi_grouping_sets1 """
        select a,sum(b+1) from mal_test1  group by grouping sets((a),(b),()) order by 1,2;
    """
    qt_expression_before_agg_func_multi_grouping_sets2 """
        select a,sum(abs(b)) from mal_test1 group by grouping sets((a),(b),(),(a,b)) order by 1,2;
    """

    qt_expression_after_agg_func_multi_grouping_sets2 """
        select a, abs(sum(b)) from mal_test1 group by grouping sets((a),(b),(),(a,b))order by 1,2;
    """

    qt_agg_func_window_func_rank_multi_grouping_sets """
       select sum(rank() over (partition by a order by pk)) from mal_test1 group by grouping sets((a),(b),(a,b)) order by 1;
    """

    qt_agg_func_window_func_sum_multi_grouping_sets """
        select sum(sum(a) over (partition by a order by pk)) from mal_test1 group by grouping sets((a),(b),()) order by 1;
    """

    qt_window_func_expression_subquery_multi_grouping_sets """
        select sum(age + (select sum(age) from test_sql)) over() from test_sql group by grouping sets ((dt,age),(dt),()) order by 1;
    """
    qt_window_func_subquery_multi_grouping_sets """
        Select sum((select sum(age) from test_sql)) over() from test_sql group by grouping sets ((city),(age),()) order by 1;
    """
}
