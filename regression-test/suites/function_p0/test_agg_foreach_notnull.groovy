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

suite("test_agg_foreach_not_null") {
   // for nereids_planner
   // now support  min min_by maxmax_by avg avg_weighted sum stddev stddev_samp_foreach variance var_samp
   // covar covar_samp corr
   // topn topn_array topn_weighted
   // count  count_by_enum approx_count_distinct
   // PERCENTILE PERCENTILE_ARRAY PERCENTILE_APPROX
   // histogram
   // GROUP_BIT_AND GROUP_BIT_OR GROUP_BIT_XOR
   // any_value
   // array_agg map_agg
   // collect_set collect_list
   // retention
   // not support
   // GROUP_BITMAP_XOR BITMAP_UNION HLL_UNION_AGG GROUPING GROUPING_ID BITMAP_AGG SEQUENCE-MATCH SEQUENCE-COUNT


    sql """ set enable_nereids_planner=true;"""
    sql """ set enable_fallback_to_original_planner=false;"""
   
    sql """
        drop table if exists foreach_table_not_null;
    """

    sql """
       CREATE TABLE IF NOT EXISTS foreach_table_not_null (
              `id` INT(11) not null COMMENT "",
              `a` array<INT> not null   COMMENT "",
              `b` array<array<INT>>  not null  COMMENT "",
              `s` array<String>  not null   COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into foreach_table_not_null values
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"]),
    (2,[20],[[2]],["cd"]),
    (3,[100],[[1]],["efg"]) , 
    (4,[null,2],[[2],null],[null,'c']);
   """

   // this case also test combinator should be case-insensitive
   qt_sql """
       select min_ForEach(a), min_by_foreach(a,a),max_foreach(a),max_by_foreach(a,a) , avg_foreach(a),avg_weighted_foreach(a,a) from foreach_table_not_null ;
   """

   qt_sql """
   select  sum_foreach(a)  , stddev_foreach(a) ,stddev_samp_foreach(a)  , variance_foreach(a) , var_samp_foreach(a) from foreach_table_not_null ;
   """

   qt_sql """
   select covar_foreach(a,a)  , covar_samp_foreach(a,a) , corr_foreach(a,a) from foreach_table_not_null ; 
   """
    qt_sql """
   select topn_foreach(a,a) ,topn_foreach(a,a,a)  , topn_array_foreach(a,a) ,topn_array_foreach(a,a,a)from foreach_table_not_null ;
   """


   qt_sql """
   select count_foreach(a)  , count_by_enum_foreach(a)  , approx_count_distinct_foreach(a) from foreach_table_not_null;
   """

   qt_sql """
   select histogram_foreach(a) from foreach_table_not_null;
   """
   
   qt_sql """
      select PERCENTILE_foreach(a,a)  from foreach_table_not_null;
   """
  
   qt_sql """
      select PERCENTILE_ARRAY_foreach(a,b) from foreach_table_not_null where id = 1;
   """

   qt_sql """

   select PERCENTILE_APPROX_foreach(a,a) from foreach_table_not_null;
   """

   qt_sql """
   select GROUP_BIT_AND_foreach(a), GROUP_BIT_OR_foreach(a), GROUP_BIT_XOR_foreach(a)  from foreach_table_not_null;
   """

   qt_sql """
   select GROUP_CONCAT_foreach(s), GROUP_CONCAT_foreach(s,s) from foreach_table_not_null;
   """
   
   qt_sql """
   select retention_foreach(a), retention_foreach(a,a ),retention_foreach(a,a,a) , retention_foreach(a,a,a ,a) from foreach_table_not_null;
   """
   
   qt_sql """
   select any_value_foreach(s), any_value_foreach(a) from foreach_table_not_null;
   """

   qt_sql """
   select collect_set_foreach(a), collect_set_foreach(s) , collect_set_foreach(a,a) from foreach_table_not_null;
   """

   qt_sql """
   select collect_list_foreach(a), collect_list_foreach(s) , collect_list_foreach(a,a) from foreach_table_not_null;
   """

   qt_sql """
   select map_agg_foreach(a,a), map_agg_foreach(a,s) , map_agg_foreach(s,s) from foreach_table_not_null;
   """
}
