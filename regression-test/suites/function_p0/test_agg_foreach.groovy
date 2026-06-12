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

suite("test_agg_foreach") {
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
	    drop table if exists foreach_table;
	"""

	sql """
       CREATE TABLE IF NOT EXISTS foreach_table (
              `id` INT(11) null COMMENT "",
              `a` array<INT> null  COMMENT "",
              `b` array<array<INT>>  null COMMENT "",
              `s` array<String>  null  COMMENT ""
            ) ENGINE=OLAP
            DUPLICATE KEY(`id`)
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "replication_allocation" = "tag.location.default: 1",
            "storage_format" = "V2"
    );
    """
    sql """
    insert into foreach_table values
    (1,[1,2,3],[[1],[1,2,3],[2]],["ab","123","114514"]),
    (2,[20],[[2]],["cd"]),
    (3,[100],[[1]],["efg"]) , 
    (4,null,[null],null),
    (5,[null,2],[[2],null],[null,'c']);
   """

   // this case also test combinator should be case-insensitive
   	qt_sql """
       select min_ForEach(a), min_by_foreach(a,a),max_foreach(a),max_by_foreach(a,a) , avg_foreach(a),avg_weighted_foreach(a,a) from foreach_table ;
   """

   	qt_sql """
   select  sum_foreach(a)  , stddev_foreach(a) ,stddev_samp_foreach(a)  , variance_foreach(a) , var_samp_foreach(a) from foreach_table ;
   """

   	qt_sql """
   select covar_foreach(a,a)  , covar_samp_foreach(a,a) , corr_foreach(a,a) from foreach_table ; 
   """

    test {
    	sql """select topn_foreach(a,a) from foreach_table;"""
    	exception "errCode"
   	}
    test {
    	sql """select topn_foreach(a,a,a) from foreach_table;"""
    	exception "errCode"
   	}
    test {
    	sql """select topn_array_foreach(a,a) from foreach_table;"""
    	exception "errCode"
   	}
    test {
    	sql """select topn_array_foreach(a,a,a) from foreach_table;"""
    	exception "errCode"
   	}

        qt_sql """
        select count_foreach(a), approx_count_distinct_foreach(a) from foreach_table;
        """

        qt_count_by_enum """
        select
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].cbe."1"'),
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].cbe."20"'),
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].cbe."100"'),
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].notnull'),
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].null'),
            get_json_string(element_at(count_by_enum_foreach(a), 1), '\$.[0].all'),
            get_json_string(element_at(count_by_enum_foreach(a), 2), '\$.[0].cbe."2"'),
            get_json_string(element_at(count_by_enum_foreach(a), 2), '\$.[0].notnull'),
            get_json_string(element_at(count_by_enum_foreach(a), 2), '\$.[0].null'),
            get_json_string(element_at(count_by_enum_foreach(a), 2), '\$.[0].all'),
            get_json_string(element_at(count_by_enum_foreach(a), 3), '\$.[0].cbe."3"'),
            get_json_string(element_at(count_by_enum_foreach(a), 3), '\$.[0].notnull'),
            get_json_string(element_at(count_by_enum_foreach(a), 3), '\$.[0].null'),
            get_json_string(element_at(count_by_enum_foreach(a), 3), '\$.[0].all')
        from foreach_table;
        """

    qt_sql """select array_agg_foreach(a) from foreach_table;"""
   	qt_sql """select array_agg_foreach(s) from foreach_table;"""

    qt_array_agg_nested """
        select /*+ SET_VAR(parallel_pipeline_task_num=1) */
            size(array_agg_foreach(b)),
            size(element_at(array_agg_foreach(b), 1)),
            size(element_at(array_agg_foreach(b), 2)),
            size(element_at(array_agg_foreach(b), 3))
        from foreach_table;
    """

   	qt_sql """
   	select histogram_foreach(a) from foreach_table;
   	"""
   
	test {
		sql """select PERCENTILE_foreach(a,a)  from foreach_table;"""
		exception "Unsupport the func"
	}

	test {
		sql """select PERCENTILE_ARRAY_foreach(a,b) from foreach_table where id = 1;"""
		exception "Unsupport the func"
	}

	test {
		sql """select PERCENTILE_APPROX_foreach(a,a) from foreach_table;"""
		exception "Unsupport the func"
	}
}
