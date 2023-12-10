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

suite("nereids_lateral_view") {
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_nereids_planner=true"

    sql """DROP TABLE IF EXISTS nlv_test"""

    sql """
        CREATE TABLE `nlv_test` (
            `c1` int NULL,
            `c2` varchar(100) NULL,
            `c3` varchar(100) NULL,
            `c4` varchar(100) NULL
        ) ENGINE=OLAP
        DISTRIBUTED BY HASH(`c1`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
    """
    sql """INSERT INTO nlv_test VALUES(1, '["abc", "def"]', '[1,2]', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(2, 'valid', '[1,2]', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(3, '["abc", "def"]', 'valid', '[1.1,2.2]')"""
    sql """INSERT INTO nlv_test VALUES(4, '["abc", "def"]', '[1,2]', 'valid')"""


    order_qt_all_function_inner """
        SELECT * FROM nlv_test
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double(c4) lv4 AS clv4
          order by c1, c2, c3, c4, clv1, clv2, clv3, clv4
    """

    order_qt_all_function_outer """
        SELECT * FROM nlv_test
          LATERAL VIEW explode_numbers_outer(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int_outer(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
          order by c1, c2, c3, c4, clv1, clv2, clv3, clv4
    """

    order_qt_column_prune """
        SELECT clv1, clv3, c2, c4 FROM nlv_test
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
          order by c1, c2, c3, c4, clv1, clv2, clv3, clv4
    """

    order_qt_alias_query """
        SELECT clv1, clv3, c2, c4 FROM (SELECT * FROM nlv_test) tmp
          LATERAL VIEW explode_numbers(c1) lv1 AS clv1
          LATERAL VIEW explode_json_array_string_outer(c2) lv2 AS clv2
          LATERAL VIEW explode_json_array_int(c3) lv3 AS clv3
          LATERAL VIEW explode_json_array_double_outer(c4) lv4 AS clv4
         order by c1, c2, c3, c4, clv1, clv2, clv3, clv4
    """

    order_qt_function_nested """
        select * from (
            select 1 hour,'a' pid_code ,'u1' uid, 10 money
            union all
            select 3 hourr,'a' pid_code ,'u1' uid, 10 money
        ) example1 lateral view explode_bitmap(bitmap_from_string("1,2,3,4")) tmp1 as e1 where hour=e1 order by hour;
    """

       sql """ DROP TABLE IF EXISTS test_explode_bitmap"""	
       sql """
		CREATE TABLE `test_explode_bitmap` (
		  `dt` int(11) NULL COMMENT "",
		  `page` varchar(10) NULL COMMENT "",
		  `user_id` bitmap BITMAP_UNION NULL COMMENT ""
		) ENGINE=OLAP
		AGGREGATE KEY(`dt`, `page`)
		DISTRIBUTED BY HASH(`dt`) BUCKETS 2 
		properties("replication_num"="1");
	"""
	sql """ insert into test_explode_bitmap values(1, '11', bitmap_from_string("1,2,3"));"""
	sql """ insert into test_explode_bitmap values(2, '22', bitmap_from_string("22,33,44"));"""
	qt_sql_explode_bitmap """ select dt, e1 from test_explode_bitmap lateral view explode_bitmap(user_id) tmp1 as e1 order by dt, e1;"""

}
