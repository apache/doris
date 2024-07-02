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

suite("lateral_view", "arrow_flight_sql") {
    sql """ DROP TABLE IF EXISTS `test_explode_bitmap` """
	sql """
		CREATE TABLE `test_explode_bitmap` (
		  `dt` int(11) NULL COMMENT "",
		  `page` varchar(10) NULL COMMENT "",
		  `user_id` bitmap BITMAP_UNION  COMMENT ""
		) ENGINE=OLAP
		AGGREGATE KEY(`dt`, `page`)
		DISTRIBUTED BY HASH(`dt`) BUCKETS 2 
		properties("replication_num"="1");
	"""
	sql """ insert into test_explode_bitmap values(1, '11', bitmap_from_string("1,2,3"));"""
	sql """ insert into test_explode_bitmap values(2, '22', bitmap_from_string("22,33,44"));"""

	qt_sql_explode_bitmap1 """ select dt, e1 from test_explode_bitmap lateral view explode_bitmap(user_id) tmp1 as e1 order by dt, e1;"""
}
