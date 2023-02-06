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

suite("test_count_distinct_with_case_function") {
    sql "DROP DATABASE IF EXISTS test_count_distinct_with_case_function"

    sql "CREATE DATABASE test_count_distinct_with_case_function"
    
    sql "USE test_count_distinct_with_case_function"   
  
    sql """
        CREATE TABLE `a` (
             `k1` int(11) NULL COMMENT "",
  	     `k2` bitmap BITMAP_UNION NULL COMMENT "",
  	     `k3` bitmap BITMAP_UNION NULL COMMENT ""
	) ENGINE=OLAP
	AGGREGATE KEY(`k1`)
	DISTRIBUTED BY HASH(`k1`) BUCKETS 10
	PROPERTIES (
	     "replication_num" = "1",
	     "in_memory" = "false",
	     "storage_format" = "V2"
	);
        """

    sql """
        CREATE TABLE `b` (
  	    `k1` int(11) NULL COMMENT ""
	) ENGINE=OLAP
	DUPLICATE KEY(`k1`)
	DISTRIBUTED BY HASH(`k1`) BUCKETS 10
	PROPERTIES (
             "replication_num" = "1",
	     "in_memory" = "false",
	     "storage_format" = "V2"
	); 
        """
    sql "insert into a values(1,to_bitmap(1),to_bitmap(1));"
    sql "insert into a values(1,to_bitmap(1),to_bitmap(2));"
    sql "insert into a values(2,to_bitmap(1),to_bitmap(1));"
    sql "insert into b values(1);"
    
    qt_select "select count(distinct case when false then k2 when true then k3 end) as tmp from a where k1 in (select k1 from b group by k1);"
}
