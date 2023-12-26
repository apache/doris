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

suite("test_datetime_filter_stats0") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    
    sql "DROP TABLE IF EXISTS test_datetime_filter_stats0"
    sql """ CREATE TABLE `test_datetime_filter_stats0` (
	`id` int(11),
    	`is_delete` int,
    	`company_id` int,
    	`book_time` DATETIMEV2
    )ENGINE=OLAP
    unique key (id)
    distributed by hash(id) buckets 10
    properties(
        "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """ alter table test_datetime_filter_stats0 modify column id set stats('row_count'='52899687', 'ndv'='52899687', 'num_nulls'='0', 'min_value'='1', 'max_value'='52899687', 'data_size'='4'); """
    sql """ alter table test_datetime_filter_stats0 modify column book_time set stats('row_count'='52899687', 'ndv'='23622730', 'num_nulls'='0', 'min_value'='2002-01-01 00:45:39', 'max_value'='2027-09-25 23:03:00', 'data_size'='10'); """
    sql """ alter table test_datetime_filter_stats0 modify column is_delete set stats('row_count'='52899687', 'ndv'='2', 'num_nulls'='0', 'min_value'='0', 'max_value'='1', 'data_size'='4'); """
    sql """ alter table test_datetime_filter_stats0 modify column company_id set stats('row_count'='52899687', 'ndv'='7559', 'num_nulls'='0', 'min_value'='2', 'max_value'='876981', 'data_size'='4'); """

    explain {
        sql("physical plan select count(1) from test_datetime_filter_stats0 o where o.book_time >= '2020-03-01 00:00:00.0' and o.book_time <= '2020-03-01 23:59:59.0';");
        notContains"stats=2.24"
    }

    explain {
        sql("physical plan select count(1) from test_datetime_filter_stats0 o where o.book_time >= '2020-03-01 00:00:00.0' and o.book_time <= '2020-03-01 00:00:01.0';");
        notContains"stats=2.24"
    }
}
