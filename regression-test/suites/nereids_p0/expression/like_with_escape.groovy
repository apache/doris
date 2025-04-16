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

suite("like_with_escape") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
	
	sql """drop database if exists test_like_with_escape"""
    sql """create database test_like_with_escape"""
    sql """use test_like_with_escape"""
	sql "drop table if exists test_employees"
	
	sql """  
       CREATE TABLE `test_employees_001`( `id` tinyint, `name` char(20) ) ENGINE=OLAP DUPLICATE KEY(`id`, `name`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" );
    """

	sql """insert into test_employees_001 values (1, 'A_%'),(2, 'B_%'),(3, 'C_D'),(4, 'E_F'),(5, 'F_%');"""

	def result = sql "select * from test_employees_001 where name like '%\_\%' escape '\\';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees_001 where name like '%|_|%' escape '|';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees_001 where name like '%@_@%' escape '@';"
	assertEquals(3, result.size())
	
	result = sql "select * from test_employees_001 where name like '%#_#%' escape '#';"
	assertEquals(3, result.size())
	
	sql """drop database if exists test_like_with_escape"""
}