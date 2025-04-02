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

suite("test_kill") {
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
	
	def db_name = "test_kill_db"
    sql """DROP DATABASE IF EXISTS ${db_name}"""
    sql """CREATE DATABASE ${db_name}"""
    sql """use ${db_name}"""
	
    sql """  
       CREATE TABLE `test_employees`( `id` tinyint, `name` char(20) ) ENGINE=OLAP DUPLICATE KEY(`id`, `name`) DISTRIBUTED BY HASH(`id`) BUCKETS 1 PROPERTIES ( "replication_allocation" = "tag.location.default: 1" );
    """
	
    sql """insert into test_employees values (1, 'A_%');"""
	sql """insert into test_employees values (2, 'B_%');"""
	def result = sql """SHOW PROCESSLIST;"""
	connectionId0 = result[0][1]
	sql """kill ${connectionId0}"""
	
	result = sql """SHOW PROCESSLIST;"""
	connectionId1 = result[0][1]
	sql """kill connection ${connectionId1}"""
	
	result = sql """SHOW PROCESSLIST;"""
	connectionId2 = result[0][1]
	queryId0 = result[0][10]
	sql """kill query '${queryId0}'"""
	sql """kill connection ${connectionId2}"""
	
	sql "DROP DATABASE IF EXISTS ${db_name}"
}